//! The pure "functional core": every rule about *when a connection must go away*.
//!
//! No locks, no I/O, no clocks of its own — just functions of `(connection, config, now)`.
//! Both the sync and async pools call these, so the rules live in exactly one place and can be
//! unit-tested without threads.

use crate::core::connection::{CloseReason, PooledConnection};
use crate::policy::config::{IdleStrategy, PoolConfig};
use crate::rng::Rng;

/// Builds a connection wrapper, setting `expires_at` from `max_lifetime` with per-connection
/// jitter so a whole pool never expires at the same instant.
pub(crate) fn new_connection<T>(
    raw: T,
    cfg: &PoolConfig,
    now: f64,
    rng: &mut dyn Rng,
) -> PooledConnection<T> {
    let expires_at = cfg
        .max_lifetime
        .map(|life| now + life * (1.0 - cfg.lifetime_jitter * rng.next_f64()));
    PooledConnection::new(raw, now, expires_at)
}

/// Pops one idle connection per the strategy (LIFO = hottest/last, FIFO = oldest/first),
/// mutating `idle` in place. Returns `None` when empty.
pub(crate) fn pop_idle<T>(
    idle: &mut Vec<PooledConnection<T>>,
    strategy: IdleStrategy,
) -> Option<PooledConnection<T>> {
    if idle.is_empty() {
        return None;
    }
    match strategy {
        IdleStrategy::Lifo => idle.pop(),
        IdleStrategy::Fifo => Some(idle.remove(0)),
    }
}

/// Connections that must be retired no matter what (lifetime / use-count rotation).
pub(crate) fn rotation_reason<T>(
    c: &PooledConnection<T>,
    cfg: &PoolConfig,
    now: f64,
) -> Option<CloseReason> {
    if let Some(exp) = c.expires_at {
        if now >= exp {
            return Some(CloseReason::Expired);
        }
    }
    if let Some(max) = cfg.max_uses {
        if c.use_count >= max {
            return Some(CloseReason::MaxUses);
        }
    }
    None
}

/// Whether the connection has sat idle at least `max_idle_time`.
pub(crate) fn idle_too_long<T>(c: &PooledConnection<T>, cfg: &PoolConfig, now: f64) -> bool {
    matches!(cfg.max_idle_time, Some(limit) if c.idle_time(now) >= limit)
}

/// Whether the connection should be validated before handing it out.
pub(crate) fn needs_validation<T>(c: &PooledConnection<T>, cfg: &PoolConfig, now: f64) -> bool {
    matches!(cfg.validate_after_idle, Some(limit) if c.idle_time(now) >= limit)
}

/// The static checks applied to an idle connection about to be handed out (validation, which
/// needs I/O, is done by the pool afterwards).
pub(crate) fn borrow_reason<T>(
    c: &PooledConnection<T>,
    cfg: &PoolConfig,
    now: f64,
) -> Option<CloseReason> {
    match rotation_reason(c, cfg, now) {
        Some(r) => Some(r),
        None if idle_too_long(c, cfg, now) => Some(CloseReason::Idle),
        None => None,
    }
}

/// `(keep, doomed)` — the two partitions a [`sweep`] produces.
pub(crate) type SweepResult<T> = (
    Vec<PooledConnection<T>>,
    Vec<(PooledConnection<T>, CloseReason)>,
);

/// Splits idle connections into `(keep, doomed)`. Rotation victims always go; idle-timeout
/// victims go only while the pool stays at or above `cfg.min_size`.
pub(crate) fn sweep<T>(
    idle: Vec<PooledConnection<T>>,
    total_size: usize,
    cfg: &PoolConfig,
    now: f64,
) -> SweepResult<T> {
    let mut keep = Vec::with_capacity(idle.len());
    let mut doomed = Vec::new();
    let mut remaining = total_size;
    for c in idle {
        let mut reason = rotation_reason(&c, cfg, now);
        if reason.is_none() && idle_too_long(&c, cfg, now) && remaining > cfg.min_size {
            reason = Some(CloseReason::Idle);
        }
        match reason {
            None => keep.push(c),
            Some(r) => {
                doomed.push((c, r));
                remaining -= 1;
            }
        }
    }
    (keep, doomed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::SystemRng;

    fn cfg() -> PoolConfig {
        PoolConfig::default()
    }

    #[test]
    fn new_connection_applies_lifetime_jitter() {
        let mut rng = SystemRng::with_seed(1);
        let c = PoolConfig {
            max_lifetime: Some(100.0),
            lifetime_jitter: 0.1,
            ..cfg()
        };
        for _ in 0..200 {
            let pc = new_connection((), &c, 1000.0, &mut rng);
            let exp = pc.expires_at.unwrap();
            // lifetime reduced by up to 10% -> expires in [1090, 1100].
            assert!((1090.0..=1100.0).contains(&exp), "exp {exp}");
        }
        // None lifetime -> no expiry.
        let c2 = PoolConfig {
            max_lifetime: None,
            ..cfg()
        };
        assert!(new_connection((), &c2, 0.0, &mut rng).expires_at.is_none());
    }

    #[test]
    fn pop_idle_lifo_and_fifo() {
        let mut v: Vec<PooledConnection<u32>> = vec![
            PooledConnection::new(1, 0.0, None),
            PooledConnection::new(2, 0.0, None),
        ];
        assert_eq!(pop_idle(&mut v, IdleStrategy::Lifo).unwrap().raw, 2);
        assert_eq!(pop_idle(&mut v, IdleStrategy::Fifo).unwrap().raw, 1);
        assert!(pop_idle(&mut v, IdleStrategy::Lifo).is_none());
    }

    #[test]
    fn rotation_reason_checks_expiry_then_uses() {
        let c = PoolConfig {
            max_uses: Some(3),
            ..cfg()
        };
        let mut pc = PooledConnection::new((), 0.0, Some(50.0));
        assert_eq!(rotation_reason(&pc, &c, 40.0), None);
        assert_eq!(rotation_reason(&pc, &c, 50.0), Some(CloseReason::Expired));
        // not expired but over uses
        let mut pc2 = PooledConnection::new((), 0.0, Some(100.0));
        pc2.use_count = 3;
        assert_eq!(rotation_reason(&pc2, &c, 40.0), Some(CloseReason::MaxUses));
        // no limits
        pc.expires_at = None;
        let c2 = PoolConfig {
            max_uses: None,
            ..cfg()
        };
        assert_eq!(rotation_reason(&pc, &c2, 9999.0), None);
    }

    #[test]
    fn idle_and_validation_thresholds() {
        let c = PoolConfig {
            max_idle_time: Some(10.0),
            validate_after_idle: Some(5.0),
            ..cfg()
        };
        let pc = PooledConnection::new((), 0.0, None); // last_returned = 0
        assert!(!idle_too_long(&pc, &c, 9.0));
        assert!(idle_too_long(&pc, &c, 10.0));
        assert!(!needs_validation(&pc, &c, 4.0));
        assert!(needs_validation(&pc, &c, 5.0));
        // None disables
        let c2 = PoolConfig {
            max_idle_time: None,
            validate_after_idle: None,
            ..cfg()
        };
        assert!(!idle_too_long(&pc, &c2, 1e9));
        assert!(!needs_validation(&pc, &c2, 1e9));
    }

    #[test]
    fn borrow_reason_precedence() {
        let c = PoolConfig {
            max_idle_time: Some(10.0),
            max_uses: Some(1),
            ..cfg()
        };
        // rotation wins over idle
        let mut pc = PooledConnection::new((), 0.0, None);
        pc.use_count = 1;
        assert_eq!(borrow_reason(&pc, &c, 100.0), Some(CloseReason::MaxUses));
        // idle only
        let pc2 = PooledConnection::new((), 0.0, None);
        assert_eq!(borrow_reason(&pc2, &c, 20.0), Some(CloseReason::Idle));
        // neither
        assert_eq!(borrow_reason(&pc2, &c, 1.0), None);
    }

    #[test]
    fn sweep_evicts_rotation_always_and_idle_above_min() {
        let c = PoolConfig {
            min_size: 1,
            max_idle_time: Some(10.0),
            max_lifetime: Some(100.0),
            ..cfg()
        };
        // one expired, two idle-too-long; min_size=1 so only one idle victim allowed.
        let expired = PooledConnection::new(0u32, 0.0, Some(1.0));
        let idle_a = PooledConnection::new(1u32, 0.0, Some(1000.0));
        let idle_b = PooledConnection::new(2u32, 0.0, Some(1000.0));
        let (keep, doomed) = sweep(vec![expired, idle_a, idle_b], 3, &c, 50.0);
        let reasons: Vec<_> = doomed.iter().map(|(_, r)| *r).collect();
        assert!(reasons.contains(&CloseReason::Expired));
        assert!(reasons.contains(&CloseReason::Idle));
        // started at 3, expired drops to 2, one idle drops to 1 == min_size, last idle kept.
        assert_eq!(keep.len(), 1);
        assert_eq!(doomed.len(), 2);
    }
}
