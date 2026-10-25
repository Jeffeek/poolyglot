//! `PoolConfig`: the immutable, validated pool configuration, and `IdleStrategy`.

use crate::core::errors::PoolError;
use crate::resilience::breaker::BreakerConfig;
use crate::resilience::retry::RetryPolicy;

/// Which idle connection is reused first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdleStrategy {
    /// Reuse the hottest connection; cold ones age out and get evicted.
    Lifo,
    /// Reuse the oldest idle connection; spreads load / rotates evenly.
    Fifo,
}

impl IdleStrategy {
    /// The snake_case wire string for this strategy.
    pub fn as_str(&self) -> &'static str {
        match self {
            IdleStrategy::Lifo => "lifo",
            IdleStrategy::Fifo => "fifo",
        }
    }
}

impl std::fmt::Display for IdleStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An immutable, validated pool configuration. Build one with struct-update syntax from
/// [`PoolConfig::default`], then hand it to a pool — which re-validates it.
///
/// ```
/// use connpool::PoolConfig;
/// let cfg = PoolConfig { max_size: 20, min_size: 2, ..Default::default() };
/// assert!(cfg.validate().is_ok());
/// ```
#[derive(Debug, Clone)]
pub struct PoolConfig {
    // --- sizing ---
    /// Minimum connections kept warm by `open()` and the maintenance pass.
    pub min_size: usize,
    /// Hard cap on total connections (idle + in use + being created).
    pub max_size: usize,

    // --- timeouts (seconds; None = no limit) ---
    /// Default wait in `acquire()`.
    pub acquire_timeout: Option<f64>,
    /// Enforced by the async pool only; blocking factories must enforce their own connect
    /// timeout.
    pub create_timeout: Option<f64>,
    /// Backpressure: cap callers blocked in `acquire()` (None = unlimited; 0 = never wait
    /// when full).
    pub max_waiters: Option<usize>,

    // --- rotation / recycling ---
    /// Retire connections after this age.
    pub max_lifetime: Option<f64>,
    /// Shave up to this fraction off each lifetime so a whole pool never expires at once.
    pub lifetime_jitter: f64,
    /// Evict idle connections (down to `min_size`).
    pub max_idle_time: Option<f64>,
    /// Retire after N checkouts.
    pub max_uses: Option<u64>,
    /// LIFO or FIFO idle reuse.
    pub idle_strategy: IdleStrategy,

    // --- health ---
    /// Validate on checkout if idle >= this (0 = always, None = never).
    pub validate_after_idle: Option<f64>,
    /// Background sweep period (None = off).
    pub maintenance_interval: Option<f64>,
    /// Warn when a borrowed connection is held longer than this (None = off); needs `open()`.
    pub leak_detection_timeout: Option<f64>,
    /// Validate idle connections during the maintenance sweep and evict dead ones (needs
    /// `open()`; adds I/O).
    pub validate_idle: bool,

    // --- resilience ---
    /// Retry policy for `factory.create()`.
    pub retry: RetryPolicy,
    /// Circuit breaker config (None = off).
    pub breaker: Option<BreakerConfig>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_size: 0,
            max_size: 10,
            acquire_timeout: Some(30.0),
            create_timeout: Some(10.0),
            max_waiters: None,
            max_lifetime: Some(1800.0),
            lifetime_jitter: 0.1,
            max_idle_time: Some(600.0),
            max_uses: None,
            idle_strategy: IdleStrategy::Lifo,
            validate_after_idle: Some(30.0),
            maintenance_interval: Some(30.0),
            leak_detection_timeout: None,
            validate_idle: false,
            retry: RetryPolicy::default(),
            breaker: Some(BreakerConfig::default()),
        }
    }
}

impl PoolConfig {
    /// Checks the config's invariants, returning [`PoolError::Config`] on the first violation.
    pub fn validate(&self) -> Result<(), PoolError> {
        let need = |ok: bool, msg: &str| -> Result<(), PoolError> {
            if ok {
                Ok(())
            } else {
                Err(PoolError::Config(msg.to_string()))
            }
        };

        need(self.max_size >= 1, "max_size must be >= 1")?;
        need(
            self.min_size <= self.max_size,
            "min_size must be within [0, max_size]",
        )?;
        need(
            self.acquire_timeout.map(|v| v >= 0.0).unwrap_or(true),
            "acquire_timeout must be >= 0",
        )?;
        for (value, name) in [
            (self.create_timeout, "create_timeout"),
            (self.max_lifetime, "max_lifetime"),
            (self.max_idle_time, "max_idle_time"),
            (self.maintenance_interval, "maintenance_interval"),
            (self.leak_detection_timeout, "leak_detection_timeout"),
        ] {
            need(
                value.map(|v| v > 0.0).unwrap_or(true),
                &format!("{name} must be > 0 or None"),
            )?;
        }
        need(
            self.validate_after_idle.map(|v| v >= 0.0).unwrap_or(true),
            "validate_after_idle must be >= 0 or None",
        )?;
        need(
            (0.0..=1.0).contains(&self.lifetime_jitter),
            "lifetime_jitter must be within [0, 1]",
        )?;
        need(
            self.max_uses.map(|v| v >= 1).unwrap_or(true),
            "max_uses must be >= 1 or None",
        )?;
        // max_waiters is usize so it is always >= 0; kept for parity of intent.

        self.retry.validate()?;
        if let Some(b) = &self.breaker {
            b.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_strategy_wire_values() {
        assert_eq!(IdleStrategy::Lifo.as_str(), "lifo");
        assert_eq!(IdleStrategy::Fifo.as_str(), "fifo");
        assert_eq!(IdleStrategy::Fifo.to_string(), "fifo");
        assert_eq!(format!("{:?}", IdleStrategy::Lifo), "Lifo");
    }

    #[test]
    fn defaults_match_spec() {
        let c = PoolConfig::default();
        assert_eq!(c.min_size, 0);
        assert_eq!(c.max_size, 10);
        assert_eq!(c.acquire_timeout, Some(30.0));
        assert_eq!(c.create_timeout, Some(10.0));
        assert_eq!(c.max_waiters, None);
        assert_eq!(c.max_lifetime, Some(1800.0));
        assert_eq!(c.lifetime_jitter, 0.1);
        assert_eq!(c.max_idle_time, Some(600.0));
        assert_eq!(c.max_uses, None);
        assert_eq!(c.idle_strategy, IdleStrategy::Lifo);
        assert_eq!(c.validate_after_idle, Some(30.0));
        assert_eq!(c.maintenance_interval, Some(30.0));
        assert_eq!(c.leak_detection_timeout, None);
        assert!(!c.validate_idle);
        assert!(c.breaker.is_some());
        assert!(c.validate().is_ok());
        assert!(format!("{c:?}").contains("PoolConfig"));
        assert!(c.clone().validate().is_ok());
    }

    #[test]
    fn validation_rejects_bad_values() {
        let bad = |c: PoolConfig| c.validate().unwrap_err().to_string();
        assert_eq!(
            bad(PoolConfig {
                max_size: 0,
                ..Default::default()
            }),
            "max_size must be >= 1"
        );
        assert_eq!(
            bad(PoolConfig {
                min_size: 5,
                max_size: 4,
                ..Default::default()
            }),
            "min_size must be within [0, max_size]"
        );
        assert_eq!(
            bad(PoolConfig {
                acquire_timeout: Some(-1.0),
                ..Default::default()
            }),
            "acquire_timeout must be >= 0"
        );
        assert_eq!(
            bad(PoolConfig {
                create_timeout: Some(0.0),
                ..Default::default()
            }),
            "create_timeout must be > 0 or None"
        );
        assert_eq!(
            bad(PoolConfig {
                max_lifetime: Some(-2.0),
                ..Default::default()
            }),
            "max_lifetime must be > 0 or None"
        );
        assert_eq!(
            bad(PoolConfig {
                validate_after_idle: Some(-1.0),
                ..Default::default()
            }),
            "validate_after_idle must be >= 0 or None"
        );
        assert_eq!(
            bad(PoolConfig {
                lifetime_jitter: 1.5,
                ..Default::default()
            }),
            "lifetime_jitter must be within [0, 1]"
        );
        assert_eq!(
            bad(PoolConfig {
                max_uses: Some(0),
                ..Default::default()
            }),
            "max_uses must be >= 1 or None"
        );
        // propagates retry + breaker validation
        assert!(
            PoolConfig {
                retry: RetryPolicy {
                    max_attempts: 0,
                    ..Default::default()
                },
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            PoolConfig {
                breaker: Some(BreakerConfig {
                    failure_threshold: 0,
                    ..Default::default()
                }),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        // None-valued optionals are accepted
        assert!(
            PoolConfig {
                create_timeout: None,
                max_lifetime: None,
                max_idle_time: None,
                maintenance_interval: None,
                leak_detection_timeout: None,
                validate_after_idle: None,
                acquire_timeout: None,
                breaker: None,
                ..Default::default()
            }
            .validate()
            .is_ok()
        );
    }
}
