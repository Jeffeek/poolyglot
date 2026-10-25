//! Circuit breaker: `closed -> open -> half-open`, counting one failure per logical creation.

use std::sync::{Arc, Mutex};

use crate::core::clock::{Clock, SystemClock};
use crate::core::errors::PoolError;

/// The circuit breaker's state-machine position.
///
/// ```text
/// CLOSED --(N consecutive failures)--> OPEN --(recovery_timeout)--> HALF_OPEN
///    ^                                   ^                              |
///    |                                   +------ any probe fails -------+
///    +------------ enough probes succeed ---------------------------------+
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BreakerState {
    /// Calls flow normally.
    Closed,
    /// Calls are rejected to protect a failing backend.
    Open,
    /// A limited number of probe calls are allowed to test recovery.
    HalfOpen,
}

impl BreakerState {
    /// The snake_case wire string for this state.
    pub fn as_str(&self) -> &'static str {
        match self {
            BreakerState::Closed => "closed",
            BreakerState::Open => "open",
            BreakerState::HalfOpen => "half_open",
        }
    }
}

impl std::fmt::Display for BreakerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Configuration for a [`CircuitBreaker`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BreakerConfig {
    /// Consecutive failures that trip the breaker.
    pub failure_threshold: u32,
    /// Seconds to stay `Open` before probing.
    pub recovery_timeout: f64,
    /// Concurrent probe calls allowed while `HalfOpen`.
    pub half_open_max_calls: u32,
    /// Probe successes needed to close again.
    pub success_threshold: u32,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout: 30.0,
            half_open_max_calls: 1,
            success_threshold: 1,
        }
    }
}

impl BreakerConfig {
    /// Checks the configuration is well-formed.
    pub fn validate(&self) -> Result<(), PoolError> {
        if self.failure_threshold < 1 || self.half_open_max_calls < 1 || self.success_threshold < 1
        {
            return Err(PoolError::Config("breaker thresholds must be >= 1".into()));
        }
        if self.recovery_timeout < 0.0 {
            return Err(PoolError::Config(
                "breaker.recovery_timeout must be >= 0".into(),
            ));
        }
        Ok(())
    }
}

/// A callback invoked (outside the lock) whenever the breaker changes state.
pub type OnStateChange = Box<dyn Fn(BreakerState, BreakerState) + Send + Sync>;

struct Inner {
    state: BreakerState,
    failures: u32,
    opened_at: f64,
    probes_inflight: u32,
    probe_successes: u32,
}

/// A thread-safe circuit breaker. Usage: [`before_call`](CircuitBreaker::before_call) -> do
/// work -> [`record_success`](CircuitBreaker::record_success) /
/// [`record_failure`](CircuitBreaker::record_failure) /
/// [`record_abort`](CircuitBreaker::record_abort).
pub struct CircuitBreaker {
    cfg: BreakerConfig,
    clock: Arc<dyn Clock>,
    on_change: Option<OnStateChange>,
    inner: Mutex<Inner>,
}

impl CircuitBreaker {
    /// Builds a breaker. `clock` defaults to [`SystemClock`] when `None`.
    pub fn new(
        config: BreakerConfig,
        clock: Option<Arc<dyn Clock>>,
        on_state_change: Option<OnStateChange>,
    ) -> Self {
        Self {
            cfg: config,
            clock: clock.unwrap_or_else(|| Arc::new(SystemClock::new())),
            on_change: on_state_change,
            inner: Mutex::new(Inner {
                state: BreakerState::Closed,
                failures: 0,
                opened_at: 0.0,
                probes_inflight: 0,
                probe_successes: 0,
            }),
        }
    }

    /// The current state, refreshing `Open -> HalfOpen` if the recovery timeout elapsed.
    pub fn state(&self) -> BreakerState {
        let mut g = self.inner.lock().unwrap();
        let transition = self.refresh(&mut g);
        let state = g.state;
        drop(g);
        self.emit(transition);
        state
    }

    /// Returns [`PoolError::CircuitOpen`] if the call must not be attempted.
    pub fn before_call(&self) -> Result<(), PoolError> {
        let mut call_err = None;
        let mut g = self.inner.lock().unwrap();
        let transition = self.refresh(&mut g);
        match g.state {
            BreakerState::Open => {
                // refresh() already moved us out of OPEN once recovery elapsed, so wait > 0.
                let wait = (self.cfg.recovery_timeout
                    - (self.clock.monotonic() - g.opened_at))
                    .max(0.0);
                call_err = Some(PoolError::CircuitOpen { retry_after: wait });
            }
            BreakerState::HalfOpen => {
                if g.probes_inflight >= self.cfg.half_open_max_calls {
                    call_err = Some(PoolError::CircuitOpen { retry_after: 0.0 });
                } else {
                    g.probes_inflight += 1;
                }
            }
            BreakerState::Closed => {}
        }
        drop(g);
        self.emit(transition);
        match call_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Records a successful call.
    pub fn record_success(&self) {
        let mut transition = None;
        let mut g = self.inner.lock().unwrap();
        match g.state {
            BreakerState::Closed => g.failures = 0,
            BreakerState::HalfOpen => {
                g.probes_inflight = g.probes_inflight.saturating_sub(1);
                g.probe_successes += 1;
                if g.probe_successes >= self.cfg.success_threshold {
                    transition = self.set_state(&mut g, BreakerState::Closed);
                }
            }
            BreakerState::Open => {}
        }
        drop(g);
        self.emit(transition);
    }

    /// Records a failed call.
    pub fn record_failure(&self) {
        let mut transition = None;
        let mut g = self.inner.lock().unwrap();
        match g.state {
            BreakerState::Closed => {
                g.failures += 1;
                if g.failures >= self.cfg.failure_threshold {
                    transition = self.set_state(&mut g, BreakerState::Open);
                }
            }
            BreakerState::HalfOpen => {
                transition = self.set_state(&mut g, BreakerState::Open);
            }
            // OPEN: a late failure from a call started before tripping; ignore.
            BreakerState::Open => {}
        }
        drop(g);
        self.emit(transition);
    }

    /// Releases a half-open probe slot without counting it either way (the call was cancelled
    /// or timed out before the backend answered).
    pub fn record_abort(&self) {
        let mut g = self.inner.lock().unwrap();
        if g.state == BreakerState::HalfOpen {
            g.probes_inflight = g.probes_inflight.saturating_sub(1);
        }
    }

    /// Convenience wrapper for an arbitrary fallible closure.
    pub fn call<F, R, E>(&self, f: F) -> Result<R, PoolError>
    where
        F: FnOnce() -> Result<R, E>,
        PoolError: From<E>,
    {
        self.before_call()?;
        match f() {
            Ok(r) => {
                self.record_success();
                Ok(r)
            }
            Err(e) => {
                self.record_failure();
                Err(PoolError::from(e))
            }
        }
    }

    /// Forces the breaker back to `Closed`.
    pub fn reset(&self) {
        let mut g = self.inner.lock().unwrap();
        let transition = self.set_state(&mut g, BreakerState::Closed);
        drop(g);
        self.emit(transition);
    }

    // -- internals (call with the lock held) --------------------------------

    fn refresh(&self, g: &mut Inner) -> Option<(BreakerState, BreakerState)> {
        if g.state == BreakerState::Open
            && self.clock.monotonic() - g.opened_at >= self.cfg.recovery_timeout
        {
            return self.set_state(g, BreakerState::HalfOpen);
        }
        None
    }

    fn set_state(&self, g: &mut Inner, new: BreakerState) -> Option<(BreakerState, BreakerState)> {
        let old = g.state;
        if old == new {
            return None;
        }
        g.state = new;
        g.failures = 0;
        g.probes_inflight = 0;
        g.probe_successes = 0;
        if new == BreakerState::Open {
            g.opened_at = self.clock.monotonic();
        }
        Some((old, new))
    }

    fn emit(&self, transition: Option<(BreakerState, BreakerState)>) {
        // Fired after the lock is released so callbacks may safely re-enter the breaker.
        if let (Some((old, new)), Some(cb)) = (transition, &self.on_change) {
            cb(old, new);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn breaker(cfg: BreakerConfig, clock: Arc<ManualClock>) -> CircuitBreaker {
        CircuitBreaker::new(cfg, Some(clock), None)
    }

    use crate::core::clock::ManualClock;

    #[test]
    fn wire_values_and_config() {
        assert_eq!(BreakerState::Closed.as_str(), "closed");
        assert_eq!(BreakerState::Open.as_str(), "open");
        assert_eq!(BreakerState::HalfOpen.as_str(), "half_open");
        assert_eq!(BreakerState::Open.to_string(), "open");
        assert_eq!(format!("{:?}", BreakerState::Open), "Open");
        let c = BreakerConfig::default();
        assert_eq!(c.failure_threshold, 5);
        assert_eq!(c.recovery_timeout, 30.0);
        assert_eq!(c.half_open_max_calls, 1);
        assert_eq!(c.success_threshold, 1);
        assert!(c.validate().is_ok());
        assert!(
            BreakerConfig {
                failure_threshold: 0,
                ..c
            }
            .validate()
            .is_err()
        );
        assert!(
            BreakerConfig {
                recovery_timeout: -1.0,
                ..c
            }
            .validate()
            .is_err()
        );
        assert_eq!(c, c.clone());
    }

    #[test]
    fn trips_after_threshold_and_recovers() {
        let clock = Arc::new(ManualClock::new(1000.0));
        let cfg = BreakerConfig {
            failure_threshold: 2,
            recovery_timeout: 10.0,
            half_open_max_calls: 1,
            success_threshold: 1,
        };
        let b = breaker(cfg, clock.clone());
        assert_eq!(b.state(), BreakerState::Closed);
        b.before_call().unwrap();
        b.record_failure();
        assert_eq!(b.state(), BreakerState::Closed); // one failure, still closed
        b.before_call().unwrap();
        b.record_failure(); // second -> trips
        assert_eq!(b.state(), BreakerState::Open);
        // Open rejects with retry_after.
        let err = b.before_call().unwrap_err();
        assert!(matches!(err, PoolError::CircuitOpen { retry_after } if retry_after > 0.0));
        // After recovery -> half-open, probe allowed, success closes.
        clock.advance(10.0);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        b.before_call().unwrap();
        b.record_success();
        assert_eq!(b.state(), BreakerState::Closed);
    }

    #[test]
    fn half_open_limits_probes_and_failure_reopens() {
        let clock = Arc::new(ManualClock::new(0.0));
        let changes = Arc::new(AtomicU32::new(0));
        let changes2 = changes.clone();
        let cfg = BreakerConfig {
            failure_threshold: 1,
            recovery_timeout: 5.0,
            half_open_max_calls: 1,
            success_threshold: 2,
        };
        let b = CircuitBreaker::new(
            cfg,
            Some(clock.clone()),
            Some(Box::new(move |_, _| {
                changes2.fetch_add(1, Ordering::SeqCst);
            })),
        );
        b.before_call().unwrap();
        b.record_failure(); // trip -> open (1 change)
        clock.advance(5.0);
        b.before_call().unwrap(); // half-open (2 changes), probe inflight
        // second concurrent probe rejected with retry_after 0
        assert!(matches!(
            b.before_call().unwrap_err(),
            PoolError::CircuitOpen { retry_after } if retry_after == 0.0
        ));
        b.record_failure(); // probe fails -> back to open (3 changes)
        assert_eq!(b.state(), BreakerState::Open);
        // need 2 successes to close
        clock.advance(5.0);
        b.before_call().unwrap();
        b.record_success(); // 1/2, stays half-open
        assert_eq!(b.state(), BreakerState::HalfOpen);
        b.before_call().unwrap();
        b.record_success(); // 2/2 -> closed
        assert_eq!(b.state(), BreakerState::Closed);
        assert!(changes.load(Ordering::SeqCst) >= 3);
    }

    #[test]
    fn abort_releases_probe_without_counting() {
        let clock = Arc::new(ManualClock::new(0.0));
        let cfg = BreakerConfig {
            failure_threshold: 1,
            recovery_timeout: 1.0,
            half_open_max_calls: 1,
            success_threshold: 1,
        };
        let b = breaker(cfg, clock.clone());
        b.before_call().unwrap();
        b.record_failure();
        clock.advance(1.0);
        b.before_call().unwrap(); // half-open, probe inflight
        b.record_abort(); // release slot, no count
        // a new probe is allowed again
        b.before_call().unwrap();
        b.record_success();
        assert_eq!(b.state(), BreakerState::Closed);
        // abort in non-half-open is a no-op
        b.record_abort();
    }

    #[test]
    fn reset_and_late_events_and_call_wrapper() {
        let clock = Arc::new(ManualClock::new(0.0));
        let b = breaker(BreakerConfig::default(), clock.clone());
        // success in closed resets failures
        b.record_success();
        // late failure/success in open are ignored
        let cfg = BreakerConfig {
            failure_threshold: 1,
            ..BreakerConfig::default()
        };
        let b2 = breaker(cfg, clock.clone());
        b2.before_call().unwrap();
        b2.record_failure(); // open
        b2.record_failure(); // ignored (still open)
        b2.record_success(); // ignored
        assert_eq!(b2.state(), BreakerState::Open);
        b2.reset();
        assert_eq!(b2.state(), BreakerState::Closed);
        b2.reset(); // no-op (same state)

        // call() wrapper success + failure
        let ok: Result<i32, PoolError> = b.call(|| Ok::<_, PoolError>(5));
        assert_eq!(ok.unwrap(), 5);
        let err: Result<i32, PoolError> =
            b.call(|| Err::<i32, _>(PoolError::Timeout("t".into())));
        assert!(err.is_err());
    }

    #[test]
    fn defaults_to_system_clock_when_none() {
        let b = CircuitBreaker::new(BreakerConfig::default(), None, None);
        assert_eq!(b.state(), BreakerState::Closed);
        b.before_call().unwrap();
        b.record_success();
    }

    #[test]
    fn validate_each_threshold_branch() {
        for bad in [
            BreakerConfig { failure_threshold: 0, ..Default::default() },
            BreakerConfig { half_open_max_calls: 0, ..Default::default() },
            BreakerConfig { success_threshold: 0, ..Default::default() },
        ] {
            assert!(bad.validate().is_err());
        }
    }

    #[test]
    fn call_short_circuits_when_open() {
        let clock = Arc::new(ManualClock::new(0.0));
        let cfg = BreakerConfig {
            failure_threshold: 1,
            recovery_timeout: 100.0,
            ..BreakerConfig::default()
        };
        let b = breaker(cfg, clock);
        b.before_call().unwrap();
        b.record_failure();
        let r: Result<i32, PoolError> = b.call(|| Ok::<_, PoolError>(1));
        assert!(matches!(r, Err(PoolError::CircuitOpen { .. })));
    }
}
