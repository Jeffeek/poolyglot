//! Retry policy: exponential backoff with jitter for `factory.create()`.

use std::error::Error;
use std::sync::Arc;

use crate::core::errors::PoolError;
use crate::rng::Rng;

/// A predicate deciding whether a given factory error is retryable.
type RetryOn = Arc<dyn Fn(&(dyn Error + 'static)) -> bool + Send + Sync>;

/// How to retry `factory.create()`: exponential backoff with jitter.
///
/// `jitter` in `[0, 1]`: `0` = deterministic delays, `1` = "full jitter" (uniform in
/// `[0, delay]`). `retry_on` narrows what is retryable (e.g. don't retry auth failures);
/// `None` means retry everything.
#[derive(Clone)]
pub struct RetryPolicy {
    /// Maximum attempts (including the first). `1` = no retries.
    pub max_attempts: u32,
    /// Base delay in seconds for the first retry.
    pub base_delay: f64,
    /// Upper bound on any single delay, in seconds.
    pub max_delay: f64,
    /// Exponential growth factor between attempts.
    pub multiplier: f64,
    /// Jitter fraction in `[0, 1]`.
    pub jitter: f64,
    /// Optional predicate: retry only when it returns `true`. `None` = retry everything.
    pub retry_on: Option<RetryOn>,
}

impl std::fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryPolicy")
            .field("max_attempts", &self.max_attempts)
            .field("base_delay", &self.base_delay)
            .field("max_delay", &self.max_delay)
            .field("multiplier", &self.multiplier)
            .field("jitter", &self.jitter)
            .field("retry_on", &self.retry_on.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: 0.1,
            max_delay: 5.0,
            multiplier: 2.0,
            jitter: 0.5,
            retry_on: None,
        }
    }
}

impl RetryPolicy {
    /// A policy that never retries (one attempt only).
    pub fn no_retry() -> Self {
        Self {
            max_attempts: 1,
            ..Self::default()
        }
    }

    /// Returns a copy with `retry_on` set to `predicate`.
    pub fn with_retry_on<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&(dyn Error + 'static)) -> bool + Send + Sync + 'static,
    {
        self.retry_on = Some(Arc::new(predicate));
        self
    }

    /// Checks the policy's invariants.
    pub fn validate(&self) -> Result<(), PoolError> {
        if self.max_attempts < 1 {
            return Err(PoolError::Config("retry.max_attempts must be >= 1".into()));
        }
        if self.base_delay < 0.0 || self.max_delay < 0.0 || self.multiplier < 1.0 {
            return Err(PoolError::Config(
                "retry delays must be >= 0 and multiplier >= 1".into(),
            ));
        }
        if self.jitter < 0.0 || self.jitter > 1.0 {
            return Err(PoolError::Config(
                "retry.jitter must be within [0, 1]".into(),
            ));
        }
        Ok(())
    }

    /// Reports whether `err` is retryable under this policy.
    pub fn should_retry(&self, err: &(dyn Error + 'static)) -> bool {
        match &self.retry_on {
            None => true,
            Some(p) => p(err),
        }
    }

    /// The delay before the next attempt, given how many attempts have failed so far
    /// (`failed_attempts` is 1-based: pass `1` after the first failure).
    pub fn delay_for(&self, failed_attempts: u32, rng: &mut dyn Rng) -> f64 {
        // base_delay == 0 means no backoff configured; guard separately so a large
        // `multiplier.powi(..)` overflow to +Inf can't turn `0.0 * Inf` into NaN.
        let ceiling = if self.base_delay == 0.0 {
            0.0
        } else {
            self.max_delay
                .min(self.base_delay * self.multiplier.powi(failed_attempts as i32 - 1))
        };
        ceiling * (1.0 - self.jitter + self.jitter * rng.next_f64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::SystemRng;

    #[test]
    fn defaults_and_no_retry() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts, 3);
        assert_eq!(p.base_delay, 0.1);
        assert_eq!(p.max_delay, 5.0);
        assert_eq!(p.multiplier, 2.0);
        assert_eq!(p.jitter, 0.5);
        assert!(p.retry_on.is_none());
        assert_eq!(RetryPolicy::no_retry().max_attempts, 1);
        assert!(format!("{p:?}").contains("RetryPolicy"));
        assert!(format!("{:?}", p.clone().with_retry_on(|_| true)).contains("<fn>"));
    }

    #[test]
    fn validate_rules() {
        assert!(RetryPolicy::default().validate().is_ok());
        let bad = RetryPolicy {
            max_attempts: 0,
            ..Default::default()
        };
        assert!(matches!(bad.validate(), Err(PoolError::Config(_))));
        let bad = RetryPolicy {
            base_delay: -1.0,
            ..Default::default()
        };
        assert!(bad.validate().is_err());
        let bad = RetryPolicy {
            multiplier: 0.5,
            ..Default::default()
        };
        assert!(bad.validate().is_err());
        let bad = RetryPolicy {
            jitter: 1.5,
            ..Default::default()
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn should_retry_predicate() {
        let p = RetryPolicy::default();
        let err: Box<dyn Error + Send + Sync> = "x".into();
        assert!(p.should_retry(&*err));
        let p = RetryPolicy::default().with_retry_on(|e| e.to_string() == "retry");
        let yes: Box<dyn Error + Send + Sync> = "retry".into();
        let no: Box<dyn Error + Send + Sync> = "stop".into();
        assert!(p.should_retry(&*yes));
        assert!(!p.should_retry(&*no));
    }

    #[test]
    fn delay_backoff_and_jitter_bounds() {
        let mut rng = SystemRng::with_seed(7);
        let p = RetryPolicy::default();
        // failed_attempts=1 -> ceiling 0.1; jitter 0.5 -> delay in [0.05, 0.1].
        for _ in 0..1000 {
            let d = p.delay_for(1, &mut rng);
            assert!((0.05..=0.1).contains(&d), "delay {d}");
        }
        // failed_attempts=2 -> ceiling 0.2.
        let d = p.delay_for(2, &mut rng);
        assert!((0.1..=0.2).contains(&d));
        // Ceiling is capped at max_delay.
        let capped = RetryPolicy {
            base_delay: 10.0,
            max_delay: 1.0,
            jitter: 0.0,
            ..Default::default()
        };
        assert_eq!(capped.delay_for(5, &mut rng), 1.0);
        // jitter=0 -> deterministic == ceiling.
        let det = RetryPolicy {
            jitter: 0.0,
            ..Default::default()
        };
        assert_eq!(det.delay_for(1, &mut rng), 0.1);
    }

    #[test]
    fn delay_zero_base_never_nan_even_on_power_overflow() {
        let mut rng = SystemRng::with_seed(7);
        // base_delay == 0.0 && multiplier > 1 && failed_attempts large enough that
        // multiplier.powi(..) overflows to +Inf: 0.0 * Inf used to yield NaN.
        let p = RetryPolicy {
            base_delay: 0.0,
            multiplier: 2.0,
            ..Default::default()
        };
        let d = p.delay_for(10_000, &mut rng);
        assert!(d.is_finite(), "delay {d}");
        assert_eq!(d, 0.0);
    }
}
