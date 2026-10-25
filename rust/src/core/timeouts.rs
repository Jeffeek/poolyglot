//! The per-call acquire timeout argument and the deadline math.

/// The per-call acquire timeout. One of three things:
///
/// - [`Timeout::Unset`] — use the pool's configured `acquire_timeout` default.
/// - [`Timeout::Infinite`] — wait forever.
/// - [`Timeout::Of`] — wait that many seconds (`0.0` = don't wait).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Timeout {
    /// Use the configured default (`config.acquire_timeout`).
    Unset,
    /// Wait forever.
    Infinite,
    /// Wait this many seconds (`0.0` = don't wait).
    Of(f64),
}

impl Timeout {
    /// Maps `None` -> [`Timeout::Infinite`] and `Some(s)` -> [`Timeout::Of`].
    pub fn from_option(seconds: Option<f64>) -> Self {
        match seconds {
            None => Timeout::Infinite,
            Some(s) => Timeout::Of(s),
        }
    }

    /// Resolves this timeout against a default (`None` = forever) at time `now`, returning the
    /// absolute deadline (`None` = no deadline / wait forever).
    pub(crate) fn compute_deadline(self, default: Option<f64>, now: f64) -> Option<f64> {
        match self {
            Timeout::Unset => default.map(|d| now + d),
            Timeout::Infinite => None,
            Timeout::Of(s) => Some(now + s),
        }
    }
}

/// Seconds left until `deadline` (`None` = no deadline).
pub(crate) fn remaining(deadline: Option<f64>, now: f64) -> Option<f64> {
    deadline.map(|d| d - now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_option_maps_none_to_infinite() {
        assert_eq!(Timeout::from_option(None), Timeout::Infinite);
        assert_eq!(Timeout::from_option(Some(3.0)), Timeout::Of(3.0));
    }

    #[test]
    fn compute_deadline_covers_all_kinds() {
        // Unset with a default -> now + default; without -> None.
        assert_eq!(Timeout::Unset.compute_deadline(Some(30.0), 100.0), Some(130.0));
        assert_eq!(Timeout::Unset.compute_deadline(None, 100.0), None);
        // Infinite -> None regardless.
        assert_eq!(Timeout::Infinite.compute_deadline(Some(30.0), 100.0), None);
        // Of -> now + seconds (0 = immediate).
        assert_eq!(Timeout::Of(5.0).compute_deadline(None, 100.0), Some(105.0));
        assert_eq!(Timeout::Of(0.0).compute_deadline(Some(30.0), 100.0), Some(100.0));
    }

    #[test]
    fn remaining_math() {
        assert_eq!(remaining(None, 100.0), None);
        assert_eq!(remaining(Some(130.0), 100.0), Some(30.0));
        assert_eq!(remaining(Some(100.0), 130.0), Some(-30.0));
        // Debug/Clone/PartialEq are derived; touch them.
        let t = Timeout::Of(1.0);
        assert_eq!(t, t.clone());
        assert!(format!("{t:?}").contains("Of"));
    }
}
