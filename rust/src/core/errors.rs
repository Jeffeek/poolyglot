//! The error taxonomy raised by the pool.
//!
//! The sibling implementations use an exception hierarchy (Python/C#/Java) or a marker
//! interface over several structs (Go). Rust's idiom is a single enum implementing
//! [`std::error::Error`]: callers match on the variant (the analog of `except PoolTimeoutError`
//! / `errors.As(&PoolTimeoutError{})`). The taxonomy — config, closed, timeout, exhausted,
//! connection-create, circuit-open, invalid-release — is preserved one-to-one.

use std::error::Error;
use std::fmt;

/// The boxed error type a [`ConnectionFactory`](crate::ConnectionFactory) returns from
/// `create`/`reset`/`close`. Any `std::error::Error` works.
pub type FactoryError = Box<dyn Error + Send + Sync + 'static>;

/// Every error the pool raises. Mirrors the `PoolError` base type of the other languages;
/// each variant corresponds to a sibling error class.
#[derive(Debug)]
pub enum PoolError {
    /// Invalid pool / breaker / retry configuration (`PoolConfigError`).
    Config(String),
    /// The pool has been closed; no more connections can be acquired (`PoolClosedError`).
    Closed(String),
    /// No connection became available before the acquire deadline (`PoolTimeoutError`).
    Timeout(String),
    /// The pool is full and the waiter queue is at `max_waiters` (`PoolExhaustedError`).
    Exhausted(String),
    /// The factory failed to create a connection after all retries
    /// (`ConnectionCreateError`). The original error is available via
    /// [`Error::source`]/[`PoolError::source_error`].
    ConnectionCreate {
        /// Human-readable message.
        message: String,
        /// The underlying factory error, if any.
        source: Option<FactoryError>,
    },
    /// The circuit breaker is rejecting calls to protect a failing backend
    /// (`CircuitOpenError`). `retry_after` is the approximate seconds until it probes again.
    CircuitOpen {
        /// Approximate seconds until the breaker probes again.
        retry_after: f64,
    },
    /// An object that is not checked out from this pool was released — a double release?
    /// (`InvalidReleaseError`).
    InvalidRelease(String),
}

impl PoolError {
    /// Returns the underlying factory error for a [`PoolError::ConnectionCreate`], if present.
    pub fn source_error(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            PoolError::ConnectionCreate {
                source: Some(e), ..
            } => Some(&**e),
            _ => None,
        }
    }
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolError::Config(m)
            | PoolError::Closed(m)
            | PoolError::Timeout(m)
            | PoolError::Exhausted(m)
            | PoolError::InvalidRelease(m) => f.write_str(m),
            PoolError::ConnectionCreate { message, .. } => f.write_str(message),
            PoolError::CircuitOpen { retry_after } => {
                write!(f, "circuit breaker is open; retry in ~{retry_after:.1}s")
            }
        }
    }
}

impl Error for PoolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_match_wire_format() {
        assert_eq!(PoolError::Config("bad".into()).to_string(), "bad");
        assert_eq!(PoolError::Closed("x".into()).to_string(), "x");
        assert_eq!(PoolError::Timeout("t".into()).to_string(), "t");
        assert_eq!(PoolError::Exhausted("e".into()).to_string(), "e");
        assert_eq!(PoolError::InvalidRelease("i".into()).to_string(), "i");
        assert_eq!(
            PoolError::CircuitOpen { retry_after: 2.34 }.to_string(),
            "circuit breaker is open; retry in ~2.3s"
        );
        let cc = PoolError::ConnectionCreate {
            message: "nope".into(),
            source: None,
        };
        assert_eq!(cc.to_string(), "nope");
    }

    #[test]
    fn connection_create_exposes_source() {
        let inner: FactoryError = "boom".into();
        let err = PoolError::ConnectionCreate {
            message: "could not create".into(),
            source: Some(inner),
        };
        assert_eq!(err.source().unwrap().to_string(), "boom");
        assert_eq!(err.source_error().unwrap().to_string(), "boom");
        // Non-create errors have no source.
        assert!(PoolError::Timeout("t".into()).source().is_none());
        // Debug is derived; touch it.
        assert!(format!("{err:?}").contains("ConnectionCreate"));
    }
}
