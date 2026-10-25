//! `ConnectionFactory`: the ONE seam between the pool and the outside world (blocking).

use std::error::Error;

use crate::core::errors::FactoryError;

/// Implement this to pool anything (DB driver, HTTP client, SSH session, socket…) — or build
/// one from closures via [`FnFactory`].
///
/// `create` must return a **distinct** object each call; the pool tracks borrow state by value
/// identity and rejects a factory that returns an object it already manages.
pub trait ConnectionFactory<T>: Send + Sync {
    /// Opens a new connection. Should enforce its own connect timeout.
    fn create(&self) -> Result<T, FactoryError>;

    /// A cheap liveness check (e.g. `SELECT 1`). `false` = dead. Defaults to `true`.
    fn validate(&self, _conn: &T) -> bool {
        true
    }

    /// Called on release to scrub session state (e.g. rollback). Defaults to a no-op.
    fn reset(&self, _conn: &T) -> Result<(), FactoryError> {
        Ok(())
    }

    /// Disposes of a connection. Defaults to a no-op — most Rust connections close on `Drop`;
    /// override only when explicit teardown is needed.
    fn close(&self, _conn: &T) -> Result<(), FactoryError> {
        Ok(())
    }

    /// Whether `err` (raised by user code using the connection) proves the connection is
    /// broken, so it is discarded without further checks. Defaults to `false`.
    fn is_fatal_error(&self, _err: &(dyn Error + 'static)) -> bool {
        false
    }
}

/// Lets a boxed factory be used anywhere a `ConnectionFactory<T>` is expected (e.g. a
/// [`PoolRegistry`](crate::PoolRegistry) whose per-key factories have different concrete types).
impl<T> ConnectionFactory<T> for Box<dyn ConnectionFactory<T>> {
    fn create(&self) -> Result<T, FactoryError> {
        (**self).create()
    }
    fn validate(&self, conn: &T) -> bool {
        (**self).validate(conn)
    }
    fn reset(&self, conn: &T) -> Result<(), FactoryError> {
        (**self).reset(conn)
    }
    fn close(&self, conn: &T) -> Result<(), FactoryError> {
        (**self).close(conn)
    }
    fn is_fatal_error(&self, err: &(dyn Error + 'static)) -> bool {
        (**self).is_fatal_error(err)
    }
}

type CreateFn<T> = Box<dyn Fn() -> Result<T, FactoryError> + Send + Sync>;
type ValidateFn<T> = Box<dyn Fn(&T) -> bool + Send + Sync>;
type ResetFn<T> = Box<dyn Fn(&T) -> Result<(), FactoryError> + Send + Sync>;
type CloseFn<T> = Box<dyn Fn(&T) -> Result<(), FactoryError> + Send + Sync>;
type IsFatalFn = Box<dyn Fn(&(dyn Error + 'static)) -> bool + Send + Sync>;

/// A [`ConnectionFactory`] built from plain closures. Unset hooks fall back to the trait
/// defaults.
pub struct FnFactory<T> {
    create_fn: CreateFn<T>,
    validate_fn: Option<ValidateFn<T>>,
    reset_fn: Option<ResetFn<T>>,
    close_fn: Option<CloseFn<T>>,
    is_fatal_fn: Option<IsFatalFn>,
}

impl<T> FnFactory<T> {
    /// Builds a factory from a `create` closure.
    pub fn new<F>(create: F) -> Self
    where
        F: Fn() -> Result<T, FactoryError> + Send + Sync + 'static,
    {
        Self {
            create_fn: Box::new(create),
            validate_fn: None,
            reset_fn: None,
            close_fn: None,
            is_fatal_fn: None,
        }
    }

    /// Sets the liveness check.
    pub fn with_validate<F>(mut self, f: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        self.validate_fn = Some(Box::new(f));
        self
    }

    /// Sets the release-time reset.
    pub fn with_reset<F>(mut self, f: F) -> Self
    where
        F: Fn(&T) -> Result<(), FactoryError> + Send + Sync + 'static,
    {
        self.reset_fn = Some(Box::new(f));
        self
    }

    /// Sets the disposal hook.
    pub fn with_close<F>(mut self, f: F) -> Self
    where
        F: Fn(&T) -> Result<(), FactoryError> + Send + Sync + 'static,
    {
        self.close_fn = Some(Box::new(f));
        self
    }

    /// Sets the fatal-error classifier.
    pub fn with_is_fatal_error<F>(mut self, f: F) -> Self
    where
        F: Fn(&(dyn Error + 'static)) -> bool + Send + Sync + 'static,
    {
        self.is_fatal_fn = Some(Box::new(f));
        self
    }
}

impl<T> ConnectionFactory<T> for FnFactory<T>
where
    T: Send + Sync,
{
    fn create(&self) -> Result<T, FactoryError> {
        (self.create_fn)()
    }

    fn validate(&self, conn: &T) -> bool {
        match &self.validate_fn {
            Some(f) => f(conn),
            None => true,
        }
    }

    fn reset(&self, conn: &T) -> Result<(), FactoryError> {
        match &self.reset_fn {
            Some(f) => f(conn),
            None => Ok(()),
        }
    }

    fn close(&self, conn: &T) -> Result<(), FactoryError> {
        match &self.close_fn {
            Some(f) => f(conn),
            None => Ok(()),
        }
    }

    fn is_fatal_error(&self, err: &(dyn Error + 'static)) -> bool {
        match &self.is_fatal_fn {
            Some(f) => f(err),
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_apply_when_unset() {
        let f = FnFactory::new(|| Ok::<_, FactoryError>(7u32));
        assert_eq!(f.create().unwrap(), 7);
        assert!(f.validate(&7));
        assert!(f.reset(&7).is_ok());
        assert!(f.close(&7).is_ok());
        let err: FactoryError = "x".into();
        assert!(!f.is_fatal_error(&*err));
    }

    #[test]
    fn custom_hooks_are_used() {
        let f = FnFactory::new(|| Ok::<_, FactoryError>(1u32))
            .with_validate(|c| *c == 1)
            .with_reset(|_| Err::<(), FactoryError>("reset boom".into()))
            .with_close(|_| Err::<(), FactoryError>("close boom".into()))
            .with_is_fatal_error(|e| e.to_string() == "fatal");
        assert!(f.validate(&1));
        assert!(!f.validate(&2));
        assert_eq!(f.reset(&1).unwrap_err().to_string(), "reset boom");
        assert_eq!(f.close(&1).unwrap_err().to_string(), "close boom");
        let fatal: FactoryError = "fatal".into();
        let ok: FactoryError = "meh".into();
        assert!(f.is_fatal_error(&*fatal));
        assert!(!f.is_fatal_error(&*ok));
    }

    #[test]
    fn create_can_fail() {
        let f = FnFactory::new(|| Err::<u32, FactoryError>("down".into()));
        assert_eq!(f.create().unwrap_err().to_string(), "down");
    }

    #[test]
    fn trait_defaults_apply_to_a_minimal_impl() {
        struct Minimal;
        impl ConnectionFactory<u32> for Minimal {
            fn create(&self) -> Result<u32, FactoryError> {
                Ok(1)
            }
        }
        let m = Minimal;
        assert_eq!(m.create().unwrap(), 1);
        assert!(m.validate(&1));
        assert!(m.reset(&1).is_ok());
        assert!(m.close(&1).is_ok());
        let e: FactoryError = "x".into();
        assert!(!m.is_fatal_error(&*e));
    }

    #[test]
    fn boxed_factory_delegates() {
        let boxed: Box<dyn ConnectionFactory<u32>> = Box::new(
            FnFactory::new(|| Ok::<_, FactoryError>(5u32))
                .with_validate(|c| *c == 5)
                .with_reset(|_| Ok(()))
                .with_close(|_| Ok(()))
                .with_is_fatal_error(|_| true),
        );
        assert_eq!(boxed.create().unwrap(), 5);
        assert!(boxed.validate(&5));
        assert!(boxed.reset(&5).is_ok());
        assert!(boxed.close(&5).is_ok());
        let e: FactoryError = "x".into();
        assert!(boxed.is_fatal_error(&*e));
    }
}
