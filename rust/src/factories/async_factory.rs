//! `AsyncConnectionFactory`: the async seam users implement to pool anything (feature `tokio`).
//!
//! Unlike the sibling languages there is no cancellation token: Rust async cancellation is
//! "drop the future". The pool enforces `create_timeout` by wrapping `create` in a timer, and
//! shields disposal by running `close` to completion on a detached task.

use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use crate::core::errors::FactoryError;

/// A boxed, `Send` future — the stored-closure form used by [`AsyncFnFactory`].
pub type BoxFuture<'a, O> = Pin<Box<dyn Future<Output = O> + Send + 'a>>;

/// Implement this to pool anything asynchronously. `create` must return a distinct object each
/// call (tracked by value identity).
pub trait AsyncConnectionFactory<T>: Send + Sync {
    /// Opens a new connection.
    fn create(&self) -> impl Future<Output = Result<T, FactoryError>> + Send;

    /// A cheap liveness check. Defaults to `true`.
    fn validate(&self, _conn: &T) -> impl Future<Output = bool> + Send {
        async { true }
    }

    /// Scrubs session state on release. Defaults to a no-op.
    fn reset(&self, _conn: &T) -> impl Future<Output = Result<(), FactoryError>> + Send {
        async { Ok(()) }
    }

    /// Disposes of a connection. Defaults to a no-op.
    fn close(&self, _conn: &T) -> impl Future<Output = Result<(), FactoryError>> + Send {
        async { Ok(()) }
    }

    /// Whether `err` proves the connection is broken. Defaults to `false`.
    fn is_fatal_error(&self, _err: &(dyn Error + 'static)) -> bool {
        false
    }
}

type CreateFn<T> = Box<dyn Fn() -> BoxFuture<'static, Result<T, FactoryError>> + Send + Sync>;
type ValidateFn<T> = Box<dyn for<'a> Fn(&'a T) -> BoxFuture<'a, bool> + Send + Sync>;
type ResetFn<T> = Box<dyn for<'a> Fn(&'a T) -> BoxFuture<'a, Result<(), FactoryError>> + Send + Sync>;
type CloseFn<T> = Box<dyn for<'a> Fn(&'a T) -> BoxFuture<'a, Result<(), FactoryError>> + Send + Sync>;
type IsFatalFn = Box<dyn Fn(&(dyn Error + 'static)) -> bool + Send + Sync>;

/// An [`AsyncConnectionFactory`] built from closures returning [`BoxFuture`]s.
pub struct AsyncFnFactory<T> {
    create_fn: CreateFn<T>,
    validate_fn: Option<ValidateFn<T>>,
    reset_fn: Option<ResetFn<T>>,
    close_fn: Option<CloseFn<T>>,
    is_fatal_fn: Option<IsFatalFn>,
}

impl<T> AsyncFnFactory<T> {
    /// Builds a factory from a `create` closure returning a boxed future.
    pub fn new<F>(create: F) -> Self
    where
        F: Fn() -> BoxFuture<'static, Result<T, FactoryError>> + Send + Sync + 'static,
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
        F: for<'a> Fn(&'a T) -> BoxFuture<'a, bool> + Send + Sync + 'static,
    {
        self.validate_fn = Some(Box::new(f));
        self
    }

    /// Sets the release-time reset.
    pub fn with_reset<F>(mut self, f: F) -> Self
    where
        F: for<'a> Fn(&'a T) -> BoxFuture<'a, Result<(), FactoryError>> + Send + Sync + 'static,
    {
        self.reset_fn = Some(Box::new(f));
        self
    }

    /// Sets the disposal hook.
    pub fn with_close<F>(mut self, f: F) -> Self
    where
        F: for<'a> Fn(&'a T) -> BoxFuture<'a, Result<(), FactoryError>> + Send + Sync + 'static,
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

impl<T> AsyncConnectionFactory<T> for AsyncFnFactory<T>
where
    T: Send + Sync,
{
    fn create(&self) -> impl Future<Output = Result<T, FactoryError>> + Send {
        (self.create_fn)()
    }

    async fn validate(&self, conn: &T) -> bool {
        match &self.validate_fn {
            Some(f) => f(conn).await,
            None => true,
        }
    }

    async fn reset(&self, conn: &T) -> Result<(), FactoryError> {
        match &self.reset_fn {
            Some(f) => f(conn).await,
            None => Ok(()),
        }
    }

    async fn close(&self, conn: &T) -> Result<(), FactoryError> {
        match &self.close_fn {
            Some(f) => f(conn).await,
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

    #[tokio::test]
    async fn defaults_apply_when_unset() {
        let f = AsyncFnFactory::<u32>::new(|| Box::pin(async { Ok::<_, FactoryError>(7u32) }));
        assert_eq!(f.create().await.unwrap(), 7u32);
        assert!(f.validate(&7).await);
        assert!(f.reset(&7).await.is_ok());
        assert!(f.close(&7).await.is_ok());
        let err: FactoryError = "x".into();
        assert!(!f.is_fatal_error(&*err));
    }

    #[tokio::test]
    async fn custom_hooks_are_used() {
        let f = AsyncFnFactory::<u32>::new(|| Box::pin(async { Ok::<_, FactoryError>(1u32) }))
            .with_validate(|c: &u32| {
                let v = *c == 1;
                Box::pin(async move { v }) as _
            })
            .with_reset(|_| Box::pin(async { Err::<(), FactoryError>("reset boom".into()) }) as _)
            .with_close(|_| Box::pin(async { Err::<(), FactoryError>("close boom".into()) }) as _)
            .with_is_fatal_error(|e| e.to_string() == "fatal");
        assert!(f.validate(&1).await);
        assert_eq!(f.reset(&1).await.unwrap_err().to_string(), "reset boom");
        assert_eq!(f.close(&1).await.unwrap_err().to_string(), "close boom");
        let fatal: FactoryError = "fatal".into();
        assert!(f.is_fatal_error(&*fatal));
    }

    #[tokio::test]
    async fn trait_defaults_apply_to_a_minimal_impl() {
        struct Minimal;
        impl AsyncConnectionFactory<u32> for Minimal {
            async fn create(&self) -> Result<u32, FactoryError> {
                Ok(1)
            }
        }
        let m = Minimal;
        assert_eq!(m.create().await.unwrap(), 1);
        assert!(m.validate(&1).await);
        assert!(m.reset(&1).await.is_ok());
        assert!(m.close(&1).await.is_ok());
        let e: FactoryError = "x".into();
        assert!(!m.is_fatal_error(&*e));
    }
}
