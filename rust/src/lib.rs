//! `connpool` — a generic connection pool for Rust, dependency-free (blocking) with an
//! optional async pool behind the `tokio` feature.
//!
//! It pools *anything* a small user-supplied [`ConnectionFactory`] can `create`, `validate`,
//! `reset` and `close` — DB drivers, HTTP clients, SSH sessions, raw sockets. The pool knows
//! nothing about your connection except what the factory tells it.
//!
//! # Layers (each only depends on the ones above it)
//!
//! ```text
//! core           errors, clocks, connection bookkeeping, timeout math
//! resilience     circuit breaker, retry/backoff
//! policy         PoolConfig + the pure retirement rules
//! factories      ConnectionFactory: the seam users implement to pool anything
//! observability  listeners + stats
//! pools          ConnectionPool, AsyncConnectionPool, PoolRegistry
//! ```
//!
//! `connpool` is the Rust member of [poolyglot](https://github.com/Jeffeek/poolyglot): one
//! pooling model implemented per language, kept API-parallel. Field names, event names,
//! defaults, and [`CloseReason`]/[`BreakerState`] wire values are identical to the Python,
//! C#, Java, Go and TypeScript implementations — switching languages is a syntax change, not
//! a model change.
//!
//! # Example
//!
//! ```
//! use connpool::{ConnectionPool, FnFactory, PoolConfig, Timeout};
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicU64, Ordering};
//!
//! // A toy "connection": a unique id behind an Arc so clones share identity.
//! let next = Arc::new(AtomicU64::new(0));
//! let factory = FnFactory::new({
//!     let next = next.clone();
//!     move || Ok::<_, connpool::FactoryError>(Arc::new(next.fetch_add(1, Ordering::SeqCst)))
//! });
//!
//! let pool = ConnectionPool::new(factory, PoolConfig { max_size: 4, ..Default::default() }, None)
//!     .expect("valid config");
//! let conn = pool.acquire(Timeout::Unset).expect("acquire");
//! // ... use conn ...
//! pool.release(conn).expect("release");
//! ```
//!
//! Connections are tracked by value identity, so `T` must be `Clone + Eq + Hash` — use a
//! cheap handle type such as `Arc<Conn>` or an id (the direct analog of the Go port's
//! `comparable` constraint).

// Layer modules (downward-only dependency direction).
mod core;
mod factories;
mod observability;
mod policy;
mod pools;
mod resilience;
mod rng;

#[cfg(test)]
mod test_support;
#[cfg(all(test, feature = "tokio"))]
mod async_test_support;

// ---- public-API facade: one import surface re-exporting the whole library ----

pub use crate::core::clock::{Clock, ManualClock, SystemClock};
pub use crate::core::connection::{CloseReason, PooledConnection};
pub use crate::core::errors::{FactoryError, PoolError};
pub use crate::core::timeouts::Timeout;

pub use crate::resilience::breaker::{BreakerConfig, BreakerState, CircuitBreaker};
pub use crate::resilience::retry::RetryPolicy;

pub use crate::policy::config::{IdleStrategy, PoolConfig};

pub use crate::factories::sync_factory::{ConnectionFactory, FnFactory};

pub use crate::observability::events::PoolListener;
pub use crate::observability::stats::{PoolStats, StatValue};

pub use crate::pools::registry::PoolRegistry;
pub use crate::pools::sync_pool::{ConnectionPool, PoolOptions};

pub use crate::rng::{Rng, SystemRng};

#[cfg(feature = "tokio")]
pub use crate::factories::async_factory::{AsyncConnectionFactory, AsyncFnFactory};
#[cfg(feature = "tokio")]
pub use crate::pools::async_pool::AsyncConnectionPool;
#[cfg(feature = "tokio")]
pub use crate::pools::async_registry::AsyncPoolRegistry;

/// The released version of this crate, kept in lock-step with the root `VERSION` file.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
