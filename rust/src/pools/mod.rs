//! Pools layer: `ConnectionPool`, `PoolRegistry`, and (feature `tokio`)
//! `AsyncConnectionPool` / `AsyncPoolRegistry`. Depends on every layer above.

pub mod registry;
pub mod sync_pool;

#[cfg(feature = "tokio")]
pub mod async_pool;
#[cfg(feature = "tokio")]
pub mod async_registry;
