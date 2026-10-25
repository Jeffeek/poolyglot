//! Factories layer: the seam users implement to pool anything. Depends on `core`.

pub mod sync_factory;

#[cfg(feature = "tokio")]
pub mod async_factory;
