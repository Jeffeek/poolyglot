//! Core layer: errors, clocks, connection bookkeeping, timeout math. Depends on nothing else
//! in the crate.

pub mod clock;
pub mod connection;
pub mod errors;
pub mod timeouts;
