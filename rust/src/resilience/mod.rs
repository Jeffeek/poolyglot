//! Resilience layer: circuit breaker and retry/backoff. Depends only on `core`.

pub mod breaker;
pub mod retry;
