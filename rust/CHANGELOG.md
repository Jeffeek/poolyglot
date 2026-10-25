# Changelog

All notable changes to the Rust implementation are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Versions are shared across every
poolyglot language (one root `VERSION`, one behavior spec).

## [Unreleased]

### Added

- **Initial Rust implementation** (crate `connpool-rs`, imported as `connpool`; edition 2024, MSRV 1.85)
  at feature parity
  with the Python `connpool` package, the C# `ConnPool` package, the Java `connpool` package, the
  Go `connpool` module, the TypeScript `@jeffeek/connpool` package, and the shared behavior spec:
  - `ConnectionPool<T>` (thread-safe, `Mutex` + `Condvar`, **zero runtime dependencies**) and
    `AsyncConnectionPool<T, F>` (tokio-based, cancellation = future-drop, behind the optional
    `tokio` feature) as parallel implementations of the same model. `T` is tracked by value
    identity (`Clone + Eq + Hash` — use a cheap handle such as `Arc<Conn>`; the Rust analog of
    Go's `comparable`).
  - Sizing (`min_size`/`max_size` hard cap with slot reservation), rotation (`max_lifetime` with
    jitter, `max_uses`, `max_idle_time`, LIFO/FIFO), health (validate-on-checkout, optional
    background `validate_idle`), resilience (`RetryPolicy` + `CircuitBreaker`, one failure per
    logical creation), backpressure (`max_waiters`), observability (`PoolListener` + `PoolStats`
    with avg/p50/p95/p99 for acquire-wait and create-latency), graceful `close(timeout)`, warn-only
    leak detection, and `PoolRegistry` / `AsyncPoolRegistry` (pool-per-key).
  - `ConnectionFactory<T>` / `AsyncConnectionFactory<T>` (and `FnFactory` / `AsyncFnFactory`
    variants) as the seam users implement to pool anything.
  - Errors are a single `PoolError` enum (`std::error::Error`), preserving the cross-language
    taxonomy; the factory create cause is available via `Error::source`.
  - A tiny built-in SplitMix64 RNG (seeded from the standard library) keeps the blocking pool
    dependency-free; only the async pool pulls in tokio.
  - Field names, event names, defaults, and `CloseReason`/`BreakerState`/`IdleStrategy` wire values
    (plus `PoolStats::to_dictionary()` keys) are kept identical to the other poolyglot languages.
  - Tests run under `cargo-llvm-cov`; builds on Rust 1.85+ (edition 2024).
