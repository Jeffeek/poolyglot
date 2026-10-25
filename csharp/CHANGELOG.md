# Changelog

All notable changes to the .NET implementation are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Versions are shared across every
poolyglot language (one root `VERSION`, one behavior spec).

## [Released]

### Added

- **Initial .NET implementation** (`ConnPool`, `netstandard2.1`, dependency-free) at feature
  parity with the Python `connpool` package and the shared behavior spec:
  - `ConnectionPool<T>` (thread-safe, `Monitor`-based) and `AsyncConnectionPool<T>`
    (`async`/`await`, with an await-able condition variable) as parallel implementations of the
    same model.
  - Sizing (`MinSize`/`MaxSize` hard cap with slot reservation), rotation (`MaxLifetime` with
    jitter, `MaxUses`, `MaxIdleTime`, LIFO/FIFO), health (validate-on-checkout, optional
    background `ValidateIdle`), resilience (`RetryPolicy` + `CircuitBreaker`, one failure per
    logical creation), backpressure (`MaxWaiters`), observability (`PoolListener` +
    `PoolStats` with avg/p50/p95/p99 for acquire-wait and create-latency), graceful
    `Close(timeout)`, warn-only leak detection, and `PoolRegistry`/`AsyncPoolRegistry`
    (pool-per-key).
  - `ConnectionFactory<T>` / `AsyncConnectionFactory<T>` (and `CallableFactory` variants) as the
    seam users implement to pool anything.
  - Field names, event names, defaults, and `CloseReason`/`BreakerState` wire values are kept
    identical to the other poolyglot languages.
  - 100% line coverage (xUnit + coverlet), builds on the .NET 8/9/10 SDKs.
