# Changelog

All notable changes to the Java implementation are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Versions are shared across every
poolyglot language (one root `VERSION`, one behavior spec).

## [Unreleased]

### Added

- **Initial Java implementation** (`io.github.jeffeek:connpool`, Java 17, dependency-free) at
  feature parity with the Python `connpool` package, the C# `ConnPool` package, and the shared
  behavior spec:
  - `ConnectionPool<T>` (thread-safe, `ReentrantLock`/`Condition`-based) and
    `AsyncConnectionPool<T>` (`CompletableFuture`-based, cooperative `CancelToken` cancellation) as
    parallel implementations of the same model.
  - Sizing (`minSize`/`maxSize` hard cap with slot reservation), rotation (`maxLifetime` with
    jitter, `maxUses`, `maxIdleTime`, LIFO/FIFO), health (validate-on-checkout, optional background
    `validateIdle`), resilience (`RetryPolicy` + `CircuitBreaker`, one failure per logical
    creation), backpressure (`maxWaiters`), observability (`PoolListener` + `PoolStats` with
    avg/p50/p95/p99 for acquire-wait and create-latency), graceful `close(timeout)`, warn-only leak
    detection, and `PoolRegistry`/`AsyncPoolRegistry` (pool-per-key).
  - `ConnectionFactory<T>` / `AsyncConnectionFactory<T>` (and `CallableFactory` variants) as the
    seam users implement to pool anything.
  - Field names, event names, defaults, and `CloseReason`/`BreakerState` wire values are kept
    identical to the other poolyglot languages.
  - 100% line + branch coverage (JUnit 5 + JaCoCo), builds on JDK 17+.
