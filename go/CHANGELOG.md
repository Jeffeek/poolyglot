# Changelog

All notable changes to the Go implementation are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Versions are shared across every
poolyglot language (one root `VERSION`, one behavior spec).

## [Unreleased]

### Added

- **Initial Go implementation** (`github.com/Jeffeek/poolyglot/go`, package `connpool`, Go 1.21,
  standard library only) at feature parity with the Python `connpool` package, the C# `ConnPool`
  package, the Java `connpool` package, and the shared behavior spec:
  - `ConnectionPool[T]` (thread-safe, `sync.Mutex` + a channel-based broadcast condition) and
    `AsyncConnectionPool[T]` (context-based, `context.Context` cancellation) as parallel
    implementations of the same model. `T` is constrained to `comparable` (connections are tracked
    by value identity — pointer/interface types track by reference).
  - Sizing (`MinSize`/`MaxSize` hard cap with slot reservation), rotation (`MaxLifetime` with
    jitter, `MaxUses`, `MaxIdleTime`, LIFO/FIFO), health (validate-on-checkout, optional background
    `ValidateIdle`), resilience (`RetryPolicy` + `CircuitBreaker`, one failure per logical
    creation), backpressure (`MaxWaiters`), observability (`PoolListener` + `PoolStats` with
    avg/p50/p95/p99 for acquire-wait and create-latency), graceful `Close(timeout)`, warn-only leak
    detection, and `PoolRegistry`/`AsyncPoolRegistry` (pool-per-key).
  - `ConnectionFactory[T]` / `AsyncConnectionFactory[T]` (and `FuncFactory` variants) as the seam
    users implement to pool anything.
  - Field names, event names, defaults, and `CloseReason`/`BreakerState` wire values (plus
    `PoolStats.ToDictionary()` keys) are kept identical to the other poolyglot languages.
  - 100% statement coverage (`go test`), builds on Go 1.21+.
