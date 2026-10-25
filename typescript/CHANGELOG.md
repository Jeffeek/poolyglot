# Changelog

All notable changes to the TypeScript implementation are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Versions are shared across every
poolyglot language (one root `VERSION`, one behavior spec).

## [Unreleased]

### Added

- **Initial TypeScript / Node.js implementation** (`@jeffeek/connpool`, ESM, Node 22+, zero runtime
  dependencies) at feature parity with the Python `connpool`, C# `ConnPool`, Java `connpool`, and Go
  `connpool` implementations, and the shared behavior spec:
  - `AsyncConnectionPool<T>` (Promise-based, `AbortSignal` cancellation) built on a hand-rolled
    `AsyncCondition` + a "shield" helper (the twins of Python's `asyncio.Condition` / `asyncio.shield`).
    `T` is constrained to `object` (connections are tracked by object identity).
  - Sizing (`minSize`/`maxSize` hard cap with slot reservation), rotation (`maxLifetime` with jitter,
    `maxUses`, `maxIdleTime`, LIFO/FIFO), health (validate-on-checkout, optional background
    `validateIdle`), resilience (`RetryPolicy` + `CircuitBreaker`, one failure per logical creation),
    backpressure (`maxWaiters`), observability (`PoolListener` + `PoolStats` with avg/p50/p95/p99 for
    acquire-wait and create-latency), graceful `close({ timeout })`, warn-only leak detection, and
    `AsyncPoolRegistry` (pool-per-key).
  - `AsyncConnectionFactory<T>` (and `AsyncCallableFactory<T>` / `AsyncConnectionPool.fromCallables`)
    as the seam users implement to pool anything.
  - Field names, event names, defaults, and `CloseReason`/`BreakerState`/`IdleStrategy` wire values
    (plus `PoolStats.toDictionary()` keys) are kept **snake_case** and identical to the other
    poolyglot languages.
  - 100% line + branch coverage (`node:test` under `c8`); builds and tests on Node 22, 24 and 26.

### Note

- There is intentionally **no synchronous pool** (unlike the Python/C#/Java/Go ports). Node's single
  event loop has no thread-blocking `acquire`, so only the async pool maps faithfully. See the README.
