# connpool

[![connpool](https://raw.githubusercontent.com/Jeffeek/poolyglot/main/docs/assets/package_icon.png)](https://github.com/Jeffeek/poolyglot#readme)

**Connection pooling for Rust — blocking & async (`tokio`), zero deps for the blocking pool.**

The Rust member of [**poolyglot**](https://github.com/Jeffeek/poolyglot#readme) · *Pool once. Speak everywhere.*

[![rust CI](https://github.com/Jeffeek/poolyglot/actions/workflows/rust.yml/badge.svg)](https://github.com/Jeffeek/poolyglot/actions/workflows/rust.yml)
[![crates.io](https://img.shields.io/crates/v/connpool-rs?logo=rust&label=connpool-rs)](https://crates.io/crates/connpool-rs)
[![docs.rs](https://img.shields.io/docsrs/connpool-rs?logo=docsdotrs&label=docs.rs)](https://docs.rs/connpool-rs)
[![coverage](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?flag=rust&logo=codecov&label=coverage)](https://app.codecov.io/gh/Jeffeek/poolyglot?flags%5B0%5D=rust)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue?logo=rust)](#build-test-run-examples)
[![license](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE)

---

A generic connection pool for Rust. The blocking `ConnectionPool` is **standard-library only
(zero runtime dependencies)**; the async `AsyncConnectionPool` lives behind the optional `tokio`
feature. It pools *anything*: DB drivers, HTTP clients, SSH sessions, sockets… because the pool
knows nothing about connections except what a small `ConnectionFactory` tells it. `T` is tracked
by value identity (`Clone + Eq + Hash`) — e.g. `Arc<Conn>` — the Rust analog of Go's `comparable`.

```rust
use connpool::{ConnectionPool, FnFactory, PoolConfig, Timeout};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// A toy "connection": a unique id behind an Arc so clones share identity.
let next = Arc::new(AtomicU64::new(0));
let factory = FnFactory::new({
    let next = next.clone();
    move || Ok::<_, connpool::FactoryError>(Arc::new(next.fetch_add(1, Ordering::SeqCst)))
});

let pool = ConnectionPool::new(factory, PoolConfig { max_size: 20, ..Default::default() }, None)
    .expect("valid config");
pool.open()?;                                 // optional: warm-up + background sweeper

let conn = pool.acquire(Timeout::Unset)?;     // acquire -> use -> release
// ... use conn ...
pool.release(conn)?;
```

This is the Rust member of [**poolyglot**](https://github.com/Jeffeek/poolyglot#readme): one pooling model, implemented per language,
kept API-parallel. Field names, event names, defaults, and `CloseReason`/`BreakerState` wire values
are identical to the Python (`connpool`), C# (`ConnPool`), Java (`connpool`), Go (`connpool`) and
TypeScript (`@jeffeek/connpool`) implementations — switching languages is a syntax change, not a
model change.

## Install

The crate is published as **`connpool-rs`** (the crates.io name `connpool` was taken) but is
imported as **`connpool`** (the library name), so `use connpool::...` works unchanged:

```toml
[dependencies]
connpool-rs = "1"
# with the async pool:
connpool-rs = { version = "1", features = ["tokio"] }
```

Requires Rust 1.85+ (edition 2024). Only the blocking pool is dependency-free; enabling the
`tokio` feature adds a single dependency (`tokio`) used by `AsyncConnectionPool` /
`AsyncPoolRegistry`. With the feature off (the default), the crate pulls in nothing.

## Project layout

``` layout
src/                         the library (dependency-free for the blocking pool; one crate `connpool`)
  core/            errors.rs, clock.rs, connection.rs (PooledConnection/CloseReason), timeouts.rs (Timeout)
  resilience/      breaker.rs (CircuitBreaker + BreakerState/BreakerConfig), retry.rs (RetryPolicy)
  policy/          config.rs (PoolConfig/IdleStrategy), lifecycle.rs (pure "when must a connection retire?" rules)
  factories/       sync_factory.rs, async_factory.rs (tokio-gated)  <- what YOU implement
  observability/   events.rs (PoolListener), stats.rs (PoolStats)
  pools/           sync_pool.rs, async_pool.rs (tokio-gated), registry.rs, async_registry.rs (tokio-gated)
  tests inline     `#[cfg(test)] mod tests` per file (cargo test; 100% line gate via cargo-llvm-cov)
examples/                    client code - NOT part of the published crate
```

Dependency direction is strictly downward (a layer only uses layers listed above it):
`core → resilience → policy → factories/observability → pools`. Everything lives in the single
`connpool` crate, so one `use connpool::...` imports the whole public API via `src/lib.rs`.

📐 See the [**architecture diagrams**](https://github.com/Jeffeek/poolyglot/blob/main/docs/architecture.md)
— layering, connection lifecycle, and circuit-breaker states (shared across all poolyglot languages).

## What you get

| Concern | How it works |
| --- | --- |
| Sizing | `min_size` kept warm, `max_size` is a *hard* cap (slot reserved before create) |
| Timeouts | `acquire_timeout` (per-call override via `Timeout`), `create_timeout` (async), retries never sleep past the deadline |
| Rotation | `max_lifetime` (with jitter), `max_uses`, `max_idle_time`, LIFO/FIFO reuse |
| Health | validate on checkout if idle ≥ `validate_after_idle`; `reset()` on release; after an error: fatal → discard, otherwise re-validate; optional `validate_idle` pings idle connections in the background sweep and reaps dead ones |
| Retries | exponential backoff + jitter, configurable retry filter |
| Circuit breaker | closed → open → half-open, probe limits, state-change events; open = fail fast. Counts **one failure per logical creation** (a whole retry sequence), not per attempt |
| Backpressure | `max_waiters` caps the blocked-caller queue; excess callers fail fast with `PoolError::Exhausted` |
| Leak detection | `leak_detection_timeout` warns (via `PoolListener::on_connection_leak`) when a borrowed connection is held too long; warn-only, never force-reclaimed |
| Observability | `PoolListener` hooks + `pool.stats()` snapshot (counters, **avg + p50/p95/p99** for both acquire-wait and create-latency over a recent window) |
| Shutdown | idempotent `close()`, wakes blocked callers, in-use connections close on release; `close(Some(timeout))` drains gracefully |
| Cancellation | async cancellation (future-drop, see below) never leaks a slot |
| Many backends | `PoolRegistry` / `AsyncPoolRegistry` = one lazily-created pool per key (host, tenant, DB) |

## Writing a factory

Implement `ConnectionFactory<T>` (or build one from closures with `FnFactory`):

```rust
use connpool::{ConnectionFactory, FactoryError};
use std::error::Error;

struct MyFactory;

impl ConnectionFactory<Arc<MyConn>> for MyFactory {
    fn create(&self) -> Result<Arc<MyConn>, FactoryError> {
        Ok(Arc::new(dial()?))                      // enforce your own connect timeout
    }
    fn validate(&self, c: &Arc<MyConn>) -> bool {
        c.ping().is_ok()                            // cheap liveness check
    }
    fn reset(&self, c: &Arc<MyConn>) -> Result<(), FactoryError> {
        c.rollback().map_err(Into::into)            // scrub session state on release
    }
    fn close(&self, c: &Arc<MyConn>) -> Result<(), FactoryError> {
        c.disconnect().map_err(Into::into)          // default: no-op; most types close on Drop
    }
    fn is_fatal_error(&self, err: &(dyn Error + 'static)) -> bool {
        err.to_string().contains("connection reset")
    }
}
```

Async: implement `AsyncConnectionFactory<T>` (behind the `tokio` feature), or build one from
closures with `AsyncFnFactory`.

## Configuration (`PoolConfig`)

`PoolConfig` is a plain struct with `Default` — build with struct-update syntax and call
`validate()` (the pool constructor does this for you, returning `PoolError::Config` on failure).
Optional seconds / counts are `Option<f64>` / `Option<usize>` / `Option<u64>`; `None` means "no
limit".

| Field | Default | Meaning |
| --- | --- | --- |
| `min_size` / `max_size` | 0 / 10 | warm connections / hard cap |
| `acquire_timeout` | 30 | seconds to wait (`None` = forever, `0` = don't wait) |
| `max_waiters` | None | backpressure: cap callers blocked in `acquire()` (excess → `PoolError::Exhausted`; `None` = unlimited, `0` = never wait when full) |
| `create_timeout` | 10 | async only; sync factories enforce their own |
| `max_lifetime` / `lifetime_jitter` | 1800 / 0.1 | retire after age, shaved by up to 10% |
| `max_idle_time` | 600 | evict idle connections (never below `min_size`) |
| `max_uses` | None | retire after N checkouts |
| `idle_strategy` | Lifo | `Fifo` spreads load evenly |
| `validate_after_idle` | 30 | validate on checkout if idle this long (`0` = always, `None` = never) |
| `validate_idle` | false | also validate idle connections during the background sweep and reap dead ones (needs `open()`; adds I/O) |
| `maintenance_interval` | 30 | background sweep period (`None` = off) |
| `leak_detection_timeout` | None | warn when a borrowed connection is held longer than this (needs `open()`; checked once per sweep; `None` = off) |
| `retry` | 3 attempts, 0.1s base, ×2, 5s cap | `RetryPolicy` (`RetryPolicy::no_retry()` disables) |
| `breaker` | 5 failures / 30s recovery | `Option<BreakerConfig>` (`None` disables); `failure_threshold` counts failed *creations*, not retry attempts |

## Build, test, run examples

```bash
cargo build                         # the blocking library (zero deps)
cargo build --features tokio        # include the async pool
cargo test --all-features           # run all tests
./run_tests.sh                      # tests under coverage (cargo-llvm-cov)
./run_tests.sh -m                   # ...also list uncovered lines
cargo run --example demo            # runnable demo
```

Coverage needs the `llvm-tools-preview` component and `cargo-llvm-cov`:

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov
```

## Idiom mapping from Python/Go

* Errors are a single `PoolError` enum (not an exception hierarchy or a marker-interface struct
  family) — match on the variant, the analog of `except PoolTimeoutError` / `errors.As(&PoolTimeoutError{})`.
* Optional seconds/counts are `Option<f64>` / `Option<usize>` (Go's `*float64`/`*int`, Python's
  `Optional[float]`).
* Connections are tracked by value identity, so `T: Clone + Eq + Hash` — use `Arc<Conn>` or an id,
  the direct analog of Go's `T comparable`.
* No RAII guard type yet — borrowing is explicit `acquire()` / `release()`, mirroring Go's
  `Acquire`/`Release` pair (not C#'s `using lease = pool.Connection()`).
- Async cancellation is future-drop, not a token: dropping the `acquire()`/`use_()` future while
  it's pending frees the reserved slot via a `Drop` guard, the Rust analog of Python's implicit
  task-cancellation handling and C#'s `CancellationToken`.
* A tiny built-in SplitMix64 RNG (`Rng`/`SystemRng`) drives jitter so the blocking pool stays
  zero-dependency instead of pulling in `rand`.
* Wire values stay identical across languages: `CloseReason::as_str()`, `BreakerState::as_str()`,
  `IdleStrategy::as_str()`, and `PoolStats::to_dictionary()` keys are snake_case and match Python,
  C#, Java, Go and TypeScript exactly.

## Limits / extension points

* The breaker guards connection *creation*. To also trip on request failures, wrap calls with `CircuitBreaker::call()`.
* The breaker counts consecutive failed creations (one per logical acquire, retries included). A real backend failure or a `create_timeout` firing both count; an acquire-timeout (caller impatience) or cancellation mid-create does not.
* Leak detection is **warn-only** by design and only runs while the pool is `open()`ed (the maintenance sweep drives it), so registry pools need an explicit `open()`, and a warning can lag by up to one `maintenance_interval`.
* **Not fork-safe / not process-shared.** `max_size` is per-process; create a fresh pool per process.
* Pool size is fixed at construction; there is no runtime resize. Use a new pool, or a `PoolRegistry` keyed by profile.
* Wake-up order of blocked callers is not strictly FIFO (the condition broadcasts on every change).
* A blocking `create()` can't be interrupted by the pool; give your client a connect timeout. For the async pool, `create_timeout` is only enforced if your factory honors cancellation.
* Listener hooks are synchronous; push heavy work to a queue.
* A factory's `create()` must return a **distinct** object each call (`T` is pooled by value identity; use `Arc<Conn>` or similar); returning a shared/singleton connection is rejected with `PoolError::ConnectionCreate`.
* `T` is constrained to `Clone + Eq + Hash` because the pool tracks borrow state by the connection value in a map.
* The async pool requires the `tokio` feature — Rust's standard library has no async runtime or timer of its own, unlike Go (`context`) or C# (`Task`). The blocking pool stays zero-dependency; this mirrors how the TypeScript port is async-only for the opposite reason (no blocking primitives at all).
* Acquire-wait / create-latency percentiles are estimated over the last 1024 samples (bounded memory), not the full history.
* With `validate_idle` on, idle connections being pinged by the background sweep are briefly reserved and count as in-use (not idle) in `stats()` for the duration of that check.
* Coverage gate is **100% lines** (the LCOV metric Codecov reports), enforced by `run_tests.sh`. LLVM *region/branch* coverage is intentionally not gated: `async fn` state-machine codegen leaves a few region artifacts that aren't real untested paths.

## Considered, not implemented

* A `PoolGuard`/RAII wrapper that releases on `Drop` (like C#'s `using lease = pool.Connection()`) —
  skipped for now; `acquire()`/`release()` stays explicit and panics on an unreleased connection are
  avoided deliberately, since a double-free-style bug in a `Drop` impl can't return a `Result`.
* Runtime pool resize — same rationale as the other languages: a `PoolRegistry` keyed by profile
  covers the common case without adding mutable shared config.

## License

MIT — see [LICENSE](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE).
