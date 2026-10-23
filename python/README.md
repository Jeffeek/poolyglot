# connpool

A generic connection pool for Python 3.11+ — sync (threads) and `asyncio`. **Standard library only**
(no third-party runtime dependencies). It pools *anything*: DB drivers, HTTP sessions, SSH clients, sockets…
because the pool knows nothing about connections except what a small `ConnectionFactory` tells it.

```python
from connpool import ConnectionPool, PoolConfig

pool = ConnectionPool.from_callables(make_conn, validate=is_alive, config=PoolConfig(max_size=20))
with pool:                                  # optional: warm-up + background sweeper
    with pool.connection() as conn:         # acquire -> use -> release (errors handled)
        conn.do_something()
```

## Project layout

```
src/connpool/                 the library (imports: standard library only)
  core/            errors, clocks, PooledConnection/CloseReason, timeout helpers
  resilience/      breaker.py (circuit breaker), retry.py (backoff policy)
  policy/          config.py (PoolConfig), lifecycle.py (pure "when must a connection retire?" rules)
  factories/       sync_factory.py, async_factory.py  <- what YOU implement to pool anything
  observability/   events.py (PoolListener), stats.py (metrics)
  pools/           sync_pool.py, async_pool.py, registry.py (pool-per-key)
tests/             mirrors src/ (unittest, no dependencies)
  support/         fake factories and helpers
examples/          client code - NOT part of the library
  database/        generic DB-API factory (any driver), sqlite3 demo (runnable), psycopg demo
  http_client/  ssh/  async_usage/
```

Dependency direction is strictly downward (a layer only imports from layers listed above it):
`core → resilience → policy → factories/observability → pools`. The top-level `connpool/__init__.py` is a
facade re-exporting the public API, so `from connpool import ConnectionPool` keeps working even if internal
modules move.

## What you get

| Concern | How it works |
|---|---|
| Sizing | `min_size` kept warm, `max_size` is a *hard* cap (slot reserved before create) |
| Timeouts | `acquire_timeout` (per-call override), `create_timeout` (async), retries never sleep past the deadline |
| Rotation | `max_lifetime` (with jitter), `max_uses`, `max_idle_time`, `LIFO`/`FIFO` reuse |
| Health | validate on checkout if idle ≥ `validate_after_idle`; `reset()` on release; after an exception: fatal → discard, otherwise re-validate; optional `validate_idle` pings idle connections in the background sweep and reaps dead ones |
| Retries | exponential backoff + jitter, `retry_on` filter |
| Circuit breaker | closed → open → half-open, probe limits, state-change events; open = fail fast. Counts **one failure per logical creation** (a whole retry sequence), not per attempt. A create failure *or* a `create_timeout` (slow backend) counts; an acquire-timeout or cancellation mid-create does not |
| Backpressure | `max_waiters` caps the blocked-caller queue; excess callers fail fast with `PoolExhaustedError` |
| Leak detection | `leak_detection_timeout` warns (via log + `on_connection_leak`) when a borrowed connection is held too long; warn-only, never force-reclaimed |
| Observability | `PoolListener` hooks + `pool.stats()` snapshot (counters, **avg + p50/p95/p99** for both acquire-wait and create-latency over a recent window) |
| Shutdown | idempotent `close()`, wakes blocked callers, in-use connections close on release; `close(timeout=)` drains gracefully; a `ResourceWarning` fires if a pool is GC'd un-closed |
| Cancellation | asyncio cancellation never leaks a slot |
| Many backends | `PoolRegistry` / `AsyncPoolRegistry` = one lazily-created pool per key (host, tenant, DB) |

## Writing a factory

```python
class MyFactory(ConnectionFactory[MyConn]):
    def create(self):            return MyConn.connect(...)        # enforce your own connect timeout
    def validate(self, c):       return c.ping()                    # cheap liveness check
    def reset(self, c):          c.rollback()                       # scrub session state on release
    def close(self, c):          c.close()                          # default: calls c.close()
    def is_fatal_error(self, e): return isinstance(e, ConnectionError)
```
Async: subclass `AsyncConnectionFactory`, or pass sync/async callables to `AsyncConnectionPool.from_callables`.
`examples/database/dbapi_factory.py` shows one factory that covers every PEP 249 database driver.

## Configuration (`PoolConfig`)

| Field | Default | Meaning |
|---|---|---|
| `min_size` / `max_size` | 0 / 10 | warm connections / hard cap |
| `acquire_timeout` | 30 | seconds to wait (`None` = forever, `0` = don't wait) |
| `max_waiters` | None | backpressure: cap callers blocked in `acquire()` (excess → `PoolExhaustedError`; `None` = unlimited, `0` = never wait when full) |
| `create_timeout` | 10 | async only; sync factories enforce their own |
| `max_lifetime` / `lifetime_jitter` | 1800 / 0.1 | retire after age, shaved by up to 10% |
| `max_idle_time` | 600 | evict idle connections (never below `min_size`) |
| `max_uses` | None | retire after N checkouts |
| `idle_strategy` | LIFO | `FIFO` spreads load evenly |
| `validate_after_idle` | 30 | validate on checkout if idle this long (0 = always, None = never) |
| `validate_idle` | False | also validate idle connections during the background sweep and reap dead ones (needs `open()`; adds I/O) |
| `maintenance_interval` | 30 | background sweep period (None = off) |
| `leak_detection_timeout` | None | warn when a borrowed connection is held longer than this (needs `open()`; checked once per `maintenance_interval` sweep; None = off) |
| `retry` | 3 attempts, 0.1s base, ×2, 5s cap | `RetryPolicy` (`NO_RETRY` disables) |
| `breaker` | 5 failures / 30s recovery | `BreakerConfig` (`None` disables); `failure_threshold` counts failed *creations*, not retry attempts |

## Install, test, run examples

```
pip install -e .                                  # or: export PYTHONPATH=src
pip install -e ".[dev]"                           # + coverage/pytest/mypy
python -m unittest discover -s tests -t .         # 142 tests, ~2s
./run_tests.sh                                     # same, with a compact coverage report (gate: 100%)
mypy                                               # strict type check (config in pyproject.toml)
python examples/database/sqlite_demo.py
```
(`pytest` works too if installed; configuration is in `pyproject.toml`.)
Examples importing `paramiko` or `psycopg` need those packages; the library never does.

## Limits / extension points

* The breaker guards connection *creation*. To also trip on request failures wrap calls with `CircuitBreaker.call()`.
* The breaker counts consecutive failed creations (one per logical acquire, retries included). A real backend failure or a `create_timeout` firing (the backend is too slow to connect — a genuine fault) both count; an acquire-timeout (caller impatience) or cancellation mid-create does not. A rate-based window is a drop-in replacement of one class.
* Leak detection is **warn-only** by design — the pool never yanks a connection a caller may still be using. It also only runs while the pool is `open()` (the maintenance sweep drives it), so registry pools need an explicit `open()`, and a warning can lag the real leak by up to one `maintenance_interval`.
* **Not fork-safe.** A pool `open()`ed before `os.fork()` (e.g. a pre-forking server like gunicorn with `preload`, or `multiprocessing`) shares live connections/file descriptors with the children — close the pool before forking and create a fresh one per process (`max_size` is per-process).
* Pool size is fixed at construction (`PoolConfig` is immutable); there is no runtime resize. Use a new pool, or a `PoolRegistry` keyed by profile, if you need different caps at runtime.
* Wake-up order of blocked threads is not strictly FIFO.
* Sync `create()` can't be interrupted by the pool; give your client a connect timeout.
* Listener hooks are synchronous; push heavy work to a queue.
* A factory's `create()` must return a **distinct** object each call; returning a shared/singleton connection is rejected with `ConnectionCreateError` (the pool tracks borrow state by object identity).
* `acquire_wait` percentiles are estimated over the last 1024 samples (bounded memory), not the full history.
* With `validate_idle` on, idle connections being pinged by the background sweep are briefly reserved and count as `in_use` (not `idle`) in `stats()` for the duration of that check — they are genuinely unavailable while validated, not borrowed by a caller.

## Considered, not implemented

These are deliberate scope decisions, not oversights. For most, something already in the
library covers the real need; adding them would make the pool heavier for every user to serve
a minority. Listed here so the trade-off is explicit and the upgrade path is known.

| Feature | How it would be implemented | Why not (and what covers it today) |
|---|---|---|
| **Runtime resize** | Make `max_size`/`min_size` mutable behind the lock; on grow, raise the cap and `notify_all` waiters; on shrink, mark excess idle connections for disposal and let in-use ones close on release (never force-closed). | Erodes the immutable-`PoolConfig` invariant (predictability is a feature); shrink is fiddly. Use a fresh pool or a `PoolRegistry` keyed by profile — and `min_size`↔`max_size` already flexes with load. |
| **Adaptive sizing** | A control loop that tunes `max_size` from live metrics (saturation, acquire latency), with hysteresis to avoid oscillation. | Genuinely hard to do well and easy to make worse than static. `min_size` + `max_size` + idle eviction is already a simple, deterministic adaptive scheme (grow to max under load, shrink to min when quiet). |
| **Priority acquisition** | Replace the single `Condition` with a priority heap of per-waiter futures; `release()` hands the connection to the highest-priority waiter; timeouts/cancellation remove waiters from the heap. | Ordering ≠ isolation, and it complicates the hot path. Prefer **separate pools** (a `PoolRegistry` acts as bulkheads): give batch its own small pool and interactive its own. |
| **Create-rate limiting** | A token-bucket / min-interval gate in the create path, checked after reserving a slot and bounded by `acquire_timeout`. | Connection storms are already bounded by `max_size` (caps concurrent creates) and the circuit breaker (halts storms after failures). Add only if a backend imposes a literal connect/sec quota — this is the cheapest of these to add if ever needed. |
| **Lease handles (use-after-release protection)** | Return a proxy wrapping the raw connection, forwarding attribute access and invalidating itself on release. | A Python proxy is bypassable (the underlying object is always reachable), so it is a guardrail, not a guarantee, and it taxes every call. The context manager + identity tracking already give deterministic release and detect double/foreign release. |
| **Broken-connection quarantine** | Move suspect connections to a side store with a reason/timestamp and a debug API, instead of closing them. | Quarantine's only real value is forensic; in steady state you *want* broken connections closed — hoarding dead file descriptors is a resource leak by design (HikariCP/asyncpg discard too). Use the `on_connection_closed(conn, reason)` hook to snapshot metadata at close time if you need diagnostics. |
