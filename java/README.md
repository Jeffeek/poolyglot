# connpool

[![connpool](https://raw.githubusercontent.com/Jeffeek/poolyglot/main/docs/assets/package_icon.png)](https://github.com/Jeffeek/poolyglot#readme)

**Connection pooling for Java — blocking &amp; async, dependency-free.**

The Java member of [**poolyglot**](https://github.com/Jeffeek/poolyglot#readme) · *Pool once. Speak everywhere.*

[![java CI](https://github.com/Jeffeek/poolyglot/actions/workflows/java.yml/badge.svg)](https://github.com/Jeffeek/poolyglot/actions/workflows/java.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.jeffeek/connpool?logo=apachemaven&logoColor=white)](https://central.sonatype.com/artifact/io.github.jeffeek/connpool)
[![coverage](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?flag=java&logo=codecov&label=coverage)](https://app.codecov.io/gh/Jeffeek/poolyglot?flags%5B0%5D=java)
[![Java 17](https://img.shields.io/badge/java-17%2B-007396?logo=openjdk&logoColor=white)](#install)
[![license](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE)

---

A generic connection pool for Java — blocking (threads) and asynchronous
(`CompletableFuture`). **Dependency-free** (Java 17, standard library only). It pools *anything*:
JDBC drivers, HTTP clients, SSH sessions, sockets… because the pool knows nothing about
connections except what a small `ConnectionFactory<T>` tells it.

```java
import io.github.jeffeek.connpool.*;

ConnectionPool<MyConn> pool = new ConnectionPool<>(
    CallableFactory.<MyConn>builder(MyConn::connect).validate(MyConn::ping).build(),
    PoolConfig.builder().maxSize(20).build());

try (pool) {                                   // optional: open() for warm-up + background sweeper
    pool.open();
    pool.use(conn -> conn.doSomething(), Timeout.unset());  // acquire -> use -> release (errors handled)
}
```

This is the Java member of [**poolyglot**](https://github.com/Jeffeek/poolyglot#readme): one pooling model, implemented per language,
kept API-parallel. Field names, event names, defaults, and `CloseReason`/`BreakerState` wire values
are identical to the Python (`connpool`) and C# (`ConnPool`) implementations — switching languages is
a syntax change, not a model change.

## Install

Maven:

```xml
<dependency>
  <groupId>io.github.jeffeek</groupId>
  <artifactId>connpool</artifactId>
  <version>*version*</version>
</dependency>
```

Gradle:

```groovy
implementation 'io.github.jeffeek:connpool:1.0.7'
```

Requires Java 17 or newer. Zero runtime dependencies.

## Project layout

``` layout
src/main/java/io/github/jeffeek/connpool/   the library (dependency-free, one package)
  Core:          errors, Clock/SystemClock/ManualClock, PooledConnection/CloseReason, Timeout, CancelToken
  Resilience:    CircuitBreaker (+ BreakerState/BreakerConfig), RetryPolicy (backoff)
  Policy:        PoolConfig, Lifecycle (pure "when must a connection retire?" rules)
  Factories:     ConnectionFactory, AsyncConnectionFactory  <- what YOU implement
  Observability: PoolListener, PoolStats (metrics)
  Pools:         ConnectionPool, AsyncConnectionPool, PoolRegistry, AsyncPoolRegistry
src/test/java/...                           mirrors src/ (JUnit 5; 100% line + branch gate)
examples/                                   client code - NOT part of the package
```

Dependency direction is strictly downward (a layer only uses layers listed above it):
`Core → Resilience → Policy → Factories/Observability → Pools`. Everything lives in the single
`io.github.jeffeek.connpool` package, so one wildcard import brings in the whole public API.

📐 See the [**architecture diagrams**](https://github.com/Jeffeek/poolyglot/blob/main/docs/architecture.md)
— layering, connection lifecycle, and circuit-breaker states (shared across all poolyglot languages).

## What you get

| Concern | How it works |
| --- | --- |
| Sizing | `minSize` kept warm, `maxSize` is a *hard* cap (slot reserved before create) |
| Timeouts | `acquireTimeout` (per-call override via `Timeout`), `createTimeout` (async), retries never sleep past the deadline |
| Rotation | `maxLifetime` (with jitter), `maxUses`, `maxIdleTime`, LIFO/FIFO reuse |
| Health | validate on checkout if idle ≥ `validateAfterIdle`; `reset()` on release; after an exception: fatal → discard, otherwise re-validate; optional `validateIdle` pings idle connections in the background sweep and reaps dead ones |
| Retries | exponential backoff + jitter, `retryOn` filter |
| Circuit breaker | closed → open → half-open, probe limits, state-change events; open = fail fast. Counts **one failure per logical creation** (a whole retry sequence), not per attempt |
| Backpressure | `maxWaiters` caps the blocked-caller queue; excess callers fail fast with `PoolExhaustedError` |
| Leak detection | `leakDetectionTimeout` warns (via `System.Logger` + `onConnectionLeak`) when a borrowed connection is held too long; warn-only, never force-reclaimed |
| Observability | `PoolListener` hooks + `pool.stats()` snapshot (counters, **avg + p50/p95/p99** for both acquire-wait and create-latency over a recent window) |
| Shutdown | idempotent `close()`, wakes blocked callers, in-use connections close on release; `close(timeout)` drains gracefully |
| Cancellation | async cancellation (via `CancelToken`) never leaks a slot |
| Many backends | `PoolRegistry` / `AsyncPoolRegistry` = one lazily-created pool per key (host, tenant, DB) |

## Writing a factory

```java
public final class MyFactory extends ConnectionFactory<MyConn> {
    @Override public MyConn create()                 { return MyConn.connect(...); }  // enforce your own connect timeout
    @Override public boolean validate(MyConn c)      { return c.ping(); }             // cheap liveness check
    @Override public void reset(MyConn c)            { c.rollback(); }                // scrub session state on release
    @Override public void close(MyConn c)            { c.disconnect(); }              // default: close() if AutoCloseable
    @Override public boolean isFatalError(Throwable e) { return e instanceof java.net.SocketException; }
}
```

Async: subclass `AsyncConnectionFactory<T>`, or build one from lambdas with
`AsyncCallableFactory.builder(...)` (or `AsyncCallableFactory.fromSync(...)` for a synchronous
creator).

## Borrowing a connection

```java
// lambda form (exception-aware: fatal error / failed validation -> discard, else reuse)
pool.use(conn -> conn.doWork(), Timeout.unset());
// value-returning form: use a block so it binds to use(Function), not use(Consumer)
int rows = pool.use(conn -> { return conn.count(); }, Timeout.unset());

// lease form (success-path release on close, via try-with-resources)
try (ConnectionPool<MyConn>.Lease lease = pool.connection()) {
    lease.get().doWork();
}

// manual
MyConn conn = pool.acquire(Timeout.of(5.0));
try   { conn.doWork(); }
finally { pool.release(conn); }
```

Async mirrors this with `useAsync`, `acquireAsync` and `releaseAsync`, each returning a
`CompletableFuture` and taking an optional `CancelToken`.

The `Timeout` argument is one of three things: `Timeout.unset()` → use `acquireTimeout`;
`Timeout.infinite()` (or `Timeout.ofNullable(null)`) → wait forever; `Timeout.of(seconds)` →
wait that long (`0` = don't wait).

## Configuration (`PoolConfig`)

`PoolConfig` is an immutable, validated record — build it with `PoolConfig.builder()` and copy with
`cfg.toBuilder()`; both validate.

| Field | Default | Meaning |
| --- | --- | --- |
| `minSize` / `maxSize` | 0 / 10 | warm connections / hard cap |
| `acquireTimeout` | 30 | seconds to wait (`null` = forever, `0` = don't wait) |
| `maxWaiters` | null | backpressure: cap callers blocked in `acquire()` (excess → `PoolExhaustedError`; `null` = unlimited, `0` = never wait when full) |
| `createTimeout` | 10 | async only; sync factories enforce their own |
| `maxLifetime` / `lifetimeJitter` | 1800 / 0.1 | retire after age, shaved by up to 10% |
| `maxIdleTime` | 600 | evict idle connections (never below `minSize`) |
| `maxUses` | null | retire after N checkouts |
| `idleStrategy` | LIFO | `FIFO` spreads load evenly |
| `validateAfterIdle` | 30 | validate on checkout if idle this long (0 = always, null = never) |
| `validateIdle` | false | also validate idle connections during the background sweep and reap dead ones (needs `open()`; adds I/O) |
| `maintenanceInterval` | 30 | background sweep period (null = off) |
| `leakDetectionTimeout` | null | warn when a borrowed connection is held longer than this (needs `open()`; checked once per sweep; null = off) |
| `retry` | 3 attempts, 0.1s base, ×2, 5s cap | `RetryPolicy` (`RetryPolicy.NO_RETRY` disables) |
| `breaker` | 5 failures / 30s recovery | `BreakerConfig` (`null` disables); `failureThreshold` counts failed *creations*, not retry attempts |

## Build, test, run examples

```bash
mvn -q -B package           # the library
mvn test                    # run all tests (JUnit 5)
./run_tests.sh              # tests under coverage (gate: 100% line + branch)
./run_tests.sh -m          # ...also list any uncovered lines / branches
./lint.sh                   # SpotBugs:check (gate, High) + Spotless report
./lint.sh -f                # ...auto-format (spotless:apply)
./lint.sh -m                # ...also list Medium SpotBugs findings (review-only)
```

Linting uses **SpotBugs** (gated at High confidence) + **Spotless** (`googleJavaFormat`, warn-only),
configured in `pom.xml`. CI runs these as a separate `lint` job. `./lint.sh -m` lists lower-confidence
findings worth reviewing but not gated.

## Limits / extension points

* The breaker guards connection *creation*. To also trip on request failures, wrap calls with `CircuitBreaker.call()`.
* The breaker counts consecutive failed creations (one per logical acquire, retries included). A real backend failure or a `createTimeout` firing both count; an acquire-timeout (caller impatience) or cancellation mid-create does not.
* Leak detection is **warn-only** by design and only runs while the pool is `open()` (the maintenance sweep drives it), so registry pools need an explicit `open()`, and a warning can lag by up to one `maintenanceInterval`.
* **Not fork-safe / not process-shared.** `maxSize` is per-process; create a fresh pool per process.
* Pool size is fixed at construction (`PoolConfig` is immutable); there is no runtime resize. Use a new pool, or a `PoolRegistry` keyed by profile.
* Wake-up order of blocked callers is not strictly FIFO.
* Blocking `create()` can't be interrupted by the pool; give your client a connect timeout.
* The async pool runs its (cooperatively blocking) method bodies on a daemon worker pool rather than true non-blocking I/O; the observable behavior matches, and it can move to virtual threads on Java 21+.
* Listener hooks are synchronous; push heavy work to a queue.
* A factory's `create()` must return a **distinct** object each call (`T` is pooled by reference identity); returning a shared/singleton connection is rejected with `ConnectionCreateError`.
* The pool tracks borrow state by object identity (`IdentityHashMap`), so `T` must not rely on `equals`/`hashCode` for pool membership.
* Acquire-wait / create-latency percentiles are estimated over the last 1024 samples (bounded memory), not the full history.
* With `validateIdle` on, idle connections being pinged by the background sweep are briefly reserved and count as `inUse` (not `idle`) in `stats()` for the duration of that check.

## License

MIT — see [LICENSE](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE).
