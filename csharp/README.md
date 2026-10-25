# ConnPool

[![ConnPool](https://raw.githubusercontent.com/Jeffeek/poolyglot/main/docs/assets/package_icon.png)](https://github.com/Jeffeek/poolyglot#readme)

**Connection pooling for .NET — sync &amp; async, dependency-free.**

The C# member of [**poolyglot**](https://github.com/Jeffeek/poolyglot#readme) · *Pool once. Speak everywhere.*

[![.NET CI](https://github.com/Jeffeek/poolyglot/actions/workflows/dotnet.yml/badge.svg)](https://github.com/Jeffeek/poolyglot/actions/workflows/dotnet.yml)
[![NuGet](https://img.shields.io/nuget/v/ConnPool?logo=nuget&logoColor=white)](https://www.nuget.org/packages/ConnPool/)
[![coverage](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?flag=csharp&logo=codecov&label=coverage)](https://app.codecov.io/gh/Jeffeek/poolyglot?flags%5B0%5D=csharp)
[![netstandard2.1](https://img.shields.io/badge/netstandard-2.1-512BD4?logo=dotnet&logoColor=white)](#install)
[![license](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE)

---

A generic connection pool for .NET — synchronous (threads) and `async`/`await`. **Dependency-free**
(`netstandard2.1`, standard library only). It pools *anything*: DB drivers, HTTP clients, SSH
sessions, sockets… because the pool knows nothing about connections except what a small
`ConnectionFactory<T>` tells it.

```csharp
using ConnPool;

var pool = ConnectionPool<MyConn>.FromCallables(MakeConn, validate: IsAlive,
    config: new PoolConfig { MaxSize = 20 });

using (pool)                                  // optional: Open() for warm-up + background sweeper
{
    pool.Open();
    pool.Use(conn => conn.DoSomething());     // acquire -> use -> release (errors handled)
}
```

This is the C# member of [**poolyglot**](https://github.com/Jeffeek/poolyglot#readme): one pooling model, implemented per language,
kept API-parallel. Field names, event names, defaults, and `CloseReason`/`BreakerState` wire values
are identical to the Python (`connpool`) implementation — switching languages is a syntax change,
not a model change.

## Install

``` cmd
dotnet add package ConnPool
```

Targets `netstandard2.1`, so it runs on .NET Core 3.0+, .NET 5+, and Mono/Xamarin/Unity.

## Project layout

``` layout
src/ConnPool/                 the library (dependency-free)
  Core/            errors, clocks, PooledConnection/CloseReason, Timeout helpers
  Resilience/      CircuitBreaker.cs (breaker + state/config), RetryPolicy.cs (backoff)
  Policy/          PoolConfig.cs, Lifecycle.cs (pure "when must a connection retire?" rules)
  Factories/       ConnectionFactory.cs, AsyncConnectionFactory.cs  <- what YOU implement
  Observability/   Events.cs (PoolListener), Stats.cs (metrics)
  Pools/           ConnectionPool.cs, AsyncConnectionPool.cs, Registry.cs, AsyncPrimitives.cs
tests/ConnPool.Tests/         mirrors src/ (xUnit; 100% line coverage gate)
examples/                     client code - NOT part of the package
```

Dependency direction is strictly downward (a layer only uses layers listed above it):
`Core → Resilience → Policy → Factories/Observability → Pools`. Everything lives in the single
`ConnPool` namespace, so one `using ConnPool;` imports the whole public API.

📐 See the [**architecture diagrams**](https://github.com/Jeffeek/poolyglot/blob/main/docs/architecture.md)
— layering, connection lifecycle, and circuit-breaker states (shared across all poolyglot languages).

## What you get

| Concern | How it works |
| --- | --- |
| Sizing | `MinSize` kept warm, `MaxSize` is a *hard* cap (slot reserved before create) |
| Timeouts | `AcquireTimeout` (per-call override via `Timeout`), `CreateTimeout` (async), retries never sleep past the deadline |
| Rotation | `MaxLifetime` (with jitter), `MaxUses`, `MaxIdleTime`, LIFO/FIFO reuse |
| Health | validate on checkout if idle ≥ `ValidateAfterIdle`; `Reset()` on release; after an exception: fatal → discard, otherwise re-validate; optional `ValidateIdle` pings idle connections in the background sweep and reaps dead ones |
| Retries | exponential backoff + jitter, `RetryOn` filter |
| Circuit breaker | closed → open → half-open, probe limits, state-change events; open = fail fast. Counts **one failure per logical creation** (a whole retry sequence), not per attempt |
| Backpressure | `MaxWaiters` caps the blocked-caller queue; excess callers fail fast with `PoolExhaustedError` |
| Leak detection | `LeakDetectionTimeout` warns (via `Trace` + `OnConnectionLeak`) when a borrowed connection is held too long; warn-only, never force-reclaimed |
| Observability | `PoolListener` hooks + `pool.Stats()` snapshot (counters, **avg + p50/p95/p99** for both acquire-wait and create-latency over a recent window) |
| Shutdown | idempotent `Close()`, wakes blocked callers, in-use connections close on release; `Close(timeout)` drains gracefully; a leak warning is traced if a pool is finalized un-closed |
| Cancellation | async cancellation (via `CancellationToken`) never leaks a slot |
| Many backends | `PoolRegistry` / `AsyncPoolRegistry` = one lazily-created pool per key (host, tenant, DB) |

## Writing a factory

```csharp
public sealed class MyFactory : ConnectionFactory<MyConn>
{
    public override MyConn Create()           => MyConn.Connect(...);   // enforce your own connect timeout
    public override bool Validate(MyConn c)   => c.Ping();               // cheap liveness check
    public override void Reset(MyConn c)      => c.Rollback();           // scrub session state on release
    public override void Close(MyConn c)      => c.Dispose();            // default: Dispose() if IDisposable
    public override bool IsFatalError(Exception e) => e is System.Net.Sockets.SocketException;
}
```

Async: subclass `AsyncConnectionFactory<T>`, or pass delegates to
`AsyncConnectionPool<T>.FromCallables(...)` (or `AsyncCallableFactory<T>.FromSync(...)` for sync creators).
`examples/AdoNetFactory.cs` shows one factory that covers every ADO.NET provider.

## Borrowing a connection

```csharp
// delegate form (exception-aware: fatal error / failed validation -> discard, else reuse)
pool.Use(conn => conn.DoWork());
int rows = pool.Use(conn => conn.Count());

// lease form (success-path release on Dispose)
using (var lease = pool.Connection())
    lease.Value.DoWork();

// manual
var conn = pool.Acquire(timeout: 5.0);
try   { conn.DoWork(); }
finally { pool.Release(conn); }
```

Async mirrors this with `UseAsync`, `ConnectionAsync` (`await using`), and `AcquireAsync` /
`ReleaseAsync`, each taking an optional `CancellationToken`.

The `Timeout` argument is one of three things: omitted/`Timeout.Unset` → use `AcquireTimeout`;
`Timeout.Infinite` (or `(double?)null`) → wait forever; a `double` (seconds, implicitly convertible) →
wait that long (`0` = don't wait).

## Configuration (`PoolConfig`)

`PoolConfig` is an immutable, validated record — construct with an object initializer and copy with
`with`; both validate.

| Field | Default | Meaning |
| --- | --- | --- |
| `MinSize` / `MaxSize` | 0 / 10 | warm connections / hard cap |
| `AcquireTimeout` | 30 | seconds to wait (`null` = forever, `0` = don't wait) |
| `MaxWaiters` | null | backpressure: cap callers blocked in `Acquire()` (excess → `PoolExhaustedError`; `null` = unlimited, `0` = never wait when full) |
| `CreateTimeout` | 10 | async only; sync factories enforce their own |
| `MaxLifetime` / `LifetimeJitter` | 1800 / 0.1 | retire after age, shaved by up to 10% |
| `MaxIdleTime` | 600 | evict idle connections (never below `MinSize`) |
| `MaxUses` | null | retire after N checkouts |
| `IdleStrategy` | Lifo | `Fifo` spreads load evenly |
| `ValidateAfterIdle` | 30 | validate on checkout if idle this long (0 = always, null = never) |
| `ValidateIdle` | false | also validate idle connections during the background sweep and reap dead ones (needs `Open()`; adds I/O) |
| `MaintenanceInterval` | 30 | background sweep period (null = off) |
| `LeakDetectionTimeout` | null | warn when a borrowed connection is held longer than this (needs `Open()`; checked once per sweep; null = off) |
| `Retry` | 3 attempts, 0.1s base, ×2, 5s cap | `RetryPolicy` (`RetryPolicy.NoRetry` disables) |
| `Breaker` | 5 failures / 30s recovery | `BreakerConfig` (`null` disables); `FailureThreshold` counts failed *creations*, not retry attempts |

## Build, test, run examples

``` cmd
dotnet build src/ConnPool/ConnPool.csproj          # the library (netstandard2.1)
dotnet test                                         # 181 tests (net8.0 + net10.0)
./run_tests.sh                                       # tests under coverage (gate: 100% line + branch)
./run_tests.sh -m                                    # ...also list any uncovered lines / branches
dotnet run --project examples                        # runnable SQLite demo
./lint.sh                                            # Roslyn analyzers as errors (gate) + dotnet format report
./lint.sh -f                                         # ...auto-format
```

Linting uses the **Roslyn analyzers** (`AnalysisLevel=latest-recommended`), gated via
`dotnet build -warnaserror`; `dotnet format` is warn-only. `.editorconfig` silences a few rules that
conflict with the cross-language design (e.g. `PoolError` is not renamed to `PoolException`). CI runs
these as a separate `lint` job.

## Limits / extension points

* The breaker guards connection *creation*. To also trip on request failures, wrap calls with `CircuitBreaker.Call()`.
* The breaker counts consecutive failed creations (one per logical acquire, retries included). A real backend failure or a `CreateTimeout` firing both count; an acquire-timeout (caller impatience) or cancellation mid-create does not.
* Leak detection is **warn-only** by design and only runs while the pool is `Open()` (the maintenance sweep drives it), so registry pools need an explicit `Open()`, and a warning can lag by up to one `MaintenanceInterval`.
* **Not fork-safe / not process-shared.** `MaxSize` is per-process; create a fresh pool per process.
* Pool size is fixed at construction (`PoolConfig` is immutable); there is no runtime resize. Use a new pool, or a `PoolRegistry` keyed by profile.
* Wake-up order of blocked callers is not strictly FIFO.
* Sync `Create()` can't be interrupted by the pool; give your client a connect timeout.
* Listener hooks are synchronous; push heavy work to a queue.
* A factory's `Create()` must return a **distinct** object each call (`T` is pooled by reference identity); returning a shared/singleton connection is rejected with `ConnectionCreateError`.
* `T` must be a reference type (`where T : class`) — the pool tracks borrow state by object identity.
* Acquire-wait / create-latency percentiles are estimated over the last 1024 samples (bounded memory), not the full history.
* With `ValidateIdle` on, idle connections being pinged by the background sweep are briefly reserved and count as `InUse` (not `Idle`) in `Stats()` for the duration of that check.

## License

MIT — see [LICENSE](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE).
