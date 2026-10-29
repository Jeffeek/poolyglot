# connpool

[![connpool](https://raw.githubusercontent.com/Jeffeek/poolyglot/main/docs/assets/package_icon.png)](https://github.com/Jeffeek/poolyglot#readme)

**Connection pooling for Go — blocking &amp; context-based, dependency-free.**

The Go member of [**poolyglot**](https://github.com/Jeffeek/poolyglot#readme) · *Pool once. Speak everywhere.*

[![go CI](https://github.com/Jeffeek/poolyglot/actions/workflows/go.yml/badge.svg)](https://github.com/Jeffeek/poolyglot/actions/workflows/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Jeffeek/poolyglot/go.svg)](https://pkg.go.dev/github.com/Jeffeek/poolyglot/go)
[![coverage](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?flag=go&logo=codecov&label=coverage)](https://app.codecov.io/gh/Jeffeek/poolyglot?flags%5B0%5D=go)
[![Go 1.21](https://img.shields.io/badge/go-1.21%2B-00ADD8?logo=go&logoColor=white)](#install)
[![license](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE)

---

A generic connection pool for Go — blocking (goroutines) and context-based
(`context.Context` cancellation). **Dependency-free** (Go 1.21, standard library only). It
pools *anything*: `database/sql` handles, HTTP clients, SSH sessions, sockets… because the
pool knows nothing about connections except what a small `ConnectionFactory[T]` tells it.

```go
import connpool "github.com/Jeffeek/poolyglot/go"

pool, err := connpool.NewConnectionPool[*MyConn](
    connpool.FuncFactory[*MyConn]{
        CreateFn:   func() (*MyConn, error) { return dial() },     // enforce your own connect timeout
        ValidateFn: func(c *MyConn) bool { return c.Ping() == nil }, // cheap liveness check
    },
    func() connpool.PoolConfig { c := connpool.DefaultConfig(); c.MaxSize = 20; return c }(),
    nil,
)
if err != nil {
    log.Fatal(err)
}
pool.Open()                                   // optional: warm-up + background sweeper
defer pool.Close(nil)

err = pool.Use(connpool.Unset(), func(c *MyConn) error {   // acquire -> use -> release (errors handled)
    return c.DoSomething()
})
```

This is the Go member of [**poolyglot**](https://github.com/Jeffeek/poolyglot#readme): one pooling model, implemented per language,
kept API-parallel. Field names, event names, defaults, and `CloseReason`/`BreakerState` wire values
are identical to the Python (`connpool`), C# (`ConnPool`) and Java (`connpool`) implementations —
switching languages is a syntax change, not a model change.

## Install

```bash
go get github.com/Jeffeek/poolyglot/go@latest
```

```go
import connpool "github.com/Jeffeek/poolyglot/go"
```

The package is named `connpool`; because the module directory is `go/`, use the named import
above. Requires Go 1.21 or newer. Zero runtime dependencies.

## Project layout

``` layout
*.go                        the library (dependency-free, one package `connpool`)
  Core:          errors.go, clock.go, connection.go (PooledConnection/CloseReason), timeouts.go (Timeout)
  Resilience:    breaker.go (CircuitBreaker + BreakerState/BreakerConfig), retry.go (RetryPolicy)
  Policy:        config.go (PoolConfig), lifecycle.go (pure "when must a connection retire?" rules)
  Factories:     factory_sync.go, factory_async.go  <- what YOU implement
  Observability: events.go (PoolListener), stats.go (PoolStats)
  Pools:         sync_pool.go, async_pool.go, registry.go, cond.go
*_test.go                   mirrors the sources (go test; 100% statement gate)
examples/                   client code - NOT part of the module
```

Dependency direction is strictly downward (a layer only uses layers listed above it):
`Core → Resilience → Policy → Factories/Observability → Pools`. Everything lives in the single
`connpool` package, so one import brings in the whole public API.

📐 See the [**architecture diagrams**](https://github.com/Jeffeek/poolyglot/blob/main/docs/architecture.md)
— layering, connection lifecycle, and circuit-breaker states (shared across all poolyglot languages).

## What you get

| Concern | How it works |
| --- | --- |
| Sizing | `MinSize` kept warm, `MaxSize` is a *hard* cap (slot reserved before create) |
| Timeouts | `AcquireTimeout` (per-call override via `Timeout`), `CreateTimeout` (async), retries never sleep past the deadline |
| Rotation | `MaxLifetime` (with jitter), `MaxUses`, `MaxIdleTime`, LIFO/FIFO reuse |
| Health | validate on checkout if idle ≥ `ValidateAfterIdle`; `Reset()` on release; after an error: fatal → discard, otherwise re-validate; optional `ValidateIdle` pings idle connections in the background sweep and reaps dead ones |
| Retries | exponential backoff + jitter, `RetryOn` filter |
| Circuit breaker | closed → open → half-open, probe limits, state-change events; open = fail fast. Counts **one failure per logical creation** (a whole retry sequence), not per attempt |
| Backpressure | `MaxWaiters` caps the blocked-caller queue; excess callers fail fast with `*PoolExhaustedError` |
| Leak detection | `LeakDetectionTimeout` warns (via `log` + `OnConnectionLeak`) when a borrowed connection is held too long; warn-only, never force-reclaimed |
| Observability | `PoolListener` hooks + `pool.Stats()` snapshot (counters, **avg + p50/p95/p99** for both acquire-wait and create-latency over a recent window) |
| Shutdown | idempotent `Close()`, wakes blocked callers, in-use connections close on release; `Close(timeout)` drains gracefully |
| Cancellation | async cancellation (via `context.Context`) never leaks a slot |
| Many backends | `PoolRegistry` / `AsyncPoolRegistry` = one lazily-created pool per key (host, tenant, DB) |

## Writing a factory

Implement `ConnectionFactory[T]` (or build one from functions with `FuncFactory[T]`):

```go
type MyFactory struct{}

func (MyFactory) Create() (*MyConn, error)  { return dial() }              // enforce your own connect timeout
func (MyFactory) Validate(c *MyConn) bool    { return c.Ping() == nil }    // cheap liveness check
func (MyFactory) Reset(c *MyConn) error      { return c.Rollback() }       // scrub session state on release
func (MyFactory) Close(c *MyConn) error      { return c.Disconnect() }     // default: Close() if it implements io.Closer
func (MyFactory) IsFatalError(err error) bool { return errors.Is(err, io.EOF) }
```

Async: implement `AsyncConnectionFactory[T]` (every method takes a `context.Context`), or build one
from functions with `AsyncFuncFactory[T]`.

## Borrowing a connection

```go
// callback form (error-aware: fatal error / failed validation -> discard, else reuse)
err := pool.Use(connpool.Unset(), func(c *MyConn) error { return c.DoWork() })

// manual
c, err := pool.Acquire(connpool.Of(5.0))
if err != nil { /* ... */ }
defer pool.Release(c)
c.DoWork()
```

The `Timeout` argument is one of: `connpool.Unset()` → use `AcquireTimeout`; `connpool.Infinite()`
(or `connpool.OfNullable(nil)`) → wait forever; `connpool.Of(seconds)` → wait that long (`0` = don't
wait). The async pool mirrors this with `Acquire(ctx, …)`, `Use(ctx, …)` and `Release(…)`.

## Configuration (`PoolConfig`)

`PoolConfig` is a value struct — start from `connpool.DefaultConfig()`, set fields, and the pool
constructor validates it (returning a `*PoolConfigError`). Optional seconds / counts are `*float64` /
`*int`; use `connpool.Ptr(v)` to set one, or leave `nil` for "no limit".

| Field | Default | Meaning |
| --- | --- | --- |
| `MinSize` / `MaxSize` | 0 / 10 | warm connections / hard cap |
| `AcquireTimeout` | 30 | seconds to wait (`nil` = forever, `0` = don't wait) |
| `MaxWaiters` | nil | backpressure: cap callers blocked in `Acquire()` (excess → `*PoolExhaustedError`; `nil` = unlimited, `0` = never wait when full) |
| `CreateTimeout` | 10 | async only; sync factories enforce their own |
| `MaxLifetime` / `LifetimeJitter` | 1800 / 0.1 | retire after age, shaved by up to 10% |
| `MaxIdleTime` | 600 | evict idle connections (never below `MinSize`) |
| `MaxUses` | nil | retire after N checkouts |
| `IdleStrategy` | LIFO | `FIFO` spreads load evenly |
| `ValidateAfterIdle` | 30 | validate on checkout if idle this long (0 = always, nil = never) |
| `ValidateIdle` | false | also validate idle connections during the background sweep and reap dead ones (needs `Open()`; adds I/O) |
| `MaintenanceInterval` | 30 | background sweep period (nil = off) |
| `LeakDetectionTimeout` | nil | warn when a borrowed connection is held longer than this (needs `Open()`; checked once per sweep; nil = off) |
| `Retry` | 3 attempts, 0.1s base, ×2, 5s cap | `RetryPolicy` (`connpool.NoRetry` disables) |
| `Breaker` | 5 failures / 30s recovery | `*BreakerConfig` (`nil` disables); `FailureThreshold` counts failed *creations*, not retry attempts |

## Build, test, run examples

```bash
go build ./...             # the library
go test ./...              # run all tests
bash run_tests.sh          # tests under coverage (gate: 100% statements)
bash run_tests.sh -m       # ...also list any uncovered statements
(cd examples && go run .)  # runnable demo (separate module)
bash lint.sh               # golangci-lint (gate) + gofmt report
bash lint.sh -f            # ...auto-fix (gofmt -w + golangci-lint --fix)
```

Linting uses **golangci-lint v2** (`.golangci.yml`: govet, staticcheck, ineffassign, unused, errcheck),
gated; `gofmt` is warn-only. CI runs these as a separate `lint` job via `golangci/golangci-lint-action`.

## Limits / extension points

* The breaker guards connection *creation*. To also trip on request failures, wrap calls with `CircuitBreaker.Call()`.
* The breaker counts consecutive failed creations (one per logical acquire, retries included). A real backend failure or a `CreateTimeout` firing both count; an acquire-timeout (caller impatience) or context cancellation mid-create does not.
* Leak detection is **warn-only** by design and only runs while the pool is `Open()` (the maintenance sweep drives it), so registry pools need an explicit `Open()`, and a warning can lag by up to one `MaintenanceInterval`.
* **Not fork-safe / not process-shared.** `MaxSize` is per-process; create a fresh pool per process.
* Pool size is fixed at construction; there is no runtime resize. Use a new pool, or a `PoolRegistry` keyed by profile.
* Wake-up order of blocked callers is not strictly FIFO (the condition broadcasts on every change).
* A blocking sync `Create()` can't be interrupted by the pool; give your client a connect timeout. For the async pool, `CreateTimeout` is only enforced if your factory honors the passed `context.Context`.
* Listener hooks are synchronous; push heavy work to a queue.
* A factory's `Create()` must return a **distinct** object each call (`T` is pooled by value identity; use a pointer/interface type); returning a shared/singleton connection is rejected with `*ConnectionCreateError`.
* `T` is constrained to `comparable` because the pool tracks borrow state by the connection value in a map.
* Acquire-wait / create-latency percentiles are estimated over the last 1024 samples (bounded memory), not the full history.
* With `ValidateIdle` on, idle connections being pinged by the background sweep are briefly reserved and count as `InUse` (not `Idle`) in `Stats()` for the duration of that check.
* Go's cover tool measures *statement* coverage (the gate is 100% statements), the nearest equivalent to the other languages' line + branch gate.

## When to use (and when not to)

**Use a connection pool when:**
* Creating connections is expensive (≥10ms network handshake, TLS negotiation, auth)
* You make many requests over a connection's lifetime (reuse justifies overhead)
* Backends impose connection limits (DB `max_connections`, API rate limits)
* You need connection health checks or automatic retries

**Don't pool when:**
* Connection creation is cheap (<1ms). Example: in-memory SQLite, local Unix domain sockets
* Connections are single-use. Example: SMTP without keep-alive, one-shot exec.Command pipes
* You have ≤1 concurrent goroutine (no contention, just `create-and-close`)
* Short-lived Lambda/Cloud Run functions — pool setup cost > function lifetime; use the runtime's persistent connection cache instead

**Use `PoolRegistry` (multiple pools) when:**
* **Per-host HTTP client** — `PoolRegistry` keyed by hostname avoids head-of-line blocking (one slow host won't starve requests to fast hosts)

  ```go
  hosts := connpool.NewPoolRegistry(
      func(host string) connpool.ConnectionFactory[*net.Conn] {
          return &TcpFactory{host: host}
      },
      connpool.PoolConfig{MaxSize: connpool.Ptr(8)})
  defer hosts.Close()
  conn, release, _ := hosts.Connection(ctx, "api.example.com")
  defer release(conn, false, nil)
  ```

* **Multi-tenant isolation** — separate `MaxSize` per tenant for quota enforcement

  ```go
  tenants := connpool.NewPoolRegistry(
      func(tenantID string) connpool.ConnectionFactory[*sql.DB] {
          return &DbFactory{tenantID: tenantID}
      },
      connpool.PoolConfig{MaxSize: connpool.Ptr(10)})
  ```

* **Priority bulkheading** — interactive traffic gets a large pool, batch gets a small one
* **Regional backends** — one pool per datacenter (`us-east-1`, `eu-west-1`) for failover

**Common anti-patterns:**

| Anti-pattern | Why it's wrong | Fix |
| --- | --- | --- |
| **One global pool for all backends** | Cross-backend contention: slow DB queries block HTTP API calls | Use `PoolRegistry` keyed by backend type/host |
| **Pool in a serverless function** | Pool setup (MinSize warm connections) > function cold-start time | Use `http.Client` with `Transport` (has built-in pooling) or `sql.DB` (also pools) |
| **Undersized `MaxSize`** | 16 goroutines but `MaxSize=4` → 12 goroutines always wait | Set `MaxSize` ≥ peak concurrency |
| **No `AcquireTimeout`** | Infinite wait (default 30s is finite, but `nil` = forever) masks backend outages | Set finite timeout (5-30s) and fail fast |
| **Pooling stateful connections without `Reset()`** | Transaction/session state bleeds between requests | Implement `factory.Reset()` (rollback, clear session vars) |
| **Pooling `sql.DB`** | `sql.DB` already has internal connection pooling | Use `sql.DB` directly, don't wrap it in poolyglot |

## PoolRegistry patterns

### Pattern 1: Per-host TCP keep-alive

```go
package main

import (
 "context"
 "net"
 "time"
 "github.com/Jeffeek/poolyglot/go"
)

type TcpFactory struct {
 host string
 port string
}

func (f *TcpFactory) Create(ctx context.Context) (net.Conn, error) {
 conn, err := net.DialTimeout("tcp", f.host+":"+f.port, 5*time.Second)
 return conn, err  // fail in Create(), not in caller's request
}

func (f *TcpFactory) Validate(ctx context.Context, conn net.Conn) bool {
 return conn != nil  // could do a quick read/write test
}

func (f *TcpFactory) Close(ctx context.Context, conn net.Conn) error {
 return conn.Close()
}

func (f *TcpFactory) IsFatalError(err error) bool {
 _, ok := err.(net.Error)
 return ok
}

func main() {
 hosts := connpool.NewPoolRegistry(
    func(key string) connpool.ConnectionFactory[net.Conn] {
        // key format: "host:port"
        return &TcpFactory{host: "api.github.com", port: "443"}
    },
  connpool.PoolConfig{
    MaxSize:           connpool.Ptr(8),
    MaxLifetime:       connpool.Ptr(300.0),
    ValidateAfterIdle: connpool.Ptr(5.0),
  })
  defer hosts.Close()

  conn, release, err := hosts.Connection(context.Background(), "api.github.com:443")
  if err != nil {
    panic(err)
  }
  defer release(conn, false, nil)
  // ... write request, read response ...
}
```

### Pattern 2: Multi-tenant isolation

```go
// Different pool size per tier
func factoryForTenant(tenantID string) connpool.ConnectionFactory[*sql.DB] {
    tier := getTenantTier(tenantID)  // "free" | "pro" | "enterprise"
var config connpool.PoolConfig
 switch tier {
 case "free":
  config = connpool.PoolConfig{MaxSize: connpool.Ptr(2), MaxWaiters: connpool.Ptr(5)}
 case "pro":
  config = connpool.PoolConfig{MaxSize: connpool.Ptr(10), MaxWaiters: connpool.Ptr(20)}
 case "enterprise":
  config = connpool.PoolConfig{MaxSize: connpool.Ptr(50), MaxWaiters: connpool.Ptr(100)}
 }
 pool := connpool.NewConnectionPool(&DbFactory{tenantID: tenantID}, config)
 pool.SetName("tenant-" + tenantID)
 return pool
}

tenants := connpool.NewPoolRegistry(factoryForTenant)
defer tenants.Close()

conn, release, err := tenants.Connection(ctx, request.TenantID)
if err != nil {
 // handle error
}
defer release(conn, false, nil)
// ... execute queries ...
```

### Pattern 3: Regional failover

```go
primary := connpool.NewPoolRegistry(
 func(region string) connpool.ConnectionFactory[*sql.DB] {
  return &DbFactory{region: region}
 },
 connpool.PoolConfig{
  MaxSize: connpool.Ptr(20),
  Breaker: &connpool.BreakerConfig{FailureThreshold: 5},
 })
defer primary.Close()

conn, release, err := primary.Connection(ctx, "us-east-1")
if err != nil {
 // Circuit breaker open or pool exhausted
 conn, release, err = primary.Connection(ctx, "us-west-2")  // fallback region
 if err != nil {
  // handle error
 }
}
defer release(conn, false, nil)
// ... execute queries ...
```

Use `OnBreakerStateChange` listener to alert when primary region's breaker opens.

## License

MIT — see [LICENSE](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE).
