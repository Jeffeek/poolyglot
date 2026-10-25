# connpool

[![connpool](https://raw.githubusercontent.com/Jeffeek/poolyglot/main/docs/assets/package_icon.png)](https://github.com/Jeffeek/poolyglot#readme)

**Connection pooling for Node.js — async/`await`, `AbortSignal`-cancellable, dependency-free.**

The TypeScript member of [**poolyglot**](https://github.com/Jeffeek/poolyglot#readme) · *Pool once. Speak everywhere.*

[![typescript CI](https://github.com/Jeffeek/poolyglot/actions/workflows/typescript.yml/badge.svg)](https://github.com/Jeffeek/poolyglot/actions/workflows/typescript.yml)
[![npm](https://img.shields.io/npm/v/%40jeffeek%2Fconnpool?logo=npm&label=%40jeffeek%2Fconnpool)](https://www.npmjs.com/package/@jeffeek/connpool)
[![coverage](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?flag=typescript&logo=codecov&label=coverage)](https://app.codecov.io/gh/Jeffeek/poolyglot?flags%5B0%5D=typescript)
[![Node 22+](https://img.shields.io/badge/node-22%2B-5FA04E?logo=node.js&logoColor=white)](#install)
[![license](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE)

---

A generic async connection pool for Node.js, written in TypeScript. **Dependency-free** (ESM,
Node 22+). It pools *anything*: database drivers, HTTP clients, SSH sessions, sockets… because the
pool knows nothing about connections except what a small `AsyncConnectionFactory<T>` tells it.

```ts
import { AsyncConnectionPool, PoolConfig } from "@jeffeek/connpool";

const pool = AsyncConnectionPool.fromCallables<MyConn>(() => dial(), {
  validate: (c) => c.ping(),                    // cheap liveness check
  config: new PoolConfig({ maxSize: 20 }),
});

await pool.open();                              // optional: warm-up + background sweeper
try {
  const result = await pool.withConnection(async (conn) => {
    return conn.doSomething();                  // acquire -> use -> release (errors handled)
  });
} finally {
  await pool.close();
}
```

This is the TypeScript member of [**poolyglot**](https://github.com/Jeffeek/poolyglot#readme): one
pooling model, implemented per language, kept API-parallel. Field names, event names, defaults, and
`CloseReason`/`BreakerState` wire values are identical to the Python (`connpool`), C# (`ConnPool`),
Java (`connpool`) and Go (`connpool`) implementations — switching languages is a syntax change, not a
model change.

> **Async only.** Unlike the other poolyglot languages, there is **no synchronous pool**: Node's
> single event loop has no thread-blocking `acquire`. Use `AsyncConnectionPool`.

## Install

```bash
npm install @jeffeek/connpool
```

```ts
import { AsyncConnectionPool } from "@jeffeek/connpool";
```

Ships **both ESM and CommonJS** (`import` and `require` both work), with TypeScript types for each.
Requires Node 22 or newer; zero runtime dependencies. Works in Node (ESM or CJS), bundlers
(Vite/webpack/esbuild/Rollup), and frameworks like Angular/Next — the code uses only web-standard
globals (`AbortController`, `setTimeout`, `performance`), no Node built-ins.

```ts
// ESM / TypeScript
import { AsyncConnectionPool } from "@jeffeek/connpool";
```

```js
// CommonJS
const { AsyncConnectionPool } = require("@jeffeek/connpool");
```

## Project layout

``` layout
src/                         the library (dependency-free, ESM, one barrel: index.ts)
  core/          errors.ts, clock.ts, connection.ts (PooledConnection/CloseReason), timeouts.ts
  resilience/    breaker.ts (CircuitBreaker + BreakerState/BreakerConfig), retry.ts (RetryPolicy)
  policy/        config.ts (PoolConfig), lifecycle.ts (pure "when must a connection retire?" rules)
  factories/     async-factory.ts  <- what YOU implement
  observability/ events.ts (PoolListener), stats.ts (PoolStats)
  pools/         async-pool.ts, registry.ts, async-primitives.ts (AsyncCondition + shield)
test/                        mirrors src/ (node:test; 100% line + branch gate under c8)
examples/                    client code - NOT part of the package
```

Dependency direction is strictly downward (a layer only uses layers listed above it):
`core → resilience → policy → factories/observability → pools`. `index.ts` is the public-API
barrel, so one import brings in the whole public API.

📐 See the [**architecture diagrams**](https://github.com/Jeffeek/poolyglot/blob/main/docs/architecture.md)
— layering, connection lifecycle, and circuit-breaker states (shared across all poolyglot languages).

## What you get

| Concern | How it works |
| --- | --- |
| Sizing | `minSize` kept warm, `maxSize` is a *hard* cap (slot reserved before create) |
| Timeouts | `acquireTimeout` (per-call override), `createTimeout` (raced against a real timer), retries never sleep past the deadline |
| Rotation | `maxLifetime` (with jitter), `maxUses`, `maxIdleTime`, LIFO/FIFO reuse |
| Health | validate on checkout if idle ≥ `validateAfterIdle`; `reset()` on release; after an error: fatal → discard, otherwise re-validate; optional `validateIdle` pings idle connections in the background sweep and reaps dead ones |
| Retries | exponential backoff + jitter, `retryOn` filter |
| Circuit breaker | closed → open → half-open, probe limits, state-change events; open = fail fast. Counts **one failure per logical creation** (a whole retry sequence), not per attempt |
| Backpressure | `maxWaiters` caps the blocked-caller queue; excess callers fail fast with `PoolExhaustedError` |
| Leak detection | `leakDetectionTimeout` fires `onConnectionLeak` when a borrowed connection is held too long; warn-only, never force-reclaimed |
| Observability | `PoolListener` hooks + `pool.stats()` snapshot (counters, **avg + p50/p95/p99** for both acquire-wait and create-latency over a recent window) |
| Shutdown | idempotent `close()`, wakes blocked callers, in-use connections close on release; `close({ timeout })` drains gracefully |
| Cancellation | `AbortSignal` cancellation never leaks a slot |
| Many backends | `AsyncPoolRegistry` = one lazily-created pool per key (host, tenant, DB) |

## Writing a factory

Extend `AsyncConnectionFactory<T>` (only `create` is required), or build one from callables with
`AsyncCallableFactory<T>` / `AsyncConnectionPool.fromCallables`:

```ts
import { AsyncConnectionFactory } from "@jeffeek/connpool";

class MyFactory extends AsyncConnectionFactory<MyConn> {
  create()               { return dial(); }                 // required; enforce your own connect timeout
  override validate(c: MyConn) { return c.ping(); }         // cheap liveness check (sync or async)
  override reset(c: MyConn)    { return c.rollback(); }     // scrub session state on release
  override close(c: MyConn)    { return c.disconnect(); }   // default: calls c.close() if present
  override isFatalError(e: unknown) { return e instanceof ResetError; }
}
```

Every method may be sync or async — the pool awaits the result either way.

## Borrowing a connection

```ts
// callback form (error-aware: fatal error / failed validation -> discard, else reuse)
const result = await pool.withConnection(async (c) => c.doWork());

// manual
const c = await pool.acquire({ timeout: 5 });
try {
  await c.doWork();
} finally {
  await pool.release(c);
}
```

The `timeout` option is one of: omitted → use `acquireTimeout`; `null` → wait forever; a number of
seconds (`0` = don't wait). Pass an `AbortSignal` as `{ signal }` to cancel an in-flight acquire; a
cancelled acquire never leaks a slot.

## Configuration (`PoolConfig`)

`PoolConfig` is immutable and validated in its constructor (throwing `PoolConfigError`); use
`config.replace({ ... })` for a re-validated copy. Optional seconds / counts are `number | null`
(`null` = no limit).

| Field | Default | Meaning |
| --- | --- | --- |
| `minSize` / `maxSize` | 0 / 10 | warm connections / hard cap |
| `acquireTimeout` | 30 | seconds to wait (`null` = forever, `0` = don't wait) |
| `maxWaiters` | null | backpressure: cap callers blocked in `acquire()` (excess → `PoolExhaustedError`; `null` = unlimited, `0` = never wait when full) |
| `createTimeout` | 10 | cap on a single `factory.create()` (raced against a real timer) |
| `maxLifetime` / `lifetimeJitter` | 1800 / 0.1 | retire after age, shaved by up to 10% |
| `maxIdleTime` | 600 | evict idle connections (never below `minSize`) |
| `maxUses` | null | retire after N checkouts |
| `idleStrategy` | `"lifo"` | `"fifo"` spreads load evenly |
| `validateAfterIdle` | 30 | validate on checkout if idle this long (0 = always, null = never) |
| `validateIdle` | false | also validate idle connections during the background sweep and reap dead ones (needs `open()`; adds I/O) |
| `maintenanceInterval` | 30 | background sweep period (null = off) |
| `leakDetectionTimeout` | null | warn when a borrowed connection is held longer than this (needs `open()`; checked once per sweep; null = off) |
| `retry` | 3 attempts, 0.1s base, ×2, 5s cap | `RetryPolicy` (`NO_RETRY` disables retries) |
| `breaker` | 5 failures / 30s recovery | `BreakerConfig` (`null` disables); `failureThreshold` counts failed *creations*, not retry attempts |

## Build, test, run examples

```bash
npm install                # dev toolchain (tsx, c8, typescript, @types/node)
npm run build              # emit dist/ — ESM (index.js) + CJS (index.cjs) + .d.ts/.d.cts (tsup)
npm run typecheck          # tsc --noEmit over src + test
bash run_tests.sh          # tests under coverage (gate: 100% line + branch)
bash run_tests.sh -m       # ...also list any uncovered lines per file
npx tsx examples/main.ts   # runnable demo
```

## Limits / extension points

* **No synchronous pool.** Node has no thread-blocking `acquire`, so the sync/async "twin" of the other
  poolyglot languages does not map; only `AsyncConnectionPool` is provided.
* The breaker guards connection *creation*. It counts consecutive failed creations (one per logical
  acquire, retries included). A real backend failure or a `createTimeout` firing both count; an
  acquire-timeout (caller impatience) or an `AbortSignal` cancellation mid-create does not.
* Leak detection is **warn-only** by design and only runs while the pool is `open()` (the maintenance
  sweep drives it), so registry pools need an explicit `open()`, and a warning can lag by up to one
  `maintenanceInterval`.
* Pool size is fixed at construction; there is no runtime resize. Use a new pool, or an
  `AsyncPoolRegistry` keyed by profile.
* Wake-up order of blocked callers is not strictly FIFO.
* `createTimeout` is enforced by racing `factory.create()` against a timer; the underlying work is not
  forcibly aborted (JavaScript cannot interrupt arbitrary user code) — it is abandoned, and a
  cancelled/timed-out create never leaks a slot.
* Listener hooks are synchronous; push heavy work to a queue. Exceptions thrown by listeners are
  swallowed so a broken exporter can never break the pool.
* A factory's `create()` must return a **distinct** object each call (`T` is pooled by object
  identity); returning a shared/singleton connection is rejected with `ConnectionCreateError`.
* `T` is constrained to `object` because the pool tracks borrow state by connection identity.
* Acquire-wait / create-latency percentiles are estimated over the last 1024 samples (bounded memory),
  not the full history.
* With `validateIdle` on, idle connections being pinged by the background sweep are briefly reserved and
  count as `inUse` (not `idle`) in `stats()` for the duration of that check.

## License

MIT — see [LICENSE](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE).
