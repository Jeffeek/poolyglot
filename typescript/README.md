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
npm run build              # emit dist/ — ESM + CJS (tsup); .d.ts (tsc) + .d.cts (mirrored)
npm run typecheck          # tsc --noEmit over src + test
bash run_tests.sh          # tests under coverage (gate: 100% line + branch)
bash run_tests.sh -m       # ...also list any uncovered lines per file
npx tsx examples/main.ts   # runnable demo
npm run lint               # biome lint (gate)      — or: bash lint.sh
npm run lint:fix           # biome lint --write + reformat  — or: bash lint.sh -f
```

Linting & formatting use **Biome** (`biome.json`), not ESLint: `typescript-eslint` does not support
TypeScript 7 (what this package targets), while Biome has no dependency on the `typescript` package.
`tsc` still does the type-checking (`npm run typecheck`). `npm run lint` gates on Biome's errors;
`biome format` (`npm run format:check`) is warn-only. CI runs these as a separate `lint` job.

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

## When to use (and when not to)

**Use a connection pool when:**

* Creating connections is expensive (≥10ms network handshake, TLS negotiation, auth)
* You make many requests over a connection's lifetime (reuse justifies overhead)
* Backends impose connection limits (DB `max_connections`, API rate limits)
* You need connection health checks or automatic retries

**Don't pool when:**

* Connection creation is cheap (<1ms). Example: in-memory SQLite, local Unix domain sockets
* Connections are single-use. Example: SMTP without keep-alive, one-shot child processes
* You have ≤1 concurrent request (no contention, just `create-and-close`)
* Serverless functions (AWS Lambda, Vercel) — pool setup cost > function lifetime; use the runtime's persistent connection cache instead

**Use `AsyncPoolRegistry` (multiple pools) when:**

* **Per-host HTTP keep-alive** — `AsyncPoolRegistry` keyed by hostname avoids head-of-line blocking (one slow host won't starve requests to fast hosts)

  ```typescript
  const hosts = new AsyncPoolRegistry(
      (host: string) => new HttpFactory(host),
      { maxSize: 8 });
  const conn = await hosts.connection("api.example.com");
  try { /* use conn */ } finally { conn.release(); }
  ```

* **Multi-tenant isolation** — separate `maxSize` per tenant for quota enforcement

  ```typescript
  const tenants = new AsyncPoolRegistry(
      (tenantId: string) => new DbFactory(tenantId),
      { maxSize: 10 });
  ```

* **Priority bulkheading** — interactive traffic gets a large pool, batch gets a small one
* **Regional backends** — one pool per datacenter (`us-east-1`, `eu-west-1`) for failover

**Common anti-patterns:**

| Anti-pattern | Why it's wrong | Fix |
| --- | --- | --- |
| **One global pool for all backends** | Cross-backend contention: slow DB queries block HTTP API calls | Use `AsyncPoolRegistry` keyed by backend type/host |
| **Pool in a serverless function** | Pool setup (minSize warm connections) > function cold-start time | Use `node-postgres` `Pool` (has fast startup) or managed pooler (RDS Proxy, Supabase pooler) |
| **Undersized `maxSize`** | High concurrency but `maxSize=4` → many requests always wait | Set `maxSize` ≥ peak concurrency |
| **No `acquireTimeout`** | Infinite wait (default 30s is finite, but `null` = forever) masks backend outages | Set finite timeout (5-30s) and fail fast |
| **Pooling stateful connections without `reset()`** | Transaction/session state bleeds between requests | Implement `factory.reset()` (rollback, clear session vars) |
| **Pooling `axios` or `node-postgres` Pool** | They already have internal connection pooling | Use them directly, don't wrap in poolyglot |

## AsyncPoolRegistry patterns

### Pattern 1: Per-host HTTP keep-alive

```typescript
import { AsyncConnectionFactory, AsyncPoolRegistry, PoolConfig } from '@jeffeek/connpool';
import http from 'node:http';

class HttpFactory implements AsyncConnectionFactory<http.ClientRequest> {
  constructor(private host: string) {}

  async create(): Promise<http.ClientRequest> {
    return new Promise((resolve, reject) => {
      const req = http.request({ host: this.host, method: 'GET', path: '/' });
      req.on('socket', (socket) => {
        if (socket.connecting) {
          socket.on('connect', () => resolve(req));
        } else {
          resolve(req);
        }
      });
      req.on('error', reject);
      setTimeout(() => reject(new Error('connect timeout')), 5000);
    });
  }

  async validate(conn: http.ClientRequest): Promise<boolean> {
    return conn.socket?.writable === true;
  }

  async close(conn: http.ClientRequest): Promise<void> {
    conn.destroy();
  }

  isFatalError(exc: Error): boolean {
    return exc.message.includes('ECONNREFUSED') || exc.message.includes('timeout');
  }
}

const hosts = new AsyncPoolRegistry(
  (host: string) => new HttpFactory(host),
  { maxSize: 8, maxLifetime: 300, validateAfterIdle: 5 }
);

const conn = await hosts.connection("api.github.com");
try {
  // ... write request, read response ...
} finally {
  conn.release();
}
```

### Pattern 2: Multi-tenant isolation

```typescript
// Different pool size per tier
function factoryForTenant(tenantId: string): AsyncConnectionPool<DbConnection> {
  const tier = getTenantTier(tenantId);  // "free" | "pro" | "enterprise"
  const config: PoolConfig = 
    tier === "free" ? { maxSize: 2, maxWaiters: 5 } :
    tier === "pro" ? { maxSize: 10, maxWaiters: 20 } :
    { maxSize: 50, maxWaiters: 100 };
  
  return new AsyncConnectionPool(new DbFactory(tenantId), config, `tenant-${tenantId}`);
}

const tenants = new AsyncPoolRegistry(factoryForTenant);

const conn = await tenants.connection(request.tenantId);
try {
  // ... execute queries ...
} finally {
  conn.release();
}
```

### Pattern 3: Regional failover

```typescript
const primary = new AsyncPoolRegistry(
  (region: string) => new DbFactory(region),
  { 
    maxSize: 20, 
    breaker: { failureThreshold: 5, recoveryTimeout: 30 }
  }
);

try {
  const conn = await primary.connection("us-east-1");
  try {
    await conn.value.execute("SELECT ...");
  } finally {
    conn.release();
  }
} catch (err) {
  // Circuit breaker open or pool exhausted
  const conn = await primary.connection("us-west-2");  // fallback region
  try {
    await conn.value.execute("SELECT ...");
  } finally {
    conn.release();
  }
}
```

Use `onBreakerStateChange` listener to alert when primary region's breaker opens.

## License

MIT — see [LICENSE](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE).
