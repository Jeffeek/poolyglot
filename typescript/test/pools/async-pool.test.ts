import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";

import { AsyncConnectionPool } from "../../src/pools/async-pool.js";
import { AsyncCallableFactory } from "../../src/factories/async-factory.js";
import { ManualClock } from "../../src/core/clock.js";
import { CloseReason } from "../../src/core/connection.js";
import { CancelledError } from "../../src/pools/async-primitives.js";
import { BreakerConfig } from "../../src/resilience/breaker.js";
import { RetryPolicy } from "../../src/resilience/retry.js";
import {
  CircuitOpenError,
  ConnectionCreateError,
  InvalidReleaseError,
  PoolClosedError,
  PoolExhaustedError,
  PoolTimeoutError,
} from "../../src/core/errors.js";
import {
  AsyncFakeFactory,
  ConnReset,
  FakeConn,
  FakeInner,
  Recorder,
  quietConfig,
  realSleep,
} from "../support/helpers.js";
import type { PoolConfigOptions } from "../../src/policy/config.js";

const delay = realSleep;

function deferred<T = void>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error?: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

interface Harness {
  pool: AsyncConnectionPool<FakeConn>;
  factory: AsyncFakeFactory;
  inner: FakeInner;
  rec: Recorder;
}

function make(
  t: TestContext,
  cfg: PoolConfigOptions = {},
  opts: { factory?: AsyncFakeFactory; clock?: ManualClock } = {},
): Harness {
  const factory = opts.factory ?? new AsyncFakeFactory();
  const inner = factory.inner;
  const rec = new Recorder();
  const poolOpts: { listeners: Recorder[]; clock?: ManualClock } = { listeners: [rec] };
  if (opts.clock) poolOpts.clock = opts.clock;
  const pool = new AsyncConnectionPool(factory, quietConfig(cfg), poolOpts);
  t.after(() => pool.close().catch(() => {}));
  return { pool, factory, inner, rec };
}

test("reuse and stats", async (t) => {
  const { pool } = make(t);
  let a: FakeConn | undefined;
  let b: FakeConn | undefined;
  await pool.withConnection(async (c) => {
    a = c;
  });
  await pool.withConnection(async (c) => {
    b = c;
  });
  assert.equal(a, b);
  const s = pool.stats();
  assert.deepEqual([s.created, s.acquired, s.idle, s.inUse], [1, 2, 1, 0]);
});

test("timeout when exhausted", async (t) => {
  const { pool } = make(t, { maxSize: 1 });
  const held = await pool.acquire();
  await assert.rejects(pool.acquire({ timeout: 0.05 }), PoolTimeoutError);
  assert.equal(pool.stats().timeouts, 1);
  await pool.release(held);
});

test("waiter woken by release", async (t) => {
  const { pool } = make(t, { maxSize: 1 });
  const held = await pool.acquire();
  const waiter = pool.acquire({ timeout: 2 });
  await delay(0.02);
  assert.equal(pool.stats().waiting, 1);
  await pool.release(held);
  assert.equal(await waiter, held);
});

test("double release rejected", async (t) => {
  const { pool } = make(t);
  const conn = await pool.acquire();
  await pool.release(conn);
  await assert.rejects(pool.release(conn), InvalidReleaseError);
});

test("rotation by lifetime", async (t) => {
  const clock = new ManualClock();
  const { pool, rec } = make(t, { maxLifetime: 10 }, { clock });
  const a = await pool.acquire();
  await pool.release(a);
  clock.advance(11);
  const b = await pool.acquire();
  assert.notEqual(a, b);
  assert.deepEqual(rec.closed, [CloseReason.EXPIRED]);
});

test("validation and error discard", async (t) => {
  const { pool, inner } = make(t, { validateAfterIdle: 0 });
  const a = await pool.acquire();
  await pool.release(a);
  a.healthy = false;
  const b = await pool.acquire();
  assert.notEqual(a, b);
  inner.fatalTypes = [ConnReset];
  let c: FakeConn | undefined;
  await assert.rejects(
    pool.withConnection(async (conn) => {
      c = conn;
      throw new ConnReset();
    }),
    ConnReset,
  );
  assert.ok(c!.closed);
});

test("retry and breaker count one failure per logical acquire", async (t) => {
  const clock = new ManualClock();
  const inner = new FakeInner();
  inner.failAlways = true;
  const { pool } = make(
    t,
    {
      acquireTimeout: 60,
      retry: new RetryPolicy({ maxAttempts: 2, baseDelay: 1, jitter: 0 }),
      breaker: new BreakerConfig({ failureThreshold: 2, recoveryTimeout: 30 }),
    },
    { factory: new AsyncFakeFactory(inner), clock },
  );
  await assert.rejects(pool.acquire(), ConnectionCreateError); // failure #1 -> still closed
  assert.equal(inner.attempts, 2);
  assert.equal(pool.stats().breakerState, "closed");
  await assert.rejects(pool.acquire(), ConnectionCreateError); // failure #2 -> trips breaker
  assert.equal(inner.attempts, 4);
  await assert.rejects(pool.acquire(), CircuitOpenError); // fails fast, no new attempts
  assert.equal(inner.attempts, 4);
  assert.equal(pool.stats().size, 0);
  clock.advance(31);
  inner.failAlways = false;
  await pool.release(await pool.acquire());
  assert.equal(pool.stats().breakerState, "closed");
});

test("create timeout is enforced", async (t) => {
  const { pool } = make(
    t,
    { createTimeout: 0.05 },
    { factory: new AsyncFakeFactory(new FakeInner(), 5) },
  );
  await assert.rejects(pool.acquire(), ConnectionCreateError);
  assert.equal(pool.stats().size, 0);
});

test("cancel while waiting does not leak", async (t) => {
  const { pool } = make(t, { maxSize: 1, acquireTimeout: null });
  const held = await pool.acquire();
  const ctrl = new AbortController();
  const waiter = pool.acquire({ signal: ctrl.signal });
  await delay(0.02);
  ctrl.abort();
  await assert.rejects(waiter);
  assert.equal(pool.stats().waiting, 0);
  await pool.release(held);
  assert.equal(await pool.acquire({ timeout: 1 }), held);
});

test("cancel during create frees slot", async (t) => {
  const { pool } = make(
    t,
    { maxSize: 1, createTimeout: null },
    { factory: new AsyncFakeFactory(new FakeInner(), 5) },
  );
  const ctrl = new AbortController();
  const task = pool.acquire({ signal: ctrl.signal });
  await delay(0.02);
  ctrl.abort();
  await assert.rejects(task);
  assert.equal(pool.stats().size, 0);
});

test("close wakes waiters", async (t) => {
  const { pool } = make(t, { maxSize: 1, acquireTimeout: null });
  const held = await pool.acquire();
  const waiter = pool.acquire();
  await delay(0.02);
  await pool.close();
  await assert.rejects(waiter, PoolClosedError);
  await pool.release(held);
  assert.ok(held.closed);
});

test("maintenance task warms up and context closes everything", async (t) => {
  const factory = new AsyncFakeFactory();
  const pool = new AsyncConnectionPool(
    factory,
    quietConfig({ minSize: 2, maintenanceInterval: 0.02 }),
  );
  t.after(() => pool.close().catch(() => {}));
  await pool.open();
  assert.equal(pool.stats().idle, 2);
  await delay(0.05); // let the maintenance loop complete at least one successful sweep
  assert.equal(pool.stats().idle, 2);
  await pool.close();
  assert.ok(pool.closed);
  assert.ok(factory.inner.created.every((c) => c.closed));
});

test("open racing close does not leak a maintenance loop", async (t) => {
  const proceed = deferred();
  let firstCreate = true;
  const factory = new AsyncCallableFactory<FakeConn>(
    async () => {
      if (firstCreate) {
        firstCreate = false;
        await proceed.promise;
      }
      return new FakeConn();
    },
    {
      close: (c) => {
        c.closed = true;
      },
    },
  );
  const pool = new AsyncConnectionPool(
    factory,
    quietConfig({ minSize: 1, maintenanceInterval: 0.01 }),
  );
  t.after(() => pool.close().catch(() => {}));
  const openTask = pool.open();
  await delay(0); // let open() reach the gate inside fillToMin
  await pool.close(); // runs fully; the maintenance loop is never started
  proceed.resolve();
  await openTask;
  assert.ok(pool.closed);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  assert.equal((pool as any).maintTask, null);
  await delay(0.03); // a maintenance interval would have ticked
  assert.equal(pool.stats().size, 0);
});

test("cancellation during disposal still completes", async (t) => {
  const started = deferred();
  const resume = deferred();
  const closed: FakeConn[] = [];
  const factory = new AsyncCallableFactory<FakeConn>(() => new FakeConn(), {
    close: async (c) => {
      started.resolve();
      await resume.promise;
      closed.push(c);
      c.closed = true;
    },
  });
  const rec = new Recorder();
  const pool = new AsyncConnectionPool(factory, quietConfig({ maxSize: 2 }), { listeners: [rec] });
  t.after(() => pool.close().catch(() => {}));

  const conn = await pool.acquire();
  const ctrl = new AbortController();
  const releasing = pool.release(conn, { discard: true, signal: ctrl.signal });
  await started.promise; // suspended inside factory.close()
  ctrl.abort();
  await assert.rejects(releasing); // caller observes cancellation promptly
  assert.deepEqual(closed, []); // disposal paused, not yet finished

  resume.resolve(); // let the shielded disposal complete
  for (let i = 0; i < 100; i++) {
    if (pool.stats().size === 0 && rec.closed.length > 0) break;
    await delay(0.001);
  }
  assert.deepEqual(closed, [conn]);
  assert.deepEqual(rec.closed, [CloseReason.DISCARDED]);
  const s = pool.stats();
  assert.deepEqual([s.size, s.closed], [0, 1]);
});

test("concurrency stress stays within max_size", async (t) => {
  const { pool } = make(t, { maxSize: 4, acquireTimeout: 10, maxUses: 9 });
  const state = { now: 0, peak: 0 };
  const worker = async (): Promise<void> => {
    for (let i = 0; i < 40; i++) {
      await pool.withConnection(async (conn) => {
        state.now += 1;
        state.peak = Math.max(state.peak, state.now);
        await Promise.resolve();
        assert.ok(!conn.closed);
        state.now -= 1;
      });
    }
  };
  await Promise.all(Array.from({ length: 25 }, () => worker()));
  const s = pool.stats();
  assert.ok(state.peak <= 4);
  assert.deepEqual([s.inUse, s.waiting, s.acquired], [0, 0, 25 * 40]);
  assert.equal(s.created - s.closed, s.idle);
});

test("fromCallables accepts sync and async", async (t) => {
  const closed: object[] = [];
  const pool = AsyncConnectionPool.fromCallables(async () => ({}), {
    close: (c) => void closed.push(c),
    config: quietConfig(),
  });
  t.after(() => pool.close().catch(() => {}));
  await pool.withConnection(async () => {});
  await pool.close();
  assert.equal(closed.length, 1);
});

test("duplicate connection from factory is rejected", async (t) => {
  const shared = {};
  const pool = AsyncConnectionPool.fromCallables(() => shared, { config: quietConfig() });
  t.after(() => pool.close().catch(() => {}));
  const first = await pool.acquire();
  await assert.rejects(pool.acquire(), ConnectionCreateError);
  assert.equal(pool.stats().size, 1); // duplicate slot freed
  assert.equal(pool.stats().created, 1); // duplicate not counted
  await pool.release(first);
});

test("validate_idle evicts dead connections", async (t) => {
  const { pool, rec } = make(t, { validateIdle: true, maxSize: 2 });
  const a = await pool.acquire();
  const b = await pool.acquire();
  await pool.release(a);
  await pool.release(b);
  assert.equal(pool.stats().idle, 2);
  a.healthy = false;
  await pool.runMaintenance();
  assert.equal(pool.stats().idle, 1);
  assert.ok(rec.closed.includes(CloseReason.UNHEALTHY));
  await pool.withConnection(async (c) => {
    assert.equal(c, b);
  });
});

test("leak detection warns once", async (t) => {
  const clock = new ManualClock();
  const { pool } = make(t, { leakDetectionTimeout: 10 }, { clock });
  const conn = await pool.acquire();
  clock.advance(11);
  await pool.runMaintenance();
  await pool.runMaintenance();
  assert.equal(pool.stats().leaks, 1);
  await pool.release(conn);
});

test("graceful drain waits for in-use", async (t) => {
  const { pool } = make(t, { maxSize: 2 });
  const held = await pool.acquire();
  const returner = (async () => {
    await delay(0.05);
    await pool.release(held);
  })();
  await pool.close({ timeout: 2 });
  await returner;
  assert.ok(held.closed);
  assert.equal(pool.stats().inUse, 0);
});

test("drain timeout gives up", async (t) => {
  const { pool } = make(t, { maxSize: 1 });
  const held = await pool.acquire();
  await pool.close({ timeout: 0.05 });
  assert.equal(pool.stats().inUse, 1);
  assert.ok(!held.closed);
  await pool.release(held);
  assert.ok(held.closed);
});

test("open is idempotent and reopening a closed pool throws", async (t) => {
  const { pool } = make(t, { maintenanceInterval: 0.5 });
  assert.equal(await pool.open(), await pool.open());
  await pool.close();
  await assert.rejects(pool.open(), PoolClosedError);
});

test("drain with zero timeout returns immediately", async (t) => {
  const { pool } = make(t, { maxSize: 1 });
  const held = await pool.acquire();
  await pool.close({ timeout: 0 });
  assert.equal(pool.stats().inUse, 1);
  await pool.release(held);
});

test("run_maintenance on a closed pool is a no-op", async (t) => {
  const { pool } = make(t);
  await pool.close();
  await pool.runMaintenance();
});

test("maintenance evicts idle", async (t) => {
  const clock = new ManualClock();
  const { pool, rec } = make(t, { maxIdleTime: 10 }, { clock });
  const a = await pool.acquire();
  await pool.release(a);
  clock.advance(11);
  await pool.runMaintenance();
  assert.equal(pool.stats().idle, 0);
  assert.deepEqual(rec.closed, [CloseReason.IDLE]);
});

test("max_waiters fails fast", async (t) => {
  const { pool } = make(t, { maxSize: 1, maxWaiters: 0, acquireTimeout: 5 });
  const held = await pool.acquire();
  await assert.rejects(pool.acquire(), PoolExhaustedError);
  await pool.release(held);
});

test("release discard and close error do not leak", async (t) => {
  const rec = new Recorder();
  const pool = new AsyncConnectionPool(
    new AsyncCallableFactory<FakeConn>(() => new FakeConn(), {
      close: () => {
        throw new Error("close boom");
      },
    }),
    quietConfig(),
    { listeners: [rec] },
  );
  t.after(() => pool.close().catch(() => {}));
  await pool.release(await pool.acquire(), { discard: true });
  assert.equal(pool.stats().size, 0);
  assert.deepEqual(rec.closed, [CloseReason.DISCARDED]);
});

test("release reset error becomes reset_failed", async (t) => {
  const rec = new Recorder();
  const pool = new AsyncConnectionPool(
    new AsyncCallableFactory<FakeConn>(() => new FakeConn(), {
      reset: () => {
        throw new Error("reset boom");
      },
    }),
    quietConfig(),
    { listeners: [rec] },
  );
  t.after(() => pool.close().catch(() => {}));
  await pool.release(await pool.acquire());
  assert.deepEqual(rec.closed, [CloseReason.RESET_FAILED]);
});

test("release reset cancellation destroys and propagates", async (t) => {
  const rec = new Recorder();
  const pool = new AsyncConnectionPool(
    new AsyncCallableFactory<FakeConn>(() => new FakeConn(), {
      reset: () => {
        throw new CancelledError();
      },
    }),
    quietConfig(),
    { listeners: [rec] },
  );
  t.after(() => pool.close().catch(() => {}));
  const a = await pool.acquire();
  await assert.rejects(pool.release(a), CancelledError);
  assert.deepEqual(rec.closed, [CloseReason.ERROR]);
});

test("checkout validation error is unhealthy", async (t) => {
  const rec = new Recorder();
  const pool = new AsyncConnectionPool(
    new AsyncCallableFactory<FakeConn>(() => new FakeConn(), {
      validate: () => {
        throw new Error("ping boom");
      },
    }),
    quietConfig({ validateAfterIdle: 0 }),
    { listeners: [rec] },
  );
  t.after(() => pool.close().catch(() => {}));
  const a = await pool.acquire();
  await pool.release(a);
  const b = await pool.acquire();
  assert.notEqual(a, b);
  assert.deepEqual(rec.closed, [CloseReason.UNHEALTHY]);
});

test("borrow validation cancellation is not swallowed", async (t) => {
  const rec = new Recorder();
  const pool = new AsyncConnectionPool(
    new AsyncCallableFactory<FakeConn>(() => new FakeConn(), {
      validate: () => {
        throw new CancelledError();
      },
    }),
    quietConfig({ validateAfterIdle: 0 }),
    { listeners: [rec] },
  );
  t.after(() => pool.close().catch(() => {}));
  const a = await pool.acquire();
  await pool.release(a);
  await assert.rejects(pool.acquire(), CancelledError);
  assert.deepEqual(rec.closed, [CloseReason.ERROR]);
});

test("create timeout during retry does not trip the breaker", async (t) => {
  const clock = new ManualClock();
  const inner = new FakeInner();
  inner.failAlways = true;
  const { pool } = make(
    t,
    {
      acquireTimeout: 1,
      retry: new RetryPolicy({ maxAttempts: 3, baseDelay: 10, jitter: 0 }),
      breaker: new BreakerConfig({ failureThreshold: 1 }),
    },
    { factory: new AsyncFakeFactory(inner), clock },
  );
  await assert.rejects(pool.acquire(), PoolTimeoutError);
  assert.equal(pool.stats().breakerState, "closed");
});

test("create timeout counts as a breaker failure", async (t) => {
  const { pool } = make(
    t,
    {
      createTimeout: 0.05,
      acquireTimeout: 5,
      breaker: new BreakerConfig({ failureThreshold: 1 }),
    },
    { factory: new AsyncFakeFactory(new FakeInner(), 5) },
  );
  await assert.rejects(pool.acquire(), ConnectionCreateError);
  assert.equal(pool.stats().breakerState, "open");
});

test("acquire deadline timeout does not trip the breaker", async (t) => {
  const { pool } = make(
    t,
    {
      createTimeout: null,
      acquireTimeout: 0.05,
      breaker: new BreakerConfig({ failureThreshold: 1 }),
    },
    { factory: new AsyncFakeFactory(new FakeInner(), 5) },
  );
  await assert.rejects(pool.acquire(), PoolTimeoutError);
  assert.equal(pool.stats().breakerState, "closed");
});

test("cancel while registering does not leak", async (t) => {
  const built = deferred();
  const proceed = deferred();
  const closes: FakeConn[] = [];
  const factory = new AsyncCallableFactory<FakeConn>(
    async () => {
      const conn = new FakeConn();
      built.resolve();
      await proceed.promise;
      return conn;
    },
    {
      close: (c) => {
        closes.push(c);
        c.closed = true;
      },
    },
  );
  const pool = new AsyncConnectionPool(factory, quietConfig({ maxSize: 2 }));
  t.after(() => pool.close().catch(() => {}));

  const ctrl = new AbortController();
  const task = pool.acquire({ signal: ctrl.signal });
  await built.promise; // parked inside create(); slot reserved
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const cond = (pool as any).cond;
  await cond.lock(); // force registerNew's lock to block
  proceed.resolve(); // create() returns -> task hits the contended lock
  await delay(0.01); // let the task queue on the lock
  ctrl.abort(); // cancel while it waits for the lock
  cond.unlock(); // let the shielded cleanup proceed

  await assert.rejects(task);
  for (let i = 0; i < 100; i++) {
    if (pool.stats().size === 0 && closes.length === 1) break;
    await delay(0.001);
  }
  assert.equal(closes.length, 1); // closed exactly once
  assert.equal(pool.stats().size, 0); // slot accounting restored
  assert.equal(pool.stats().idle, 0);
  assert.equal(pool.stats().created, 0); // never adopted
  await pool.withConnection(async (c) => {
    assert.ok(!c.closed); // pool still usable
  });
});

test("pool closed mid-retry surfaces PoolClosedError", async (t) => {
  let pool!: AsyncConnectionPool<FakeConn>;
  const factory = new AsyncCallableFactory<FakeConn>(async () => {
    await pool.close();
    throw new Error("down");
  });
  pool = new AsyncConnectionPool(
    factory,
    quietConfig({ retry: new RetryPolicy({ maxAttempts: 3, baseDelay: 0, jitter: 0 }) }),
  );
  t.after(() => pool.close().catch(() => {}));
  await assert.rejects(pool.acquire(), PoolClosedError);
});

test("connection created during close is destroyed", async (t) => {
  let pool!: AsyncConnectionPool<FakeConn>;
  const made: FakeConn[] = [];
  const factory = new AsyncCallableFactory<FakeConn>(
    async () => {
      const c = new FakeConn();
      made.push(c);
      await pool.close();
      return c;
    },
    {
      close: (c) => {
        c.closed = true;
      },
    },
  );
  pool = new AsyncConnectionPool(factory, quietConfig());
  t.after(() => pool.close().catch(() => {}));
  await assert.rejects(pool.acquire(), PoolClosedError);
  assert.ok(made.length > 0 && made[0]!.closed);
});

test("warmup failure is not fatal", async (t) => {
  const inner = new FakeInner();
  inner.failAlways = true;
  const { pool } = make(t, { minSize: 2 }, { factory: new AsyncFakeFactory(inner) });
  await pool.open();
  assert.equal(pool.stats().size, 0);
});

test("warmup connection destroyed if closed during create", async (t) => {
  let pool!: AsyncConnectionPool<FakeConn>;
  const made: FakeConn[] = [];
  const factory = new AsyncCallableFactory<FakeConn>(
    async () => {
      const c = new FakeConn();
      made.push(c);
      await pool.close();
      return c;
    },
    {
      close: (c) => {
        c.closed = true;
      },
    },
  );
  pool = new AsyncConnectionPool(factory, quietConfig({ minSize: 1 }));
  t.after(() => pool.close().catch(() => {}));
  await pool.open();
  assert.ok(made.length > 0 && made[0]!.closed);
});

test("maintenance loop runs and survives errors", async (t) => {
  const { pool } = make(t, { maintenanceInterval: 0.01 });
  const calls: number[] = [];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (pool as any).runMaintenance = async () => {
    calls.push(1);
    throw new Error("sweep broke");
  };
  await pool.open();
  while (calls.length === 0) {
    await delay(0.005);
  }
  await delay(0.02); // kept looping despite the error
  assert.ok(calls.length >= 1);
});

test("toString mentions sizes", async (t) => {
  const { pool } = make(t);
  assert.match(pool.toString(), /AsyncConnectionPool/);
});

test("constructs with default config when none is given", async (t) => {
  const pool = new AsyncConnectionPool(new AsyncFakeFactory());
  t.after(() => pool.close().catch(() => {}));
  assert.equal(pool.stats().maxSize, 10);
});

test("fromCallables forwards listeners, clock, rng and name", async (t) => {
  const rec = new Recorder();
  const clock = new ManualClock();
  const pool = AsyncConnectionPool.fromCallables(() => new FakeConn(), {
    config: quietConfig(),
    listeners: [rec],
    clock,
    rng: () => 0.5,
    name: "tenant",
  });
  t.after(() => pool.close().catch(() => {}));
  assert.equal(pool.name, "tenant");
  await pool.withConnection(async () => {});
  assert.ok(rec.events.includes("acquired"));
});

test("withConnection accepts an abort signal", async (t) => {
  const { pool } = make(t);
  const ctrl = new AbortController();
  let seen: FakeConn | undefined;
  await pool.withConnection(
    async (c) => {
      seen = c;
    },
    { signal: ctrl.signal },
  );
  assert.ok(seen);
  assert.equal(pool.stats().idle, 1);
});

test("acquire with no create/acquire timeout skips the timeout race", async (t) => {
  const { pool } = make(t, { createTimeout: null, acquireTimeout: null });
  const c = await pool.acquire();
  await pool.release(c);
  assert.equal(pool.stats().created, 1);
});

test("release with a non-fatal error keeps a healthy conn, discards an unhealthy one", async (t) => {
  const { pool, rec } = make(t, { maxSize: 2 });
  const a = await pool.acquire();
  await pool.release(a, { error: new Error("boom") }); // healthy -> validated -> kept
  assert.equal(pool.stats().idle, 1);
  assert.deepEqual(rec.closed, []);
  const b = await pool.acquire(); // reuses a
  b.healthy = false;
  await pool.release(b, { error: new Error("boom2") }); // unhealthy -> ERROR
  assert.deepEqual(rec.closed, [CloseReason.ERROR]);
});

test("validate_idle closes a healthy connection if the pool closes mid-check", async (t) => {
  const resume = deferred();
  const rec = new Recorder();
  const pool = new AsyncConnectionPool(
    new AsyncCallableFactory<FakeConn>(() => new FakeConn(), {
      validate: async () => {
        await resume.promise;
        return true;
      },
      close: (c) => {
        c.closed = true;
      },
    }),
    quietConfig({ validateIdle: true, maxSize: 1 }),
    { listeners: [rec] },
  );
  t.after(() => pool.close().catch(() => {}));
  const a = await pool.acquire();
  await pool.release(a);
  const maint = pool.runMaintenance(); // enters healthCheckIdle; validate is gated
  await delay(0.01);
  await pool.close(); // pool closes while validation is pending
  resume.resolve(); // validate resolves healthy -> keep = healthy && !closed = false
  await maint;
  assert.ok(rec.closed.includes(CloseReason.POOL_CLOSED));
});

test("abort during retry backoff cancels at the next create attempt", async (t) => {
  const inner = new FakeInner();
  inner.failTimes = 1; // first attempt fails, then a backoff sleep precedes the retry
  const pool = new AsyncConnectionPool(
    new AsyncFakeFactory(inner),
    quietConfig({
      acquireTimeout: null,
      retry: new RetryPolicy({ maxAttempts: 3, baseDelay: 0.05, jitter: 0 }),
    }),
  );
  t.after(() => pool.close().catch(() => {}));
  const ctrl = new AbortController();
  const task = pool.acquire({ signal: ctrl.signal });
  await delay(0.02); // first attempt failed; now inside the 0.05s backoff sleep
  ctrl.abort();
  await assert.rejects(task);
  assert.equal(pool.stats().size, 0);
});
