import { test } from "node:test";
import assert from "node:assert/strict";
import { CloseReason, PooledConnection } from "../../src/core/connection.js";
import { PoolConfig, IdleStrategy } from "../../src/policy/config.js";
import {
  newConnection,
  popIdle,
  rotationReason,
  idleTooLong,
  needsValidation,
  borrowReason,
  sweep,
} from "../../src/policy/lifecycle.js";

function conn(opts: {
  created?: number;
  returned?: number | null;
  uses?: number;
  expires?: number | null;
} = {}): PooledConnection<object> {
  const { created = 0.0, returned = null, uses = 0, expires = null } = opts;
  const c = new PooledConnection(Object.create(null), created, expires, returned);
  c.useCount = uses;
  return c;
}

test("lifetime jitter stays in range", () => {
  const cfg = new PoolConfig({ maxLifetime: 100, lifetimeJitter: 0.1 });
  // deterministic rng cycling through a spread of [0,1) values
  let i = 0;
  const seq = [0, 0.25, 0.5, 0.75, 0.999, 0.1, 0.9];
  const rng = () => seq[i++ % seq.length]!;
  for (let n = 0; n < 200; n++) {
    const c = newConnection({}, cfg, 0.0, rng);
    assert.ok(c.expiresAt !== null && c.expiresAt >= 90 && c.expiresAt <= 100);
  }
});

test("newConnection with maxLifetime=null leaves expiresAt null", () => {
  const cfg = new PoolConfig({ maxLifetime: null });
  const c = newConnection({}, cfg, 5.0, () => 0.5);
  assert.equal(c.expiresAt, null);
});

test("newConnection expiry math is exact for a given rng value", () => {
  const cfg = new PoolConfig({ maxLifetime: 100, lifetimeJitter: 0.2 });
  const c = newConnection({}, cfg, 10.0, () => 0.5);
  // expiresAt = now + maxLifetime * (1 - jitter*rng) = 10 + 100*(1-0.1) = 100
  assert.equal(c.expiresAt, 100);
});

test("rotation reasons", () => {
  const cfg = new PoolConfig({ maxUses: 3 });
  assert.equal(rotationReason(conn({ expires: 5 }), cfg, 5), CloseReason.EXPIRED);
  assert.equal(rotationReason(conn({ uses: 3 }), cfg, 0), CloseReason.MAX_USES);
  assert.equal(rotationReason(conn({ uses: 2, expires: 9 }), cfg, 5), null);
});

test("idleTooLong", () => {
  const cfg = new PoolConfig({ maxIdleTime: 10 });
  const c = conn({ returned: 0 });
  assert.equal(idleTooLong(c, cfg, 9), false);
  assert.equal(idleTooLong(c, cfg, 10), true);
  assert.equal(idleTooLong(c, new PoolConfig({ maxIdleTime: null }), 1000), false);
});

test("needsValidation policy", () => {
  const c = conn({ returned: 0 });
  assert.equal(needsValidation(c, new PoolConfig({ validateAfterIdle: null }), 100), false);
  assert.equal(needsValidation(c, new PoolConfig({ validateAfterIdle: 0 }), 0), true);
  assert.equal(needsValidation(c, new PoolConfig({ validateAfterIdle: 30 }), 10), false);
  assert.equal(needsValidation(c, new PoolConfig({ validateAfterIdle: 30 }), 30), true);
});

test("borrowReason defers to rotation first, then idle", () => {
  const cfg = new PoolConfig({ maxUses: 3, maxIdleTime: 10 });
  assert.equal(borrowReason(conn({ expires: 5 }), cfg, 5), CloseReason.EXPIRED);
  assert.equal(borrowReason(conn({ uses: 3 }), cfg, 0), CloseReason.MAX_USES);
  assert.equal(borrowReason(conn({ returned: 0 }), cfg, 10), CloseReason.IDLE);
  assert.equal(borrowReason(conn({ returned: 0 }), cfg, 5), null);
});

test("sweep never goes below minSize for idle victims", () => {
  const cfg = new PoolConfig({ minSize: 2, maxIdleTime: 10 });
  const idle = [conn({ returned: 0 }), conn({ returned: 0 }), conn({ returned: 0 }), conn({ returned: 0 })];
  const { keep, doomed } = sweep(idle, 4, cfg, 11);
  assert.equal(keep.length, 2);
  assert.equal(doomed.length, 2);
  assert.ok(doomed.every(([, r]) => r === CloseReason.IDLE));
});

test("sweep always evicts expired regardless of minSize", () => {
  const cfg = new PoolConfig({ minSize: 2, maxIdleTime: null });
  const { keep, doomed } = sweep([conn({ expires: 1 }), conn()], 2, cfg, 5);
  assert.deepEqual(
    doomed.map(([, r]) => r),
    [CloseReason.EXPIRED],
  );
  assert.equal(keep.length, 1);
});

test("popIdle LIFO vs FIFO vs empty", () => {
  const a = conn();
  const b = conn();
  let arr = [a, b];
  assert.equal(popIdle(arr, IdleStrategy.LIFO), b);
  arr = [a, b];
  assert.equal(popIdle(arr, IdleStrategy.FIFO), a);
  assert.equal(popIdle([], IdleStrategy.LIFO), null);
});
