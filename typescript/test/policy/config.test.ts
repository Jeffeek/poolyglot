import { test } from "node:test";
import assert from "node:assert/strict";
import { PoolConfig } from "../../src/policy/config.js";
import { PoolConfigError } from "../../src/core/errors.js";

test("defaults are valid", () => {
  const cfg = new PoolConfig();
  assert.equal(cfg.minSize, 0);
  assert.equal(cfg.maxSize, 10);
  assert.equal(cfg.acquireTimeout, 30.0);
  assert.equal(cfg.createTimeout, 10.0);
  assert.equal(cfg.maxWaiters, null);
  assert.equal(cfg.maxLifetime, 1800.0);
  assert.equal(cfg.lifetimeJitter, 0.1);
  assert.equal(cfg.maxIdleTime, 600.0);
  assert.equal(cfg.maxUses, null);
  assert.equal(cfg.idleStrategy, "lifo");
  assert.equal(cfg.validateAfterIdle, 30.0);
  assert.equal(cfg.maintenanceInterval, 30.0);
  assert.equal(cfg.leakDetectionTimeout, null);
  assert.equal(cfg.validateIdle, false);
  assert.ok(cfg.retry);
  assert.ok(cfg.breaker);
});

test("rejects bad values", () => {
  const badOptions: Array<Record<string, unknown>> = [
    { maxSize: 0 },
    { minSize: 11 },
    { minSize: -1 },
    { acquireTimeout: -1 },
    { maxLifetime: 0 },
    { lifetimeJitter: 1.5 },
    { maxUses: 0 },
    { validateAfterIdle: -1 },
    { maintenanceInterval: 0 },
    { leakDetectionTimeout: 0 },
    { leakDetectionTimeout: -5 },
  ];
  for (const bad of badOptions) {
    assert.throws(() => new PoolConfig(bad), PoolConfigError, JSON.stringify(bad));
  }
});

test("validation messages match each rule", () => {
  assert.throws(() => new PoolConfig({ maxSize: 0 }), /maxSize must be >= 1/);
  assert.throws(() => new PoolConfig({ minSize: 11 }), /minSize must be within \[0, maxSize\]/);
  assert.throws(() => new PoolConfig({ acquireTimeout: -1 }), /acquireTimeout must be >= 0/);
  assert.throws(() => new PoolConfig({ createTimeout: 0 }), /createTimeout must be > 0 or null/);
  assert.throws(() => new PoolConfig({ maxLifetime: 0 }), /maxLifetime must be > 0 or null/);
  assert.throws(() => new PoolConfig({ maxIdleTime: 0 }), /maxIdleTime must be > 0 or null/);
  assert.throws(
    () => new PoolConfig({ maintenanceInterval: 0 }),
    /maintenanceInterval must be > 0 or null/,
  );
  assert.throws(
    () => new PoolConfig({ leakDetectionTimeout: 0 }),
    /leakDetectionTimeout must be > 0 or null/,
  );
  assert.throws(
    () => new PoolConfig({ validateAfterIdle: -1 }),
    /validateAfterIdle must be >= 0 or null/,
  );
  assert.throws(
    () => new PoolConfig({ lifetimeJitter: 1.5 }),
    /lifetimeJitter must be within \[0, 1\]/,
  );
  assert.throws(() => new PoolConfig({ maxUses: 0 }), /maxUses must be >= 1 or null/);
  assert.throws(() => new PoolConfig({ maxWaiters: -1 }), /maxWaiters must be >= 0 or null/);
});

test("accepts null for optional limits", () => {
  const cfg = new PoolConfig({
    acquireTimeout: null,
    createTimeout: null,
    maxWaiters: null,
    maxLifetime: null,
    maxIdleTime: null,
    maxUses: null,
    validateAfterIdle: null,
    maintenanceInterval: null,
    leakDetectionTimeout: null,
  });
  assert.equal(cfg.acquireTimeout, null);
  assert.equal(cfg.maxLifetime, null);
  assert.equal(cfg.leakDetectionTimeout, null);
});

test("replace returns validated copy without mutating the original", () => {
  const cfg = new PoolConfig({ maxSize: 5 });
  assert.equal(cfg.replace({ maxSize: 8 }).maxSize, 8);
  assert.equal(cfg.maxSize, 5);
  assert.throws(() => cfg.replace({ minSize: 99 }), PoolConfigError);
});

test("replace with null clears a previously-set limit and re-validates", () => {
  const cfg = new PoolConfig({ maxLifetime: 500 });
  const replaced = cfg.replace({ maxLifetime: null });
  assert.equal(replaced.maxLifetime, null);
  assert.equal(cfg.maxLifetime, 500);
});
