import { test } from "node:test";
import assert from "node:assert/strict";
import { NO_RETRY, RetryPolicy } from "../../src/resilience/retry.js";
import { PoolConfigError } from "../../src/core/errors.js";

test("invalid config rejects bad values", () => {
  assert.throws(() => new RetryPolicy({ maxAttempts: 0 }), PoolConfigError);
  assert.throws(() => new RetryPolicy({ baseDelay: -1 }), PoolConfigError);
  assert.throws(() => new RetryPolicy({ multiplier: 0.5 }), PoolConfigError);
  assert.throws(() => new RetryPolicy({ jitter: 2.0 }), PoolConfigError);
  assert.throws(() => new RetryPolicy({ jitter: -0.1 }), PoolConfigError);
  assert.throws(() => new RetryPolicy({ maxDelay: -1 }), PoolConfigError);
});

test("default config values", () => {
  const p = new RetryPolicy();
  assert.equal(p.maxAttempts, 3);
  assert.equal(p.baseDelay, 0.1);
  assert.equal(p.maxDelay, 5.0);
  assert.equal(p.multiplier, 2.0);
  assert.equal(p.jitter, 0.5);
  assert.equal(p.shouldRetry(new Error("x")), true);
});

test("backoff math with zero jitter (floor)", () => {
  const p = new RetryPolicy({ maxAttempts: 5, baseDelay: 1, maxDelay: 5, multiplier: 2, jitter: 0 });
  const rng = () => 0;
  assert.deepEqual([1, 2, 3, 4].map((n) => p.delayFor(n, rng)), [1, 2, 4, 5]);
});

test("backoff math with full jitter stays within bounds", () => {
  const full = new RetryPolicy({ baseDelay: 4, jitter: 1.0 });
  for (let i = 0; i < 100; i++) {
    const rng = () => Math.random();
    const d = full.delayFor(1, rng);
    assert.ok(d >= 0 && d <= 4);
  }
});

test("backoff with rng at ceiling (jitter=0 is unaffected by rng value)", () => {
  const p = new RetryPolicy({ maxAttempts: 5, baseDelay: 1, maxDelay: 5, multiplier: 2, jitter: 0 });
  const rng = () => 1;
  assert.deepEqual([1, 2, 3, 4].map((n) => p.delayFor(n, rng)), [1, 2, 4, 5]);
});

test("backoff with partial jitter interpolates between floor and ceiling", () => {
  const p = new RetryPolicy({ baseDelay: 4, jitter: 0.5 });
  // ceiling for failedAttempts=1 is min(5, 4*2^0) = 4
  // delay = 4 * (1 - 0.5 + 0.5 * rng())
  assert.equal(p.delayFor(1, () => 0), 4 * 0.5);
  assert.equal(p.delayFor(1, () => 1), 4 * 1.0);
});

test("retry-on filter narrows retryable errors", () => {
  class ConnectionError extends Error {}
  class ConnectionResetError extends ConnectionError {}
  const p = new RetryPolicy({ retryOn: (e) => e instanceof ConnectionError });
  assert.equal(p.shouldRetry(new ConnectionResetError()), true);
  assert.equal(p.shouldRetry(new Error("nope")), false);
});

test("NO_RETRY allows exactly one attempt", () => {
  assert.equal(NO_RETRY.maxAttempts, 1);
});

test("replace returns a new validated policy, keeping unset fields", () => {
  const p = new RetryPolicy({ maxAttempts: 4, baseDelay: 1, maxDelay: 5, multiplier: 2, jitter: 0.5 });
  const changed = p.replace({ maxAttempts: 10 });
  assert.notEqual(changed, p);
  assert.equal(changed.maxAttempts, 10);
  assert.equal(changed.baseDelay, 1);
  assert.equal(changed.maxDelay, 5);
  assert.equal(changed.multiplier, 2);
  assert.equal(changed.jitter, 0.5);
  assert.equal(changed.retryOn, p.retryOn);
});

test("replace re-validates and can throw", () => {
  const p = new RetryPolicy();
  assert.throws(() => p.replace({ maxAttempts: 0 }), PoolConfigError);
});

test("replace can override retryOn", () => {
  const p = new RetryPolicy();
  const customRetryOn = (e: unknown) => e instanceof TypeError;
  const changed = p.replace({ retryOn: customRetryOn });
  assert.equal(changed.retryOn, customRetryOn);
  assert.equal(changed.shouldRetry(new TypeError()), true);
  assert.equal(changed.shouldRetry(new Error()), false);
});
