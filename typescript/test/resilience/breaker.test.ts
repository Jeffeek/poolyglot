import { test } from "node:test";
import assert from "node:assert/strict";
import { BreakerConfig, BreakerState, CircuitBreaker } from "../../src/resilience/breaker.js";
import { CircuitOpenError, PoolConfigError } from "../../src/core/errors.js";
import { ManualClock } from "../../src/core/clock.js";

function make(overrides: ConstructorParameters<typeof BreakerConfig>[0] = {}) {
  const clock = new ManualClock();
  const changes: [BreakerState, BreakerState][] = [];
  const cfg = new BreakerConfig({ failureThreshold: 3, recoveryTimeout: 10, ...overrides });
  const breaker = new CircuitBreaker(cfg, {
    clock,
    onStateChange: (o, n) => changes.push([o, n]),
  });
  return { breaker, clock, changes };
}

function fail(b: CircuitBreaker, n = 1): void {
  for (let i = 0; i < n; i++) {
    b.beforeCall();
    b.recordFailure();
  }
}

test("opens after consecutive failures", () => {
  const { breaker } = make();
  fail(breaker, 3);
  assert.equal(breaker.state, BreakerState.OPEN);
  let caught: CircuitOpenError | undefined;
  try {
    breaker.beforeCall();
  } catch (e) {
    caught = e as CircuitOpenError;
  }
  assert.ok(caught instanceof CircuitOpenError);
  assert.ok(Math.abs(caught!.retryAfter - 10) < 0.01);
});

test("success resets failure streak", () => {
  const { breaker } = make();
  fail(breaker, 2);
  breaker.beforeCall();
  breaker.recordSuccess();
  fail(breaker, 2);
  assert.equal(breaker.state, BreakerState.CLOSED);
});

test("half-open then closes on success", () => {
  const { breaker, clock, changes } = make();
  fail(breaker, 3);
  clock.advance(10);
  assert.equal(breaker.state, BreakerState.HALF_OPEN);
  breaker.beforeCall();
  breaker.recordSuccess();
  assert.equal(breaker.state, BreakerState.CLOSED);
  assert.deepEqual(
    changes.map(([, n]) => n),
    ["open", "half_open", "closed"],
  );
});

test("half-open failure reopens", () => {
  const { breaker, clock } = make();
  fail(breaker, 3);
  clock.advance(10);
  breaker.beforeCall();
  breaker.recordFailure();
  assert.equal(breaker.state, BreakerState.OPEN);
});

test("half-open limits concurrent probes", () => {
  const { breaker, clock } = make({ halfOpenMaxCalls: 1 });
  fail(breaker, 3);
  clock.advance(10);
  breaker.beforeCall();
  assert.throws(() => breaker.beforeCall(), CircuitOpenError);
});

test("success threshold requires multiple probe successes", () => {
  const { breaker, clock } = make({ successThreshold: 2, halfOpenMaxCalls: 2 });
  fail(breaker, 3);
  clock.advance(10);
  breaker.beforeCall();
  breaker.recordSuccess();
  assert.equal(breaker.state, BreakerState.HALF_OPEN);
  breaker.beforeCall();
  breaker.recordSuccess();
  assert.equal(breaker.state, BreakerState.CLOSED);
});

test("callback may reenter breaker without deadlock", () => {
  const clock = new ManualClock();
  const breaker: CircuitBreaker = new CircuitBreaker(new BreakerConfig({ failureThreshold: 1 }), {
    clock,
    onStateChange: () => breaker.state,
  });
  breaker.beforeCall();
  breaker.recordFailure();
  assert.equal(breaker.state, BreakerState.OPEN);
});

test("record abort frees probe slot without counting", () => {
  const { breaker, clock } = make({ halfOpenMaxCalls: 1 });
  fail(breaker, 3);
  clock.advance(10);
  breaker.beforeCall();
  breaker.recordAbort();
  assert.equal(breaker.state, BreakerState.HALF_OPEN);
  breaker.beforeCall();
  breaker.recordSuccess();
  assert.equal(breaker.state, BreakerState.CLOSED);
});

test("abort in closed state is a no-op", () => {
  const { breaker } = make();
  breaker.recordAbort();
  assert.equal(breaker.state, BreakerState.CLOSED);
});

test("reset when already closed is a no-op", () => {
  const { breaker, changes } = make();
  breaker.reset();
  assert.equal(breaker.state, BreakerState.CLOSED);
  assert.deepEqual(changes, []);
});

test("reset from open transitions to closed and emits", () => {
  const { breaker, changes } = make();
  fail(breaker, 3);
  assert.equal(breaker.state, BreakerState.OPEN);
  breaker.reset();
  assert.equal(breaker.state, BreakerState.CLOSED);
  assert.deepEqual(
    changes.map(([, n]) => n),
    ["open", "closed"],
  );
});

test("late failure while open is ignored", () => {
  const { breaker, clock } = make();
  fail(breaker, 3);
  assert.equal(breaker.state, BreakerState.OPEN);
  // A call started before tripping reports failure after the breaker already opened.
  breaker.recordFailure();
  assert.equal(breaker.state, BreakerState.OPEN);
  // retryAfter still counts down from the original openedAt.
  clock.advance(10);
  assert.equal(breaker.state, BreakerState.HALF_OPEN);
});

test("invalid config rejects non-positive thresholds and negative recovery timeout", () => {
  assert.throws(() => new BreakerConfig({ failureThreshold: 0 }), PoolConfigError);
  assert.throws(() => new BreakerConfig({ recoveryTimeout: -1 }), PoolConfigError);
  assert.throws(() => new BreakerConfig({ halfOpenMaxCalls: 0 }), PoolConfigError);
  assert.throws(() => new BreakerConfig({ successThreshold: 0 }), PoolConfigError);
});

test("default config values", () => {
  const cfg = new BreakerConfig();
  assert.equal(cfg.failureThreshold, 5);
  assert.equal(cfg.recoveryTimeout, 30.0);
  assert.equal(cfg.halfOpenMaxCalls, 1);
  assert.equal(cfg.successThreshold, 1);
});

test("default CircuitBreaker uses SystemClock and no listener", () => {
  const breaker = new CircuitBreaker();
  assert.equal(breaker.state, BreakerState.CLOSED);
  breaker.beforeCall();
  breaker.recordSuccess();
  assert.equal(breaker.state, BreakerState.CLOSED);
});
