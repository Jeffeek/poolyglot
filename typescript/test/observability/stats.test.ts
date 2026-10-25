import { test } from "node:test";
import assert from "node:assert/strict";
import { ACQUIRE_WAIT_WINDOW, StatsCollector, percentile } from "../../src/observability/stats.js";
import { PooledConnection, CloseReason } from "../../src/core/connection.js";

const gauges = (overrides: Partial<Record<string, unknown>> = {}) => ({
  name: "x",
  maxSize: 1,
  size: 0,
  idle: 0,
  inUse: 0,
  waiting: 0,
  breakerState: null,
  ...overrides,
});

test("percentile of empty array is 0", () => {
  assert.equal(percentile([], 0.5), 0);
});

test("percentile known quantiles with linear interpolation", () => {
  const vals = Array.from({ length: 100 }, (_, i) => i + 1); // 1..100, sorted
  assert.equal(percentile(vals, 0.0), 1.0);
  assert.equal(percentile(vals, 1.0), 100.0);
  assert.equal(percentile(vals, 0.5), 50.5);
  assert.ok(Math.abs(percentile(vals, 0.99) - 99.01) < 1e-9);
});

test("acquire wait percentiles in snapshot", () => {
  const c = new StatsCollector();
  const conn = new PooledConnection({}, 0);
  for (const w of [0.1, 0.2, 0.3, 0.4, 100.0]) {
    c.onAcquired(conn, w);
  }
  const s = c.snapshot(gauges({ maxSize: 1 }));
  assert.ok(Math.abs(s.avgAcquireWait - 20.2) < 1e-9);
  assert.ok(s.p50AcquireWait < 1.0);
  assert.ok(s.p99AcquireWait > 50.0);
});

test("create latency percentiles in snapshot", () => {
  const c = new StatsCollector();
  const conn = new PooledConnection({}, 0);
  for (const d of [0.01, 0.02, 0.03, 0.04, 5.0]) {
    c.onConnectionCreated(conn, d);
  }
  const s = c.snapshot(gauges({ maxSize: 1 }));
  assert.ok(Math.abs(s.avgCreateLatency - 5.1 / 5) < 1e-9);
  assert.ok(s.p50CreateLatency < 1.0);
  assert.ok(s.p99CreateLatency > 1.0);
});

test("wait window is bounded", () => {
  const c = new StatsCollector();
  const conn = new PooledConnection({}, 0);
  for (let i = 0; i < ACQUIRE_WAIT_WINDOW + 500; i++) {
    c.onAcquired(conn, 1.0);
  }
  const s = c.snapshot(gauges({ maxSize: 1 }));
  // every sample is 1.0, so avg/percentiles over the bounded window are all 1.0
  assert.equal(s.avgAcquireWait, 1.0);
  assert.equal(s.p50AcquireWait, 1.0);
  assert.equal(s.p99AcquireWait, 1.0);
});

test("snapshot as dictionary has snake_case keys", () => {
  const c = new StatsCollector();
  const s = c.snapshot(gauges({ maxSize: 2, size: 1, idle: 1, inUse: 0, waiting: 0 }));
  const d = s.toDictionary();
  assert.equal(d.name, "x");
  assert.equal(d.max_size, 2);
  assert.ok("p99_acquire_wait" in d);
});

test("StatsCollector counters via hooks and closed_by_reason keyed by wire string", () => {
  const c = new StatsCollector();
  const conn = new PooledConnection({}, 0);

  c.onConnectionCreated(conn, 0.5);
  c.onCreateFailed(new Error("nope"));
  c.onConnectionClosed(conn, CloseReason.EXPIRED);
  c.onConnectionClosed(conn, CloseReason.EXPIRED);
  c.onConnectionClosed(conn, CloseReason.IDLE);
  c.onAcquired(conn, 0.1);
  c.onReleased(conn, 0.2);
  c.onAcquireTimeout(1.0);
  c.onConnectionLeak(conn, 5.0);

  const s = c.snapshot(gauges({ maxSize: 3, size: 2, idle: 1, inUse: 1, waiting: 0, breakerState: "closed" }));

  assert.equal(s.created, 1);
  assert.equal(s.createFailures, 1);
  assert.equal(s.closed, 3);
  assert.equal(s.acquired, 1);
  assert.equal(s.released, 1);
  assert.equal(s.timeouts, 1);
  assert.equal(s.leaks, 1);
  assert.deepEqual(s.closedByReason, { expired: 2, idle: 1 });
  assert.equal(s.breakerState, "closed");

  const d = s.toDictionary();
  assert.deepEqual(d.closed_by_reason, { expired: 2, idle: 1 });
  assert.equal(d.breaker_state, "closed");
  assert.equal(d.created, 1);
  assert.equal(d.create_failures, 1);
  assert.equal(d.acquired, 1);
  assert.equal(d.released, 1);
  assert.equal(d.timeouts, 1);
  assert.equal(d.leaks, 1);
  assert.equal(d.size, 2);
  assert.equal(d.idle, 1);
  assert.equal(d.in_use, 1);
  assert.equal(d.waiting, 0);
});

test("empty snapshot has zero averages and percentiles", () => {
  const c = new StatsCollector();
  const s = c.snapshot(gauges());
  assert.equal(s.avgAcquireWait, 0);
  assert.equal(s.p50AcquireWait, 0);
  assert.equal(s.p95AcquireWait, 0);
  assert.equal(s.p99AcquireWait, 0);
  assert.equal(s.avgCreateLatency, 0);
  assert.equal(s.p50CreateLatency, 0);
  assert.equal(s.p95CreateLatency, 0);
  assert.equal(s.p99CreateLatency, 0);
  assert.deepEqual(s.closedByReason, {});
});
