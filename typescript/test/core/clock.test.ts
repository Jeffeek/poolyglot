import { test } from "node:test";
import assert from "node:assert/strict";
import { SystemClock, ManualClock } from "../../src/core/clock.js";

test("SystemClock monotonic is non-decreasing across a sleep", async () => {
  const clock = new SystemClock();
  const before = clock.monotonic();
  await clock.asyncSleep(0.001);
  assert.ok(clock.monotonic() >= before);
});

test("SystemClock asyncSleep resolves", async () => {
  await new SystemClock().asyncSleep(0.001);
});

test("ManualClock time only moves when told", () => {
  const clock = new ManualClock(100.0);
  assert.equal(clock.monotonic(), 100.0);
  clock.advance(5);
  assert.equal(clock.monotonic(), 105.0);
});

test("ManualClock asyncSleep advances time", async () => {
  const clock = new ManualClock(0.0);
  await clock.asyncSleep(3);
  assert.equal(clock.monotonic(), 3.0);
});

test("ManualClock defaults start to 1000", () => {
  const clock = new ManualClock();
  assert.equal(clock.monotonic(), 1000.0);
});
