import { test } from "node:test";
import assert from "node:assert/strict";

import {
  AsyncCondition,
  CancelledError,
  abortError,
  awaitShielded,
} from "../../src/pools/async-primitives.js";
import { realSleep } from "../support/helpers.js";

const tick = (): Promise<void> => realSleep(0);

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

test("CancelledError carries a default and custom message", () => {
  assert.equal(new CancelledError().message, "operation cancelled");
  assert.equal(new CancelledError("boom").message, "boom");
});

test("abortError returns the reason, or a CancelledError when absent", () => {
  const reason = new Error("r");
  assert.equal(abortError({ reason } as unknown as AbortSignal), reason);
  assert.ok(abortError({ reason: undefined } as unknown as AbortSignal) instanceof CancelledError);
});

test("lock is exclusive and hands off to the next queued locker", async () => {
  const c = new AsyncCondition();
  await c.lock();
  let got = false;
  const second = c.lock().then(() => {
    got = true;
  });
  await tick();
  assert.ok(!got); // still held
  c.unlock();
  await second;
  assert.ok(got);
  c.unlock();
});

test("lock rejects immediately when the signal is already aborted", async () => {
  const c = new AsyncCondition();
  const ctrl = new AbortController();
  ctrl.abort();
  await assert.rejects(c.lock(ctrl.signal));
});

test("lock rejects when aborted while queued", async () => {
  const c = new AsyncCondition();
  await c.lock();
  const ctrl = new AbortController();
  const queued = c.lock(ctrl.signal);
  await tick();
  ctrl.abort();
  await assert.rejects(queued);
  c.unlock();
});

test("lock hands off to a queued signal-bearing locker when not aborted", async () => {
  const c = new AsyncCondition();
  await c.lock();
  const ctrl = new AbortController();
  const queued = c.lock(ctrl.signal);
  await tick();
  c.unlock(); // hands off to the queued locker (has a signal, not aborted)
  await queued;
  c.unlock();
});

test("wait returns false on timeout", async () => {
  const c = new AsyncCondition();
  await c.lock();
  assert.equal(await c.wait(0.01), false);
  c.unlock();
});

test("wait returns true when notified", async () => {
  const c = new AsyncCondition();
  await c.lock();
  const w = c.wait(null);
  await tick();
  await c.lock();
  c.notify();
  c.unlock();
  assert.equal(await w, true);
  c.unlock();
});

test("wait throws when aborted while waiting, and when pre-aborted", async () => {
  const c = new AsyncCondition();
  await c.lock();
  const ctrl = new AbortController();
  const w = c.wait(null, ctrl.signal);
  await tick();
  ctrl.abort();
  await assert.rejects(w);
  c.unlock();

  await c.lock();
  const pre = new AbortController();
  pre.abort();
  await assert.rejects(c.wait(null, pre.signal));
  c.unlock();
});

test("notify wakes exactly n waiters, notifyAll wakes the rest", async () => {
  const c = new AsyncCondition();
  await c.lock();
  const w1 = c.wait(null);
  await c.lock();
  const w2 = c.wait(null);

  await c.lock();
  c.notify(1); // wake only w1 -> exercises the n-reached break with w2 still queued
  c.unlock();
  assert.equal(await w1, true); // w1 reacquired the lock

  let w2done = false;
  void w2.then(() => {
    w2done = true;
  });
  await tick();
  assert.ok(!w2done);

  c.notifyAll(); // we hold the lock (from w1); wake the rest
  c.unlock();
  assert.equal(await w2, true);
  c.unlock();
});

test("awaitShielded passes through without a signal and when not aborted", async () => {
  await awaitShielded(Promise.resolve());
  const ctrl = new AbortController();
  await awaitShielded(Promise.resolve(), ctrl.signal);
});

test("awaitShielded propagates a rejection from the work", async () => {
  await assert.rejects(
    awaitShielded(Promise.reject(new Error("x")), new AbortController().signal),
    /x/,
  );
});

test("awaitShielded throws on a pre-aborted signal", async () => {
  const ctrl = new AbortController();
  ctrl.abort();
  await assert.rejects(awaitShielded(new Promise<void>(() => {}), ctrl.signal));
});

test("awaitShielded bails on abort but lets the work finish", async () => {
  const d = deferred();
  let finished = false;
  const work = d.promise.then(() => {
    finished = true;
  });
  const ctrl = new AbortController();
  const shielded = awaitShielded(work, ctrl.signal);
  ctrl.abort();
  await assert.rejects(shielded);
  assert.ok(!finished);
  d.resolve();
  await work;
  assert.ok(finished);
});
