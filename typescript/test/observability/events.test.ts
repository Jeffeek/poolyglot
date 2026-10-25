import { test } from "node:test";
import assert from "node:assert/strict";
import { ListenerGroup, type PoolListener } from "../../src/observability/events.js";
import { PooledConnection } from "../../src/core/connection.js";

test("dispatch calls each listener's hook", () => {
  const calls: Array<[unknown, number]> = [];
  const listener: PoolListener = {
    onAcquired(conn, waitTime) {
      calls.push([conn, waitTime]);
    },
  };
  const group = new ListenerGroup([listener]);
  const conn = new PooledConnection({}, 0);
  group.dispatch("onAcquired", conn, 1.5);
  assert.equal(calls.length, 1);
  assert.equal(calls[0]![0], conn);
  assert.equal(calls[0]![1], 1.5);
});

test("dispatch skips listeners missing the hook", () => {
  const withHook: PoolListener = { onAcquireTimeout: () => {} };
  const withoutHook: PoolListener = {};
  const group = new ListenerGroup([withoutHook, withHook]);
  // should not throw even though withoutHook lacks the hook
  assert.doesNotThrow(() => group.dispatch("onAcquireTimeout", 1.0));
});

test("dispatch swallows a thrown error and still calls later listeners", () => {
  const calls: string[] = [];
  const throwing: PoolListener = {
    onAcquireTimeout() {
      calls.push("throwing");
      throw new Error("boom");
    },
  };
  const after: PoolListener = {
    onAcquireTimeout() {
      calls.push("after");
    },
  };
  const group = new ListenerGroup([throwing, after]);
  assert.doesNotThrow(() => group.dispatch("onAcquireTimeout", 2.0));
  assert.deepEqual(calls, ["throwing", "after"]);
});
