import { test } from "node:test";
import assert from "node:assert/strict";
import {
  PoolError,
  PoolConfigError,
  PoolClosedError,
  PoolTimeoutError,
  PoolExhaustedError,
  ConnectionCreateError,
  CircuitOpenError,
  InvalidReleaseError,
} from "../../src/core/errors.js";

test("PoolError is an Error with name PoolError", () => {
  const e = new PoolError("boom");
  assert.ok(e instanceof Error);
  assert.ok(e instanceof PoolError);
  assert.equal(e.name, "PoolError");
  assert.equal(e.message, "boom");
});

test("PoolConfigError", () => {
  const e = new PoolConfigError("bad config");
  assert.ok(e instanceof PoolError);
  assert.equal(e.name, "PoolConfigError");
  assert.equal(e.message, "bad config");
});

test("PoolClosedError", () => {
  const e = new PoolClosedError("closed");
  assert.ok(e instanceof PoolError);
  assert.equal(e.name, "PoolClosedError");
});

test("PoolTimeoutError", () => {
  const e = new PoolTimeoutError("timeout");
  assert.ok(e instanceof PoolError);
  assert.equal(e.name, "PoolTimeoutError");
});

test("PoolExhaustedError", () => {
  const e = new PoolExhaustedError("exhausted");
  assert.ok(e instanceof PoolError);
  assert.equal(e.name, "PoolExhaustedError");
});

test("ConnectionCreateError carries cause", () => {
  const cause = new Error("backend down");
  const e = new ConnectionCreateError("create failed", { cause });
  assert.ok(e instanceof PoolError);
  assert.equal(e.name, "ConnectionCreateError");
  assert.equal(e.cause, cause);
});

test("CircuitOpenError computes retryAfter and message", () => {
  const e = new CircuitOpenError(2.5);
  assert.ok(e instanceof PoolError);
  assert.equal(e.name, "CircuitOpenError");
  assert.equal(e.retryAfter, 2.5);
  assert.ok(e.message.includes("~2.5s"));
});

test("InvalidReleaseError", () => {
  const e = new InvalidReleaseError("not checked out");
  assert.ok(e instanceof PoolError);
  assert.equal(e.name, "InvalidReleaseError");
});
