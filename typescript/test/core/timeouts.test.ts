import { test } from "node:test";
import assert from "node:assert/strict";
import { UNSET, computeDeadline, remaining } from "../../src/core/timeouts.js";

test("computeDeadline with UNSET uses the configured default", () => {
  assert.equal(computeDeadline(UNSET, 5, 100), 105);
});

test("computeDeadline with null default means wait forever", () => {
  assert.equal(computeDeadline(null, 5, 100), null);
});

test("computeDeadline with UNSET and null default also waits forever", () => {
  assert.equal(computeDeadline(UNSET, null, 100), null);
});

test("computeDeadline with an explicit number", () => {
  assert.equal(computeDeadline(3, 5, 100), 103);
});

test("remaining with no deadline", () => {
  assert.equal(remaining(null, 0), null);
});

test("remaining with a deadline", () => {
  assert.equal(remaining(110, 100), 10);
});
