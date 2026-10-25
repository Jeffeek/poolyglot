import { test } from "node:test";
import assert from "node:assert/strict";
import { PooledConnection } from "../../src/core/connection.js";

test("age, idle, and hold times", () => {
  const c = new PooledConnection(new Object(), 100.0);
  assert.equal(c.age(110.0), 10.0);
  assert.equal(c.idleTime(105.0), 5.0); // lastReturnedAt defaults to createdAt
  assert.equal(c.holdTime(110.0), 0.0); // not checked out yet

  c.markCheckedOut(120.0);
  assert.equal(c.useCount, 1);
  assert.equal(c.holdTime(123.0), 3.0);

  c.markReturned(130.0);
  assert.equal(c.idleTime(131.0), 1.0);
  assert.equal(c.checkedOutAt, null);
});

test("toString includes class name", () => {
  const c = new PooledConnection(new Object(), 0.0);
  assert.ok(c.toString().includes("PooledConnection"));
});

test("markCheckedOut resets leakReported", () => {
  const c = new PooledConnection(new Object(), 0.0);
  c.leakReported = true;
  c.markCheckedOut(1.0);
  assert.equal(c.leakReported, false);
});

test("constructor honors explicit expiresAt and lastReturnedAt", () => {
  const c = new PooledConnection(new Object(), 0.0, 50.0, 10.0);
  assert.equal(c.expiresAt, 50.0);
  assert.equal(c.lastReturnedAt, 10.0);
});

test("ids are unique across instances", () => {
  const a = new PooledConnection(new Object(), 0.0);
  const b = new PooledConnection(new Object(), 0.0);
  assert.notEqual(a.id, b.id);
});
