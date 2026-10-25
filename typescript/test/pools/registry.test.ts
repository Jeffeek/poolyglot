import { test } from "node:test";
import assert from "node:assert/strict";

import { AsyncPoolRegistry } from "../../src/pools/registry.js";
import { AsyncCallableFactory } from "../../src/factories/async-factory.js";
import { quietConfig } from "../support/helpers.js";

test("one pool per key", async () => {
  const made: string[] = [];
  const reg = new AsyncPoolRegistry<string, { key: string }>((key) => {
    made.push(key);
    return new AsyncCallableFactory(() => ({ key }));
  }, quietConfig());

  let a1: { key: string } | undefined;
  let a2: { key: string } | undefined;
  let b: { key: string } | undefined;
  await reg.withConnection("a", async (c) => {
    a1 = c;
  });
  await reg.withConnection("a", async (c) => {
    a2 = c;
  });
  await reg.withConnection("b", async (c) => {
    b = c;
  });
  assert.equal(a1, a2);
  assert.deepEqual([...made].sort(), ["a", "b"]);
  assert.equal(b!.key, "b");
  assert.ok(!reg.closed);

  await reg.close();
  assert.ok(reg.closed);
  assert.throws(() => reg.pool("a"), /registry is closed/);
});
