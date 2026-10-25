import { test } from "node:test";
import assert from "node:assert/strict";
import { AsyncConnectionFactory, AsyncCallableFactory } from "../../src/factories/async-factory.js";

class Conn {
  closed = false;
  close(): void {
    this.closed = true;
  }
}

test("AsyncConnectionFactory base class defaults", async () => {
  class Minimal extends AsyncConnectionFactory<Conn> {
    create(): Conn {
      return new Conn();
    }
  }

  const f = new Minimal();
  const c = f.create();
  assert.equal(await f.validate(c), true);
  assert.equal(await f.reset(c), undefined);
  assert.equal(f.isFatalError(new Error("boom")), false);
  await f.close(c);
  assert.equal(c.closed, true);
});

test("AsyncConnectionFactory base close without close method", async () => {
  class Minimal extends AsyncConnectionFactory<object> {
    create(): object {
      return {};
    }
  }

  await assert.doesNotReject(Minimal.prototype.close.call(new Minimal(), {}));
});

test("AsyncCallableFactory all callbacks, sync and async", async () => {
  const resets: Conn[] = [];
  const areset = async (c: Conn) => {
    resets.push(c);
  };

  const f = new AsyncCallableFactory<Conn>(() => new Conn(), {
    validate: (_c) => true,
    reset: areset,
    isFatalError: (e) => e instanceof RangeError,
  });

  const c = await f.create();
  assert.equal(await f.validate(c), true);
  await f.reset(c);
  assert.deepEqual(resets, [c]);
  assert.equal(f.isFatalError(new RangeError()), true);
  assert.equal(f.isFatalError(new Error()), false);
});

test("AsyncCallableFactory defaults when callbacks omitted", async () => {
  const f = new AsyncCallableFactory<Conn>(() => new Conn());
  const c = await f.create();
  assert.equal(await f.validate(c), true);
  await f.reset(c);
  assert.equal(f.isFatalError(new Error("boom")), false);
  await f.close(c);
  assert.equal(c.closed, true);
});

test("AsyncCallableFactory create supports async callable", async () => {
  const f = new AsyncCallableFactory<Conn>(async () => new Conn());
  const c = await f.create();
  assert.ok(c instanceof Conn);
});

test("AsyncCallableFactory validate coerces truthy/falsy", async () => {
  const truthy = new AsyncCallableFactory<Conn>(() => new Conn(), { validate: () => 1 });
  assert.equal(await truthy.validate(new Conn()), true);

  const falsy = new AsyncCallableFactory<Conn>(() => new Conn(), { validate: () => 0 });
  assert.equal(await falsy.validate(new Conn()), false);
});

test("AsyncCallableFactory close uses provided close instead of falling back", async () => {
  let calledWith: Conn | undefined;
  const f = new AsyncCallableFactory<Conn>(() => new Conn(), {
    close: (c) => {
      calledWith = c;
    },
  });
  const c = await f.create();
  await f.close(c);
  assert.equal(calledWith, c);
  assert.equal(c.closed, false); // provided close did NOT call conn.close()
});
