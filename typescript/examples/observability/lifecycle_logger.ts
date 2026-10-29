/**
 * A PoolListener that logs every connection lifecycle event.
 *
 *   npx tsx examples/observability/lifecycle_logger.ts
 *
 * Durations are seconds. Hooks run synchronously and their exceptions are swallowed by the pool.
 */
import {
  AsyncConnectionFactory,
  AsyncConnectionPool,
  BreakerConfig,
  CircuitOpenError,
  ConnectionCreateError,
  NO_RETRY,
  PoolConfig,
  PoolTimeoutError,
  type PoolListener,
} from "../../src/index.js";

const ms = (s: number): string => `${(s * 1000).toFixed(1)}ms`;

const logger: PoolListener = {
  onConnectionCreated: (c, d) => console.log(`  created   #${c.id} in ${ms(d)}`),
  onConnectionClosed: (c, reason) => console.log(`  closed    #${c.id} reason=${reason}`),
  onCreateFailed: (e) => console.log(`  createFailed: ${(e as Error).message}`),
  onAcquired: (c, w) => console.log(`  acquired  #${c.id} waited ${ms(w)} use=${c.useCount}`),
  onReleased: (c, h) => console.log(`  released  #${c.id} held ${ms(h)}`),
  onAcquireTimeout: (w) => console.log(`  acquireTimeout after ${ms(w)}`),
  onConnectionLeak: (c, h) => console.log(`  leak?     #${c.id} held ${ms(h)}`),
  onBreakerStateChange: (o, n) => console.log(`  breaker   ${o} -> ${n}`),
};

let failCreates = false;

class Factory extends AsyncConnectionFactory<{ ok: true }> {
  create(): { ok: true } {
    if (failCreates) throw new Error("backend down");
    return { ok: true };
  }
}

async function main(): Promise<void> {
  const pool = new AsyncConnectionPool(
    new Factory(),
    new PoolConfig({
      maxSize: 1,
      maxUses: 2,
      acquireTimeout: 0.05,
      retry: NO_RETRY,
      breaker: new BreakerConfig({ failureThreshold: 2, recoveryTimeout: 60 }),
    }),
    { listeners: [logger], name: "lifecycle" },
  );
  try {
    console.log("normal use (maxUses=2 rotates the connection):");
    for (let i = 0; i < 3; i++) await pool.withConnection(() => undefined);

    console.log("discard on release:");
    const c = await pool.acquire();
    await pool.release(c, { discard: true });

    console.log("acquire timeout (maxSize=1 and the only connection is held):");
    const held = await pool.acquire();
    await pool.acquire().catch((e) => {
      if (!(e instanceof PoolTimeoutError)) throw e;
    });
    await pool.release(held, { discard: true });

    console.log("create failures open the breaker:");
    failCreates = true;
    for (let i = 0; i < 3; i++) {
      await pool.acquire().catch((e) => {
        if (!(e instanceof ConnectionCreateError || e instanceof CircuitOpenError)) throw e;
        console.log(`  -> ${e.name}`);
      });
    }
  } finally {
    console.log("closing pool:");
    await pool.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
