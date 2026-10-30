/**
 * One pool per tenant via AsyncPoolRegistry: a noisy tenant exhausts only its own pool.
 *
 *   npx tsx examples/registry/multi_tenant.ts
 */
import {
  AsyncCallableFactory,
  AsyncPoolRegistry,
  PoolConfig,
  PoolTimeoutError,
} from "../../src/index.js";

interface TenantConn {
  readonly tenant: string;
  readonly id: number;
}

let nextId = 1;

async function main(): Promise<void> {
  const registry = new AsyncPoolRegistry<string, TenantConn>(
    (tenant) => new AsyncCallableFactory(() => ({ tenant, id: nextId++ })),
    new PoolConfig({ maxSize: 2, acquireTimeout: 0.05 }),
  );
  try {
    // Tenant "acme" checks out its entire quota (2 connections) and keeps them.
    const acme = registry.pool("acme");
    const held = [await acme.acquire(), await acme.acquire()];

    try {
      await acme.acquire();
    } catch (err) {
      if (!(err instanceof PoolTimeoutError)) throw err;
      console.log("acme: third acquire timed out (tenant quota reached)");
    }

    // Tenant "globex" is unaffected and gets its own connections.
    const r = await registry.withConnection("globex", (c) => `globex served by ${c.tenant}#${c.id}`);
    console.log(r);

    for (const c of held) await acme.release(c);
    for (const t of ["acme", "globex"]) {
      const s = registry.pool(t).stats();
      console.log(`${s.name}: size=${s.size} idle=${s.idle} inUse=${s.inUse} timeouts=${s.timeouts}`);
    }
  } finally {
    await registry.close();
  }
  console.log(`registry closed: ${registry.closed}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
