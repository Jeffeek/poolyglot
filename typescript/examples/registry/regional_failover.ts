/**
 * Regional failover on top of AsyncPoolRegistry (one pool per region), deterministic.
 *
 *   npx tsx examples/registry/regional_failover.ts
 *
 * Outages are simulated by toggling the `down` set *before* connection creation, never by
 * touching pool internals. `validateAfterIdle: 0` makes the pool validate a cached connection on
 * every checkout, so a downed region's idle connection is retired and the re-create fails.
 */
import {
  AsyncConnectionFactory,
  AsyncPoolRegistry,
  ConnectionCreateError,
  NO_RETRY,
  PoolConfig,
} from "../../src/index.js";

interface RegionConn {
  readonly region: string;
}

const REGIONS = ["us-east", "eu-west"]; // priority order
const down = new Set<string>();

class RegionFactory extends AsyncConnectionFactory<RegionConn> {
  constructor(private readonly region: string) {
    super();
  }

  create(): RegionConn {
    if (down.has(this.region)) throw new Error(`${this.region} unreachable`);
    return { region: this.region };
  }

  override validate(): boolean {
    return !down.has(this.region);
  }
}

const registry = new AsyncPoolRegistry<string, RegionConn>(
  (region) => new RegionFactory(region),
  new PoolConfig({ maxSize: 2, retry: NO_RETRY, breaker: null, validateAfterIdle: 0 }),
);

/** Try regions in priority order; fail over only on connection-creation errors. */
async function query(label: string): Promise<void> {
  for (const region of REGIONS) {
    try {
      const served = await registry.withConnection(region, (c) => c.region);
      console.log(`${label}: served by ${served}`);
      return;
    } catch (err) {
      if (!(err instanceof ConnectionCreateError)) throw err;
      console.log(`${label}: ${region} failed (${(err.cause as Error).message}), failing over`);
    }
  }
  console.log(`${label}: all regions down`);
}

async function main(): Promise<void> {
  try {
    await query("1 healthy");
    down.add("us-east");
    await query("2 us-east outage");
    down.add("eu-west");
    await query("3 total outage");
    down.clear();
    await query("4 recovered");
  } finally {
    await registry.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
