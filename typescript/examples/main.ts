/**
 * Runnable demo of @jeffeek/connpool. This is client code, NOT part of the package.
 *
 *   npx tsx examples/main.ts
 *
 * It pools a tiny fake "connection" so the demo stays dependency-free; a real factory would
 * dial a database / HTTP client / socket instead. Imports go through the source barrel so the
 * example runs without a build step.
 */
import {
  AsyncConnectionFactory,
  AsyncConnectionPool,
  AsyncPoolRegistry,
  PoolConfig,
  type PoolListener,
} from "../src/index.js";

let nextId = 1;

/** A stand-in for a real connection. */
class FakeConnection {
  readonly id = nextId++;
  closed = false;

  query(sql: string): string {
    if (this.closed) throw new Error("connection is closed");
    return `${sql} -> ok (conn #${this.id})`;
  }

  close(): void {
    this.closed = true;
  }
}

/** The seam you implement to pool anything. */
class FakeFactory extends AsyncConnectionFactory<FakeConnection> {
  async create(): Promise<FakeConnection> {
    // simulate a little connect latency
    await new Promise((r) => setTimeout(r, 5));
    return new FakeConnection();
  }

  override validate(conn: FakeConnection): boolean {
    return !conn.closed;
  }

  override close(conn: FakeConnection): void {
    conn.close();
  }
}

/** A listener that just logs what the pool is doing. */
const logger: PoolListener = {
  onConnectionCreated: (c, ms) => console.log(`  + created #${(c.raw as FakeConnection).id} in ${(ms * 1000).toFixed(1)}ms`),
  onConnectionClosed: (c, reason) => console.log(`  - closed #${(c.raw as FakeConnection).id} (${reason})`),
  onBreakerStateChange: (o, n) => console.log(`  ! breaker ${o} -> ${n}`),
};

async function main(): Promise<void> {
  console.log("== single pool ==");
  const pool = new AsyncConnectionPool(new FakeFactory(), new PoolConfig({ minSize: 1, maxSize: 4 }), {
    listeners: [logger],
    name: "demo",
  });
  await pool.open();

  // Borrow in parallel; the pool caps concurrency at maxSize and reuses connections.
  const results = await Promise.all(
    Array.from({ length: 8 }, (_, i) =>
      pool.withConnection((conn) => conn.query(`SELECT ${i}`)),
    ),
  );
  for (const r of results) console.log("  " + r);

  const s = pool.stats();
  console.log(
    `  stats: created=${s.created} acquired=${s.acquired} idle=${s.idle} inUse=${s.inUse} ` +
      `p95_acquire_wait=${s.toDictionary().p95_acquire_wait}`,
  );
  await pool.close();

  console.log("\n== registry (one pool per key) ==");
  const registry = new AsyncPoolRegistry<string, FakeConnection>(
    () => new FakeFactory(),
    new PoolConfig({ maxSize: 2 }),
  );
  for (const host of ["db-a", "db-b", "db-a"]) {
    const answer = await registry.withConnection(host, (conn) => conn.query(`ping ${host}`));
    console.log(`  ${host}: ${answer}`);
  }
  await registry.close();
  console.log("\ndone.");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
