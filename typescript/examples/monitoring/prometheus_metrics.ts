/**
 * Render pool metrics in the Prometheus text exposition format (no client library).
 *
 *   npx tsx examples/monitoring/prometheus_metrics.ts
 *
 * Counters/gauges come from `pool.stats()`; the acquire-wait histogram is fed by a PoolListener
 * (stats only keep a sample window, a Prometheus histogram needs cumulative buckets).
 * In a real service, return `render()` from your `/metrics` HTTP handler.
 */
import { AsyncConnectionPool, PoolConfig, type PoolListener } from "../../src/index.js";

const BUCKETS = [0.001, 0.005, 0.025, 0.1, 0.5]; // seconds

/** Cumulative-bucket histogram of acquire wait times. */
class WaitHistogram implements PoolListener {
  readonly counts = new Array<number>(BUCKETS.length).fill(0);
  total = 0;
  sum = 0;

  onAcquired(_conn: unknown, waitTime: number): void {
    BUCKETS.forEach((le, i) => {
      if (waitTime <= le) this.counts[i] = (this.counts[i] ?? 0) + 1;
    });
    this.total += 1;
    this.sum += waitTime;
  }
}

function render(pool: AsyncConnectionPool<object>, hist: WaitHistogram): string {
  const s = pool.stats();
  const l = `pool="${s.name}"`;
  const lines: string[] = [];
  const metric = (name: string, type: "gauge" | "counter", help: string, value: number): void => {
    lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`, `${name}{${l}} ${value}`);
  };
  metric("connpool_connections", "gauge", "Open connections (idle + in use).", s.size);
  metric("connpool_connections_idle", "gauge", "Idle connections.", s.idle);
  metric("connpool_connections_in_use", "gauge", "Checked-out connections.", s.inUse);
  metric("connpool_waiters", "gauge", "Callers blocked in acquire().", s.waiting);
  metric("connpool_created_total", "counter", "Connections created.", s.created);
  metric("connpool_create_failures_total", "counter", "Failed creates.", s.createFailures);
  metric("connpool_acquired_total", "counter", "Successful acquires.", s.acquired);
  metric("connpool_acquire_timeouts_total", "counter", "Acquire timeouts.", s.timeouts);

  lines.push("# HELP connpool_closed_total Connections closed, by reason.");
  lines.push("# TYPE connpool_closed_total counter");
  for (const [reason, n] of Object.entries(s.closedByReason)) {
    lines.push(`connpool_closed_total{${l},reason="${reason}"} ${n}`);
  }

  const h = "connpool_acquire_wait_seconds";
  lines.push(`# HELP ${h} Time spent waiting in acquire().`, `# TYPE ${h} histogram`);
  BUCKETS.forEach((le, i) => lines.push(`${h}_bucket{${l},le="${le}"} ${hist.counts[i]}`));
  lines.push(`${h}_bucket{${l},le="+Inf"} ${hist.total}`);
  lines.push(`${h}_sum{${l}} ${hist.sum}`, `${h}_count{${l}} ${hist.total}`);
  return `${lines.join("\n")}\n`;
}

async function main(): Promise<void> {
  const hist = new WaitHistogram();
  const pool = AsyncConnectionPool.fromCallables(() => ({ id: Math.random() }), {
    config: new PoolConfig({ maxSize: 2, maxUses: 5 }),
    listeners: [hist],
    name: "demo",
  });
  try {
    await pool.open();
    await Promise.all(
      Array.from({ length: 12 }, () =>
        pool.withConnection(() => new Promise((r) => setTimeout(r, 2))),
      ),
    );
    process.stdout.write(render(pool, hist));
  } finally {
    await pool.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
