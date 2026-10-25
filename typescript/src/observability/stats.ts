/** Built-in metrics: a listener that counts events, plus an immutable snapshot. */
import type { CloseReason, PooledConnection } from "../core/connection.js";
import type { PoolListener } from "./events.js";

/**
 * How many recent acquire-wait / create-latency samples to keep. `avg` and all percentiles are
 * computed over this same window (bounded memory), so they always describe the same population.
 */
export const ACQUIRE_WAIT_WINDOW = 1024;

/** Linear-interpolated percentile of an already-sorted list. `q` in [0, 1]. */
export function percentile(sortedValues: number[], q: number): number {
  if (sortedValues.length === 0) {
    return 0.0;
  }
  const k = (sortedValues.length - 1) * q;
  const lo = Math.floor(k);
  const hi = Math.min(lo + 1, sortedValues.length - 1);
  return sortedValues[lo]! + (sortedValues[hi]! - sortedValues[lo]!) * (k - lo);
}

/** A bounded FIFO window of the most recent `cap` samples. */
class SampleWindow {
  private readonly buf: number[] = [];
  private start = 0;

  constructor(private readonly cap: number) {}

  push(value: number): void {
    if (this.buf.length < this.cap) {
      this.buf.push(value);
    } else {
      this.buf[this.start] = value;
      this.start = (this.start + 1) % this.cap;
    }
  }

  sorted(): number[] {
    return [...this.buf].sort((a, b) => a - b);
  }
}

export interface PoolStatsGauges {
  name: string;
  maxSize: number;
  size: number;
  idle: number;
  inUse: number;
  waiting: number;
  breakerState: string | null;
}

export class PoolStats {
  readonly name: string;
  readonly maxSize: number;
  readonly size: number; // idle + in use + being created
  readonly idle: number; // available for checkout right now
  readonly inUse: number; // checked out by a caller (or briefly by a background health check)
  readonly waiting: number; // callers currently blocked in acquire()
  readonly created: number;
  readonly createFailures: number;
  readonly acquired: number;
  readonly released: number;
  readonly timeouts: number;
  readonly closed: number;
  readonly leaks: number; // borrowed connections flagged as possibly leaked
  readonly closedByReason: Record<string, number>;
  // acquire-wait aggregates, all over the last ACQUIRE_WAIT_WINDOW samples
  readonly avgAcquireWait: number;
  readonly p50AcquireWait: number;
  readonly p95AcquireWait: number;
  readonly p99AcquireWait: number;
  // create-latency aggregates (factory.create() duration), same window
  readonly avgCreateLatency: number;
  readonly p50CreateLatency: number;
  readonly p95CreateLatency: number;
  readonly p99CreateLatency: number;
  readonly breakerState: string | null;

  constructor(fields: {
    gauges: PoolStatsGauges;
    created: number;
    createFailures: number;
    acquired: number;
    released: number;
    timeouts: number;
    closed: number;
    leaks: number;
    closedByReason: Record<string, number>;
    waits: number[];
    creates: number[];
  }) {
    const { gauges, waits, creates } = fields;
    this.name = gauges.name;
    this.maxSize = gauges.maxSize;
    this.size = gauges.size;
    this.idle = gauges.idle;
    this.inUse = gauges.inUse;
    this.waiting = gauges.waiting;
    this.breakerState = gauges.breakerState;
    this.created = fields.created;
    this.createFailures = fields.createFailures;
    this.acquired = fields.acquired;
    this.released = fields.released;
    this.timeouts = fields.timeouts;
    this.closed = fields.closed;
    this.leaks = fields.leaks;
    this.closedByReason = fields.closedByReason;
    this.avgAcquireWait = waits.length ? waits.reduce((a, b) => a + b, 0) / waits.length : 0.0;
    this.p50AcquireWait = percentile(waits, 0.5);
    this.p95AcquireWait = percentile(waits, 0.95);
    this.p99AcquireWait = percentile(waits, 0.99);
    this.avgCreateLatency = creates.length
      ? creates.reduce((a, b) => a + b, 0) / creates.length
      : 0.0;
    this.p50CreateLatency = percentile(creates, 0.5);
    this.p95CreateLatency = percentile(creates, 0.95);
    this.p99CreateLatency = percentile(creates, 0.99);
  }

  /** Snapshot as a plain object with the shared snake_case wire keys. */
  toDictionary(): Record<string, unknown> {
    return {
      name: this.name,
      max_size: this.maxSize,
      size: this.size,
      idle: this.idle,
      in_use: this.inUse,
      waiting: this.waiting,
      created: this.created,
      create_failures: this.createFailures,
      acquired: this.acquired,
      released: this.released,
      timeouts: this.timeouts,
      closed: this.closed,
      leaks: this.leaks,
      closed_by_reason: { ...this.closedByReason },
      avg_acquire_wait: this.avgAcquireWait,
      p50_acquire_wait: this.p50AcquireWait,
      p95_acquire_wait: this.p95AcquireWait,
      p99_acquire_wait: this.p99AcquireWait,
      avg_create_latency: this.avgCreateLatency,
      p50_create_latency: this.p50CreateLatency,
      p95_create_latency: this.p95CreateLatency,
      p99_create_latency: this.p99CreateLatency,
      breaker_state: this.breakerState,
    };
  }
}

export class StatsCollector implements PoolListener {
  private readonly counts = {
    created: 0,
    createFailures: 0,
    acquired: 0,
    released: 0,
    timeouts: 0,
    closed: 0,
    leaks: 0,
  };
  private readonly byReason = new Map<string, number>();
  private readonly waits = new SampleWindow(ACQUIRE_WAIT_WINDOW);
  private readonly creates = new SampleWindow(ACQUIRE_WAIT_WINDOW);

  onConnectionCreated(_conn: PooledConnection<unknown>, duration: number): void {
    this.counts.created += 1;
    this.creates.push(duration);
  }

  onCreateFailed(_error: unknown): void {
    this.counts.createFailures += 1;
  }

  onConnectionClosed(_conn: PooledConnection<unknown>, reason: CloseReason): void {
    this.counts.closed += 1;
    this.byReason.set(reason, (this.byReason.get(reason) ?? 0) + 1);
  }

  onAcquired(_conn: PooledConnection<unknown>, waitTime: number): void {
    this.counts.acquired += 1;
    this.waits.push(waitTime);
  }

  onReleased(_conn: PooledConnection<unknown>, _holdTime: number): void {
    this.counts.released += 1;
  }

  onAcquireTimeout(_waited: number): void {
    this.counts.timeouts += 1;
  }

  onConnectionLeak(_conn: PooledConnection<unknown>, _heldFor: number): void {
    this.counts.leaks += 1;
  }

  snapshot(gauges: PoolStatsGauges): PoolStats {
    return new PoolStats({
      gauges,
      created: this.counts.created,
      createFailures: this.counts.createFailures,
      acquired: this.counts.acquired,
      released: this.counts.released,
      timeouts: this.counts.timeouts,
      closed: this.counts.closed,
      leaks: this.counts.leaks,
      closedByReason: Object.fromEntries(this.byReason),
      waits: this.waits.sorted(),
      creates: this.creates.sorted(),
    });
  }
}
