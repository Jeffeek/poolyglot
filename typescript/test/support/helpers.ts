/** Shared test doubles — the TypeScript twins of python/tests/support/helpers.py. */
import { AsyncConnectionFactory } from "../../src/factories/async-factory.js";
import { NO_RETRY } from "../../src/resilience/retry.js";
import { PoolConfig, type PoolConfigOptions } from "../../src/policy/config.js";
import { type CloseReason } from "../../src/core/connection.js";
import { type PoolListener } from "../../src/observability/events.js";
import { type BreakerState } from "../../src/resilience/breaker.js";

/** Backend errors used by the fakes. */
export class BackendDown extends Error {}
export class ConnReset extends Error {}

let nextId = 1;

export class FakeConn {
  readonly id = nextId++;
  closed = false;
  healthy = true;
  resets = 0;

  toString(): string {
    return `<FakeConn ${this.id}>`;
  }
}

/** The shared counters + failure switches behind the async factory. */
export class FakeInner {
  created: FakeConn[] = [];
  attempts = 0;
  failTimes = 0;
  failAlways = false;
  fatalTypes: Array<new (...args: never[]) => Error> = [];
  resetRaises = false;

  create(): FakeConn {
    this.attempts += 1;
    if (this.failAlways || this.failTimes > 0) {
      this.failTimes = Math.max(0, this.failTimes - 1);
      throw new BackendDown("backend down");
    }
    const conn = new FakeConn();
    this.created.push(conn);
    return conn;
  }

  validate(conn: FakeConn): boolean {
    return conn.healthy && !conn.closed;
  }

  reset(conn: FakeConn): void {
    if (this.resetRaises) {
      throw new Error("reset failed");
    }
    conn.resets += 1;
  }

  close(conn: FakeConn): void {
    conn.closed = true;
  }

  isFatalError(error: unknown): boolean {
    return this.fatalTypes.some((t) => error instanceof t);
  }
}

export function realSleep(seconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, seconds * 1000));
}

export class AsyncFakeFactory extends AsyncConnectionFactory<FakeConn> {
  readonly inner: FakeInner;
  readonly createDelay: number;

  constructor(inner: FakeInner = new FakeInner(), createDelay = 0.0) {
    super();
    this.inner = inner;
    this.createDelay = createDelay;
  }

  async create(): Promise<FakeConn> {
    if (this.createDelay) {
      await realSleep(this.createDelay);
    }
    return this.inner.create();
  }

  override validate(conn: FakeConn): boolean {
    return this.inner.validate(conn);
  }

  override reset(conn: FakeConn): void {
    this.inner.reset(conn);
  }

  override async close(conn: FakeConn): Promise<void> {
    this.inner.close(conn);
  }

  override isFatalError(error: unknown): boolean {
    return this.inner.isFatalError(error);
  }
}

export class Recorder implements PoolListener {
  closed: CloseReason[] = [];
  events: string[] = [];

  onConnectionCreated(): void {
    this.events.push("created");
  }

  onConnectionClosed(_conn: unknown, reason: CloseReason): void {
    this.closed.push(reason);
    this.events.push(`closed:${reason}`);
  }

  onAcquired(): void {
    this.events.push("acquired");
  }

  onReleased(): void {
    this.events.push("released");
  }

  onBreakerStateChange(oldState: BreakerState, newState: BreakerState): void {
    this.events.push(`breaker:${oldState}->${newState}`);
  }
}

/** Deterministic baseline: no jitter, no background work, no retries, no breaker. */
export function quietConfig(overrides: PoolConfigOptions = {}): PoolConfig {
  return new PoolConfig({
    maxLifetime: null,
    maxIdleTime: null,
    validateAfterIdle: null,
    maintenanceInterval: null,
    acquireTimeout: 0.3,
    lifetimeJitter: 0.0,
    retry: NO_RETRY,
    breaker: null,
    ...overrides,
  });
}
