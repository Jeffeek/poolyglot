/**
 * Async connection pool. Same model and same rules ({@link lifecycle}) as the other poolyglot
 * languages' pools. Promise-based throughout; cancellation is cooperative via `AbortSignal`.
 *
 * Differences worth knowing:
 *   * `createTimeout` is enforced here (racing a real timer); the injected {@link Clock} drives
 *     only *logical* time (lifetime, idle, breaker recovery, backoff) — timeouts/waits use real
 *     time, exactly as the Python pool's `asyncio.timeout` does.
 *   * Listener hooks stay synchronous (keep them fast).
 */
import { type Clock, SystemClock } from "../core/clock.js";
import { CloseReason, type PooledConnection } from "../core/connection.js";
import {
  ConnectionCreateError,
  InvalidReleaseError,
  PoolClosedError,
  PoolExhaustedError,
  PoolTimeoutError,
} from "../core/errors.js";
import { type Timeout, UNSET, computeDeadline, remaining } from "../core/timeouts.js";
import {
  AsyncCallableFactory,
  type AsyncCallableFactoryOptions,
  type AsyncConnectionFactory,
} from "../factories/async-factory.js";
import { ListenerGroup, type PoolListener } from "../observability/events.js";
import { type PoolStats, StatsCollector } from "../observability/stats.js";
import { PoolConfig } from "../policy/config.js";
import * as lifecycle from "../policy/lifecycle.js";
import { CircuitBreaker } from "../resilience/breaker.js";
import type { Rng } from "../resilience/retry.js";
import { AsyncCondition, CancelledError, abortError, awaitShielded } from "./async-primitives.js";

/** Internal marker: the `createTimeout`/deadline limit elapsed while creating a connection. */
class CreateLimitTimeout extends Error {
  constructor() {
    super("create limit reached");
    this.name = "CreateLimitTimeout";
  }
}

export interface AsyncConnectionPoolOptions {
  listeners?: Iterable<PoolListener>;
  clock?: Clock;
  rng?: Rng;
  name?: string;
}

export interface AcquireOptions {
  timeout?: Timeout;
  signal?: AbortSignal;
}

export interface ReleaseOptions {
  discard?: boolean;
  error?: unknown;
  signal?: AbortSignal;
}

export interface FromCallablesOptions<T> extends AsyncCallableFactoryOptions<T> {
  config?: PoolConfig;
  listeners?: Iterable<PoolListener>;
  clock?: Clock;
  rng?: Rng;
  name?: string;
}

export class AsyncConnectionPool<T extends object> {
  readonly name: string;
  private readonly factory: AsyncConnectionFactory<T>;
  private readonly cfg: PoolConfig;
  private readonly clock: Clock;
  private readonly rng: Rng;

  private readonly cond = new AsyncCondition();
  private idle: Array<PooledConnection<T>> = [];
  private readonly checkedOut = new Map<T, PooledConnection<T>>();
  private size = 0;
  private waiters = 0;
  private _closed = false;

  private readonly statsCollector = new StatsCollector();
  private readonly events: ListenerGroup;
  private readonly breaker: CircuitBreaker | null;

  private maintTask: Promise<void> | null = null;
  private maintAbort: AbortController | null = null;
  private opened = false;

  constructor(
    factory: AsyncConnectionFactory<T>,
    config?: PoolConfig,
    options: AsyncConnectionPoolOptions = {},
  ) {
    this.name = options.name ?? "pool";
    this.factory = factory;
    this.cfg = config ?? new PoolConfig();
    this.clock = options.clock ?? new SystemClock();
    this.rng = options.rng ?? Math.random;

    this.events = new ListenerGroup([this.statsCollector, ...(options.listeners ?? [])]);
    this.breaker = this.cfg.breaker
      ? new CircuitBreaker(this.cfg.breaker, {
          clock: this.clock,
          onStateChange: (o, n) => this.events.dispatch("onBreakerStateChange", o, n),
        })
      : null;
  }

  static fromCallables<T extends object>(
    create: () => T | Promise<T>,
    options: FromCallablesOptions<T> = {},
  ): AsyncConnectionPool<T> {
    const { config, listeners, clock, rng, name, ...factoryOpts } = options;
    const poolOptions: AsyncConnectionPoolOptions = {};
    if (listeners !== undefined) poolOptions.listeners = listeners;
    if (clock !== undefined) poolOptions.clock = clock;
    if (rng !== undefined) poolOptions.rng = rng;
    if (name !== undefined) poolOptions.name = name;
    return new AsyncConnectionPool(
      new AsyncCallableFactory(create, factoryOpts),
      config,
      poolOptions,
    );
  }

  // ------------------------------------------------------------------ lifecycle
  async open(): Promise<this> {
    if (this._closed) {
      throw new PoolClosedError(`pool '${this.name}' is closed`);
    }
    if (this.opened) {
      return this;
    }
    this.opened = true;
    await this.fillToMin();
    // fillToMin awaited, so close() may have run meanwhile. Only start the loop if still open —
    // otherwise a maintenance loop would outlive shutdown.
    if (this.cfg.maintenanceInterval !== null && !this._closed) {
      this.startMaintenanceLoop(this.cfg.maintenanceInterval);
    }
    return this;
  }

  async close(options: { timeout?: number | null } = {}): Promise<void> {
    const timeout = options.timeout ?? null;
    let idle: Array<PooledConnection<T>>;
    await this.cond.lock();
    try {
      if (this._closed) {
        return;
      }
      this._closed = true;
      idle = [...this.idle];
      this.idle.length = 0;
      this.cond.notifyAll();
      if (timeout !== null) {
        const deadline = this.clock.monotonic() + timeout;
        while (this.checkedOut.size > 0) {
          const left = deadline - this.clock.monotonic();
          if (left <= 0) {
            break;
          }
          await this.cond.wait(left);
        }
      }
    } finally {
      this.cond.unlock();
    }
    if (this.maintTask) {
      this.maintAbort?.abort();
      await this.maintTask; // the loop is fully guarded and always resolves
      this.maintTask = null;
    }
    for (const pooled of idle) {
      await this.destroy(pooled, CloseReason.POOL_CLOSED);
    }
  }

  get closed(): boolean {
    return this._closed;
  }

  // --------------------------------------------------------------------- usage
  /** Borrow a connection, run `fn`, and always release it (discarding it if `fn` throws). */
  async withConnection<R>(
    fn: (conn: T) => R | Promise<R>,
    options: AcquireOptions = {},
  ): Promise<R> {
    const conn = await this.acquire(options);
    const releaseOpts: ReleaseOptions = {};
    if (options.signal !== undefined) releaseOpts.signal = options.signal;
    let result: R;
    try {
      result = await fn(conn);
    } catch (error) {
      await this.release(conn, { ...releaseOpts, error });
      throw error;
    }
    await this.release(conn, releaseOpts);
    return result;
  }

  async acquire(options: AcquireOptions = {}): Promise<T> {
    const timeout = options.timeout === undefined ? UNSET : options.timeout;
    const signal = options.signal;
    const started = this.clock.monotonic();
    const deadline = computeDeadline(timeout, this.cfg.acquireTimeout, started);
    try {
      for (;;) {
        const taken = await this.takeIdleOrReserve(deadline, signal);
        if (taken === null) {
          const [created, createdIn] = await this.create(deadline, signal);
          try {
            await this.registerNew(created, createdIn, signal);
          } catch (error) {
            if (error instanceof ConnectionCreateError || error instanceof PoolClosedError) {
              throw error; // registerNew already cleaned up its own cases
            }
            await this.destroy(created, CloseReason.ERROR); // cancelled while registering
            throw error;
          }
          return this.adopt(created, started);
        }
        let reason: CloseReason | null;
        try {
          reason = await this.borrowReason(taken);
        } catch (error) {
          await this.destroy(taken, CloseReason.ERROR); // e.g. cancelled mid-validation
          throw error;
        }
        if (reason === null) {
          return this.adopt(taken, started);
        }
        await this.destroy(taken, reason);
      }
    } catch (error) {
      if (error instanceof PoolTimeoutError) {
        this.events.dispatch("onAcquireTimeout", this.clock.monotonic() - started);
      }
      throw error;
    }
  }

  private adopt(pooled: PooledConnection<T>, started: number): T {
    const now = this.clock.monotonic();
    pooled.markCheckedOut(now);
    this.events.dispatch("onAcquired", pooled, now - started);
    return pooled.raw;
  }

  async release(conn: T, options: ReleaseOptions = {}): Promise<void> {
    const { discard = false, error, signal } = options;
    let pooled: PooledConnection<T> | undefined;
    await this.cond.lock();
    try {
      pooled = this.checkedOut.get(conn);
      if (pooled) {
        this.checkedOut.delete(conn);
      }
    } finally {
      this.cond.unlock();
    }
    if (!pooled) {
      throw new InvalidReleaseError("object is not checked out from this pool (double release?)");
    }

    const now = this.clock.monotonic();
    const hold = pooled.holdTime(now);
    let reason: CloseReason | null;
    try {
      reason = await this.releaseReason(pooled, conn, discard, error, now);
      if (reason === null && !(await this.park(pooled, now))) {
        reason = CloseReason.POOL_CLOSED;
      }
    } catch (err) {
      await this.destroy(pooled, CloseReason.ERROR, signal); // cancelled during reset/validate
      throw err;
    }
    if (reason !== null) {
      await this.destroy(pooled, reason, signal);
    }
    this.events.dispatch("onReleased", pooled, hold);
  }

  stats(): PoolStats {
    return this.statsCollector.snapshot({
      name: this.name,
      maxSize: this.cfg.maxSize,
      size: this.size,
      idle: this.idle.length,
      inUse: this.checkedOut.size,
      waiting: this.waiters,
      breakerState: this.breaker ? this.breaker.state : null,
    });
  }

  async runMaintenance(): Promise<void> {
    await this.cond.lock();
    let doomed: Array<[PooledConnection<T>, CloseReason]> = [];
    try {
      if (this._closed) {
        return;
      }
      const res = lifecycle.sweep(this.idle, this.size, this.cfg, this.clock.monotonic());
      this.idle = res.keep;
      doomed = res.doomed;
    } finally {
      this.cond.unlock();
    }
    for (const [pooled, reason] of doomed) {
      await this.destroy(pooled, reason);
    }
    await this.healthCheckIdle();
    await this.fillToMin();
    this.checkLeaks();
  }

  private async healthCheckIdle(): Promise<void> {
    if (!this.cfg.validateIdle) {
      return;
    }
    let batch: Array<PooledConnection<T>>;
    await this.cond.lock();
    try {
      batch = [...this.idle];
      this.idle.length = 0;
      for (const pooled of batch) {
        this.checkedOut.set(pooled.raw, pooled);
      }
    } finally {
      this.cond.unlock();
    }
    for (const pooled of batch) {
      const healthy = await this.isHealthy(pooled.raw);
      let keep: boolean;
      await this.cond.lock();
      try {
        this.checkedOut.delete(pooled.raw);
        keep = healthy && !this._closed;
        if (keep) {
          this.idle.push(pooled);
          this.cond.notify();
        }
      } finally {
        this.cond.unlock();
      }
      if (!keep) {
        await this.destroy(pooled, healthy ? CloseReason.POOL_CLOSED : CloseReason.UNHEALTHY);
      }
    }
  }

  private checkLeaks(): void {
    const timeout = this.cfg.leakDetectionTimeout;
    if (timeout === null) {
      return;
    }
    const now = this.clock.monotonic();
    for (const pooled of this.checkedOut.values()) {
      if (!pooled.leakReported && pooled.holdTime(now) >= timeout) {
        pooled.leakReported = true;
        this.events.dispatch("onConnectionLeak", pooled, pooled.holdTime(now));
      }
    }
  }

  // ----------------------------------------------------------------- internals
  private async takeIdleOrReserve(
    deadline: number | null,
    signal: AbortSignal | undefined,
  ): Promise<PooledConnection<T> | null> {
    await this.cond.lock(signal);
    try {
      for (;;) {
        if (this._closed) {
          throw new PoolClosedError(`pool '${this.name}' is closed`);
        }
        const pooled = lifecycle.popIdle(this.idle, this.cfg.idleStrategy);
        if (pooled !== null) {
          this.checkedOut.set(pooled.raw, pooled);
          return pooled;
        }
        if (this.size < this.cfg.maxSize) {
          this.size += 1;
          return null;
        }
        const left = remaining(deadline, this.clock.monotonic());
        if (left !== null && left <= 0) {
          throw new PoolTimeoutError(
            `pool '${this.name}': no connection available (maxSize=${this.cfg.maxSize})`,
          );
        }
        if (this.cfg.maxWaiters !== null && this.waiters >= this.cfg.maxWaiters) {
          throw new PoolExhaustedError(
            `pool '${this.name}': waiter queue full (maxWaiters=${this.cfg.maxWaiters})`,
          );
        }
        this.waiters += 1;
        try {
          await this.cond.wait(left, signal); // notified / timed out; abort throws
        } finally {
          this.waiters -= 1;
        }
      }
    } finally {
      this.cond.unlock();
    }
  }

  private async borrowReason(pooled: PooledConnection<T>): Promise<CloseReason | null> {
    const now = this.clock.monotonic();
    const reason = lifecycle.borrowReason(pooled, this.cfg, now);
    if (reason === null && lifecycle.needsValidation(pooled, this.cfg, now)) {
      if (!(await this.isHealthy(pooled.raw))) {
        return CloseReason.UNHEALTHY;
      }
    }
    return reason;
  }

  private async releaseReason(
    pooled: PooledConnection<T>,
    conn: T,
    discard: boolean,
    error: unknown,
    now: number,
  ): Promise<CloseReason | null> {
    if (discard) {
      return CloseReason.DISCARDED;
    }
    if (
      error !== undefined &&
      error !== null &&
      (this.factory.isFatalError(error) || !(await this.isHealthy(conn)))
    ) {
      return CloseReason.ERROR;
    }
    const reason = lifecycle.rotationReason(pooled, this.cfg, now);
    if (reason !== null) {
      return reason;
    }
    try {
      await this.factory.reset(conn);
    } catch (err) {
      if (err instanceof CancelledError) {
        throw err;
      }
      return CloseReason.RESET_FAILED;
    }
    return null;
  }

  private async isHealthy(raw: T): Promise<boolean> {
    try {
      return Boolean(await this.factory.validate(raw));
    } catch (err) {
      if (err instanceof CancelledError) {
        throw err; // cancellation is not a "failed validation"
      }
      return false;
    }
  }

  private async park(pooled: PooledConnection<T>, now: number): Promise<boolean> {
    await this.cond.lock();
    try {
      if (this._closed) {
        return false;
      }
      pooled.markReturned(now);
      this.idle.push(pooled);
      this.cond.notify();
      return true;
    } finally {
      this.cond.unlock();
    }
  }

  private async create(
    deadline: number | null,
    signal: AbortSignal | undefined,
  ): Promise<[PooledConnection<T>, number]> {
    const started = this.clock.monotonic();
    let raw: T;
    try {
      raw = await this.createRaw(deadline, signal);
    } catch (error) {
      await this.freeSlot();
      throw error;
    }
    const now = this.clock.monotonic();
    const pooled = lifecycle.newConnection(raw, this.cfg, now, this.rng);
    return [pooled, now - started];
  }

  /**
   * The time budget for a single `factory.create()`, and whether the **acquire deadline** (not
   * `createTimeout`) is the binding limit. `deadlineBound` is decided here, up front, rather than by
   * re-reading the clock after the timer fires: the timer (libuv) and the clock
   * (`performance.now`) are different time sources, so a post-hoc `remaining <= 0` comparison is
   * racy exactly at the boundary where the two limits coincide. Python avoids this because
   * `asyncio.timeout` and the injected clock share one monotonic clock.
   */
  private createLimit(deadline: number | null): { limit: number | null; deadlineBound: boolean } {
    const createTimeout = this.cfg.createTimeout;
    const rem = remaining(deadline, this.clock.monotonic());
    // The acquire deadline binds iff it exists and is no later than createTimeout (ties -> deadline,
    // matching the Python `left <= 0` outcome when the two limits are equal).
    const deadlineBound = rem !== null && (createTimeout === null || rem <= createTimeout);
    let limit: number | null;
    if (rem === null) {
      // No acquire deadline: fall back to createTimeout (itself null = unbounded).
      limit = createTimeout;
    } else if (createTimeout === null) {
      limit = Math.max(0, rem);
    } else {
      limit = Math.max(0, Math.min(createTimeout, rem));
    }
    return { limit, deadlineBound };
  }

  private async registerNew(
    pooled: PooledConnection<T>,
    createdIn: number,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    let action: "ok" | "duplicate" | "closed";
    await this.cond.lock(signal);
    try {
      if (this._closed) {
        action = "closed";
      } else if (this.checkedOut.has(pooled.raw) || this.idle.some((p) => p.raw === pooled.raw)) {
        action = "duplicate"; // already tracked (idle or checked out)
      } else {
        this.checkedOut.set(pooled.raw, pooled);
        action = "ok";
      }
    } finally {
      this.cond.unlock();
    }
    if (action === "duplicate") {
      // Same object we already manage: not a new connection, not counted, and never closed
      // (its real owner still holds it).
      await this.freeSlot();
      throw new ConnectionCreateError(
        `pool '${this.name}': factory.create() returned an object the pool already manages; ` +
          "each create() must return a distinct connection",
      );
    }
    this.events.dispatch("onConnectionCreated", pooled, createdIn);
    if (action === "closed") {
      await this.destroy(pooled, CloseReason.POOL_CLOSED);
      throw new PoolClosedError(`pool '${this.name}' is closed`);
    }
  }

  private async createRaw(deadline: number | null, signal: AbortSignal | undefined): Promise<T> {
    // One logical creation per call: fail-fast when the breaker is open, then a single
    // success/failure for the whole retry sequence.
    if (this.breaker) {
      this.breaker.beforeCall();
    }
    let raw: T;
    try {
      raw = await this.createWithRetry(deadline, signal);
    } catch (error) {
      if (this.breaker) {
        if (error instanceof ConnectionCreateError) {
          this.breaker.recordFailure(); // the backend actually failed
        } else {
          this.breaker.recordAbort(); // timeout / closed / cancelled: not a backend fault
        }
      }
      throw error;
    }
    if (this.breaker) {
      this.breaker.recordSuccess();
    }
    return raw;
  }

  private async createWithRetry(
    deadline: number | null,
    signal: AbortSignal | undefined,
  ): Promise<T> {
    const retry = this.cfg.retry;
    let attempt = 0;
    for (;;) {
      if (this._closed) {
        throw new PoolClosedError(`pool '${this.name}' is closed`);
      }
      attempt += 1;
      const { limit, deadlineBound } = this.createLimit(deadline);
      try {
        return await this.callCreateWithLimit(limit, signal);
      } catch (exc) {
        if (signal?.aborted) {
          throw exc; // cancellation: don't retry, don't count as a create failure
        }
        // A timeout whose binding limit was the acquire deadline is caller impatience, not a
        // backend fault: surface PoolTimeoutError so the breaker records an abort. A
        // createTimeout firing is a backend fault and flows through the create-failure path.
        if (exc instanceof CreateLimitTimeout && deadlineBound) {
          throw new PoolTimeoutError(
            `pool '${this.name}': acquire deadline reached while creating connection`,
          );
        }
        this.events.dispatch("onCreateFailed", exc);
        if (attempt >= retry.maxAttempts || !retry.shouldRetry(exc)) {
          throw new ConnectionCreateError(
            `pool '${this.name}': could not create connection after ${attempt} attempt(s): ${String(exc)}`,
            { cause: exc },
          );
        }
        const delay = retry.delayFor(attempt, this.rng);
        const left = remaining(deadline, this.clock.monotonic());
        if (left !== null && delay >= left) {
          throw new PoolTimeoutError(
            `pool '${this.name}': acquire deadline reached while retrying creation`,
          );
        }
        await this.clock.asyncSleep(delay);
      }
    }
  }

  private callCreateWithLimit(limit: number | null, signal: AbortSignal | undefined): Promise<T> {
    const work = Promise.resolve(this.factory.create());
    if (signal?.aborted) {
      work.catch(() => {});
      return Promise.reject(abortError(signal));
    }
    if (limit === null && !signal) {
      return work;
    }
    return new Promise<T>((resolve, reject) => {
      let settled = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let onAbort: (() => void) | undefined;
      const finish = (fn: (v: never) => void, value: never): void => {
        if (settled) return;
        settled = true;
        if (timer !== undefined) clearTimeout(timer);
        if (signal && onAbort) signal.removeEventListener("abort", onAbort);
        fn(value);
      };
      if (limit !== null) {
        timer = setTimeout(
          () => finish(reject as (v: never) => void, new CreateLimitTimeout() as never),
          Math.max(0, limit) * 1000,
        );
      }
      if (signal) {
        onAbort = () => finish(reject as (v: never) => void, abortError(signal) as never);
        signal.addEventListener("abort", onAbort, { once: true });
      }
      work.then(
        (v) => finish(resolve as (v: never) => void, v as never),
        (e: unknown) => finish(reject as (v: never) => void, e as never),
      );
    });
  }

  private async freeSlot(): Promise<void> {
    await this.cond.lock();
    try {
      this.size -= 1;
      this.cond.notify();
    } finally {
      this.cond.unlock();
    }
  }

  private destroy(
    pooled: PooledConnection<T>,
    reason: CloseReason,
    signal?: AbortSignal,
  ): Promise<void> {
    // Shielded: a cancellation that lands while factory.close() awaits I/O can neither interrupt
    // disposal, skip the slot decrement, nor drop the on_connection_closed event.
    return awaitShielded(this.dispose(pooled, reason), signal);
  }

  private async dispose(pooled: PooledConnection<T>, reason: CloseReason): Promise<void> {
    try {
      await this.factory.close(pooled.raw);
    } catch {
      // error closing: keep the accounting balanced regardless
    } finally {
      await this.cond.lock();
      try {
        this.checkedOut.delete(pooled.raw);
        this.size -= 1;
        this.cond.notify();
      } finally {
        this.cond.unlock();
      }
    }
    this.events.dispatch("onConnectionClosed", pooled, reason);
  }

  private async fillToMin(): Promise<void> {
    for (;;) {
      let proceed: boolean;
      await this.cond.lock();
      try {
        if (this._closed || this.size >= this.cfg.minSize) {
          proceed = false;
        } else {
          this.size += 1;
          proceed = true;
        }
      } finally {
        this.cond.unlock();
      }
      if (!proceed) {
        return;
      }
      let pooled: PooledConnection<T>;
      let createdIn: number;
      try {
        [pooled, createdIn] = await this.create(null, undefined);
      } catch {
        return; // warm-up failure is not fatal; create() already freed the slot
      }
      this.events.dispatch("onConnectionCreated", pooled, createdIn);
      if (!(await this.park(pooled, this.clock.monotonic()))) {
        await this.destroy(pooled, CloseReason.POOL_CLOSED);
      }
    }
  }

  private startMaintenanceLoop(interval: number): void {
    const controller = new AbortController();
    this.maintAbort = controller;
    const signal = controller.signal;
    this.maintTask = (async () => {
      while (!this._closed && !signal.aborted) {
        try {
          await this.abortableDelay(interval, signal);
        } catch {
          break; // aborted on close
        }
        try {
          await this.runMaintenance(); // a no-op if the pool was closed meanwhile
        } catch {
          // a failed sweep must not kill the loop
        }
      }
    })();
  }

  private abortableDelay(seconds: number, signal: AbortSignal): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const onAbort = (): void => {
        clearTimeout(timer);
        reject(abortError(signal));
      };
      const timer = setTimeout(() => {
        signal.removeEventListener("abort", onAbort);
        resolve();
      }, seconds * 1000);
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  toString(): string {
    const s = this.stats();
    return `<AsyncConnectionPool '${this.name}' size=${s.size}/${s.maxSize} idle=${s.idle} inUse=${s.inUse}>`;
  }
}
