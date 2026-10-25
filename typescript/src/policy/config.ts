/** Pool configuration: one immutable, validated object. */
import { PoolConfigError } from "../core/errors.js";
import { BreakerConfig } from "../resilience/breaker.js";
import { RetryPolicy } from "../resilience/retry.js";

/** Which idle connection to reuse next. Values are the shared snake_case wire strings. */
export const IdleStrategy = {
  LIFO: "lifo", // reuse the hottest connection; cold ones age out and get evicted
  FIFO: "fifo", // reuse the oldest idle connection; spreads load / rotates evenly
} as const;

export type IdleStrategy = (typeof IdleStrategy)[keyof typeof IdleStrategy];

export interface PoolConfigOptions {
  minSize?: number;
  maxSize?: number;
  acquireTimeout?: number | null;
  createTimeout?: number | null;
  maxWaiters?: number | null;
  maxLifetime?: number | null;
  lifetimeJitter?: number;
  maxIdleTime?: number | null;
  maxUses?: number | null;
  idleStrategy?: IdleStrategy;
  validateAfterIdle?: number | null;
  maintenanceInterval?: number | null;
  leakDetectionTimeout?: number | null;
  validateIdle?: boolean;
  retry?: RetryPolicy;
  breaker?: BreakerConfig | null;
}

/** `val` if explicitly provided (even as `null`), otherwise `fallback`. */
function def<V>(val: V | undefined, fallback: V): V {
  return val !== undefined ? val : fallback;
}

export class PoolConfig {
  // --- sizing ---
  readonly minSize: number;
  readonly maxSize: number;

  // --- timeouts (seconds; null = no limit) ---
  readonly acquireTimeout: number | null;
  readonly createTimeout: number | null;
  readonly maxWaiters: number | null;

  // --- rotation / recycling ---
  readonly maxLifetime: number | null;
  readonly lifetimeJitter: number;
  readonly maxIdleTime: number | null;
  readonly maxUses: number | null;
  readonly idleStrategy: IdleStrategy;

  // --- health ---
  readonly validateAfterIdle: number | null;
  readonly maintenanceInterval: number | null;
  readonly leakDetectionTimeout: number | null;
  readonly validateIdle: boolean;

  // --- resilience ---
  readonly retry: RetryPolicy;
  readonly breaker: BreakerConfig | null;

  constructor(options: PoolConfigOptions = {}) {
    this.minSize = def(options.minSize, 0);
    this.maxSize = def(options.maxSize, 10);
    this.acquireTimeout = def(options.acquireTimeout, 30.0);
    this.createTimeout = def(options.createTimeout, 10.0);
    this.maxWaiters = def(options.maxWaiters, null);
    this.maxLifetime = def(options.maxLifetime, 1800.0);
    this.lifetimeJitter = def(options.lifetimeJitter, 0.1);
    this.maxIdleTime = def(options.maxIdleTime, 600.0);
    this.maxUses = def(options.maxUses, null);
    this.idleStrategy = def(options.idleStrategy, IdleStrategy.LIFO);
    this.validateAfterIdle = def(options.validateAfterIdle, 30.0);
    this.maintenanceInterval = def(options.maintenanceInterval, 30.0);
    this.leakDetectionTimeout = def(options.leakDetectionTimeout, null);
    this.validateIdle = def(options.validateIdle, false);
    this.retry = def(options.retry, new RetryPolicy());
    this.breaker = def(options.breaker, new BreakerConfig());

    const need = (ok: boolean, msg: string): void => {
      if (!ok) {
        throw new PoolConfigError(msg);
      }
    };

    need(this.maxSize >= 1, "maxSize must be >= 1");
    need(this.minSize >= 0 && this.minSize <= this.maxSize, "minSize must be within [0, maxSize]");
    need(this.acquireTimeout === null || this.acquireTimeout >= 0, "acquireTimeout must be >= 0");
    const positiveOrNull: Array<[string, number | null]> = [
      ["createTimeout", this.createTimeout],
      ["maxLifetime", this.maxLifetime],
      ["maxIdleTime", this.maxIdleTime],
      ["maintenanceInterval", this.maintenanceInterval],
      ["leakDetectionTimeout", this.leakDetectionTimeout],
    ];
    for (const [name, value] of positiveOrNull) {
      need(value === null || value > 0, `${name} must be > 0 or null`);
    }
    need(
      this.validateAfterIdle === null || this.validateAfterIdle >= 0,
      "validateAfterIdle must be >= 0 or null",
    );
    need(
      this.lifetimeJitter >= 0.0 && this.lifetimeJitter <= 1.0,
      "lifetimeJitter must be within [0, 1]",
    );
    need(this.maxUses === null || this.maxUses >= 1, "maxUses must be >= 1 or null");
    need(this.maxWaiters === null || this.maxWaiters >= 0, "maxWaiters must be >= 0 or null");
  }

  /** Return a modified copy (the config itself is immutable). Re-validates. */
  replace(changes: PoolConfigOptions): PoolConfig {
    return new PoolConfig({
      minSize: def(changes.minSize, this.minSize),
      maxSize: def(changes.maxSize, this.maxSize),
      acquireTimeout: def(changes.acquireTimeout, this.acquireTimeout),
      createTimeout: def(changes.createTimeout, this.createTimeout),
      maxWaiters: def(changes.maxWaiters, this.maxWaiters),
      maxLifetime: def(changes.maxLifetime, this.maxLifetime),
      lifetimeJitter: def(changes.lifetimeJitter, this.lifetimeJitter),
      maxIdleTime: def(changes.maxIdleTime, this.maxIdleTime),
      maxUses: def(changes.maxUses, this.maxUses),
      idleStrategy: def(changes.idleStrategy, this.idleStrategy),
      validateAfterIdle: def(changes.validateAfterIdle, this.validateAfterIdle),
      maintenanceInterval: def(changes.maintenanceInterval, this.maintenanceInterval),
      leakDetectionTimeout: def(changes.leakDetectionTimeout, this.leakDetectionTimeout),
      validateIdle: def(changes.validateIdle, this.validateIdle),
      retry: def(changes.retry, this.retry),
      breaker: def(changes.breaker, this.breaker),
    });
  }
}
