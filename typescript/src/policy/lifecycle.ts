/**
 * The pure "functional core": every rule about *when a connection must go away*.
 *
 * No locks, no I/O, no clocks of its own — just functions of (connection, config, now).
 */
import { CloseReason, PooledConnection } from "../core/connection.js";
import { type Rng } from "../resilience/retry.js";
import { IdleStrategy, type PoolConfig } from "./config.js";

export function newConnection<T>(
  raw: T,
  cfg: PoolConfig,
  now: number,
  rng: Rng,
): PooledConnection<T> {
  let expiresAt: number | null = null;
  if (cfg.maxLifetime !== null) {
    expiresAt = now + cfg.maxLifetime * (1.0 - cfg.lifetimeJitter * rng());
  }
  return new PooledConnection(raw, now, expiresAt);
}

/** Pop the next idle connection per the strategy, or null if there are none. Mutates `idle`. */
export function popIdle<T>(
  idle: Array<PooledConnection<T>>,
  strategy: IdleStrategy,
): PooledConnection<T> | null {
  if (idle.length === 0) {
    return null;
  }
  return (strategy === IdleStrategy.LIFO ? idle.pop() : idle.shift()) as PooledConnection<T>;
}

/** Connections that must be retired no matter what (lifetime / use-count rotation). */
export function rotationReason(
  c: PooledConnection<unknown>,
  cfg: PoolConfig,
  now: number,
): CloseReason | null {
  if (c.expiresAt !== null && now >= c.expiresAt) {
    return CloseReason.EXPIRED;
  }
  if (cfg.maxUses !== null && c.useCount >= cfg.maxUses) {
    return CloseReason.MAX_USES;
  }
  return null;
}

export function idleTooLong(c: PooledConnection<unknown>, cfg: PoolConfig, now: number): boolean {
  return cfg.maxIdleTime !== null && c.idleTime(now) >= cfg.maxIdleTime;
}

export function needsValidation(
  c: PooledConnection<unknown>,
  cfg: PoolConfig,
  now: number,
): boolean {
  return cfg.validateAfterIdle !== null && c.idleTime(now) >= cfg.validateAfterIdle;
}

/**
 * Static checks applied to an idle connection about to be handed out (validation, which
 * needs I/O, is done by the pool afterwards).
 */
export function borrowReason(
  c: PooledConnection<unknown>,
  cfg: PoolConfig,
  now: number,
): CloseReason | null {
  const reason = rotationReason(c, cfg, now);
  if (reason === null && idleTooLong(c, cfg, now)) {
    return CloseReason.IDLE;
  }
  return reason;
}

/**
 * Split idle connections into (keep, doomed). Rotation victims always go; idle-timeout
 * victims go only while the pool stays at or above `minSize`.
 */
export function sweep<T>(
  idle: Iterable<PooledConnection<T>>,
  totalSize: number,
  cfg: PoolConfig,
  now: number,
): { keep: Array<PooledConnection<T>>; doomed: Array<[PooledConnection<T>, CloseReason]> } {
  const keep: Array<PooledConnection<T>> = [];
  const doomed: Array<[PooledConnection<T>, CloseReason]> = [];
  let remaining = totalSize;
  for (const c of idle) {
    let reason = rotationReason(c, cfg, now);
    if (reason === null && idleTooLong(c, cfg, now) && remaining > cfg.minSize) {
      reason = CloseReason.IDLE;
    }
    if (reason === null) {
      keep.push(c);
    } else {
      doomed.push([c, reason]);
      remaining -= 1;
    }
  }
  return { keep, doomed };
}
