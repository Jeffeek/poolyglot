/** Bookkeeping wrapper around a raw connection. */

/**
 * Why a connection left the pool. The values are the snake_case **wire strings** shared by
 * every poolyglot language — copy them verbatim, never localize them.
 */
export const CloseReason = {
  EXPIRED: "expired", // max_lifetime reached (rotation)
  MAX_USES: "max_uses", // max_uses reached (rotation)
  IDLE: "idle", // sat idle longer than max_idle_time
  UNHEALTHY: "unhealthy", // failed validation
  ERROR: "error", // caller's block raised and the connection is suspect
  RESET_FAILED: "reset_failed", // factory.reset() raised on release
  DISCARDED: "discarded", // caller asked for it
  POOL_CLOSED: "pool_closed",
} as const;

export type CloseReason = (typeof CloseReason)[keyof typeof CloseReason];

let nextId = 1;

/** A raw connection plus the metadata the pool needs to manage it. Identity-tracked. */
export class PooledConnection<T> {
  readonly raw: T;
  readonly createdAt: number;
  expiresAt: number | null;
  lastReturnedAt: number;
  checkedOutAt: number | null;
  useCount: number;
  leakReported: boolean;
  readonly id: number;

  constructor(
    raw: T,
    createdAt: number,
    expiresAt: number | null = null,
    lastReturnedAt: number | null = null,
  ) {
    this.raw = raw;
    this.createdAt = createdAt;
    this.expiresAt = expiresAt;
    this.lastReturnedAt = lastReturnedAt ?? createdAt;
    this.checkedOutAt = null;
    this.useCount = 0;
    this.leakReported = false;
    this.id = nextId++;
  }

  age(now: number): number {
    return now - this.createdAt;
  }

  idleTime(now: number): number {
    return now - this.lastReturnedAt;
  }

  holdTime(now: number): number {
    return this.checkedOutAt === null ? 0 : now - this.checkedOutAt;
  }

  markCheckedOut(now: number): void {
    this.checkedOutAt = now;
    this.useCount += 1;
    this.leakReported = false;
  }

  markReturned(now: number): void {
    this.checkedOutAt = null;
    this.lastReturnedAt = now;
  }

  toString(): string {
    return `<PooledConnection #${this.id} uses=${this.useCount}>`;
  }
}
