/** Tiny shared timeout helpers (internal). */

/** Sentinel: "use the configured default" (`null` already means "wait forever"). */
export const UNSET = Symbol("connpool.UNSET");
export type Unset = typeof UNSET;

/**
 * An acquire timeout: a number of seconds, `null` (wait forever), or {@link UNSET}
 * (use the pool's configured default).
 */
export type Timeout = number | null | Unset;

/** Resolve a timeout against the config default and `now` into an absolute deadline (or null). */
export function computeDeadline(
  timeout: Timeout,
  defaultSeconds: number | null,
  now: number,
): number | null {
  const resolved = timeout === UNSET ? defaultSeconds : timeout;
  return resolved === null ? null : now + resolved;
}

/** Seconds left until `deadline` (null = no deadline). */
export function remaining(deadline: number | null, now: number): number | null {
  return deadline === null ? null : deadline - now;
}
