/**
 * Observability hooks. Implement {@link PoolListener} and provide the hooks you care about
 * (metrics, tracing, logging). Hooks run synchronously, outside the pool's critical sections,
 * so keep them fast. Exceptions thrown by listeners are swallowed — a broken metrics exporter
 * must never break the pool.
 */
import type { BreakerState } from "../resilience/breaker.js";
import type { CloseReason, PooledConnection } from "../core/connection.js";

export interface PoolListener {
  onConnectionCreated?(conn: PooledConnection<unknown>, duration: number): void;
  onConnectionClosed?(conn: PooledConnection<unknown>, reason: CloseReason): void;
  onCreateFailed?(error: unknown): void;
  onAcquired?(conn: PooledConnection<unknown>, waitTime: number): void;
  onReleased?(conn: PooledConnection<unknown>, holdTime: number): void;
  onAcquireTimeout?(waited: number): void;
  onConnectionLeak?(conn: PooledConnection<unknown>, heldFor: number): void;
  onBreakerStateChange?(oldState: BreakerState, newState: BreakerState): void;
}

type HookArgs<K extends keyof PoolListener> = Parameters<NonNullable<PoolListener[K]>>;

export class ListenerGroup {
  private readonly listeners: PoolListener[];

  constructor(listeners: Iterable<PoolListener>) {
    this.listeners = [...listeners];
  }

  dispatch<K extends keyof PoolListener>(hook: K, ...args: HookArgs<K>): void {
    for (const listener of this.listeners) {
      const fn = listener[hook];
      if (typeof fn === "function") {
        try {
          (fn as (...a: HookArgs<K>) => void).apply(listener, args);
        } catch {
          // listeners must never break the pool
        }
      }
    }
  }
}
