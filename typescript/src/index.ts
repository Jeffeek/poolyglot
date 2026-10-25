/**
 * connpool — a generic async connection pool for Node.js, zero runtime dependencies.
 *
 * Layers (each only depends on the ones above it):
 *
 *     core           errors, clocks, connection bookkeeping
 *     resilience     circuit breaker, retry/backoff
 *     policy         PoolConfig + the pure retirement rules
 *     factories      AsyncConnectionFactory: the seam users implement to pool anything
 *     observability  listeners + stats
 *     pools          AsyncConnectionPool, AsyncPoolRegistry
 *
 * This module is the **public API facade**: everything users need can be imported straight from
 * `@jeffeek/connpool` so that internal modules can move without breaking anyone.
 *
 * Note: unlike the Python/C#/Java/Go ports, there is no *synchronous* pool — Node's single event
 * loop has no thread-blocking acquire. Use the async pool.
 */

// --- errors ---
export {
  CircuitOpenError,
  ConnectionCreateError,
  InvalidReleaseError,
  PoolClosedError,
  PoolConfigError,
  PoolError,
  PoolExhaustedError,
  PoolTimeoutError,
} from "./core/errors.js";

// --- core: clocks + connection + timeouts ---
export { type Clock, ManualClock, SystemClock } from "./core/clock.js";
export { CloseReason, PooledConnection } from "./core/connection.js";
export { type Timeout, UNSET } from "./core/timeouts.js";

// --- resilience ---
export {
  BreakerConfig,
  type BreakerConfigOptions,
  BreakerState,
  CircuitBreaker,
  type CircuitBreakerOptions,
} from "./resilience/breaker.js";
export { NO_RETRY, type Rng, RetryPolicy, type RetryPolicyOptions } from "./resilience/retry.js";

// --- policy ---
export { IdleStrategy, PoolConfig, type PoolConfigOptions } from "./policy/config.js";

// --- factories ---
export {
  AsyncCallableFactory,
  type AsyncCallableFactoryOptions,
  AsyncConnectionFactory,
} from "./factories/async-factory.js";

// --- observability ---
export { type PoolListener } from "./observability/events.js";
export { PoolStats } from "./observability/stats.js";

// --- pools ---
export {
  type AcquireOptions,
  AsyncConnectionPool,
  type AsyncConnectionPoolOptions,
  type FromCallablesOptions,
  type ReleaseOptions,
} from "./pools/async-pool.js";
export { AsyncPoolRegistry } from "./pools/registry.js";
export { CancelledError } from "./pools/async-primitives.js";
