/**
 * Exception hierarchy. Catch {@link PoolError} to handle anything the pool raises.
 *
 * The taxonomy is identical across every poolyglot language (see python/csharp/java/go).
 */

/** Base class for every error raised by connpool. */
export class PoolError extends Error {
  constructor(message?: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "PoolError";
  }
}

/** Invalid pool / breaker / retry configuration. */
export class PoolConfigError extends PoolError {
  constructor(message?: string) {
    super(message);
    this.name = "PoolConfigError";
  }
}

/** The pool has been closed; no more connections can be acquired. */
export class PoolClosedError extends PoolError {
  constructor(message?: string) {
    super(message);
    this.name = "PoolClosedError";
  }
}

/** No connection became available before the acquire deadline. */
export class PoolTimeoutError extends PoolError {
  constructor(message?: string) {
    super(message);
    this.name = "PoolTimeoutError";
  }
}

/** The pool is full and the waiter queue is at `maxWaiters`; shedding load. */
export class PoolExhaustedError extends PoolError {
  constructor(message?: string) {
    super(message);
    this.name = "PoolExhaustedError";
  }
}

/**
 * The factory failed to create a connection (after all retries).
 * The original error is available as `.cause`.
 */
export class ConnectionCreateError extends PoolError {
  constructor(message?: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "ConnectionCreateError";
  }
}

/** The circuit breaker is rejecting calls to protect a failing backend. */
export class CircuitOpenError extends PoolError {
  readonly retryAfter: number;

  constructor(retryAfter: number) {
    super(`circuit breaker is open; retry in ~${retryAfter.toFixed(1)}s`);
    this.name = "CircuitOpenError";
    this.retryAfter = retryAfter;
  }
}

/** Released an object that is not checked out from this pool (double release?). */
export class InvalidReleaseError extends PoolError {
  constructor(message?: string) {
    super(message);
    this.name = "InvalidReleaseError";
  }
}
