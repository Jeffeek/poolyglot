/** Retry policy for connection creation: exponential backoff with jitter. */
import { PoolConfigError } from "../core/errors.js";

/** A source of uniform randomness in [0, 1), injectable for deterministic tests. */
export type Rng = () => number;

export interface RetryPolicyOptions {
  maxAttempts?: number;
  baseDelay?: number;
  maxDelay?: number;
  multiplier?: number;
  /** In [0, 1]: 0 = deterministic delays, 1 = "full jitter" (uniform in [0, delay]). */
  jitter?: number;
  /** Narrows what is retryable (e.g. don't retry auth failures). Default: retry everything. */
  retryOn?: (error: unknown) => boolean;
}

/**
 * How to retry `factory.create()`. Immutable; use {@link RetryPolicy.replace} for a modified copy.
 */
export class RetryPolicy {
  readonly maxAttempts: number;
  readonly baseDelay: number;
  readonly maxDelay: number;
  readonly multiplier: number;
  readonly jitter: number;
  readonly retryOn: (error: unknown) => boolean;

  constructor(options: RetryPolicyOptions = {}) {
    this.maxAttempts = options.maxAttempts ?? 3;
    this.baseDelay = options.baseDelay ?? 0.1;
    this.maxDelay = options.maxDelay ?? 5.0;
    this.multiplier = options.multiplier ?? 2.0;
    this.jitter = options.jitter ?? 0.5;
    this.retryOn = options.retryOn ?? (() => true);

    if (this.maxAttempts < 1) {
      throw new PoolConfigError("retry.maxAttempts must be >= 1");
    }
    if (this.baseDelay < 0 || this.maxDelay < 0 || this.multiplier < 1) {
      throw new PoolConfigError("retry delays must be >= 0 and multiplier >= 1");
    }
    if (!(this.jitter >= 0.0 && this.jitter <= 1.0)) {
      throw new PoolConfigError("retry.jitter must be within [0, 1]");
    }
  }

  shouldRetry(error: unknown): boolean {
    return this.retryOn(error);
  }

  /** Delay before the next attempt, given how many attempts have failed so far. */
  delayFor(failedAttempts: number, rng: Rng): number {
    const ceiling = Math.min(
      this.maxDelay,
      this.baseDelay * this.multiplier ** (failedAttempts - 1),
    );
    return ceiling * (1.0 - this.jitter + this.jitter * rng());
  }

  replace(changes: RetryPolicyOptions): RetryPolicy {
    return new RetryPolicy({
      maxAttempts: changes.maxAttempts ?? this.maxAttempts,
      baseDelay: changes.baseDelay ?? this.baseDelay,
      maxDelay: changes.maxDelay ?? this.maxDelay,
      multiplier: changes.multiplier ?? this.multiplier,
      jitter: changes.jitter ?? this.jitter,
      retryOn: changes.retryOn ?? this.retryOn,
    });
  }
}

export const NO_RETRY = new RetryPolicy({ maxAttempts: 1 });
