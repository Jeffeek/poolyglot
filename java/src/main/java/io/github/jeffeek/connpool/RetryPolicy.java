package io.github.jeffeek.connpool;

import java.util.Random;
import java.util.function.Predicate;

/**
 * How to retry {@code factory.create()}: exponential backoff with jitter.
 *
 * <p>{@code jitter} in [0, 1]: 0 = deterministic delays, 1 = "full jitter" (uniform in [0, delay]).
 * Spreading retries avoids thundering herds. {@code retryOn} narrows what is retryable (e.g. don't
 * retry auth failures).
 */
public record RetryPolicy(
    int maxAttempts,
    double baseDelay,
    double maxDelay,
    double multiplier,
    double jitter,
    Predicate<Throwable> retryOn) {
  public RetryPolicy {
    if (maxAttempts < 1) {
      throw new PoolConfigError("retry.max_attempts must be >= 1");
    }
    if (baseDelay < 0 || maxDelay < 0 || multiplier < 1) {
      throw new PoolConfigError("retry delays must be >= 0 and multiplier >= 1");
    }
    if (jitter < 0.0 || jitter > 1.0) {
      throw new PoolConfigError("retry.jitter must be within [0, 1]");
    }
  }

  /** A policy that never retries (one attempt only). */
  public static final RetryPolicy NO_RETRY = builder().maxAttempts(1).build();

  /** The default policy: 3 attempts, 0.1s base, x2, 5s cap, 0.5 jitter, retry everything. */
  public static RetryPolicy defaults() {
    return builder().build();
  }

  public boolean shouldRetry(Throwable exc) {
    return retryOn.test(exc);
  }

  /** Delay before the next attempt, given how many attempts have failed so far. */
  public double delayFor(int failedAttempts, Random rng) {
    // baseDelay == 0 means no backoff; avoid 0 * Inf = NaN when multiplier^(n-1) overflows.
    double ceiling =
        baseDelay == 0.0
            ? 0.0
            : Math.min(maxDelay, baseDelay * Math.pow(multiplier, failedAttempts - 1));
    return ceiling * (1.0 - jitter + jitter * rng.nextDouble());
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder()
        .maxAttempts(maxAttempts)
        .baseDelay(baseDelay)
        .maxDelay(maxDelay)
        .multiplier(multiplier)
        .jitter(jitter)
        .retryOn(retryOn);
  }

  public static final class Builder {
    private int maxAttempts = 3;
    private double baseDelay = 0.1;
    private double maxDelay = 5.0;
    private double multiplier = 2.0;
    private double jitter = 0.5;
    private Predicate<Throwable> retryOn = exc -> true;

    public Builder maxAttempts(int value) {
      this.maxAttempts = value;
      return this;
    }

    public Builder baseDelay(double value) {
      this.baseDelay = value;
      return this;
    }

    public Builder maxDelay(double value) {
      this.maxDelay = value;
      return this;
    }

    public Builder multiplier(double value) {
      this.multiplier = value;
      return this;
    }

    public Builder jitter(double value) {
      this.jitter = value;
      return this;
    }

    public Builder retryOn(Predicate<Throwable> value) {
      this.retryOn = value;
      return this;
    }

    public RetryPolicy build() {
      return new RetryPolicy(maxAttempts, baseDelay, maxDelay, multiplier, jitter, retryOn);
    }
  }
}
