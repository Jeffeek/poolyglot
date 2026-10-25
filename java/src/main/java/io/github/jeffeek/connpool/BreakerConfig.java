package io.github.jeffeek.connpool;

/**
 * Circuit breaker configuration. Immutable and validated; use {@link #builder()} / {@link
 * #toBuilder()} for construction and copy-with-override (both re-validate).
 */
public record BreakerConfig(
    int failureThreshold, // consecutive failures that trip the breaker
    double recoveryTimeout, // seconds to stay OPEN before probing
    int halfOpenMaxCalls, // concurrent probe calls allowed while HALF_OPEN
    int successThreshold // probe successes needed to close again
    ) {
  public BreakerConfig {
    if (failureThreshold < 1 || halfOpenMaxCalls < 1 || successThreshold < 1) {
      throw new PoolConfigError("breaker thresholds must be >= 1");
    }
    if (recoveryTimeout < 0) {
      throw new PoolConfigError("breaker.recovery_timeout must be >= 0");
    }
  }

  /** The default configuration: 5 failures / 30s recovery / 1 probe / 1 success. */
  public static BreakerConfig defaults() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder()
        .failureThreshold(failureThreshold)
        .recoveryTimeout(recoveryTimeout)
        .halfOpenMaxCalls(halfOpenMaxCalls)
        .successThreshold(successThreshold);
  }

  public static final class Builder {
    private int failureThreshold = 5;
    private double recoveryTimeout = 30.0;
    private int halfOpenMaxCalls = 1;
    private int successThreshold = 1;

    public Builder failureThreshold(int value) {
      this.failureThreshold = value;
      return this;
    }

    public Builder recoveryTimeout(double value) {
      this.recoveryTimeout = value;
      return this;
    }

    public Builder halfOpenMaxCalls(int value) {
      this.halfOpenMaxCalls = value;
      return this;
    }

    public Builder successThreshold(int value) {
      this.successThreshold = value;
      return this;
    }

    public BreakerConfig build() {
      return new BreakerConfig(
          failureThreshold, recoveryTimeout, halfOpenMaxCalls, successThreshold);
    }
  }
}
