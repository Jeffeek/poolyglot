package io.github.jeffeek.connpool;

import java.util.Locale;

/** The circuit breaker is rejecting calls to protect a failing backend. */
public final class CircuitOpenError extends PoolError {
    private final double retryAfter;

    public CircuitOpenError(double retryAfter) {
        super(String.format(Locale.ROOT, "circuit breaker is open; retry in ~%.1fs", retryAfter));
        this.retryAfter = retryAfter;
    }

    /** Approximate seconds until the breaker will probe again. */
    public double getRetryAfter() {
        return retryAfter;
    }
}
