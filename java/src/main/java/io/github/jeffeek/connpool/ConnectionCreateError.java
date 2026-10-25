package io.github.jeffeek.connpool;

/**
 * The factory failed to create a connection (after all retries).
 * The original exception is available via {@link Throwable#getCause()}.
 */
public final class ConnectionCreateError extends PoolError {
    public ConnectionCreateError(String message) {
        super(message);
    }

    public ConnectionCreateError(String message, Throwable cause) {
        super(message, cause);
    }
}
