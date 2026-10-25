package io.github.jeffeek.connpool;

/** Base class for every error raised by connpool. Catch this to handle anything the pool throws. */
public class PoolError extends RuntimeException {
    public PoolError(String message) {
        super(message);
    }

    public PoolError(String message, Throwable cause) {
        super(message, cause);
    }
}
