package io.github.jeffeek.connpool;

/** The pool has been closed; no more connections can be acquired. */
public final class PoolClosedError extends PoolError {
    public PoolClosedError(String message) {
        super(message);
    }
}
