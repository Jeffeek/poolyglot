package io.github.jeffeek.connpool;

/** Released an object that is not checked out from this pool (double release?). */
public final class InvalidReleaseError extends PoolError {
    public InvalidReleaseError(String message) {
        super(message);
    }
}
