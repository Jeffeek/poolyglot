package io.github.jeffeek.connpool;

import java.util.concurrent.CancellationException;

/**
 * Cooperative cancellation signal, the Java analog of .NET's {@code CancellationToken}.
 * Pass one to the async pool / factory to cancel a pending acquire, release or create.
 */
public final class CancelToken {
    /** A token that can never be cancelled. */
    public static final CancelToken NONE = new CancelToken(null);

    private final CancelTokenSource source;

    CancelToken(CancelTokenSource source) {
        this.source = source;
    }

    public boolean isCancellationRequested() {
        return source != null && source.isCancelled();
    }

    /** False for {@link #NONE}: there is nothing that could ever fire it. */
    public boolean canBeCancelled() {
        return source != null;
    }

    public void throwIfCancellationRequested() {
        if (isCancellationRequested()) {
            throw new CancellationException();
        }
    }

    /**
     * Run {@code callback} when this token is cancelled (immediately if already cancelled).
     * Returns a {@link Registration} that detaches the callback when closed.
     */
    public Registration register(Runnable callback) {
        return source == null ? Registration.NOOP : source.register(callback);
    }
}
