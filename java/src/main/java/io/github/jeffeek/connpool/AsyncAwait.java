package io.github.jeffeek.connpool;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Block (on an async-pool worker thread) for a {@link CompletableFuture}, with optional cancellation. */
final class AsyncAwait {
    AsyncAwait() {
    }

    /** Wait for {@code f} to complete; not cancellable (used for cleanup that must finish). */
    static <X> X join(CompletableFuture<X> f) {
        try {
            return f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CancellationException();
        } catch (ExecutionException e) {
            throw rethrow(e.getCause());
        }
    }

    /**
     * Wait for {@code f}, but abort with {@link CancellationException} if {@code token} fires first
     * ({@code f} keeps running in the background). The twin of {@code asyncio.shield} + cancellation.
     */
    static <X> X await(CompletableFuture<X> f, CancelToken token) {
        if (!token.canBeCancelled()) {
            return join(f);
        }
        if (token.isCancellationRequested()) {
            throw new CancellationException();
        }
        CompletableFuture<X> cancelF = new CompletableFuture<>();
        Registration reg = token.register(() -> cancelF.completeExceptionally(new CancellationException()));
        try {
            return join(f.applyToEither(cancelF, x -> x));
        } finally {
            reg.close();
        }
    }

    private static RuntimeException rethrow(Throwable t) {
        // Future.get() already unwraps one CompletionException layer into the ExecutionException cause.
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        return new CompletionException(t);
    }
}
