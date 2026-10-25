package io.github.jeffeek.connpool;

import java.util.concurrent.CompletableFuture;

/** Asynchronous factory contract: async twin of {@link ConnectionFactory}. */
public abstract class AsyncConnectionFactory<T> {
    public abstract CompletableFuture<T> createAsync(CancelToken token);

    public CompletableFuture<Boolean> validateAsync(T conn, CancelToken token) {
        return CompletableFuture.completedFuture(true);
    }

    public CompletableFuture<Void> resetAsync(T conn, CancelToken token) {
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> closeAsync(T conn, CancelToken token) {
        if (conn instanceof AutoCloseable) {
            try {
                ((AutoCloseable) conn).close();
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    public boolean isFatalError(Throwable exc) {
        return false;
    }
}
