package io.github.jeffeek.connpool;

import java.util.concurrent.CompletableFuture;

/** Time source abstraction so every time-dependent rule is deterministic in tests. */
public interface Clock {
    /** A monotonic clock reading in seconds (only differences are meaningful). */
    double monotonic();

    /** Block the current thread for {@code seconds} (no-op for seconds &lt;= 0). */
    void sleep(double seconds);

    /**
     * Asynchronous sleep: the returned future completes after {@code seconds}, or completes
     * exceptionally with {@link java.util.concurrent.CancellationException} if {@code token} fires.
     */
    CompletableFuture<Void> asyncSleep(double seconds, CancelToken token);
}
