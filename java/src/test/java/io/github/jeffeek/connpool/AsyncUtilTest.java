package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class AsyncUtilTest {
    @Test
    void holderInstantiable() {
        new AsyncAwait();
    }

    @Test
    void joinSuccess() {
        assertEquals("ok", AsyncAwait.join(CompletableFuture.completedFuture("ok")));
    }

    @Test
    void joinUnwrapsRuntimeException() {
        CompletableFuture<Object> f = new CompletableFuture<>();
        f.completeExceptionally(new TestSupport.ConnErr("boom"));
        assertThrows(TestSupport.ConnErr.class, () -> AsyncAwait.join(f));
    }

    @Test
    void joinRecursesThroughCompletionException() {
        CompletableFuture<Object> f = new CompletableFuture<>();
        f.completeExceptionally(new CompletionException(new TestSupport.ConnErr("inner")));
        assertThrows(TestSupport.ConnErr.class, () -> AsyncAwait.join(f));
    }

    @Test
    void joinRethrowsError() {
        CompletableFuture<Object> f = new CompletableFuture<>();
        f.completeExceptionally(new AssertionError("err"));
        assertThrows(AssertionError.class, () -> AsyncAwait.join(f));
    }

    @Test
    void joinWrapsCheckedException() {
        CompletableFuture<Object> f = new CompletableFuture<>();
        f.completeExceptionally(new IOException("io"));
        assertThrows(CompletionException.class, () -> AsyncAwait.join(f));
    }

    @Test
    void joinInterrupted() {
        Thread.currentThread().interrupt();
        assertThrows(CancellationException.class, () -> AsyncAwait.join(new CompletableFuture<>()));
        Thread.interrupted(); // clear the flag
    }

    @Test
    void awaitNotCancellableJoins() {
        assertEquals("x", AsyncAwait.await(CompletableFuture.completedFuture("x"), CancelToken.NONE));
    }

    @Test
    void awaitAlreadyCancelled() {
        CancelTokenSource cts = new CancelTokenSource();
        cts.cancel();
        assertThrows(CancellationException.class,
            () -> AsyncAwait.await(new CompletableFuture<>(), cts.token()));
    }

    @Test
    void awaitSuccessWithLiveToken() {
        CancelTokenSource cts = new CancelTokenSource();
        assertEquals("y", AsyncAwait.await(CompletableFuture.completedFuture("y"), cts.token()));
    }

    @Test
    void awaitFutureFailsWithLiveToken() {
        CancelTokenSource cts = new CancelTokenSource();
        CompletableFuture<Object> f = new CompletableFuture<>();
        f.completeExceptionally(new TestSupport.ConnErr("nope"));
        assertThrows(TestSupport.ConnErr.class, () -> AsyncAwait.await(f, cts.token()));
    }

    @Test
    void awaitCancelledWhileBlocked() throws Exception {
        CancelTokenSource cts = new CancelTokenSource();
        Thread canceller = new Thread(() -> {
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            cts.cancel();
        });
        canceller.start();
        assertThrows(CancellationException.class,
            () -> AsyncAwait.await(new CompletableFuture<>(), cts.token()));
        canceller.join();
    }
}
