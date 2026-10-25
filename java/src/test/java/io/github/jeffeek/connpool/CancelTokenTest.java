package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CancelTokenTest {
    @Test
    void noneNeverCancels() {
        assertFalse(CancelToken.NONE.isCancellationRequested());
        assertFalse(CancelToken.NONE.canBeCancelled());
        CancelToken.NONE.throwIfCancellationRequested(); // no-op
        CancelToken.NONE.register(() -> { }).close();    // Registration.NOOP
    }

    @Test
    void cancelFiresRegisteredCallbacks() {
        CancelTokenSource cts = new CancelTokenSource();
        AtomicInteger n = new AtomicInteger();
        cts.token().register(n::incrementAndGet);
        assertFalse(cts.token().isCancellationRequested());
        cts.cancel();
        assertTrue(cts.token().isCancellationRequested());
        assertEquals(1, n.get());
        cts.cancel(); // idempotent
        assertEquals(1, n.get());
        assertThrows(CancellationException.class, () -> cts.token().throwIfCancellationRequested());
    }

    @Test
    void registerAfterCancelRunsImmediately() {
        CancelTokenSource cts = new CancelTokenSource();
        cts.cancel();
        AtomicInteger n = new AtomicInteger();
        Registration reg = cts.token().register(n::incrementAndGet);
        assertEquals(1, n.get());
        reg.close(); // NOOP
    }

    @Test
    void unregisterBeforeCancelDetaches() {
        CancelTokenSource cts = new CancelTokenSource();
        AtomicInteger n = new AtomicInteger();
        Registration reg = cts.token().register(n::incrementAndGet);
        reg.close();          // detaches (callbacks still non-null)
        cts.cancel();
        assertEquals(0, n.get());
    }

    @Test
    void unregisterAfterCancelIsSafe() {
        CancelTokenSource cts = new CancelTokenSource();
        Registration reg = cts.token().register(() -> { });
        cts.cancel();         // callbacks list cleared
        reg.close();          // hits the callbacks == null branch, no throw
    }

    @Test
    void linkedCancelsWithParent() {
        CancelTokenSource parent = new CancelTokenSource();
        CancelTokenSource linked = CancelTokenSource.linkedWithTimeout(parent.token(), null);
        parent.cancel();
        assertTrue(linked.token().isCancellationRequested());
        linked.close(); // timer == null branch
    }

    @Test
    void linkedWithNonCancellableParentAndTimerClose() {
        CancelTokenSource linked = CancelTokenSource.linkedWithTimeout(CancelToken.NONE, 10.0);
        assertFalse(linked.token().isCancellationRequested());
        linked.close(); // timer != null branch; parentReg == NOOP
    }

    @Test
    void linkedTimerFires() {
        CancelTokenSource linked = CancelTokenSource.linkedWithTimeout(CancelToken.NONE, 0.0);
        TestSupport.spinUntil(() -> linked.token().isCancellationRequested());
        assertTrue(linked.token().isCancellationRequested());
        linked.close();
    }
}
