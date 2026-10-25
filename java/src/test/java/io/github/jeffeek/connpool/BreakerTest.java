package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BreakerTest {
    private ManualClock clock;
    private List<String> changes;

    private CircuitBreaker make(int failureThreshold, double recoveryTimeout, int halfOpenMaxCalls, int successThreshold) {
        clock = new ManualClock();
        changes = new ArrayList<>();
        BreakerConfig cfg = BreakerConfig.builder()
            .failureThreshold(failureThreshold)
            .recoveryTimeout(recoveryTimeout)
            .halfOpenMaxCalls(halfOpenMaxCalls)
            .successThreshold(successThreshold)
            .build();
        return new CircuitBreaker(cfg, clock, (o, n) -> changes.add(n.value()));
    }

    private CircuitBreaker make() {
        return make(3, 10, 1, 1);
    }

    private static void fail(CircuitBreaker b, int n) {
        for (int i = 0; i < n; i++) {
            b.beforeCall();
            b.recordFailure();
        }
    }

    @Test
    void opensAfterConsecutiveFailures() {
        CircuitBreaker b = make();
        fail(b, 3);
        assertEquals(BreakerState.OPEN, b.state());
        CircuitOpenError ex = assertThrows(CircuitOpenError.class, b::beforeCall);
        assertEquals(10.0, ex.getRetryAfter(), 0.01);
    }

    @Test
    void successResetsFailureStreak() {
        CircuitBreaker b = make();
        fail(b, 2);
        b.beforeCall();
        b.recordSuccess();
        fail(b, 2);
        assertEquals(BreakerState.CLOSED, b.state());
    }

    @Test
    void halfOpenThenClosesOnSuccess() {
        CircuitBreaker b = make();
        fail(b, 3);
        clock.advance(10);
        assertEquals(BreakerState.HALF_OPEN, b.state());
        b.beforeCall();
        b.recordSuccess();
        assertEquals(BreakerState.CLOSED, b.state());
        assertEquals(List.of("open", "half_open", "closed"), changes);
    }

    @Test
    void halfOpenFailureReopens() {
        CircuitBreaker b = make();
        fail(b, 3);
        clock.advance(10);
        b.beforeCall();
        b.recordFailure();
        assertEquals(BreakerState.OPEN, b.state());
    }

    @Test
    void halfOpenLimitsConcurrentProbes() {
        CircuitBreaker b = make(3, 10, 1, 1);
        fail(b, 3);
        clock.advance(10);
        b.beforeCall();
        assertThrows(CircuitOpenError.class, b::beforeCall);
    }

    @Test
    void successThreshold() {
        CircuitBreaker b = make(3, 10, 2, 2);
        fail(b, 3);
        clock.advance(10);
        b.beforeCall();
        b.recordSuccess();
        assertEquals(BreakerState.HALF_OPEN, b.state());
        b.beforeCall();
        b.recordSuccess();
        assertEquals(BreakerState.CLOSED, b.state());
    }

    @Test
    void callWrapperAndReset() {
        CircuitBreaker b = make(1, 10, 1, 1);
        assertThrows(ArithmeticException.class, () -> b.call(() -> 1 / Integer.parseInt("0")));
        assertEquals(BreakerState.OPEN, b.state());
        b.reset();
        assertEquals(42, (int) b.call(() -> 42));
    }

    @Test
    void callbackMayReenterBreaker() {
        ManualClock c = new ManualClock();
        CircuitBreaker[] holder = new CircuitBreaker[1];
        holder[0] = new CircuitBreaker(BreakerConfig.builder().failureThreshold(1).build(), c,
            (o, n) -> holder[0].state());
        holder[0].beforeCall();
        holder[0].recordFailure(); // would deadlock if the callback ran under the lock
    }

    @Test
    void recordAbortFreesProbeWithoutCounting() {
        CircuitBreaker b = make(3, 10, 1, 1);
        fail(b, 3);
        clock.advance(10);
        b.beforeCall();
        b.recordAbort();
        assertEquals(BreakerState.HALF_OPEN, b.state());
        b.beforeCall();
        b.recordSuccess();
        assertEquals(BreakerState.CLOSED, b.state());
    }

    @Test
    void abortInClosedStateIsNoop() {
        CircuitBreaker b = make();
        b.recordAbort();
        assertEquals(BreakerState.CLOSED, b.state());
    }

    @Test
    void resetWhenAlreadyClosedIsNoop() {
        CircuitBreaker b = make();
        b.reset();
        assertEquals(BreakerState.CLOSED, b.state());
        assertTrue(changes.isEmpty());
    }

    @Test
    void defaultsWhenConfigAndClockOmitted() {
        CircuitBreaker b = new CircuitBreaker();
        assertEquals(BreakerState.CLOSED, b.state());
        for (int i = 0; i < 5; i++) {
            b.recordFailure();
        }
        assertEquals(BreakerState.OPEN, b.state());
    }

    @Test
    void recordSuccessInClosedResetsFailures() {
        CircuitBreaker b = make();
        b.beforeCall();
        b.recordFailure();
        b.recordSuccess(); // closed-state success path
        assertEquals(BreakerState.CLOSED, b.state());
    }

    @Test
    void recordSuccessWhileOpenIsIgnored() {
        CircuitBreaker b = make();
        fail(b, 3);
        assertEquals(BreakerState.OPEN, b.state());
        b.recordSuccess(); // OPEN: neither closed nor half-open branch
        assertEquals(BreakerState.OPEN, b.state());
    }

    @Test
    void lateFailureWhileOpenIsIgnored() {
        CircuitBreaker b = make();
        fail(b, 3);
        assertEquals(BreakerState.OPEN, b.state());
        b.recordFailure(); // OPEN: ignored
        assertEquals(BreakerState.OPEN, b.state());
    }

    @Test
    void invalidConfig() {
        assertThrows(PoolConfigError.class, () -> BreakerConfig.builder().failureThreshold(0).build());
        assertThrows(PoolConfigError.class, () -> BreakerConfig.builder().recoveryTimeout(-1).build());
        assertThrows(PoolConfigError.class, () -> BreakerConfig.builder().halfOpenMaxCalls(0).build());
        assertThrows(PoolConfigError.class, () -> BreakerConfig.builder().successThreshold(0).build());
    }

    @Test
    void configAccessorsAndToBuilder() {
        BreakerConfig c = BreakerConfig.builder()
            .failureThreshold(4).recoveryTimeout(12).halfOpenMaxCalls(2).successThreshold(3).build();
        assertEquals(4, c.failureThreshold());
        assertEquals(12.0, c.recoveryTimeout());
        assertEquals(2, c.halfOpenMaxCalls());
        assertEquals(3, c.successThreshold());
        BreakerConfig copy = c.toBuilder().failureThreshold(7).build();
        assertEquals(7, copy.failureThreshold());
        assertEquals(12.0, copy.recoveryTimeout());
    }
}
