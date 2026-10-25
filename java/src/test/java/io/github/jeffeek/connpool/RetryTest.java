package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import org.junit.jupiter.api.Test;

class RetryTest {
    @Test
    void invalidConfig() {
        assertThrows(PoolConfigError.class, () -> RetryPolicy.builder().maxAttempts(0).build());
        assertThrows(PoolConfigError.class, () -> RetryPolicy.builder().baseDelay(-1).build());
        assertThrows(PoolConfigError.class, () -> RetryPolicy.builder().maxDelay(-1).build());
        assertThrows(PoolConfigError.class, () -> RetryPolicy.builder().multiplier(0.5).build());
        assertThrows(PoolConfigError.class, () -> RetryPolicy.builder().jitter(2.0).build());
        assertThrows(PoolConfigError.class, () -> RetryPolicy.builder().jitter(-0.5).build());
    }

    @Test
    void backoff() {
        RetryPolicy p = RetryPolicy.builder()
            .maxAttempts(5).baseDelay(1).maxDelay(5).multiplier(2).jitter(0).build();
        Random rng = new Random(0);
        assertEquals(1.0, p.delayFor(1, rng));
        assertEquals(2.0, p.delayFor(2, rng));
        assertEquals(4.0, p.delayFor(3, rng));
        assertEquals(5.0, p.delayFor(4, rng)); // capped at maxDelay

        RetryPolicy full = RetryPolicy.builder().baseDelay(4).jitter(1.0).build();
        for (int i = 0; i < 100; i++) {
            double d = full.delayFor(1, rng);
            assertTrue(d >= 0.0 && d <= 4.0);
        }
    }

    @Test
    void retryOnFilter() {
        RetryPolicy p = RetryPolicy.builder().retryOn(e -> e instanceof TestSupport.ConnErr).build();
        assertTrue(p.shouldRetry(new TestSupport.ConnReset("x")));
        assertFalse(p.shouldRetry(new IllegalArgumentException()));
    }

    @Test
    void accessorsDefaultsAndNoRetry() {
        RetryPolicy p = RetryPolicy.builder()
            .maxAttempts(4).baseDelay(0.2).maxDelay(4).multiplier(3).jitter(0.25).build();
        assertEquals(4, p.maxAttempts());
        assertEquals(0.2, p.baseDelay());
        assertEquals(4.0, p.maxDelay());
        assertEquals(3.0, p.multiplier());
        assertEquals(0.25, p.jitter());
        assertTrue(p.retryOn().test(new RuntimeException()));

        RetryPolicy d = RetryPolicy.defaults();
        assertEquals(3, d.maxAttempts());
        assertEquals(1, RetryPolicy.NO_RETRY.maxAttempts());

        RetryPolicy copy = d.toBuilder().maxAttempts(9).build();
        assertEquals(9, copy.maxAttempts());
        assertEquals(0.1, copy.baseDelay());
        assertTrue(copy.retryOn().test(new RuntimeException()));
    }
}
