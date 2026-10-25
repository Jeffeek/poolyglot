package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TimeoutTest {
    @Test
    void unsetResolvesToConfiguredDefault() {
        assertTrue(Timeout.unset().isUnset());
        assertEquals(30.0, Timeout.unset().resolve(30.0));
        assertNull(Timeout.unset().resolve(null));
    }

    @Test
    void infiniteResolvesToNull() {
        assertFalse(Timeout.infinite().isUnset());
        assertNull(Timeout.infinite().resolve(30.0));
    }

    @Test
    void ofResolvesToValue() {
        assertEquals(5.0, Timeout.of(5.0).resolve(30.0));
        assertEquals(0.0, Timeout.of(0.0).resolve(30.0));
    }

    @Test
    void ofNullableMapsNullToInfinite() {
        assertNull(Timeout.ofNullable(null).resolve(30.0));
        assertEquals(7.0, Timeout.ofNullable(7.0).resolve(30.0));
    }

    @Test
    void computeDeadline() {
        assertEquals(105.0, Timeout.computeDeadline(Timeout.of(5.0), 30.0, 100.0));
        assertEquals(130.0, Timeout.computeDeadline(Timeout.unset(), 30.0, 100.0));
        assertNull(Timeout.computeDeadline(Timeout.infinite(), 30.0, 100.0));
    }

    @Test
    void remaining() {
        assertEquals(5.0, Timeout.remaining(105.0, 100.0));
        assertNull(Timeout.remaining(null, 100.0));
    }
}
