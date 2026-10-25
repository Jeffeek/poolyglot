package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ConnectionTest {
    @Test
    void ageIdleAndHoldTimes() {
        PooledConnection<Object> c = new PooledConnection<>(new Object(), 100.0);
        assertEquals(10.0, c.age(110.0));
        assertEquals(5.0, c.idleTime(105.0));   // lastReturnedAt defaults to createdAt
        assertEquals(0.0, c.holdTime(110.0));    // not checked out yet

        c.markCheckedOut(120.0);
        assertEquals(1, c.useCount());
        assertEquals(3.0, c.holdTime(123.0));
        assertTrue(c.leakReported() == false);

        c.markReturned(130.0);
        assertEquals(1.0, c.idleTime(131.0));
        assertNull(c.checkedOutAt());
    }

    @Test
    void leakReportedFlagRoundTrips() {
        PooledConnection<Object> c = new PooledConnection<>(new Object(), 0.0);
        c.setLeakReported(true);
        assertTrue(c.leakReported());
        c.markCheckedOut(1.0); // clears the flag
        assertTrue(!c.leakReported());
    }

    @Test
    void metadataAccessors() {
        Object raw = new Object();
        PooledConnection<Object> c = new PooledConnection<>(raw, 7.0, 99.0, 8.0);
        assertEquals(raw, c.raw());
        assertTrue(c.id() > 0);
        assertEquals(7.0, c.createdAt());
        assertEquals(99.0, c.expiresAt());
        assertEquals(8.0, c.lastReturnedAt());
    }

    @Test
    void toStringMentionsType() {
        PooledConnection<Object> c = new PooledConnection<>(new Object(), 0.0);
        assertTrue(c.toString().contains("PooledConnection"));
    }

    @Test
    void closeReasonWireValues() {
        assertEquals("expired", CloseReason.EXPIRED.value());
        assertEquals("max_uses", CloseReason.MAX_USES.value());
        assertEquals("idle", CloseReason.IDLE.value());
        assertEquals("unhealthy", CloseReason.UNHEALTHY.value());
        assertEquals("error", CloseReason.ERROR.value());
        assertEquals("reset_failed", CloseReason.RESET_FAILED.value());
        assertEquals("discarded", CloseReason.DISCARDED.value());
        assertEquals("pool_closed", CloseReason.POOL_CLOSED.value());
    }
}
