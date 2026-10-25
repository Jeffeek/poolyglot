package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class LifecycleTest {
  private static PooledConnection<Object> conn(
      double created, Double returned, int uses, Double expires) {
    PooledConnection<Object> c = new PooledConnection<>(new Object(), created, expires, returned);
    for (int i = 0; i < uses; i++) {
      c.markCheckedOut(created);
    }
    return c;
  }

  @Test
  void holderInstantiable() {
    new Lifecycle();
  }

  @Test
  void lifetimeJitterStaysInRange() {
    PoolConfig cfg = PoolConfig.builder().maxLifetime(100.0).lifetimeJitter(0.1).build();
    Random rng = new Random(7);
    for (int i = 0; i < 200; i++) {
      PooledConnection<Object> c = Lifecycle.newConnection(new Object(), cfg, 0.0, rng);
      assertTrue(c.expiresAt() != null);
      assertTrue(c.expiresAt() >= 90.0 && c.expiresAt() <= 100.0);
    }
  }

  @Test
  void noExpiryWhenLifetimeNull() {
    PoolConfig cfg = PoolConfig.builder().maxLifetime(null).build();
    assertNull(Lifecycle.newConnection(new Object(), cfg, 0.0, new Random(1)).expiresAt());
  }

  @Test
  void rotationReasons() {
    PoolConfig cfg = PoolConfig.builder().maxUses(3).build();
    assertEquals(CloseReason.EXPIRED, Lifecycle.rotationReason(conn(0, null, 0, 5.0), cfg, 5));
    assertEquals(CloseReason.MAX_USES, Lifecycle.rotationReason(conn(0, null, 3, null), cfg, 0));
    assertNull(Lifecycle.rotationReason(conn(0, null, 2, 9.0), cfg, 5));
  }

  @Test
  void borrowReasonFlagsIdle() {
    PoolConfig cfg = PoolConfig.builder().maxIdleTime(10.0).build();
    assertEquals(CloseReason.IDLE, Lifecycle.borrowReason(conn(0, 0.0, 0, null), cfg, 11));
    assertNull(Lifecycle.borrowReason(conn(0, 0.0, 0, null), cfg, 5));
  }

  @Test
  void sweepNeverGoesBelowMinSize() {
    PoolConfig cfg = PoolConfig.builder().minSize(2).maxIdleTime(10.0).build();
    List<PooledConnection<Object>> idle = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      idle.add(conn(0, 0.0, 0, null));
    }
    Lifecycle.SweepResult<Object> r = Lifecycle.sweep(idle, 4, cfg, 11);
    assertEquals(2, r.keep().size());
    assertEquals(2, r.doomed().size());
    for (Lifecycle.Doomed<Object> d : r.doomed()) {
      assertEquals(CloseReason.IDLE, d.reason());
    }
  }

  @Test
  void sweepAlwaysEvictsExpired() {
    PoolConfig cfg = PoolConfig.builder().minSize(2).maxIdleTime(null).build();
    List<PooledConnection<Object>> idle = new ArrayList<>();
    idle.add(conn(0, null, 0, 1.0));
    idle.add(conn(0, null, 0, null));
    Lifecycle.SweepResult<Object> r = Lifecycle.sweep(idle, 2, cfg, 5);
    assertEquals(1, r.doomed().size());
    assertEquals(CloseReason.EXPIRED, r.doomed().get(0).reason());
    assertEquals(1, r.keep().size());
  }

  @Test
  void validationPolicy() {
    PooledConnection<Object> c = conn(0, 0.0, 0, null);
    assertFalse(
        Lifecycle.needsValidation(c, PoolConfig.builder().validateAfterIdle(null).build(), 100));
    assertTrue(
        Lifecycle.needsValidation(c, PoolConfig.builder().validateAfterIdle(0.0).build(), 0));
    assertFalse(
        Lifecycle.needsValidation(c, PoolConfig.builder().validateAfterIdle(30.0).build(), 10));
    assertTrue(
        Lifecycle.needsValidation(c, PoolConfig.builder().validateAfterIdle(30.0).build(), 30));
  }

  @Test
  void popStrategies() {
    PooledConnection<Object> a = conn(0, null, 0, null);
    PooledConnection<Object> b = conn(0, null, 0, null);
    Deque<PooledConnection<Object>> d = new ArrayDeque<>(List.of(a, b));
    assertSame(b, Lifecycle.popIdle(d, IdleStrategy.LIFO));
    d = new ArrayDeque<>(List.of(a, b));
    assertSame(a, Lifecycle.popIdle(d, IdleStrategy.FIFO));
    assertNull(Lifecycle.popIdle(new ArrayDeque<>(), IdleStrategy.LIFO));
  }
}
