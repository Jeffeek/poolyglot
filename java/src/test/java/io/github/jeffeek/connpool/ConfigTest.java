package io.github.jeffeek.connpool;

import static io.github.jeffeek.connpool.TestSupport.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ConfigTest {
  @Test
  void defaultsAreValid() {
    PoolConfig cfg = PoolConfig.defaults();
    assertEquals(0, cfg.minSize());
    assertEquals(10, cfg.maxSize());
    assertEquals(30.0, cfg.acquireTimeout());
    assertEquals(10.0, cfg.createTimeout());
    assertEquals(1800.0, cfg.maxLifetime());
    assertEquals(0.1, cfg.lifetimeJitter());
    assertEquals(600.0, cfg.maxIdleTime());
    assertEquals(30.0, cfg.validateAfterIdle());
    assertEquals(30.0, cfg.maintenanceInterval());
    assertEquals(IdleStrategy.LIFO, cfg.idleStrategy());
  }

  @Test
  void rejectsBadValues() {
    assertThrowsExactly(PoolConfigError.class, () -> PoolConfig.builder().maxSize(0).build());
    assertThrowsExactly(PoolConfigError.class, () -> PoolConfig.builder().minSize(11).build());
    assertThrowsExactly(PoolConfigError.class, () -> PoolConfig.builder().minSize(-1).build());
    assertThrowsExactly(
        PoolConfigError.class, () -> PoolConfig.builder().acquireTimeout(-1.0).build());
    assertThrowsExactly(PoolConfigError.class, () -> PoolConfig.builder().maxLifetime(0.0).build());
    assertThrowsExactly(
        PoolConfigError.class, () -> PoolConfig.builder().lifetimeJitter(1.5).build());
    assertThrowsExactly(
        PoolConfigError.class, () -> PoolConfig.builder().lifetimeJitter(-0.5).build());
    assertThrowsExactly(PoolConfigError.class, () -> PoolConfig.builder().maxUses(0).build());
    assertThrowsExactly(
        PoolConfigError.class, () -> PoolConfig.builder().validateAfterIdle(-1.0).build());
    assertThrowsExactly(
        PoolConfigError.class, () -> PoolConfig.builder().maintenanceInterval(0.0).build());
    assertThrowsExactly(
        PoolConfigError.class, () -> PoolConfig.builder().leakDetectionTimeout(0.0).build());
    assertThrowsExactly(
        PoolConfigError.class, () -> PoolConfig.builder().leakDetectionTimeout(-5.0).build());
    assertThrowsExactly(
        PoolConfigError.class, () -> PoolConfig.builder().createTimeout(0.0).build());
    assertThrowsExactly(PoolConfigError.class, () -> PoolConfig.builder().maxWaiters(-1).build());
    assertThrowsExactly(PoolConfigError.class, () -> PoolConfig.builder().maxIdleTime(0.0).build());
    assertThrowsExactly(
        PoolConfigError.class, () -> PoolConfig.builder().minSize(5).maxSize(3).build());
  }

  @Test
  void acceptsBoundaryValues() {
    PoolConfig.builder().maxSize(1).build();
    PoolConfig.builder().minSize(0).build();
    PoolConfig.builder().maxSize(3).minSize(3).build();
    PoolConfig.builder().acquireTimeout(0.0).build();
    PoolConfig.builder().acquireTimeout(null).build();
    PoolConfig.builder().lifetimeJitter(0).build();
    PoolConfig.builder().lifetimeJitter(1).build();
    PoolConfig.builder().maxUses(1).build();
    PoolConfig.builder().maxUses(null).build();
    PoolConfig.builder().maxWaiters(0).build();
    PoolConfig.builder().maxWaiters(null).build();
    PoolConfig.builder().leakDetectionTimeout(null).build();
    PoolConfig.builder().validateAfterIdle(0.0).build();
    PoolConfig.builder().createTimeout(0.001).build();
    PoolConfig.builder().maxLifetime(0.001).build();
    PoolConfig.builder().maxIdleTime(0.001).build();
    PoolConfig.builder().maintenanceInterval(0.001).build();
    PoolConfig.builder().leakDetectionTimeout(0.001).build();
    PoolConfig.builder().createTimeout(null).build();
  }

  @Test
  void idleStrategyWireValues() {
    assertEquals("lifo", IdleStrategy.LIFO.value());
    assertEquals("fifo", IdleStrategy.FIFO.value());
  }

  @Test
  void toBuilderReturnsValidatedCopy() {
    PoolConfig cfg = PoolConfig.builder().maxSize(5).build();
    assertEquals(8, cfg.toBuilder().maxSize(8).build().maxSize());
    assertEquals(5, cfg.maxSize()); // original unchanged
    assertThrowsExactly(PoolConfigError.class, () -> cfg.toBuilder().minSize(99).build());
  }

  @Test
  void toBuilderPreservesEveryField() {
    PoolConfig cfg =
        PoolConfig.builder()
            .minSize(1)
            .maxSize(7)
            .acquireTimeout(9.0)
            .createTimeout(3.0)
            .maxWaiters(4)
            .maxLifetime(99.0)
            .lifetimeJitter(0.2)
            .maxIdleTime(55.0)
            .maxUses(6)
            .idleStrategy(IdleStrategy.FIFO)
            .validateAfterIdle(11.0)
            .maintenanceInterval(12.0)
            .leakDetectionTimeout(13.0)
            .validateIdle(true)
            .retry(RetryPolicy.NO_RETRY)
            .breaker(null)
            .build();
    PoolConfig copy = cfg.toBuilder().build();
    assertEquals(cfg, copy);
  }
}
