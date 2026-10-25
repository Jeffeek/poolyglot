package io.github.jeffeek.connpool;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;

/**
 * The pure "functional core": every rule about <em>when a connection must go away</em>.
 *
 * <p>No locks, no I/O, no clocks of its own - just functions of (connection, config, now). Both the
 * sync and async pools call these, so the rules live in exactly one place.
 */
final class Lifecycle {
  Lifecycle() {}

  /** One doomed idle connection and the reason it is being retired. */
  record Doomed<T>(PooledConnection<T> connection, CloseReason reason) {}

  /** The result of a sweep: connections to keep, and connections to retire. */
  record SweepResult<T>(List<PooledConnection<T>> keep, List<Doomed<T>> doomed) {}

  static <T> PooledConnection<T> newConnection(T raw, PoolConfig cfg, double now, Random rng) {
    Double expiresAt = null;
    if (cfg.maxLifetime() != null) {
      expiresAt = now + cfg.maxLifetime() * (1.0 - cfg.lifetimeJitter() * rng.nextDouble());
    }
    return new PooledConnection<>(raw, now, expiresAt, null);
  }

  static <T> PooledConnection<T> popIdle(Deque<PooledConnection<T>> idle, IdleStrategy strategy) {
    if (idle.isEmpty()) {
      return null;
    }
    return strategy == IdleStrategy.LIFO ? idle.removeLast() : idle.removeFirst();
  }

  /** Connections that must be retired no matter what (lifetime / use-count rotation). */
  static CloseReason rotationReason(PooledConnection<?> c, PoolConfig cfg, double now) {
    if (c.expiresAt() != null && now >= c.expiresAt()) {
      return CloseReason.EXPIRED;
    }
    if (cfg.maxUses() != null && c.useCount() >= cfg.maxUses()) {
      return CloseReason.MAX_USES;
    }
    return null;
  }

  static boolean idleTooLong(PooledConnection<?> c, PoolConfig cfg, double now) {
    return cfg.maxIdleTime() != null && c.idleTime(now) >= cfg.maxIdleTime();
  }

  static boolean needsValidation(PooledConnection<?> c, PoolConfig cfg, double now) {
    return cfg.validateAfterIdle() != null && c.idleTime(now) >= cfg.validateAfterIdle();
  }

  /**
   * Static checks applied to an idle connection about to be handed out (validation, which needs
   * I/O, is done by the pool afterwards).
   */
  static CloseReason borrowReason(PooledConnection<?> c, PoolConfig cfg, double now) {
    CloseReason reason = rotationReason(c, cfg, now);
    if (reason == null && idleTooLong(c, cfg, now)) {
      reason = CloseReason.IDLE;
    }
    return reason;
  }

  /**
   * Split idle connections into (keep, doomed). Rotation victims always go; idle-timeout victims go
   * only while the pool stays at or above minSize.
   */
  static <T> SweepResult<T> sweep(
      Iterable<PooledConnection<T>> idle, int totalSize, PoolConfig cfg, double now) {
    List<PooledConnection<T>> keep = new ArrayList<>();
    List<Doomed<T>> doomed = new ArrayList<>();
    int remaining = totalSize;
    for (PooledConnection<T> c : idle) {
      CloseReason reason = rotationReason(c, cfg, now);
      if (reason == null && idleTooLong(c, cfg, now) && remaining > cfg.minSize()) {
        reason = CloseReason.IDLE;
      }
      if (reason == null) {
        keep.add(c);
      } else {
        doomed.add(new Doomed<>(c, reason));
        remaining -= 1;
      }
    }
    return new SweepResult<>(keep, doomed);
  }
}
