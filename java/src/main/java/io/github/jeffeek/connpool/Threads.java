package io.github.jeffeek.connpool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/** Shared thread helpers used by both pools. */
final class Threads {
  Threads() {}

  /** Wait on {@code cond} (caller holds the lock); null seconds = forever. */
  static void awaitCondition(Condition cond, Double seconds, String name) {
    try {
      if (seconds == null) {
        cond.await();
      } else {
        cond.await((long) (seconds * 1_000_000_000.0), TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PoolError("pool '" + name + "': interrupted while waiting for a connection");
    }
  }

  /** Join a background thread (up to 5s); never self-joins, restores the interrupt flag. */
  static void join(Thread t) {
    if (t == null || t == Thread.currentThread()) {
      return;
    }
    try {
      t.join(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
