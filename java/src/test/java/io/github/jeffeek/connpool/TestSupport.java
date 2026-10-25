package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.function.Executable;

/** Shared test doubles and helpers (the mirror of python/tests/support/helpers.py). */
final class TestSupport {
  private TestSupport() {}

  static <T extends Throwable> void assertThrowsExactly(
      Class<T> expectedType, Executable executable) {
    assertEquals(
        expectedType,
        org.junit.jupiter.api.Assertions.assertThrowsExactly(expectedType, executable).getClass());
  }

  // Exception stand-ins mirroring the Python builtins the suite relies on.
  static class ConnErr extends RuntimeException {
    ConnErr(String message) {
      super(message);
    }
  }

  static final class ConnRefused extends ConnErr {
    ConnRefused(String message) {
      super(message);
    }
  }

  static final class ConnReset extends ConnErr {
    ConnReset(String message) {
      super(message);
    }
  }

  static final class AuthError extends RuntimeException {
    AuthError(String message) {
      super(message);
    }
  }

  static final class FakeConn {
    private static final AtomicInteger IDS = new AtomicInteger(0);
    final int id = IDS.incrementAndGet();
    volatile boolean closed;
    volatile boolean healthy = true;
    volatile int resets;

    @Override
    public String toString() {
      return "<FakeConn " + id + ">";
    }
  }

  static class FakeFactory extends ConnectionFactory<FakeConn> {
    final List<FakeConn> created = new ArrayList<>();
    int attempts;
    int failTimes;
    boolean failAlways;
    List<Class<? extends Throwable>> fatalTypes = new ArrayList<>();
    boolean resetRaises;
    private final Object lock = new Object();

    @Override
    public FakeConn create() {
      synchronized (lock) {
        attempts += 1;
        if (failAlways || failTimes > 0) {
          failTimes = Math.max(0, failTimes - 1);
          throw new ConnRefused("backend down");
        }
        FakeConn conn = new FakeConn();
        created.add(conn);
        return conn;
      }
    }

    @Override
    public boolean validate(FakeConn conn) {
      return conn.healthy && !conn.closed;
    }

    @Override
    public void reset(FakeConn conn) {
      if (resetRaises) {
        throw new IllegalStateException("reset failed");
      }
      conn.resets += 1;
    }

    @Override
    public void close(FakeConn conn) {
      conn.closed = true;
    }

    @Override
    public boolean isFatalError(Throwable exc) {
      for (Class<? extends Throwable> t : fatalTypes) {
        if (t.isInstance(exc)) {
          return true;
        }
      }
      return false;
    }
  }

  /** Async view over a FakeFactory (shares its counters and failure switches). */
  static final class AsyncFakeFactory extends AsyncConnectionFactory<FakeConn> {
    final FakeFactory inner;
    final double createDelay;

    AsyncFakeFactory() {
      this(new FakeFactory(), 0.0);
    }

    AsyncFakeFactory(FakeFactory inner, double createDelay) {
      this.inner = inner;
      this.createDelay = createDelay;
    }

    @Override
    public CompletableFuture<FakeConn> createAsync(CancelToken token) {
      if (createDelay <= 0) {
        try {
          return CompletableFuture.completedFuture(inner.create());
        } catch (RuntimeException e) {
          return CompletableFuture.failedFuture(e);
        }
      }
      CompletableFuture<FakeConn> f = new CompletableFuture<>();
      ScheduledFuture<?> timer =
          AsyncScheduler.INSTANCE.schedule(
              () -> {
                try {
                  f.complete(inner.create());
                } catch (RuntimeException e) {
                  f.completeExceptionally(e);
                }
              },
              (long) (createDelay * 1000.0),
              TimeUnit.MILLISECONDS);
      Registration reg =
          token.register(
              () -> {
                f.completeExceptionally(new CancellationException());
                timer.cancel(false);
              });
      f.whenComplete((v, e) -> reg.close());
      return f;
    }

    @Override
    public CompletableFuture<Boolean> validateAsync(FakeConn conn, CancelToken token) {
      return CompletableFuture.completedFuture(inner.validate(conn));
    }

    @Override
    public CompletableFuture<Void> resetAsync(FakeConn conn, CancelToken token) {
      try {
        inner.reset(conn);
      } catch (RuntimeException e) {
        return CompletableFuture.failedFuture(e);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> closeAsync(FakeConn conn, CancelToken token) {
      inner.close(conn);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isFatalError(Throwable exc) {
      return inner.isFatalError(exc);
    }
  }

  static final class Recorder extends PoolListener {
    final List<CloseReason> closed = new ArrayList<>();
    final List<String> events = new ArrayList<>();
    private final Object lock = new Object();

    @Override
    public void onConnectionCreated(PooledConnection<?> conn, double duration) {
      synchronized (lock) {
        events.add("created");
      }
    }

    @Override
    public void onConnectionClosed(PooledConnection<?> conn, CloseReason reason) {
      synchronized (lock) {
        closed.add(reason);
        events.add("closed:" + reason.value());
      }
    }

    @Override
    public void onAcquired(PooledConnection<?> conn, double waitTime) {
      synchronized (lock) {
        events.add("acquired");
      }
    }

    @Override
    public void onReleased(PooledConnection<?> conn, double holdTime) {
      synchronized (lock) {
        events.add("released");
      }
    }

    @Override
    public void onBreakerStateChange(BreakerState old, BreakerState current) {
      synchronized (lock) {
        events.add("breaker:" + old.value() + "->" + current.value());
      }
    }

    int countEvent(String e) {
      synchronized (lock) {
        int n = 0;
        for (String x : events) {
          if (x.equals(e)) {
            n += 1;
          }
        }
        return n;
      }
    }

    List<CloseReason> closedSnapshot() {
      synchronized (lock) {
        return new ArrayList<>(closed);
      }
    }
  }

  /** Deterministic baseline: no jitter, no background work, no retries, no breaker. */
  static PoolConfig.Builder quiet() {
    return PoolConfig.builder()
        .maxLifetime(null)
        .maxIdleTime(null)
        .validateAfterIdle(null)
        .maintenanceInterval(null)
        .acquireTimeout(0.3)
        .lifetimeJitter(0.0)
        .retry(RetryPolicy.NO_RETRY)
        .breaker(null);
  }

  static PoolConfig quietConfig() {
    return quiet().build();
  }

  /** Spin until {@code condition} holds or {@code timeoutSeconds} elapses. */
  static void spinUntil(java.util.function.BooleanSupplier condition, double timeoutSeconds) {
    long deadline = System.nanoTime() + (long) (timeoutSeconds * 1_000_000_000.0);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      if (Thread.currentThread().isInterrupted()) {
        return;
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
    }
  }

  static void spinUntil(java.util.function.BooleanSupplier condition) {
    spinUntil(condition, 3.0);
  }
}
