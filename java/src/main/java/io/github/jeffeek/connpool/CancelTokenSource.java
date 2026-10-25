package io.github.jeffeek.connpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Owns a {@link CancelToken} and fires it. The Java analog of .NET's {@code
 * CancellationTokenSource}.
 */
public final class CancelTokenSource implements AutoCloseable {
  private final CancelToken token = new CancelToken(this);
  private final Object lock = new Object();
  private boolean cancelled;
  private List<Runnable> callbacks = new ArrayList<>();
  private ScheduledFuture<?> timer;
  private Registration parentReg = Registration.NOOP;

  public CancelToken token() {
    return token;
  }

  boolean isCancelled() {
    return cancelled;
  }

  public void cancel() {
    List<Runnable> toRun;
    synchronized (lock) {
      if (cancelled) {
        return;
      }
      cancelled = true;
      toRun = callbacks;
      callbacks = null;
    }
    for (Runnable cb : toRun) {
      cb.run();
    }
  }

  Registration register(Runnable callback) {
    synchronized (lock) {
      if (!cancelled) {
        callbacks.add(callback);
        return () -> {
          synchronized (lock) {
            if (callbacks != null) {
              callbacks.remove(callback);
            }
          }
        };
      }
    }
    callback.run();
    return Registration.NOOP;
  }

  /**
   * A source cancelled when {@code parent} cancels, or after {@code seconds} (null = no timer).
   * {@link #close()} releases the timer and parent registration.
   */
  static CancelTokenSource linkedWithTimeout(CancelToken parent, Double seconds) {
    CancelTokenSource cts = new CancelTokenSource();
    if (parent.canBeCancelled()) {
      cts.parentReg = parent.register(cts::cancel);
    }
    if (seconds != null) {
      long ms = seconds > 0 ? (long) (seconds * 1000.0) : 0L;
      cts.timer = AsyncScheduler.INSTANCE.schedule(cts::cancel, ms, TimeUnit.MILLISECONDS);
    }
    return cts;
  }

  @Override
  public void close() {
    parentReg.close();
    if (timer != null) {
      timer.cancel(false);
    }
  }
}
