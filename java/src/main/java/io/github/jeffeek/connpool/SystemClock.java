package io.github.jeffeek.connpool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Real time. The default. */
public final class SystemClock implements Clock {
  @Override
  public double monotonic() {
    return System.nanoTime() / 1_000_000_000.0;
  }

  @Override
  public void sleep(double seconds) {
    if (seconds > 0) {
      try {
        Thread.sleep((long) (seconds * 1000.0));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("interrupted while sleeping", e);
      }
    }
  }

  @Override
  public CompletableFuture<Void> asyncSleep(double seconds, CancelToken token) {
    CompletableFuture<Void> done = new CompletableFuture<>();
    if (token.isCancellationRequested()) {
      done.completeExceptionally(new java.util.concurrent.CancellationException());
      return done;
    }
    long delayMs = seconds > 0 ? (long) (seconds * 1000.0) : 0L;
    ScheduledFuture<?> timer =
        AsyncScheduler.INSTANCE.schedule(() -> done.complete(null), delayMs, TimeUnit.MILLISECONDS);
    Registration reg =
        token.register(
            () -> {
              done.completeExceptionally(new java.util.concurrent.CancellationException());
              timer.cancel(false);
            });
    done.whenComplete((v, e) -> reg.close());
    return done;
  }
}
