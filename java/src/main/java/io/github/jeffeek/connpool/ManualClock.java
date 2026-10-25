package io.github.jeffeek.connpool;

import java.util.concurrent.CompletableFuture;

/** Fake clock for tests: time moves only when you say so; sleeping just advances it. */
public final class ManualClock implements Clock {
  private double now;

  public ManualClock() {
    this(1000.0);
  }

  public ManualClock(double start) {
    this.now = start;
  }

  @Override
  public synchronized double monotonic() {
    return now;
  }

  public synchronized void advance(double seconds) {
    now += seconds;
  }

  @Override
  public synchronized void sleep(double seconds) {
    now += seconds;
  }

  @Override
  public CompletableFuture<Void> asyncSleep(double seconds, CancelToken token) {
    if (token.isCancellationRequested()) {
      CompletableFuture<Void> f = new CompletableFuture<>();
      f.completeExceptionally(new java.util.concurrent.CancellationException());
      return f;
    }
    synchronized (this) {
      now += seconds;
    }
    return CompletableFuture.completedFuture(null);
  }
}
