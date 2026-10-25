package io.github.jeffeek.connpool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/** Shared daemon pools backing {@link SystemClock#asyncSleep} and the async connection pool. */
final class AsyncScheduler {
  static final ScheduledExecutorService INSTANCE =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "connpool-scheduler");
            t.setDaemon(true);
            return t;
          });

  /** Workers that run the async pool's (cooperatively blocking) method bodies. */
  static final ExecutorService EXECUTOR =
      Executors.newCachedThreadPool(
          new java.util.concurrent.ThreadFactory() {
            private final AtomicLong n = new AtomicLong();

            @Override
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r, "connpool-async-" + n.incrementAndGet());
              t.setDaemon(true);
              return t;
            }
          });

  AsyncScheduler() {}
}
