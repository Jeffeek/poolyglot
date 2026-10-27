package io.github.jeffeek.connpool.fuzz;

import static org.junit.jupiter.api.Assertions.fail;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import io.github.jeffeek.connpool.AsyncCallableFactory;
import io.github.jeffeek.connpool.AsyncConnectionPool;
import io.github.jeffeek.connpool.CancelTokenSource;
import io.github.jeffeek.connpool.PoolConfig;
import io.github.jeffeek.connpool.PoolStats;
import io.github.jeffeek.connpool.PoolTimeoutError;
import io.github.jeffeek.connpool.RetryPolicy;
import io.github.jeffeek.connpool.Timeout;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Jazzer harness for the divergent async primitives: {@code AsyncAwait.await}'s shield/
 * cancellation race (the Java twin of Python's {@code asyncio.shield} / C#'s {@code
 * AwaitShielded} + {@code AsyncCondition}) and {@link AsyncConnectionPool}'s lock/Condition
 * waiter bookkeeping, driven through the public {@code acquireAsync}/{@code releaseAsync}
 * surface with a {@code CancelToken} cancelled mid-flight.
 *
 * <p>One fuzz input decodes into a bounded sequence of pool operations against a tiny pool
 * (maxSize=2) so waiters actually queue and contend. Every op blocks synchronously via {@code
 * .get()}, mirroring the C# SharpFuzz harness's synchronous {@code GetAwaiter().GetResult()}.
 *
 * <p>Standalone module (see ../pom.xml) - never built, tested, or linted by java/'s own
 * run_tests.sh / lint.sh, so Jazzer never touches the library's 100% coverage + SpotBugs gate.
 *
 * <p>Run (after {@code cd .. && mvn -q -DskipTests install} to publish connpool locally):
 *
 * <pre>
 *   mvn test                                               # replay the seed corpus as a JUnit test
 *   JAZZER_FUZZ=1 mvn test -DargLine="-Djazzer.time=30"     # short coverage-guided fuzz run
 * </pre>
 */
class AsyncPrimitivesFuzzTest {

  private static final ScheduledExecutorService TIMERS =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "fuzz-cancel-timer");
            t.setDaemon(true);
            return t;
          });

  @FuzzTest
  void fuzzAcquireReleaseCancellation(FuzzedDataProvider data) throws Exception {
    PoolConfig cfg =
        PoolConfig.builder()
            .maxSize(2)
            .minSize(0)
            .maintenanceInterval(null)
            .acquireTimeout(0.2)
            .retry(RetryPolicy.NO_RETRY)
            .breaker(null)
            .build();
    AsyncConnectionPool<Object> pool =
        new AsyncConnectionPool<>(AsyncCallableFactory.<Object>fromSync(Object::new).build(), cfg);
    pool.openAsync().get(5, TimeUnit.SECONDS);

    List<Object> held = new ArrayList<>();
    try {
      // Bound the op count so one input can't run forever under the fuzzer's watchdog.
      int ops = Math.min(32, Math.max(0, data.remainingBytes()));
      for (int i = 0; i < ops; i++) {
        runOp(pool, held, data, Math.floorMod(data.consumeByte(), 4));
      }

      // Drain whatever is still held, then assert the pool's invariant: every connection the
      // pool thinks it owns is accounted for (idle + in-use == size), i.e. no reserved slot
      // leaked from a cancelled acquireAsync.
      for (Object conn : held) {
        try {
          pool.releaseAsync(conn).get(5, TimeUnit.SECONDS);
        } catch (ExecutionException ignoredDoubleRelease) {
          // already released by a cancelled-but-completed path
        }
      }

      PoolStats stats = pool.stats();
      if (stats.idle() + stats.inUse() != stats.size()) {
        fail(
            "invariant violated: idle("
                + stats.idle()
                + ") + in_use("
                + stats.inUse()
                + ") != size("
                + stats.size()
                + ")");
      }
    } catch (java.util.concurrent.TimeoutException e) {
      fail("deadlock: op sequence did not finish in time");
    } finally {
      try {
        pool.closeAsync().get(5, TimeUnit.SECONDS);
      } catch (Exception ignoredClose) {
        // best-effort cleanup
      }
    }
  }

  private static void runOp(
      AsyncConnectionPool<Object> pool, List<Object> held, FuzzedDataProvider data, int op)
      throws Exception {
    switch (op) {
      case 0 -> { // plain acquire
        try {
          held.add(pool.acquireAsync().get(5, TimeUnit.SECONDS));
        } catch (ExecutionException e) {
          expectCause(e, PoolTimeoutError.class); // exhausted - expected under contention
        }
      }
      case 1 -> { // release a held connection, if any
        if (!held.isEmpty()) {
          pool.releaseAsync(held.remove(held.size() - 1)).get(5, TimeUnit.SECONDS);
        }
      }
      case 2 -> { // acquire, then cancel the token mid-flight (byte picks the delay)
        try (CancelTokenSource cts = new CancelTokenSource()) {
          int delayMs = Math.floorMod(data.consumeByte(), 5);
          TIMERS.schedule(cts::cancel, delayMs, TimeUnit.MILLISECONDS);
          try {
            held.add(pool.acquireAsync(Timeout.unset(), cts.token()).get(5, TimeUnit.SECONDS));
          } catch (ExecutionException e) {
            expectCause(e, CancellationException.class, PoolTimeoutError.class);
          }
        }
      }
      default -> { // release under an already-cancelled token (shielded disposal path)
        if (!held.isEmpty()) {
          Object conn = held.remove(held.size() - 1);
          try (CancelTokenSource cts = new CancelTokenSource()) {
            cts.cancel();
            try {
              pool.releaseAsync(conn, false, null, cts.token()).get(5, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
              expectCause(e, CancellationException.class); // shielded disposal still finished
            }
          }
        }
      }
    }
  }

  @SafeVarargs
  private static void expectCause(ExecutionException e, Class<? extends Throwable>... expected) {
    Throwable cause = e.getCause();
    for (Class<? extends Throwable> c : expected) {
      if (c.isInstance(cause)) {
        return;
      }
    }
    throw new CompletionException(cause);
  }
}
