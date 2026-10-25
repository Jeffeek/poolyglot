package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ClockTest {
  @Test
  void systemClockMonotonicAndSleep() {
    SystemClock clock = new SystemClock();
    double before = clock.monotonic();
    clock.sleep(0.001);
    clock.sleep(0); // no-op branch
    assertTrue(clock.monotonic() >= before);
  }

  @Test
  void systemClockSleepInterrupted() {
    Thread.currentThread().interrupt();
    assertThrows(RuntimeException.class, () -> new SystemClock().sleep(5));
    // clear the interrupt flag set by the catch block so later tests are unaffected
    assertTrue(Thread.interrupted());
  }

  @Test
  void systemAsyncSleepCompletes() {
    AsyncAwait.join(new SystemClock().asyncSleep(0.001, CancelToken.NONE));
    AsyncAwait.join(new SystemClock().asyncSleep(0, CancelToken.NONE)); // delayMs == 0 branch
  }

  @Test
  void systemAsyncSleepAlreadyCancelled() {
    CancelTokenSource cts = new CancelTokenSource();
    cts.cancel();
    assertThrows(
        java.util.concurrent.CancellationException.class,
        () -> AsyncAwait.join(new SystemClock().asyncSleep(1, cts.token())));
  }

  @Test
  void systemAsyncSleepCancelledMidway() {
    CancelTokenSource cts = new CancelTokenSource();
    var f = new SystemClock().asyncSleep(10, cts.token());
    cts.cancel();
    assertThrows(java.util.concurrent.CancellationException.class, f::join);
  }

  @Test
  void manualClockTimeOnlyMovesWhenTold() {
    ManualClock clock = new ManualClock(100.0);
    assertEquals(100.0, clock.monotonic());
    clock.advance(5);
    assertEquals(105.0, clock.monotonic());
    clock.sleep(2); // sleeping just advances time
    assertEquals(107.0, clock.monotonic());
  }

  @Test
  void manualClockDefaultStart() {
    assertEquals(1000.0, new ManualClock().monotonic());
  }

  @Test
  void manualAsyncSleepAdvances() {
    ManualClock clock = new ManualClock(0.0);
    AsyncAwait.join(clock.asyncSleep(3, CancelToken.NONE));
    assertEquals(3.0, clock.monotonic());
  }

  @Test
  void manualAsyncSleepAlreadyCancelled() {
    CancelTokenSource cts = new CancelTokenSource();
    cts.cancel();
    assertThrows(
        java.util.concurrent.CancellationException.class,
        () -> AsyncAwait.join(new ManualClock(0.0).asyncSleep(3, cts.token())));
  }

  @Test
  void schedulerHolderIsInstantiable() {
    new AsyncScheduler(); // cover the holder constructor
  }
}
