package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StatsTest {
  @Test
  void holderInstantiable() {
    new Statistics();
  }

  @Test
  void percentileEmptyIsZero() {
    assertEquals(0.0, Statistics.percentile(new ArrayList<>(), 0.5));
  }

  @Test
  void percentileKnownQuantiles() {
    List<Double> vals = new ArrayList<>();
    for (int i = 1; i <= 100; i++) {
      vals.add((double) i);
    }
    assertEquals(1.0, Statistics.percentile(vals, 0.0), 1e-6);
    assertEquals(100.0, Statistics.percentile(vals, 1.0), 1e-6);
    assertEquals(50.5, Statistics.percentile(vals, 0.5), 1e-6);
    assertEquals(99.01, Statistics.percentile(vals, 0.99), 1e-6);
  }

  @Test
  void acquireWaitPercentilesInSnapshot() {
    StatsCollector c = new StatsCollector();
    for (double w : new double[] {0.1, 0.2, 0.3, 0.4, 100.0}) {
      c.onAcquired(null, w);
    }
    PoolStats s = c.snapshot("x", 1, 0, 0, 0, 0, null);
    assertEquals(20.2, s.avgAcquireWait(), 1e-3);
    assertTrue(s.p50AcquireWait() < 1.0);
    assertTrue(s.p99AcquireWait() > 50.0);
  }

  @Test
  void createLatencyPercentilesInSnapshot() {
    StatsCollector c = new StatsCollector();
    for (double d : new double[] {0.01, 0.02, 0.03, 0.04, 5.0}) {
      c.onConnectionCreated(null, d);
    }
    PoolStats s = c.snapshot("x", 1, 0, 0, 0, 0, null);
    assertEquals(5.1 / 5, s.avgCreateLatency(), 1e-6);
    assertTrue(s.p50CreateLatency() < 1.0);
    assertTrue(s.p99CreateLatency() > 1.0);
  }

  @Test
  void waitWindowIsBounded() {
    StatsCollector c = new StatsCollector();
    for (int i = 0; i < Statistics.ACQUIRE_WAIT_WINDOW + 500; i++) {
      c.onAcquired(null, 1.0);
    }
    assertEquals(Statistics.ACQUIRE_WAIT_WINDOW, c.waitsCount());
  }

  @Test
  void allHooksAndSnapshot() {
    StatsCollector c = new StatsCollector();
    c.onCreateFailed(new RuntimeException("x"));
    c.onReleased(null, 1.0);
    c.onAcquireTimeout(2.0);
    c.onConnectionLeak(null, 3.0);
    c.onConnectionClosed(null, CloseReason.EXPIRED);
    c.onConnectionClosed(null, CloseReason.EXPIRED);
    c.onConnectionClosed(null, CloseReason.IDLE);
    PoolStats s = c.snapshot("p", 10, 3, 2, 1, 0, "closed");
    assertEquals(1, s.createFailures());
    assertEquals(1, s.released());
    assertEquals(1, s.timeouts());
    assertEquals(1, s.leaks());
    assertEquals(3, s.closed());
    assertEquals("closed", s.breakerState());
    assertEquals(2, s.closedByReason().get("expired"));
    assertEquals(1, s.closedByReason().get("idle"));
  }

  @Test
  void snapshotToDictionary() {
    StatsCollector c = new StatsCollector();
    PoolStats s = c.snapshot("x", 2, 1, 1, 0, 0, null);
    Map<String, Object> d = s.toDictionary();
    assertEquals("x", d.get("name"));
    assertEquals(2, d.get("max_size"));
    assertTrue(d.containsKey("p99_acquire_wait"));
    assertTrue(d.containsKey("closed_by_reason"));
    assertTrue(d.containsKey("in_use"));
  }

  @Test
  void poolStatsToStringMentionsName() {
    PoolStats s = new StatsCollector().snapshot("mypool", 5, 2, 1, 1, 0, null);
    assertTrue(s.toString().contains("mypool"));
  }
}
