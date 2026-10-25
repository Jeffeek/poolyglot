package io.github.jeffeek.connpool;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable metrics snapshot. The {@link #toDictionary()} keys are identical across every language.
 */
public record PoolStats(
    String name,
    int maxSize,
    int size, // idle + in use + being created
    int idle, // available for checkout right now
    int inUse, // checked out, or briefly reserved by a background idle check
    int waiting, // callers currently blocked in acquire()
    int created,
    int createFailures,
    int acquired,
    int released,
    int timeouts,
    int closed,
    int leaks, // borrowed connections flagged as possibly leaked
    Map<String, Integer> closedByReason,
    // acquire-wait aggregates, all over the last ACQUIRE_WAIT_WINDOW samples
    double avgAcquireWait,
    double p50AcquireWait,
    double p95AcquireWait,
    double p99AcquireWait,
    // create-latency aggregates (factory.create() duration), same window
    double avgCreateLatency,
    double p50CreateLatency,
    double p95CreateLatency,
    double p99CreateLatency,
    String breakerState) {
  /** Snake-cased view, with keys identical across every poolyglot language. */
  public Map<String, Object> toDictionary() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("name", name);
    m.put("max_size", maxSize);
    m.put("size", size);
    m.put("idle", idle);
    m.put("in_use", inUse);
    m.put("waiting", waiting);
    m.put("created", created);
    m.put("create_failures", createFailures);
    m.put("acquired", acquired);
    m.put("released", released);
    m.put("timeouts", timeouts);
    m.put("closed", closed);
    m.put("leaks", leaks);
    m.put("closed_by_reason", closedByReason);
    m.put("avg_acquire_wait", avgAcquireWait);
    m.put("p50_acquire_wait", p50AcquireWait);
    m.put("p95_acquire_wait", p95AcquireWait);
    m.put("p99_acquire_wait", p99AcquireWait);
    m.put("avg_create_latency", avgCreateLatency);
    m.put("p50_create_latency", p50CreateLatency);
    m.put("p95_create_latency", p95CreateLatency);
    m.put("p99_create_latency", p99CreateLatency);
    m.put("breaker_state", breakerState);
    return m;
  }

  @Override
  public String toString() {
    return "PoolStats(name="
        + name
        + ", size="
        + size
        + "/"
        + maxSize
        + ", idle="
        + idle
        + ", inUse="
        + inUse
        + ", waiting="
        + waiting
        + ", created="
        + created
        + ", closed="
        + closed
        + ")";
  }
}
