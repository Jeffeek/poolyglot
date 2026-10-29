package examples;

import io.github.jeffeek.connpool.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demonstrates bridging poolyglot's {@link PoolListener} observability hooks to Prometheus/StatsD
 * metrics export.
 *
 * <p>This example shows:
 * <ul>
 *   <li>Counter-based metrics on pool lifecycle events (creates, closes, timeouts, leaks)
 *   <li>Gauge export from {@link PoolStats} snapshots (pool sizes, latencies, breaker state)
 *   <li>Thread-safe metric accumulation using {@link AtomicLong} and {@link ConcurrentHashMap}
 *   <li>Non-blocking event capture with deferred export to backend
 * </ul>
 *
 * <p><b>Usage pattern:</b>
 * <ol>
 *   <li>Pool fires events -> listener hooks increment counters in memory
 *   <li>Periodically (e.g., 15s), call {@link #exportGauges(Map)} to snapshot gauges
 *   <li>Push formatted metric lines to Prometheus/StatsD backend
 *   <li>In production, push from a background worker to avoid blocking the pool
 * </ol>
 *
 * <p><b>Run:</b> {@code mvn compile && mvn exec:java -Dexec.mainClass="examples.PrometheusMetrics"}
 */
public class PrometheusMetrics {

    /**
     * A {@link PoolListener} implementation that captures pool events as Prometheus-style counters
     * and exports gauges from pool stats snapshots.
     *
     * <p><b>Thread safety:</b> Counters use {@link AtomicLong} for lock-free atomic updates;
     * per-reason closure dictionaries use {@link ConcurrentHashMap} for thread-safe writes.
     */
    static class PrometheusMetricsListener extends PoolListener {
        private final String poolName;
        private final AtomicLong connectionsCreatedTotal = new AtomicLong(0);
        private final AtomicLong connectionsClosedTotal = new AtomicLong(0);
        private final AtomicLong createFailuresTotal = new AtomicLong(0);
        private final AtomicLong acquireTimeoutsTotal = new AtomicLong(0);
        private final AtomicLong leaksDetectedTotal = new AtomicLong(0);
        private final AtomicLong breakerStateChangesTotal = new AtomicLong(0);

        // Per-reason closure counters: { "expired" -> count, "max_uses" -> count, ... }
        private final ConcurrentHashMap<String, AtomicLong> closuresByReason = new ConcurrentHashMap<>();

        /**
         * Constructs a metrics listener for the given pool name.
         *
         * @param poolName the pool name used as a label on all metrics
         */
        public PrometheusMetricsListener(String poolName) {
            this.poolName = poolName;
        }

        @Override
        public void onConnectionCreated(PooledConnection<?> conn, double duration) {
            connectionsCreatedTotal.incrementAndGet();
        }

        @Override
        public void onConnectionClosed(PooledConnection<?> conn, CloseReason reason) {
            connectionsClosedTotal.incrementAndGet();
            closuresByReason.computeIfAbsent(reason.value(), k -> new AtomicLong(0)).incrementAndGet();
        }

        @Override
        public void onCreateFailed(Throwable error) {
            createFailuresTotal.incrementAndGet();
        }

        @Override
        public void onAcquireTimeout(double waited) {
            acquireTimeoutsTotal.incrementAndGet();
        }

        @Override
        public void onConnectionLeak(PooledConnection<?> conn, double heldFor) {
            leaksDetectedTotal.incrementAndGet();
            System.out.printf(
                    "[LEAK] pool=%s connection=%s held_for_sec=%.3f%n",
                    poolName, conn, heldFor);
        }

        @Override
        public void onBreakerStateChange(BreakerState oldState, BreakerState newState) {
            breakerStateChangesTotal.incrementAndGet();
            System.out.printf(
                    "[BREAKER] pool=%s transition=%s->%s%n",
                    poolName, oldState.value(), newState.value());
        }

        /**
         * Exports counter metrics in Prometheus text format.
         *
         * <p>Format: {@code metric_name{pool="name",label="value"} count}
         *
         * @return a list of formatted counter metric lines
         */
        public List<String> exportCounters() {
            List<String> lines = new ArrayList<>();
            lines.add(String.format("connections_created_total{pool=\"%s\"} %d", poolName, connectionsCreatedTotal.get()));
            lines.add(String.format("connections_closed_total{pool=\"%s\"} %d", poolName, connectionsClosedTotal.get()));
            lines.add(String.format("create_failures_total{pool=\"%s\"} %d", poolName, createFailuresTotal.get()));
            lines.add(String.format("acquire_timeouts_total{pool=\"%s\"} %d", poolName, acquireTimeoutsTotal.get()));
            lines.add(String.format("leaks_detected_total{pool=\"%s\"} %d", poolName, leaksDetectedTotal.get()));
            lines.add(String.format("breaker_state_changes_total{pool=\"%s\"} %d", poolName, breakerStateChangesTotal.get()));

            // Per-reason closure counters
            for (Map.Entry<String, AtomicLong> entry : closuresByReason.entrySet()) {
                lines.add(String.format(
                        "connections_closed_by_reason{pool=\"%s\",reason=\"%s\"} %d",
                        poolName, entry.getKey(), entry.getValue().get()));
            }

            return lines;
        }

        /**
         * Exports gauge metrics from a {@link PoolStats} snapshot in Prometheus text format.
         *
         * <p>Includes pool sizes, latency percentiles, and breaker state (0=closed, 1=open, 2=half_open).
         *
         * @param stats the pool stats dictionary returned by {@link PoolStats#toDictionary()}
         * @return a list of formatted gauge metric lines
         */
        public List<String> exportGauges(Map<String, Object> stats) {
            List<String> lines = new ArrayList<>();

            // Pool sizes
            lines.add(String.format("pool_size{pool=\"%s\"} %s", poolName, stats.get("size")));
            lines.add(String.format("pool_idle{pool=\"%s\"} %s", poolName, stats.get("idle")));
            lines.add(String.format("pool_in_use{pool=\"%s\"} %s", poolName, stats.get("in_use")));
            lines.add(String.format("pool_waiting{pool=\"%s\"} %s", poolName, stats.get("waiting")));
            lines.add(String.format("pool_max_size{pool=\"%s\"} %s", poolName, stats.get("max_size")));

            // Acquire wait latencies (seconds)
            Double acquireWaitP50 = (Double) stats.get("p50_acquire_wait");
            Double acquireWaitP95 = (Double) stats.get("p95_acquire_wait");
            Double acquireWaitP99 = (Double) stats.get("p99_acquire_wait");
            if (acquireWaitP50 != null) {
                lines.add(String.format("acquire_wait_p50_sec{pool=\"%s\"} %.6f", poolName, acquireWaitP50));
            }
            if (acquireWaitP95 != null) {
                lines.add(String.format("acquire_wait_p95_sec{pool=\"%s\"} %.6f", poolName, acquireWaitP95));
            }
            if (acquireWaitP99 != null) {
                lines.add(String.format("acquire_wait_p99_sec{pool=\"%s\"} %.6f", poolName, acquireWaitP99));
            }

            // Create latencies (seconds)
            Double createLatencyP50 = (Double) stats.get("p50_create_latency");
            Double createLatencyP95 = (Double) stats.get("p95_create_latency");
            Double createLatencyP99 = (Double) stats.get("p99_create_latency");
            if (createLatencyP50 != null) {
                lines.add(String.format("create_latency_p50_sec{pool=\"%s\"} %.6f", poolName, createLatencyP50));
            }
            if (createLatencyP95 != null) {
                lines.add(String.format("create_latency_p95_sec{pool=\"%s\"} %.6f", poolName, createLatencyP95));
            }
            if (createLatencyP99 != null) {
                lines.add(String.format("create_latency_p99_sec{pool=\"%s\"} %.6f", poolName, createLatencyP99));
            }

            // Circuit breaker state (enum to int: 0=closed, 1=open, 2=half_open)
            String breakerState = (String) stats.getOrDefault("breaker_state", "closed");
            int breakerStateValue = switch (breakerState) {
                case "closed" -> 0;
                case "open" -> 1;
                case "half_open" -> 2;
                default -> -1;
            };
            lines.add(String.format("breaker_state{pool=\"%s\"} %d", poolName, breakerStateValue));

            return lines;
        }
    }

    /** Trivial stand-in for a real backend connection (no JDBC driver needed for the demo). */
    static final class SampleConnection implements AutoCloseable {
        private static final AtomicInteger IDS = new AtomicInteger();
        final int id = IDS.incrementAndGet();

        @Override
        public void close() {
            // no-op
        }

        @Override
        public String toString() {
            return "conn#" + id;
        }
    }

    /**
     * A minimal connection factory for demonstration purposes, built from lambdas via
     * {@link CallableFactory}.
     */
    static final class SampleFactory extends ConnectionFactory<SampleConnection> {
        private final CallableFactory<SampleConnection> delegate =
            CallableFactory.<SampleConnection>builder(SampleConnection::new)
                .validate(c -> true)
                .build();

        @Override
        public SampleConnection create() {
            return delegate.create();
        }

        @Override
        public boolean validate(SampleConnection conn) {
            return delegate.validate(conn);
        }

        @Override
        public void close(SampleConnection conn) {
            conn.close();
        }
    }

    /**
     * Demonstrates metrics collection and export.
     *
     * <p>Creates a pool with a metrics listener, runs acquire/release cycles, then exports
     * counters and gauges in Prometheus format.
     */
    public static void main(String[] args) throws Exception {
        String poolName = "demo_pool";
        PrometheusMetricsListener metricsListener = new PrometheusMetricsListener(poolName);

        PoolConfig config = PoolConfig.builder()
                .maxSize(5)
                .maxIdleTime(30.0)
                .maxUses(100)
                .leakDetectionTimeout(10.0)
                .build();

        SampleFactory factory = new SampleFactory();

        try (ConnectionPool<SampleConnection> pool = new ConnectionPool<>(
                factory,
                config,
                java.util.List.of(metricsListener), null, null, poolName)) {
            pool.open();
            System.out.printf("=== Pool started: %s ===%n", poolName);

            // Simulate workload: 10 acquire/release cycles
            for (int i = 0; i < 10; i++) {
                try (ConnectionPool<SampleConnection>.Lease borrowed = pool.connection(Timeout.of(5.0))) {
                    SampleConnection conn = borrowed.get();
                    // Simulate work
                    Thread.sleep(50);
                } catch (Exception e) {
                    System.err.printf("Acquire failed: %s%n", e.getMessage());
                }
            }

            System.out.println("\n=== Counter Metrics ===");
            for (String line : metricsListener.exportCounters()) {
                System.out.println(line);
            }

            System.out.println("\n=== Gauge Metrics ===");
            PoolStats poolStats = pool.stats();
            for (String line : metricsListener.exportGauges(poolStats.toDictionary())) {
                System.out.println(line);
            }

            System.out.println("\n=== Pool Stats ===");
            System.out.println(poolStats);
        }

        System.out.println("\nPool closed.");
    }
}
