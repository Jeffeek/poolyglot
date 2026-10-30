package examples;

import io.github.jeffeek.connpool.BreakerState;
import io.github.jeffeek.connpool.CallableFactory;
import io.github.jeffeek.connpool.CloseReason;
import io.github.jeffeek.connpool.ConnectionPool;
import io.github.jeffeek.connpool.PoolConfig;
import io.github.jeffeek.connpool.PoolListener;
import io.github.jeffeek.connpool.PooledConnection;
import io.github.jeffeek.connpool.Timeout;
import java.time.Instant;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Example: Comprehensive connection lifecycle logging via PoolListener.
 *
 * <p>This demonstrates using PoolListener to log every pool event for debugging and diagnostics.
 * Useful for understanding connection behavior, troubleshooting leaks, and analyzing performance.
 *
 * <p>Key patterns:
 * <ul>
 *   <li>PoolListener that logs all events with timestamps
 *   <li>Correlate connection lifecycle: created -> acquired -> released -> closed
 *   <li>Track connection age, hold time, wait time
 *   <li>Log breaker state changes and failures
 * </ul>
 *
 * <p>Use cases:
 * <ul>
 *   <li>Development/staging: verbose logging for debugging
 *   <li>Production: conditional logging (only log slow operations, errors, leaks)
 *   <li>Performance analysis: identify hot connections, wait patterns
 * </ul>
 *
 * <p>Run: {@code mvn compile && mvn exec:java -Dexec.mainClass="examples.LifecycleLogger"}
 */
public final class LifecycleLogger {
    private LifecycleLogger() {
    }

    /**
     * PoolListener that logs every event with detailed context. Tracks connection lifetimes and
     * correlates events by connection ID.
     */
    static final class LoggingListener extends PoolListener {
        private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
        private final String poolName;
        private final ConcurrentHashMap<Long, Instant> connectionBirthTimes = new ConcurrentHashMap<>();

        LoggingListener(String poolName) {
            this.poolName = poolName;
        }

        private static void log(String level, String message) {
            String timestamp = LocalTime.now().format(TIME_FMT);
            System.out.println(timestamp + " [" + level + "] " + message);
        }

        @Override
        public void onConnectionCreated(PooledConnection<?> conn, double duration) {
            long connId = conn.id();
            connectionBirthTimes.put(connId, Instant.now());
            log("INFO",
                String.format(
                    "[%s] Connection #%d CREATED in %.3fs (uses=%d)",
                    poolName, connId, duration, conn.useCount()));
        }

        @Override
        public void onConnectionClosed(PooledConnection<?> conn, CloseReason reason) {
            long connId = conn.id();
            Instant birthTime = connectionBirthTimes.remove(connId);
            double age = birthTime != null
                ? java.time.Duration.between(birthTime, Instant.now()).toMillis() / 1000.0
                : 0.0;

            log("INFO",
                String.format(
                    "[%s] Connection #%d CLOSED after %.1fs (reason=%s, uses=%d)",
                    poolName, connId, age, reason.value(), conn.useCount()));
        }

        @Override
        public void onCreateFailed(Throwable error) {
            log("ERROR",
                String.format(
                    "[%s] Connection CREATE FAILED: %s: %s",
                    poolName, error.getClass().getSimpleName(), error.getMessage()));
        }

        @Override
        public void onAcquired(PooledConnection<?> conn, double waitTime) {
            long connId = conn.id();
            if (waitTime > 0.1) {  // Log slow acquires (>100ms wait)
                log("WARN",
                    String.format(
                        "[%s] Connection #%d ACQUIRED after %.3fs wait (slow: pool may be undersized)",
                        poolName, connId, waitTime));
            } else {
                log("DEBUG",
                    String.format(
                        "[%s] Connection #%d ACQUIRED (wait=%.3fs, uses=%d)",
                        poolName, connId, waitTime, conn.useCount()));
            }
        }

        @Override
        public void onReleased(PooledConnection<?> conn, double holdTime) {
            long connId = conn.id();
            if (holdTime > 10.0) {  // Log long-held connections (>10s)
                log("WARN",
                    String.format(
                        "[%s] Connection #%d RELEASED after %.3fs (held long: possible leak or slow query)",
                        poolName, connId, holdTime));
            } else {
                log("DEBUG",
                    String.format(
                        "[%s] Connection #%d RELEASED (held=%.3fs, uses=%d)",
                        poolName, connId, holdTime, conn.useCount()));
            }
        }

        @Override
        public void onAcquireTimeout(double waited) {
            log("ERROR",
                String.format(
                    "[%s] ACQUIRE TIMEOUT after %.3fs (pool exhausted or connections taking too long)",
                    poolName, waited));
        }

        @Override
        public void onConnectionLeak(PooledConnection<?> conn, double heldFor) {
            long connId = conn.id();
            log("ERROR",
                String.format(
                    "[%s] CONNECTION LEAK DETECTED: #%d held for %.1fs (uses=%d) - caller did not release!",
                    poolName, connId, heldFor, conn.useCount()));
        }

        @Override
        public void onBreakerStateChange(BreakerState oldState, BreakerState newState) {
            if (newState == BreakerState.OPEN) {
                log("ERROR",
                    String.format(
                        "[%s] Circuit breaker OPENED (%s -> %s) - failing fast",
                        poolName, oldState.value(), newState.value()));
            } else if (newState == BreakerState.HALF_OPEN) {
                log("WARN",
                    String.format(
                        "[%s] Circuit breaker HALF-OPEN (%s -> %s) - probing recovery",
                        poolName, oldState.value(), newState.value()));
            } else if (newState == BreakerState.CLOSED) {
                log("INFO",
                    String.format(
                        "[%s] Circuit breaker CLOSED (%s -> %s) - recovered",
                        poolName, oldState.value(), newState.value()));
            }
        }
    }

    /** Trivial connection stand-in for the demo (no external dependencies). */
    static final class DemoConnection implements AutoCloseable {
        private final int id;

        DemoConnection(int id) {
            this.id = id;
        }

        void execute(String sql) {
            // Simulate query execution
        }

        @Override
        public void close() {
            // Cleanup
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Lifecycle Logging Demo ===\n");

        // Set up pool with lifecycle logger
        LoggingListener logger = new LoggingListener("demo_pool");
        CallableFactory<DemoConnection> factory = CallableFactory.<DemoConnection>builder(() -> {
            int id = (int) (Math.random() * 10000);
            return new DemoConnection(id);
        }).validate(conn -> true).close(DemoConnection::close).build();

        PoolConfig config = PoolConfig.builder()
            .minSize(2)  // Warm 2 connections
            .maxSize(4)  // Max 4 connections
            .maxUses(5)  // Rotate after 5 uses
            .maintenanceInterval(2.0)  // Run maintenance every 2s
            .leakDetectionTimeout(3.0)  // Warn if held >3s
            .build();

        ConnectionPool<DemoConnection> pool = new ConnectionPool<>(
            factory,
            config,
            java.util.List.of(logger), null, null, "demo_pool");

        try (pool) {
            pool.open();
            System.out.println("Pool opened (warming min_size connections)\n");
            Thread.sleep(500);  // Let maintenance warm connections

            System.out.println("\n--- Scenario 1: Normal usage ---");
            pool.use(conn -> {
                conn.execute("SELECT 1");
                try {
                    Thread.sleep(200);  // Simulate query work
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            }, Timeout.of(5.0));

            System.out.println("\n--- Scenario 2: Concurrent requests ---");
            ExecutorService executor = Executors.newFixedThreadPool(8);
            try {
                IntStream.range(0, 10).forEach(i -> executor.submit(() -> {
                    try {
                        pool.use(conn -> {
                            conn.execute("SELECT " + i);
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            return null;
                        }, Timeout.of(5.0));
                    } catch (Exception e) {
                        // Ignore for demo
                    }
                }));
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } finally {
                if (!executor.isTerminated()) {
                    executor.shutdownNow();
                }
            }

            System.out.println("\n--- Scenario 3: Leak detection (simulated) ---");
            ConnectionPool<DemoConnection>.Lease lease = pool.connection(Timeout.of(5.0));
            DemoConnection conn = lease.get();
            System.out.println("Acquired connection, sleeping for 4s (will trigger leak warning)...");
            Thread.sleep(4000);  // Longer than leak_detection_timeout (3s)
            lease.close();

            System.out.println("\n--- Scenario 4: Connection rotation (max_uses=5) ---");
            // Use connections repeatedly to trigger rotation
            for (int i = 0; i < 6; i++) {
                pool.use(conn2 -> {
                    conn2.execute("SELECT 1");
                    return null;
                }, Timeout.of(5.0));
            }

            Thread.sleep(500);  // Let logs flush

            System.out.println("\n--- Pool closed ---");
        }

        System.out.println("\n=== Demo complete ===");
        System.out.println("Review the logs above to see the full connection lifecycle.");
    }
}
