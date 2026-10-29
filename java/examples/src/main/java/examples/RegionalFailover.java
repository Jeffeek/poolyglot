package examples;

import io.github.jeffeek.connpool.BreakerConfig;
import io.github.jeffeek.connpool.BreakerState;
import io.github.jeffeek.connpool.ConnectionFactory;
import io.github.jeffeek.connpool.ConnectionPool;
import io.github.jeffeek.connpool.PoolConfig;
import io.github.jeffeek.connpool.PoolListener;
import io.github.jeffeek.connpool.PoolRegistry;
import io.github.jeffeek.connpool.PoolStats;
import io.github.jeffeek.connpool.RetryPolicy;
import io.github.jeffeek.connpool.Timeout;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Example: Regional failover with circuit breaker via PoolRegistry.
 *
 * <p>This demonstrates using PoolRegistry with circuit breakers to handle regional database
 * failover. When the primary region's breaker opens (too many connection failures), requests
 * automatically fail fast and can fallback to a secondary region.
 *
 * <h2>Key patterns:</h2>
 * <ul>
 *   <li>PoolRegistry with one pool per region (us-east-1, us-west-2)
 *   <li>Circuit breaker guards connection creation in each pool
 *   <li>OnBreakerStateChange listener for alerting
 *   <li>Graceful degradation: try primary, fallback to secondary on breaker open
 * </ul>
 *
 * <h2>Use cases:</h2>
 * <ul>
 *   <li>Multi-region database replicas with automatic failover
 *   <li>Geographic load distribution with health-aware routing
 *   <li>Disaster recovery with circuit-breaker-driven failover
 * </ul>
 *
 * <p>Run with: {@code mvn compile && mvn exec:java -Dexec.mainClass="examples.RegionalFailover"}
 */
public final class RegionalFailover {
    private RegionalFailover() {
    }

    /**
     * Listener that alerts on circuit breaker state changes. Thread-safe: hook runs synchronously
     * under the pool's internal lock.
     */
    static final class BreakerAlertListener extends PoolListener {
        private final String region;

        BreakerAlertListener(String region) {
            this.region = region;
        }

        @Override
        public void onBreakerStateChange(BreakerState old, BreakerState current) {
            if (current == BreakerState.OPEN) {
                System.out.println("ALERT: " + region + " circuit breaker OPENED (region unhealthy)");
            } else if (current == BreakerState.HALF_OPEN) {
                System.out.println("WARN:  " + region + " circuit breaker HALF-OPEN (probing recovery)");
            } else if (current == BreakerState.CLOSED && old != BreakerState.CLOSED) {
                System.out.println("OK:    " + region + " circuit breaker CLOSED (region recovered)");
            }
        }
    }

    /**
     * Fake database connection for demonstration. Thread-safe: immutable after construction (id is
     * final, queries are read-only).
     */
    static final class DbConnection implements AutoCloseable {
        private static int nextId = 0;
        final int id;
        final String region;

        DbConnection(String region) {
            synchronized (DbConnection.class) {
                this.id = ++nextId;
            }
            this.region = region;
        }

        String query(String sql) {
            return "conn#" + id + "@" + region + " -> " + sql;
        }

        @Override
        public void close() {
            // no-op for demo
        }
    }

    /**
     * Factory that simulates regional connection failures. Thread-safe: failureRate reads via
     * AtomicReference (volatile read), Random is ThreadLocalRandom under the hood for lock-free
     * random generation.
     */
    static final class RegionalFailoverFactory extends ConnectionFactory<DbConnection> {
        private final String region;
        private final AtomicReference<Double> failureRate;
        private final Random random = new Random();

        RegionalFailoverFactory(String region, double failureRate) {
            this.region = region;
            this.failureRate = new AtomicReference<>(failureRate);
        }

        void setFailureRate(double rate) {
            failureRate.set(rate);
        }

        @Override
        public DbConnection create() {
            // Simulate connection failures
            if (random.nextDouble() < failureRate.get()) {
                throw new RuntimeException("Failed to connect to " + region);
            }
            return new DbConnection(region);
        }

        @Override
        public boolean validate(DbConnection conn) {
            return true;
        }

        @Override
        public boolean isFatalError(Throwable exc) {
            return exc instanceof RuntimeException;
        }
    }

    /**
     * Result of a query attempt: which region served it, status, and elapsed time.
     */
    record QueryResult(String region, String status, double elapsed) {
    }

    // Factories registry for injecting failures during the demo
    private static final ConcurrentHashMap<String, RegionalFailoverFactory> factories =
        new ConcurrentHashMap<>();

    /**
     * Returns a {@link ConnectionFactory} for the given region, storing it so failure rates can be
     * injected during the demo. PoolRegistry uses this to build one pool per region on demand.
     */
    private static ConnectionFactory<DbConnection> factoryForRegion(String region) {
        // Simulate regional health: both start healthy
        RegionalFailoverFactory factory = new RegionalFailoverFactory(region, 0.0);
        factories.put(region, factory);
        return factory;
    }

    /** Shared pool config used by the registry for every region pool. */
    private static PoolConfig regionConfig() {
        return PoolConfig.builder()
            .minSize(1)
            .maxSize(5)
            .acquireTimeout(3.0)
            .maxLifetime(300.0)
            // Circuit breaker config: open after 3 failures, recover after 10s
            .breaker(BreakerConfig.builder()
                .failureThreshold(3)
                .recoveryTimeout(10.0)
                .halfOpenMaxCalls(2)
                .build())
            // Retry once before giving up (breaker counts this as 1 failure)
            .retry(RetryPolicy.builder()
                .maxAttempts(2)
                .build())
            .build();
    }

    /**
     * Executes a query with automatic failover from primary to secondary region. Thread-safe: uses
     * pool's thread-safe acquire/release, no shared mutable state.
     */
    private static QueryResult executeQueryWithFailover(
        PoolRegistry<String, DbConnection> registry, String query) {
        final String primary = "us-east-1";
        final String secondary = "us-west-2";

        long start = System.nanoTime();

        // Try primary region
        try {
            try (ConnectionPool<DbConnection>.Lease lease = registry.connection(primary, Timeout.of(3.0))) {
                lease.get().query(query);
                double elapsed = (System.nanoTime() - start) / 1e9;
                return new QueryResult(primary, "success", elapsed);
            }
        } catch (Exception primaryError) {
            System.out.println("Primary region (" + primary + ") failed: " + primaryError.getMessage());

            // Fallback to secondary region
            try {
                try (ConnectionPool<DbConnection>.Lease lease = registry.connection(secondary, Timeout.of(3.0))) {
                    lease.get().query(query);
                    double elapsed = (System.nanoTime() - start) / 1e9;
                    System.out.println("OK: Failover to " + secondary + " succeeded");
                    return new QueryResult(secondary, "success (failover)", elapsed);
                }
            } catch (Exception secondaryError) {
                double elapsed = (System.nanoTime() - start) / 1e9;
                return new QueryResult("both_failed", "error", elapsed);
            }
        }
    }

    /**
     * Runs the regional failover demonstration in three phases: healthy, degraded (primary
     * failing), and breaker-open (fast failover).
     */
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Regional Failover Demo ===\n");

        PoolRegistry<String, DbConnection> regions = new PoolRegistry<>(RegionalFailover::factoryForRegion, regionConfig());

        // Initialize databases
        for (String region : new String[] {"us-east-1", "us-west-2"}) {
            try (ConnectionPool<DbConnection>.Lease lease = regions.pool(region).connection(Timeout.of(3.0))) {
                System.out.println("Initialized " + region + " database");
            }
        }

        System.out.println("\nPhase 1: Both regions healthy\n");
        for (int i = 0; i < 3; i++) {
            QueryResult result = executeQueryWithFailover(regions, "SELECT COUNT(*) FROM metrics");
            System.out.printf("Request %d: %s - %s (%.3fs)%n", i + 1, result.region, result.status, result.elapsed);
            Thread.sleep(500);
        }

        System.out.println("\n" + "=".repeat(60));
        System.out.println("Phase 2: Injecting failures into primary region (us-east-1)");
        System.out.println("=".repeat(60) + "\n");

        // Simulate primary region becoming unhealthy
        factories.get("us-east-1").setFailureRate(1.0);  // 100% failure rate

        System.out.println("Attempting requests that will fail on primary...\n");
        for (int i = 0; i < 5; i++) {
            QueryResult result = executeQueryWithFailover(regions, "SELECT COUNT(*) FROM metrics");
            System.out.printf("Request %d: %s - %s%n", i + 1, result.region, result.status);
            Thread.sleep(500);
        }

        System.out.println("\n" + "=".repeat(60));
        System.out.println("Phase 3: Circuit breaker opened - fast failover to secondary");
        System.out.println("=".repeat(60) + "\n");

        // Now requests should fail fast and immediately use secondary
        for (int i = 0; i < 3; i++) {
            QueryResult result = executeQueryWithFailover(regions, "SELECT COUNT(*) FROM metrics");
            System.out.printf("Request %d: %s - %s (%.3fs)%n", i + 1, result.region, result.status, result.elapsed);
            System.out.println("  (Notice fast failover - no slow connection attempts)");
            Thread.sleep(500);
        }

        System.out.println("\n=== Pool Statistics ===\n");
        for (String region : new String[] {"us-east-1", "us-west-2"}) {
            ConnectionPool<DbConnection> pool = regions.pool(region);
            PoolStats stats = pool.stats();
            System.out.println(region + ":");
            System.out.println("  Breaker state: " + stats.breakerState());
            System.out.println("  Total created: " + stats.created());
            System.out.println("  Total closed: " + stats.closed());
            System.out.println("  Size/max: " + stats.size() + "/" + stats.maxSize());
            System.out.printf("  Acquire wait p95: %.3fs%n%n", stats.p95AcquireWait());
        }

        regions.close();
        System.out.println("=== Demo complete ===");
        System.out.println("Key takeaway: Circuit breaker enables fast failover without waiting for timeouts.");
    }
}
