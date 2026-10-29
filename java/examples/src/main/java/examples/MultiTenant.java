package examples;

import io.github.jeffeek.connpool.CallableFactory;
import io.github.jeffeek.connpool.ConnectionFactory;
import io.github.jeffeek.connpool.ConnectionPool;
import io.github.jeffeek.connpool.PoolConfig;
import io.github.jeffeek.connpool.PoolRegistry;
import io.github.jeffeek.connpool.PoolStats;
import io.github.jeffeek.connpool.Timeout;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Multi-tenant connection pooling with per-tier quotas via {@link PoolRegistry}.
 *
 * <p>This demonstrates using {@link PoolRegistry} to enforce different pool sizes per tenant tier.
 * Each tenant gets an isolated pool with appropriate limits based on their subscription.
 *
 * <h2>Key patterns</h2>
 * <ul>
 *   <li>{@link PoolRegistry} with a factory function that returns different configs per tenant
 *   <li>Quota enforcement via {@code maxSize} (caps concurrent connections)
 *   <li>Backpressure via {@code maxWaiters} (caps queued callers, fail fast when exhausted)
 *   <li>Per-tenant {@link PoolStats} for observability
 * </ul>
 *
 * <h2>Use cases</h2>
 * <ul>
 *   <li>SaaS platforms with free/pro/enterprise tiers
 *   <li>API gateways with per-customer rate limits
 *   <li>Multi-tenant databases with connection quotas
 * </ul>
 */
public final class MultiTenant {
    private MultiTenant() {
    }

    /**
     * Subscription tier configuration.
     *
     * @param name             Tier name (free/pro/enterprise)
     * @param maxConnections   Hard cap on concurrent DB connections
     * @param maxWaiters       Cap on queued callers (backpressure)
     * @param requestTimeout   Max seconds to wait for a connection
     */
    record TenantTier(String name, int maxConnections, int maxWaiters, double requestTimeout) {
    }

    /** Tier definitions. */
    private static final Map<String, TenantTier> TIERS = Map.of(
        "free", new TenantTier("free", 2, 5, 2.0),
        "pro", new TenantTier("pro", 10, 20, 5.0),
        "enterprise", new TenantTier("enterprise", 50, 100, 10.0)
    );

    /** Trivial stand-in for a real database connection (no JDBC driver needed for the demo). */
    static final class TenantConn implements AutoCloseable {
        private static final AtomicInteger IDS = new AtomicInteger();
        final int id = IDS.incrementAndGet();
        final String tenantId;

        TenantConn(String tenantId) {
            this.tenantId = tenantId;
        }

        String query(String sql) {
            return "conn#" + id + "@" + tenantId + " -> " + sql;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /**
     * Simulates tier lookup (in production, query from a user service).
     *
     * @param tenantId Tenant identifier
     * @return Tier name (free/pro/enterprise)
     */
    private static String getTenantTier(String tenantId) {
        Map<String, String> tierMap = Map.of(
            "tenant_1", "free",
            "tenant_2", "pro",
            "tenant_3", "enterprise"
        );
        return tierMap.getOrDefault(tenantId, "free");
    }

    /**
     * Factory function: returns a {@link ConnectionFactory} configured for the tenant's tier.
     *
     * <p>PoolRegistry calls this once per tenant key and builds the pool itself.
     *
     * @param tenantId Tenant identifier
     * @return ConnectionFactory for the tenant's connections
     */
    private static ConnectionFactory<TenantConn> factoryForTenant(String tenantId) {
        String tierName = getTenantTier(tenantId);
        TenantTier tier = TIERS.get(tierName);

        System.out.println("Creating factory for " + tenantId + " (tier: " + tier.name
            + ", max_size: " + tier.maxConnections + ")");

        return CallableFactory.<TenantConn>builder(() -> new TenantConn(tenantId))
            .validate(conn -> true)
            .close(TenantConn::close)
            .build();
    }

    /**
     * Build a per-tenant pool config based on tier.
     */
    private static PoolConfig configForTenant(String tenantId) {
        String tierName = getTenantTier(tenantId);
        TenantTier tier = TIERS.get(tierName);
        return PoolConfig.builder()
            .minSize(0)
            .maxSize(tier.maxConnections)
            .maxWaiters(tier.maxWaiters)
            .acquireTimeout(tier.requestTimeout)
            .maxLifetime(600.0)
            .validateAfterIdle(30.0)
            .build();
    }

    /**
     * Request result.
     *
     * @param tenantId  Tenant identifier
     * @param requestId Request number
     * @param status    "success" or "error"
     * @param elapsed   Elapsed time in seconds
     * @param error     Error message (null on success)
     */
    record RequestResult(String tenantId, int requestId, String status, double elapsed, String error) {
    }

    /**
     * Simulates a tenant's API request that needs a database connection.
     *
     * @param registry  Pool registry
     * @param tenantId  Tenant identifier
     * @param requestId Request number
     * @return Result with status and timing
     */
    private static RequestResult simulateTenantRequest(
        PoolRegistry<String, TenantConn> registry,
        String tenantId,
        int requestId
    ) {
        long start = System.nanoTime();
        try {
            try (ConnectionPool<TenantConn>.Lease lease = registry.connection(tenantId, Timeout.of(30.0))) {
                TenantConn conn = lease.get();
                // Simulate query work
                String result = conn.query("SELECT 1");
                Thread.sleep(10);
                double elapsed = (System.nanoTime() - start) / 1e9;
                return new RequestResult(tenantId, requestId, "success", elapsed, null);
            }
        } catch (Exception e) {
            double elapsed = (System.nanoTime() - start) / 1e9;
            return new RequestResult(tenantId, requestId, "error", elapsed, e.getMessage());
        }
    }

    /**
     * Runs the multi-tenant connection pooling demonstration.
     *
     * <p>Simulates 60 concurrent requests (20 per tenant) to demonstrate:
     * <ul>
     *   <li>Free tier (max_size=2) hits backpressure and fails fast
     *   <li>Pro tier (max_size=10) handles most requests
     *   <li>Enterprise tier (max_size=50) handles all requests
     * </ul>
     *
     * @throws Exception if initialization or execution fails
     */
    public static void run() throws Exception {
        System.out.println("=== Multi-Tenant Connection Pooling Demo ===\n");

        // PoolRegistry expects Function<K, ConnectionFactory<T>>.
        // We build per-tenant configs by wrapping the pool construction manually.
        // Since PoolRegistry uses a single shared config, we use per-tenant pools
        // via separate registries keyed to pre-built pools. Instead, we use
        // ConnectionPool directly per tenant and a simple Map.
        Map<String, ConnectionPool<TenantConn>> pools = new HashMap<>();
        for (String tenantId : List.of("tenant_1", "tenant_2", "tenant_3")) {
            ConnectionPool<TenantConn> pool = new ConnectionPool<>(
                factoryForTenant(tenantId),
                configForTenant(tenantId),
                null, null, null, "tenant-" + tenantId);
            pool.open();
            pools.put(tenantId, pool);
        }

        System.out.println("\n=== Simulating concurrent requests ===");

        // Simulate burst of requests: 20 requests per tenant concurrently
        // tenant_1 (free, max_size=2) will see queue exhaustion
        // tenant_2 (pro, max_size=10) should handle most requests
        // tenant_3 (enterprise, max_size=50) should handle all requests
        ExecutorService executor = Executors.newFixedThreadPool(60);
        List<Future<RequestResult>> futures = new ArrayList<>();

        for (String tenantId : List.of("tenant_1", "tenant_2", "tenant_3")) {
            ConnectionPool<TenantConn> pool = pools.get(tenantId);
            for (int reqId = 0; reqId < 20; reqId++) {
                final int rid = reqId;
                futures.add(executor.submit(() -> {
                    long start = System.nanoTime();
                    try {
                        try (ConnectionPool<TenantConn>.Lease lease = pool.connection(Timeout.of(30.0))) {
                            TenantConn conn = lease.get();
                            conn.query("SELECT " + rid);
                            Thread.sleep(10);
                            double elapsed = (System.nanoTime() - start) / 1e9;
                            return new RequestResult(tenantId, rid, "success", elapsed, null);
                        }
                    } catch (Exception e) {
                        double elapsed = (System.nanoTime() - start) / 1e9;
                        return new RequestResult(tenantId, rid, "error", elapsed, e.getMessage());
                    }
                }));
            }
        }

        List<RequestResult> results = new ArrayList<>();
        CompletableFuture.allOf(futures.stream()
            .map(f -> CompletableFuture.supplyAsync(() -> {
                try {
                    return f.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }))
            .toArray(CompletableFuture[]::new))
            .join();
        for (Future<RequestResult> future : futures) {
            results.add(future.get());
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        // Analyze results
        System.out.println("\n=== Results Summary ===");
        for (String tenantId : List.of("tenant_1", "tenant_2", "tenant_3")) {
            List<RequestResult> tenantResults = results.stream()
                .filter(r -> r.tenantId().equals(tenantId))
                .toList();
            long successes = tenantResults.stream().filter(r -> "success".equals(r.status())).count();
            List<RequestResult> errors = tenantResults.stream()
                .filter(r -> "error".equals(r.status()))
                .toList();

            String tierName = getTenantTier(tenantId);
            TenantTier tier = TIERS.get(tierName);

            System.out.println("\n" + tenantId + " (" + tier.name + " tier, max_size=" + tier.maxConnections + "):");
            System.out.println("  Total requests: " + tenantResults.size());
            System.out.println("  Successes: " + successes);
            System.out.println("  Errors: " + errors.size());

            if (!errors.isEmpty()) {
                Map<String, AtomicInteger> errorTypes = new ConcurrentHashMap<>();
                for (RequestResult err : errors) {
                    String errMsg = err.error() != null ? err.error() : "";
                    if (errMsg.contains("PoolExhaustedError") || errMsg.contains("exhausted")) {
                        errorTypes.computeIfAbsent("pool_exhausted", k -> new AtomicInteger()).incrementAndGet();
                    } else if (errMsg.toLowerCase().contains("timeout")) {
                        errorTypes.computeIfAbsent("timeout", k -> new AtomicInteger()).incrementAndGet();
                    } else {
                        errorTypes.computeIfAbsent("other", k -> new AtomicInteger()).incrementAndGet();
                    }
                }
                Map<String, Integer> errorBreakdown = new HashMap<>();
                errorTypes.forEach((k, v) -> errorBreakdown.put(k, v.get()));
                System.out.println("  Error breakdown: " + errorBreakdown);
            }

            // Pool stats
            ConnectionPool<TenantConn> pool = pools.get(tenantId);
            PoolStats stats = pool.stats();
            System.out.println("  Pool stats: size=" + stats.size() + ", idle=" + stats.idle()
                + ", in_use=" + stats.inUse());
            System.out.printf("  Acquire wait p95: %.3fs%n", stats.p95AcquireWait());
        }

        // Cleanup
        for (ConnectionPool<TenantConn> pool : pools.values()) {
            pool.close();
        }
        System.out.println("\n=== Demonstration complete ===");
        System.out.println("Key takeaway: Free tier hit backpressure limits, enterprise tier handled all load.");
    }

    /**
     * Entry point.
     *
     * @param args Command-line arguments (unused)
     * @throws Exception if the demo fails
     */
    public static void main(String[] args) throws Exception {
        run();
    }
}
