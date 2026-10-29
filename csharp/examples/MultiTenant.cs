// Example: Multi-tenant connection pooling with per-tier quotas via PoolRegistry.
//
// This demonstrates using PoolRegistry to enforce different pool sizes per tenant tier.
// Each tenant gets an isolated pool with appropriate limits based on their subscription.
//
// Key patterns:
// - PoolRegistry with a factory function that returns a ConnectionFactory per tenant
// - Quota enforcement via MaxSize (caps concurrent connections) — shared config here
// - Backpressure via MaxWaiters (caps queued callers, fail fast when exhausted)
// - Per-tenant Stats() for observability
//
// NOTE: PoolRegistry uses a single shared PoolConfig for all pools it creates. If you need
// genuinely different per-tenant configs (different MaxSize per tier), manage pools manually
// in a Dictionary<string, ConnectionPool<T>> instead.
//
// Use cases:
// - SaaS platforms with free/pro/enterprise tiers
// - API gateways with per-customer rate limits
// - Multi-tenant databases with connection quotas

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConnPool;
using Microsoft.Data.Sqlite;

namespace ConnPool.Examples;

public record TenantTier(
    string Name,
    int MaxConnections,    // Hard cap on concurrent DB connections
    int MaxWaiters,        // Cap on queued callers (backpressure)
    double RequestTimeout  // Max seconds to wait for a connection
);

public static class MultiTenantDemo
{
    // Tier definitions
    private static readonly Dictionary<string, TenantTier> Tiers = new()
    {
        ["free"] = new("free", MaxConnections: 2, MaxWaiters: 5, RequestTimeout: 2.0),
        ["pro"] = new("pro", MaxConnections: 10, MaxWaiters: 20, RequestTimeout: 5.0),
        ["enterprise"] = new("enterprise", MaxConnections: 50, MaxWaiters: 100, RequestTimeout: 10.0)
    };

    private static string GetTenantTier(string tenantId)
    {
        // Simulate tier lookup (in production, query from a user service)
        var tierMap = new Dictionary<string, string>
        {
            ["tenant_1"] = "free",
            ["tenant_2"] = "pro",
            ["tenant_3"] = "enterprise"
        };
        return tierMap.TryGetValue(tenantId, out var tier) ? tier : "free";
    }

    // Returns a ConnectionFactory<SqliteConnection> for the given tenant key.
    // PoolRegistry uses this factory when it lazily creates a pool for that tenant.
    private static ConnectionFactory<SqliteConnection> CreateFactoryForTenant(string tenantId)
    {
        var tierName = GetTenantTier(tenantId);
        var tier = Tiers[tierName];

        Console.WriteLine($"Creating factory for {tenantId} (tier: {tier.Name}, max_size: {tier.MaxConnections})");

        var dbPath = $"Data Source={System.IO.Path.GetTempPath()}tenant_{tenantId}.db";

        return new CallableFactory<SqliteConnection>(
            create: () =>
            {
                var conn = new SqliteConnection(dbPath);
                conn.Open();
                return conn;
            },
            validate: conn => conn.State == System.Data.ConnectionState.Open,
            close: conn => conn.Dispose());
    }

    private static (string tenantId, int requestId, string status, double elapsed, string? error) SimulateTenantRequest(
        PoolRegistry<string, SqliteConnection> registry,
        string tenantId,
        int requestId)
    {
        var start = DateTime.UtcNow;
        try
        {
            using var lease = registry.Connection(tenantId);
            // Simulate query work
            using var cmd = lease.Value.CreateCommand();
            cmd.CommandText = "SELECT 1";
            cmd.ExecuteScalar();

            var elapsed = (DateTime.UtcNow - start).TotalSeconds;
            return (tenantId, requestId, "success", elapsed, null);
        }
        catch (Exception e)
        {
            var elapsed = (DateTime.UtcNow - start).TotalSeconds;
            return (tenantId, requestId, "error", elapsed, e.Message);
        }
    }

    public static void Run()
    {
        Console.WriteLine("=== Multi-Tenant Connection Pooling Demo ===\n");

        // Shared config — use a moderate limit to demonstrate backpressure on free tier.
        // (For truly per-tenant configs, manage pools in a Dictionary<string, ConnectionPool<T>>.)
        var sharedConfig = new PoolConfig
        {
            MinSize = 0,  // Lazy: don't warm connections until first request
            MaxSize = 2,
            MaxWaiters = 5,
            AcquireTimeout = 2.0,
            MaxLifetime = 600,
            ValidateAfterIdle = 30
        };

        // Create per-tenant pool registry
        var tenants = new PoolRegistry<string, SqliteConnection>(
            CreateFactoryForTenant,
            sharedConfig);

        // Initialize tenant databases
        foreach (var tenantId in new[] { "tenant_1", "tenant_2", "tenant_3" })
        {
            using var lease = tenants.Connection(tenantId);
            using var cmd = lease.Value.CreateCommand();
            cmd.CommandText = "CREATE TABLE IF NOT EXISTS data (id INTEGER, value TEXT)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO data VALUES (1, 'test')";
            cmd.ExecuteNonQuery();
        }

        Console.WriteLine("\n=== Simulating concurrent requests ===");

        // Simulate burst of requests: 20 requests per tenant concurrently
        var tasks = new List<Task<(string, int, string, double, string?)>>();
        foreach (var tenantId in new[] { "tenant_1", "tenant_2", "tenant_3" })
        {
            for (int reqId = 0; reqId < 20; reqId++)
            {
                var tid = tenantId;
                var rid = reqId;
                tasks.Add(Task.Run(() => SimulateTenantRequest(tenants, tid, rid)));
            }
        }

        Task.WaitAll(tasks.ToArray());
        var results = tasks.Select(t => t.Result).ToList();

        // Analyze results
        Console.WriteLine("\n=== Results Summary ===");
        foreach (var tenantId in new[] { "tenant_1", "tenant_2", "tenant_3" })
        {
            var tenantResults = results.Where(r => r.Item1 == tenantId).ToList();
            var successes = tenantResults.Where(r => r.Item3 == "success").ToList();
            var errors = tenantResults.Where(r => r.Item3 == "error").ToList();

            var tierName = GetTenantTier(tenantId);
            var tier = Tiers[tierName];

            Console.WriteLine($"\n{tenantId} ({tier.Name} tier):");
            Console.WriteLine($"  Total requests: {tenantResults.Count}");
            Console.WriteLine($"  Successes: {successes.Count}");
            Console.WriteLine($"  Errors: {errors.Count}");

            if (errors.Any())
            {
                var errorTypes = new Dictionary<string, int>();
                foreach (var err in errors)
                {
                    var errMsg = err.Item5 ?? "";
                    if (errMsg.Contains("PoolExhaustedError") || errMsg.Contains("exhausted"))
                        errorTypes["pool_exhausted"] = errorTypes.GetValueOrDefault("pool_exhausted") + 1;
                    else if (errMsg.ToLower().Contains("timeout"))
                        errorTypes["timeout"] = errorTypes.GetValueOrDefault("timeout") + 1;
                    else
                        errorTypes["other"] = errorTypes.GetValueOrDefault("other") + 1;
                }
                Console.WriteLine($"  Error breakdown: {string.Join(", ", errorTypes.Select(kv => $"{kv.Key}={kv.Value}"))}");
            }

            // Pool stats
            var pool = tenants.Pool(tenantId);
            var stats = pool.Stats();
            Console.WriteLine($"  Pool stats: size={stats.Size}, idle={stats.Idle}, in_use={stats.InUse}");
            Console.WriteLine($"  Acquire wait p95: {stats.P95AcquireWait:F3}s");
        }

        // Cleanup
        tenants.Close();
        Console.WriteLine("\n=== Demonstration complete ===");
        Console.WriteLine("Key takeaway: PoolRegistry isolates tenants; backpressure fires when the shared pool is full.");
    }
}
