// Example: Regional failover with circuit breaker via PoolRegistry.
//
// This demonstrates using PoolRegistry with circuit breakers to handle regional database
// failover. When the primary region's breaker opens (too many connection failures),
// requests automatically fail fast and can fallback to a secondary region.
//
// Key patterns:
// - PoolRegistry with one pool per region (us-east-1, us-west-2)
// - Circuit breaker guards connection creation in each pool (shared config)
// - Graceful degradation: try primary, fallback to secondary on breaker open
//
// Use cases:
// - Multi-region database replicas with automatic failover
// - Geographic load distribution with health-aware routing
// - Disaster recovery with circuit-breaker-driven failover
//
// NOTE: PoolRegistry uses a single shared PoolConfig for all pools it creates. Per-pool
// listeners are not supported by the registry; to wire per-region listeners, manage pools
// manually in a Dictionary<string, ConnectionPool<T>>.

using System;
using System.Threading;
using ConnPool;
using Microsoft.Data.Sqlite;

namespace ConnPool.Examples;

public class BreakerAlertListener : PoolListener
{
    private readonly string _region;

    public BreakerAlertListener(string region)
    {
        _region = region;
    }

    public override void OnBreakerStateChange(BreakerState old, BreakerState @new)
    {
        if (@new == BreakerState.Open)
            Console.WriteLine($"ALERT: {_region} circuit breaker OPENED (region unhealthy)");
        else if (@new == BreakerState.HalfOpen)
            Console.WriteLine($"{_region} circuit breaker HALF-OPEN (probing recovery)");
        else if (@new == BreakerState.Closed && old != BreakerState.Closed)
            Console.WriteLine($"{_region} circuit breaker CLOSED (region recovered)");
    }
}

public class RegionalFailoverFactory : ConnectionFactory<SqliteConnection>
{
    private readonly string _region;
    private double _failureRate;

    public RegionalFailoverFactory(string region, double failureRate = 0.0)
    {
        _region = region;
        _failureRate = failureRate;
    }

    public void SetFailureRate(double rate) => _failureRate = rate;

    public override SqliteConnection Create()
    {
        // Simulate connection failures
        if (new Random().NextDouble() < _failureRate)
            throw new InvalidOperationException($"Failed to connect to {_region}");

        var dbPath = $"Data Source={System.IO.Path.GetTempPath()}db_{_region}.db";
        var conn = new SqliteConnection(dbPath);
        conn.Open();
        return conn;
    }

    public override bool Validate(SqliteConnection conn) => conn.State == System.Data.ConnectionState.Open;

    public override bool IsFatalError(Exception exc) => exc is InvalidOperationException;
}

public static class RegionalFailoverDemo
{
    private static readonly Dictionary<string, RegionalFailoverFactory> _factories = new();

    // Returns a ConnectionFactory<SqliteConnection> for the given region key.
    // PoolRegistry uses this factory when it lazily creates a pool for that region.
    private static ConnectionFactory<SqliteConnection> CreateFactoryForRegion(string region)
    {
        // Simulate regional health: both start healthy
        var factory = new RegionalFailoverFactory(region, failureRate: 0.0);
        _factories[region] = factory;
        return factory;
    }

    private static (string region, string status, double elapsed) ExecuteQueryWithFailover(
        PoolRegistry<string, SqliteConnection> registry,
        string query)
    {
        const string primary = "us-east-1";
        const string secondary = "us-west-2";

        var start = DateTime.UtcNow;

        // Try primary region
        try
        {
            using var lease = registry.Connection(primary);
            using var cmd = lease.Value.CreateCommand();
            cmd.CommandText = query;
            cmd.ExecuteScalar();

            var elapsed = (DateTime.UtcNow - start).TotalSeconds;
            return (primary, "success", elapsed);
        }
        catch (Exception primaryError)
        {
            Console.WriteLine($"Primary region ({primary}) failed: {primaryError.Message}");

            // Fallback to secondary region
            try
            {
                using var lease = registry.Connection(secondary);
                using var cmd = lease.Value.CreateCommand();
                cmd.CommandText = query;
                cmd.ExecuteScalar();

                var elapsed = (DateTime.UtcNow - start).TotalSeconds;
                Console.WriteLine($"Failover to {secondary} succeeded");
                return (secondary, "success (failover)", elapsed);
            }
            catch (Exception)
            {
                var elapsed = (DateTime.UtcNow - start).TotalSeconds;
                return ("both_failed", "error", elapsed);
            }
        }
    }

    public static void Run()
    {
        Console.WriteLine("=== Regional Failover Demo ===\n");

        // Shared config with circuit breaker: open after 3 failures, recover after 10s
        var config = new PoolConfig
        {
            MinSize = 1,
            MaxSize = 5,
            AcquireTimeout = 3.0,
            MaxLifetime = 300,
            Breaker = new BreakerConfig
            {
                FailureThreshold = 3,
                RecoveryTimeout = 10.0,
                HalfOpenMaxCalls = 2
            },
            // Retry once before giving up (breaker counts this as 1 failure)
            Retry = new RetryPolicy { MaxAttempts = 2, BaseDelay = 0.1 }
        };

        var regions = new PoolRegistry<string, SqliteConnection>(CreateFactoryForRegion, config);

        // Initialize databases
        foreach (var region in new[] { "us-east-1", "us-west-2" })
        {
            using var lease = regions.Connection(region);
            using var cmd = lease.Value.CreateCommand();
            cmd.CommandText = "CREATE TABLE IF NOT EXISTS metrics (ts INTEGER, value REAL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = $"INSERT INTO metrics VALUES ({DateTimeOffset.UtcNow.ToUnixTimeSeconds()}, 42.0)";
            cmd.ExecuteNonQuery();
        }

        Console.WriteLine("Phase 1: Both regions healthy\n");
        for (int i = 0; i < 3; i++)
        {
            var result = ExecuteQueryWithFailover(regions, "SELECT COUNT(*) FROM metrics");
            Console.WriteLine($"Request {i + 1}: {result.region} - {result.status} ({result.elapsed:F3}s)");
            Thread.Sleep(500);
        }

        Console.WriteLine("\n" + new string('=', 60));
        Console.WriteLine("Phase 2: Injecting failures into primary region (us-east-1)");
        Console.WriteLine(new string('=', 60) + "\n");

        // Simulate primary region becoming unhealthy
        _factories["us-east-1"].SetFailureRate(1.0);  // 100% failure rate

        Console.WriteLine("Attempting requests that will fail on primary...\n");
        for (int i = 0; i < 5; i++)
        {
            var result = ExecuteQueryWithFailover(regions, "SELECT COUNT(*) FROM metrics");
            Console.WriteLine($"Request {i + 1}: {result.region} - {result.status}");
            Thread.Sleep(500);
        }

        Console.WriteLine("\n" + new string('=', 60));
        Console.WriteLine("Phase 3: Circuit breaker opened - fast failover to secondary");
        Console.WriteLine(new string('=', 60) + "\n");

        // Now requests should fail fast and immediately use secondary
        for (int i = 0; i < 3; i++)
        {
            var result = ExecuteQueryWithFailover(regions, "SELECT COUNT(*) FROM metrics");
            Console.WriteLine($"Request {i + 1}: {result.region} - {result.status} ({result.elapsed:F3}s)");
            Console.WriteLine("  (Notice fast failover - no slow connection attempts)");
            Thread.Sleep(500);
        }

        Console.WriteLine("\n=== Pool Statistics ===\n");
        foreach (var region in new[] { "us-east-1", "us-west-2" })
        {
            var pool = regions.Pool(region);
            var stats = pool.Stats();
            Console.WriteLine($"{region}:");
            // PoolStats.BreakerState is the snake_case wire string ("closed"/"open"/"half_open"), or null.
            Console.WriteLine($"  Breaker state: {stats.BreakerState ?? "(unknown)"}");
            Console.WriteLine($"  Total created: {stats.Created}");
            Console.WriteLine($"  Total closed: {stats.Closed}");
            Console.WriteLine($"  Size/max: {stats.Size}/{stats.MaxSize}");
            Console.WriteLine($"  Acquire wait p95: {stats.P95AcquireWait:F3}s\n");
        }

        regions.Close();
        Console.WriteLine("=== Demo complete ===");
        Console.WriteLine("Key takeaway: Circuit breaker enables fast failover without waiting for timeouts.");
    }
}
