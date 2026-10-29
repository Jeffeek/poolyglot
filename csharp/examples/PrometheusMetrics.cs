// Example: Exporting pool metrics in Prometheus/StatsD format via PoolListener.
//
// This demonstrates how to bridge poolyglot's observability hooks to external monitoring
// systems. The listener tracks counters and periodically samples PoolStats for gauges.
//
// Key patterns:
// - Implement PoolListener to capture pool events
// - Use Stats() snapshot for gauge values (sizes, percentiles)
// - Map CloseReason/BreakerState to metric labels
// - Push heavy I/O (actual HTTP export) to a background queue/task
//
// NOTE: Synchronous hooks mean you should NOT do blocking I/O directly in the listener.
// Instead, push events to a queue and let a background worker send them.

using System;
using System.Collections.Generic;
using System.Linq;
using ConnPool;

namespace ConnPool.Examples;

public class PrometheusMetrics : PoolListener
{
    /// <summary>
    /// PoolListener that tracks metrics compatible with Prometheus/StatsD.
    ///
    /// Counters are incremented on events; gauges are read from pool.Stats() periodically.
    /// In production, push these to your metrics backend (Prometheus pushgateway, StatsD, etc.)
    /// from a background task to avoid blocking the pool.
    /// </summary>
    private readonly string _poolName;

    // Counters (monotonic) — exposed as internal for the demo's Run() method
    internal long ConnectionsCreatedTotal;
    internal long ConnectionsClosedTotal;
    private readonly Dictionary<string, long> _connectionsClosedByReason = new();
    internal long CreateFailuresTotal;
    internal long AcquiresTotal;
    internal long AcquireTimeoutsTotal;
    internal long ReleasesTotal;
    internal long LeaksDetectedTotal;
    internal long BreakerStateChangesTotal;
    private BreakerState _breakerStateCurrent = BreakerState.Closed;

    public PrometheusMetrics(string poolName = "default")
    {
        _poolName = poolName;
    }

    public override void OnConnectionCreated(PooledConnection? conn, double duration)
    {
        Interlocked.Increment(ref ConnectionsCreatedTotal);
        // Example: connpool_connections_created_total{pool="default"} 42
        Console.WriteLine($"connpool_connections_created_total{{pool=\"{_poolName}\"}} {ConnectionsCreatedTotal}");
    }

    public override void OnConnectionClosed(PooledConnection? conn, CloseReason reason)
    {
        Interlocked.Increment(ref ConnectionsClosedTotal);
        var reasonLabel = reason.Value();  // "expired", "max_uses", "error", "evicted", "shutdown"

        lock (_connectionsClosedByReason)
        {
            if (!_connectionsClosedByReason.ContainsKey(reasonLabel))
                _connectionsClosedByReason[reasonLabel] = 0;
            _connectionsClosedByReason[reasonLabel]++;
        }

        // Example: connpool_connections_closed_total{pool="default",reason="expired"} 12
        Console.WriteLine(
            $"connpool_connections_closed_total{{pool=\"{_poolName}\",reason=\"{reasonLabel}\"}} " +
            $"{_connectionsClosedByReason[reasonLabel]}");
    }

    public override void OnCreateFailed(Exception error)
    {
        Interlocked.Increment(ref CreateFailuresTotal);
        Console.WriteLine($"connpool_create_failures_total{{pool=\"{_poolName}\"}} {CreateFailuresTotal}");
    }

    public override void OnAcquired(PooledConnection? conn, double waitTime)
    {
        Interlocked.Increment(ref AcquiresTotal);
    }

    public override void OnReleased(PooledConnection? conn, double holdTime)
    {
        Interlocked.Increment(ref ReleasesTotal);
    }

    public override void OnAcquireTimeout(double waited)
    {
        Interlocked.Increment(ref AcquireTimeoutsTotal);
        Console.WriteLine($"connpool_acquire_timeouts_total{{pool=\"{_poolName}\"}} {AcquireTimeoutsTotal}");
    }

    public override void OnConnectionLeak(PooledConnection? conn, double heldFor)
    {
        Interlocked.Increment(ref LeaksDetectedTotal);
        Console.WriteLine(
            $"connpool_leaks_detected_total{{pool=\"{_poolName}\"}} {LeaksDetectedTotal} " +
            $"(connection held for {heldFor:F1}s)");
    }

    public override void OnBreakerStateChange(BreakerState old, BreakerState @new)
    {
        Interlocked.Increment(ref BreakerStateChangesTotal);
        _breakerStateCurrent = @new;
        Console.WriteLine(
            $"connpool_breaker_state_changes_total{{pool=\"{_poolName}\"}} {BreakerStateChangesTotal} " +
            $"(state: {old.Value()} -> {@new.Value()})");
    }

    /// <summary>
    /// Convert PoolStats snapshot to Prometheus gauge format.
    /// Call this periodically (e.g., every 15s) and push to your metrics backend.
    /// </summary>
    public List<string> ExportGauges(PoolStats stats)
    {
        var lines = new List<string>
        {
            // Pool sizes
            $"connpool_size{{pool=\"{_poolName}\"}} {stats.Size}",
            $"connpool_idle{{pool=\"{_poolName}\"}} {stats.Idle}",
            $"connpool_in_use{{pool=\"{_poolName}\"}} {stats.InUse}",
            $"connpool_waiting{{pool=\"{_poolName}\"}} {stats.Waiting}",
            $"connpool_max_size{{pool=\"{_poolName}\"}} {stats.MaxSize}",

            // Acquire-wait percentiles (seconds)
            $"connpool_acquire_wait_p50{{pool=\"{_poolName}\"}} {stats.P50AcquireWait}",
            $"connpool_acquire_wait_p95{{pool=\"{_poolName}\"}} {stats.P95AcquireWait}",
            $"connpool_acquire_wait_p99{{pool=\"{_poolName}\"}} {stats.P99AcquireWait}",

            // Create-latency percentiles (seconds)
            $"connpool_create_latency_p50{{pool=\"{_poolName}\"}} {stats.P50CreateLatency}",
            $"connpool_create_latency_p95{{pool=\"{_poolName}\"}} {stats.P95CreateLatency}",
            $"connpool_create_latency_p99{{pool=\"{_poolName}\"}} {stats.P99CreateLatency}",

            // Circuit breaker state (0=closed, 1=open, 2=half_open)
            $"connpool_breaker_state{{pool=\"{_poolName}\"}} {BreakerStateValue(_breakerStateCurrent)}"
        };

        return lines;
    }

    private static int BreakerStateValue(BreakerState state)
    {
        return state.Value() switch
        {
            "closed" => 0,
            "open" => 1,
            "half_open" => 2,
            _ => -1
        };
    }
}

// Example usage
public static class PrometheusMetricsDemo
{
    public static void Run()
    {
        Console.WriteLine("=== Prometheus Metrics Demo ===\n");

        // Set up pool with metrics listener
        var metrics = new PrometheusMetrics("demo_pool");
        var factory = new AdoNetFactory(
            () => new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:"));
        var config = new PoolConfig
        {
            MinSize = 2,
            MaxSize = 4,
            MaintenanceInterval = 2
        };

        using var pool = new ConnectionPool<System.Data.Common.DbConnection>(
            factory,
            config,
            listeners: new[] { (PoolListener)metrics },
            name: "demo_pool");

        pool.Open();

        // Do some work
        for (int i = 0; i < 10; i++)
        {
            pool.Use(conn =>
            {
                var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT 1";
                cmd.ExecuteScalar();
            });
            System.Threading.Thread.Sleep(100);
        }

        // Export gauges periodically (in production, do this from a background task)
        var stats = pool.Stats();
        Console.WriteLine("\n=== Prometheus Metrics ===");
        foreach (var line in metrics.ExportGauges(stats))
        {
            Console.WriteLine(line);
        }

        Console.WriteLine($"\nconnpool_connections_created_total{{pool=\"demo_pool\"}} {metrics.ConnectionsCreatedTotal}");
        Console.WriteLine($"connpool_acquires_total{{pool=\"demo_pool\"}} {metrics.AcquiresTotal}");
        Console.WriteLine($"connpool_releases_total{{pool=\"demo_pool\"}} {metrics.ReleasesTotal}");
    }
}
