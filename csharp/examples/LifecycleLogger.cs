// Example: Comprehensive connection lifecycle logging via PoolListener.
//
// This demonstrates using PoolListener to log every pool event for debugging and diagnostics.
// Useful for understanding connection behavior, troubleshooting leaks, and analyzing performance.
//
// Key patterns:
// - PoolListener that logs all events with timestamps
// - Correlate connection lifecycle: created -> acquired -> released -> closed
// - Track connection age, hold time, wait time
// - Log breaker state changes and failures
//
// Use cases:
// - Development/staging: verbose logging for debugging
// - Production: conditional logging (only log slow operations, errors, leaks)
// - Performance analysis: identify hot connections, wait patterns

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ConnPool;
using Microsoft.Data.Sqlite;

namespace ConnPool.Examples;

public class LifecycleLogger : PoolListener
{
    /// <summary>
    /// PoolListener that logs every event with detailed context.
    /// Tracks connection lifetimes and correlates events by connection ID.
    /// </summary>
    private readonly string _poolName;
    private readonly ConcurrentDictionary<long, DateTime> _connectionBirthTimes = new();

    public LifecycleLogger(string poolName = "default")
    {
        _poolName = poolName;
    }

    private static void Log(string level, string message)
    {
        var timestamp = DateTime.Now.ToString("HH:mm:ss.fff");
        Console.WriteLine($"{timestamp} [{level}] {message}");
    }

    public override void OnConnectionCreated(PooledConnection? conn, double duration)
    {
        if (conn is null) return;
        var connId = conn.Id;
        _connectionBirthTimes[connId] = DateTime.UtcNow;
        Log("INFO",
            $"[{_poolName}] Connection #{connId} CREATED in {duration:F3}s " +
            $"(uses={conn.UseCount})");
    }

    public override void OnConnectionClosed(PooledConnection? conn, CloseReason reason)
    {
        if (conn is null) return;
        var connId = conn.Id;
        var age = _connectionBirthTimes.TryRemove(connId, out var birthTime)
            ? (DateTime.UtcNow - birthTime).TotalSeconds
            : 0.0;

        Log("INFO",
            $"[{_poolName}] Connection #{connId} CLOSED after {age:F1}s " +
            $"(reason={reason.Value()}, uses={conn.UseCount})");
    }

    public override void OnCreateFailed(Exception error)
    {
        Log("ERROR",
            $"[{_poolName}] Connection CREATE FAILED: {error.GetType().Name}: {error.Message}");
    }

    public override void OnAcquired(PooledConnection? conn, double waitTime)
    {
        if (conn is null) return;
        var connId = conn.Id;
        if (waitTime > 0.1)  // Log slow acquires (>100ms wait)
        {
            Log("WARN",
                $"[{_poolName}] Connection #{connId} ACQUIRED after {waitTime:F3}s wait " +
                "(slow: pool may be undersized)");
        }
        else
        {
            Log("DEBUG",
                $"[{_poolName}] Connection #{connId} ACQUIRED (wait={waitTime:F3}s, " +
                $"uses={conn.UseCount})");
        }
    }

    public override void OnReleased(PooledConnection? conn, double holdTime)
    {
        if (conn is null) return;
        var connId = conn.Id;
        if (holdTime > 10.0)  // Log long-held connections (>10s)
        {
            Log("WARN",
                $"[{_poolName}] Connection #{connId} RELEASED after {holdTime:F3}s " +
                "(held long: possible leak or slow query)");
        }
        else
        {
            Log("DEBUG",
                $"[{_poolName}] Connection #{connId} RELEASED (held={holdTime:F3}s, " +
                $"uses={conn.UseCount})");
        }
    }

    public override void OnAcquireTimeout(double waited)
    {
        Log("ERROR",
            $"[{_poolName}] ACQUIRE TIMEOUT after {waited:F3}s " +
            "(pool exhausted or connections taking too long)");
    }

    public override void OnConnectionLeak(PooledConnection? conn, double heldFor)
    {
        if (conn is null) return;
        var connId = conn.Id;
        Log("ERROR",
            $"[{_poolName}] CONNECTION LEAK DETECTED: #{connId} held for {heldFor:F1}s " +
            $"(uses={conn.UseCount}) - caller did not release!");
    }

    public override void OnBreakerStateChange(BreakerState oldState, BreakerState newState)
    {
        if (newState == BreakerState.Open)
        {
            Log("ERROR",
                $"[{_poolName}] Circuit breaker OPENED " +
                $"({oldState.Value()} -> {newState.Value()}) - failing fast");
        }
        else if (newState == BreakerState.HalfOpen)
        {
            Log("WARN",
                $"[{_poolName}] Circuit breaker HALF-OPEN " +
                $"({oldState.Value()} -> {newState.Value()}) - probing recovery");
        }
        else if (newState == BreakerState.Closed)
        {
            Log("INFO",
                $"[{_poolName}] Circuit breaker CLOSED " +
                $"({oldState.Value()} -> {newState.Value()}) - recovered");
        }
    }
}

public static class LifecycleLoggerDemo
{
    public static void Run()
    {
        Console.WriteLine("=== Lifecycle Logging Demo ===\n");

        // Set up pool with lifecycle logger
        var logger = new LifecycleLogger("demo_pool");
        var factory = new AdoNetFactory(
            () => new SqliteConnection("Data Source=:memory:"));

        var config = new PoolConfig
        {
            MinSize = 2,  // Warm 2 connections
            MaxSize = 4,  // Max 4 connections
            MaxUses = 5,  // Rotate after 5 uses
            MaintenanceInterval = 2,  // Run maintenance every 2s
            LeakDetectionTimeout = 3.0  // Warn if held >3s
        };

        using var pool = new ConnectionPool<System.Data.Common.DbConnection>(
            factory,
            config,
            listeners: new[] { (PoolListener)logger },
            name: "demo_pool");

        pool.Open();
        Console.WriteLine("Pool opened (warming min_size connections)\n");
        Thread.Sleep(500);  // Let maintenance warm connections

        Console.WriteLine("\n--- Scenario 1: Normal usage ---");
        pool.Use(conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1";
            cmd.ExecuteScalar();
            Thread.Sleep(200);  // Simulate query work
        });

        Console.WriteLine("\n--- Scenario 2: Concurrent requests ---");
        var tasks = Enumerable.Range(0, 10).Select(i => Task.Run(() =>
        {
            pool.Use(conn =>
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT {i}";
                cmd.ExecuteScalar();
                Thread.Sleep(100);
            });
        })).ToArray();
        Task.WaitAll(tasks);

        Console.WriteLine("\n--- Scenario 3: Leak detection (simulated) ---");
        var conn2 = pool.Acquire();
        Console.WriteLine($"Acquired connection, sleeping for 4s (will trigger leak warning)...");
        Thread.Sleep(4000);  // Longer than leak_detection_timeout (3s)
        pool.Release(conn2);

        Console.WriteLine("\n--- Scenario 4: Connection rotation (max_uses=5) ---");
        // Use same connection repeatedly to trigger rotation
        for (int i = 0; i < 6; i++)
        {
            pool.Use(conn =>
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT 1";
                cmd.ExecuteScalar();
            });
        }

        Thread.Sleep(500);  // Let logs flush

        Console.WriteLine("\n--- Pool closed ---");
        Console.WriteLine("\n=== Demo complete ===");
        Console.WriteLine("Review the logs above to see the full connection lifecycle.");
    }
}
