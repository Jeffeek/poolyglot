// Built-in metrics: a listener that counts events, plus an immutable snapshot.
using System;
using System.Collections.Generic;
using System.Linq;

namespace ConnPool
{
    public sealed record PoolStats
    {
        public string Name { get; init; } = "pool";
        public int MaxSize { get; init; }
        public int Size { get; init; }            // idle + in use + being created
        public int Idle { get; init; }            // available for checkout right now
        public int InUse { get; init; }           // checked out, or briefly reserved by a background idle check
        public int Waiting { get; init; }         // callers currently blocked in Acquire()
        public int Created { get; init; }
        public int CreateFailures { get; init; }
        public int Acquired { get; init; }
        public int Released { get; init; }
        public int Timeouts { get; init; }
        public int Closed { get; init; }
        public int Leaks { get; init; }           // borrowed connections flagged as possibly leaked
        public IReadOnlyDictionary<string, int> ClosedByReason { get; init; } =
            new Dictionary<string, int>();

        // acquire-wait aggregates, all over the last AcquireWaitWindow samples
        public double AvgAcquireWait { get; init; }
        public double P50AcquireWait { get; init; }
        public double P95AcquireWait { get; init; }
        public double P99AcquireWait { get; init; }

        // create-latency aggregates (factory.Create() duration), same window
        public double AvgCreateLatency { get; init; }
        public double P50CreateLatency { get; init; }
        public double P95CreateLatency { get; init; }
        public double P99CreateLatency { get; init; }

        public string? BreakerState { get; init; }

        /// <summary>Snake-cased view, with keys identical across every poolyglot language.</summary>
        public IReadOnlyDictionary<string, object?> ToDictionary() => new Dictionary<string, object?>
        {
            ["name"] = Name,
            ["max_size"] = MaxSize,
            ["size"] = Size,
            ["idle"] = Idle,
            ["in_use"] = InUse,
            ["waiting"] = Waiting,
            ["created"] = Created,
            ["create_failures"] = CreateFailures,
            ["acquired"] = Acquired,
            ["released"] = Released,
            ["timeouts"] = Timeouts,
            ["closed"] = Closed,
            ["leaks"] = Leaks,
            ["closed_by_reason"] = ClosedByReason,
            ["avg_acquire_wait"] = AvgAcquireWait,
            ["p50_acquire_wait"] = P50AcquireWait,
            ["p95_acquire_wait"] = P95AcquireWait,
            ["p99_acquire_wait"] = P99AcquireWait,
            ["avg_create_latency"] = AvgCreateLatency,
            ["p50_create_latency"] = P50CreateLatency,
            ["p95_create_latency"] = P95CreateLatency,
            ["p99_create_latency"] = P99CreateLatency,
            ["breaker_state"] = BreakerState,
        };

        public override string ToString() =>
            $"PoolStats(name={Name}, size={Size}/{MaxSize}, idle={Idle}, in_use={InUse}, " +
            $"waiting={Waiting}, created={Created}, closed={Closed})";
    }

    public static class Statistics
    {
        /// <summary>How many recent acquire-wait / create-latency samples to keep (bounded memory).</summary>
        public const int AcquireWaitWindow = 1024;

        // The quantiles reported in every PoolStats snapshot.
        public const double P50 = 0.50;
        public const double P95 = 0.95;
        public const double P99 = 0.99;

        /// <summary>Linear-interpolated percentile of an already-sorted list. <paramref name="quantile"/> in [0, 1].</summary>
        public static double Percentile(IReadOnlyList<double> sortedValues, double quantile)
        {
            if (sortedValues.Count == 0)
                return 0.0;
            var rank = (sortedValues.Count - 1) * quantile;
            var lower = (int)rank;
            var upper = Math.Min(lower + 1, sortedValues.Count - 1);
            return sortedValues[lower] + (sortedValues[upper] - sortedValues[lower]) * (rank - lower);
        }
    }

    internal sealed class StatsCollector : PoolListener
    {
        // One lock guards everything so a snapshot of counters + windows + by-reason is consistent.
        // (A ConcurrentDictionary would make the by-reason map lock-free but break that atomicity,
        // so plain fields + one lock is both simpler and gives coherent snapshots.)
        private readonly object _lock = new();

        private int _created;
        private int _createFailures;
        private int _acquired;
        private int _released;
        private int _timeouts;
        private int _closed;
        private int _leaks;
        private readonly Dictionary<CloseReason, int> _closedByReason = new();
        private readonly Queue<double> _acquireWaits = new();
        private readonly Queue<double> _createLatencies = new();

        // exposed for the bounded-window test
        internal int WaitsCount
        {
            get { lock (_lock) return _acquireWaits.Count; }
        }

        private static void Push(Queue<double> window, double value)
        {
            window.Enqueue(value);
            if (window.Count > Statistics.AcquireWaitWindow)
                window.Dequeue();
        }

        public override void OnConnectionCreated(PooledConnection? conn, double duration)
        {
            lock (_lock)
            {
                _created += 1;
                Push(_createLatencies, duration);
            }
        }

        public override void OnCreateFailed(Exception error)
        {
            lock (_lock) _createFailures += 1;
        }

        public override void OnConnectionClosed(PooledConnection? conn, CloseReason reason)
        {
            lock (_lock)
            {
                _closed += 1;
                _closedByReason.TryGetValue(reason, out var count);
                _closedByReason[reason] = count + 1;
            }
        }

        public override void OnAcquired(PooledConnection? conn, double waitTime)
        {
            lock (_lock)
            {
                _acquired += 1;
                Push(_acquireWaits, waitTime);
            }
        }

        public override void OnReleased(PooledConnection? conn, double holdTime)
        {
            lock (_lock) _released += 1;
        }

        public override void OnAcquireTimeout(double waited)
        {
            lock (_lock) _timeouts += 1;
        }

        public override void OnConnectionLeak(PooledConnection? conn, double heldFor)
        {
            lock (_lock) _leaks += 1;
        }

        public PoolStats Snapshot(string name, int maxSize, int size, int idle, int inUse, int waiting,
            string? breakerState)
        {
            lock (_lock)
            {
                var waits = _acquireWaits.ToList();
                waits.Sort();
                var creates = _createLatencies.ToList();
                creates.Sort();
                return new PoolStats
                {
                    Name = name,
                    MaxSize = maxSize,
                    Size = size,
                    Idle = idle,
                    InUse = inUse,
                    Waiting = waiting,
                    Created = _created,
                    CreateFailures = _createFailures,
                    Acquired = _acquired,
                    Released = _released,
                    Timeouts = _timeouts,
                    Closed = _closed,
                    Leaks = _leaks,
                    ClosedByReason = _closedByReason.ToDictionary(kv => kv.Key.Value(), kv => kv.Value),
                    AvgAcquireWait = waits.Count > 0 ? waits.Sum() / waits.Count : 0.0,
                    P50AcquireWait = Statistics.Percentile(waits, Statistics.P50),
                    P95AcquireWait = Statistics.Percentile(waits, Statistics.P95),
                    P99AcquireWait = Statistics.Percentile(waits, Statistics.P99),
                    AvgCreateLatency = creates.Count > 0 ? creates.Sum() / creates.Count : 0.0,
                    P50CreateLatency = Statistics.Percentile(creates, Statistics.P50),
                    P95CreateLatency = Statistics.Percentile(creates, Statistics.P95),
                    P99CreateLatency = Statistics.Percentile(creates, Statistics.P99),
                    BreakerState = breakerState,
                };
            }
        }
    }
}
