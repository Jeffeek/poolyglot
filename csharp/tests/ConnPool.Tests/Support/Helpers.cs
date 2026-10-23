// Shared test doubles (the mirror of python/tests/support/helpers.py).
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ConnPool;

namespace ConnPool.Tests.Support
{
    // Exception stand-ins mirroring the Python builtins the suite relies on:
    //   ConnErr      ~ ConnectionError (base, retryable in the filter tests)
    //   ConnRefused  ~ ConnectionRefusedError (a create failure)
    //   ConnReset    ~ ConnectionResetError (used as a fatal error)
    //   AuthError    ~ PermissionError (a non-retryable error)
    public class ConnErr : Exception
    {
        public ConnErr(string? message = null) : base(message) { }
    }

    public sealed class ConnRefused : ConnErr
    {
        public ConnRefused(string? message = null) : base(message) { }
    }

    public sealed class ConnReset : ConnErr
    {
        public ConnReset(string? message = null) : base(message) { }
    }

    public sealed class AuthError : Exception
    {
        public AuthError(string? message = null) : base(message) { }
    }

    public sealed class FakeConn
    {
        private static int _ids;

        public int Id { get; }
        public bool Closed;
        public bool Healthy = true;
        public int Resets;

        public FakeConn() => Id = Interlocked.Increment(ref _ids);

        public override string ToString() => $"<FakeConn {Id}>";
    }

    public class FakeFactory : ConnectionFactory<FakeConn>
    {
        public readonly List<FakeConn> Created = new List<FakeConn>();
        public int Attempts;
        public int FailTimes;
        public bool FailAlways;
        public Type[] FatalTypes = Array.Empty<Type>();
        public bool ResetRaises;
        private readonly object _lock = new object();

        public override FakeConn Create()
        {
            lock (_lock)
            {
                Attempts += 1;
                if (FailAlways || FailTimes > 0)
                {
                    FailTimes = Math.Max(0, FailTimes - 1);
                    throw new ConnRefused("backend down");
                }
                var conn = new FakeConn();
                Created.Add(conn);
                return conn;
            }
        }

        public override bool Validate(FakeConn conn) => conn.Healthy && !conn.Closed;

        public override void Reset(FakeConn conn)
        {
            if (ResetRaises)
                throw new InvalidOperationException("reset failed");
            conn.Resets += 1;
        }

        public override void Close(FakeConn conn) => conn.Closed = true;

        public override bool IsFatalError(Exception exc) => FatalTypes.Any(t => t.IsInstanceOfType(exc));
    }

    /// <summary>Async view over a FakeFactory (shares its counters and failure switches).</summary>
    public sealed class AsyncFakeFactory : AsyncConnectionFactory<FakeConn>
    {
        public readonly FakeFactory Inner;
        public readonly double CreateDelay;

        public AsyncFakeFactory(FakeFactory? inner = null, double createDelay = 0.0)
        {
            Inner = inner ?? new FakeFactory();
            CreateDelay = createDelay;
        }

        public override async Task<FakeConn> CreateAsync(CancellationToken cancellationToken)
        {
            if (CreateDelay > 0)
                await Task.Delay(TimeSpan.FromSeconds(CreateDelay), cancellationToken).ConfigureAwait(false);
            return Inner.Create();
        }

        public override Task<bool> ValidateAsync(FakeConn conn, CancellationToken cancellationToken) =>
            Task.FromResult(Inner.Validate(conn));

        public override Task ResetAsync(FakeConn conn, CancellationToken cancellationToken)
        {
            Inner.Reset(conn);
            return Task.CompletedTask;
        }

        public override Task CloseAsync(FakeConn conn, CancellationToken cancellationToken)
        {
            Inner.Close(conn);
            return Task.CompletedTask;
        }

        public override bool IsFatalError(Exception exc) => Inner.IsFatalError(exc);
    }

    public sealed class Recorder : PoolListener
    {
        public readonly List<CloseReason> Closed = new List<CloseReason>();
        public readonly List<string> Events = new List<string>();
        private readonly object _lock = new object();

        public override void OnConnectionCreated(PooledConnection? conn, double duration)
        {
            lock (_lock) Events.Add("created");
        }

        public override void OnConnectionClosed(PooledConnection? conn, CloseReason reason)
        {
            lock (_lock)
            {
                Closed.Add(reason);
                Events.Add($"closed:{reason.Value()}");
            }
        }

        public override void OnAcquired(PooledConnection? conn, double waitTime)
        {
            lock (_lock) Events.Add("acquired");
        }

        public override void OnReleased(PooledConnection? conn, double holdTime)
        {
            lock (_lock) Events.Add("released");
        }

        public override void OnBreakerStateChange(BreakerState old, BreakerState @new)
        {
            lock (_lock) Events.Add($"breaker:{old.Value()}->{@new.Value()}");
        }

        public int CountEvent(string e)
        {
            lock (_lock) return Events.Count(x => x == e);
        }

        public List<CloseReason> ClosedSnapshot()
        {
            lock (_lock) return Closed.ToList();
        }
    }

    public static class Helpers
    {
        /// <summary>Deterministic baseline: no jitter, no background work, no retries, no breaker.</summary>
        public static PoolConfig QuietConfig() => new PoolConfig
        {
            MaxLifetime = null,
            MaxIdleTime = null,
            ValidateAfterIdle = null,
            MaintenanceInterval = null,
            AcquireTimeout = 0.3,
            LifetimeJitter = 0.0,
            Retry = RetryPolicy.NoRetry,
            Breaker = null,
        };

        /// <summary>Spin until <paramref name="condition"/> holds or <paramref name="timeoutSeconds"/> elapses.</summary>
        public static void SpinUntil(Func<bool> condition, double timeoutSeconds = 3.0)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (!condition() && sw.Elapsed.TotalSeconds < timeoutSeconds)
                Thread.Sleep(5);
        }

        public static async Task SpinUntilAsync(Func<bool> condition, double timeoutSeconds = 3.0)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (!condition() && sw.Elapsed.TotalSeconds < timeoutSeconds)
                await Task.Delay(5).ConfigureAwait(false);
        }
    }
}
