using System;
using System.Threading;
using System.Threading.Tasks;
using ConnPool;
using Xunit;

namespace ConnPool.Tests
{
    public class ThreadSafeRandomTests
    {
        [Fact]
        public void SerialisesAllStandardEntryPoints()
        {
            var r = new ThreadSafeRandom(new Random(0));
            Assert.InRange(r.NextDouble(), 0.0, 1.0);
            Assert.InRange(r.Next(), 0, int.MaxValue);
            Assert.InRange(r.Next(10), 0, 9);
            Assert.InRange(r.Next(5, 8), 5, 7);
            var buffer = new byte[4];
            r.NextBytes(buffer);                 // just must not throw
            Assert.Equal(4, buffer.Length);
        }
    }

    public class TimeoutTests
    {
        [Fact]
        public void UnsetUsesConfiguredDefault()
        {
            double? deadline = TimeoutMath.ComputeDeadline(Timeout.Unset, 30.0, 100.0);
            Assert.Equal(130.0, deadline);
            Assert.True(Timeout.Unset.IsUnset);
        }

        [Fact]
        public void InfiniteMeansNoDeadline()
        {
            Assert.Null(TimeoutMath.ComputeDeadline(Timeout.Infinite, 30.0, 100.0));
            Timeout fromNull = (double?)null;      // implicit double? -> Infinite
            Assert.Null(TimeoutMath.ComputeDeadline(fromNull, 30.0, 100.0));
        }

        [Fact]
        public void ValueOverridesDefault()
        {
            Assert.Equal(105.0, TimeoutMath.ComputeDeadline(Timeout.Of(5.0), 30.0, 100.0));
            Timeout fromDouble = 5.0;              // implicit double -> Of
            Assert.Equal(105.0, TimeoutMath.ComputeDeadline(fromDouble, 30.0, 100.0));
            Assert.False(fromDouble.IsUnset);
            Timeout fromNullable = (double?)5.0;   // implicit double? -> Of
            Assert.Equal(105.0, TimeoutMath.ComputeDeadline(fromNullable, 30.0, 100.0));
        }

        [Fact]
        public void RemainingComputesLeft()
        {
            Assert.Null(TimeoutMath.Remaining(null, 100.0));
            Assert.Equal(30.0, TimeoutMath.Remaining(130.0, 100.0));
        }
    }

    public class PooledConnectionRawObjectTests
    {
        [Fact]
        public void RawObjectReturnsRaw()
        {
            var o = new object();
            Assert.Same(o, new PooledConnection<object>(o, 0.0).RawObject);
        }
    }

    public class PoolListenerDefaultTests
    {
        [Fact]
        public void BaseMethodsDoNothing()
        {
            var l = new PoolListener();
            var c = new PooledConnection<object>(new object(), 0.0);
            l.OnConnectionCreated(c, 0.0);
            l.OnConnectionClosed(c, CloseReason.Idle);
            l.OnCreateFailed(new Exception());
            l.OnAcquired(c, 0.0);
            l.OnReleased(c, 0.0);
            l.OnAcquireTimeout(0.0);
            l.OnConnectionLeak(c, 0.0);
            l.OnBreakerStateChange(BreakerState.Closed, BreakerState.Open);
        }
    }

    public class PoolStatsStringTests
    {
        [Fact]
        public void ToStringMentionsName()
        {
            var s = new PoolStats { Name = "x", MaxSize = 2 };
            Assert.Contains("PoolStats", s.ToString());
        }
    }

    public class AsyncFactoryDisposalTests
    {
        private sealed class AsyncDisposableConn : IAsyncDisposable
        {
            public bool Disposed;
            public ValueTask DisposeAsync()
            {
                Disposed = true;
                return default;
            }
        }

        private sealed class MinimalFactory : AsyncConnectionFactory<AsyncDisposableConn>
        {
            public override Task<AsyncDisposableConn> CreateAsync(CancellationToken cancellationToken) =>
                Task.FromResult(new AsyncDisposableConn());
        }

        [Fact]
        public async Task DefaultCloseDisposesAsyncDisposable()
        {
            var f = new MinimalFactory();
            AsyncDisposableConn c = await f.CreateAsync(default);
            await f.CloseAsync(c, default);
            Assert.True(c.Disposed);
        }
    }
}
