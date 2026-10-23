using System;
using System.Collections.Generic;
using System.Linq;
using ConnPool;
using Xunit;

namespace ConnPool.Tests.Policy
{
    public class LifecycleTests
    {
        private static PooledConnection<object> Conn(
            double created = 0.0, double? returned = null, int uses = 0, double? expires = null)
        {
            var c = new PooledConnection<object>(new object(), created, expires, returned);
            c.UseCount = uses;
            return c;
        }

        [Fact]
        public void LifetimeJitterStaysInRange()
        {
            var cfg = new PoolConfig { MaxLifetime = 100, LifetimeJitter = 0.1 };
            var rng = new Random(7);
            for (int i = 0; i < 200; i++)
            {
                PooledConnection<object> c = Lifecycle.NewConnection(new object(), cfg, 0.0, rng);
                Assert.NotNull(c.ExpiresAt);
                Assert.InRange(c.ExpiresAt!.Value, 90.0, 100.0);
            }
        }

        [Fact]
        public void RotationReasons()
        {
            var cfg = new PoolConfig { MaxUses = 3 };
            Assert.Equal(CloseReason.Expired, Lifecycle.RotationReason(Conn(expires: 5), cfg, 5));
            Assert.Equal(CloseReason.MaxUses, Lifecycle.RotationReason(Conn(uses: 3), cfg, 0));
            Assert.Null(Lifecycle.RotationReason(Conn(uses: 2, expires: 9), cfg, 5));
        }

        [Fact]
        public void SweepNeverGoesBelowMinSize()
        {
            var cfg = new PoolConfig { MinSize = 2, MaxIdleTime = 10 };
            var idle = Enumerable.Range(0, 4).Select(_ => Conn(returned: 0)).ToList();
            var (keep, doomed) = Lifecycle.Sweep(idle, 4, cfg, 11);
            Assert.Equal(2, keep.Count);
            Assert.Equal(2, doomed.Count);
            Assert.All(doomed, d => Assert.Equal(CloseReason.Idle, d.Item2));
        }

        [Fact]
        public void SweepAlwaysEvictsExpired()
        {
            var cfg = new PoolConfig { MinSize = 2, MaxIdleTime = null };
            var (keep, doomed) = Lifecycle.Sweep(new List<PooledConnection<object>> { Conn(expires: 1), Conn() }, 2, cfg, 5);
            Assert.Equal(new[] { CloseReason.Expired }, doomed.Select(d => d.Item2));
            Assert.Single(keep);
        }

        [Fact]
        public void ValidationPolicy()
        {
            PooledConnection<object> c = Conn(returned: 0);
            Assert.False(Lifecycle.NeedsValidation(c, new PoolConfig { ValidateAfterIdle = null }, 100));
            Assert.True(Lifecycle.NeedsValidation(c, new PoolConfig { ValidateAfterIdle = 0 }, 0));
            Assert.False(Lifecycle.NeedsValidation(c, new PoolConfig { ValidateAfterIdle = 30 }, 10));
            Assert.True(Lifecycle.NeedsValidation(c, new PoolConfig { ValidateAfterIdle = 30 }, 30));
        }

        [Fact]
        public void PopStrategies()
        {
            PooledConnection<object> a = Conn(), b = Conn();
            var d = new LinkedList<PooledConnection<object>>(new[] { a, b });
            Assert.Same(b, Lifecycle.PopIdle(d, IdleStrategy.Lifo));
            d = new LinkedList<PooledConnection<object>>(new[] { a, b });
            Assert.Same(a, Lifecycle.PopIdle(d, IdleStrategy.Fifo));
            Assert.Null(Lifecycle.PopIdle(new LinkedList<PooledConnection<object>>(), IdleStrategy.Lifo));
        }
    }
}
