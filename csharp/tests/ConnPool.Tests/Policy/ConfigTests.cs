using System;
using ConnPool;
using Xunit;

namespace ConnPool.Tests.Policy
{
    public class ConfigTests
    {
        [Fact]
        public void DefaultsAreValid()
        {
            _ = new PoolConfig();
        }

        [Fact]
        public void RejectsBadValues()
        {
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { MaxSize = 0 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { MinSize = 11 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { MinSize = -1 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { AcquireTimeout = -1 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { MaxLifetime = 0 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { LifetimeJitter = 1.5 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { LifetimeJitter = -0.5 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { MaxUses = 0 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { ValidateAfterIdle = -1 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { MaintenanceInterval = 0 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { LeakDetectionTimeout = 0 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { LeakDetectionTimeout = -5 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { CreateTimeout = 0 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { MaxWaiters = -1 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { MaxIdleTime = 0 });
            Assert.Throws<PoolConfigError>(() => _ = new PoolConfig { MinSize = 5, MaxSize = 3 });
        }

        [Fact]
        public void AcceptsBoundaryValues()
        {
            // The smallest/edge values on the valid side of each range must be accepted - this
            // pins the boundary so a future off-by-one in validation is caught alongside the
            // "rejects bad values" cases above.
            _ = new PoolConfig { MaxSize = 1 };                       // smallest cap
            _ = new PoolConfig { MinSize = 0 };                       // smallest min
            _ = new PoolConfig { MaxSize = 3, MinSize = 3 };          // min == max
            _ = new PoolConfig { AcquireTimeout = 0 };                // 0 = don't wait
            _ = new PoolConfig { AcquireTimeout = null };             // null = forever
            _ = new PoolConfig { LifetimeJitter = 0 };                // [0, 1] inclusive
            _ = new PoolConfig { LifetimeJitter = 1 };
            _ = new PoolConfig { MaxUses = 1 };                       // >= 1
            _ = new PoolConfig { MaxUses = null };
            _ = new PoolConfig { MaxWaiters = 0 };                    // >= 0
            _ = new PoolConfig { MaxWaiters = null };                 // null = unlimited
            _ = new PoolConfig { LeakDetectionTimeout = null };       // null = off
            _ = new PoolConfig { ValidateAfterIdle = 0 };             // >= 0 (0 = always)
            foreach (var cfg in new[]
            {
                new PoolConfig { CreateTimeout = 0.001 },             // > 0
                new PoolConfig { MaxLifetime = 0.001 },
                new PoolConfig { MaxIdleTime = 0.001 },
                new PoolConfig { MaintenanceInterval = 0.001 },
                new PoolConfig { LeakDetectionTimeout = 0.001 },
                new PoolConfig { CreateTimeout = null },              // null = off
            })
            {
                Assert.NotNull(cfg);
            }
        }

        [Fact]
        public void IdleStrategyWireValues()
        {
            Assert.Equal("lifo", IdleStrategy.Lifo.Value());
            Assert.Equal("fifo", IdleStrategy.Fifo.Value());
        }

        [Fact]
        public void WithReturnsValidatedCopy()
        {
            var cfg = new PoolConfig { MaxSize = 5 };
            Assert.Equal(8, (cfg with { MaxSize = 8 }).MaxSize);
            Assert.Equal(5, cfg.MaxSize); // original unchanged
            Assert.Throws<PoolConfigError>(() => _ = cfg with { MinSize = 99 });
        }
    }
}
