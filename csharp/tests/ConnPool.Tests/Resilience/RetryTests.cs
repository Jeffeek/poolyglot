using System;
using ConnPool;
using ConnPool.Tests.Support;
using Xunit;

namespace ConnPool.Tests.Resilience
{
    public class RetryPolicyTests
    {
        [Fact]
        public void InvalidConfig()
        {
            Assert.Throws<PoolConfigError>(() => _ = new RetryPolicy { MaxAttempts = 0 });
            Assert.Throws<PoolConfigError>(() => _ = new RetryPolicy { BaseDelay = -1 });
            Assert.Throws<PoolConfigError>(() => _ = new RetryPolicy { Multiplier = 0.5 });
            Assert.Throws<PoolConfigError>(() => _ = new RetryPolicy { Jitter = 2.0 });
            Assert.Throws<PoolConfigError>(() => _ = new RetryPolicy { Jitter = -0.5 });
        }

        [Fact]
        public void Backoff()
        {
            var p = new RetryPolicy { MaxAttempts = 5, BaseDelay = 1, MaxDelay = 5, Multiplier = 2, Jitter = 0 };
            var rng = new Random(0);
            Assert.Equal(
                new[] { 1.0, 2.0, 4.0, 5.0 },
                new[] { p.DelayFor(1, rng), p.DelayFor(2, rng), p.DelayFor(3, rng), p.DelayFor(4, rng) });

            var full = new RetryPolicy { BaseDelay = 4, Jitter = 1.0 };
            for (int i = 0; i < 100; i++)
            {
                double d = full.DelayFor(1, rng);
                Assert.InRange(d, 0.0, 4.0);
            }
        }

        [Fact]
        public void ZeroBaseDelayNeverNaN()
        {
            var p = new RetryPolicy { BaseDelay = 0, Multiplier = 2, Jitter = 0.5 };
            var rng = new Random(0);
            foreach (int n in new[] { 1, 2, 100, 1000 })
            {
                double d = p.DelayFor(n, rng);
                Assert.False(double.IsNaN(d));
                Assert.Equal(0.0, d);
            }
        }

        [Fact]
        public void RetryOnFilter()
        {
            var p = new RetryPolicy { RetryOn = e => e is ConnErr };
            Assert.True(p.ShouldRetry(new ConnReset()));
            Assert.False(p.ShouldRetry(new ArgumentException()));
        }

        [Fact]
        public void PropertiesAreReadable()
        {
            var p = new RetryPolicy { MaxAttempts = 4, BaseDelay = 0.2, MaxDelay = 4, Multiplier = 3, Jitter = 0.25 };
            Assert.Equal(4, p.MaxAttempts);
            Assert.Equal(0.2, p.BaseDelay);
            Assert.Equal(4, p.MaxDelay);
            Assert.Equal(3, p.Multiplier);
            Assert.Equal(0.25, p.Jitter);
            Assert.True(p.RetryOn(new Exception()));
            Assert.Throws<PoolConfigError>(() => _ = new RetryPolicy { MaxDelay = -1 });
        }
    }
}
