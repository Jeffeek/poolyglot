using System.Collections.Generic;
using System.Linq;
using ConnPool;
using Xunit;

namespace ConnPool.Tests.Resilience
{
    public class BreakerTests
    {
        private ManualClock _clock = null!;
        private List<(BreakerState Old, BreakerState New)> _changes = null!;

        private CircuitBreaker Make(
            int failureThreshold = 3, double recoveryTimeout = 10, int halfOpenMaxCalls = 1, int successThreshold = 1)
        {
            _clock = new ManualClock();
            _changes = new List<(BreakerState, BreakerState)>();
            var cfg = new BreakerConfig
            {
                FailureThreshold = failureThreshold,
                RecoveryTimeout = recoveryTimeout,
                HalfOpenMaxCalls = halfOpenMaxCalls,
                SuccessThreshold = successThreshold,
            };
            return new CircuitBreaker(cfg, _clock, (o, n) => _changes.Add((o, n)));
        }

        private static void Fail(CircuitBreaker b, int n = 1)
        {
            for (int i = 0; i < n; i++)
            {
                b.BeforeCall();
                b.RecordFailure();
            }
        }

        [Fact]
        public void OpensAfterConsecutiveFailures()
        {
            CircuitBreaker b = Make();
            Fail(b, 3);
            Assert.Equal(BreakerState.Open, b.State);
            CircuitOpenError ex = Assert.Throws<CircuitOpenError>(() => b.BeforeCall());
            Assert.Equal(10, ex.RetryAfter, 2);
        }

        [Fact]
        public void SuccessResetsFailureStreak()
        {
            CircuitBreaker b = Make();
            Fail(b, 2);
            b.BeforeCall();
            b.RecordSuccess();
            Fail(b, 2);
            Assert.Equal(BreakerState.Closed, b.State);
        }

        [Fact]
        public void HalfOpenThenClosesOnSuccess()
        {
            CircuitBreaker b = Make();
            Fail(b, 3);
            _clock.Advance(10);
            Assert.Equal(BreakerState.HalfOpen, b.State);
            b.BeforeCall();
            b.RecordSuccess();
            Assert.Equal(BreakerState.Closed, b.State);
            Assert.Equal(new[] { "open", "half_open", "closed" }, _changes.Select(c => c.New.Value()));
        }

        [Fact]
        public void HalfOpenFailureReopens()
        {
            CircuitBreaker b = Make();
            Fail(b, 3);
            _clock.Advance(10);
            b.BeforeCall();
            b.RecordFailure();
            Assert.Equal(BreakerState.Open, b.State);
        }

        [Fact]
        public void HalfOpenLimitsConcurrentProbes()
        {
            CircuitBreaker b = Make(halfOpenMaxCalls: 1);
            Fail(b, 3);
            _clock.Advance(10);
            b.BeforeCall();                       // the one probe
            Assert.Throws<CircuitOpenError>(() => b.BeforeCall()); // second is rejected
        }

        [Fact]
        public void SuccessThreshold()
        {
            CircuitBreaker b = Make(successThreshold: 2, halfOpenMaxCalls: 2);
            Fail(b, 3);
            _clock.Advance(10);
            b.BeforeCall();
            b.RecordSuccess();
            Assert.Equal(BreakerState.HalfOpen, b.State);
            b.BeforeCall();
            b.RecordSuccess();
            Assert.Equal(BreakerState.Closed, b.State);
        }

        [Fact]
        public void CallWrapperAndReset()
        {
            CircuitBreaker b = Make(failureThreshold: 1);
            Assert.Throws<System.DivideByZeroException>(() => b.Call(() =>
            {
                int zero = 0;
                return 1 / zero;
            }));
            Assert.Equal(BreakerState.Open, b.State);
            b.Reset();
            Assert.Equal(42, b.Call(() => 42));
        }

        [Fact]
        public void CallbackMayReenterBreaker()
        {
            var clock = new ManualClock();
            CircuitBreaker b = null!;
            b = new CircuitBreaker(new BreakerConfig { FailureThreshold = 1 }, clock, (o, n) => { _ = b.State; });
            b.BeforeCall();
            b.RecordFailure(); // would deadlock if callback ran under the lock
        }

        [Fact]
        public void RecordAbortFreesProbeWithoutCounting()
        {
            CircuitBreaker b = Make(halfOpenMaxCalls: 1);
            Fail(b, 3);
            _clock.Advance(10);
            b.BeforeCall();                       // take the one probe slot
            b.RecordAbort();                      // neither success nor failure: slot released
            Assert.Equal(BreakerState.HalfOpen, b.State);
            b.BeforeCall();                       // a fresh probe is allowed again
            b.RecordSuccess();
            Assert.Equal(BreakerState.Closed, b.State);
        }

        [Fact]
        public void AbortInClosedStateIsNoop()
        {
            CircuitBreaker b = Make();
            b.RecordAbort();
            Assert.Equal(BreakerState.Closed, b.State);
        }

        [Fact]
        public void ResetWhenAlreadyClosedIsNoop()
        {
            CircuitBreaker b = Make();
            b.Reset();
            Assert.Equal(BreakerState.Closed, b.State);
            Assert.Empty(_changes);
        }

        [Fact]
        public void DefaultsWhenConfigAndClockOmitted()
        {
            var b = new CircuitBreaker();   // null config -> BreakerConfig(); null clock -> SystemClock
            Assert.Equal(BreakerState.Closed, b.State);
            for (int i = 0; i < 5; i++)     // default FailureThreshold = 5
                b.RecordFailure();
            Assert.Equal(BreakerState.Open, b.State);
        }

        [Fact]
        public void InvalidConfig()
        {
            Assert.Throws<PoolConfigError>(() => _ = new BreakerConfig { FailureThreshold = 0 });
            Assert.Throws<PoolConfigError>(() => _ = new BreakerConfig { RecoveryTimeout = -1 });
            Assert.Throws<PoolConfigError>(() => _ = new BreakerConfig { HalfOpenMaxCalls = 0 });
            Assert.Throws<PoolConfigError>(() => _ = new BreakerConfig { SuccessThreshold = 0 });
        }

        [Fact]
        public void ConfigPropertiesAreReadable()
        {
            var c = new BreakerConfig { FailureThreshold = 4, RecoveryTimeout = 12, HalfOpenMaxCalls = 2, SuccessThreshold = 3 };
            Assert.Equal(4, c.FailureThreshold);
            Assert.Equal(12, c.RecoveryTimeout);
            Assert.Equal(2, c.HalfOpenMaxCalls);
            Assert.Equal(3, c.SuccessThreshold);
        }
    }
}
