using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ConnPool;
using ConnPool.Tests.Support;
using Xunit;

namespace ConnPool.Tests.Pools
{
    public class SyncPoolTests : IDisposable
    {
        private readonly List<ConnectionPool<FakeConn>> _pools = new List<ConnectionPool<FakeConn>>();
        private ConnectionFactory<FakeConn> _factory = null!;
        private Recorder _rec = null!;
        private ManualClock? _clock;

        private ConnectionPool<FakeConn> Make(
            ConnectionFactory<FakeConn>? factory = null, ManualClock? clock = null, PoolConfig? cfg = null)
        {
            _factory = factory ?? new FakeFactory();
            _rec = new Recorder();
            _clock = clock;
            var pool = new ConnectionPool<FakeConn>(
                _factory, cfg ?? Helpers.QuietConfig(), new PoolListener[] { _rec }, clock);
            _pools.Add(pool);
            return pool;
        }

        private FakeFactory Fake => (FakeFactory)_factory;

        public void Dispose()
        {
            foreach (ConnectionPool<FakeConn> pool in _pools)
            {
                try { pool.Close(); }
                catch { /* best effort cleanup */ }
            }
        }

        // ----------------------------------------------------------------- basics
        [Fact]
        public void ConnectionIsReused()
        {
            ConnectionPool<FakeConn> pool = Make();
            FakeConn a = pool.Use(c => c);
            FakeConn b = pool.Use(c => c);
            Assert.Same(a, b);
            Assert.Single(Fake.Created);
            Assert.Equal(2, a.Resets);
        }

        [Fact]
        public void MaxSizeIsHardCapAndAcquireTimesOut()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1 });
            FakeConn held = pool.Acquire();
            Assert.Throws<PoolTimeoutError>(() => pool.Acquire(0.05));
            Assert.Equal(1, pool.Stats().Timeouts);
            pool.Release(held);
        }

        [Fact]
        public void ZeroTimeoutIsNonBlocking()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1 });
            FakeConn held = pool.Acquire();
            Assert.Throws<PoolTimeoutError>(() => pool.Acquire(0));
            pool.Release(held);
        }

        [Fact]
        public void WaiterGetsConnectionWhenReleased()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1 });
            FakeConn held = pool.Acquire();
            FakeConn? got = null;
            var t = Task.Run(() => { got = pool.Acquire(2); });
            Helpers.SpinUntil(() => pool.Stats().Waiting == 1);
            Assert.Equal(1, pool.Stats().Waiting);
            pool.Release(held);
            t.Wait(2000);
            Assert.Same(held, got);
        }

        [Fact]
        public void DoubleAndForeignReleaseRejected()
        {
            ConnectionPool<FakeConn> pool = Make();
            FakeConn conn = pool.Acquire();
            pool.Release(conn);
            Assert.Throws<InvalidReleaseError>(() => pool.Release(conn));
            Assert.Throws<InvalidReleaseError>(() => pool.Release(new FakeConn()));
        }

        [Fact]
        public void IdleStrategyLifoVsFifo()
        {
            foreach ((IdleStrategy strategy, int expectedIndex) in new[] { (IdleStrategy.Lifo, 2), (IdleStrategy.Fifo, 0) })
            {
                ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { IdleStrategy = strategy });
                var conns = new[] { pool.Acquire(), pool.Acquire(), pool.Acquire() };
                foreach (FakeConn c in conns)
                    pool.Release(c);
                Assert.Same(conns[expectedIndex], pool.Acquire());
            }
        }

        private sealed class Thing : IDisposable
        {
            public bool Closed;
            public void Dispose() => Closed = true;
        }

        [Fact]
        public void FromCallablesAndDefaultClose()
        {
            var pool = ConnectionPool<Thing>.FromCallables(() => new Thing(), config: Helpers.QuietConfig());
            Thing t = pool.Use(c => c);
            pool.Close();
            Assert.True(t.Closed);
        }

        [Fact]
        public void ExplicitDiscard()
        {
            ConnectionPool<FakeConn> pool = Make();
            FakeConn conn = pool.Acquire();
            pool.Release(conn, discard: true);
            Assert.True(conn.Closed);
            Assert.Equal(0, pool.Stats().Size);
            Assert.Equal(new[] { CloseReason.Discarded }, _rec.ClosedSnapshot());
        }

        [Fact]
        public void ValidateIdleEvictsDeadConnections()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { ValidateIdle = true, MaxSize = 2 });
            FakeConn a = pool.Acquire();
            FakeConn b = pool.Acquire();
            pool.Release(a);
            pool.Release(b);
            Assert.Equal(2, pool.Stats().Idle);
            a.Healthy = false;                 // a goes silently dead while idle
            pool.RunMaintenance();
            Assert.Equal(1, pool.Stats().Idle);
            Assert.Contains(CloseReason.Unhealthy, _rec.ClosedSnapshot());
            FakeConn c = pool.Use(x => x);
            Assert.Same(b, c);                 // the healthy survivor is reused
        }

        [Fact]
        public void DefaultConfigWhenOmitted()
        {
            var pool = new ConnectionPool<FakeConn>(new FakeFactory()); // null config -> new PoolConfig()
            _pools.Add(pool);
            FakeConn c = pool.Acquire();
            pool.Release(c);
            Assert.Equal(1, pool.Stats().Size);
        }

        [Fact]
        public void HealthCheckClosedMidValidationReportsPoolClosed()
        {
            // A connection that is still healthy but whose pool closes *during* the idle
            // health-check must be closed as PoolClosed, not Unhealthy.
            ConnectionPool<FakeConn> pool = null!;
            var closedOnce = false;
            var factory = new CallableFactory<FakeConn>(
                () => new FakeConn(),
                validate: _ =>
                {
                    if (!closedOnce) { closedOnce = true; pool.Close(); }
                    return true;  // the connection itself is fine
                });
            pool = Make(factory, cfg: Helpers.QuietConfig() with { ValidateIdle = true });
            FakeConn a = pool.Acquire();
            pool.Release(a);
            pool.RunMaintenance();
            Assert.Contains(CloseReason.PoolClosed, _rec.ClosedSnapshot());
        }

        [Fact]
        public void RetrySleepsWhenAcquireTimeoutIsInfinite()
        {
            // With no acquire deadline the retry loop must not consult 'left'; it just sleeps.
            var f = new FakeFactory { FailTimes = 1 };
            ConnectionPool<FakeConn> pool = Make(f, new ManualClock(), Helpers.QuietConfig() with
            {
                AcquireTimeout = null,
                Retry = new RetryPolicy { MaxAttempts = 3, BaseDelay = 1, Jitter = 0 },
            });
            FakeConn c = pool.Acquire();  // 1st create fails -> sleep -> 2nd succeeds
            Assert.Equal(2, f.Attempts);
            Assert.NotNull(c);
        }

        [Fact]
        public void DuplicateConnectionFromFactoryIsRejected()
        {
            var shared = new FakeConn();
            ConnectionPool<FakeConn> pool = Make(new CallableFactory<FakeConn>(() => shared));
            FakeConn first = pool.Acquire();
            Assert.Throws<ConnectionCreateError>(() => pool.Acquire());
            Assert.Equal(1, pool.Stats().Size);    // the duplicate slot was freed
            Assert.Equal(1, pool.Stats().Created); // duplicate not counted as created
            pool.Release(first);
            Assert.Equal(0, pool.Stats().InUse);
        }

        // ----------------------------------------------------------------- rotation
        [Fact]
        public void ExpiredConnectionIsNotReused()
        {
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(clock: clock, cfg: Helpers.QuietConfig() with { MaxLifetime = 100 });
            FakeConn a = pool.Acquire();
            pool.Release(a);
            clock.Advance(101);
            FakeConn b = pool.Acquire();
            Assert.NotSame(a, b);
            Assert.True(a.Closed);
            Assert.Equal(new[] { CloseReason.Expired }, _rec.ClosedSnapshot());
            pool.Release(b);
        }

        [Fact]
        public void ConnectionExpiringWhileInUseIsClosedOnRelease()
        {
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(clock: clock, cfg: Helpers.QuietConfig() with { MaxLifetime = 100 });
            FakeConn a = pool.Acquire();
            clock.Advance(150);
            pool.Release(a);
            Assert.True(a.Closed);
            Assert.Equal(0, pool.Stats().Idle);
        }

        [Fact]
        public void MaxUsesRotation()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxUses = 2 });
            FakeConn first = pool.Acquire();
            pool.Release(first);
            FakeConn again = pool.Acquire();
            pool.Release(again);            // 2nd use -> retired
            Assert.Same(first, again);
            Assert.True(first.Closed);
            FakeConn third = pool.Acquire();
            Assert.NotSame(third, first);
            Assert.Equal(new[] { CloseReason.MaxUses }, _rec.ClosedSnapshot());
            pool.Release(third);
        }

        [Fact]
        public void IdleExpiredConnectionDroppedOnBorrow()
        {
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(clock: clock, cfg: Helpers.QuietConfig() with { MaxIdleTime = 10 });
            FakeConn a = pool.Acquire();
            pool.Release(a);
            clock.Advance(11);
            FakeConn b = pool.Acquire();
            Assert.NotSame(a, b);
            Assert.Equal(new[] { CloseReason.Idle }, _rec.ClosedSnapshot());
            pool.Release(b);
        }

        [Fact]
        public void IdleEvictionRespectsMinSize()
        {
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(clock: clock, cfg: Helpers.QuietConfig() with { MinSize = 1, MaxIdleTime = 10 });
            var conns = new[] { pool.Acquire(), pool.Acquire(), pool.Acquire() };
            foreach (FakeConn c in conns)
                pool.Release(c);
            clock.Advance(11);
            pool.RunMaintenance();
            Assert.Equal(1, pool.Stats().Idle);
            Assert.Equal(2, _rec.ClosedSnapshot().Count(r => r == CloseReason.Idle));
        }

        [Fact]
        public void MaintenanceReplacesExpiredToKeepMinSize()
        {
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(clock: clock, cfg: Helpers.QuietConfig() with { MinSize = 2, MaxLifetime = 50 });
            pool.Open();
            Assert.Equal(2, pool.Stats().Idle);
            clock.Advance(51);
            pool.RunMaintenance();
            Assert.Equal(2, pool.Stats().Idle);
            Assert.Equal(4, Fake.Created.Count);
            Assert.Equal(2, Fake.Created.Count(c => c.Closed));
        }

        [Fact]
        public void BackgroundMaintenanceThread()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaintenanceInterval = 0.02, MaxIdleTime = 0.05 });
            pool.Open();
            pool.Release(pool.Acquire());
            Helpers.SpinUntil(() => pool.Stats().Size == 0, 3);
            Assert.Equal(0, pool.Stats().Size);
        }

        // ----------------------------------------------------------------- health
        [Fact]
        public void UnhealthyIdleConnectionReplacedOnBorrow()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { ValidateAfterIdle = 0 });
            FakeConn a = pool.Acquire();
            pool.Release(a);
            a.Healthy = false;
            FakeConn b = pool.Acquire();
            Assert.NotSame(a, b);
            Assert.True(a.Closed);
            Assert.Equal(new[] { CloseReason.Unhealthy }, _rec.ClosedSnapshot());
            pool.Release(b);
        }

        [Fact]
        public void ValidationSkippedWhenRecentlyUsed()
        {
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(clock: clock, cfg: Helpers.QuietConfig() with { ValidateAfterIdle = 30 });
            FakeConn a = pool.Acquire();
            pool.Release(a);
            a.Healthy = false;                 // would fail validation, but we don't ask
            Assert.Same(a, pool.Acquire());
        }

        [Fact]
        public void FatalErrorDiscardsConnection()
        {
            var f = new FakeFactory { FatalTypes = new[] { typeof(ConnReset) } };
            ConnectionPool<FakeConn> pool = Make(f);
            FakeConn captured = null!;
            Assert.Throws<ConnReset>(() => pool.Use(c => { captured = c; throw new ConnReset(); }));
            Assert.True(captured.Closed);
            Assert.Equal(new[] { CloseReason.Error }, _rec.ClosedSnapshot());
        }

        [Fact]
        public void BenignErrorKeepsHealthyConnection()
        {
            ConnectionPool<FakeConn> pool = Make();
            FakeConn captured = null!;
            Assert.Throws<ArgumentException>(() => pool.Use(c => { captured = c; throw new ArgumentException("app bug"); }));
            Assert.False(captured.Closed);
            Assert.Equal(1, pool.Stats().Idle);
        }

        [Fact]
        public void ErrorPlusFailedValidationDiscards()
        {
            ConnectionPool<FakeConn> pool = Make();
            FakeConn captured = null!;
            Assert.Throws<ArgumentException>(() => pool.Use(c =>
            {
                captured = c;
                c.Healthy = false;
                throw new ArgumentException();
            }));
            Assert.True(captured.Closed);
        }

        [Fact]
        public void ResetFailureDiscards()
        {
            var f = new FakeFactory();
            ConnectionPool<FakeConn> pool = Make(f);
            f.ResetRaises = true;
            FakeConn captured = pool.Use(c => c);
            Assert.True(captured.Closed);
            Assert.Equal(new[] { CloseReason.ResetFailed }, _rec.ClosedSnapshot());
        }

        private sealed class BadCloseFactory : FakeFactory
        {
            public override void Close(FakeConn conn) => throw new System.IO.IOException("close failed");
        }

        [Fact]
        public void CloseErrorsDoNotLeakSlots()
        {
            ConnectionPool<FakeConn> pool = Make(new BadCloseFactory());
            pool.Release(pool.Acquire(), discard: true);
            Assert.Equal(0, pool.Stats().Size);
        }

        [Fact]
        public void ValidationErrorIsTreatedAsUnhealthy()
        {
            ConnectionPool<FakeConn> pool = Make(
                new CallableFactory<FakeConn>(() => new FakeConn(), validate: _ => throw new InvalidOperationException("ping blew up")),
                cfg: Helpers.QuietConfig() with { ValidateAfterIdle = 0 });
            FakeConn a = pool.Acquire();
            pool.Release(a);
            FakeConn b = pool.Acquire();
            Assert.NotSame(a, b);
            Assert.Equal(new[] { CloseReason.Unhealthy }, _rec.ClosedSnapshot());
            pool.Release(b);
        }

        // ----------------------------------------------------------------- resilience
        [Fact]
        public void RetryWithBackoffSucceeds()
        {
            var f = new FakeFactory { FailTimes = 2 };
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(f, clock, Helpers.QuietConfig() with
            {
                Retry = new RetryPolicy { MaxAttempts = 3, BaseDelay = 1, Multiplier = 2, Jitter = 0 },
                AcquireTimeout = 60,
            });
            double start = clock.Monotonic();
            pool.Acquire();
            Assert.Equal(3, f.Attempts);
            Assert.Equal(3, clock.Monotonic() - start); // slept 1s then 2s
        }

        [Fact]
        public void ExhaustedRetriesRaiseAndDoNotLeakSlot()
        {
            var f = new FakeFactory { FailAlways = true };
            ConnectionPool<FakeConn> pool = Make(f, new ManualClock(), Helpers.QuietConfig() with
            {
                Retry = new RetryPolicy { MaxAttempts = 2, Jitter = 0 },
            });
            ConnectionCreateError ex = Assert.Throws<ConnectionCreateError>(() => pool.Acquire());
            Assert.IsType<ConnRefused>(ex.InnerException);
            Assert.Equal(2, f.Attempts);
            Assert.Equal(0, pool.Stats().Size);
            Assert.Equal(2, pool.Stats().CreateFailures);
        }

        private sealed class AuthFailFactory : FakeFactory
        {
            public override FakeConn Create()
            {
                Attempts += 1;
                throw new AuthError("bad password");
            }
        }

        [Fact]
        public void NonRetryableErrorFailsImmediately()
        {
            var f = new AuthFailFactory();
            ConnectionPool<FakeConn> pool = Make(f, new ManualClock(), Helpers.QuietConfig() with
            {
                Retry = new RetryPolicy { MaxAttempts = 5, RetryOn = e => e is ConnErr },
            });
            Assert.Throws<ConnectionCreateError>(() => pool.Acquire());
            Assert.Equal(1, f.Attempts);
        }

        [Fact]
        public void RetryNeverSleepsPastAcquireDeadline()
        {
            var f = new FakeFactory { FailAlways = true };
            ConnectionPool<FakeConn> pool = Make(f, new ManualClock(), Helpers.QuietConfig() with
            {
                AcquireTimeout = 1,
                Retry = new RetryPolicy { MaxAttempts = 5, BaseDelay = 5, Jitter = 0 },
            });
            Assert.Throws<PoolTimeoutError>(() => pool.Acquire());
            Assert.Equal(1, f.Attempts);
        }

        [Fact]
        public void CircuitBreakerFullCycle()
        {
            var f = new FakeFactory { FailAlways = true };
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(f, clock, Helpers.QuietConfig() with
            {
                Breaker = new BreakerConfig { FailureThreshold = 2, RecoveryTimeout = 30 },
            });
            for (int i = 0; i < 2; i++)
                Assert.Throws<ConnectionCreateError>(() => pool.Acquire());
            int attempts = f.Attempts;
            Assert.Throws<CircuitOpenError>(() => pool.Acquire()); // fails fast, backend untouched
            Assert.Equal(attempts, f.Attempts);
            Assert.Equal("open", pool.Stats().BreakerState);

            clock.Advance(31);
            f.FailAlways = false;
            FakeConn conn = pool.Acquire();  // half-open probe succeeds
            Assert.Equal("closed", pool.Stats().BreakerState);
            Assert.Equal(1, _rec.CountEvent("breaker:open->half_open"));
            Assert.Equal(1, pool.Stats().Size);
            pool.Release(conn);
        }

        [Fact]
        public void FailedProbeReopensBreaker()
        {
            var f = new FakeFactory { FailAlways = true };
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(f, clock, Helpers.QuietConfig() with
            {
                Breaker = new BreakerConfig { FailureThreshold = 1, RecoveryTimeout = 5 },
            });
            Assert.Throws<ConnectionCreateError>(() => pool.Acquire());
            clock.Advance(6);
            Assert.Throws<ConnectionCreateError>(() => pool.Acquire()); // the probe fails
            Assert.Throws<CircuitOpenError>(() => pool.Acquire());
        }

        [Fact]
        public void BreakerCountsOneFailurePerAcquireNotPerAttempt()
        {
            var f = new FakeFactory { FailAlways = true };
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(f, clock, Helpers.QuietConfig() with
            {
                AcquireTimeout = 60,
                Retry = new RetryPolicy { MaxAttempts = 3, BaseDelay = 1, Jitter = 0 },
                Breaker = new BreakerConfig { FailureThreshold = 2, RecoveryTimeout = 30 },
            });
            Assert.Throws<ConnectionCreateError>(() => pool.Acquire()); // 3 attempts, 1 breaker failure
            Assert.Equal(3, f.Attempts);
            Assert.Equal("closed", pool.Stats().BreakerState);
            Assert.Throws<ConnectionCreateError>(() => pool.Acquire()); // 2nd failure -> trips
            Assert.Equal("open", pool.Stats().BreakerState);
        }

        [Fact]
        public void CreateTimeoutDoesNotTripBreaker()
        {
            var f = new FakeFactory { FailAlways = true };
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(f, clock, Helpers.QuietConfig() with
            {
                AcquireTimeout = 1,
                Retry = new RetryPolicy { MaxAttempts = 3, BaseDelay = 10, Jitter = 0 },
                Breaker = new BreakerConfig { FailureThreshold = 1 },
            });
            Assert.Throws<PoolTimeoutError>(() => pool.Acquire()); // ran out of time while retrying
            Assert.Equal("closed", pool.Stats().BreakerState);     // a timeout is not a backend fault
        }

        [Fact]
        public void PoolClosedMidRetryStopsCreating()
        {
            ConnectionPool<FakeConn> pool = null!;
            var factory = new CallableFactory<FakeConn>(() =>
            {
                pool.Close();
                throw new ConnErr("down");
            });
            pool = Make(factory, cfg: Helpers.QuietConfig() with { Retry = new RetryPolicy { MaxAttempts = 3, BaseDelay = 0, Jitter = 0 } });
            Assert.Throws<PoolClosedError>(() => pool.Acquire());
        }

        // ----------------------------------------------------- shutdown + observability
        [Fact]
        public void CloseSemantics()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1 });
            FakeConn held = pool.Acquire();
            var errors = new List<Exception>();
            var t = Task.Run(() =>
            {
                try { pool.Acquire(5); }
                catch (PoolClosedError e) { lock (errors) errors.Add(e); }
            });
            Helpers.SpinUntil(() => pool.Stats().Waiting == 1);
            pool.Close();
            t.Wait(2000);
            Assert.Single(errors);           // blocked caller was released
            Assert.False(held.Closed);       // in-use connection untouched...
            pool.Release(held);
            Assert.True(held.Closed);        // ...closed once returned
            Assert.Throws<PoolClosedError>(() => pool.Acquire());
            pool.Close();                    // idempotent
        }

        [Fact]
        public void CloseClosesIdleConnections()
        {
            ConnectionPool<FakeConn> pool = Make();
            FakeConn c = pool.Acquire();
            pool.Release(c);
            pool.Close();
            Assert.True(c.Closed);
        }

        [Fact]
        public void ContextManagerOpensAndCloses()
        {
            var f = new FakeFactory();
            var pool = new ConnectionPool<FakeConn>(f, Helpers.QuietConfig() with { MinSize = 2 });
            pool.Open();
            Assert.Equal(2, pool.Stats().Idle);
            pool.Close();
            Assert.True(pool.Closed);
            Assert.True(f.Created.All(c => c.Closed));
        }

        [Fact]
        public void WarmupFailureIsNotFatal()
        {
            var f = new FakeFactory { FailAlways = true };
            ConnectionPool<FakeConn> pool = Make(f, cfg: Helpers.QuietConfig() with { MinSize = 2 });
            pool.Open();
            Assert.Equal(0, pool.Stats().Size);
        }

        [Fact]
        public void StatsAndEvents()
        {
            ConnectionPool<FakeConn> pool = Make();
            pool.Use(new Action<FakeConn>(_ =>
            {
                PoolStats s = pool.Stats();
                Assert.Equal((1, 0, 1), (s.Size, s.Idle, s.InUse));
            }));
            PoolStats s2 = pool.Stats();
            Assert.Equal((1, 1, 1, 1, 0), (s2.Created, s2.Acquired, s2.Released, s2.Idle, s2.InUse));
            Assert.Equal(new[] { "created", "acquired", "released" }, _rec.Events);
        }

        private sealed class BoomListener : PoolListener
        {
            public override void OnAcquired(PooledConnection? conn, double waitTime) =>
                throw new InvalidOperationException("metrics down");
        }

        [Fact]
        public void BrokenListenerCannotBreakPool()
        {
            var pool = new ConnectionPool<FakeConn>(new FakeFactory(), Helpers.QuietConfig(), new PoolListener[] { new BoomListener() });
            try
            {
                pool.Use(new Action<FakeConn>(_ => { }));
            }
            finally
            {
                pool.Close();
            }
        }

        [Fact]
        public void ConnectionCreatedDuringCloseDoesNotLeak()
        {
            ConnectionPool<FakeConn> pool = null!;
            var made = new List<FakeConn>();
            var factory = new CallableFactory<FakeConn>(
                () => { var c = new FakeConn(); made.Add(c); pool.Close(); return c; },
                close: c => c.Closed = true);
            pool = Make(factory);
            Assert.Throws<PoolClosedError>(() => pool.Acquire());
            Assert.Single(made);
            Assert.True(made[0].Closed);
            Assert.Equal(0, pool.Stats().Size);
        }

        [Fact]
        public void GracefulDrainWaitsForInUse()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 2 });
            FakeConn held = pool.Acquire();
            var released = new ManualResetEventSlim(false);
            var t = Task.Run(() =>
            {
                Thread.Sleep(50);
                pool.Release(held);
                released.Set();
            });
            pool.Close(2);               // should block until the returner releases
            t.Wait(2000);
            Assert.True(released.IsSet);
            Assert.True(held.Closed);
            Assert.Equal(0, pool.Stats().InUse);
        }

        [Fact]
        public void GracefulDrainGivesUpAfterTimeout()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1 });
            FakeConn held = pool.Acquire();
            pool.Close(0.05);            // caller never returns it
            Assert.Equal(1, pool.Stats().InUse);
            Assert.False(held.Closed);
            pool.Release(held);
            Assert.True(held.Closed);
        }

        // ----------------------------------------------------------------- concurrency
        [Fact]
        public void StressRespectsMaxSizeAndLeaksNothing()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 5, AcquireTimeout = 10, MaxUses = 7 });
            var lockObj = new object();
            int now = 0, peak = 0;
            var errors = new ConcurrentBag<Exception>();

            void Worker(int seed)
            {
                var rng = new Random(seed);
                try
                {
                    for (int i = 0; i < 60; i++)
                    {
                        pool.Use(new Action<FakeConn>(conn =>
                        {
                            lock (lockObj) { now += 1; peak = Math.Max(peak, now); }
                            Assert.False(conn.Closed);
                            Thread.Sleep((int)(rng.NextDouble() * 2));
                            lock (lockObj) { now -= 1; }
                        }));
                    }
                }
                catch (Exception exc)
                {
                    errors.Add(exc);
                }
            }

            var threads = Enumerable.Range(0, 20).Select(i => new Thread(() => Worker(i))).ToList();
            foreach (Thread t in threads) t.Start();
            foreach (Thread t in threads) t.Join(60000);

            Assert.Empty(errors);
            Assert.True(peak <= 5);
            PoolStats s = pool.Stats();
            Assert.Equal((0, 0), (s.InUse, s.Waiting));
            Assert.Equal(s.Idle, s.Size);
            Assert.Equal(20 * 60, s.Acquired);
            Assert.Equal(s.Idle, s.Created - s.Closed); // every conn accounted for
            Assert.True(pool.IdleSnapshot().All(c => !c.Closed));
        }

        // ----------------------------------------------------------------- leak detection
        [Fact]
        public void LeakDetectionWarnsOnce()
        {
            var clock = new ManualClock();
            ConnectionPool<FakeConn> pool = Make(clock: clock, cfg: Helpers.QuietConfig() with { LeakDetectionTimeout = 10 });
            FakeConn conn = pool.Acquire();
            pool.RunMaintenance();           // not held long enough yet
            Assert.Equal(0, pool.Stats().Leaks);
            clock.Advance(11);
            pool.RunMaintenance();
            pool.RunMaintenance();           // second sweep: no duplicate report
            Assert.Equal(1, pool.Stats().Leaks);
            pool.Release(conn);
        }

        // ----------------------------------------------------------------- backpressure
        [Fact]
        public void ZeroWaitersFailsFastWhenFull()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1, MaxWaiters = 0, AcquireTimeout = 5 });
            FakeConn held = pool.Acquire();
            Assert.Throws<PoolExhaustedError>(() => pool.Acquire());
            pool.Release(held);
        }

        [Fact]
        public void WaiterQueueIsCapped()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1, MaxWaiters = 1, AcquireTimeout = 5 });
            FakeConn held = pool.Acquire();
            var t = Task.Run(() =>
            {
                try { pool.Release(pool.Acquire(5)); }
                catch { /* ignore */ }
            });
            Helpers.SpinUntil(() => pool.Stats().Waiting >= 1);
            Assert.Throws<PoolExhaustedError>(() => pool.Acquire(5));
            pool.Release(held);
            t.Wait(2000);
        }

        // ----------------------------------------------------------------- lifecycle edges
        [Fact]
        public void OpenOnClosedPoolRaises()
        {
            ConnectionPool<FakeConn> pool = Make();
            pool.Close();
            Assert.Throws<PoolClosedError>(() => pool.Open());
        }

        [Fact]
        public void OpenIsIdempotent()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaintenanceInterval = 0.5 });
            Assert.Same(pool.Open(), pool.Open());
        }

        [Fact]
        public void RunMaintenanceOnClosedPoolIsNoop()
        {
            ConnectionPool<FakeConn> pool = Make();
            pool.Close();
            pool.RunMaintenance(); // returns quietly
        }

        [Fact]
        public void WarmupConnectionDestroyedIfClosedDuringCreate()
        {
            var made = new List<FakeConn>();
            ConnectionPool<FakeConn> pool = null!;
            var factory = new CallableFactory<FakeConn>(
                () => { var c = new FakeConn(); made.Add(c); pool.Close(); return c; },
                close: c => c.Closed = true);
            pool = new ConnectionPool<FakeConn>(factory, Helpers.QuietConfig() with { MinSize = 1 });
            pool.Open();
            Assert.True(made.Count > 0 && made[0].Closed);
        }

        [Fact]
        public void ReprMentionsSizes()
        {
            ConnectionPool<FakeConn> pool = Make();
            Assert.Contains("ConnectionPool", pool.ToString());
        }

        [Fact]
        public void LeakWarningWhenNotClosed()
        {
            var pool = new ConnectionPool<FakeConn>(new FakeFactory(), Helpers.QuietConfig());
            pool.Acquire();                  // size > 0, never closed
            Assert.True(pool.WarnIfLeaked());
            pool.Close();
            Assert.False(pool.WarnIfLeaked());
        }

        private sealed class BoomMaintenancePool : ConnectionPool<FakeConn>
        {
            public int Calls;
            public BoomMaintenancePool(PoolConfig cfg) : base(new FakeFactory(), cfg)
            {
            }

            public override void RunMaintenance()
            {
                Interlocked.Increment(ref Calls);
                throw new InvalidOperationException("sweep broke");
            }
        }

        private sealed class BlockingMaintenancePool : ConnectionPool<FakeConn>
        {
            public readonly ManualResetEventSlim Entered = new ManualResetEventSlim(false);
            public BlockingMaintenancePool(PoolConfig cfg) : base(new FakeFactory(), cfg) { }

            public override void RunMaintenance()
            {
                Entered.Set();
                Thread.Sleep(200); // stay inside the sweep so Close() sees a live thread to join
                base.RunMaintenance();
            }
        }

        [Fact]
        public void CloseJoinsLiveMaintenanceThread()
        {
            var pool = new BlockingMaintenancePool(Helpers.QuietConfig() with { MaintenanceInterval = 0.01 });
            pool.Open();
            Assert.True(pool.Entered.Wait(2000));
            pool.Close(); // thread is inside RunMaintenance (alive) -> Join runs
            Assert.True(pool.Closed);
        }

        [Fact]
        public void DisposeViaIDisposableClosesPool()
        {
            var pool = new ConnectionPool<FakeConn>(new FakeFactory(), Helpers.QuietConfig());
            using (var lease = pool.Connection())
            {
                Assert.NotNull(lease.Value);
            }
            Assert.Equal(0, pool.Stats().InUse);
            ((IDisposable)pool).Dispose();
            Assert.True(pool.Closed);
        }

        [Fact]
        public void UseResultPropagatesError()
        {
            ConnectionPool<FakeConn> pool = Make();
            Assert.Throws<ArgumentException>(() => pool.Use<int>(_ => throw new ArgumentException()));
            Assert.Equal(1, pool.Stats().Idle); // benign error -> connection kept
        }

        [Fact]
        public void InfiniteWaitWaiterIsServed()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1, AcquireTimeout = null });
            FakeConn held = pool.Acquire();
            FakeConn? got = null;
            var t = Task.Run(() => { got = pool.Acquire(); }); // Unset -> config null -> wait forever
            Helpers.SpinUntil(() => pool.Stats().Waiting == 1);
            pool.Release(held);
            t.Wait(2000);
            Assert.Same(held, got);
        }

        [Fact]
        public void HealthyIdleConnectionValidatedAndReused()
        {
            ConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { ValidateAfterIdle = 0 });
            FakeConn a = pool.Acquire();
            pool.Release(a);
            FakeConn b = pool.Acquire(); // idle_time 0 >= 0 -> validate -> healthy -> reuse
            Assert.Same(a, b);
            pool.Release(b);
        }

        [Fact]
        public void MaintenanceLoopSurvivesRunErrors()
        {
            var pool = new BoomMaintenancePool(Helpers.QuietConfig() with { MaintenanceInterval = 0.01 });
            var t = new Thread(() => pool.MaintenanceLoop()) { IsBackground = true };
            t.Start();
            Helpers.SpinUntil(() => pool.Calls > 0);
            pool.SignalStop();
            t.Join(1000);
            Assert.True(pool.Calls > 0);    // kept looping despite the error
            pool.Close();
        }
    }
}
