using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ConnPool;
using ConnPool.Tests.Support;
using Xunit;

namespace ConnPool.Tests.Pools
{
    public class AsyncPoolTests : IAsyncLifetime
    {
        private readonly List<AsyncConnectionPool<FakeConn>> _pools = new List<AsyncConnectionPool<FakeConn>>();
        private FakeFactory _inner = null!;
        private AsyncConnectionFactory<FakeConn> _factory = null!;
        private Recorder _rec = null!;
        private ManualClock? _clock;

        private AsyncConnectionPool<FakeConn> Make(
            AsyncConnectionFactory<FakeConn>? factory = null, ManualClock? clock = null, PoolConfig? cfg = null)
        {
            _inner = new FakeFactory();
            _factory = factory ?? new AsyncFakeFactory(_inner);
            _inner = (_factory as AsyncFakeFactory)?.Inner ?? _inner;
            _rec = new Recorder();
            _clock = clock;
            var pool = new AsyncConnectionPool<FakeConn>(
                _factory, cfg ?? Helpers.QuietConfig(), new PoolListener[] { _rec }, clock);
            _pools.Add(pool);
            return pool;
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            foreach (AsyncConnectionPool<FakeConn> pool in _pools)
            {
                try { await pool.CloseAsync(); }
                catch { /* best effort cleanup */ }
            }
        }

        [Fact]
        public async Task ReuseAndStats()
        {
            AsyncConnectionPool<FakeConn> pool = Make();
            FakeConn a = await pool.UseAsync(c => Task.FromResult(c));
            FakeConn b = await pool.UseAsync(c => Task.FromResult(c));
            Assert.Same(a, b);
            PoolStats s = pool.Stats();
            Assert.Equal((1, 2, 1, 0), (s.Created, s.Acquired, s.Idle, s.InUse));
        }

        [Fact]
        public async Task TimeoutWhenExhausted()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1 });
            FakeConn held = await pool.AcquireAsync();
            await Assert.ThrowsAsync<PoolTimeoutError>(() => pool.AcquireAsync(0.05));
            Assert.Equal(1, pool.Stats().Timeouts);
            await pool.ReleaseAsync(held);
        }

        [Fact]
        public async Task WaiterWokenByRelease()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1 });
            FakeConn held = await pool.AcquireAsync();
            Task<FakeConn> waiter = pool.AcquireAsync(2);
            await Helpers.SpinUntilAsync(() => pool.Stats().Waiting == 1);
            Assert.Equal(1, pool.Stats().Waiting);
            await pool.ReleaseAsync(held);
            Assert.Same(held, await waiter);
        }

        [Fact]
        public async Task DoubleReleaseRejected()
        {
            AsyncConnectionPool<FakeConn> pool = Make();
            FakeConn conn = await pool.AcquireAsync();
            await pool.ReleaseAsync(conn);
            await Assert.ThrowsAsync<InvalidReleaseError>(() => pool.ReleaseAsync(conn));
        }

        [Fact]
        public async Task RotationByLifetime()
        {
            var clock = new ManualClock();
            AsyncConnectionPool<FakeConn> pool = Make(clock: clock, cfg: Helpers.QuietConfig() with { MaxLifetime = 10 });
            FakeConn a = await pool.AcquireAsync();
            await pool.ReleaseAsync(a);
            clock.Advance(11);
            FakeConn b = await pool.AcquireAsync();
            Assert.NotSame(a, b);
            Assert.Equal(new[] { CloseReason.Expired }, _rec.ClosedSnapshot());
            await pool.ReleaseAsync(b);
        }

        [Fact]
        public async Task ValidationAndErrorDiscard()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { ValidateAfterIdle = 0 });
            FakeConn a = await pool.AcquireAsync();
            await pool.ReleaseAsync(a);
            a.Healthy = false;
            FakeConn b = await pool.AcquireAsync();
            Assert.NotSame(a, b);
            await pool.ReleaseAsync(b);

            _inner.FatalTypes = new[] { typeof(ConnReset) };
            FakeConn captured = null!;
            await Assert.ThrowsAsync<ConnReset>(() => pool.UseAsync(async c =>
            {
                captured = c;
                await Task.Yield();
                throw new ConnReset();
            }));
            Assert.True(captured.Closed);
        }

        [Fact]
        public async Task RetryAndBreaker()
        {
            var inner = new FakeFactory { FailAlways = true };
            var clock = new ManualClock();
            AsyncConnectionPool<FakeConn> pool = Make(new AsyncFakeFactory(inner), clock, Helpers.QuietConfig() with
            {
                AcquireTimeout = 60,
                Retry = new RetryPolicy { MaxAttempts = 2, BaseDelay = 1, Jitter = 0 },
                Breaker = new BreakerConfig { FailureThreshold = 2, RecoveryTimeout = 30 },
            });
            // one breaker failure per logical acquire, not per retry attempt
            await Assert.ThrowsAsync<ConnectionCreateError>(() => pool.AcquireAsync());
            Assert.Equal(2, inner.Attempts);
            Assert.Equal("closed", pool.Stats().BreakerState);
            await Assert.ThrowsAsync<ConnectionCreateError>(() => pool.AcquireAsync());
            Assert.Equal(4, inner.Attempts);
            await Assert.ThrowsAsync<CircuitOpenError>(() => pool.AcquireAsync());
            Assert.Equal(4, inner.Attempts);
            Assert.Equal(0, pool.Stats().Size);
            clock.Advance(31);
            inner.FailAlways = false;
            await pool.ReleaseAsync(await pool.AcquireAsync());
            Assert.Equal("closed", pool.Stats().BreakerState);
        }

        [Fact]
        public async Task CreateTimeoutIsEnforced()
        {
            AsyncConnectionPool<FakeConn> pool = Make(new AsyncFakeFactory(createDelay: 5), cfg: Helpers.QuietConfig() with { CreateTimeout = 0.05 });
            await Assert.ThrowsAsync<ConnectionCreateError>(() => pool.AcquireAsync());
            Assert.Equal(0, pool.Stats().Size);
        }

        [Fact]
        public async Task CancelWhileWaitingDoesNotLeak()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1, AcquireTimeout = null });
            FakeConn held = await pool.AcquireAsync();
            var cts = new CancellationTokenSource();
            Task<FakeConn> waiter = pool.AcquireAsync(cancellationToken: cts.Token);
            await Helpers.SpinUntilAsync(() => pool.Stats().Waiting == 1);
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await waiter);
            Assert.Equal(0, pool.Stats().Waiting);
            await pool.ReleaseAsync(held);
            Assert.Same(held, await pool.AcquireAsync(1));
        }

        [Fact]
        public async Task CancelDuringCreateFreesSlot()
        {
            AsyncConnectionPool<FakeConn> pool = Make(new AsyncFakeFactory(createDelay: 5), cfg: Helpers.QuietConfig() with { MaxSize = 1, CreateTimeout = null });
            var cts = new CancellationTokenSource();
            Task<FakeConn> task = pool.AcquireAsync(cancellationToken: cts.Token);
            await Task.Delay(20);
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await task);
            await Helpers.SpinUntilAsync(() => pool.Stats().Size == 0);
            Assert.Equal(0, pool.Stats().Size);
        }

        [Fact]
        public async Task CloseWakesWaiters()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1, AcquireTimeout = null });
            FakeConn held = await pool.AcquireAsync();
            Task<FakeConn> waiter = pool.AcquireAsync();
            await Helpers.SpinUntilAsync(() => pool.Stats().Waiting == 1);
            await pool.CloseAsync();
            await Assert.ThrowsAsync<PoolClosedError>(async () => await waiter);
            await pool.ReleaseAsync(held);
            Assert.True(held.Closed);
        }

        [Fact]
        public async Task MaintenanceTaskAndContextManager()
        {
            var factory = new AsyncFakeFactory();
            var pool = new AsyncConnectionPool<FakeConn>(factory, Helpers.QuietConfig() with { MinSize = 2, MaintenanceInterval = 0.02 });
            await pool.OpenAsync();
            Assert.Equal(2, pool.Stats().Idle);
            await pool.CloseAsync();
            Assert.True(pool.Closed);
            Assert.True(factory.Inner.Created.All(c => c.Closed));
        }

        private sealed class GatedPool : AsyncConnectionPool<FakeConn>
        {
            private readonly TaskCompletionSource<bool> _gate =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            public GatedPool(PoolConfig cfg) : base(new AsyncFakeFactory(), cfg) { }

            public void OpenGate() => _gate.TrySetResult(true);

            internal override async Task FillToMinAsync()
            {
                await _gate.Task;
                await base.FillToMinAsync();
            }
        }

        [Fact]
        public async Task OpenRacingCloseDoesNotLeakMaintenanceTask()
        {
            var pool = new GatedPool(Helpers.QuietConfig() with { MaintenanceInterval = 0.01 });
            Task<AsyncConnectionPool<FakeConn>> openTask = pool.OpenAsync();
            await Task.Yield();            // let OpenAsync reach the gate
            await pool.CloseAsync();       // runs fully; _task is still null
            pool.OpenGate();
            await openTask;

            Assert.True(pool.Closed);
            Assert.Null(pool.MaintenanceTask);
            await Task.Delay(30);          // a maintenance interval would have ticked
            Assert.Null(pool.MaintenanceTask);
        }

        [Fact]
        public async Task CancellationDuringDisposalStillCompletes()
        {
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var resume = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var closed = new List<object>();

            Func<object, CancellationToken, Task> slowClose = async (c, _) =>
            {
                started.TrySetResult(true);
                await resume.Task;
                closed.Add(c);
            };
            var rec = new Recorder();
            var pool = new AsyncConnectionPool<object>(
                new AsyncCallableFactory<object>(_ => Task.FromResult(new object()), close: slowClose),
                Helpers.QuietConfig() with { MaxSize = 2 }, new PoolListener[] { rec });

            object conn = await pool.AcquireAsync();
            var cts = new CancellationTokenSource();
            Task releasing = pool.ReleaseAsync(conn, discard: true, cancellationToken: cts.Token);
            await started.Task;                    // now suspended inside factory.CloseAsync()
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await releasing);
            Assert.Empty(closed);                  // disposal paused, not yet finished

            resume.TrySetResult(true);             // let the shielded disposal complete
            await Helpers.SpinUntilAsync(() => pool.Stats().Size == 0 && rec.ClosedSnapshot().Count > 0);

            Assert.Equal(new[] { conn }, closed);
            Assert.Equal(new[] { CloseReason.Discarded }, rec.ClosedSnapshot());
            PoolStats s = pool.Stats();
            Assert.Equal((0, 1), (s.Size, s.Closed));
            await pool.CloseAsync();
        }

        [Fact]
        public async Task ConcurrencyStress()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 4, AcquireTimeout = 10, MaxUses = 9 });
            var lockObj = new object();
            int now = 0, peak = 0;

            async Task Worker()
            {
                for (int i = 0; i < 40; i++)
                {
                    await pool.UseAsync(async conn =>
                    {
                        lock (lockObj) { now += 1; peak = Math.Max(peak, now); }
                        await Task.Yield();
                        Assert.False(conn.Closed);
                        lock (lockObj) { now -= 1; }
                    });
                }
            }

            await Task.WhenAll(Enumerable.Range(0, 25).Select(_ => Worker()));
            PoolStats s = pool.Stats();
            Assert.True(peak <= 4);
            Assert.Equal((0, 0, 25 * 40), (s.InUse, s.Waiting, s.Acquired));
            Assert.Equal(s.Idle, s.Created - s.Closed);
        }

        [Fact]
        public async Task FromCallablesAcceptsSyncAndAsync()
        {
            var closed = new List<object>();
            var pool = AsyncConnectionPool<object>.FromCallables(
                _ => Task.FromResult(new object()),
                close: (c, _) => { closed.Add(c); return Task.CompletedTask; },
                config: Helpers.QuietConfig());
            await pool.UseAsync(_ => Task.CompletedTask);
            await pool.CloseAsync();
            Assert.Single(closed);
        }

        [Fact]
        public async Task DuplicateConnectionFromFactoryIsRejected()
        {
            var shared = new object();
            var pool = AsyncConnectionPool<object>.FromCallables(_ => Task.FromResult(shared), config: Helpers.QuietConfig());
            object first = await pool.AcquireAsync();
            await Assert.ThrowsAsync<ConnectionCreateError>(() => pool.AcquireAsync());
            Assert.Equal(1, pool.Stats().Size);
            Assert.Equal(1, pool.Stats().Created);
            await pool.ReleaseAsync(first);
            await pool.CloseAsync();
        }

        [Fact]
        public async Task ValidateIdleEvictsDeadConnections()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { ValidateIdle = true, MaxSize = 2 });
            FakeConn a = await pool.AcquireAsync();
            FakeConn b = await pool.AcquireAsync();
            await pool.ReleaseAsync(a);
            await pool.ReleaseAsync(b);
            Assert.Equal(2, pool.Stats().Idle);
            a.Healthy = false;
            await pool.RunMaintenanceAsync();
            Assert.Equal(1, pool.Stats().Idle);
            Assert.Contains(CloseReason.Unhealthy, _rec.ClosedSnapshot());
            FakeConn c = await pool.UseAsync(x => Task.FromResult(x));
            Assert.Same(b, c);
        }

        [Fact]
        public async Task DefaultConfigWhenOmitted()
        {
            var pool = new AsyncConnectionPool<FakeConn>(new AsyncFakeFactory()); // null config -> new PoolConfig()
            _pools.Add(pool);
            FakeConn c = await pool.AcquireAsync();
            await pool.ReleaseAsync(c);
            Assert.Equal(1, pool.Stats().Size);
        }

        [Fact]
        public async Task ExplicitRngIsAccepted()
        {
            // A caller may inject its own Random (for deterministic jitter); it is used instead of a fresh one.
            var pool = new AsyncConnectionPool<FakeConn>(new AsyncFakeFactory(), rng: new Random(123));
            _pools.Add(pool);
            FakeConn c = await pool.AcquireAsync();
            await pool.ReleaseAsync(c);
            Assert.Equal(1, pool.Stats().Size);
        }

        [Fact]
        public async Task HealthCheckClosedMidValidationReportsPoolClosed()
        {
            AsyncConnectionPool<FakeConn> pool = null!;
            var closedOnce = false;
            var factory = new AsyncCallableFactory<FakeConn>(
                _ => Task.FromResult(new FakeConn()),
                validate: async (_, __) =>
                {
                    if (!closedOnce) { closedOnce = true; await pool.CloseAsync(); }
                    return true;  // the connection itself is fine
                });
            pool = Make(factory, cfg: Helpers.QuietConfig() with { ValidateIdle = true });
            FakeConn a = await pool.AcquireAsync();
            await pool.ReleaseAsync(a);
            await pool.RunMaintenanceAsync();
            Assert.Contains(CloseReason.PoolClosed, _rec.ClosedSnapshot());
        }

        [Fact]
        public async Task AcquireWithNoTimeoutsUsesUnboundedCreate()
        {
            // Neither an acquire deadline nor a create timeout: the create path takes its
            // "no limit" branches (no timeout CTS, linked token from the caller only).
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with
            {
                AcquireTimeout = null,
                CreateTimeout = null,
            });
            FakeConn c = await pool.AcquireAsync();
            await pool.ReleaseAsync(c);
            Assert.Equal(1, pool.Stats().Created);
        }

        [Fact]
        public async Task RetrySleepsWhenAcquireTimeoutIsInfinite()
        {
            var inner = new FakeFactory { FailTimes = 1 };
            AsyncConnectionPool<FakeConn> pool = Make(new AsyncFakeFactory(inner), new ManualClock(),
                Helpers.QuietConfig() with
                {
                    AcquireTimeout = null,
                    Retry = new RetryPolicy { MaxAttempts = 3, BaseDelay = 1, Jitter = 0 },
                });
            FakeConn c = await pool.AcquireAsync();  // 1st create fails -> sleep -> 2nd succeeds
            Assert.Equal(2, inner.Attempts);
            Assert.NotNull(c);
        }

        [Fact]
        public async Task CreateFailureWithEmptyMessageIsDescribedByType()
        {
            var factory = new AsyncCallableFactory<FakeConn>(
                _ => throw new ConnErr(""));          // empty message -> Describe() uses the type name
            AsyncConnectionPool<FakeConn> pool = Make(factory, cfg: Helpers.QuietConfig() with
            {
                Retry = RetryPolicy.NoRetry,
            });
            var ex = await Assert.ThrowsAsync<ConnectionCreateError>(() => pool.AcquireAsync());
            Assert.Contains("ConnErr", ex.Message);
        }

        [Fact]
        public async Task LeakDetectionWarnsOnce()
        {
            var clock = new ManualClock();
            AsyncConnectionPool<FakeConn> pool = Make(clock: clock, cfg: Helpers.QuietConfig() with { LeakDetectionTimeout = 10 });
            FakeConn conn = await pool.AcquireAsync();
            clock.Advance(11);
            await pool.RunMaintenanceAsync();
            await pool.RunMaintenanceAsync();
            Assert.Equal(1, pool.Stats().Leaks);
            await pool.ReleaseAsync(conn);
        }

        [Fact]
        public async Task GracefulDrainWaitsForInUse()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 2 });
            FakeConn held = await pool.AcquireAsync();
            Task returner = Task.Run(async () =>
            {
                await Task.Delay(50);
                await pool.ReleaseAsync(held);
            });
            await pool.CloseAsync(2);
            await returner;
            Assert.True(held.Closed);
            Assert.Equal(0, pool.Stats().InUse);
        }

        [Fact]
        public async Task DrainTimeoutGivesUp()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1 });
            FakeConn held = await pool.AcquireAsync();
            await pool.CloseAsync(0.05);
            Assert.Equal(1, pool.Stats().InUse);
            Assert.False(held.Closed);
            await pool.ReleaseAsync(held);
            Assert.True(held.Closed);
        }

        [Fact]
        public async Task OpenEdges()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaintenanceInterval = 0.5 });
            Assert.Same(await pool.OpenAsync(), await pool.OpenAsync());
            await pool.CloseAsync();
            await Assert.ThrowsAsync<PoolClosedError>(() => pool.OpenAsync());
        }

        [Fact]
        public async Task DrainZeroTimeoutReturnsImmediately()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1 });
            FakeConn held = await pool.AcquireAsync();
            await pool.CloseAsync(0);
            Assert.Equal(1, pool.Stats().InUse);
            await pool.ReleaseAsync(held);
        }

        [Fact]
        public async Task RunMaintenanceOnClosedPoolIsNoop()
        {
            AsyncConnectionPool<FakeConn> pool = Make();
            await pool.CloseAsync();
            await pool.RunMaintenanceAsync();
        }

        [Fact]
        public async Task MaintenanceEvictsIdle()
        {
            var clock = new ManualClock();
            AsyncConnectionPool<FakeConn> pool = Make(clock: clock, cfg: Helpers.QuietConfig() with { MaxIdleTime = 10 });
            FakeConn a = await pool.AcquireAsync();
            await pool.ReleaseAsync(a);
            clock.Advance(11);
            await pool.RunMaintenanceAsync();
            Assert.Equal(0, pool.Stats().Idle);
            Assert.Equal(new[] { CloseReason.Idle }, _rec.ClosedSnapshot());
        }

        [Fact]
        public async Task MaxWaitersFailsFast()
        {
            AsyncConnectionPool<FakeConn> pool = Make(cfg: Helpers.QuietConfig() with { MaxSize = 1, MaxWaiters = 0, AcquireTimeout = 5 });
            FakeConn held = await pool.AcquireAsync();
            await Assert.ThrowsAsync<PoolExhaustedError>(() => pool.AcquireAsync());
            await pool.ReleaseAsync(held);
        }

        [Fact]
        public async Task ReleaseDiscardAndCloseErrorDoNotLeak()
        {
            AsyncConnectionPool<FakeConn> pool = Make(new AsyncCallableFactory<FakeConn>(
                _ => Task.FromResult(new FakeConn()),
                close: (c, _) => throw new System.IO.IOException("close boom")));
            await pool.ReleaseAsync(await pool.AcquireAsync(), discard: true);
            Assert.Equal(0, pool.Stats().Size);
            Assert.Equal(new[] { CloseReason.Discarded }, _rec.ClosedSnapshot());
        }

        [Fact]
        public async Task ReleaseResetErrorIsResetFailed()
        {
            AsyncConnectionPool<FakeConn> pool = Make(new AsyncCallableFactory<FakeConn>(
                _ => Task.FromResult(new FakeConn()),
                reset: (c, _) => throw new InvalidOperationException("reset boom")));
            await pool.ReleaseAsync(await pool.AcquireAsync());
            Assert.Equal(new[] { CloseReason.ResetFailed }, _rec.ClosedSnapshot());
        }

        [Fact]
        public async Task ReleaseResetCancellationDestroys()
        {
            AsyncConnectionPool<FakeConn> pool = Make(new AsyncCallableFactory<FakeConn>(
                _ => Task.FromResult(new FakeConn()),
                reset: (c, _) => throw new OperationCanceledException()));
            FakeConn a = await pool.AcquireAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pool.ReleaseAsync(a));
            Assert.Equal(new[] { CloseReason.Error }, _rec.ClosedSnapshot());
        }

        [Fact]
        public async Task CheckoutValidationErrorIsUnhealthy()
        {
            AsyncConnectionPool<FakeConn> pool = Make(
                new AsyncCallableFactory<FakeConn>(_ => Task.FromResult(new FakeConn()),
                    validate: (c, _) => throw new InvalidOperationException("ping boom")),
                cfg: Helpers.QuietConfig() with { ValidateAfterIdle = 0 });
            FakeConn a = await pool.AcquireAsync();
            await pool.ReleaseAsync(a);
            FakeConn b = await pool.AcquireAsync();
            Assert.NotSame(a, b);
            Assert.Equal(new[] { CloseReason.Unhealthy }, _rec.ClosedSnapshot());
            await pool.ReleaseAsync(b);
        }

        [Fact]
        public async Task BorrowValidationCancellationIsNotSwallowed()
        {
            AsyncConnectionPool<FakeConn> pool = Make(
                new AsyncCallableFactory<FakeConn>(_ => Task.FromResult(new FakeConn()),
                    validate: (c, _) => throw new OperationCanceledException()),
                cfg: Helpers.QuietConfig() with { ValidateAfterIdle = 0 });
            FakeConn a = await pool.AcquireAsync();
            await pool.ReleaseAsync(a);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pool.AcquireAsync());
            Assert.Equal(new[] { CloseReason.Error }, _rec.ClosedSnapshot());
        }

        [Fact]
        public async Task CreateTimeoutDuringRetryDoesNotTripBreaker()
        {
            var inner = new FakeFactory { FailAlways = true };
            var clock = new ManualClock();
            AsyncConnectionPool<FakeConn> pool = Make(new AsyncFakeFactory(inner), clock, Helpers.QuietConfig() with
            {
                AcquireTimeout = 1,
                Retry = new RetryPolicy { MaxAttempts = 3, BaseDelay = 10, Jitter = 0 },
                Breaker = new BreakerConfig { FailureThreshold = 1 },
            });
            await Assert.ThrowsAsync<PoolTimeoutError>(() => pool.AcquireAsync());
            Assert.Equal("closed", pool.Stats().BreakerState);
        }

        [Fact]
        public async Task CreateTimeoutCountsAsBreakerFailure()
        {
            AsyncConnectionPool<FakeConn> pool = Make(new AsyncFakeFactory(createDelay: 5), cfg: Helpers.QuietConfig() with
            {
                CreateTimeout = 0.05,
                AcquireTimeout = 5,
                Breaker = new BreakerConfig { FailureThreshold = 1 },
            });
            await Assert.ThrowsAsync<ConnectionCreateError>(() => pool.AcquireAsync());
            Assert.Equal("open", pool.Stats().BreakerState);
        }

        [Fact]
        public async Task AcquireDeadlineTimeoutDoesNotTripBreaker()
        {
            AsyncConnectionPool<FakeConn> pool = Make(new AsyncFakeFactory(createDelay: 5), cfg: Helpers.QuietConfig() with
            {
                CreateTimeout = null,
                AcquireTimeout = 0.05,
                Breaker = new BreakerConfig { FailureThreshold = 1 },
            });
            await Assert.ThrowsAsync<PoolTimeoutError>(() => pool.AcquireAsync());
            Assert.Equal("closed", pool.Stats().BreakerState);
        }

        [Fact]
        public async Task CancelWhileRegisteringDoesNotLeak()
        {
            var built = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var proceed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var closes = new List<FakeConn>();

            Func<CancellationToken, Task<FakeConn>> create = async ct =>
            {
                var conn = new FakeConn();
                built.TrySetResult(true);       // connection built; _create is about to return
                await proceed.Task;             // hold until the test is holding _cond
                return conn;
            };
            Func<FakeConn, CancellationToken, Task> closer = (conn, _) =>
            {
                closes.Add(conn);
                conn.Closed = true;
                return Task.CompletedTask;
            };

            var pool = new AsyncConnectionPool<FakeConn>(
                new AsyncCallableFactory<FakeConn>(create, close: closer), Helpers.QuietConfig() with { MaxSize = 2 });

            var cts = new CancellationTokenSource();
            Task<FakeConn> task = pool.AcquireAsync(cancellationToken: cts.Token);
            await built.Task;                   // parked inside create(); slot reserved

            AsyncCondition.Releaser holder = await pool.Cond.LockAsync(); // force RegisterNew's lock to block
            try
            {
                // Cancel BEFORE RegisterNew runs: its lock-wait then observes an already-cancelled
                // token while the lock is still held, so there is no Cancel/Release race.
                cts.Cancel();
                proceed.TrySetResult(true);     // create() returns -> RegisterNew blocks on the held lock
                await Task.Delay(50);           // let the cancellation unwind while we still hold the lock
            }
            finally
            {
                holder.Dispose();               // let the shielded cleanup proceed
            }

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await task);
            await Helpers.SpinUntilAsync(() => closes.Count == 1 && pool.Stats().Size == 0);
            Assert.Single(closes);              // closed exactly once
            Assert.Equal(0, pool.Stats().Size); // slot accounting restored
            Assert.Equal(0, pool.Stats().Idle);
            Assert.Equal(0, pool.Stats().Created); // never adopted -> not counted
            FakeConn reused = await pool.UseAsync(async c => { await Task.Yield(); return c; });
            Assert.False(reused.Closed);        // pool still usable
            await pool.CloseAsync();
        }

        [Fact]
        public async Task ShieldedDestroyRaceNeverDoubleDestroysOnPoolClosed()
        {
            // Regression test for the closed-path double-free: AsyncUtil.AwaitShielded's internal
            // Task.WhenAny(work, cancelSignal) can pick the cancellation branch even though `work`
            // (the shielded factory.CloseAsync + bookkeeping) has *already run to completion*, if the
            // two complete concurrently on different threads (proven racy: see AwaitShielded's own
            // behaviour under concurrent completion/cancel). When that happens inside RegisterNewAsync's
            // "closed" path, a redelivered OperationCanceledException must not escape to Acquire's
            // catch-all and Destroy() the same connection a second time.
            const int iterations = 200; // bounded, deterministic-enough to reliably hit the window

            for (int i = 0; i < iterations; i++)
            {
                var closeGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                var closeStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                var closes = new List<FakeConn>();
                AsyncConnectionPool<FakeConn> pool = null!;

                // Close the pool from inside create(), after the connection is built but before
                // RegisterNewAsync's lock-check runs - the same technique as
                // ConnectionCreatedDuringCloseIsDestroyed, so RegisterNewAsync reliably observes
                // "closed" and goes through the Destroy(..., PoolClosed) + guarded-throw path.
                Func<CancellationToken, Task<FakeConn>> create = async ct =>
                {
                    var conn = new FakeConn();
                    await pool.CloseAsync().ConfigureAwait(false);
                    return conn;
                };
                Func<FakeConn, CancellationToken, Task> closer = async (conn, _) =>
                {
                    closeStarted.TrySetResult(true);
                    await closeGate.Task.ConfigureAwait(false); // released concurrently with Cancel()
                    lock (closes) closes.Add(conn);
                    conn.Closed = true;
                };

                pool = new AsyncConnectionPool<FakeConn>(
                    new AsyncCallableFactory<FakeConn>(create, close: closer), Helpers.QuietConfig());

                var cts = new CancellationTokenSource();
                Task<FakeConn> acquire = pool.AcquireAsync(cancellationToken: cts.Token);
                await closeStarted.Task; // the shielded Dispose() is now awaiting factory.CloseAsync()

                // Race factory.CloseAsync() finishing against the external cancellation, matching
                // AwaitShielded's real concurrent-completion window.
                var t1 = Task.Run(() => closeGate.TrySetResult(true));
                var t2 = Task.Run(() => cts.Cancel());
                await Task.WhenAll(t1, t2);

                Exception observed;
                try
                {
                    await acquire;
                    observed = null!;
                }
                catch (Exception ex)
                {
                    observed = ex;
                }

                // With the guard in place, a redelivered cancellation must never replace the domain
                // error - the caller always observes PoolClosedError, never OperationCanceledException,
                // for this path.
                Assert.IsType<PoolClosedError>(observed);

                await Helpers.SpinUntilAsync(() => closes.Count >= 1);
                Assert.Single(closes); // never closed twice
            }
        }

        [Fact]
        public async Task PoolClosedMidRetry()
        {
            AsyncConnectionPool<FakeConn> pool = null!;
            var factory = new AsyncCallableFactory<FakeConn>(async ct =>
            {
                await pool.CloseAsync();
                throw new ConnErr("down");
            });
            pool = Make(factory, cfg: Helpers.QuietConfig() with { Retry = new RetryPolicy { MaxAttempts = 3, BaseDelay = 0, Jitter = 0 } });
            await Assert.ThrowsAsync<PoolClosedError>(() => pool.AcquireAsync());
        }

        [Fact]
        public async Task ConnectionCreatedDuringCloseIsDestroyed()
        {
            AsyncConnectionPool<FakeConn> pool = null!;
            var made = new List<FakeConn>();
            var factory = new AsyncCallableFactory<FakeConn>(
                async ct => { var c = new FakeConn(); made.Add(c); await pool.CloseAsync(); return c; },
                close: (c, _) => { c.Closed = true; return Task.CompletedTask; });
            pool = Make(factory);
            await Assert.ThrowsAsync<PoolClosedError>(() => pool.AcquireAsync());
            Assert.True(made.Count > 0 && made[0].Closed);
        }

        [Fact]
        public async Task WarmupFailureIsNotFatal()
        {
            var inner = new FakeFactory { FailAlways = true };
            AsyncConnectionPool<FakeConn> pool = Make(new AsyncFakeFactory(inner), cfg: Helpers.QuietConfig() with { MinSize = 2 });
            await pool.OpenAsync();
            Assert.Equal(0, pool.Stats().Size);
        }

        [Fact]
        public async Task WarmupConnectionDestroyedIfClosedDuringCreate()
        {
            AsyncConnectionPool<FakeConn> pool = null!;
            var made = new List<FakeConn>();
            var factory = new AsyncCallableFactory<FakeConn>(
                async ct => { var c = new FakeConn(); made.Add(c); await pool.CloseAsync(); return c; },
                close: (c, _) => { c.Closed = true; return Task.CompletedTask; });
            pool = new AsyncConnectionPool<FakeConn>(factory, Helpers.QuietConfig() with { MinSize = 1 });
            await pool.OpenAsync();
            Assert.True(made.Count > 0 && made[0].Closed);
            await pool.CloseAsync();
        }

        private sealed class BoomMaintenancePool : AsyncConnectionPool<FakeConn>
        {
            public int Calls;
            public BoomMaintenancePool(PoolConfig cfg) : base(new AsyncFakeFactory(), cfg) { }

            public override async Task RunMaintenanceAsync()
            {
                Interlocked.Increment(ref Calls);
                await Task.Yield();
                throw new InvalidOperationException("sweep broke");
            }
        }

        [Fact]
        public async Task MaintenanceLoopRunsAndSurvivesErrors()
        {
            var pool = new BoomMaintenancePool(Helpers.QuietConfig() with { MaintenanceInterval = 0.01 });
            await pool.OpenAsync();
            await Helpers.SpinUntilAsync(() => pool.Calls > 0);
            await Task.Delay(30);
            Assert.True(pool.Calls >= 1);
            await pool.CloseAsync();
        }

        [Fact]
        public async Task AwaitUsingDisposesPool()
        {
            var factory = new AsyncFakeFactory();
            await using (var pool = new AsyncConnectionPool<FakeConn>(factory, Helpers.QuietConfig() with { MinSize = 1 }))
            {
                await pool.OpenAsync();
                Assert.Equal(1, pool.Stats().Idle);
            }
            Assert.True(factory.Inner.Created.All(c => c.Closed));
        }

        [Fact]
        public async Task UseResultPropagatesError()
        {
            AsyncConnectionPool<FakeConn> pool = Make();
            await Assert.ThrowsAsync<ArgumentException>(() => pool.UseAsync<int>(async _ =>
            {
                await Task.Yield();
                throw new ArgumentException();
            }));
            Assert.Equal(1, pool.Stats().Idle);
        }

        [Fact]
        public async Task AsyncLeaseReleasesOnDispose()
        {
            AsyncConnectionPool<FakeConn> pool = Make();
            await using (AsyncConnectionPool<FakeConn>.AsyncLease lease = await pool.ConnectionAsync())
            {
                Assert.NotNull(lease.Value);
            }
            Assert.Equal(0, pool.Stats().InUse);
        }

        private sealed class CountingMaintenancePool : AsyncConnectionPool<FakeConn>
        {
            public int Calls;
            public CountingMaintenancePool(PoolConfig cfg) : base(new AsyncFakeFactory(), cfg) { }

            public override async Task RunMaintenanceAsync()
            {
                await base.RunMaintenanceAsync();
                Interlocked.Increment(ref Calls);
            }
        }

        [Fact]
        public async Task MaintenanceLoopRunsSuccessfully()
        {
            var pool = new CountingMaintenancePool(Helpers.QuietConfig() with { MaintenanceInterval = 0.01 });
            await pool.OpenAsync();
            await Helpers.SpinUntilAsync(() => pool.Calls > 0);
            Assert.True(pool.Calls > 0);
            await pool.CloseAsync();
        }

        [Fact]
        public async Task ExpiredConnectionDroppedWithLiveToken()
        {
            var clock = new ManualClock();
            AsyncConnectionPool<FakeConn> pool = Make(clock: clock, cfg: Helpers.QuietConfig() with { MaxLifetime = 10 });
            using var cts = new CancellationTokenSource();
            FakeConn a = await pool.AcquireAsync(cancellationToken: cts.Token);
            await pool.ReleaseAsync(a, cancellationToken: cts.Token);
            clock.Advance(11);
            // drops expired 'a' via a shielded Destroy that completes under a cancellable (live) token
            FakeConn b = await pool.AcquireAsync(cancellationToken: cts.Token);
            Assert.NotSame(a, b);
            await pool.ReleaseAsync(b);
        }

        [Fact]
        public void ReprMentionsSizes()
        {
            Assert.Contains("AsyncConnectionPool", Make().ToString());
        }

        [Fact]
        public async Task LeakWarningWhenNotClosed()
        {
            var pool = new AsyncConnectionPool<FakeConn>(new AsyncFakeFactory(), Helpers.QuietConfig());
            await pool.AcquireAsync();
            Assert.True(pool.WarnIfLeaked());
            await pool.CloseAsync();
            Assert.False(pool.WarnIfLeaked());
        }
    }
}
