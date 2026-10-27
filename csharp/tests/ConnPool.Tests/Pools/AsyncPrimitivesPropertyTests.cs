// Property-based companion to the fuzz/ConnPool.Fuzz SharpFuzz harness (scaffold-only on this
// platform - see fuzz/ConnPool.Fuzz/README.md for the libFuzzer manual steps). Runs in the normal
// gate, counts toward the 100% coverage threshold, and gives on-platform coverage of the same
// invariants the fuzz harness targets: the AsyncCondition waiter LinkedList (SemaphoreSlim mutex +
// TaskCompletionSource waiters) and AwaitShielded, driven through AsyncConnectionPool<T>'s public
// AcquireAsync/ReleaseAsync surface with a CancellationToken that may fire mid-acquire.
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ConnPool;
using ConnPool.Tests.Support;
using Xunit;

namespace ConnPool.Tests.Pools
{
    public class AsyncPrimitivesPropertyTests
    {
        private static AsyncConnectionPool<FakeConn> MakePool(int maxSize = 2) =>
            AsyncConnectionPool<FakeConn>.FromCallables(
                create: _ => Task.FromResult(new FakeConn()),
                close: (c, _) => { c.Closed = true; return Task.CompletedTask; },
                config: Helpers.QuietConfig() with { MaxSize = maxSize, MinSize = 0 },
                name: "prop");

        /// <summary>
        /// Drive a bounded op sequence derived from a seed through a tiny pool, racing cancellation
        /// against AcquireAsync/ReleaseAsync, then assert the pool never leaks a reserved slot:
        /// idle + in_use == size at rest. Mirrors the op decoding in fuzz/ConnPool.Fuzz/Program.cs.
        /// </summary>
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(12345)]
        [InlineData(int.MaxValue)]
        public async Task AcquireReleaseWithCancellation_NeverLeaksASlot(int seed)
        {
            var rng = new Random(seed);
            await using AsyncConnectionPool<FakeConn> pool = MakePool();
            await pool.OpenAsync();

            var held = new List<FakeConn>();
            for (int i = 0; i < 40; i++)
            {
                switch (rng.Next(4))
                {
                    case 0:
                        try
                        {
                            held.Add(await pool.AcquireAsync());
                        }
                        catch (PoolTimeoutError) { /* exhausted under contention - expected */ }
                        break;

                    case 1:
                        if (held.Count > 0)
                        {
                            var conn = held[^1];
                            held.RemoveAt(held.Count - 1);
                            await pool.ReleaseAsync(conn);
                        }
                        break;

                    case 2:
                        using (var cts = new CancellationTokenSource())
                        {
                            cts.CancelAfter(TimeSpan.FromMilliseconds(rng.Next(5)));
                            try
                            {
                                held.Add(await pool.AcquireAsync(cancellationToken: cts.Token));
                            }
                            catch (OperationCanceledException) { /* caller cancelled - expected */ }
                            catch (PoolTimeoutError) { /* deadline reached - expected */ }
                        }
                        break;

                    default:
                        if (held.Count > 0)
                        {
                            var conn = held[^1];
                            held.RemoveAt(held.Count - 1);
                            using var cts = new CancellationTokenSource();
                            cts.Cancel();
                            try
                            {
                                await pool.ReleaseAsync(conn, cancellationToken: cts.Token);
                            }
                            catch (OperationCanceledException) { /* shielded disposal still ran */ }
                        }
                        break;
                }
            }

            foreach (var conn in held)
            {
                try { await pool.ReleaseAsync(conn); }
                catch (InvalidReleaseError) { /* already released on a cancelled-but-completed path */ }
            }

            PoolStats stats = pool.Stats();
            Assert.Equal(stats.Size, stats.Idle + stats.InUse);
        }

        /// <summary>
        /// A cancelled AcquireAsync throws OperationCanceledException and never reserves a slot it
        /// doesn't give back: with MaxSize=1 and the one slot checked out, a second, pre-cancelled
        /// acquire must fail without bumping size.
        /// </summary>
        [Fact]
        public async Task CancelledAcquire_ThrowsAndReservesNoSlot()
        {
            await using AsyncConnectionPool<FakeConn> pool = MakePool(maxSize: 1);
            await pool.OpenAsync();
            FakeConn first = await pool.AcquireAsync();

            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => pool.AcquireAsync(cancellationToken: cts.Token));

            PoolStats stats = pool.Stats();
            Assert.Equal(1, stats.Size);
            Assert.Equal(0, stats.Idle);
            Assert.Equal(1, stats.InUse);

            await pool.ReleaseAsync(first);
        }

        /// <summary>
        /// AwaitShielded: releasing under an already-cancelled token still lets factory disposal
        /// finish (close observed), while the caller sees OperationCanceledException - the twin of
        /// asyncio.shield letting cleanup finish under the background token.
        /// </summary>
        [Fact]
        public async Task ReleaseUnderCancelledToken_StillClosesDiscardedConnection()
        {
            await using AsyncConnectionPool<FakeConn> pool = MakePool(maxSize: 1);
            await pool.OpenAsync();
            FakeConn conn = await pool.AcquireAsync();

            using var cts = new CancellationTokenSource();
            cts.Cancel();
            // discard: true forces the Destroy/AwaitShielded path even though the token is already
            // cancelled when ReleaseAsync is entered. Whether the caller observes
            // OperationCanceledException depends on whether disposal (synchronous here) or the
            // cancellation registration wins the race - AwaitShielded's actual guarantee is that
            // disposal always finishes regardless of which one wins.
            try
            {
                await pool.ReleaseAsync(conn, discard: true, cancellationToken: cts.Token);
            }
            catch (OperationCanceledException) { /* shielded: disposal still completes */ }

            await Helpers.SpinUntilAsync(() => conn.Closed);
            Assert.True(conn.Closed);
        }

        /// <summary>
        /// AsyncCondition.NotifyAll must wake every still-live waiter and leave the waiter list
        /// empty at rest, even when several callers are queued behind a saturated pool.
        /// </summary>
        [Fact]
        public async Task ManyQueuedWaiters_AllDrainOnRelease()
        {
            await using AsyncConnectionPool<FakeConn> pool = MakePool(maxSize: 1);
            await pool.OpenAsync();
            FakeConn held = await pool.AcquireAsync();

            Task<FakeConn>[] waiters = new Task<FakeConn>[5];
            for (int i = 0; i < waiters.Length; i++)
                waiters[i] = pool.AcquireAsync(timeout: 2.0);

            await Helpers.SpinUntilAsync(() => pool.Stats().Waiting == waiters.Length);
            await pool.ReleaseAsync(held);

            // Drain the waiters by repeatedly releasing whichever one completes next, freeing the
            // slot for the next queued waiter in turn.
            var pending = new List<Task<FakeConn>>(waiters);
            while (pending.Count > 0)
            {
                Task<FakeConn> done = await Task.WhenAny(pending);
                pending.Remove(done);
                FakeConn c = await done;
                await pool.ReleaseAsync(c);
            }

            PoolStats stats = pool.Stats();
            Assert.Equal(0, stats.Waiting);
            Assert.Equal(stats.Size, stats.Idle + stats.InUse);
        }
    }
}
