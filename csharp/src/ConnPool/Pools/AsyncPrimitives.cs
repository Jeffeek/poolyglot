// Async synchronisation primitives the AsyncConnectionPool needs but the BCL does not ship
// for netstandard2.1: an await-able condition variable (the twin of asyncio.Condition) and a
// "shield" helper (the twin of asyncio.shield).
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ConnPool
{
    /// <summary>
    /// Random wrapper that is safe to call from pool code running on several threads. All standard
    /// entry points are serialised (not just NextDouble) so no caller can accidentally reach the
    /// non-thread-safe base <see cref="Random"/>. (netstandard2.1 has no <c>Random.Shared</c>.)
    /// </summary>
    internal sealed class ThreadSafeRandom : Random
    {
        private readonly Random _inner;
        private readonly object _lock = new();

        public ThreadSafeRandom(Random inner) => _inner = inner;

        public override double NextDouble()
        {
            lock (_lock) return _inner.NextDouble();
        }

        public override int Next()
        {
            lock (_lock) return _inner.Next();
        }

        public override int Next(int maxValue)
        {
            lock (_lock) return _inner.Next(maxValue);
        }

        public override int Next(int minValue, int maxValue)
        {
            lock (_lock) return _inner.Next(minValue, maxValue);
        }

        public override void NextBytes(byte[] buffer)
        {
            lock (_lock) _inner.NextBytes(buffer);
        }
    }

    /// <summary>
    /// Await-able condition variable: owns a mutex, lets a holder release it while waiting to be
    /// notified, then re-acquires it before returning. Mirrors Python's <c>asyncio.Condition</c>.
    /// </summary>
    internal sealed class AsyncCondition
    {
        private readonly SemaphoreSlim _mutex = new(1, 1);
        private readonly LinkedList<TaskCompletionSource<bool>> _waiters = new();

        public readonly struct Releaser : IDisposable
        {
            private readonly AsyncCondition _owner;
            internal Releaser(AsyncCondition owner) => _owner = owner;
            public void Dispose() => _owner.Exit();
        }

        public async Task<Releaser> LockAsync(CancellationToken cancellationToken = default)
        {
            await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new Releaser(this);
        }

        /// <summary>
        /// Acquire the lock synchronously for a short, non-awaiting critical section (e.g. reading
        /// a stats snapshot). Safe because the lock is only ever held across synchronous sections.
        /// </summary>
        public Releaser LockSync()
        {
            _mutex.Wait();
            return new Releaser(this);
        }

        private void Exit() => _mutex.Release();

        /// <summary>
        /// Release the lock, wait to be notified (or time out / be cancelled), then re-acquire it.
        /// Returns true if notified, false if the timeout elapsed; throws if <paramref name="cancellationToken"/> fires.
        /// Precondition: the caller holds the lock.
        /// </summary>
        public async Task<bool> WaitAsync(double? timeoutSeconds, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var node = _waiters.AddLast(tcs);
            Exit();

            var timeoutCts = timeoutSeconds is null
                ? null
                : new CancellationTokenSource(TimeSpan.FromSeconds(Math.Max(0.0, timeoutSeconds.Value)));
            try
            {
                using (timeoutCts)
                using (var linked = timeoutCts is null
                    ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                    : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token))
                using (linked.Token.Register(static s => ((TaskCompletionSource<bool>)s!).TrySetCanceled(), tcs))
                {
                    await tcs.Task.ConfigureAwait(false);
                }
                await ReacquireAsync(node).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException)
            {
                await ReacquireAsync(node).ConfigureAwait(false);
                if (cancellationToken.IsCancellationRequested)
                    throw new OperationCanceledException(cancellationToken);
                return false; // timed out
            }
        }

        private async Task ReacquireAsync(LinkedListNode<TaskCompletionSource<bool>> node)
        {
            await _mutex.WaitAsync().ConfigureAwait(false); // reacquire is not cancellable
            if (node.List != null)
                _waiters.Remove(node);
        }

        /// <summary>Wake up to <paramref name="n"/> live waiters. Precondition: the caller holds the lock.</summary>
        public void Notify(int n = 1)
        {
            var node = _waiters.First;
            while (node != null && n > 0)
            {
                var next = node.Next;
                _waiters.Remove(node);
                if (node.Value.TrySetResult(true))
                    n -= 1;
                node = next;
            }
        }

        public void NotifyAll() => Notify(int.MaxValue);
    }

    internal static class AsyncUtil
    {
        /// <summary>
        /// Await <paramref name="work"/>, but if <paramref name="cancellationToken"/> fires first,
        /// throw <see cref="OperationCanceledException"/> while <paramref name="work"/> keeps running
        /// to completion in the background. The twin of asyncio.shield.
        /// </summary>
        public static async Task AwaitShielded(Task work, CancellationToken cancellationToken)
        {
            if (!cancellationToken.CanBeCanceled)
            {
                await work.ConfigureAwait(false);
                return;
            }

            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (cancellationToken.Register(static s => ((TaskCompletionSource<bool>)s!).TrySetCanceled(), tcs))
            {
                var completed = await Task.WhenAny(work, tcs.Task).ConfigureAwait(false);
                if (completed != work)
                    throw new OperationCanceledException(cancellationToken);
            }
            await work.ConfigureAwait(false);
        }
    }
}
