// Async connection pool. Same model and same rules (Lifecycle) as the sync pool.
//
// Differences worth knowing:
//   * CreateTimeout is enforced here (via a linked CancellationTokenSource); sync factories enforce their own.
//   * Listener hooks stay synchronous (keep them fast).
//   * Cancellation is explicit: pass a CancellationToken to cancel a pending acquire/release.
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ConnPool
{
    public class AsyncConnectionPool<T> : IAsyncDisposable where T : class
    {
        public string Name { get; }

        private readonly AsyncConnectionFactory<T> _factory;
        private readonly PoolConfig _cfg;
        private readonly IClock _clock;
        private readonly Random _rng;

        private readonly AsyncCondition _cond = new();
        private LinkedList<PooledConnection<T>> _idle = new();
        private readonly Dictionary<T, PooledConnection<T>> _checkedOut;
        private int _size;
        private int _waiters;
        private bool _closed;

        private readonly StatsCollector _stats = new();
        private readonly ListenerGroup _events;
        private readonly CircuitBreaker? _breaker;
        private readonly CancellationTokenSource _maintCts = new();
        private Task? _task;
        private bool _opened;

        public AsyncConnectionPool(
            AsyncConnectionFactory<T> factory,
            PoolConfig? config = null,
            IEnumerable<PoolListener>? listeners = null,
            IClock? clock = null,
            Random? rng = null,
            string name = "pool")
        {
            Name = name;
            _factory = factory;
            _cfg = config ?? new PoolConfig();
            _clock = clock ?? new SystemClock();
            _rng = new ThreadSafeRandom(rng ?? new Random());
            _checkedOut = new(ReferenceEqualityComparer<T>.Instance);

            var all = new List<PoolListener> { _stats };
            if (listeners != null)
                all.AddRange(listeners);
            _events = new(all);
            _breaker = _cfg.Breaker != null
                ? new CircuitBreaker(_cfg.Breaker, _clock,
                    (o, n) => _events.Dispatch(l => l.OnBreakerStateChange(o, n)))
                : null;
        }

        public static AsyncConnectionPool<T> FromCallables(
            Func<CancellationToken, Task<T>> create,
            Func<T, CancellationToken, Task>? close = null,
            Func<T, CancellationToken, Task<bool>>? validate = null,
            Func<T, CancellationToken, Task>? reset = null,
            PoolConfig? config = null,
            IEnumerable<PoolListener>? listeners = null,
            IClock? clock = null,
            Random? rng = null,
            string name = "pool") =>
            new(new AsyncCallableFactory<T>(create, close, validate, reset), config, listeners, clock, rng, name);

        // ------------------------------------------------------------------ lifecycle
        public async Task<AsyncConnectionPool<T>> OpenAsync()
        {
            if (_closed)
                throw new PoolClosedError($"pool '{Name}' is closed");
            if (_opened)
                return this;
            _opened = true;
            await FillToMinAsync().ConfigureAwait(false);
            // FillToMin awaits, so Close() may have run meanwhile. Start the maintenance task only
            // while still open, under the lock, so a completed Close() can never miss cancelling it.
            using (await _cond.LockAsync().ConfigureAwait(false))
            {
                if (_cfg.MaintenanceInterval != null && !_closed)
                    _task = MaintenanceLoopAsync(_maintCts.Token);
            }
            return this;
        }

        public async Task CloseAsync(double? timeout = null)
        {
            List<PooledConnection<T>> idle;
            using (await _cond.LockAsync().ConfigureAwait(false))
            {
                if (_closed)
                    return;
                _closed = true;
                idle = _idle.ToList();
                _idle.Clear();
                _cond.NotifyAll();
                if (timeout != null)
                {
                    var deadline = _clock.Monotonic() + timeout.Value;
                    while (_checkedOut.Count > 0)
                    {
                        var left = deadline - _clock.Monotonic();
                        if (left <= 0)
                            break;
                        await _cond.WaitAsync(left, CancellationToken.None).ConfigureAwait(false);
                    }
                }
            }
            _maintCts.Cancel();
            if (_task != null)
                await _task.ConfigureAwait(false); // the loop swallows its own errors, so this never faults
            _maintCts.Dispose();
            foreach (var pooled in idle)
                await Destroy(pooled, CloseReason.PoolClosed).ConfigureAwait(false);
        }

        public bool Closed => _closed;

        public async ValueTask DisposeAsync() => await CloseAsync().ConfigureAwait(false);

        // --------------------------------------------------------------------- usage
        public async Task<T> AcquireAsync(Timeout timeout = default, CancellationToken cancellationToken = default)
        {
            var started = _clock.Monotonic();
            var deadline = TimeoutMath.ComputeDeadline(timeout, _cfg.AcquireTimeout, started);
            PooledConnection<T> pooled;
            try
            {
                while (true)
                {
                    var taken = await TakeIdleOrReserveAsync(deadline, cancellationToken).ConfigureAwait(false);
                    if (taken is null)
                    {
                        var (created, createdIn) = await CreateAsync(deadline, cancellationToken).ConfigureAwait(false);
                        try
                        {
                            await RegisterNewAsync(created, createdIn, cancellationToken).ConfigureAwait(false);
                        }
                        catch (ConnectionCreateError)
                        {
                            throw;
                        }
                        catch (PoolClosedError)
                        {
                            throw;
                        }
                        catch (Exception)
                        {
                            // Cleanup disposal is not cancellable here; it always completes, then the
                            // original failure (e.g. cancellation while registering) propagates.
                            await Destroy(created, CloseReason.Error).ConfigureAwait(false);
                            throw;
                        }
                        pooled = created;
                        break;
                    }
                    CloseReason? reason;
                    try
                    {
                        reason = await BorrowReasonAsync(taken, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        await Destroy(taken, CloseReason.Error, cancellationToken).ConfigureAwait(false);
                        throw;
                    }
                    if (reason is null)
                    {
                        pooled = taken;
                        break;
                    }
                    await Destroy(taken, reason.Value, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (PoolTimeoutError)
            {
                _events.Dispatch(l => l.OnAcquireTimeout(_clock.Monotonic() - started));
                throw;
            }
            var now = _clock.Monotonic();
            pooled.MarkCheckedOut(now);
            _events.Dispatch(l => l.OnAcquired(pooled, now - started));
            return pooled.Raw;
        }

        public async Task ReleaseAsync(
            T conn, bool discard = false, Exception? error = null, CancellationToken cancellationToken = default)
        {
            PooledConnection<T>? pooled;
            using (await _cond.LockAsync().ConfigureAwait(false))
            {
                _checkedOut.Remove(conn, out pooled);
            }
            if (pooled is null)
                throw new InvalidReleaseError("object is not checked out from this pool (double release?)");

            var now = _clock.Monotonic();
            var hold = pooled.HoldTime(now);
            CloseReason? reason;
            try
            {
                reason = await ReleaseReasonAsync(pooled, conn, discard, error, now, cancellationToken).ConfigureAwait(false);
                if (reason is null && !await ParkAsync(pooled, now).ConfigureAwait(false))
                    reason = CloseReason.PoolClosed;
            }
            catch (Exception)
            {
                await Destroy(pooled, CloseReason.Error, cancellationToken).ConfigureAwait(false);
                throw;
            }
            if (reason != null)
                await Destroy(pooled, reason.Value, cancellationToken).ConfigureAwait(false);
            _events.Dispatch(l => l.OnReleased(pooled, hold));
        }

        public async Task UseAsync(Func<T, Task> body, Timeout timeout = default, CancellationToken cancellationToken = default)
        {
            var conn = await AcquireAsync(timeout, cancellationToken).ConfigureAwait(false);
            try
            {
                await body(conn).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await ReleaseAsync(conn, error: ex, cancellationToken: cancellationToken).ConfigureAwait(false);
                throw;
            }
            await ReleaseAsync(conn, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public async Task<TResult> UseAsync<TResult>(
            Func<T, Task<TResult>> body, Timeout timeout = default, CancellationToken cancellationToken = default)
        {
            var conn = await AcquireAsync(timeout, cancellationToken).ConfigureAwait(false);
            TResult result;
            try
            {
                result = await body(conn).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await ReleaseAsync(conn, error: ex, cancellationToken: cancellationToken).ConfigureAwait(false);
                throw;
            }
            await ReleaseAsync(conn, cancellationToken: cancellationToken).ConfigureAwait(false);
            return result;
        }

        public readonly struct AsyncLease : IAsyncDisposable
        {
            private readonly AsyncConnectionPool<T> _pool;
            public T Value { get; }
            internal AsyncLease(AsyncConnectionPool<T> pool, T value)
            {
                _pool = pool;
                Value = value;
            }
            public ValueTask DisposeAsync() => new(_pool.ReleaseAsync(Value));
        }

        public async Task<AsyncLease> ConnectionAsync(Timeout timeout = default, CancellationToken cancellationToken = default) =>
            new(this, await AcquireAsync(timeout, cancellationToken).ConfigureAwait(false));

        public PoolStats Stats()
        {
            int size, idle, inUse, waiting;
            using (_cond.LockSync())
            {
                size = _size;
                idle = _idle.Count;
                inUse = _checkedOut.Count;
                waiting = _waiters;
            }
            return _stats.Snapshot(Name, _cfg.MaxSize, size, idle, inUse, waiting, _breaker?.State.Value());
        }

        public virtual async Task RunMaintenanceAsync()
        {
            List<(PooledConnection<T>, CloseReason)> doomed;
            using (await _cond.LockAsync().ConfigureAwait(false))
            {
                if (_closed)
                    return;
                var (keep, swept) = Lifecycle.Sweep(_idle, _size, _cfg, _clock.Monotonic());
                doomed = swept;
                _idle = new(keep);
            }
            foreach (var (pooled, reason) in doomed)
                await Destroy(pooled, reason).ConfigureAwait(false);
            await HealthCheckIdleAsync().ConfigureAwait(false);
            await FillToMinAsync().ConfigureAwait(false);
            await CheckLeaksAsync().ConfigureAwait(false);
        }

        private async Task HealthCheckIdleAsync()
        {
            if (!_cfg.ValidateIdle)
                return;
            List<PooledConnection<T>> batch;
            using (await _cond.LockAsync().ConfigureAwait(false))
            {
                batch = _idle.ToList();
                _idle.Clear();
                foreach (var pooled in batch)
                    _checkedOut[pooled.Raw] = pooled;
            }
            foreach (var pooled in batch)
            {
                var healthy = await IsHealthyAsync(pooled.Raw, CancellationToken.None).ConfigureAwait(false);
                bool keep;
                using (await _cond.LockAsync().ConfigureAwait(false))
                {
                    _checkedOut.Remove(pooled.Raw);
                    keep = healthy && !_closed;
                    if (keep)
                    {
                        _idle.AddLast(pooled);
                        _cond.Notify();
                    }
                }
                if (!keep)
                    await Destroy(pooled, healthy ? CloseReason.PoolClosed : CloseReason.Unhealthy).ConfigureAwait(false);
            }
        }

        private async Task CheckLeaksAsync()
        {
            var timeout = _cfg.LeakDetectionTimeout;
            if (timeout is null)
                return;
            var now = _clock.Monotonic();
            var leaked = new List<(PooledConnection<T>, double)>();
            using (await _cond.LockAsync().ConfigureAwait(false))
            {
                foreach (var pooled in _checkedOut.Values)
                {
                    if (!pooled.LeakReported && pooled.HoldTime(now) >= timeout.Value)
                    {
                        pooled.LeakReported = true;
                        leaked.Add((pooled, pooled.HoldTime(now)));
                    }
                }
            }
            foreach (var (pooled, held) in leaked)
            {
                Trace.TraceWarning($"pool '{Name}': possible connection leak: {pooled} held for {held:0.0}s");
                _events.Dispatch(l => l.OnConnectionLeak(pooled, held));
            }
        }

        // ----------------------------------------------------------------- internals
        private async Task<PooledConnection<T>?> TakeIdleOrReserveAsync(double? deadline, CancellationToken cancellationToken)
        {
            using (await _cond.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                while (true)
                {
                    if (_closed)
                        throw new PoolClosedError($"pool '{Name}' is closed");
                    var pooled = Lifecycle.PopIdle(_idle, _cfg.IdleStrategy);
                    if (pooled != null)
                    {
                        _checkedOut[pooled.Raw] = pooled;
                        return pooled;
                    }
                    if (_size < _cfg.MaxSize)
                    {
                        _size += 1;
                        return null;
                    }
                    var left = TimeoutMath.Remaining(deadline, _clock.Monotonic());
                    if (left != null && left <= 0)
                        throw new PoolTimeoutError(
                            $"pool '{Name}': no connection available (max_size={_cfg.MaxSize})");
                    if (_cfg.MaxWaiters != null && _waiters >= _cfg.MaxWaiters)
                        throw new PoolExhaustedError(
                            $"pool '{Name}': waiter queue full (max_waiters={_cfg.MaxWaiters})");
                    _waiters += 1;
                    try
                    {
                        await _cond.WaitAsync(left, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        _waiters -= 1;
                    }
                }
            }
        }

        private async Task<CloseReason?> BorrowReasonAsync(PooledConnection<T> pooled, CancellationToken cancellationToken)
        {
            var now = _clock.Monotonic();
            var reason = Lifecycle.BorrowReason(pooled, _cfg, now);
            if (reason is null && Lifecycle.NeedsValidation(pooled, _cfg, now))
            {
                if (!await IsHealthyAsync(pooled.Raw, cancellationToken).ConfigureAwait(false))
                    return CloseReason.Unhealthy;
            }
            return reason;
        }

        private async Task<CloseReason?> ReleaseReasonAsync(
            PooledConnection<T> pooled, T conn, bool discard, Exception? error, double now, CancellationToken cancellationToken)
        {
            if (discard)
                return CloseReason.Discarded;
            if (error != null && (_factory.IsFatalError(error) || !await IsHealthyAsync(conn, cancellationToken).ConfigureAwait(false)))
                return CloseReason.Error;
            var reason = Lifecycle.RotationReason(pooled, _cfg, now);
            if (reason != null)
                return reason;
            try
            {
                await _factory.ResetAsync(conn, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Trace.TraceWarning($"pool '{Name}': reset failed; discarding {pooled}: {ex}");
                return CloseReason.ResetFailed;
            }
            return null;
        }

        private async Task<bool> IsHealthyAsync(T raw, CancellationToken cancellationToken)
        {
            try
            {
                return await _factory.ValidateAsync(raw, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private async Task<bool> ParkAsync(PooledConnection<T> pooled, double now)
        {
            using (await _cond.LockAsync().ConfigureAwait(false))
            {
                if (_closed)
                    return false;
                pooled.MarkReturned(now);
                _idle.AddLast(pooled);
                _cond.Notify();
                return true;
            }
        }

        private async Task<(PooledConnection<T>, double)> CreateAsync(double? deadline, CancellationToken cancellationToken)
        {
            var started = _clock.Monotonic();
            T raw;
            try
            {
                raw = await CreateRawAsync(deadline, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                await FreeSlotAsync().ConfigureAwait(false);
                throw;
            }
            var now = _clock.Monotonic();
            var pooled = Lifecycle.NewConnection(raw, _cfg, now, _rng);
            return (pooled, now - started);
        }

        // The create cancellation limit (seconds), plus whether the acquire *deadline* is the
        // binding constraint. The flag lets the caller tell "deadline reached" (PoolTimeoutError)
        // from "create timed out" (a fault) without re-reading the clock after the fact: the
        // CancellationTokenSource timer and the monotonic clock are independent, so a boundary
        // re-read of the remaining time can misclassify a genuine deadline timeout.
        private (double? Limit, bool DeadlineBinding) CreateLimit(double? deadline)
        {
            var rem = TimeoutMath.Remaining(deadline, _clock.Monotonic());
            var createTimeout = _cfg.CreateTimeout;
            var deadlineBinding = rem != null && (createTimeout == null || rem.Value <= createTimeout.Value);
            var limits = new List<double>();
            if (createTimeout != null)
                limits.Add(createTimeout.Value);
            if (rem != null)
                limits.Add(rem.Value);
            return (limits.Count == 0 ? (double?)null : Math.Max(0.0, limits.Min()), deadlineBinding);
        }

        private async Task RegisterNewAsync(PooledConnection<T> pooled, double createdIn, CancellationToken cancellationToken)
        {
            string action;
            using (await _cond.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                if (_closed)
                {
                    action = "closed";
                }
                else if (_checkedOut.ContainsKey(pooled.Raw) || _idle.Any(p => ReferenceEquals(p.Raw, pooled.Raw)))
                {
                    action = "duplicate";
                }
                else
                {
                    _checkedOut[pooled.Raw] = pooled;
                    action = "ok";
                }
            }
            if (action == "duplicate")
            {
                // Unlike the "closed" path below, FreeSlotAsync() takes no CancellationToken (its
                // lock-wait is deliberately uncancellable, mirroring the create-failure cleanup at
                // CreateAsync's catch above) - so a cancellation can never be redelivered between it
                // and the throw below. ConnectionCreateError is always what Acquire observes here,
                // so this path can't fall into the catch-all's second Destroy().
                await FreeSlotAsync().ConfigureAwait(false);
                throw new ConnectionCreateError(
                    $"pool '{Name}': factory.CreateAsync() returned an object the pool already manages; " +
                    "each CreateAsync() must return a distinct connection");
            }
            _events.Dispatch(l => l.OnConnectionCreated(pooled, createdIn));
            if (action == "closed")
            {
                // Same reasoning as the duplicate path above: Destroy() is already shielded so
                // disposal itself always completes, but a cancellation redelivered right after it
                // returns (and before the throw) must not replace PoolClosedError - that would
                // route to Acquire's catch-all and dispose this connection a second time.
                try
                {
                    await Destroy(pooled, CloseReason.PoolClosed, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                throw new PoolClosedError($"pool '{Name}' is closed");
            }
        }

        private async Task<T> CreateRawAsync(double? deadline, CancellationToken cancellationToken)
        {
            _breaker?.BeforeCall();
            T raw;
            try
            {
                raw = await CreateWithRetryAsync(deadline, cancellationToken).ConfigureAwait(false);
            }
            catch (ConnectionCreateError)
            {
                _breaker?.RecordFailure();
                throw;
            }
            catch (Exception)
            {
                _breaker?.RecordAbort();
                throw;
            }
            _breaker?.RecordSuccess();
            return raw;
        }

        private async Task<T> CreateWithRetryAsync(double? deadline, CancellationToken cancellationToken)
        {
            var retry = _cfg.Retry;
            var attempt = 0;
            while (true)
            {
                if (_closed)
                    throw new PoolClosedError($"pool '{Name}' is closed");
                attempt += 1;
                var (limit, deadlineBinding) = CreateLimit(deadline);
                try
                {
                    using var timeoutCts =
                        limit is null ? null : new CancellationTokenSource(TimeSpan.FromSeconds(limit.Value));
                    using var linked = timeoutCts is null
                        ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                        : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                    return await _factory.CreateAsync(linked.Token).ConfigureAwait(false);
                }
                catch (Exception exc)
                {
                    // External cancellation is caller impatience, not a backend fault: propagate it.
                    cancellationToken.ThrowIfCancellationRequested();
                    var left = TimeoutMath.Remaining(deadline, _clock.Monotonic());
                    var wasTimeout = exc is OperationCanceledException;
                    // A timeout whose binding limit was the acquire deadline is the deadline being
                    // reached, not a backend fault - surface it as PoolTimeoutError (breaker records
                    // an abort, not a failure). Decided by which limit bound this attempt, not by a
                    // clock re-read, so it can't flake on CTS-timer vs monotonic-clock skew.
                    if (wasTimeout && deadlineBinding)
                        throw new PoolTimeoutError(
                            $"pool '{Name}': acquire deadline reached while creating connection");
                    _events.Dispatch(l => l.OnCreateFailed(exc));
                    if (attempt >= retry.MaxAttempts || !retry.ShouldRetry(exc))
                        throw new ConnectionCreateError(
                            $"pool '{Name}': could not create connection after {attempt} attempt(s): {Describe(exc)}", exc);
                    var delay = retry.DelayFor(attempt, _rng);
                    if (left != null && delay >= left)
                        throw new PoolTimeoutError(
                            $"pool '{Name}': acquire deadline reached while retrying creation");
                    await _clock.AsyncSleepAsync(delay, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private static string Describe(Exception exc) =>
            string.IsNullOrEmpty(exc.Message) ? exc.GetType().Name : exc.Message;

        private async Task FreeSlotAsync()
        {
            using (await _cond.LockAsync().ConfigureAwait(false))
            {
                _size -= 1;
                _cond.Notify();
            }
        }

        /// <summary>
        /// Dispose of a connection exactly once. Shielded, so a cancellation that lands while
        /// factory.CloseAsync() is awaiting I/O can neither interrupt backend disposal, skip the
        /// slot decrement, nor drop the OnConnectionClosed event; the original exception still
        /// propagates to the caller unchanged.
        /// </summary>
        private Task Destroy(PooledConnection<T> pooled, CloseReason reason, CancellationToken cancellationToken = default)
        {
            var disposal = Dispose(pooled, reason);
            return AsyncUtil.AwaitShielded(disposal, cancellationToken);
        }

        private async Task Dispose(PooledConnection<T> pooled, CloseReason reason)
        {
            try
            {
                await _factory.CloseAsync(pooled.Raw, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Trace.TraceWarning($"pool '{Name}': error closing {pooled}: {ex}");
            }
            finally
            {
                // Sync-only bookkeeping (no await) - take the lock synchronously, mirroring the
                // sync pool's `lock (_cond)` in Destroy. Avoids an await inside a finally.
                using (_cond.LockSync())
                {
                    _checkedOut.Remove(pooled.Raw);
                    _size -= 1;
                    _cond.Notify();
                }
            }
            _events.Dispatch(l => l.OnConnectionClosed(pooled, reason));
        }

        internal virtual async Task FillToMinAsync()
        {
            while (true)
            {
                using (await _cond.LockAsync().ConfigureAwait(false))
                {
                    if (_closed || _size >= _cfg.MinSize)
                        return;
                    _size += 1;
                }
                PooledConnection<T> pooled;
                double createdIn;
                try
                {
                    (pooled, createdIn) = await CreateAsync(null, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Trace.TraceWarning($"pool '{Name}': warm-up failed: {ex.Message}");
                    return;
                }
                _events.Dispatch(l => l.OnConnectionCreated(pooled, createdIn));
                if (!await ParkAsync(pooled, _clock.Monotonic()).ConfigureAwait(false))
                    await Destroy(pooled, CloseReason.PoolClosed).ConfigureAwait(false);
            }
        }

        private async Task MaintenanceLoopAsync(CancellationToken cancellationToken)
        {
            var interval = _cfg.MaintenanceInterval!.Value;
            var intervalMs = (int)Math.Max(1, interval * 1000);
            while (true)
            {
                try
                {
                    await Task.Delay(intervalMs, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                try
                {
                    await RunMaintenanceAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Trace.TraceError($"pool '{Name}': maintenance pass failed: {ex}");
                }
            }
        }

        // test seams
        internal Task? MaintenanceTask => _task;

        internal AsyncCondition Cond => _cond;

        internal bool WarnIfLeaked()
        {
            if (!_closed && _size > 0)
            {
                Trace.TraceWarning($"pool '{Name}' was not closed; {_size} connection(s) may leak");
                return true;
            }
            return false;
        }

        public override string ToString()
        {
            var s = Stats();
            return $"<AsyncConnectionPool '{Name}' size={s.Size}/{s.MaxSize} idle={s.Idle} in_use={s.InUse}>";
        }

        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        ~AsyncConnectionPool() => WarnIfLeaked();
    }
}
