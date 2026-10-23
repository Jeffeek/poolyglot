// Thread-safe synchronous connection pool.
//
// Design in one paragraph: a single Monitor guards a tiny amount of state (idle list,
// checked-out map, a slot counter). *All* I/O - create, validate, reset, close - happens
// outside that lock, so a slow backend never blocks unrelated callers. A "slot" is
// reserved under the lock *before* creating, which is what makes MaxSize a hard cap.
// The rules for when connections retire live in Lifecycle; this file is plumbing.
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace ConnPool
{
    public class ConnectionPool<T> : IDisposable where T : class
    {
        public string Name { get; }

        private readonly ConnectionFactory<T> _factory;
        private readonly PoolConfig _cfg;
        private readonly IClock _clock;
        private readonly Random _rng;

        private readonly object _cond = new();
        private LinkedList<PooledConnection<T>> _idle = new();
        private readonly Dictionary<T, PooledConnection<T>> _checkedOut;
        private int _size;      // idle + checked out + being created/closed
        private int _waiters;
        private bool _closed;

        private readonly StatsCollector _stats = new();
        private readonly ListenerGroup _events;
        private readonly CircuitBreaker? _breaker;
        private readonly ManualResetEventSlim _stop = new(false);
        private Thread? _thread;

        public ConnectionPool(
            ConnectionFactory<T> factory,
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

        /// <summary>Pool anything from plain delegates.</summary>
        public static ConnectionPool<T> FromCallables(
            Func<T> create,
            Action<T>? close = null,
            Func<T, bool>? validate = null,
            Action<T>? reset = null,
            PoolConfig? config = null,
            IEnumerable<PoolListener>? listeners = null,
            IClock? clock = null,
            Random? rng = null,
            string name = "pool") =>
            new(new CallableFactory<T>(create, close, validate, reset), config, listeners, clock, rng, name);

        // ------------------------------------------------------------------ lifecycle
        /// <summary>Warm up MinSize connections and start the background maintenance thread.
        /// Optional: without it the pool is purely lazy and does no background work.</summary>
        public ConnectionPool<T> Open()
        {
            lock (_cond)
            {
                if (_closed)
                    throw new PoolClosedError($"pool '{Name}' is closed");
                if (_thread != null)
                    return this;
                if (_cfg.MaintenanceInterval != null)
                    _thread = new Thread(MaintenanceLoop) { IsBackground = true, Name = $"connpool-{Name}" };
            }
            FillToMin();
            _thread?.Start();
            return this;
        }

        /// <summary>Close idle connections now; checked-out ones close on release. Idempotent.
        /// <paramref name="timeout"/> (seconds) drains gracefully: wait up to this long for in-use
        /// connections to return. null does not wait; 0 returns immediately.</summary>
        public void Close(double? timeout = null)
        {
            List<PooledConnection<T>> idle;
            lock (_cond)
            {
                if (_closed)
                    return;
                _closed = true;
                idle = _idle.ToList();
                _idle.Clear();
                Monitor.PulseAll(_cond);
                if (timeout != null)
                {
                    var deadline = _clock.Monotonic() + timeout.Value;
                    while (_checkedOut.Count > 0)
                    {
                        var left = deadline - _clock.Monotonic();
                        if (left <= 0)
                            break;
                        Monitor.Wait(_cond, TimeSpan.FromSeconds(left));
                    }
                }
            }
            _stop.Set();
            if (_thread != null && _thread != Thread.CurrentThread && _thread.IsAlive)
                _thread.Join(TimeSpan.FromSeconds(5));
            foreach (var pooled in idle)
                Destroy(pooled, CloseReason.PoolClosed);
        }

        public bool Closed => _closed;

        void IDisposable.Dispose() => Close();

        // --------------------------------------------------------------------- usage
        public T Acquire(Timeout timeout = default)
        {
            var started = _clock.Monotonic();
            var deadline = TimeoutMath.ComputeDeadline(timeout, _cfg.AcquireTimeout, started);
            PooledConnection<T> pooled;
            try
            {
                while (true)
                {
                    var taken = TakeIdleOrReserve(deadline);
                    if (taken is null) // slot reserved -> build a new one
                    {
                        var (created, createdIn) = Create(deadline);
                        RegisterNew(created, createdIn);
                        pooled = created;
                        break;
                    }
                    var reason = BorrowReason(taken);
                    if (reason is null)
                    {
                        pooled = taken;
                        break;
                    }
                    Destroy(taken, reason.Value); // stale/dead: drop it and try again
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

        /// <summary>Return a connection. <paramref name="discard"/> forces it to be closed instead of reused.</summary>
        public void Release(T conn, bool discard = false, Exception? error = null)
        {
            PooledConnection<T>? pooled;
            lock (_cond)
            {
                _checkedOut.Remove(conn, out pooled);
            }
            if (pooled is null)
                throw new InvalidReleaseError("object is not checked out from this pool (double release?)");

            var now = _clock.Monotonic();
            var hold = pooled.HoldTime(now);
            var reason = ReleaseReason(pooled, conn, discard, error, now);
            if (reason is null && !Park(pooled, now))
                reason = CloseReason.PoolClosed;
            if (reason != null)
                Destroy(pooled, reason.Value);
            _events.Dispatch(l => l.OnReleased(pooled, hold));
        }

        /// <summary>Borrow a connection for the duration of <paramref name="body"/>.
        /// If it throws, the connection is discarded when the factory says the error is fatal
        /// or when it fails validation; otherwise it is reused.</summary>
        public void Use(Action<T> body, Timeout timeout = default)
        {
            var conn = Acquire(timeout);
            try
            {
                body(conn);
            }
            catch (Exception ex)
            {
                Release(conn, error: ex);
                throw;
            }
            Release(conn);
        }

        public TResult Use<TResult>(Func<T, TResult> body, Timeout timeout = default)
        {
            var conn = Acquire(timeout);
            TResult result;
            try
            {
                result = body(conn);
            }
            catch (Exception ex)
            {
                Release(conn, error: ex);
                throw;
            }
            Release(conn);
            return result;
        }

        /// <summary>A borrowed connection released when disposed (success path; use <see cref="Use(Action{T}, Timeout)"/>
        /// for exception-aware discard).</summary>
        public readonly struct Lease : IDisposable
        {
            private readonly ConnectionPool<T> _pool;
            public T Value { get; }
            internal Lease(ConnectionPool<T> pool, T value)
            {
                _pool = pool;
                Value = value;
            }
            public void Dispose() => _pool.Release(Value);
        }

        public Lease Connection(Timeout timeout = default) => new(this, Acquire(timeout));

        public PoolStats Stats()
        {
            int size, idle, inUse, waiting;
            lock (_cond)
            {
                size = _size;
                idle = _idle.Count;
                inUse = _checkedOut.Count;
                waiting = _waiters;
            }
            return _stats.Snapshot(Name, _cfg.MaxSize, size, idle, inUse, waiting, _breaker?.State.Value());
        }

        /// <summary>One sweep: evict expired/over-idle connections, then refill to MinSize.</summary>
        public virtual void RunMaintenance()
        {
            List<(PooledConnection<T>, CloseReason)> doomed;
            lock (_cond)
            {
                if (_closed)
                    return;
                var (keep, swept) = Lifecycle.Sweep(_idle, _size, _cfg, _clock.Monotonic());
                doomed = swept;
                _idle = new(keep);
            }
            foreach (var (pooled, reason) in doomed)
                Destroy(pooled, reason);
            HealthCheckIdle();
            FillToMin();
            CheckLeaks();
        }

        private void HealthCheckIdle()
        {
            if (!_cfg.ValidateIdle)
                return;
            List<PooledConnection<T>> batch;
            lock (_cond)
            {
                batch = _idle.ToList();
                _idle.Clear();
                foreach (var pooled in batch)
                    _checkedOut[pooled.Raw] = pooled;
            }
            foreach (var pooled in batch)
            {
                var healthy = IsHealthy(pooled.Raw);
                bool keep;
                lock (_cond)
                {
                    _checkedOut.Remove(pooled.Raw);
                    keep = healthy && !_closed;
                    if (keep)
                    {
                        _idle.AddLast(pooled);
                        Monitor.Pulse(_cond);
                    }
                }
                if (!keep)
                    Destroy(pooled, healthy ? CloseReason.PoolClosed : CloseReason.Unhealthy);
            }
        }

        private void CheckLeaks()
        {
            var timeout = _cfg.LeakDetectionTimeout;
            if (timeout is null)
                return;
            var now = _clock.Monotonic();
            var leaked = new List<(PooledConnection<T>, double)>();
            lock (_cond)
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
        private PooledConnection<T>? TakeIdleOrReserve(double? deadline)
        {
            lock (_cond)
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
                        if (left is null)
                            Monitor.Wait(_cond);
                        else
                            Monitor.Wait(_cond, TimeSpan.FromSeconds(left.Value));
                    }
                    finally
                    {
                        _waiters -= 1;
                    }
                }
            }
        }

        private CloseReason? BorrowReason(PooledConnection<T> pooled)
        {
            var now = _clock.Monotonic();
            var reason = Lifecycle.BorrowReason(pooled, _cfg, now);
            if (reason is null && Lifecycle.NeedsValidation(pooled, _cfg, now))
            {
                if (!IsHealthy(pooled.Raw))
                    return CloseReason.Unhealthy;
            }
            return reason;
        }

        private CloseReason? ReleaseReason(
            PooledConnection<T> pooled, T conn, bool discard, Exception? error, double now)
        {
            if (discard)
                return CloseReason.Discarded;
            if (error != null && (_factory.IsFatalError(error) || !IsHealthy(conn)))
                return CloseReason.Error;
            var reason = Lifecycle.RotationReason(pooled, _cfg, now);
            if (reason != null)
                return reason;
            try
            {
                _factory.Reset(conn);
            }
            catch (Exception ex)
            {
                Trace.TraceWarning($"pool '{Name}': reset failed; discarding {pooled}: {ex}");
                return CloseReason.ResetFailed;
            }
            return null;
        }

        private bool IsHealthy(T raw)
        {
            try
            {
                return _factory.Validate(raw);
            }
            catch (Exception)
            {
                return false;
            }
        }

        private bool Park(PooledConnection<T> pooled, double now)
        {
            lock (_cond)
            {
                if (_closed)
                    return false;
                pooled.MarkReturned(now);
                _idle.AddLast(pooled);
                Monitor.Pulse(_cond);
                return true;
            }
        }

        private (PooledConnection<T>, double) Create(double? deadline)
        {
            var started = _clock.Monotonic();
            T raw;
            try
            {
                raw = CreateRaw(deadline);
            }
            catch
            {
                FreeSlot();
                throw;
            }
            var now = _clock.Monotonic();
            var pooled = Lifecycle.NewConnection(raw, _cfg, now, _rng);
            return (pooled, now - started);
        }

        private void RegisterNew(PooledConnection<T> pooled, double createdIn)
        {
            string action;
            lock (_cond)
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
                FreeSlot();
                throw new ConnectionCreateError(
                    $"pool '{Name}': factory.Create() returned an object the pool already manages; " +
                    "each Create() must return a distinct connection");
            }
            _events.Dispatch(l => l.OnConnectionCreated(pooled, createdIn));
            if (action == "closed")
            {
                Destroy(pooled, CloseReason.PoolClosed);
                throw new PoolClosedError($"pool '{Name}' is closed");
            }
        }

        private T CreateRaw(double? deadline)
        {
            _breaker?.BeforeCall();
            T raw;
            try
            {
                raw = CreateWithRetry(deadline);
            }
            catch (ConnectionCreateError)
            {
                _breaker?.RecordFailure();
                throw;
            }
            catch
            {
                _breaker?.RecordAbort();
                throw;
            }
            _breaker?.RecordSuccess();
            return raw;
        }

        private T CreateWithRetry(double? deadline)
        {
            var retry = _cfg.Retry;
            var attempt = 0;
            while (true)
            {
                if (_closed)
                    throw new PoolClosedError($"pool '{Name}' is closed");
                attempt += 1;
                try
                {
                    return _factory.Create();
                }
                catch (Exception exc)
                {
                    _events.Dispatch(l => l.OnCreateFailed(exc));
                    if (attempt >= retry.MaxAttempts || !retry.ShouldRetry(exc))
                        throw new ConnectionCreateError(
                            $"pool '{Name}': could not create connection after {attempt} attempt(s): {exc.Message}", exc);
                    var delay = retry.DelayFor(attempt, _rng);
                    var left = TimeoutMath.Remaining(deadline, _clock.Monotonic());
                    if (left != null && delay >= left)
                        throw new PoolTimeoutError(
                            $"pool '{Name}': acquire deadline reached while retrying creation");
                    _clock.Sleep(delay);
                }
            }
        }

        private void FreeSlot()
        {
            lock (_cond)
            {
                _size -= 1;
                Monitor.Pulse(_cond);
            }
        }

        private void Destroy(PooledConnection<T> pooled, CloseReason reason)
        {
            try
            {
                _factory.Close(pooled.Raw);
            }
            catch (Exception ex)
            {
                Trace.TraceWarning($"pool '{Name}': error closing {pooled}: {ex}");
            }
            finally
            {
                lock (_cond)
                {
                    _checkedOut.Remove(pooled.Raw);
                    _size -= 1;
                    Monitor.Pulse(_cond);
                }
            }
            _events.Dispatch(l => l.OnConnectionClosed(pooled, reason));
        }

        private void FillToMin()
        {
            while (true)
            {
                lock (_cond)
                {
                    if (_closed || _size >= _cfg.MinSize)
                        return;
                    _size += 1;
                }
                PooledConnection<T> pooled;
                double createdIn;
                try
                {
                    (pooled, createdIn) = Create(null);
                }
                catch (Exception ex)
                {
                    Trace.TraceWarning($"pool '{Name}': warm-up failed: {ex.Message}");
                    return;
                }
                _events.Dispatch(l => l.OnConnectionCreated(pooled, createdIn));
                if (!Park(pooled, _clock.Monotonic()))
                    Destroy(pooled, CloseReason.PoolClosed);
            }
        }

        internal void MaintenanceLoop()
        {
            var interval = _cfg.MaintenanceInterval!.Value;
            var intervalMs = (int)Math.Max(1, interval * 1000);
            while (!_stop.Wait(intervalMs))
            {
                try
                {
                    RunMaintenance();
                }
                catch (Exception ex)
                {
                    Trace.TraceError($"pool '{Name}': maintenance pass failed: {ex}");
                }
            }
        }

        // test seam: stop the maintenance loop started directly by a test
        internal void SignalStop() => _stop.Set();

        // test seam: a snapshot of the idle connections' raw values
        internal T[] IdleSnapshot()
        {
            lock (_cond)
            {
                return _idle.Select(p => p.Raw).ToArray();
            }
        }

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
            return $"<ConnectionPool '{Name}' size={s.Size}/{s.MaxSize} idle={s.Idle} in_use={s.InUse}>";
        }

        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        ~ConnectionPool() => WarnIfLeaked();
    }
}
