// Circuit breaker (thread-safe; the lock is never held across user callbacks).
//
//     CLOSED --(N consecutive failures)--> OPEN --(recovery_timeout)--> HALF_OPEN
//        ^                                   ^                              |
//        |                                   +------ any probe fails -------+
//        +------------ enough probes succeed ---------------------------------+
using System;

namespace ConnPool
{
    public enum BreakerState
    {
        Closed,
        Open,
        HalfOpen,
    }

    public static class BreakerStateExtensions
    {
        public static string Value(this BreakerState state) => state switch
        {
            BreakerState.Closed => "closed",
            BreakerState.Open => "open",
            _ => "half_open",
        };
    }

    public sealed record BreakerConfig
    {
        private readonly int _failureThreshold = 5;
        private readonly double _recoveryTimeout = 30.0;
        private readonly int _halfOpenMaxCalls = 1;
        private readonly int _successThreshold = 1;

        /// <summary>Consecutive failures that trip the breaker.</summary>
        public int FailureThreshold
        {
            get => _failureThreshold;
            init
            {
                if (value < 1)
                    throw new PoolConfigError("breaker thresholds must be >= 1");
                _failureThreshold = value;
            }
        }

        /// <summary>Seconds to stay OPEN before probing.</summary>
        public double RecoveryTimeout
        {
            get => _recoveryTimeout;
            init
            {
                if (value < 0)
                    throw new PoolConfigError("breaker.RecoveryTimeout must be >= 0");
                _recoveryTimeout = value;
            }
        }

        /// <summary>Concurrent probe calls allowed while HALF_OPEN.</summary>
        public int HalfOpenMaxCalls
        {
            get => _halfOpenMaxCalls;
            init
            {
                if (value < 1)
                    throw new PoolConfigError("breaker thresholds must be >= 1");
                _halfOpenMaxCalls = value;
            }
        }

        /// <summary>Probe successes needed to close again.</summary>
        public int SuccessThreshold
        {
            get => _successThreshold;
            init
            {
                if (value < 1)
                    throw new PoolConfigError("breaker thresholds must be >= 1");
                _successThreshold = value;
            }
        }
    }

    /// <summary>Usage: <c>BeforeCall()</c> -> do work -> <c>RecordSuccess()</c> / <c>RecordFailure()</c>.</summary>
    public sealed class CircuitBreaker
    {
        private readonly BreakerConfig _cfg;
        private readonly IClock _clock;
        private readonly Action<BreakerState, BreakerState>? _onChange;
        private readonly object _lock = new object();

        private BreakerState _state = BreakerState.Closed;
        private int _failures;
        private double _openedAt;
        private int _probesInflight;
        private int _probeSuccesses;

        public CircuitBreaker(
            BreakerConfig? config = null,
            IClock? clock = null,
            Action<BreakerState, BreakerState>? onStateChange = null)
        {
            _cfg = config ?? new BreakerConfig();
            _clock = clock ?? new SystemClock();
            _onChange = onStateChange;
        }

        public BreakerState State
        {
            get
            {
                (BreakerState, BreakerState)? transition;
                BreakerState state;
                lock (_lock)
                {
                    transition = Refresh();
                    state = _state;
                }
                Emit(transition);
                return state;
            }
        }

        /// <summary>Raise <see cref="CircuitOpenError"/> if the call must not be attempted.</summary>
        public void BeforeCall()
        {
            CircuitOpenError? error = null;
            (BreakerState, BreakerState)? transition;
            lock (_lock)
            {
                transition = Refresh();
                if (_state == BreakerState.Open)
                {
                    var wait = _cfg.RecoveryTimeout - (_clock.Monotonic() - _openedAt);
                    error = new CircuitOpenError(Math.Max(0.0, wait));
                }
                else if (_state == BreakerState.HalfOpen)
                {
                    if (_probesInflight >= _cfg.HalfOpenMaxCalls)
                        error = new CircuitOpenError(0.0);
                    else
                        _probesInflight += 1;
                }
            }
            Emit(transition);
            if (error != null)
                throw error;
        }

        public void RecordSuccess()
        {
            (BreakerState, BreakerState)? transition = null;
            lock (_lock)
            {
                if (_state == BreakerState.Closed)
                {
                    _failures = 0;
                }
                else if (_state == BreakerState.HalfOpen)
                {
                    _probesInflight = Math.Max(0, _probesInflight - 1);
                    _probeSuccesses += 1;
                    if (_probeSuccesses >= _cfg.SuccessThreshold)
                        transition = SetState(BreakerState.Closed);
                }
            }
            Emit(transition);
        }

        public void RecordFailure()
        {
            (BreakerState, BreakerState)? transition = null;
            lock (_lock)
            {
                if (_state == BreakerState.Closed)
                {
                    _failures += 1;
                    if (_failures >= _cfg.FailureThreshold)
                        transition = SetState(BreakerState.Open);
                }
                else if (_state == BreakerState.HalfOpen)
                {
                    transition = SetState(BreakerState.Open);
                }
                // OPEN: a late failure from a call started before tripping; ignore.
            }
            Emit(transition);
        }

        /// <summary>
        /// The call neither succeeded nor failed (e.g. cancelled or timed out before the
        /// backend answered). Release a half-open probe slot without counting it either way.
        /// </summary>
        public void RecordAbort()
        {
            lock (_lock)
            {
                if (_state == BreakerState.HalfOpen)
                    _probesInflight = Math.Max(0, _probesInflight - 1);
            }
        }

        /// <summary>Convenience wrapper for arbitrary callables.</summary>
        public TResult Call<TResult>(Func<TResult> fn)
        {
            BeforeCall();
            TResult result;
            try
            {
                result = fn();
            }
            catch
            {
                RecordFailure();
                throw;
            }
            RecordSuccess();
            return result;
        }

        public void Reset()
        {
            (BreakerState, BreakerState)? transition;
            lock (_lock)
            {
                transition = SetState(BreakerState.Closed);
            }
            Emit(transition);
        }

        // -- internals (call with the lock held) --------------------------------
        private (BreakerState, BreakerState)? Refresh()
        {
            if (_state == BreakerState.Open
                && _clock.Monotonic() - _openedAt >= _cfg.RecoveryTimeout)
            {
                return SetState(BreakerState.HalfOpen);
            }
            return null;
        }

        private (BreakerState, BreakerState)? SetState(BreakerState @new)
        {
            var old = _state;
            if (old == @new)
                return null;
            _state = @new;
            _failures = 0;
            _probesInflight = 0;
            _probeSuccesses = 0;
            if (@new == BreakerState.Open)
                _openedAt = _clock.Monotonic();
            return (old, @new);
        }

        private void Emit((BreakerState, BreakerState)? transition)
        {
            // Fired after the lock is released so callbacks may safely re-enter the breaker.
            if (transition != null && _onChange != null)
                _onChange(transition.Value.Item1, transition.Value.Item2);
        }
    }
}
