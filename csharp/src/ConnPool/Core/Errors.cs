// Exception hierarchy. Catch PoolError to handle anything the pool raises.
using System;

namespace ConnPool
{
    /// <summary>Base class for every error raised by ConnPool.</summary>
    public class PoolError : Exception
    {
        public PoolError(string? message = null, Exception? innerException = null)
            : base(message, innerException)
        {
        }
    }

    /// <summary>Invalid pool / breaker / retry configuration.</summary>
    public sealed class PoolConfigError : PoolError
    {
        public PoolConfigError(string message) : base(message)
        {
        }
    }

    /// <summary>The pool has been closed; no more connections can be acquired.</summary>
    public sealed class PoolClosedError : PoolError
    {
        public PoolClosedError(string message) : base(message)
        {
        }
    }

    /// <summary>No connection became available before the acquire deadline.</summary>
    public sealed class PoolTimeoutError : PoolError
    {
        public PoolTimeoutError(string message) : base(message)
        {
        }
    }

    /// <summary>The pool is full and the waiter queue is at MaxWaiters; shedding load.</summary>
    public sealed class PoolExhaustedError : PoolError
    {
        public PoolExhaustedError(string message) : base(message)
        {
        }
    }

    /// <summary>
    /// The factory failed to create a connection (after all retries).
    /// The original exception is available as <see cref="Exception.InnerException"/>.
    /// </summary>
    public sealed class ConnectionCreateError : PoolError
    {
        public ConnectionCreateError(string message, Exception? innerException = null)
            : base(message, innerException)
        {
        }
    }

    /// <summary>The circuit breaker is rejecting calls to protect a failing backend.</summary>
    public sealed class CircuitOpenError : PoolError
    {
        public double RetryAfter { get; }

        public CircuitOpenError(double retryAfter)
            : base($"circuit breaker is open; retry in ~{retryAfter:0.0}s") => RetryAfter = retryAfter;
    }

    /// <summary>Released an object that is not checked out from this pool (double release?).</summary>
    public sealed class InvalidReleaseError : PoolError
    {
        public InvalidReleaseError(string message) : base(message)
        {
        }
    }
}
