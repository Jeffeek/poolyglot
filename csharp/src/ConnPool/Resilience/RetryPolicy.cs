// Retry policy for connection creation: exponential backoff with jitter.
using System;

namespace ConnPool
{
    /// <summary>
    /// How to retry <c>factory.Create()</c>.
    /// <para>
    /// <see cref="Jitter"/> in [0, 1]: 0 = deterministic delays, 1 = "full jitter"
    /// (uniform in [0, delay]). Spreading retries avoids thundering herds.
    /// <see cref="RetryOn"/> narrows what is retryable (e.g. don't retry auth failures).
    /// </para>
    /// </summary>
    public sealed record RetryPolicy
    {
        private readonly int _maxAttempts = 3;
        private readonly double _baseDelay = 0.1;
        private readonly double _maxDelay = 5.0;
        private readonly double _multiplier = 2.0;
        private readonly double _jitter = 0.5;

        public int MaxAttempts
        {
            get => _maxAttempts;
            init
            {
                if (value < 1)
                    throw new PoolConfigError("retry.MaxAttempts must be >= 1");
                _maxAttempts = value;
            }
        }

        public double BaseDelay
        {
            get => _baseDelay;
            init
            {
                if (value < 0)
                    throw new PoolConfigError("retry delays must be >= 0 and multiplier >= 1");
                _baseDelay = value;
            }
        }

        public double MaxDelay
        {
            get => _maxDelay;
            init
            {
                if (value < 0)
                    throw new PoolConfigError("retry delays must be >= 0 and multiplier >= 1");
                _maxDelay = value;
            }
        }

        public double Multiplier
        {
            get => _multiplier;
            init
            {
                if (value < 1)
                    throw new PoolConfigError("retry delays must be >= 0 and multiplier >= 1");
                _multiplier = value;
            }
        }

        public double Jitter
        {
            get => _jitter;
            init
            {
                if (value < 0.0 || value > 1.0)
                    throw new PoolConfigError("retry.Jitter must be within [0, 1]");
                _jitter = value;
            }
        }

        /// <summary>Predicate narrowing what is retryable. Default: retry everything.</summary>
        public Func<Exception, bool> RetryOn { get; init; } = _ => true;

        public bool ShouldRetry(Exception exc) => RetryOn(exc);

        /// <summary>Delay before the next attempt, given how many attempts have failed so far.</summary>
        public double DelayFor(int failedAttempts, Random rng)
        {
            double ceiling = Math.Min(_maxDelay, _baseDelay * Math.Pow(_multiplier, failedAttempts - 1));
            return ceiling * (1.0 - _jitter + _jitter * rng.NextDouble());
        }

        /// <summary>A policy that never retries (one attempt only).</summary>
        public static readonly RetryPolicy NoRetry = new RetryPolicy { MaxAttempts = 1 };
    }
}
