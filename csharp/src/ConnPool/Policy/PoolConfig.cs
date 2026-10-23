// Pool configuration: one immutable, validated object.
//
// Each property validates itself in its `init` accessor, so both construction
// (`new PoolConfig { ... }`) and copy-and-modify (`cfg with { MaxSize = 8 }`)
// reject invalid values - the mirror of Python's dataclass __post_init__.
namespace ConnPool
{
    public enum IdleStrategy
    {
        Lifo, // reuse the hottest connection; cold ones age out and get evicted
        Fifo, // reuse the oldest idle connection; spreads load / rotates evenly
    }

    public static class IdleStrategyExtensions
    {
        public static string Value(this IdleStrategy strategy) =>
            strategy == IdleStrategy.Lifo ? "lifo" : "fifo";
    }

    public sealed record PoolConfig
    {
        private readonly int _minSize = 0;
        private int _maxSize = 10;
        private readonly double? _acquireTimeout = 30.0;
        private readonly double? _createTimeout = 10.0;
        private readonly int? _maxWaiters = null;
        private readonly double? _maxLifetime = 1800.0;
        private readonly double _lifetimeJitter = 0.1;
        private readonly double? _maxIdleTime = 600.0;
        private readonly int? _maxUses = null;
        private readonly double? _validateAfterIdle = 30.0;
        private readonly double? _maintenanceInterval = 30.0;
        private readonly double? _leakDetectionTimeout = null;

        // --- sizing ---

        /// <summary>Connections kept warm by Open() and the maintenance pass.</summary>
        public int MinSize
        {
            get => _minSize;
            init
            {
                if (value < 0 || value > _maxSize)
                    throw new PoolConfigError("min_size must be within [0, max_size]");
                _minSize = value;
            }
        }

        /// <summary>Hard cap (idle + in use + being created).</summary>
        public int MaxSize
        {
            get => _maxSize;
            init
            {
                if (value < 1)
                    throw new PoolConfigError("max_size must be >= 1");
                if (_minSize > value)
                    throw new PoolConfigError("min_size must be within [0, max_size]");
                _maxSize = value;
            }
        }

        // --- timeouts (seconds; null = no limit) ---

        /// <summary>Default wait in Acquire().</summary>
        public double? AcquireTimeout
        {
            get => _acquireTimeout;
            init
            {
                if (value is < 0)
                    throw new PoolConfigError("acquire_timeout must be >= 0");
                _acquireTimeout = value;
            }
        }

        /// <summary>Enforced by AsyncConnectionPool only; sync factories enforce their own.</summary>
        public double? CreateTimeout
        {
            get => _createTimeout;
            init
            {
                if (value is <= 0)
                    throw new PoolConfigError("create_timeout must be > 0 or None");
                _createTimeout = value;
            }
        }

        /// <summary>Backpressure: cap callers blocked in Acquire() (null = unlimited; 0 = never wait when full).</summary>
        public int? MaxWaiters
        {
            get => _maxWaiters;
            init
            {
                if (value is < 0)
                    throw new PoolConfigError("max_waiters must be >= 0 or None");
                _maxWaiters = value;
            }
        }

        // --- rotation / recycling ---

        /// <summary>Retire connections after this age.</summary>
        public double? MaxLifetime
        {
            get => _maxLifetime;
            init
            {
                if (value is <= 0)
                    throw new PoolConfigError("max_lifetime must be > 0 or None");
                _maxLifetime = value;
            }
        }

        /// <summary>Shave up to this fraction off each lifetime so a whole pool never expires at once.</summary>
        public double LifetimeJitter
        {
            get => _lifetimeJitter;
            init
            {
                if (value < 0.0 || value > 1.0)
                    throw new PoolConfigError("lifetime_jitter must be within [0, 1]");
                _lifetimeJitter = value;
            }
        }

        /// <summary>Evict idle connections (down to MinSize).</summary>
        public double? MaxIdleTime
        {
            get => _maxIdleTime;
            init
            {
                if (value is <= 0)
                    throw new PoolConfigError("max_idle_time must be > 0 or None");
                _maxIdleTime = value;
            }
        }

        /// <summary>Retire after N checkouts.</summary>
        public int? MaxUses
        {
            get => _maxUses;
            init
            {
                if (value is < 1)
                    throw new PoolConfigError("max_uses must be >= 1 or None");
                _maxUses = value;
            }
        }

        public IdleStrategy IdleStrategy { get; init; } = IdleStrategy.Lifo;

        // --- health ---

        /// <summary>Validate on checkout if idle >= this (0 = always, null = never).</summary>
        public double? ValidateAfterIdle
        {
            get => _validateAfterIdle;
            init
            {
                if (value is < 0)
                    throw new PoolConfigError("validate_after_idle must be >= 0 or None");
                _validateAfterIdle = value;
            }
        }

        /// <summary>Background sweep period (null = off).</summary>
        public double? MaintenanceInterval
        {
            get => _maintenanceInterval;
            init
            {
                if (value is <= 0)
                    throw new PoolConfigError("maintenance_interval must be > 0 or None");
                _maintenanceInterval = value;
            }
        }

        /// <summary>Warn when a borrowed connection is held longer than this (null = off); needs Open().</summary>
        public double? LeakDetectionTimeout
        {
            get => _leakDetectionTimeout;
            init
            {
                if (value is <= 0)
                    throw new PoolConfigError("leak_detection_timeout must be > 0 or None");
                _leakDetectionTimeout = value;
            }
        }

        /// <summary>Validate idle connections during the maintenance sweep and evict dead ones (needs Open(); adds I/O).</summary>
        public bool ValidateIdle { get; init; } = false;

        // --- resilience ---

        public RetryPolicy Retry { get; init; } = new RetryPolicy();

        /// <summary>Circuit breaker config; null = off.</summary>
        public BreakerConfig? Breaker { get; init; } = new BreakerConfig();
    }
}
