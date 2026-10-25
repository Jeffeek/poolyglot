package io.github.jeffeek.connpool;

/**
 * Pool configuration: one immutable, validated object. Use {@link #builder()} /
 * {@link #toBuilder()} for construction and copy-with-override; both re-validate,
 * the mirror of Python's dataclass {@code __post_init__}.
 *
 * <p>Timeouts are seconds; a {@code null} {@link Double}/{@link Integer} means "no limit".
 */
public record PoolConfig(
    // --- sizing ---
    int minSize,                 // kept warm by open() and the maintenance pass
    int maxSize,                 // hard cap (idle + in use + being created)
    // --- timeouts (seconds; null = no limit) ---
    Double acquireTimeout,       // default wait in acquire()
    Double createTimeout,        // enforced by AsyncConnectionPool only; sync factories enforce their own
    Integer maxWaiters,          // backpressure: cap callers blocked in acquire() (null = unlimited; 0 = never wait)
    // --- rotation / recycling ---
    Double maxLifetime,          // retire connections after this age
    double lifetimeJitter,       // shave up to this fraction off each lifetime
    Double maxIdleTime,          // evict idle connections (down to minSize)
    Integer maxUses,             // retire after N checkouts
    IdleStrategy idleStrategy,
    // --- health ---
    Double validateAfterIdle,    // validate on checkout if idle >= this (0 = always, null = never)
    Double maintenanceInterval,  // background sweep period (null = off)
    Double leakDetectionTimeout, // warn when held longer than this (null = off); needs open()
    boolean validateIdle,        // validate idle connections during the maintenance sweep (needs open())
    // --- resilience ---
    RetryPolicy retry,
    BreakerConfig breaker        // null = off
) {
    public PoolConfig {
        need(maxSize >= 1, "max_size must be >= 1");
        need(minSize >= 0 && minSize <= maxSize, "min_size must be within [0, max_size]");
        need(acquireTimeout == null || acquireTimeout >= 0, "acquire_timeout must be >= 0");
        need(createTimeout == null || createTimeout > 0, "create_timeout must be > 0 or None");
        need(maxLifetime == null || maxLifetime > 0, "max_lifetime must be > 0 or None");
        need(maxIdleTime == null || maxIdleTime > 0, "max_idle_time must be > 0 or None");
        need(maintenanceInterval == null || maintenanceInterval > 0, "maintenance_interval must be > 0 or None");
        need(leakDetectionTimeout == null || leakDetectionTimeout > 0, "leak_detection_timeout must be > 0 or None");
        need(validateAfterIdle == null || validateAfterIdle >= 0, "validate_after_idle must be >= 0 or None");
        need(lifetimeJitter >= 0.0 && lifetimeJitter <= 1.0, "lifetime_jitter must be within [0, 1]");
        need(maxUses == null || maxUses >= 1, "max_uses must be >= 1 or None");
        need(maxWaiters == null || maxWaiters >= 0, "max_waiters must be >= 0 or None");
    }

    private static void need(boolean ok, String message) {
        if (!ok) {
            throw new PoolConfigError(message);
        }
    }

    /** The default configuration. */
    public static PoolConfig defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder()
            .minSize(minSize)
            .maxSize(maxSize)
            .acquireTimeout(acquireTimeout)
            .createTimeout(createTimeout)
            .maxWaiters(maxWaiters)
            .maxLifetime(maxLifetime)
            .lifetimeJitter(lifetimeJitter)
            .maxIdleTime(maxIdleTime)
            .maxUses(maxUses)
            .idleStrategy(idleStrategy)
            .validateAfterIdle(validateAfterIdle)
            .maintenanceInterval(maintenanceInterval)
            .leakDetectionTimeout(leakDetectionTimeout)
            .validateIdle(validateIdle)
            .retry(retry)
            .breaker(breaker);
    }

    public static final class Builder {
        private int minSize = 0;
        private int maxSize = 10;
        private Double acquireTimeout = 30.0;
        private Double createTimeout = 10.0;
        private Integer maxWaiters = null;
        private Double maxLifetime = 1800.0;
        private double lifetimeJitter = 0.1;
        private Double maxIdleTime = 600.0;
        private Integer maxUses = null;
        private IdleStrategy idleStrategy = IdleStrategy.LIFO;
        private Double validateAfterIdle = 30.0;
        private Double maintenanceInterval = 30.0;
        private Double leakDetectionTimeout = null;
        private boolean validateIdle = false;
        private RetryPolicy retry = RetryPolicy.defaults();
        private BreakerConfig breaker = BreakerConfig.defaults();

        public Builder minSize(int value) {
            this.minSize = value;
            return this;
        }

        public Builder maxSize(int value) {
            this.maxSize = value;
            return this;
        }

        public Builder acquireTimeout(Double value) {
            this.acquireTimeout = value;
            return this;
        }

        public Builder createTimeout(Double value) {
            this.createTimeout = value;
            return this;
        }

        public Builder maxWaiters(Integer value) {
            this.maxWaiters = value;
            return this;
        }

        public Builder maxLifetime(Double value) {
            this.maxLifetime = value;
            return this;
        }

        public Builder lifetimeJitter(double value) {
            this.lifetimeJitter = value;
            return this;
        }

        public Builder maxIdleTime(Double value) {
            this.maxIdleTime = value;
            return this;
        }

        public Builder maxUses(Integer value) {
            this.maxUses = value;
            return this;
        }

        public Builder idleStrategy(IdleStrategy value) {
            this.idleStrategy = value;
            return this;
        }

        public Builder validateAfterIdle(Double value) {
            this.validateAfterIdle = value;
            return this;
        }

        public Builder maintenanceInterval(Double value) {
            this.maintenanceInterval = value;
            return this;
        }

        public Builder leakDetectionTimeout(Double value) {
            this.leakDetectionTimeout = value;
            return this;
        }

        public Builder validateIdle(boolean value) {
            this.validateIdle = value;
            return this;
        }

        public Builder retry(RetryPolicy value) {
            this.retry = value;
            return this;
        }

        public Builder breaker(BreakerConfig value) {
            this.breaker = value;
            return this;
        }

        public PoolConfig build() {
            return new PoolConfig(minSize, maxSize, acquireTimeout, createTimeout, maxWaiters,
                maxLifetime, lifetimeJitter, maxIdleTime, maxUses, idleStrategy, validateAfterIdle,
                maintenanceInterval, leakDetectionTimeout, validateIdle, retry, breaker);
        }
    }
}
