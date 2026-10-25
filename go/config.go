package connpool

// IdleStrategy selects which idle connection is reused first.
type IdleStrategy string

const (
	// LIFO reuses the hottest connection; cold ones age out and get evicted.
	LIFO IdleStrategy = "lifo"
	// FIFO reuses the oldest idle connection; spreads load / rotates evenly.
	FIFO IdleStrategy = "fifo"
)

// PoolConfig is an immutable, validated pool configuration.
type PoolConfig struct {
	// --- sizing ---
	MinSize int // kept warm by open() and the maintenance pass
	MaxSize int // hard cap (idle + in use + being created)

	// --- timeouts (seconds; nil = no limit) ---
	AcquireTimeout *float64 // default wait in Acquire()
	CreateTimeout  *float64 // enforced by AsyncConnectionPool only; sync factories must enforce their own connect timeout
	MaxWaiters     *int     // backpressure: cap callers blocked in Acquire() (nil = unlimited; 0 = never wait when full)

	// --- rotation / recycling ---
	MaxLifetime    *float64     // retire connections after this age
	LifetimeJitter float64      // shave up to 10% off each lifetime so a whole pool never expires at the same instant
	MaxIdleTime    *float64     // evict idle connections (down to MinSize)
	MaxUses        *int         // retire after N checkouts
	IdleStrategy   IdleStrategy // LIFO or FIFO

	// --- health ---
	ValidateAfterIdle    *float64 // validate on checkout if idle >= this (0 = always, nil = never)
	MaintenanceInterval  *float64 // background sweep period (nil = off)
	LeakDetectionTimeout *float64 // warn when a borrowed connection is held longer than this (nil = off); needs Open()
	ValidateIdle         bool     // validate idle connections during the maintenance sweep and evict dead ones (needs Open(); adds I/O)

	// --- resilience ---
	Retry   RetryPolicy
	Breaker *BreakerConfig // nil = off
}

// DefaultConfig returns the default PoolConfig.
func DefaultConfig() PoolConfig {
	breaker := DefaultBreakerConfig()
	return PoolConfig{
		MinSize:              0,
		MaxSize:              10,
		AcquireTimeout:       Ptr(30.0),
		CreateTimeout:        Ptr(10.0),
		MaxWaiters:           nil,
		MaxLifetime:          Ptr(1800.0),
		LifetimeJitter:       0.1,
		MaxIdleTime:          Ptr(600.0),
		MaxUses:              nil,
		IdleStrategy:         LIFO,
		ValidateAfterIdle:    Ptr(30.0),
		MaintenanceInterval:  Ptr(30.0),
		LeakDetectionTimeout: nil,
		ValidateIdle:         false,
		Retry:                DefaultRetryPolicy(),
		Breaker:              &breaker,
	}
}

// Validate checks the config's invariants, returning a *PoolConfigError on the first violation.
func (c PoolConfig) Validate() error {
	need := func(ok bool, msg string) error {
		if !ok {
			return &PoolConfigError{Msg: msg}
		}
		return nil
	}

	if err := need(c.MaxSize >= 1, "max_size must be >= 1"); err != nil {
		return err
	}
	if err := need(c.MinSize >= 0 && c.MinSize <= c.MaxSize, "min_size must be within [0, max_size]"); err != nil {
		return err
	}
	if err := need(c.AcquireTimeout == nil || *c.AcquireTimeout >= 0, "acquire_timeout must be >= 0"); err != nil {
		return err
	}
	for _, nv := range []struct {
		value *float64
		name  string
	}{
		{c.CreateTimeout, "create_timeout"},
		{c.MaxLifetime, "max_lifetime"},
		{c.MaxIdleTime, "max_idle_time"},
		{c.MaintenanceInterval, "maintenance_interval"},
		{c.LeakDetectionTimeout, "leak_detection_timeout"},
	} {
		if err := need(nv.value == nil || *nv.value > 0, nv.name+" must be > 0 or None"); err != nil {
			return err
		}
	}
	if err := need(c.ValidateAfterIdle == nil || *c.ValidateAfterIdle >= 0, "validate_after_idle must be >= 0 or None"); err != nil {
		return err
	}
	if err := need(c.LifetimeJitter >= 0.0 && c.LifetimeJitter <= 1.0, "lifetime_jitter must be within [0, 1]"); err != nil {
		return err
	}
	if err := need(c.MaxUses == nil || *c.MaxUses >= 1, "max_uses must be >= 1 or None"); err != nil {
		return err
	}
	if err := need(c.MaxWaiters == nil || *c.MaxWaiters >= 0, "max_waiters must be >= 0 or None"); err != nil {
		return err
	}

	if err := c.Retry.Validate(); err != nil {
		return err
	}
	if c.Breaker != nil {
		if err := c.Breaker.Validate(); err != nil {
			return err
		}
	}

	return nil
}
