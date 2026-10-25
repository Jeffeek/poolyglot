package connpool

import (
	"errors"
	"testing"
)

func TestConfig_DefaultsAreValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig() should validate, got %v", err)
	}
	if cfg.MinSize != 0 {
		t.Errorf("MinSize = %d, want 0", cfg.MinSize)
	}
	if cfg.MaxSize != 10 {
		t.Errorf("MaxSize = %d, want 10", cfg.MaxSize)
	}
	if cfg.AcquireTimeout == nil || *cfg.AcquireTimeout != 30.0 {
		t.Errorf("AcquireTimeout = %v, want 30.0", cfg.AcquireTimeout)
	}
	if cfg.CreateTimeout == nil || *cfg.CreateTimeout != 10.0 {
		t.Errorf("CreateTimeout = %v, want 10.0", cfg.CreateTimeout)
	}
	if cfg.MaxLifetime == nil || *cfg.MaxLifetime != 1800.0 {
		t.Errorf("MaxLifetime = %v, want 1800.0", cfg.MaxLifetime)
	}
	if cfg.LifetimeJitter != 0.1 {
		t.Errorf("LifetimeJitter = %v, want 0.1", cfg.LifetimeJitter)
	}
	if cfg.MaxIdleTime == nil || *cfg.MaxIdleTime != 600.0 {
		t.Errorf("MaxIdleTime = %v, want 600.0", cfg.MaxIdleTime)
	}
	if cfg.ValidateAfterIdle == nil || *cfg.ValidateAfterIdle != 30.0 {
		t.Errorf("ValidateAfterIdle = %v, want 30.0", cfg.ValidateAfterIdle)
	}
	if cfg.MaintenanceInterval == nil || *cfg.MaintenanceInterval != 30.0 {
		t.Errorf("MaintenanceInterval = %v, want 30.0", cfg.MaintenanceInterval)
	}
	if cfg.IdleStrategy != LIFO {
		t.Errorf("IdleStrategy = %v, want LIFO", cfg.IdleStrategy)
	}
}

func TestConfig_RejectsBadValues(t *testing.T) {
	cases := map[string]func(*PoolConfig){
		"max_size=0":                func(c *PoolConfig) { c.MaxSize = 0 },
		"min_size=11>max":           func(c *PoolConfig) { c.MinSize = 11 },
		"min_size=-1":               func(c *PoolConfig) { c.MinSize = -1 },
		"acquire_timeout=-1":        func(c *PoolConfig) { c.AcquireTimeout = Ptr(-1.0) },
		"max_lifetime=0":            func(c *PoolConfig) { c.MaxLifetime = Ptr(0.0) },
		"lifetime_jitter=1.5":       func(c *PoolConfig) { c.LifetimeJitter = 1.5 },
		"lifetime_jitter=-0.5":      func(c *PoolConfig) { c.LifetimeJitter = -0.5 },
		"max_uses=0":                func(c *PoolConfig) { c.MaxUses = Ptr(0) },
		"validate_after_idle=-1":    func(c *PoolConfig) { c.ValidateAfterIdle = Ptr(-1.0) },
		"maintenance_interval=0":    func(c *PoolConfig) { c.MaintenanceInterval = Ptr(0.0) },
		"leak_detection_timeout=0":  func(c *PoolConfig) { c.LeakDetectionTimeout = Ptr(0.0) },
		"leak_detection_timeout=-5": func(c *PoolConfig) { c.LeakDetectionTimeout = Ptr(-5.0) },
		"create_timeout=0":          func(c *PoolConfig) { c.CreateTimeout = Ptr(0.0) },
		"max_waiters=-1":            func(c *PoolConfig) { c.MaxWaiters = Ptr(-1) },
		"max_idle_time=0":           func(c *PoolConfig) { c.MaxIdleTime = Ptr(0.0) },
		"min_size=5>max_size=3": func(c *PoolConfig) {
			c.MinSize = 5
			c.MaxSize = 3
		},
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected PoolConfigError for %s, got nil", name)
			}
			var target *PoolConfigError
			if !errors.As(err, &target) {
				t.Fatalf("expected *PoolConfigError, got %T (%v)", err, err)
			}
		})
	}
}

func TestConfig_AcceptsBoundaryValues(t *testing.T) {
	cases := map[string]func(*PoolConfig){
		"max_size=1": func(c *PoolConfig) { c.MaxSize = 1 },
		"min_size=0": func(c *PoolConfig) { c.MinSize = 0 },
		"max_size=3,min_size=3": func(c *PoolConfig) {
			c.MaxSize = 3
			c.MinSize = 3
		},
		"acquire_timeout=0":            func(c *PoolConfig) { c.AcquireTimeout = Ptr(0.0) },
		"acquire_timeout=nil":          func(c *PoolConfig) { c.AcquireTimeout = nil },
		"lifetime_jitter=0":            func(c *PoolConfig) { c.LifetimeJitter = 0 },
		"lifetime_jitter=1":            func(c *PoolConfig) { c.LifetimeJitter = 1 },
		"max_uses=1":                   func(c *PoolConfig) { c.MaxUses = Ptr(1) },
		"max_uses=nil":                 func(c *PoolConfig) { c.MaxUses = nil },
		"max_waiters=0":                func(c *PoolConfig) { c.MaxWaiters = Ptr(0) },
		"max_waiters=nil":              func(c *PoolConfig) { c.MaxWaiters = nil },
		"leak_detection_timeout=nil":   func(c *PoolConfig) { c.LeakDetectionTimeout = nil },
		"validate_after_idle=0":        func(c *PoolConfig) { c.ValidateAfterIdle = Ptr(0.0) },
		"create_timeout=0.001":         func(c *PoolConfig) { c.CreateTimeout = Ptr(0.001) },
		"max_lifetime=0.001":           func(c *PoolConfig) { c.MaxLifetime = Ptr(0.001) },
		"max_idle_time=0.001":          func(c *PoolConfig) { c.MaxIdleTime = Ptr(0.001) },
		"maintenance_interval=0.001":   func(c *PoolConfig) { c.MaintenanceInterval = Ptr(0.001) },
		"leak_detection_timeout=0.001": func(c *PoolConfig) { c.LeakDetectionTimeout = Ptr(0.001) },
		"create_timeout=nil":           func(c *PoolConfig) { c.CreateTimeout = nil },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			mutate(&cfg)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("expected %s to be valid, got %v", name, err)
			}
		})
	}
}

func TestConfig_IdleStrategyWireValues(t *testing.T) {
	if string(LIFO) != "lifo" {
		t.Errorf("LIFO = %q, want %q", LIFO, "lifo")
	}
	if string(FIFO) != "fifo" {
		t.Errorf("FIFO = %q, want %q", FIFO, "fifo")
	}
}

// TestConfig_CopyReturnsValidatedCopy mirrors Python's PoolConfig.replace()/Java's
// toBuilder(): Go's PoolConfig is a plain value type, so copying the struct and mutating
// the copy is the idiomatic equivalent. The original must be left untouched, and the
// mutated copy must still go through Validate().
func TestConfig_CopyReturnsValidatedCopy(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxSize = 5

	copy := cfg
	copy.MaxSize = 8
	if copy.MaxSize != 8 {
		t.Errorf("copy.MaxSize = %d, want 8", copy.MaxSize)
	}
	if cfg.MaxSize != 5 {
		t.Errorf("original cfg.MaxSize = %d, want 5 (unchanged)", cfg.MaxSize)
	}

	bad := cfg
	bad.MinSize = 99
	if err := bad.Validate(); err == nil {
		t.Fatal("expected PoolConfigError for min_size=99 > max_size")
	}
}

// TestConfig_NewConnectionPoolRejectsInvalidConfig exercises the config validation seam
// actually used by pool construction (Go has no config-owning constructor besides
// NewConnectionPool itself).
func TestConfig_NewConnectionPoolRejectsInvalidConfig(t *testing.T) {
	factory := &FakeFactory{}
	cfg := DefaultConfig()
	cfg.MaxSize = 0

	_, err := NewConnectionPool[*FakeConn](factory, cfg, nil)
	if err == nil {
		t.Fatal("expected error constructing pool with invalid config")
	}
	var target *PoolConfigError
	if !errors.As(err, &target) {
		t.Fatalf("expected *PoolConfigError, got %T (%v)", err, err)
	}
}
