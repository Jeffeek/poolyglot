package connpool

import (
	"errors"
	"testing"
)

// breakerFixture bundles a breaker with the clock and recorded transitions, mirroring the
// python/java test helpers' `make`.
type breakerFixture struct {
	clock   *ManualClock
	changes []string
	b       *CircuitBreaker
}

func makeBreaker(cfg BreakerConfig) *breakerFixture {
	f := &breakerFixture{clock: NewManualClock(0)}
	f.b = NewCircuitBreaker(&cfg, f.clock, func(old, nw BreakerState) {
		f.changes = append(f.changes, nw.Value())
	})
	return f
}

func defaultBreakerCfg() BreakerConfig {
	return BreakerConfig{FailureThreshold: 3, RecoveryTimeout: 10, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
}

func (f *breakerFixture) fail(n int) {
	for i := 0; i < n; i++ {
		if err := f.b.BeforeCall(); err != nil {
			panic(err)
		}
		f.b.RecordFailure()
	}
}

func TestBreaker_OpensAfterConsecutiveFailures(t *testing.T) {
	f := makeBreaker(defaultBreakerCfg())
	f.fail(3)
	if f.b.State() != BreakerOpen {
		t.Fatalf("expected OPEN, got %v", f.b.State())
	}
	err := f.b.BeforeCall()
	var openErr *CircuitOpenError
	if !errors.As(err, &openErr) {
		t.Fatalf("expected CircuitOpenError, got %v", err)
	}
	if diff := openErr.RetryAfter - 10; diff > 0.01 || diff < -0.01 {
		t.Fatalf("expected retry_after ~10, got %v", openErr.RetryAfter)
	}
}

func TestBreaker_SuccessResetsFailureStreak(t *testing.T) {
	f := makeBreaker(defaultBreakerCfg())
	f.fail(2)
	if err := f.b.BeforeCall(); err != nil {
		t.Fatal(err)
	}
	f.b.RecordSuccess()
	f.fail(2)
	if f.b.State() != BreakerClosed {
		t.Fatalf("expected CLOSED, got %v", f.b.State())
	}
}

func TestBreaker_HalfOpenThenClosesOnSuccess(t *testing.T) {
	f := makeBreaker(defaultBreakerCfg())
	f.fail(3)
	f.clock.Advance(10)
	if f.b.State() != BreakerHalfOpen {
		t.Fatalf("expected HALF_OPEN, got %v", f.b.State())
	}
	if err := f.b.BeforeCall(); err != nil {
		t.Fatal(err)
	}
	f.b.RecordSuccess()
	if f.b.State() != BreakerClosed {
		t.Fatalf("expected CLOSED, got %v", f.b.State())
	}
	want := []string{"open", "half_open", "closed"}
	if len(f.changes) != len(want) {
		t.Fatalf("expected %v, got %v", want, f.changes)
	}
	for i, v := range want {
		if f.changes[i] != v {
			t.Fatalf("expected %v, got %v", want, f.changes)
		}
	}
}

func TestBreaker_HalfOpenFailureReopens(t *testing.T) {
	f := makeBreaker(defaultBreakerCfg())
	f.fail(3)
	f.clock.Advance(10)
	if err := f.b.BeforeCall(); err != nil {
		t.Fatal(err)
	}
	f.b.RecordFailure()
	if f.b.State() != BreakerOpen {
		t.Fatalf("expected OPEN, got %v", f.b.State())
	}
}

func TestBreaker_HalfOpenLimitsConcurrentProbes(t *testing.T) {
	cfg := defaultBreakerCfg()
	cfg.HalfOpenMaxCalls = 1
	f := makeBreaker(cfg)
	f.fail(3)
	f.clock.Advance(10)
	if err := f.b.BeforeCall(); err != nil {
		t.Fatal(err)
	}
	err := f.b.BeforeCall()
	var openErr *CircuitOpenError
	if !errors.As(err, &openErr) {
		t.Fatalf("expected CircuitOpenError, got %v", err)
	}
}

func TestBreaker_SuccessThreshold(t *testing.T) {
	cfg := defaultBreakerCfg()
	cfg.SuccessThreshold = 2
	cfg.HalfOpenMaxCalls = 2
	f := makeBreaker(cfg)
	f.fail(3)
	f.clock.Advance(10)
	if err := f.b.BeforeCall(); err != nil {
		t.Fatal(err)
	}
	f.b.RecordSuccess()
	if f.b.State() != BreakerHalfOpen {
		t.Fatalf("expected HALF_OPEN, got %v", f.b.State())
	}
	if err := f.b.BeforeCall(); err != nil {
		t.Fatal(err)
	}
	f.b.RecordSuccess()
	if f.b.State() != BreakerClosed {
		t.Fatalf("expected CLOSED, got %v", f.b.State())
	}
}

func TestBreaker_CallWrapperAndReset(t *testing.T) {
	cfg := defaultBreakerCfg()
	cfg.FailureThreshold = 1
	f := makeBreaker(cfg)
	wantErr := errors.New("boom")
	err := f.b.Call(func() error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped error, got %v", err)
	}
	if f.b.State() != BreakerOpen {
		t.Fatalf("expected OPEN, got %v", f.b.State())
	}
	f.b.Reset()
	result := 0
	if err := f.b.Call(func() error { result = 42; return nil }); err != nil {
		t.Fatal(err)
	}
	if result != 42 {
		t.Fatalf("expected 42, got %v", result)
	}
}

func TestBreaker_CallbackMayReenterBreaker(t *testing.T) {
	clock := NewManualClock(0)
	var b *CircuitBreaker
	cfg := BreakerConfig{FailureThreshold: 1, RecoveryTimeout: 10, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
	b = NewCircuitBreaker(&cfg, clock, func(old, nw BreakerState) {
		b.State() // would deadlock if the callback ran under the lock
	})
	if err := b.BeforeCall(); err != nil {
		t.Fatal(err)
	}
	b.RecordFailure()
}

func TestBreaker_RecordAbortFreesProbeWithoutCounting(t *testing.T) {
	cfg := defaultBreakerCfg()
	cfg.HalfOpenMaxCalls = 1
	f := makeBreaker(cfg)
	f.fail(3)
	f.clock.Advance(10)
	if err := f.b.BeforeCall(); err != nil {
		t.Fatal(err)
	}
	f.b.RecordAbort()
	if f.b.State() != BreakerHalfOpen {
		t.Fatalf("expected HALF_OPEN, got %v", f.b.State())
	}
	if err := f.b.BeforeCall(); err != nil {
		t.Fatal(err)
	}
	f.b.RecordSuccess()
	if f.b.State() != BreakerClosed {
		t.Fatalf("expected CLOSED, got %v", f.b.State())
	}
}

func TestBreaker_AbortInClosedStateIsNoop(t *testing.T) {
	f := makeBreaker(defaultBreakerCfg())
	f.b.RecordAbort()
	if f.b.State() != BreakerClosed {
		t.Fatalf("expected CLOSED, got %v", f.b.State())
	}
}

func TestBreaker_ResetWhenAlreadyClosedIsNoop(t *testing.T) {
	f := makeBreaker(defaultBreakerCfg())
	f.b.Reset()
	if f.b.State() != BreakerClosed {
		t.Fatalf("expected CLOSED, got %v", f.b.State())
	}
	if len(f.changes) != 0 {
		t.Fatalf("expected no transitions, got %v", f.changes)
	}
}

func TestBreaker_DefaultsWhenConfigAndClockOmitted(t *testing.T) {
	b := NewCircuitBreaker(nil, nil, nil)
	if b.State() != BreakerClosed {
		t.Fatalf("expected CLOSED, got %v", b.State())
	}
	for i := 0; i < 5; i++ {
		b.RecordFailure()
	}
	if b.State() != BreakerOpen {
		t.Fatalf("expected OPEN, got %v", b.State())
	}
}

func TestBreaker_RecordSuccessInClosedResetsFailures(t *testing.T) {
	f := makeBreaker(defaultBreakerCfg())
	if err := f.b.BeforeCall(); err != nil {
		t.Fatal(err)
	}
	f.b.RecordFailure()
	f.b.RecordSuccess() // closed-state success path
	if f.b.State() != BreakerClosed {
		t.Fatalf("expected CLOSED, got %v", f.b.State())
	}
}

func TestBreaker_RecordSuccessWhileOpenIsIgnored(t *testing.T) {
	f := makeBreaker(defaultBreakerCfg())
	f.fail(3)
	if f.b.State() != BreakerOpen {
		t.Fatalf("expected OPEN, got %v", f.b.State())
	}
	f.b.RecordSuccess() // OPEN: neither closed nor half-open branch
	if f.b.State() != BreakerOpen {
		t.Fatalf("expected OPEN, got %v", f.b.State())
	}
}

func TestBreaker_LateFailureWhileOpenIsIgnored(t *testing.T) {
	f := makeBreaker(defaultBreakerCfg())
	f.fail(3)
	if f.b.State() != BreakerOpen {
		t.Fatalf("expected OPEN, got %v", f.b.State())
	}
	f.b.RecordFailure() // OPEN: ignored
	if f.b.State() != BreakerOpen {
		t.Fatalf("expected OPEN, got %v", f.b.State())
	}
}

func TestBreaker_InvalidConfig(t *testing.T) {
	cases := []BreakerConfig{
		{FailureThreshold: 0, RecoveryTimeout: 10, HalfOpenMaxCalls: 1, SuccessThreshold: 1},
		{FailureThreshold: 1, RecoveryTimeout: -1, HalfOpenMaxCalls: 1, SuccessThreshold: 1},
		{FailureThreshold: 1, RecoveryTimeout: 10, HalfOpenMaxCalls: 0, SuccessThreshold: 1},
		{FailureThreshold: 1, RecoveryTimeout: 10, HalfOpenMaxCalls: 1, SuccessThreshold: 0},
	}
	for _, cfg := range cases {
		var target *PoolConfigError
		if err := cfg.Validate(); !errors.As(err, &target) {
			t.Fatalf("expected PoolConfigError for %+v, got %v", cfg, err)
		}
	}
}

func TestBreaker_DefaultConfigValues(t *testing.T) {
	cfg := DefaultBreakerConfig()
	if cfg.FailureThreshold != 5 || cfg.RecoveryTimeout != 30.0 || cfg.HalfOpenMaxCalls != 1 || cfg.SuccessThreshold != 1 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid default config, got %v", err)
	}
}

func TestBreaker_StateValue(t *testing.T) {
	if BreakerClosed.Value() != "closed" || BreakerOpen.Value() != "open" || BreakerHalfOpen.Value() != "half_open" {
		t.Fatal("unexpected BreakerState.Value() mapping")
	}
}
