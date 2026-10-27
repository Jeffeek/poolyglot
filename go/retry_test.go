package connpool

import (
	"errors"
	"math"
	"math/rand"
	"testing"
)

func TestRetry_InvalidConfig(t *testing.T) {
	base := DefaultRetryPolicy()

	cases := map[string]RetryPolicy{
		"max_attempts": func() RetryPolicy { p := base; p.MaxAttempts = 0; return p }(),
		"base_delay":   func() RetryPolicy { p := base; p.BaseDelay = -1; return p }(),
		"max_delay":    func() RetryPolicy { p := base; p.MaxDelay = -1; return p }(),
		"multiplier":   func() RetryPolicy { p := base; p.Multiplier = 0.5; return p }(),
		"jitter_high":  func() RetryPolicy { p := base; p.Jitter = 2.0; return p }(),
		"jitter_low":   func() RetryPolicy { p := base; p.Jitter = -0.5; return p }(),
	}

	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			var cfgErr *PoolConfigError
			if err := p.Validate(); !errors.As(err, &cfgErr) {
				t.Fatalf("expected *PoolConfigError, got %v", err)
			}
		})
	}
}

func TestRetry_Backoff(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 5, BaseDelay: 1, MaxDelay: 5, Multiplier: 2, Jitter: 0}
	if err := p.Validate(); err != nil {
		t.Fatalf("unexpected validate error: %v", err)
	}
	rng := rand.New(rand.NewSource(0))
	want := []float64{1, 2, 4, 5}
	for i, n := range []int{1, 2, 3, 4} {
		got := p.DelayFor(n, rng)
		if got != want[i] {
			t.Errorf("DelayFor(%d) = %v, want %v", n, got, want[i])
		}
	}

	full := RetryPolicy{MaxAttempts: 3, BaseDelay: 4, MaxDelay: 5, Multiplier: 2, Jitter: 1.0}
	if err := full.Validate(); err != nil {
		t.Fatalf("unexpected validate error: %v", err)
	}
	for i := 0; i < 100; i++ {
		d := full.DelayFor(1, rng)
		if d < 0.0 || d > 4.0 {
			t.Fatalf("DelayFor(1) = %v, out of [0, 4]", d)
		}
	}
}

func TestRetry_ZeroBaseDelayNeverNaN(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 1, BaseDelay: 0, MaxDelay: 5, Multiplier: 2, Jitter: 0.5}
	if err := p.Validate(); err != nil {
		t.Fatalf("unexpected validate error: %v", err)
	}
	rng := rand.New(rand.NewSource(0))
	// failedAttempts large enough that Multiplier^(n-1) overflows to +Inf; BaseDelay==0 must
	// still yield a finite 0 delay instead of 0*Inf = NaN.
	got := p.DelayFor(2000, rng)
	if math.IsNaN(got) || math.IsInf(got, 0) {
		t.Fatalf("DelayFor(2000) = %v, want finite 0", got)
	}
	if got != 0 {
		t.Errorf("DelayFor(2000) = %v, want 0", got)
	}
}

func TestRetry_OnFilter(t *testing.T) {
	p := DefaultRetryPolicy()
	p.RetryOn = func(err error) bool { return errors.Is(err, errBackendDown) }

	if !p.ShouldRetry(errBackendDown) {
		t.Error("expected errBackendDown to be retryable")
	}
	if p.ShouldRetry(errors.New("other")) {
		t.Error("expected unrelated error to not be retryable")
	}
}

func TestRetry_NilRetryOnRetriesEverything(t *testing.T) {
	p := DefaultRetryPolicy()
	p.RetryOn = nil
	if !p.ShouldRetry(errors.New("anything")) {
		t.Error("nil RetryOn should retry everything")
	}
}

func TestRetry_DefaultsAndNoRetry(t *testing.T) {
	d := DefaultRetryPolicy()
	if d.MaxAttempts != 3 {
		t.Errorf("DefaultRetryPolicy().MaxAttempts = %d, want 3", d.MaxAttempts)
	}
	if d.BaseDelay != 0.1 || d.MaxDelay != 5.0 || d.Multiplier != 2.0 || d.Jitter != 0.5 {
		t.Errorf("DefaultRetryPolicy() = %+v, unexpected fields", d)
	}
	if err := d.Validate(); err != nil {
		t.Fatalf("DefaultRetryPolicy() should be valid: %v", err)
	}

	if NoRetry.MaxAttempts != 1 {
		t.Errorf("NoRetry.MaxAttempts = %d, want 1", NoRetry.MaxAttempts)
	}
	if err := NoRetry.Validate(); err != nil {
		t.Fatalf("NoRetry should be valid: %v", err)
	}

	copy := d
	copy.MaxAttempts = 9
	if copy.MaxAttempts != 9 {
		t.Errorf("copy.MaxAttempts = %d, want 9", copy.MaxAttempts)
	}
	if copy.BaseDelay != 0.1 {
		t.Errorf("copy.BaseDelay = %v, want 0.1 (unchanged)", copy.BaseDelay)
	}
	if d.MaxAttempts != 3 {
		t.Errorf("original d.MaxAttempts mutated to %d, want 3", d.MaxAttempts)
	}
}

func TestRetry_ValidMultiplierExactlyOne(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 1, BaseDelay: 0, MaxDelay: 0, Multiplier: 1, Jitter: 0}
	if err := p.Validate(); err != nil {
		t.Fatalf("multiplier == 1 should be valid: %v", err)
	}
}

func TestRetry_ValidJitterBounds(t *testing.T) {
	for _, j := range []float64{0.0, 1.0} {
		p := DefaultRetryPolicy()
		p.Jitter = j
		if err := p.Validate(); err != nil {
			t.Errorf("jitter=%v should be valid: %v", j, err)
		}
	}
}
