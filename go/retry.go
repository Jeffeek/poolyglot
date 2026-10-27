package connpool

import "math"
import "math/rand"

// RetryPolicy defines how to retry factory.Create(): exponential backoff with jitter.
//
// Jitter in [0, 1]: 0 = deterministic delays, 1 = "full jitter" (uniform in
// [0, delay]). Spreading retries avoids thundering herds. RetryOn narrows
// what is retryable (e.g. don't retry auth failures); nil means retry everything.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   float64
	MaxDelay    float64
	Multiplier  float64
	Jitter      float64
	RetryOn     func(error) bool
}

// DefaultRetryPolicy returns the default retry policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   0.1,
		MaxDelay:    5.0,
		Multiplier:  2.0,
		Jitter:      0.5,
		RetryOn:     nil,
	}
}

// NoRetry is a policy that never retries (one attempt only).
var NoRetry = RetryPolicy{MaxAttempts: 1, BaseDelay: 0.1, MaxDelay: 5.0, Multiplier: 2.0, Jitter: 0.5}

// Validate checks the policy's invariants.
func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 {
		return &PoolConfigError{Msg: "retry.max_attempts must be >= 1"}
	}
	if p.BaseDelay < 0 || p.MaxDelay < 0 || p.Multiplier < 1 {
		return &PoolConfigError{Msg: "retry delays must be >= 0 and multiplier >= 1"}
	}
	if p.Jitter < 0.0 || p.Jitter > 1.0 {
		return &PoolConfigError{Msg: "retry.jitter must be within [0, 1]"}
	}
	return nil
}

// ShouldRetry reports whether err is retryable under this policy.
func (p RetryPolicy) ShouldRetry(err error) bool {
	if p.RetryOn == nil {
		return true
	}
	return p.RetryOn(err)
}

// DelayFor returns the delay before the next attempt, given how many attempts have failed so far.
func (p RetryPolicy) DelayFor(failedAttempts int, rng *rand.Rand) float64 {
	ceiling := 0.0
	if p.BaseDelay != 0.0 { // avoids 0 * Inf = NaN when Multiplier^(n-1) overflows
		ceiling = math.Min(p.MaxDelay, p.BaseDelay*math.Pow(p.Multiplier, float64(failedAttempts-1)))
	}
	return ceiling * (1.0 - p.Jitter + p.Jitter*rng.Float64())
}
