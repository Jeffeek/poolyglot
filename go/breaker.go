package connpool

import "sync"

// BreakerState is the circuit breaker's state machine position.
//
//	CLOSED --(N consecutive failures)--> OPEN --(recovery_timeout)--> HALF_OPEN
//	   ^                                   ^                              |
//	   |                                   +------ any probe fails -------+
//	   +------------ enough probes succeed ---------------------------------+
type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"
	BreakerOpen     BreakerState = "open"
	BreakerHalfOpen BreakerState = "half_open"
)

// Value returns the wire representation of the state.
func (s BreakerState) Value() string { return string(s) }

// BreakerConfig configures a CircuitBreaker.
type BreakerConfig struct {
	FailureThreshold int     // consecutive failures that trip the breaker
	RecoveryTimeout  float64 // seconds to stay OPEN before probing
	HalfOpenMaxCalls int     // concurrent probe calls allowed while HALF_OPEN
	SuccessThreshold int     // probe successes needed to close again
}

// DefaultBreakerConfig returns the default breaker configuration.
func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{FailureThreshold: 5, RecoveryTimeout: 30.0, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
}

// Validate checks that the breaker configuration is well-formed.
func (c BreakerConfig) Validate() error {
	if c.FailureThreshold < 1 || c.HalfOpenMaxCalls < 1 || c.SuccessThreshold < 1 {
		return &PoolConfigError{Msg: "breaker thresholds must be >= 1"}
	}
	if c.RecoveryTimeout < 0 {
		return &PoolConfigError{Msg: "breaker.recovery_timeout must be >= 0"}
	}
	return nil
}

type breakerTransition struct {
	old BreakerState
	new BreakerState
}

// CircuitBreaker is a thread-safe circuit breaker. Usage: BeforeCall() -> do work ->
// RecordSuccess() / RecordFailure().
type CircuitBreaker struct {
	cfg      BreakerConfig
	clock    Clock
	onChange func(old, new BreakerState)

	mu             sync.Mutex
	state          BreakerState
	failures       int
	openedAt       float64
	probesInflight int
	probeSuccesses int
}

// NewCircuitBreaker builds a CircuitBreaker. config nil uses DefaultBreakerConfig(); clock nil
// uses NewSystemClock().
func NewCircuitBreaker(config *BreakerConfig, clock Clock, onStateChange func(old, new BreakerState)) *CircuitBreaker {
	cfg := DefaultBreakerConfig()
	if config != nil {
		cfg = *config
	}
	if clock == nil {
		clock = NewSystemClock()
	}
	return &CircuitBreaker{cfg: cfg, clock: clock, onChange: onStateChange, state: BreakerClosed}
}

// State returns the current state, refreshing OPEN -> HALF_OPEN if the recovery timeout elapsed.
func (b *CircuitBreaker) State() BreakerState {
	b.mu.Lock()
	transition := b.refresh()
	state := b.state
	b.mu.Unlock()
	b.emit(transition)
	return state
}

// BeforeCall returns a *CircuitOpenError if the call must not be attempted.
func (b *CircuitBreaker) BeforeCall() error {
	var callErr error
	b.mu.Lock()
	transition := b.refresh()
	switch b.state {
	case BreakerOpen:
		// refresh() already moved us out of OPEN once recovery elapsed, so wait is > 0 here;
		// max mirrors the Python max(0.0, wait) and stays safe under clock skew.
		wait := max(0.0, b.cfg.RecoveryTimeout-(b.clock.Monotonic()-b.openedAt))
		callErr = &CircuitOpenError{RetryAfter: wait}
	case BreakerHalfOpen:
		if b.probesInflight >= b.cfg.HalfOpenMaxCalls {
			callErr = &CircuitOpenError{RetryAfter: 0}
		} else {
			b.probesInflight++
		}
	}
	b.mu.Unlock()
	b.emit(transition)
	return callErr
}

// RecordSuccess records a successful call.
func (b *CircuitBreaker) RecordSuccess() {
	var transition *breakerTransition
	b.mu.Lock()
	switch b.state {
	case BreakerClosed:
		b.failures = 0
	case BreakerHalfOpen:
		b.probesInflight = max(0, b.probesInflight-1)
		b.probeSuccesses++
		if b.probeSuccesses >= b.cfg.SuccessThreshold {
			transition = b.setState(BreakerClosed)
		}
	}
	b.mu.Unlock()
	b.emit(transition)
}

// RecordFailure records a failed call.
func (b *CircuitBreaker) RecordFailure() {
	var transition *breakerTransition
	b.mu.Lock()
	switch b.state {
	case BreakerClosed:
		b.failures++
		if b.failures >= b.cfg.FailureThreshold {
			transition = b.setState(BreakerOpen)
		}
	case BreakerHalfOpen:
		transition = b.setState(BreakerOpen)
	}
	// OPEN: a late failure from a call started before tripping; ignore.
	b.mu.Unlock()
	b.emit(transition)
}

// RecordAbort releases a half-open probe slot without counting it either way (e.g. the call
// was cancelled or timed out before the backend answered).
func (b *CircuitBreaker) RecordAbort() {
	b.mu.Lock()
	if b.state == BreakerHalfOpen {
		b.probesInflight = max(0, b.probesInflight-1)
	}
	b.mu.Unlock()
}

// Call is a convenience wrapper for arbitrary functions.
func (b *CircuitBreaker) Call(fn func() error) error {
	if err := b.BeforeCall(); err != nil {
		return err
	}
	if err := fn(); err != nil {
		b.RecordFailure()
		return err
	}
	b.RecordSuccess()
	return nil
}

// Reset forces the breaker back to CLOSED.
func (b *CircuitBreaker) Reset() {
	b.mu.Lock()
	transition := b.setState(BreakerClosed)
	b.mu.Unlock()
	b.emit(transition)
}

// -- internals (call with the lock held) -----------------------------------

func (b *CircuitBreaker) refresh() *breakerTransition {
	if b.state == BreakerOpen && b.clock.Monotonic()-b.openedAt >= b.cfg.RecoveryTimeout {
		return b.setState(BreakerHalfOpen)
	}
	return nil
}

func (b *CircuitBreaker) setState(new BreakerState) *breakerTransition {
	old := b.state
	if old == new {
		return nil
	}
	b.state = new
	b.failures = 0
	b.probesInflight = 0
	b.probeSuccesses = 0
	if new == BreakerOpen {
		b.openedAt = b.clock.Monotonic()
	}
	return &breakerTransition{old: old, new: new}
}

func (b *CircuitBreaker) emit(transition *breakerTransition) {
	// Fired after the lock is released so callbacks may safely re-enter the breaker.
	if transition != nil && b.onChange != nil {
		b.onChange(transition.old, transition.new)
	}
}
