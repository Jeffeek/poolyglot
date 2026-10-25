package connpool

import "fmt"

// PoolError is implemented by every error connpool raises, so callers can detect "any
// pool error" with errors.As(err, new(PoolError))-style checks (each concrete type below
// also works with errors.As). It mirrors the PoolError base class in the other languages.
type PoolError interface {
	error
	poolError()
}

// PoolConfigError signals invalid pool / breaker / retry configuration.
type PoolConfigError struct{ Msg string }

func (e *PoolConfigError) Error() string { return e.Msg }
func (e *PoolConfigError) poolError()    {}

// PoolClosedError signals that the pool has been closed; no more connections can be acquired.
type PoolClosedError struct{ Msg string }

func (e *PoolClosedError) Error() string { return e.Msg }
func (e *PoolClosedError) poolError()    {}

// PoolTimeoutError signals that no connection became available before the acquire deadline.
type PoolTimeoutError struct{ Msg string }

func (e *PoolTimeoutError) Error() string { return e.Msg }
func (e *PoolTimeoutError) poolError()    {}

// PoolExhaustedError signals that the pool is full and the waiter queue is at max_waiters.
type PoolExhaustedError struct{ Msg string }

func (e *PoolExhaustedError) Error() string { return e.Msg }
func (e *PoolExhaustedError) poolError()    {}

// ConnectionCreateError signals that the factory failed to create a connection (after all
// retries). The original error is available via Unwrap / errors.Unwrap.
type ConnectionCreateError struct {
	Msg   string
	Cause error
}

func (e *ConnectionCreateError) Error() string { return e.Msg }
func (e *ConnectionCreateError) Unwrap() error { return e.Cause }
func (e *ConnectionCreateError) poolError()    {}

// CircuitOpenError signals that the circuit breaker is rejecting calls to protect a
// failing backend. RetryAfter is the approximate seconds until the breaker probes again.
type CircuitOpenError struct {
	RetryAfter float64
}

func (e *CircuitOpenError) Error() string {
	return fmt.Sprintf("circuit breaker is open; retry in ~%.1fs", e.RetryAfter)
}
func (e *CircuitOpenError) poolError() {}

// InvalidReleaseError signals that an object that is not checked out from this pool was
// released (a double release?).
type InvalidReleaseError struct{ Msg string }

func (e *InvalidReleaseError) Error() string { return e.Msg }
func (e *InvalidReleaseError) poolError()    {}
