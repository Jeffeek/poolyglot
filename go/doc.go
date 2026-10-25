// Package connpool is a generic connection pool for Go (blocking + context-based),
// standard library only.
//
// Layers (each only depends on the ones above it):
//
//	core           errors, clocks, connection bookkeeping
//	resilience     circuit breaker, retry/backoff
//	policy         PoolConfig + the pure retirement rules
//	factories      ConnectionFactory: the seam users implement to pool anything
//	observability  listeners + stats
//	pools          ConnectionPool, AsyncConnectionPool, PoolRegistry
//
// It is the Go member of poolyglot: one pooling model implemented per language, kept
// API-parallel. Field names, event names, defaults, and CloseReason/BreakerState wire
// values are identical to the Python (connpool), C# (ConnPool) and Java implementations —
// switching languages is a syntax change, not a model change.
//
// Everything lives in the single connpool package, so one import brings in the whole
// public API. The blocking ConnectionPool and the context-based AsyncConnectionPool are
// deliberate parallel implementations of the same model.
package connpool

// Version is the released version of this package. Kept in lock-step with the root
// VERSION file by release.sh; the module's import version lives in its git tag (go/vX.Y.Z).
const Version = "1.0.10"

// Ptr returns a pointer to v. It is a convenience for the optional (*T) fields of
// PoolConfig — e.g. connpool.Ptr(5.0) for a 5-second timeout, or leaving a field nil for
// "no limit".
func Ptr[T any](v T) *T { return &v }
