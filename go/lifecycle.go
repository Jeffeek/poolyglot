package connpool

import "math/rand"

// The pure "functional core": every rule about *when a connection must go away*.
//
// No locks, no I/O, no clocks of its own - just functions of (connection, config, now).
// Both the sync and async pools call these, so the rules live in exactly one place and
// can be unit-tested without threads or goroutines.

func newConnection[T any](raw T, cfg PoolConfig, now float64, rng *rand.Rand) *PooledConnection[T] {
	var expiresAt *float64
	if cfg.MaxLifetime != nil {
		e := now + *cfg.MaxLifetime*(1.0-cfg.LifetimeJitter*rng.Float64())
		expiresAt = &e
	}
	return newPooledConnection(raw, now, expiresAt)
}

func popIdle[T any](idle []*PooledConnection[T], strategy IdleStrategy) (*PooledConnection[T], []*PooledConnection[T]) {
	if len(idle) == 0 {
		return nil, idle
	}
	if strategy == LIFO {
		last := len(idle) - 1
		return idle[last], idle[:last]
	}
	return idle[0], idle[1:]
}

// rotationReason reports connections that must be retired no matter what (lifetime / use-count rotation).
func rotationReason[T any](c *PooledConnection[T], cfg PoolConfig, now float64) (CloseReason, bool) {
	if c.ExpiresAt != nil && now >= *c.ExpiresAt {
		return CloseReasonExpired, true
	}
	if cfg.MaxUses != nil && c.UseCount >= *cfg.MaxUses {
		return CloseReasonMaxUses, true
	}
	return "", false
}

func idleTooLong[T any](c *PooledConnection[T], cfg PoolConfig, now float64) bool {
	return cfg.MaxIdleTime != nil && c.IdleTime(now) >= *cfg.MaxIdleTime
}

func needsValidation[T any](c *PooledConnection[T], cfg PoolConfig, now float64) bool {
	return cfg.ValidateAfterIdle != nil && c.IdleTime(now) >= *cfg.ValidateAfterIdle
}

// borrowReason applies the static checks to an idle connection about to be handed out
// (validation, which needs I/O, is done by the pool afterwards).
func borrowReason[T any](c *PooledConnection[T], cfg PoolConfig, now float64) (CloseReason, bool) {
	reason, ok := rotationReason(c, cfg, now)
	if !ok && idleTooLong(c, cfg, now) {
		return CloseReasonIdle, true
	}
	return reason, ok
}

type doomedConn[T any] struct {
	Conn   *PooledConnection[T]
	Reason CloseReason
}

// sweep splits idle connections into (keep, doomed). Rotation victims always go;
// idle-timeout victims go only while the pool stays at or above cfg.MinSize.
func sweep[T any](idle []*PooledConnection[T], totalSize int, cfg PoolConfig, now float64) ([]*PooledConnection[T], []doomedConn[T]) {
	keep := make([]*PooledConnection[T], 0, len(idle))
	var doomed []doomedConn[T]
	remaining := totalSize
	for _, c := range idle {
		reason, ok := rotationReason(c, cfg, now)
		if !ok && idleTooLong(c, cfg, now) && remaining > cfg.MinSize {
			reason, ok = CloseReasonIdle, true
		}
		if !ok {
			keep = append(keep, c)
		} else {
			doomed = append(doomed, doomedConn[T]{Conn: c, Reason: reason})
			remaining--
		}
	}
	return keep, doomed
}
