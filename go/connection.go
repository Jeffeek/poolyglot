package connpool

import (
	"fmt"
	"sync/atomic"
)

// CloseReason explains why a connection left the pool. Its string values are wire values
// kept identical across every poolyglot language — do not localize them.
type CloseReason string

const (
	CloseReasonExpired     CloseReason = "expired"      // max_lifetime reached (rotation)
	CloseReasonMaxUses     CloseReason = "max_uses"     // max_uses reached (rotation)
	CloseReasonIdle        CloseReason = "idle"         // sat idle longer than max_idle_time
	CloseReasonUnhealthy   CloseReason = "unhealthy"    // failed validation
	CloseReasonError       CloseReason = "error"        // caller's block raised and the connection is suspect
	CloseReasonResetFailed CloseReason = "reset_failed" // factory.Reset() failed on release
	CloseReasonDiscarded   CloseReason = "discarded"    // caller asked for it
	CloseReasonPoolClosed  CloseReason = "pool_closed"
)

// Value returns the wire string for the reason.
func (r CloseReason) Value() string { return string(r) }

var connIDs int64

// PooledConnection is a raw connection plus the metadata the pool needs to manage it.
// Instances are tracked by pointer identity by the pool.
type PooledConnection[T any] struct {
	Raw            T
	CreatedAt      float64
	ExpiresAt      *float64
	LastReturnedAt *float64
	CheckedOutAt   *float64
	UseCount       int
	LeakReported   bool
	ID             int64
}

// newPooledConnection builds a connection wrapper; last_returned defaults to created_at.
func newPooledConnection[T any](raw T, createdAt float64, expiresAt *float64) *PooledConnection[T] {
	ret := createdAt
	return &PooledConnection[T]{
		Raw:            raw,
		CreatedAt:      createdAt,
		ExpiresAt:      expiresAt,
		LastReturnedAt: &ret,
		ID:             atomic.AddInt64(&connIDs, 1),
	}
}

func (c *PooledConnection[T]) Age(now float64) float64 { return now - c.CreatedAt }

func (c *PooledConnection[T]) IdleTime(now float64) float64 {
	base := c.CreatedAt
	if c.LastReturnedAt != nil {
		base = *c.LastReturnedAt
	}
	return now - base
}

func (c *PooledConnection[T]) HoldTime(now float64) float64 {
	if c.CheckedOutAt == nil {
		return 0.0
	}
	return now - *c.CheckedOutAt
}

func (c *PooledConnection[T]) MarkCheckedOut(now float64) {
	c.CheckedOutAt = &now
	c.UseCount++
	c.LeakReported = false
}

func (c *PooledConnection[T]) MarkReturned(now float64) {
	c.CheckedOutAt = nil
	c.LastReturnedAt = &now
}

func (c *PooledConnection[T]) String() string {
	return fmt.Sprintf("<PooledConnection #%d uses=%d>", c.ID, c.UseCount)
}
