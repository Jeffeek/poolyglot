package connpool

import (
	"context"
	"sync"
	"time"
)

// Clock is a time source abstraction so every time-dependent rule is deterministic in
// tests. Time is measured in float seconds (like the other languages).
type Clock interface {
	// Monotonic returns a monotonically increasing time in seconds.
	Monotonic() float64
	// Sleep blocks for the given number of seconds.
	Sleep(seconds float64)
	// SleepContext blocks for the given number of seconds or until ctx is done,
	// returning ctx.Err() if it was cancelled first.
	SleepContext(ctx context.Context, seconds float64) error
}

// SystemClock is real time. The default.
type SystemClock struct {
	start time.Time
}

// NewSystemClock returns a SystemClock anchored at the current instant.
func NewSystemClock() *SystemClock { return &SystemClock{start: time.Now()} }

func (c *SystemClock) Monotonic() float64 { return time.Since(c.start).Seconds() }

func (c *SystemClock) Sleep(seconds float64) {
	if seconds > 0 {
		time.Sleep(time.Duration(seconds * float64(time.Second)))
	}
}

func (c *SystemClock) SleepContext(ctx context.Context, seconds float64) error {
	if seconds <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(time.Duration(seconds * float64(time.Second)))
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ManualClock is a fake clock for tests: time moves only when you say so; sleeping just
// advances it.
type ManualClock struct {
	mu  sync.Mutex
	now float64
}

// NewManualClock returns a ManualClock starting at the given time (use 1000.0 to match the
// other languages' default).
func NewManualClock(start float64) *ManualClock { return &ManualClock{now: start} }

func (c *ManualClock) Monotonic() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by the given number of seconds.
func (c *ManualClock) Advance(seconds float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now += seconds
}

func (c *ManualClock) Sleep(seconds float64) { c.Advance(seconds) }

func (c *ManualClock) SleepContext(ctx context.Context, seconds float64) error {
	c.Advance(seconds)
	// Yield like asyncio.sleep(0): a cancellation point without real waiting.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
