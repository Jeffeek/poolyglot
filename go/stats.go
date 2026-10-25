package connpool

import (
	"sort"
	"sync"
)

// acquireWaitWindow caps how many recent acquire-wait / create-latency samples are kept.
// avg and all percentiles are computed over this same window (bounded memory), so they
// always describe the same population.
const acquireWaitWindow = 1024

// percentile returns the linear-interpolated percentile of an already-sorted slice.
// q is in [0, 1]. An empty slice returns 0.
func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0.0
	}
	k := float64(len(sorted)-1) * q
	lo := int(k)
	hi := lo + 1
	if hi > len(sorted)-1 {
		hi = len(sorted) - 1
	}
	return sorted[lo] + (sorted[hi]-sorted[lo])*(k-float64(lo))
}

// PoolStats is an immutable snapshot of pool metrics.
type PoolStats struct {
	Name    string
	MaxSize int
	Size    int // idle + in use + being created
	Idle    int // available for checkout right now
	InUse   int // checked out, or briefly reserved by a background idle check
	Waiting int // callers currently blocked in Acquire

	Created        int
	CreateFailures int
	Acquired       int
	Released       int
	Timeouts       int
	Closed         int
	Leaks          int // borrowed connections flagged as possibly leaked

	ClosedByReason map[string]int

	AvgAcquireWait float64
	P50AcquireWait float64
	P95AcquireWait float64
	P99AcquireWait float64

	AvgCreateLatency float64
	P50CreateLatency float64
	P95CreateLatency float64
	P99CreateLatency float64

	BreakerState *string
}

// ToDictionary returns the snake_case wire representation, identical across every
// poolyglot language.
func (s PoolStats) ToDictionary() map[string]any {
	var breaker any
	if s.BreakerState != nil {
		breaker = *s.BreakerState
	}
	return map[string]any{
		"name":               s.Name,
		"max_size":           s.MaxSize,
		"size":               s.Size,
		"idle":               s.Idle,
		"in_use":             s.InUse,
		"waiting":            s.Waiting,
		"created":            s.Created,
		"create_failures":    s.CreateFailures,
		"acquired":           s.Acquired,
		"released":           s.Released,
		"timeouts":           s.Timeouts,
		"closed":             s.Closed,
		"leaks":              s.Leaks,
		"closed_by_reason":   s.ClosedByReason,
		"avg_acquire_wait":   s.AvgAcquireWait,
		"p50_acquire_wait":   s.P50AcquireWait,
		"p95_acquire_wait":   s.P95AcquireWait,
		"p99_acquire_wait":   s.P99AcquireWait,
		"avg_create_latency": s.AvgCreateLatency,
		"p50_create_latency": s.P50CreateLatency,
		"p95_create_latency": s.P95CreateLatency,
		"p99_create_latency": s.P99CreateLatency,
		"breaker_state":      breaker,
	}
}

// statsGauges carries the point-in-time pool gauges a collector doesn't itself track.
type statsGauges struct {
	Name         string
	MaxSize      int
	Size         int
	Idle         int
	InUse        int
	Waiting      int
	BreakerState *string
}

// statsCollector is a PoolListener that counts events and keeps bounded windows of
// acquire-wait and create-latency samples for percentile reporting.
type statsCollector[T any] struct {
	mu             sync.Mutex
	created        int
	createFailures int
	acquired       int
	released       int
	timeouts       int
	closed         int
	leaks          int
	byReason       map[string]int
	waits          []float64
	creates        []float64
}

func newStatsCollector[T any]() *statsCollector[T] {
	return &statsCollector[T]{byReason: make(map[string]int)}
}

// pushWindow appends value, dropping the oldest sample once the window exceeds
// acquireWaitWindow entries.
func pushWindow(window []float64, value float64) []float64 {
	window = append(window, value)
	if len(window) > acquireWaitWindow {
		window = window[len(window)-acquireWaitWindow:]
	}
	return window
}

func (c *statsCollector[T]) OnConnectionCreated(conn *PooledConnection[T], duration float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.created++
	c.creates = pushWindow(c.creates, duration)
}

func (c *statsCollector[T]) OnConnectionClosed(conn *PooledConnection[T], reason CloseReason) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed++
	c.byReason[reason.Value()]++
}

func (c *statsCollector[T]) OnCreateFailed(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.createFailures++
}

func (c *statsCollector[T]) OnAcquired(conn *PooledConnection[T], waitTime float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acquired++
	c.waits = pushWindow(c.waits, waitTime)
}

func (c *statsCollector[T]) OnReleased(conn *PooledConnection[T], holdTime float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.released++
}

func (c *statsCollector[T]) OnAcquireTimeout(waited float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timeouts++
}

func (c *statsCollector[T]) OnConnectionLeak(conn *PooledConnection[T], heldFor float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leaks++
}

func (c *statsCollector[T]) OnBreakerStateChange(old, new BreakerState) {}

// snapshot returns an immutable PoolStats combining the collected counters/windows with
// the given point-in-time gauges.
func (c *statsCollector[T]) snapshot(g statsGauges) PoolStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	waits := sortedCopy(c.waits)
	creates := sortedCopy(c.creates)
	byReason := make(map[string]int, len(c.byReason))
	for k, v := range c.byReason {
		byReason[k] = v
	}

	return PoolStats{
		Name:    g.Name,
		MaxSize: g.MaxSize,
		Size:    g.Size,
		Idle:    g.Idle,
		InUse:   g.InUse,
		Waiting: g.Waiting,

		Created:        c.created,
		CreateFailures: c.createFailures,
		Acquired:       c.acquired,
		Released:       c.released,
		Timeouts:       c.timeouts,
		Closed:         c.closed,
		Leaks:          c.leaks,
		ClosedByReason: byReason,

		AvgAcquireWait: average(waits),
		P50AcquireWait: percentile(waits, 0.50),
		P95AcquireWait: percentile(waits, 0.95),
		P99AcquireWait: percentile(waits, 0.99),

		AvgCreateLatency: average(creates),
		P50CreateLatency: percentile(creates, 0.50),
		P95CreateLatency: percentile(creates, 0.95),
		P99CreateLatency: percentile(creates, 0.99),

		BreakerState: g.BreakerState,
	}
}

func sortedCopy(values []float64) []float64 {
	out := make([]float64, len(values))
	copy(out, values)
	sort.Float64s(out)
	return out
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0.0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}
