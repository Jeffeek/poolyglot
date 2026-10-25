package connpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Shared test doubles, mirroring python/tests/support/helpers.py and java TestSupport.

// errBackendDown is the error a failing FakeFactory.Create returns.
var errBackendDown = errors.New("backend down")

// errResetFailed is raised by a FakeFactory whose resetRaises switch is on.
var errResetFailed = errors.New("reset failed")

var fakeIDs int64

// FakeConn is a pooled fake connection. Fields are only ever touched by the single goroutine
// that currently holds the connection (the pool guarantees exclusive checkout) plus the
// factory's close on disposal, so no locking is needed.
type FakeConn struct {
	ID      int64
	Closed  bool
	Healthy bool
	Resets  int
}

func newFakeConn() *FakeConn {
	return &FakeConn{ID: atomic.AddInt64(&fakeIDs, 1), Healthy: true}
}

// FakeFactory is a controllable ConnectionFactory[*FakeConn].
type FakeFactory struct {
	mu          sync.Mutex
	Created     []*FakeConn
	Attempts    int
	FailTimes   int  // fail this many creates, then succeed
	FailAlways  bool // always fail Create
	ResetRaises bool // Reset returns an error
	Fatal       func(error) bool
}

func (f *FakeFactory) Create() (*FakeConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Attempts++
	if f.FailAlways || f.FailTimes > 0 {
		if f.FailTimes > 0 {
			f.FailTimes--
		}
		return nil, errBackendDown
	}
	c := newFakeConn()
	f.Created = append(f.Created, c)
	return c, nil
}

func (f *FakeFactory) Validate(c *FakeConn) bool { return c.Healthy && !c.Closed }

func (f *FakeFactory) Reset(c *FakeConn) error {
	if f.ResetRaises {
		return errResetFailed
	}
	c.Resets++
	return nil
}

func (f *FakeFactory) Close(c *FakeConn) error {
	c.Closed = true
	return nil
}

func (f *FakeFactory) IsFatalError(err error) bool {
	return f.Fatal != nil && f.Fatal(err)
}

// attemptCount returns the number of Create attempts so far (thread-safe).
func (f *FakeFactory) attemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Attempts
}

// createdCount returns the number of successfully created connections (thread-safe).
func (f *FakeFactory) createdCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Created)
}

// AsyncFakeFactory is an async view over a FakeFactory (shares its counters/switches).
// createDelay (seconds, real time) lets create_timeout tests exercise the derived context.
type AsyncFakeFactory struct {
	Inner       *FakeFactory
	CreateDelay float64
}

func newAsyncFakeFactory() *AsyncFakeFactory {
	return &AsyncFakeFactory{Inner: &FakeFactory{}}
}

func (f *AsyncFakeFactory) Create(ctx context.Context) (*FakeConn, error) {
	if f.CreateDelay > 0 {
		t := time.NewTimer(time.Duration(f.CreateDelay * float64(time.Second)))
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return f.Inner.Create()
}

func (f *AsyncFakeFactory) Validate(ctx context.Context, c *FakeConn) bool {
	return f.Inner.Validate(c)
}

func (f *AsyncFakeFactory) Reset(ctx context.Context, c *FakeConn) error {
	return f.Inner.Reset(c)
}

func (f *AsyncFakeFactory) Close(ctx context.Context, c *FakeConn) error {
	return f.Inner.Close(c)
}

func (f *AsyncFakeFactory) IsFatalError(err error) bool {
	return f.Inner.IsFatalError(err)
}

// Recorder is a PoolListener that records events for assertions.
type Recorder struct {
	BasePoolListener[*FakeConn]
	mu     sync.Mutex
	Closed []CloseReason
	Events []string
}

func (r *Recorder) OnConnectionCreated(conn *PooledConnection[*FakeConn], duration float64) {
	r.add("created")
}

func (r *Recorder) OnConnectionClosed(conn *PooledConnection[*FakeConn], reason CloseReason) {
	r.mu.Lock()
	r.Closed = append(r.Closed, reason)
	r.Events = append(r.Events, "closed:"+reason.Value())
	r.mu.Unlock()
}

func (r *Recorder) OnAcquired(conn *PooledConnection[*FakeConn], waitTime float64) {
	r.add("acquired")
}

func (r *Recorder) OnReleased(conn *PooledConnection[*FakeConn], holdTime float64) {
	r.add("released")
}

func (r *Recorder) OnBreakerStateChange(old, nw BreakerState) {
	r.add("breaker:" + old.Value() + "->" + nw.Value())
}

func (r *Recorder) add(event string) {
	r.mu.Lock()
	r.Events = append(r.Events, event)
	r.mu.Unlock()
}

// events returns a copy of the recorded events (thread-safe).
func (r *Recorder) events() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.Events...)
}

// closedReasons returns a copy of the recorded close reasons (thread-safe).
func (r *Recorder) closedReasons() []CloseReason {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]CloseReason{}, r.Closed...)
}

// quietConfig is a deterministic baseline: no jitter, no background work, no retries, no
// breaker. Mirrors helpers.quiet_config; tests mutate the returned value's fields.
func quietConfig() PoolConfig {
	c := DefaultConfig()
	c.MaxLifetime = nil
	c.MaxIdleTime = nil
	c.ValidateAfterIdle = nil
	c.MaintenanceInterval = nil
	c.AcquireTimeout = Ptr(0.3)
	c.LifetimeJitter = 0.0
	c.Retry = NoRetry
	c.Breaker = nil
	return c
}

// mustPool builds a sync pool from a config, failing the test on a config error.
func mustPool(factory ConnectionFactory[*FakeConn], cfg PoolConfig, opts *Options[*FakeConn]) *ConnectionPool[*FakeConn] {
	p, err := NewConnectionPool(factory, cfg, opts)
	if err != nil {
		panic(err)
	}
	return p
}

// mustAsyncPool builds an async pool from a config, failing the test on a config error.
func mustAsyncPool(factory AsyncConnectionFactory[*FakeConn], cfg PoolConfig, opts *Options[*FakeConn]) *AsyncConnectionPool[*FakeConn] {
	p, err := NewAsyncConnectionPool(factory, cfg, opts)
	if err != nil {
		panic(err)
	}
	return p
}
