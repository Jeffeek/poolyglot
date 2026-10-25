package connpool

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Options configures a pool at construction. A nil *Options means all defaults. Name
// defaults to "pool"; Clock defaults to a SystemClock; Rand defaults to a time-seeded
// generator. Listeners are added in addition to the built-in stats collector.
type Options[T comparable] struct {
	Name      string
	Clock     Clock
	Rand      *rand.Rand
	Listeners []PoolListener[T]
}

// ReleaseOption tweaks how a connection is returned (see Discard / WithError).
type ReleaseOption func(*releaseOpts)

type releaseOpts struct {
	discard bool
	err     error
}

// Discard forces the connection to be closed instead of reused.
func Discard() ReleaseOption { return func(o *releaseOpts) { o.discard = true } }

// WithError marks the release as happening because the caller's work raised err; the pool
// discards the connection if the factory deems the error fatal or validation fails.
func WithError(err error) ReleaseOption { return func(o *releaseOpts) { o.err = err } }

// ConnectionPool is a thread-safe synchronous connection pool.
//
// A single mutex guards a tiny amount of state (idle slice, checked-out map, a slot
// counter). All I/O — create, validate, reset, close — happens outside that lock, so a slow
// backend never blocks unrelated callers. A slot is reserved under the lock before creating,
// which makes MaxSize a hard cap. The retirement rules live in lifecycle.go; this file is
// plumbing. T is pooled by value identity (pointer/interface types track by reference).
type ConnectionPool[T comparable] struct {
	Name    string
	factory ConnectionFactory[T]
	cfg     PoolConfig
	clock   Clock
	rng     *rand.Rand
	rngMu   sync.Mutex

	mu         sync.Mutex
	notify     chan struct{}
	idle       []*PooledConnection[T]
	checkedOut map[T]*PooledConnection[T]
	size       int
	waiters    int
	closed     bool
	opened     bool

	stats   *statsCollector[T]
	events  *listenerGroup[T]
	breaker *CircuitBreaker

	stopCh       chan struct{}
	maintRunning bool
	wg           sync.WaitGroup
}

// NewConnectionPool builds a pool. It validates config (returning *PoolConfigError on bad
// config). opts may be nil.
func NewConnectionPool[T comparable](factory ConnectionFactory[T], config PoolConfig, opts *Options[T]) (*ConnectionPool[T], error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &Options[T]{}
	}
	clock := opts.Clock
	if clock == nil {
		clock = NewSystemClock()
	}
	rng := opts.Rand
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	name := opts.Name
	if name == "" {
		name = "pool"
	}
	stats := newStatsCollector[T]()
	listeners := append([]PoolListener[T]{stats}, opts.Listeners...)
	events := newListenerGroup(listeners)
	p := &ConnectionPool[T]{
		Name:       name,
		factory:    factory,
		cfg:        config,
		clock:      clock,
		rng:        rng,
		notify:     make(chan struct{}),
		checkedOut: make(map[T]*PooledConnection[T]),
		stats:      stats,
		events:     events,
		stopCh:     make(chan struct{}),
	}
	if config.Breaker != nil {
		p.breaker = NewCircuitBreaker(config.Breaker, clock, func(old, nw BreakerState) {
			p.events.safe(func(l PoolListener[T]) { l.OnBreakerStateChange(old, nw) })
		})
	}
	return p, nil
}

// signal wakes all waiters. Caller must hold p.mu.
func (p *ConnectionPool[T]) signal() {
	close(p.notify)
	p.notify = make(chan struct{})
}

func (p *ConnectionPool[T]) newConn(raw T, now float64) *PooledConnection[T] {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return newConnection(raw, p.cfg, now, p.rng)
}

func (p *ConnectionPool[T]) retryDelay(attempt int) float64 {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return p.cfg.Retry.DelayFor(attempt, p.rng)
}

// ------------------------------------------------------------------ lifecycle

// Open warms up MinSize connections and starts the background maintenance goroutine.
// Optional: without it the pool is purely lazy. Idempotent.
func (p *ConnectionPool[T]) Open() (*ConnectionPool[T], error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, &PoolClosedError{Msg: fmt.Sprintf("pool %q is closed", p.Name)}
	}
	if p.opened {
		p.mu.Unlock()
		return p, nil
	}
	p.opened = true
	startThread := p.cfg.MaintenanceInterval != nil
	if startThread {
		p.maintRunning = true
	}
	p.mu.Unlock()

	p.fillToMin()
	if startThread {
		p.wg.Add(1)
		go p.maintenanceLoop()
	}
	return p, nil
}

// Close closes idle connections now; checked-out ones close when released. Blocked
// Acquire callers get a PoolClosedError. Idempotent.
//
// timeout (nil = don't wait) drains gracefully: wait up to that many seconds for in-use
// connections to return before giving up (they still close on release). 0 returns at once.
func (p *ConnectionPool[T]) Close(timeout *float64) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.signal()
	if timeout != nil {
		deadline := p.clock.Monotonic() + *timeout
		for len(p.checkedOut) > 0 {
			left := deadline - p.clock.Monotonic()
			if left <= 0 {
				break
			}
			ch := p.notify
			p.mu.Unlock()
			waitSignal(ch, &left)
			p.mu.Lock()
		}
	}
	maintRunning := p.maintRunning
	p.maintRunning = false
	p.mu.Unlock()

	if maintRunning {
		close(p.stopCh)
		p.wg.Wait()
	}
	for _, pooled := range idle {
		p.destroy(pooled, CloseReasonPoolClosed)
	}
	return nil
}

// Closed reports whether Close has been called.
func (p *ConnectionPool[T]) Closed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// --------------------------------------------------------------------- usage

// Use borrows a connection for the duration of fn. If fn returns an error the connection is
// released with that error (discarded if fatal or validation fails); otherwise it is reused.
func (p *ConnectionPool[T]) Use(timeout Timeout, fn func(conn T) error) error {
	conn, err := p.Acquire(timeout)
	if err != nil {
		return err
	}
	if ferr := fn(conn); ferr != nil {
		_ = p.release(conn, releaseOpts{err: ferr})
		return ferr
	}
	return p.release(conn, releaseOpts{})
}

// Acquire gets a connection. timeout Unset() uses config.AcquireTimeout; Infinite() waits
// forever; Of(0) never waits. Returns *PoolTimeoutError, *PoolClosedError,
// *ConnectionCreateError, *PoolExhaustedError or *CircuitOpenError.
func (p *ConnectionPool[T]) Acquire(timeout Timeout) (T, error) {
	var zero T
	started := p.clock.Monotonic()
	deadline := computeDeadline(timeout, p.cfg.AcquireTimeout, started)
	pooled, err := p.acquireLoop(deadline)
	if err != nil {
		if _, ok := err.(*PoolTimeoutError); ok {
			waited := p.clock.Monotonic() - started
			p.events.safe(func(l PoolListener[T]) { l.OnAcquireTimeout(waited) })
		}
		return zero, err
	}
	now := p.clock.Monotonic()
	// Mark under the lock: the connection is already visible in p.checkedOut, so the
	// maintenance goroutine's checkLeaks/healthCheckIdle may touch its fields concurrently.
	p.mu.Lock()
	pooled.MarkCheckedOut(now)
	p.mu.Unlock()
	wait := now - started
	p.events.safe(func(l PoolListener[T]) { l.OnAcquired(pooled, wait) })
	return pooled.Raw, nil
}

func (p *ConnectionPool[T]) acquireLoop(deadline *float64) (*PooledConnection[T], error) {
	for {
		pooled, err := p.takeIdleOrReserve(deadline)
		if err != nil {
			return nil, err
		}
		if pooled == nil { // slot reserved -> build a new one
			newp, createdIn, cerr := p.create(deadline)
			if cerr != nil {
				return nil, cerr
			}
			if rerr := p.registerNew(newp, createdIn); rerr != nil {
				return nil, rerr
			}
			return newp, nil
		}
		reason, has := p.checkBorrow(pooled)
		if !has {
			return pooled, nil
		}
		p.destroy(pooled, reason) // stale/dead: drop it and try again
	}
}

// Release returns a connection. Pass Discard() to force-close it, or WithError(err) to
// signal the caller's work failed.
func (p *ConnectionPool[T]) Release(conn T, opts ...ReleaseOption) error {
	var o releaseOpts
	for _, opt := range opts {
		opt(&o)
	}
	return p.release(conn, o)
}

func (p *ConnectionPool[T]) release(conn T, o releaseOpts) error {
	p.mu.Lock()
	pooled, ok := p.checkedOut[conn]
	if ok {
		delete(p.checkedOut, conn)
	}
	p.mu.Unlock()
	if !ok {
		return &InvalidReleaseError{Msg: "object is not checked out from this pool (double release?)"}
	}
	now := p.clock.Monotonic()
	hold := pooled.HoldTime(now)
	reason, has := p.checkRelease(pooled, conn, o.discard, o.err, now)
	if !has && !p.park(pooled, now) {
		reason, has = CloseReasonPoolClosed, true
	}
	if has {
		p.destroy(pooled, reason)
	}
	p.events.safe(func(l PoolListener[T]) { l.OnReleased(pooled, hold) })
	return nil
}

// Stats returns an immutable snapshot of pool counters and gauges.
func (p *ConnectionPool[T]) Stats() PoolStats {
	p.mu.Lock()
	g := statsGauges{
		Name: p.Name, MaxSize: p.cfg.MaxSize, Size: p.size,
		Idle: len(p.idle), InUse: len(p.checkedOut), Waiting: p.waiters,
	}
	p.mu.Unlock()
	if p.breaker != nil {
		s := p.breaker.State().Value()
		g.BreakerState = &s
	}
	return p.stats.snapshot(g)
}

// RunMaintenance performs one sweep: evict expired/over-idle connections, then refill to
// MinSize. Runs periodically after Open(); public for tests and cron-style use.
func (p *ConnectionPool[T]) RunMaintenance() {
	var doomed []doomedConn[T]
	closed := func() bool {
		p.mu.Lock()
		defer p.mu.Unlock() // defer so a panic in the sweep can't leave the lock held
		if p.closed {
			return true
		}
		var keep []*PooledConnection[T]
		keep, doomed = sweep(p.idle, p.size, p.cfg, p.clock.Monotonic())
		p.idle = keep
		return false
	}()
	if closed {
		return
	}
	for _, d := range doomed {
		p.destroy(d.Conn, d.Reason)
	}
	p.healthCheckIdle()
	p.fillToMin()
	p.checkLeaks()
}

func (p *ConnectionPool[T]) healthCheckIdle() {
	if !p.cfg.ValidateIdle {
		return
	}
	p.mu.Lock()
	batch := p.idle
	p.idle = nil
	for _, pooled := range batch {
		p.checkedOut[pooled.Raw] = pooled
	}
	p.mu.Unlock()
	for _, pooled := range batch {
		healthy := p.isHealthy(pooled.Raw)
		p.mu.Lock()
		delete(p.checkedOut, pooled.Raw)
		keep := healthy && !p.closed
		if keep {
			p.idle = append(p.idle, pooled)
			p.signal()
		}
		p.mu.Unlock()
		if !keep {
			reason := CloseReasonPoolClosed
			if !healthy {
				reason = CloseReasonUnhealthy
			}
			p.destroy(pooled, reason)
		}
	}
}

func (p *ConnectionPool[T]) checkLeaks() {
	timeout := p.cfg.LeakDetectionTimeout
	if timeout == nil {
		return
	}
	now := p.clock.Monotonic()
	type leak struct {
		conn *PooledConnection[T]
		held float64
	}
	var leaked []leak
	p.mu.Lock()
	for _, pooled := range p.checkedOut {
		if !pooled.LeakReported && pooled.HoldTime(now) >= *timeout {
			pooled.LeakReported = true
			leaked = append(leaked, leak{pooled, pooled.HoldTime(now)})
		}
	}
	p.mu.Unlock()
	for _, l := range leaked {
		log.Printf("pool %q: possible connection leak: %v held for %.1fs", p.Name, l.conn, l.held)
		p.events.safe(func(ls PoolListener[T]) { ls.OnConnectionLeak(l.conn, l.held) })
	}
}

// ----------------------------------------------------------------- internals

func (p *ConnectionPool[T]) takeIdleOrReserve(deadline *float64) (*PooledConnection[T], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.closed {
			return nil, &PoolClosedError{Msg: fmt.Sprintf("pool %q is closed", p.Name)}
		}
		conn, rest := popIdle(p.idle, p.cfg.IdleStrategy)
		if conn != nil {
			p.idle = rest
			p.checkedOut[conn.Raw] = conn
			return conn, nil
		}
		if p.size < p.cfg.MaxSize {
			p.size++
			return nil, nil
		}
		left := remaining(deadline, p.clock.Monotonic())
		if left != nil && *left <= 0 {
			return nil, &PoolTimeoutError{Msg: fmt.Sprintf("pool %q: no connection available (max_size=%d)", p.Name, p.cfg.MaxSize)}
		}
		if p.cfg.MaxWaiters != nil && p.waiters >= *p.cfg.MaxWaiters {
			return nil, &PoolExhaustedError{Msg: fmt.Sprintf("pool %q: waiter queue full (max_waiters=%d)", p.Name, *p.cfg.MaxWaiters)}
		}
		ch := p.notify
		p.waiters++
		p.mu.Unlock()
		waitSignal(ch, left)
		p.mu.Lock()
		p.waiters--
	}
}

func (p *ConnectionPool[T]) checkBorrow(pooled *PooledConnection[T]) (CloseReason, bool) {
	now := p.clock.Monotonic()
	if r, ok := borrowReason(pooled, p.cfg, now); ok {
		return r, true
	}
	if needsValidation(pooled, p.cfg, now) && !p.isHealthy(pooled.Raw) {
		return CloseReasonUnhealthy, true
	}
	return "", false
}

func (p *ConnectionPool[T]) checkRelease(pooled *PooledConnection[T], conn T, discard bool, cerr error, now float64) (CloseReason, bool) {
	if discard {
		return CloseReasonDiscarded, true
	}
	if cerr != nil && (p.factory.IsFatalError(cerr) || !p.isHealthy(conn)) {
		return CloseReasonError, true
	}
	if r, ok := rotationReason(pooled, p.cfg, now); ok {
		return r, true
	}
	if rerr := p.resetConn(conn); rerr != nil {
		log.Printf("pool %q: reset failed; discarding %v: %v", p.Name, pooled, rerr)
		return CloseReasonResetFailed, true
	}
	return "", false
}

func (p *ConnectionPool[T]) isHealthy(raw T) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
		}
	}()
	return p.factory.Validate(raw)
}

func (p *ConnectionPool[T]) resetConn(conn T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("reset panicked: %v", r)
		}
	}()
	return p.factory.Reset(conn)
}

func (p *ConnectionPool[T]) park(pooled *PooledConnection[T], now float64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	pooled.MarkReturned(now)
	p.idle = append(p.idle, pooled)
	p.signal()
	return true
}

func (p *ConnectionPool[T]) create(deadline *float64) (*PooledConnection[T], float64, error) {
	started := p.clock.Monotonic()
	raw, err := p.createRaw(deadline)
	if err != nil {
		p.freeSlot()
		return nil, 0, err
	}
	now := p.clock.Monotonic()
	return p.newConn(raw, now), now - started, nil
}

func (p *ConnectionPool[T]) registerNew(pooled *PooledConnection[T], createdIn float64) error {
	p.mu.Lock()
	var action string
	if p.closed {
		action = "closed"
	} else if _, dup := p.checkedOut[pooled.Raw]; dup || p.idleContains(pooled.Raw) {
		action = "duplicate"
	} else {
		p.checkedOut[pooled.Raw] = pooled
		action = "ok"
	}
	p.mu.Unlock()
	if action == "duplicate" {
		p.freeSlot()
		return &ConnectionCreateError{Msg: fmt.Sprintf(
			"pool %q: factory.Create() returned an object the pool already manages; each create must return a distinct connection", p.Name)}
	}
	p.events.safe(func(l PoolListener[T]) { l.OnConnectionCreated(pooled, createdIn) })
	if action == "closed" {
		p.destroy(pooled, CloseReasonPoolClosed)
		return &PoolClosedError{Msg: fmt.Sprintf("pool %q is closed", p.Name)}
	}
	return nil
}

// idleContains reports whether raw is already in the idle slice. Caller holds p.mu.
func (p *ConnectionPool[T]) idleContains(raw T) bool {
	for _, c := range p.idle {
		if c.Raw == raw {
			return true
		}
	}
	return false
}

func (p *ConnectionPool[T]) createRaw(deadline *float64) (T, error) {
	var zero T
	if p.breaker != nil {
		if err := p.breaker.BeforeCall(); err != nil {
			return zero, err
		}
	}
	raw, err := p.createWithRetry(deadline)
	if err != nil {
		if p.breaker != nil {
			if _, ok := err.(*ConnectionCreateError); ok {
				p.breaker.RecordFailure()
			} else {
				p.breaker.RecordAbort()
			}
		}
		return zero, err
	}
	if p.breaker != nil {
		p.breaker.RecordSuccess()
	}
	return raw, nil
}

func (p *ConnectionPool[T]) createWithRetry(deadline *float64) (T, error) {
	var zero T
	retry := p.cfg.Retry
	attempt := 0
	for {
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()
		if closed {
			return zero, &PoolClosedError{Msg: fmt.Sprintf("pool %q is closed", p.Name)}
		}
		attempt++
		raw, err := p.createOne()
		if err == nil {
			return raw, nil
		}
		p.events.safe(func(l PoolListener[T]) { l.OnCreateFailed(err) })
		if attempt >= retry.MaxAttempts || !retry.ShouldRetry(err) {
			return zero, &ConnectionCreateError{
				Msg:   fmt.Sprintf("pool %q: could not create connection after %d attempt(s): %v", p.Name, attempt, err),
				Cause: err,
			}
		}
		delay := p.retryDelay(attempt)
		left := remaining(deadline, p.clock.Monotonic())
		if left != nil && delay >= *left {
			return zero, &PoolTimeoutError{Msg: fmt.Sprintf("pool %q: acquire deadline reached while retrying creation", p.Name)}
		}
		p.clock.Sleep(delay)
	}
}

func (p *ConnectionPool[T]) createOne() (raw T, err error) {
	defer func() {
		if r := recover(); r != nil {
			var zero T
			raw, err = zero, fmt.Errorf("create panicked: %v", r)
		}
	}()
	return p.factory.Create()
}

func (p *ConnectionPool[T]) freeSlot() {
	p.mu.Lock()
	p.size--
	p.signal()
	p.mu.Unlock()
}

func (p *ConnectionPool[T]) destroy(pooled *PooledConnection[T], reason CloseReason) {
	p.closeConn(pooled)
	p.mu.Lock()
	delete(p.checkedOut, pooled.Raw)
	p.size--
	p.signal()
	p.mu.Unlock()
	p.events.safe(func(l PoolListener[T]) { l.OnConnectionClosed(pooled, reason) })
}

func (p *ConnectionPool[T]) closeConn(pooled *PooledConnection[T]) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("pool %q: error closing %v: %v", p.Name, pooled, r)
		}
	}()
	if err := p.factory.Close(pooled.Raw); err != nil {
		log.Printf("pool %q: error closing %v: %v", p.Name, pooled, err)
	}
}

func (p *ConnectionPool[T]) fillToMin() {
	for {
		p.mu.Lock()
		if p.closed || p.size >= p.cfg.MinSize {
			p.mu.Unlock()
			return
		}
		p.size++
		p.mu.Unlock()
		pooled, createdIn, err := p.create(nil)
		if err != nil {
			log.Printf("pool %q: warm-up failed: %v", p.Name, err)
			return
		}
		p.events.safe(func(l PoolListener[T]) { l.OnConnectionCreated(pooled, createdIn) })
		if !p.park(pooled, p.clock.Monotonic()) {
			p.destroy(pooled, CloseReasonPoolClosed)
		}
	}
}

func (p *ConnectionPool[T]) maintenanceLoop() {
	defer p.wg.Done()
	interval := time.Duration(*p.cfg.MaintenanceInterval * float64(time.Second))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.runMaintenanceSafe()
		}
	}
}

func (p *ConnectionPool[T]) runMaintenanceSafe() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("pool %q: maintenance pass failed: %v", p.Name, r)
		}
	}()
	p.RunMaintenance()
}
