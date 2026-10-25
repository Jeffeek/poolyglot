package connpool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// AsyncConnectionPool is the context-based twin of ConnectionPool: the same model and the
// same rules (lifecycle.go), but every blocking method takes a context.Context for
// cancellation, and CreateTimeout is enforced here (via a derived context) rather than left
// to the factory.
//
// Cancellation never leaks a slot: a context that is done mid-wait, mid-create or
// mid-release unwinds the reservation and (for a half-built connection) disposes it.
// Disposal itself is "shielded" — factory.Close runs under context.Background(), so a
// cancelled caller context can neither interrupt backend disposal nor unbalance the
// created/closed accounting.
type AsyncConnectionPool[T comparable] struct {
	Name    string
	factory AsyncConnectionFactory[T]
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

// NewAsyncConnectionPool builds an async pool. It validates config. opts may be nil.
func NewAsyncConnectionPool[T comparable](factory AsyncConnectionFactory[T], config PoolConfig, opts *Options[T]) (*AsyncConnectionPool[T], error) {
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
	p := &AsyncConnectionPool[T]{
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

func (p *AsyncConnectionPool[T]) signal() {
	close(p.notify)
	p.notify = make(chan struct{})
}

func (p *AsyncConnectionPool[T]) newConn(raw T, now float64) *PooledConnection[T] {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return newConnection(raw, p.cfg, now, p.rng)
}

func (p *AsyncConnectionPool[T]) retryDelay(attempt int) float64 {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return p.cfg.Retry.DelayFor(attempt, p.rng)
}

func (p *AsyncConnectionPool[T]) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// ------------------------------------------------------------------ lifecycle

// Open warms up MinSize connections and starts background maintenance. Idempotent.
func (p *AsyncConnectionPool[T]) Open(ctx context.Context) (*AsyncConnectionPool[T], error) {
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
	wantThread := p.cfg.MaintenanceInterval != nil
	p.mu.Unlock()

	p.fillToMin(ctx)

	start := false
	p.mu.Lock()
	if wantThread && !p.closed {
		p.maintRunning = true
		start = true
	}
	p.mu.Unlock()
	if start {
		p.wg.Add(1)
		go p.maintenanceLoop()
	}
	return p, nil
}

// Close closes idle connections now; checked-out ones close on release. Idempotent.
// timeout (nil = don't wait) drains gracefully up to that many seconds.
func (p *AsyncConnectionPool[T]) Close(ctx context.Context, timeout *float64) error {
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
			werr := waitSignalCtx(ctx, ch, &left)
			p.mu.Lock()
			if werr != nil {
				break
			}
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
func (p *AsyncConnectionPool[T]) Closed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// --------------------------------------------------------------------- usage

// Use borrows a connection for the duration of fn (see ConnectionPool.Use).
func (p *AsyncConnectionPool[T]) Use(ctx context.Context, timeout Timeout, fn func(conn T) error) error {
	conn, err := p.Acquire(ctx, timeout)
	if err != nil {
		return err
	}
	if ferr := fn(conn); ferr != nil {
		_ = p.release(conn, releaseOpts{err: ferr})
		return ferr
	}
	return p.release(conn, releaseOpts{})
}

// Acquire gets a connection, honoring ctx cancellation and the acquire timeout.
func (p *AsyncConnectionPool[T]) Acquire(ctx context.Context, timeout Timeout) (T, error) {
	var zero T
	started := p.clock.Monotonic()
	deadline := computeDeadline(timeout, p.cfg.AcquireTimeout, started)
	pooled, err := p.acquireLoop(ctx, deadline)
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

func (p *AsyncConnectionPool[T]) acquireLoop(ctx context.Context, deadline *float64) (*PooledConnection[T], error) {
	for {
		pooled, err := p.takeIdleOrReserve(ctx, deadline)
		if err != nil {
			return nil, err
		}
		if pooled == nil { // slot reserved -> build a new one
			newp, createdIn, cerr := p.create(ctx, deadline)
			if cerr != nil {
				return nil, cerr
			}
			if rerr := p.registerNew(newp, createdIn); rerr != nil {
				return nil, rerr // registerNew already cleaned up its own cases (duplicate/closed)
			}
			return newp, nil
		}
		reason, has := p.checkBorrow(ctx, pooled)
		if !has {
			return pooled, nil
		}
		p.destroy(pooled, reason)
	}
}

// Release returns a connection (see ConnectionPool.Release for options).
func (p *AsyncConnectionPool[T]) Release(conn T, opts ...ReleaseOption) error {
	var o releaseOpts
	for _, opt := range opts {
		opt(&o)
	}
	return p.release(conn, o)
}

func (p *AsyncConnectionPool[T]) release(conn T, o releaseOpts) error {
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
func (p *AsyncConnectionPool[T]) Stats() PoolStats {
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

// RunMaintenance performs one sweep (see ConnectionPool.RunMaintenance).
func (p *AsyncConnectionPool[T]) RunMaintenance(ctx context.Context) {
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
	p.healthCheckIdle(ctx)
	p.fillToMin(ctx)
	p.checkLeaks()
}

func (p *AsyncConnectionPool[T]) healthCheckIdle(ctx context.Context) {
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
		healthy := p.isHealthy(ctx, pooled.Raw)
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

func (p *AsyncConnectionPool[T]) checkLeaks() {
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

func (p *AsyncConnectionPool[T]) takeIdleOrReserve(ctx context.Context, deadline *float64) (*PooledConnection[T], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
		werr := waitSignalCtx(ctx, ch, left)
		p.mu.Lock()
		p.waiters--
		if werr != nil {
			return nil, werr
		}
	}
}

func (p *AsyncConnectionPool[T]) checkBorrow(ctx context.Context, pooled *PooledConnection[T]) (CloseReason, bool) {
	now := p.clock.Monotonic()
	if r, ok := borrowReason(pooled, p.cfg, now); ok {
		return r, true
	}
	if needsValidation(pooled, p.cfg, now) && !p.isHealthy(ctx, pooled.Raw) {
		return CloseReasonUnhealthy, true
	}
	return "", false
}

func (p *AsyncConnectionPool[T]) checkRelease(pooled *PooledConnection[T], conn T, discard bool, cerr error, now float64) (CloseReason, bool) {
	if discard {
		return CloseReasonDiscarded, true
	}
	if cerr != nil && (p.factory.IsFatalError(cerr) || !p.isHealthy(context.Background(), conn)) {
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

func (p *AsyncConnectionPool[T]) isHealthy(ctx context.Context, raw T) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
		}
	}()
	return p.factory.Validate(ctx, raw)
}

func (p *AsyncConnectionPool[T]) resetConn(conn T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("reset panicked: %v", r)
		}
	}()
	return p.factory.Reset(context.Background(), conn)
}

func (p *AsyncConnectionPool[T]) park(pooled *PooledConnection[T], now float64) bool {
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

func (p *AsyncConnectionPool[T]) create(ctx context.Context, deadline *float64) (*PooledConnection[T], float64, error) {
	started := p.clock.Monotonic()
	raw, err := p.createRaw(ctx, deadline)
	if err != nil {
		p.freeSlot()
		return nil, 0, err
	}
	now := p.clock.Monotonic()
	return p.newConn(raw, now), now - started, nil
}

func (p *AsyncConnectionPool[T]) registerNew(pooled *PooledConnection[T], createdIn float64) error {
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

// idleContains reports whether raw is already idle. Caller holds p.mu.
func (p *AsyncConnectionPool[T]) idleContains(raw T) bool {
	for _, c := range p.idle {
		if c.Raw == raw {
			return true
		}
	}
	return false
}

func (p *AsyncConnectionPool[T]) createRaw(ctx context.Context, deadline *float64) (T, error) {
	var zero T
	if p.breaker != nil {
		if err := p.breaker.BeforeCall(); err != nil {
			return zero, err
		}
	}
	raw, err := p.createWithRetry(ctx, deadline)
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

// createLimit is the tightest of create_timeout and the remaining acquire deadline (nil = no
// limit). Mirrors async_pool._create_limit.
func (p *AsyncConnectionPool[T]) createLimit(deadline *float64) *float64 {
	var limits []float64
	if p.cfg.CreateTimeout != nil {
		limits = append(limits, *p.cfg.CreateTimeout)
	}
	if r := remaining(deadline, p.clock.Monotonic()); r != nil {
		limits = append(limits, *r)
	}
	if len(limits) == 0 {
		return nil
	}
	m := limits[0]
	for _, x := range limits[1:] {
		if x < m {
			m = x
		}
	}
	if m < 0 {
		m = 0
	}
	return &m
}

func (p *AsyncConnectionPool[T]) createWithRetry(ctx context.Context, deadline *float64) (T, error) {
	var zero T
	retry := p.cfg.Retry
	attempt := 0
	for {
		if p.isClosed() {
			return zero, &PoolClosedError{Msg: fmt.Sprintf("pool %q is closed", p.Name)}
		}
		attempt++
		raw, err := p.createOne(ctx, p.createLimit(deadline))
		if err == nil {
			return raw, nil
		}
		// Caller cancellation / caller-supplied deadline is not a backend fault: surface it so
		// createRaw records an abort, not a breaker failure.
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		left := remaining(deadline, p.clock.Monotonic())
		// A timeout whose binding limit was the acquire deadline is caller impatience.
		if errors.Is(err, context.DeadlineExceeded) && left != nil && *left <= 0 {
			return zero, &PoolTimeoutError{Msg: fmt.Sprintf("pool %q: acquire deadline reached while creating connection", p.Name)}
		}
		// Everything else (including a create_timeout firing) is a backend fault.
		p.events.safe(func(l PoolListener[T]) { l.OnCreateFailed(err) })
		if attempt >= retry.MaxAttempts || !retry.ShouldRetry(err) {
			return zero, &ConnectionCreateError{
				Msg:   fmt.Sprintf("pool %q: could not create connection after %d attempt(s): %v", p.Name, attempt, err),
				Cause: err,
			}
		}
		delay := p.retryDelay(attempt)
		if left != nil && delay >= *left {
			return zero, &PoolTimeoutError{Msg: fmt.Sprintf("pool %q: acquire deadline reached while retrying creation", p.Name)}
		}
		if serr := p.clock.SleepContext(ctx, delay); serr != nil {
			return zero, serr // cancelled during backoff -> abort
		}
	}
}

func (p *AsyncConnectionPool[T]) createOne(ctx context.Context, limit *float64) (raw T, err error) {
	defer func() {
		if r := recover(); r != nil {
			var zero T
			raw, err = zero, fmt.Errorf("create panicked: %v", r)
		}
	}()
	cctx := ctx
	if limit != nil {
		var cancel context.CancelFunc
		cctx, cancel = context.WithTimeout(ctx, time.Duration(*limit*float64(time.Second)))
		defer cancel()
	}
	raw, err = p.factory.Create(cctx)
	// Normalize a create-limit expiry (the derived context firing while the caller context is
	// still live) to DeadlineExceeded so createWithRetry can classify it.
	if err != nil && limit != nil && cctx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
		err = context.DeadlineExceeded
	}
	return raw, err
}

func (p *AsyncConnectionPool[T]) freeSlot() {
	p.mu.Lock()
	p.size--
	p.signal()
	p.mu.Unlock()
}

func (p *AsyncConnectionPool[T]) destroy(pooled *PooledConnection[T], reason CloseReason) {
	p.closeConn(pooled)
	p.mu.Lock()
	delete(p.checkedOut, pooled.Raw)
	p.size--
	p.signal()
	p.mu.Unlock()
	p.events.safe(func(l PoolListener[T]) { l.OnConnectionClosed(pooled, reason) })
}

func (p *AsyncConnectionPool[T]) closeConn(pooled *PooledConnection[T]) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("pool %q: error closing %v: %v", p.Name, pooled, r)
		}
	}()
	// Shielded: background context so a cancelled caller cannot interrupt disposal.
	if err := p.factory.Close(context.Background(), pooled.Raw); err != nil {
		log.Printf("pool %q: error closing %v: %v", p.Name, pooled, err)
	}
}

func (p *AsyncConnectionPool[T]) fillToMin(ctx context.Context) {
	for {
		p.mu.Lock()
		if p.closed || p.size >= p.cfg.MinSize {
			p.mu.Unlock()
			return
		}
		p.size++
		p.mu.Unlock()
		pooled, createdIn, err := p.create(ctx, nil)
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

func (p *AsyncConnectionPool[T]) maintenanceLoop() {
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

func (p *AsyncConnectionPool[T]) runMaintenanceSafe() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("pool %q: maintenance pass failed: %v", p.Name, r)
		}
	}()
	p.RunMaintenance(context.Background())
}
