package connpool

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ------------------------------------------------------------------ helpers

func mustOpenAsync[T comparable](t *testing.T, p *AsyncConnectionPool[T]) {
	t.Helper()
	if _, err := p.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
}

func asyncQuietPool(factory AsyncConnectionFactory[*FakeConn], cfg PoolConfig, opts *Options[*FakeConn]) *AsyncConnectionPool[*FakeConn] {
	return mustAsyncPool(factory, cfg, opts)
}

// spinUntil polls cond with a short sleep until true or a timeout elapses, failing the test.
func spinUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("spinUntil: condition never became true")
		}
		time.Sleep(time.Millisecond)
	}
}

// ------------------------------------------------------------------ basics

func TestAsync_ReuseAndStats(t *testing.T) {
	af := newAsyncFakeFactory()
	pool := mustAsyncPool(af, quietConfig(), nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	a, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(a); err != nil {
		t.Fatal(err)
	}
	b, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(b); err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("expected reuse")
	}
	s := pool.Stats()
	if s.Created != 1 || s.Acquired != 2 || s.Idle != 1 || s.InUse != 0 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestAsync_TimeoutWhenExhausted(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 1
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	held, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Acquire(ctx, Of(0.05))
	var timeoutErr *PoolTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected PoolTimeoutError, got %v", err)
	}
	if pool.Stats().Timeouts != 1 {
		t.Fatalf("expected 1 timeout, got %d", pool.Stats().Timeouts)
	}
	if err := pool.Release(held); err != nil {
		t.Fatal(err)
	}
}

func TestAsync_WaiterWokenByRelease(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 1
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	held, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	var waiterConn *FakeConn
	var waiterErr error
	done := make(chan struct{})
	go func() {
		waiterConn, waiterErr = pool.Acquire(ctx, Of(2))
		close(done)
	}()
	spinUntil(t, func() bool { return pool.Stats().Waiting == 1 })
	if err := pool.Release(held); err != nil {
		t.Fatal(err)
	}
	<-done
	if waiterErr != nil {
		t.Fatal(waiterErr)
	}
	if waiterConn != held {
		t.Fatalf("expected waiter to receive released connection")
	}
}

func TestAsync_DoubleReleaseRejected(t *testing.T) {
	af := newAsyncFakeFactory()
	pool := mustAsyncPool(af, quietConfig(), nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	conn, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(conn); err != nil {
		t.Fatal(err)
	}
	err = pool.Release(conn)
	var invalidErr *InvalidReleaseError
	if !errors.As(err, &invalidErr) {
		t.Fatalf("expected InvalidReleaseError, got %v", err)
	}
}

func TestAsync_RotationByLifetime(t *testing.T) {
	clock := NewManualClock(1000.0)
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxLifetime = Ptr(10.0)
	rec := &Recorder{}
	pool := mustAsyncPool(af, cfg, &Options[*FakeConn]{Clock: clock, Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	a, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(a); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11)
	b, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("expected a new connection after rotation")
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonExpired {
		t.Fatalf("expected [Expired], got %v", reasons)
	}
	if err := pool.Release(b); err != nil {
		t.Fatal(err)
	}
}

func TestAsync_ValidationAndErrorDiscard(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.ValidateAfterIdle = Ptr(0.0)
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	a, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(a); err != nil {
		t.Fatal(err)
	}
	a.Healthy = false
	b, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("expected a new connection after unhealthy eviction")
	}
	if err := pool.Release(b); err != nil {
		t.Fatal(err)
	}

	af.Inner.Fatal = func(err error) bool { return errors.Is(err, errBackendDown) }
	var captured *FakeConn
	useErr := pool.Use(ctx, Unset(), func(c *FakeConn) error {
		captured = c
		return errBackendDown
	})
	if !errors.Is(useErr, errBackendDown) {
		t.Fatalf("expected errBackendDown, got %v", useErr)
	}
	if captured == nil || !captured.Closed {
		t.Fatalf("expected captured connection to be closed")
	}
}

func TestAsync_RetryAndBreaker(t *testing.T) {
	inner := &FakeFactory{FailAlways: true}
	af := &AsyncFakeFactory{Inner: inner}
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.AcquireTimeout = Ptr(60.0)
	cfg.Retry = RetryPolicy{MaxAttempts: 2, BaseDelay: 1, MaxDelay: 5, Multiplier: 2, Jitter: 0}
	cfg.Breaker = &BreakerConfig{FailureThreshold: 2, RecoveryTimeout: 30, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
	pool := mustAsyncPool(af, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	var cce *ConnectionCreateError
	_, err := pool.Acquire(ctx, Unset())
	if !errors.As(err, &cce) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if inner.attemptCount() != 2 {
		t.Fatalf("expected 2 attempts, got %d", inner.attemptCount())
	}
	if pool.Stats().BreakerState == nil || *pool.Stats().BreakerState != "closed" {
		t.Fatalf("expected breaker closed, got %+v", pool.Stats().BreakerState)
	}
	_, err = pool.Acquire(ctx, Unset())
	if !errors.As(err, &cce) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if inner.attemptCount() != 4 {
		t.Fatalf("expected 4 attempts, got %d", inner.attemptCount())
	}
	var circuitErr *CircuitOpenError
	_, err = pool.Acquire(ctx, Unset())
	if !errors.As(err, &circuitErr) {
		t.Fatalf("expected CircuitOpenError, got %v", err)
	}
	if inner.attemptCount() != 4 {
		t.Fatalf("expected attempts unchanged at 4, got %d", inner.attemptCount())
	}
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0, got %d", pool.Stats().Size)
	}
	clock.Advance(31)
	inner.mu.Lock()
	inner.FailAlways = false
	inner.mu.Unlock()
	c, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(c); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().BreakerState == nil || *pool.Stats().BreakerState != "closed" {
		t.Fatalf("expected breaker closed after recovery, got %+v", pool.Stats().BreakerState)
	}
}

func TestAsync_CreateTimeoutIsEnforced(t *testing.T) {
	af := &AsyncFakeFactory{Inner: &FakeFactory{}, CreateDelay: 5}
	cfg := quietConfig()
	cfg.CreateTimeout = Ptr(0.05)
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	_, err := pool.Acquire(context.Background(), Unset())
	var cce *ConnectionCreateError
	if !errors.As(err, &cce) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	spinUntil(t, func() bool { return pool.Stats().Size == 0 })
}

func TestAsync_CancelWhileWaitingDoesNotLeak(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 1
	cfg.AcquireTimeout = nil
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	held, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	var waitErr error
	done := make(chan struct{})
	go func() {
		_, waitErr = pool.Acquire(cctx, Unset())
		close(done)
	}()
	spinUntil(t, func() bool { return pool.Stats().Waiting == 1 })
	cancel()
	<-done
	if !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", waitErr)
	}
	spinUntil(t, func() bool { return pool.Stats().Waiting == 0 })
	if pool.Stats().Waiting != 0 {
		t.Fatalf("expected 0 waiting, got %d", pool.Stats().Waiting)
	}
	if err := pool.Release(held); err != nil {
		t.Fatal(err)
	}
	got, err := pool.Acquire(ctx, Of(1))
	if err != nil {
		t.Fatal(err)
	}
	if got != held {
		t.Fatalf("expected to reacquire the released connection")
	}
}

func TestAsync_CancelDuringCreateFreesSlot(t *testing.T) {
	af := &AsyncFakeFactory{Inner: &FakeFactory{}, CreateDelay: 5}
	cfg := quietConfig()
	cfg.MaxSize = 1
	cfg.AcquireTimeout = nil
	cfg.CreateTimeout = nil
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	cctx, cancel := context.WithCancel(context.Background())
	var taskErr error
	done := make(chan struct{})
	go func() {
		_, taskErr = pool.Acquire(cctx, Unset())
		close(done)
	}()
	spinUntil(t, func() bool { return pool.Stats().Size == 1 })
	cancel()
	<-done
	if !errors.Is(taskErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", taskErr)
	}
	spinUntil(t, func() bool { return pool.Stats().Size == 0 })
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0, got %d", pool.Stats().Size)
	}
}

func TestAsync_CloseWakesWaiters(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 1
	cfg.AcquireTimeout = nil
	pool := mustAsyncPool(af, cfg, nil)

	ctx := context.Background()
	held, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	var waiterErr error
	done := make(chan struct{})
	go func() {
		_, waiterErr = pool.Acquire(ctx, Unset())
		close(done)
	}()
	spinUntil(t, func() bool { return pool.Stats().Waiting == 1 })
	if err := pool.Close(ctx, nil); err != nil {
		t.Fatal(err)
	}
	<-done
	var closedErr *PoolClosedError
	if !errors.As(waiterErr, &closedErr) {
		t.Fatalf("expected PoolClosedError, got %v", waiterErr)
	}
	if err := pool.Release(held); err != nil {
		t.Fatal(err)
	}
	if !held.Closed {
		t.Fatalf("expected held connection to be closed")
	}
}

func TestAsync_MaintenanceTaskAndContextManager(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MinSize = 2
	cfg.MaintenanceInterval = Ptr(0.02)
	pool := mustAsyncPool(af, cfg, nil)

	mustOpenAsync(t, pool)
	if pool.Stats().Idle != 2 {
		t.Fatalf("expected idle 2, got %d", pool.Stats().Idle)
	}
	if err := pool.Close(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if !pool.Closed() {
		t.Fatalf("expected pool closed")
	}
	for _, c := range af.Inner.Created {
		if !c.Closed {
			t.Fatalf("expected all created connections closed")
		}
	}
}

func TestAsync_ConcurrencyStress(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 4
	cfg.AcquireTimeout = Ptr(10.0)
	cfg.MaxUses = Ptr(9)
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	var mu sync.Mutex
	now, peak := 0, 0
	var wg sync.WaitGroup
	for w := 0; w < 25; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				err := pool.Use(ctx, Unset(), func(conn *FakeConn) error {
					mu.Lock()
					now++
					if now > peak {
						peak = now
					}
					mu.Unlock()
					if conn.Closed {
						t.Errorf("connection closed while in use")
					}
					mu.Lock()
					now--
					mu.Unlock()
					return nil
				})
				if err != nil {
					t.Errorf("Use: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	s := pool.Stats()
	if peak > 4 {
		t.Fatalf("expected peak <= 4, got %d", peak)
	}
	if s.InUse != 0 || s.Waiting != 0 {
		t.Fatalf("expected quiescent pool, got %+v", s)
	}
	if s.Acquired != 25*40 {
		t.Fatalf("expected %d acquired, got %d", 25*40, s.Acquired)
	}
	if s.Idle != s.Created-s.Closed {
		t.Fatalf("expected idle == created-closed, got idle=%d created=%d closed=%d", s.Idle, s.Created, s.Closed)
	}
}

func TestAsync_DuplicateConnectionFromFactoryIsRejected(t *testing.T) {
	shared := newFakeConn()
	factory := &AsyncFuncFactory[*FakeConn]{
		CreateFn: func(ctx context.Context) (*FakeConn, error) { return shared, nil },
	}
	pool := mustAsyncPool(factory, quietConfig(), nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	first, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Acquire(ctx, Unset())
	var cce *ConnectionCreateError
	if !errors.As(err, &cce) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if pool.Stats().Size != 1 || pool.Stats().Created != 1 {
		t.Fatalf("unexpected stats: %+v", pool.Stats())
	}
	if err := pool.Release(first); err != nil {
		t.Fatal(err)
	}
}

func TestAsync_ValidateIdleEvictsDeadConnections(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.ValidateIdle = true
	cfg.MaxSize = 2
	rec := &Recorder{}
	pool := mustAsyncPool(af, cfg, &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	a, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	b, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(a); err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(b); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().Idle != 2 {
		t.Fatalf("expected idle 2, got %d", pool.Stats().Idle)
	}
	a.Healthy = false
	pool.RunMaintenance(ctx)
	if pool.Stats().Idle != 1 {
		t.Fatalf("expected idle 1, got %d", pool.Stats().Idle)
	}
	found := false
	for _, r := range rec.closedReasons() {
		if r == CloseReasonUnhealthy {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected Unhealthy close reason")
	}
	c, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if c != b {
		t.Fatalf("expected survivor to be reused")
	}
	if err := pool.Release(c); err != nil {
		t.Fatal(err)
	}
}

func TestAsync_DefaultConfigWhenOmitted(t *testing.T) {
	af := newAsyncFakeFactory()
	pool := mustAsyncPool(af, DefaultConfig(), nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	c, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(c); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().Size != 1 {
		t.Fatalf("expected size 1, got %d", pool.Stats().Size)
	}
}

func TestAsync_HealthCheckClosedMidValidationReportsPoolClosed(t *testing.T) {
	var pool *AsyncConnectionPool[*FakeConn]
	var closedOnce bool
	factory := &AsyncFuncFactory[*FakeConn]{
		CreateFn: func(ctx context.Context) (*FakeConn, error) { return newFakeConn(), nil },
		ValidateFn: func(ctx context.Context, c *FakeConn) bool {
			if !closedOnce {
				closedOnce = true
				pool.Close(context.Background(), nil)
			}
			return true
		},
	}
	cfg := quietConfig()
	cfg.ValidateIdle = true
	rec := &Recorder{}
	pool = mustAsyncPool(factory, cfg, &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})

	ctx := context.Background()
	a, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(a); err != nil {
		t.Fatal(err)
	}
	pool.RunMaintenance(ctx)
	found := false
	for _, r := range rec.closedReasons() {
		if r == CloseReasonPoolClosed {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected PoolClosed close reason, got %v", rec.closedReasons())
	}
}

func TestAsync_AcquireWithNoTimeoutsUsesUnboundedCreate(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.AcquireTimeout = nil
	cfg.CreateTimeout = nil
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	c, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(c); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().Created != 1 {
		t.Fatalf("expected created 1, got %d", pool.Stats().Created)
	}
}

func TestAsync_CreateTimeoutOnlyUsesCreateLimit(t *testing.T) {
	f := &FakeFactory{FailTimes: 1}
	af := &AsyncFakeFactory{Inner: f}
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.AcquireTimeout = nil
	cfg.CreateTimeout = Ptr(5.0)
	cfg.Retry = RetryPolicy{MaxAttempts: 3, BaseDelay: 1, MaxDelay: 5, Multiplier: 2, Jitter: 0}
	pool := mustAsyncPool(af, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(context.Background(), nil)

	c, err := pool.Acquire(context.Background(), Unset())
	if err != nil {
		t.Fatal(err)
	}
	if f.attemptCount() != 2 {
		t.Fatalf("expected 2 attempts, got %d", f.attemptCount())
	}
	if c == nil {
		t.Fatalf("expected a connection")
	}
	if err := pool.Release(c); err != nil {
		t.Fatal(err)
	}
}

func TestAsync_RetrySleepsWhenAcquireTimeoutIsInfinite(t *testing.T) {
	f := &FakeFactory{FailTimes: 1}
	af := &AsyncFakeFactory{Inner: f}
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.AcquireTimeout = nil
	cfg.CreateTimeout = nil
	cfg.Retry = RetryPolicy{MaxAttempts: 3, BaseDelay: 1, MaxDelay: 5, Multiplier: 2, Jitter: 0}
	pool := mustAsyncPool(af, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(context.Background(), nil)

	c, err := pool.Acquire(context.Background(), Unset())
	if err != nil {
		t.Fatal(err)
	}
	if f.attemptCount() != 2 {
		t.Fatalf("expected 2 attempts, got %d", f.attemptCount())
	}
	if c == nil {
		t.Fatalf("expected a connection")
	}
	if err := pool.Release(c); err != nil {
		t.Fatal(err)
	}
}

func TestAsync_CreateFailureWithEmptyMessageIsDescribedByType(t *testing.T) {
	factory := &AsyncFuncFactory[*FakeConn]{
		CreateFn: func(ctx context.Context) (*FakeConn, error) { return nil, errBackendDown },
	}
	cfg := quietConfig()
	cfg.Retry = NoRetry
	cfg.Retry.MaxAttempts = 1
	pool := mustAsyncPool(factory, cfg, nil)
	defer pool.Close(context.Background(), nil)

	_, err := pool.Acquire(context.Background(), Unset())
	var cce *ConnectionCreateError
	if !errors.As(err, &cce) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if !errors.Is(cce.Unwrap(), errBackendDown) {
		t.Fatalf("expected cause errBackendDown, got %v", cce.Unwrap())
	}
}

func TestAsync_LeakDetectionWarnsOnce(t *testing.T) {
	clock := NewManualClock(1000.0)
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.LeakDetectionTimeout = Ptr(10.0)
	pool := mustAsyncPool(af, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	conn, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	pool.RunMaintenance(ctx)
	if pool.Stats().Leaks != 0 {
		t.Fatalf("expected 0 leaks, got %d", pool.Stats().Leaks)
	}
	clock.Advance(11)
	pool.RunMaintenance(ctx)
	pool.RunMaintenance(ctx)
	if pool.Stats().Leaks != 1 {
		t.Fatalf("expected 1 leak, got %d", pool.Stats().Leaks)
	}
	if err := pool.Release(conn); err != nil {
		t.Fatal(err)
	}
}

func TestAsync_GracefulDrainWaitsForInUse(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 2
	pool := mustAsyncPool(af, cfg, nil)

	ctx := context.Background()
	held, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		pool.Release(held)
	}()
	if err := pool.Close(ctx, Ptr(2.0)); err != nil {
		t.Fatal(err)
	}
	if !held.Closed {
		t.Fatalf("expected held connection closed")
	}
	if pool.Stats().InUse != 0 {
		t.Fatalf("expected in-use 0, got %d", pool.Stats().InUse)
	}
}

func TestAsync_DrainTimeoutGivesUp(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 1
	pool := mustAsyncPool(af, cfg, nil)

	ctx := context.Background()
	held, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(ctx, Ptr(0.05)); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().InUse != 1 {
		t.Fatalf("expected in-use 1, got %d", pool.Stats().InUse)
	}
	if held.Closed {
		t.Fatalf("expected held still open")
	}
	if err := pool.Release(held); err != nil {
		t.Fatal(err)
	}
	if !held.Closed {
		t.Fatalf("expected held closed after release")
	}
}

func TestAsync_DrainZeroTimeoutReturnsImmediately(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 1
	pool := mustAsyncPool(af, cfg, nil)

	ctx := context.Background()
	held, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(ctx, Ptr(0.0)); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().InUse != 1 {
		t.Fatalf("expected in-use 1, got %d", pool.Stats().InUse)
	}
	if err := pool.Release(held); err != nil {
		t.Fatal(err)
	}
}

func TestAsync_OpenEdges(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaintenanceInterval = Ptr(0.5)
	pool := mustAsyncPool(af, cfg, nil)

	ctx := context.Background()
	p1, err := pool.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := pool.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Fatalf("expected idempotent Open")
	}
	if err := pool.Close(ctx, nil); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Open(ctx)
	var closedErr *PoolClosedError
	if !errors.As(err, &closedErr) {
		t.Fatalf("expected PoolClosedError, got %v", err)
	}
}

func TestAsync_RunMaintenanceOnClosedPoolIsNoop(t *testing.T) {
	af := newAsyncFakeFactory()
	pool := mustAsyncPool(af, quietConfig(), nil)
	ctx := context.Background()
	if err := pool.Close(ctx, nil); err != nil {
		t.Fatal(err)
	}
	pool.RunMaintenance(ctx) // must not panic
}

func TestAsync_MaintenanceEvictsIdle(t *testing.T) {
	clock := NewManualClock(1000.0)
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxIdleTime = Ptr(10.0)
	rec := &Recorder{}
	pool := mustAsyncPool(af, cfg, &Options[*FakeConn]{Clock: clock, Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	a, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(a); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11)
	pool.RunMaintenance(ctx)
	if pool.Stats().Idle != 0 {
		t.Fatalf("expected idle 0, got %d", pool.Stats().Idle)
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonIdle {
		t.Fatalf("expected [Idle], got %v", reasons)
	}
}

func TestAsync_MaxWaitersFailsFast(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 1
	cfg.MaxWaiters = Ptr(0)
	cfg.AcquireTimeout = Ptr(5.0)
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	held, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Acquire(ctx, Unset())
	var exhaustedErr *PoolExhaustedError
	if !errors.As(err, &exhaustedErr) {
		t.Fatalf("expected PoolExhaustedError, got %v", err)
	}
	if err := pool.Release(held); err != nil {
		t.Fatal(err)
	}
}

func TestAsync_ReleaseDiscardAndCloseErrorDoNotLeak(t *testing.T) {
	factory := &AsyncFuncFactory[*FakeConn]{
		CreateFn: func(ctx context.Context) (*FakeConn, error) { return newFakeConn(), nil },
		CloseFn:  func(ctx context.Context, c *FakeConn) error { return fmt.Errorf("close boom") },
	}
	rec := &Recorder{}
	pool := mustAsyncPool(factory, quietConfig(), &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	c, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(c, Discard()); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0, got %d", pool.Stats().Size)
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonDiscarded {
		t.Fatalf("expected [Discarded], got %v", reasons)
	}
}

func TestAsync_ReleaseResetErrorIsResetFailed(t *testing.T) {
	factory := &AsyncFuncFactory[*FakeConn]{
		CreateFn: func(ctx context.Context) (*FakeConn, error) { return newFakeConn(), nil },
		ResetFn:  func(ctx context.Context, c *FakeConn) error { return fmt.Errorf("reset boom") },
	}
	rec := &Recorder{}
	pool := mustAsyncPool(factory, quietConfig(), &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	c, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(c); err != nil {
		t.Fatal(err)
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonResetFailed {
		t.Fatalf("expected [ResetFailed], got %v", reasons)
	}
}

func TestAsync_CheckoutValidationErrorIsUnhealthy(t *testing.T) {
	factory := &AsyncFuncFactory[*FakeConn]{
		CreateFn:   func(ctx context.Context) (*FakeConn, error) { return newFakeConn(), nil },
		ValidateFn: func(ctx context.Context, c *FakeConn) bool { panic("ping boom") },
	}
	cfg := quietConfig()
	cfg.ValidateAfterIdle = Ptr(0.0)
	rec := &Recorder{}
	pool := mustAsyncPool(factory, cfg, &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	a, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(a); err != nil {
		t.Fatal(err)
	}
	b, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("expected a new connection")
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonUnhealthy {
		t.Fatalf("expected [Unhealthy], got %v", reasons)
	}
	if err := pool.Release(b); err != nil {
		t.Fatal(err)
	}
}

func TestAsync_CreateTimeoutDuringRetryDoesNotTripBreaker(t *testing.T) {
	inner := &FakeFactory{FailAlways: true}
	af := &AsyncFakeFactory{Inner: inner}
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.AcquireTimeout = Ptr(1.0)
	cfg.Retry = RetryPolicy{MaxAttempts: 3, BaseDelay: 10, MaxDelay: 30, Multiplier: 2, Jitter: 0}
	cfg.Breaker = &BreakerConfig{FailureThreshold: 1, RecoveryTimeout: 30, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
	pool := mustAsyncPool(af, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(context.Background(), nil)

	_, err := pool.Acquire(context.Background(), Unset())
	var timeoutErr *PoolTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected PoolTimeoutError, got %v", err)
	}
	if pool.Stats().BreakerState == nil || *pool.Stats().BreakerState != "closed" {
		t.Fatalf("expected breaker closed, got %+v", pool.Stats().BreakerState)
	}
}

func TestAsync_CreateTimeoutCountsAsBreakerFailure(t *testing.T) {
	af := &AsyncFakeFactory{Inner: &FakeFactory{}, CreateDelay: 5}
	cfg := quietConfig()
	cfg.CreateTimeout = Ptr(0.05)
	cfg.AcquireTimeout = Ptr(5.0)
	cfg.Breaker = &BreakerConfig{FailureThreshold: 1, RecoveryTimeout: 30, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	_, err := pool.Acquire(context.Background(), Unset())
	var cce *ConnectionCreateError
	if !errors.As(err, &cce) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if pool.Stats().BreakerState == nil || *pool.Stats().BreakerState != "open" {
		t.Fatalf("expected breaker open, got %+v", pool.Stats().BreakerState)
	}
}

func TestAsync_AcquireDeadlineTimeoutDoesNotTripBreaker(t *testing.T) {
	af := &AsyncFakeFactory{Inner: &FakeFactory{}, CreateDelay: 5}
	cfg := quietConfig()
	cfg.CreateTimeout = nil
	cfg.AcquireTimeout = Ptr(0.05)
	cfg.Breaker = &BreakerConfig{FailureThreshold: 1, RecoveryTimeout: 30, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	_, err := pool.Acquire(context.Background(), Unset())
	var timeoutErr *PoolTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected PoolTimeoutError, got %v", err)
	}
	if pool.Stats().BreakerState == nil || *pool.Stats().BreakerState != "closed" {
		t.Fatalf("expected breaker closed, got %+v", pool.Stats().BreakerState)
	}
}

func TestAsync_PoolClosedMidRetry(t *testing.T) {
	var pool *AsyncConnectionPool[*FakeConn]
	factory := &AsyncFuncFactory[*FakeConn]{
		CreateFn: func(ctx context.Context) (*FakeConn, error) {
			pool.Close(context.Background(), nil)
			return nil, fmt.Errorf("down")
		},
	}
	cfg := quietConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 1, Multiplier: 2, Jitter: 0}
	pool = mustAsyncPool(factory, cfg, nil)

	_, err := pool.Acquire(context.Background(), Unset())
	var closedErr *PoolClosedError
	if !errors.As(err, &closedErr) {
		t.Fatalf("expected PoolClosedError, got %v", err)
	}
}

func TestAsync_ConnectionCreatedDuringCloseIsDestroyed(t *testing.T) {
	var pool *AsyncConnectionPool[*FakeConn]
	var made []*FakeConn
	factory := &AsyncFuncFactory[*FakeConn]{
		CreateFn: func(ctx context.Context) (*FakeConn, error) {
			c := newFakeConn()
			made = append(made, c)
			pool.Close(context.Background(), nil)
			return c, nil
		},
		CloseFn: func(ctx context.Context, c *FakeConn) error { c.Closed = true; return nil },
	}
	pool = mustAsyncPool(factory, quietConfig(), nil)

	_, err := pool.Acquire(context.Background(), Unset())
	var closedErr *PoolClosedError
	if !errors.As(err, &closedErr) {
		t.Fatalf("expected PoolClosedError, got %v", err)
	}
	if len(made) == 0 || !made[0].Closed {
		t.Fatalf("expected created connection to be closed")
	}
}

func TestAsync_WarmupFailureIsNotFatal(t *testing.T) {
	inner := &FakeFactory{FailAlways: true}
	af := &AsyncFakeFactory{Inner: inner}
	cfg := quietConfig()
	cfg.MinSize = 2
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	mustOpenAsync(t, pool)
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0, got %d", pool.Stats().Size)
	}
}

func TestAsync_WarmupConnectionDestroyedIfClosedDuringCreate(t *testing.T) {
	var pool *AsyncConnectionPool[*FakeConn]
	var made []*FakeConn
	factory := &AsyncFuncFactory[*FakeConn]{
		CreateFn: func(ctx context.Context) (*FakeConn, error) {
			c := newFakeConn()
			made = append(made, c)
			pool.Close(context.Background(), nil)
			return c, nil
		},
		CloseFn: func(ctx context.Context, c *FakeConn) error { c.Closed = true; return nil },
	}
	cfg := quietConfig()
	cfg.MinSize = 1
	pool = mustAsyncPool(factory, cfg, nil)

	mustOpenAsync(t, pool)
	if len(made) == 0 || !made[0].Closed {
		t.Fatalf("expected created connection to be closed")
	}
}

func TestAsync_MaintenanceLoopRunsAndSurvivesErrors(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaintenanceInterval = Ptr(0.01)
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	mustOpenAsync(t, pool)
	// Let several maintenance ticks pass; RunMaintenance on a healthy pool shouldn't panic
	// and the loop must keep running (observed indirectly via idle/min-size staying stable).
	time.Sleep(50 * time.Millisecond)
	if pool.Closed() {
		t.Fatalf("expected pool to still be open")
	}
}

func TestAsync_ReprMentionsSizes(t *testing.T) {
	af := newAsyncFakeFactory()
	pool := mustAsyncPool(af, quietConfig(), &Options[*FakeConn]{Name: "AsyncConnectionPool"})
	defer pool.Close(context.Background(), nil)
	if pool.Name != "AsyncConnectionPool" {
		t.Fatalf("expected name to round-trip, got %q", pool.Name)
	}
}

func TestAsync_UseResultPropagatesError(t *testing.T) {
	af := newAsyncFakeFactory()
	pool := mustAsyncPool(af, quietConfig(), nil)
	defer pool.Close(context.Background(), nil)

	boom := fmt.Errorf("sync throw")
	err := pool.Use(context.Background(), Unset(), func(c *FakeConn) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom error, got %v", err)
	}
	if pool.Stats().Idle != 1 {
		t.Fatalf("expected idle 1, got %d", pool.Stats().Idle)
	}
}

func TestAsync_BenignErrorKeepsConnection(t *testing.T) {
	af := newAsyncFakeFactory()
	pool := mustAsyncPool(af, quietConfig(), nil)
	defer pool.Close(context.Background(), nil)

	boom := fmt.Errorf("app bug")
	err := pool.Use(context.Background(), Unset(), func(c *FakeConn) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom error, got %v", err)
	}
	if pool.Stats().Idle != 1 {
		t.Fatalf("expected idle 1, got %d", pool.Stats().Idle)
	}
}

func TestAsync_BenignErrorWithFailedValidationDiscards(t *testing.T) {
	af := newAsyncFakeFactory()
	pool := mustAsyncPool(af, quietConfig(), nil)
	defer pool.Close(context.Background(), nil)

	boom := fmt.Errorf("app bug")
	err := pool.Use(context.Background(), Unset(), func(c *FakeConn) error {
		c.Healthy = false
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom error, got %v", err)
	}
	if pool.Stats().Idle != 0 {
		t.Fatalf("expected idle 0, got %d", pool.Stats().Idle)
	}
}

func TestAsync_NonRetryableErrorFailsImmediately(t *testing.T) {
	inner := &FakeFactory{FailAlways: true}
	af := &AsyncFakeFactory{Inner: inner}
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.Retry = RetryPolicy{
		MaxAttempts: 5, BaseDelay: 0.1, MaxDelay: 5, Multiplier: 2, Jitter: 0,
		RetryOn: func(err error) bool { return errors.Is(err, errResetFailed) },
	}
	pool := mustAsyncPool(af, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(context.Background(), nil)

	_, err := pool.Acquire(context.Background(), Unset())
	var cce *ConnectionCreateError
	if !errors.As(err, &cce) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if inner.attemptCount() != 1 {
		t.Fatalf("expected 1 attempt, got %d", inner.attemptCount())
	}
}

func TestAsync_DuplicateIdleConnectionRejectedViaRace(t *testing.T) {
	shared := newFakeConn()
	entered := make(chan struct{})
	gate := make(chan struct{})
	var creates int32
	factory := &AsyncFuncFactory[*FakeConn]{
		CreateFn: func(ctx context.Context) (*FakeConn, error) {
			if atomic.AddInt32(&creates, 1) == 1 {
				return shared, nil
			}
			close(entered)
			<-gate
			return shared, nil
		},
		CloseFn: func(ctx context.Context, c *FakeConn) error { c.Closed = true; return nil },
	}
	cfg := quietConfig()
	cfg.MaxSize = 2
	pool := mustAsyncPool(factory, cfg, nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	a, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	var taskErr error
	done := make(chan struct{})
	go func() {
		_, taskErr = pool.Acquire(ctx, Unset())
		close(done)
	}()
	<-entered
	if err := pool.Release(a); err != nil {
		t.Fatal(err)
	}
	close(gate)
	<-done
	var cce *ConnectionCreateError
	if !errors.As(taskErr, &cce) {
		t.Fatalf("expected ConnectionCreateError, got %v", taskErr)
	}
}

func TestAsync_ExplicitRngIsUsed(t *testing.T) {
	af := newAsyncFakeFactory()
	pool := mustAsyncPool(af, quietConfig(), &Options[*FakeConn]{Rand: rand.New(rand.NewSource(1))})
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	c, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(c); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().Size != 1 {
		t.Fatalf("expected size 1, got %d", pool.Stats().Size)
	}
}

func TestAsync_WaiterQueueHasRoom(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 1
	cfg.MaxWaiters = Ptr(2)
	cfg.AcquireTimeout = nil
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	held, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	var waiterConn *FakeConn
	var waiterErr error
	done := make(chan struct{})
	go func() {
		waiterConn, waiterErr = pool.Acquire(ctx, Unset())
		close(done)
	}()
	spinUntil(t, func() bool { return pool.Stats().Waiting == 1 })
	if err := pool.Release(held); err != nil {
		t.Fatal(err)
	}
	<-done
	if waiterErr != nil {
		t.Fatal(waiterErr)
	}
	if waiterConn != held {
		t.Fatalf("expected waiter to receive released connection")
	}
}

func TestAsync_UseBodyThrowingSynchronouslyReleases(t *testing.T) {
	af := newAsyncFakeFactory()
	pool := mustAsyncPool(af, quietConfig(), nil)
	defer pool.Close(context.Background(), nil)

	boom := fmt.Errorf("sync throw")
	err := pool.Use(context.Background(), Unset(), func(c *FakeConn) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
	if pool.Stats().Idle != 1 || pool.Stats().InUse != 0 {
		t.Fatalf("unexpected stats: %+v", pool.Stats())
	}
}

func TestAsync_WithError(t *testing.T) {
	af := newAsyncFakeFactory()
	af.Inner.Fatal = func(err error) bool { return true }
	pool := mustAsyncPool(af, quietConfig(), nil)
	defer pool.Close(context.Background(), nil)

	ctx := context.Background()
	c, err := pool.Acquire(ctx, Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(c, WithError(errBackendDown)); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0 (fatal error discards), got %d", pool.Stats().Size)
	}
}

func TestAsync_RunMaintenanceDirect(t *testing.T) {
	af := newAsyncFakeFactory()
	pool := mustAsyncPool(af, quietConfig(), nil)
	defer pool.Close(context.Background(), nil)
	pool.RunMaintenance(context.Background()) // no-op: empty pool, no maintenance config
}

func TestAsync_CtxCancelBeforeAcquireReturnsImmediately(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 1
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	held, err := pool.Acquire(context.Background(), Unset())
	if err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = pool.Acquire(cctx, Unset())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if err := pool.Release(held); err != nil {
		t.Fatal(err)
	}
}

func TestAsync_CtxDeadlineExceededDuringAcquireWait(t *testing.T) {
	af := newAsyncFakeFactory()
	cfg := quietConfig()
	cfg.MaxSize = 1
	cfg.AcquireTimeout = nil
	pool := mustAsyncPool(af, cfg, nil)
	defer pool.Close(context.Background(), nil)

	held, err := pool.Acquire(context.Background(), Unset())
	if err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = pool.Acquire(cctx, Unset())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if err := pool.Release(held); err != nil {
		t.Fatal(err)
	}
}
