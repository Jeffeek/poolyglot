package connpool

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// ------------------------------------------------------------------- basics

func TestSyncBasics_ConnectionIsReused(t *testing.T) {
	factory := &FakeFactory{}
	pool := mustPool(factory, quietConfig(), nil)
	defer pool.Close(nil)

	a, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(a); err != nil {
		t.Fatal(err)
	}
	b, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(b); err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("expected reuse, got %v vs %v", a, b)
	}
	if factory.createdCount() != 1 {
		t.Fatalf("expected 1 created, got %d", factory.createdCount())
	}
	if a.Resets != 2 {
		t.Fatalf("expected 2 resets, got %d", a.Resets)
	}
}

func TestSyncBasics_MaxSizeHardCapAndTimeout(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 1
	rec := &Recorder{}
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	held, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Acquire(Of(0.05))
	var timeoutErr *PoolTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected PoolTimeoutError, got %v", err)
	}
	if pool.Stats().Timeouts != 1 {
		t.Fatalf("expected 1 timeout, got %d", pool.Stats().Timeouts)
	}
	pool.Release(held)
}

func TestSyncBasics_ZeroTimeoutIsNonBlocking(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 1
	pool := mustPool(&FakeFactory{}, cfg, nil)
	defer pool.Close(nil)

	held, _ := pool.Acquire(Unset())
	_, err := pool.Acquire(Of(0))
	var timeoutErr *PoolTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected PoolTimeoutError, got %v", err)
	}
	pool.Release(held)
}

func TestSyncBasics_WaiterGetsConnectionWhenReleased(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 1
	pool := mustPool(&FakeFactory{}, cfg, nil)
	defer pool.Close(nil)

	held, _ := pool.Acquire(Unset())
	gotCh := make(chan *FakeConn, 1)
	go func() {
		got, err := pool.Acquire(Of(2))
		if err != nil {
			close(gotCh)
			return
		}
		gotCh <- got
	}()
	deadline := time.Now().Add(2 * time.Second)
	for pool.Stats().Waiting != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if pool.Stats().Waiting != 1 {
		t.Fatalf("expected 1 waiter, got %d", pool.Stats().Waiting)
	}
	pool.Release(held)
	select {
	case got := <-gotCh:
		if got != held {
			t.Fatalf("expected waiter to get %v, got %v", held, got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for waiter goroutine")
	}
}

func TestSyncBasics_DoubleAndForeignReleaseRejected(t *testing.T) {
	pool := mustPool(&FakeFactory{}, quietConfig(), nil)
	defer pool.Close(nil)

	conn, _ := pool.Acquire(Unset())
	if err := pool.Release(conn); err != nil {
		t.Fatal(err)
	}
	var invalid *InvalidReleaseError
	if err := pool.Release(conn); !errors.As(err, &invalid) {
		t.Fatalf("expected InvalidReleaseError, got %v", err)
	}
	if err := pool.Release(newFakeConn()); !errors.As(err, &invalid) {
		t.Fatalf("expected InvalidReleaseError, got %v", err)
	}
}

func TestSyncBasics_IdleStrategyLIFOvsFIFO(t *testing.T) {
	cases := []struct {
		strategy IdleStrategy
		index    int
	}{
		{LIFO, 2},
		{FIFO, 0},
	}
	for _, tc := range cases {
		t.Run(string(tc.strategy), func(t *testing.T) {
			cfg := quietConfig()
			cfg.IdleStrategy = tc.strategy
			pool := mustPool(&FakeFactory{}, cfg, nil)
			defer pool.Close(nil)

			conns := make([]*FakeConn, 3)
			for i := range conns {
				c, err := pool.Acquire(Unset())
				if err != nil {
					t.Fatal(err)
				}
				conns[i] = c
			}
			for _, c := range conns {
				pool.Release(c)
			}
			got, err := pool.Acquire(Unset())
			if err != nil {
				t.Fatal(err)
			}
			if got != conns[tc.index] {
				t.Fatalf("expected %v, got %v", conns[tc.index], got)
			}
		})
	}
}

type closableThing struct {
	closed bool
}

func TestSyncBasics_FuncFactoryAndDefaultClose(t *testing.T) {
	var made *closableThing
	factory := FuncFactory[*closableThing]{
		CreateFn: func() (*closableThing, error) {
			made = &closableThing{}
			return made, nil
		},
		CloseFn: func(c *closableThing) error { c.closed = true; return nil },
	}
	pool, err := NewConnectionPool[*closableThing](factory, quietConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	err = pool.Use(Unset(), func(c *closableThing) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	pool.Close(nil)
	if !made.closed {
		t.Fatal("expected default close to run")
	}
}

func TestSyncBasics_ExplicitDiscard(t *testing.T) {
	rec := &Recorder{}
	pool := mustPool(&FakeFactory{}, quietConfig(), &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	conn, _ := pool.Acquire(Unset())
	if err := pool.Release(conn, Discard()); err != nil {
		t.Fatal(err)
	}
	if !conn.Closed {
		t.Fatal("expected connection to be closed")
	}
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0, got %d", pool.Stats().Size)
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonDiscarded {
		t.Fatalf("expected [discarded], got %v", reasons)
	}
}

func TestSyncBasics_ValidateIdleEvictsDeadConnections(t *testing.T) {
	cfg := quietConfig()
	cfg.ValidateIdle = true
	cfg.MaxSize = 2
	rec := &Recorder{}
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	a, _ := pool.Acquire(Unset())
	b, _ := pool.Acquire(Unset())
	pool.Release(a)
	pool.Release(b)
	if pool.Stats().Idle != 2 {
		t.Fatalf("expected 2 idle, got %d", pool.Stats().Idle)
	}
	a.Healthy = false
	pool.RunMaintenance()
	if pool.Stats().Idle != 1 {
		t.Fatalf("expected 1 idle, got %d", pool.Stats().Idle)
	}
	found := false
	for _, r := range rec.closedReasons() {
		if r == CloseReasonUnhealthy {
			found = true
		}
	}
	if !found {
		t.Fatal("expected unhealthy close reason")
	}
	c, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if c != b {
		t.Fatalf("expected survivor %v, got %v", b, c)
	}
	pool.Release(c)
}

func TestSyncBasics_DuplicateConnectionFromFactoryIsRejected(t *testing.T) {
	shared := newFakeConn()
	factory := FuncFactory[*FakeConn]{
		CreateFn: func() (*FakeConn, error) { return shared, nil },
		CloseFn:  func(c *FakeConn) error { c.Closed = true; return nil },
	}
	cfg := quietConfig()
	cfg.MaxSize = 2
	pool, err := NewConnectionPool[*FakeConn](factory, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close(nil)

	first, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Acquire(Unset())
	var createErr *ConnectionCreateError
	if !errors.As(err, &createErr) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if pool.Stats().Size != 1 {
		t.Fatalf("expected size 1, got %d", pool.Stats().Size)
	}
	if pool.Stats().Created != 1 {
		t.Fatalf("expected created 1, got %d", pool.Stats().Created)
	}
	pool.Release(first)
	if pool.Stats().InUse != 0 {
		t.Fatalf("expected in_use 0, got %d", pool.Stats().InUse)
	}
}

// ------------------------------------------------------------------ rotation

func TestSyncRotation_ExpiredConnectionIsNotReused(t *testing.T) {
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.MaxLifetime = Ptr(100.0)
	rec := &Recorder{}
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Clock: clock, Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	a, _ := pool.Acquire(Unset())
	pool.Release(a)
	clock.Advance(101)
	b, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("expected a new connection")
	}
	if !a.Closed {
		t.Fatal("expected a to be closed")
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonExpired {
		t.Fatalf("expected [expired], got %v", reasons)
	}
	pool.Release(b)
}

func TestSyncRotation_ExpiringWhileInUseClosedOnRelease(t *testing.T) {
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.MaxLifetime = Ptr(100.0)
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(nil)

	a, _ := pool.Acquire(Unset())
	clock.Advance(150)
	pool.Release(a)
	if !a.Closed {
		t.Fatal("expected closed")
	}
	if pool.Stats().Idle != 0 {
		t.Fatalf("expected idle 0, got %d", pool.Stats().Idle)
	}
}

func TestSyncRotation_MaxUsesRotation(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxUses = Ptr(2)
	rec := &Recorder{}
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	first, _ := pool.Acquire(Unset())
	pool.Release(first)
	again, _ := pool.Acquire(Unset())
	pool.Release(again)
	if first != again {
		t.Fatalf("expected reuse, got %v vs %v", first, again)
	}
	if !first.Closed {
		t.Fatal("expected first to be retired/closed")
	}
	third, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("expected a new connection")
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonMaxUses {
		t.Fatalf("expected [max_uses], got %v", reasons)
	}
	pool.Release(third)
}

func TestSyncRotation_IdleExpiredConnectionDroppedOnBorrow(t *testing.T) {
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.MaxIdleTime = Ptr(10.0)
	rec := &Recorder{}
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Clock: clock, Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	a, _ := pool.Acquire(Unset())
	pool.Release(a)
	clock.Advance(11)
	b, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("expected a new connection")
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonIdle {
		t.Fatalf("expected [idle], got %v", reasons)
	}
	pool.Release(b)
}

func TestSyncRotation_IdleEvictionRespectsMinSize(t *testing.T) {
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.MinSize = 1
	cfg.MaxIdleTime = Ptr(10.0)
	rec := &Recorder{}
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Clock: clock, Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	conns := make([]*FakeConn, 3)
	for i := range conns {
		c, err := pool.Acquire(Unset())
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = c
	}
	for _, c := range conns {
		pool.Release(c)
	}
	clock.Advance(11)
	pool.RunMaintenance()
	if pool.Stats().Idle != 1 {
		t.Fatalf("expected idle 1, got %d", pool.Stats().Idle)
	}
	count := 0
	for _, r := range rec.closedReasons() {
		if r == CloseReasonIdle {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("expected 2 idle closes, got %d", count)
	}
}

func TestSyncRotation_MaintenanceReplacesExpiredToKeepMinSize(t *testing.T) {
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.MinSize = 2
	cfg.MaxLifetime = Ptr(50.0)
	factory := &FakeFactory{}
	pool := mustPool(factory, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(nil)

	if _, err := pool.Open(); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().Idle != 2 {
		t.Fatalf("expected idle 2, got %d", pool.Stats().Idle)
	}
	clock.Advance(51)
	pool.RunMaintenance()
	if pool.Stats().Idle != 2 {
		t.Fatalf("expected idle 2, got %d", pool.Stats().Idle)
	}
	if factory.createdCount() != 4 {
		t.Fatalf("expected 4 created, got %d", factory.createdCount())
	}
	closedCount := 0
	for _, c := range factory.Created {
		if c.Closed {
			closedCount++
		}
	}
	if closedCount != 2 {
		t.Fatalf("expected 2 closed, got %d", closedCount)
	}
}

func TestSyncRotation_BackgroundMaintenanceThread(t *testing.T) {
	cfg := quietConfig()
	cfg.MaintenanceInterval = Ptr(0.02)
	cfg.MaxIdleTime = Ptr(0.05)
	pool := mustPool(&FakeFactory{}, cfg, nil)
	defer pool.Close(nil)

	if _, err := pool.Open(); err != nil {
		t.Fatal(err)
	}
	c, _ := pool.Acquire(Unset())
	pool.Release(c)
	deadline := time.Now().Add(3 * time.Second)
	for pool.Stats().Size != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0, got %d", pool.Stats().Size)
	}
}

// -------------------------------------------------------------------- health

func TestSyncHealth_UnhealthyIdleConnectionReplacedOnBorrow(t *testing.T) {
	cfg := quietConfig()
	cfg.ValidateAfterIdle = Ptr(0.0)
	rec := &Recorder{}
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	a, _ := pool.Acquire(Unset())
	pool.Release(a)
	a.Healthy = false
	b, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("expected new connection")
	}
	if !a.Closed {
		t.Fatal("expected a closed")
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonUnhealthy {
		t.Fatalf("expected [unhealthy], got %v", reasons)
	}
	pool.Release(b)
}

func TestSyncHealth_ValidationSkippedWhenRecentlyUsed(t *testing.T) {
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.ValidateAfterIdle = Ptr(30.0)
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(nil)

	a, _ := pool.Acquire(Unset())
	pool.Release(a)
	a.Healthy = false
	got, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if got != a {
		t.Fatalf("expected same connection, got %v vs %v", a, got)
	}
}

func TestSyncHealth_FatalErrorDiscardsConnection(t *testing.T) {
	factory := &FakeFactory{Fatal: func(err error) bool { return errors.Is(err, errBackendDown) }}
	rec := &Recorder{}
	pool := mustPool(factory, quietConfig(), &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	var captured *FakeConn
	err := pool.Use(Unset(), func(c *FakeConn) error {
		captured = c
		return errBackendDown
	})
	if !errors.Is(err, errBackendDown) {
		t.Fatalf("expected errBackendDown, got %v", err)
	}
	if !captured.Closed {
		t.Fatal("expected captured closed")
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonError {
		t.Fatalf("expected [error], got %v", reasons)
	}
}

func TestSyncHealth_BenignErrorKeepsHealthyConnection(t *testing.T) {
	pool := mustPool(&FakeFactory{}, quietConfig(), nil)
	defer pool.Close(nil)

	appErr := errors.New("app bug")
	var captured *FakeConn
	err := pool.Use(Unset(), func(c *FakeConn) error {
		captured = c
		return appErr
	})
	if !errors.Is(err, appErr) {
		t.Fatalf("expected appErr, got %v", err)
	}
	if captured.Closed {
		t.Fatal("expected not closed")
	}
	if pool.Stats().Idle != 1 {
		t.Fatalf("expected idle 1, got %d", pool.Stats().Idle)
	}
}

func TestSyncHealth_ErrorPlusFailedValidationDiscards(t *testing.T) {
	pool := mustPool(&FakeFactory{}, quietConfig(), nil)
	defer pool.Close(nil)

	appErr := errors.New("app bug")
	var captured *FakeConn
	err := pool.Use(Unset(), func(c *FakeConn) error {
		captured = c
		c.Healthy = false
		return appErr
	})
	if !errors.Is(err, appErr) {
		t.Fatalf("expected appErr, got %v", err)
	}
	if !captured.Closed {
		t.Fatal("expected closed")
	}
}

func TestSyncHealth_ResetFailureDiscards(t *testing.T) {
	factory := &FakeFactory{}
	rec := &Recorder{}
	pool := mustPool(factory, quietConfig(), &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	factory.ResetRaises = true
	var captured *FakeConn
	err := pool.Use(Unset(), func(c *FakeConn) error {
		captured = c
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !captured.Closed {
		t.Fatal("expected closed")
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonResetFailed {
		t.Fatalf("expected [reset_failed], got %v", reasons)
	}
}

type badCloseFactory struct {
	*FakeFactory
}

func (f *badCloseFactory) Close(c *FakeConn) error {
	return errors.New("close failed")
}

func TestSyncHealth_CloseErrorsDoNotLeakSlots(t *testing.T) {
	pool := mustPool(&badCloseFactory{&FakeFactory{}}, quietConfig(), nil)
	defer pool.Close(nil)

	c, _ := pool.Acquire(Unset())
	if err := pool.Release(c, Discard()); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0, got %d", pool.Stats().Size)
	}
}

func TestSyncHealth_ValidationErrorIsTreatedAsUnhealthy(t *testing.T) {
	factory := FuncFactory[*FakeConn]{
		CreateFn:   func() (*FakeConn, error) { return newFakeConn(), nil },
		ValidateFn: func(c *FakeConn) bool { panic("ping blew up") },
		CloseFn:    func(c *FakeConn) error { c.Closed = true; return nil },
	}
	cfg := quietConfig()
	cfg.ValidateAfterIdle = Ptr(0.0)
	rec := &Recorder{}
	pool, err := NewConnectionPool[*FakeConn](factory, cfg, &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close(nil)

	a, _ := pool.Acquire(Unset())
	pool.Release(a)
	b, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("expected new connection")
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonUnhealthy {
		t.Fatalf("expected [unhealthy], got %v", reasons)
	}
	pool.Release(b)
}

// --------------------------------------------------------------- resilience

func TestSyncResilience_RetryWithBackoffSucceeds(t *testing.T) {
	factory := &FakeFactory{FailTimes: 2}
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 3, BaseDelay: 1, MaxDelay: 100, Multiplier: 2, Jitter: 0}
	cfg.AcquireTimeout = Ptr(60.0)
	pool := mustPool(factory, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(nil)

	start := clock.Monotonic()
	_, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if factory.attemptCount() != 3 {
		t.Fatalf("expected 3 attempts, got %d", factory.attemptCount())
	}
	if clock.Monotonic()-start != 3 {
		t.Fatalf("expected 3s elapsed, got %v", clock.Monotonic()-start)
	}
}

func TestSyncResilience_ExhaustedRetriesRaiseAndDoNotLeakSlot(t *testing.T) {
	factory := &FakeFactory{FailAlways: true}
	cfg := quietConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 2, BaseDelay: 0.1, MaxDelay: 5, Multiplier: 2, Jitter: 0}
	pool := mustPool(factory, cfg, &Options[*FakeConn]{Clock: NewManualClock(1000.0)})
	defer pool.Close(nil)

	_, err := pool.Acquire(Unset())
	var createErr *ConnectionCreateError
	if !errors.As(err, &createErr) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if !errors.Is(createErr.Cause, errBackendDown) {
		t.Fatalf("expected cause errBackendDown, got %v", createErr.Cause)
	}
	if factory.attemptCount() != 2 {
		t.Fatalf("expected 2 attempts, got %d", factory.attemptCount())
	}
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0, got %d", pool.Stats().Size)
	}
	if pool.Stats().CreateFailures != 2 {
		t.Fatalf("expected 2 create failures, got %d", pool.Stats().CreateFailures)
	}
}

func TestSyncResilience_NonRetryableErrorFailsImmediately(t *testing.T) {
	authErr := errors.New("bad password")
	factory := &FakeFactory{}
	attempts := 0
	ff := FuncFactory[*FakeConn]{
		CreateFn: func() (*FakeConn, error) { attempts++; return nil, authErr },
	}
	cfg := quietConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 5, BaseDelay: 0.1, MaxDelay: 5, Multiplier: 2, Jitter: 0, RetryOn: func(err error) bool {
		return errors.Is(err, errBackendDown)
	}}
	pool, err := NewConnectionPool[*FakeConn](ff, cfg, &Options[*FakeConn]{Clock: NewManualClock(1000.0)})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close(nil)
	_ = factory

	_, err = pool.Acquire(Unset())
	var createErr *ConnectionCreateError
	if !errors.As(err, &createErr) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestSyncResilience_RetryNeverSleepsPastAcquireDeadline(t *testing.T) {
	factory := &FakeFactory{FailAlways: true}
	cfg := quietConfig()
	cfg.AcquireTimeout = Ptr(1.0)
	cfg.Retry = RetryPolicy{MaxAttempts: 5, BaseDelay: 5, MaxDelay: 100, Multiplier: 2, Jitter: 0}
	pool := mustPool(factory, cfg, &Options[*FakeConn]{Clock: NewManualClock(1000.0)})
	defer pool.Close(nil)

	_, err := pool.Acquire(Unset())
	var timeoutErr *PoolTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected PoolTimeoutError, got %v", err)
	}
	if factory.attemptCount() != 1 {
		t.Fatalf("expected 1 attempt, got %d", factory.attemptCount())
	}
}

func TestSyncResilience_CircuitBreakerFullCycle(t *testing.T) {
	factory := &FakeFactory{FailAlways: true}
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.Breaker = &BreakerConfig{FailureThreshold: 2, RecoveryTimeout: 30, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
	rec := &Recorder{}
	pool := mustPool(factory, cfg, &Options[*FakeConn]{Clock: clock, Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	for i := 0; i < 2; i++ {
		_, err := pool.Acquire(Unset())
		var createErr *ConnectionCreateError
		if !errors.As(err, &createErr) {
			t.Fatalf("expected ConnectionCreateError, got %v", err)
		}
	}
	attempts := factory.attemptCount()
	_, err := pool.Acquire(Unset())
	var openErr *CircuitOpenError
	if !errors.As(err, &openErr) {
		t.Fatalf("expected CircuitOpenError, got %v", err)
	}
	if factory.attemptCount() != attempts {
		t.Fatalf("expected no new attempts, got %d vs %d", factory.attemptCount(), attempts)
	}
	if *pool.Stats().BreakerState != "open" {
		t.Fatalf("expected open, got %v", *pool.Stats().BreakerState)
	}

	clock.Advance(31)
	factory.FailAlways = false
	conn, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if *pool.Stats().BreakerState != "closed" {
		t.Fatalf("expected closed, got %v", *pool.Stats().BreakerState)
	}
	count := 0
	for _, e := range rec.events() {
		if e == "breaker:open->half_open" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected 1 transition event, got %d", count)
	}
	if pool.Stats().Size != 1 {
		t.Fatalf("expected size 1, got %d", pool.Stats().Size)
	}
	pool.Release(conn)
}

func TestSyncResilience_FailedProbeReopensBreaker(t *testing.T) {
	factory := &FakeFactory{FailAlways: true}
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.Breaker = &BreakerConfig{FailureThreshold: 1, RecoveryTimeout: 5, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
	pool := mustPool(factory, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(nil)

	_, err := pool.Acquire(Unset())
	var createErr *ConnectionCreateError
	if !errors.As(err, &createErr) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	clock.Advance(6)
	_, err = pool.Acquire(Unset())
	if !errors.As(err, &createErr) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	_, err = pool.Acquire(Unset())
	var openErr *CircuitOpenError
	if !errors.As(err, &openErr) {
		t.Fatalf("expected CircuitOpenError, got %v", err)
	}
}

func TestSyncResilience_BreakerCountsOneFailurePerAcquireNotPerAttempt(t *testing.T) {
	factory := &FakeFactory{FailAlways: true}
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.AcquireTimeout = Ptr(60.0)
	cfg.Retry = RetryPolicy{MaxAttempts: 3, BaseDelay: 1, MaxDelay: 100, Multiplier: 2, Jitter: 0}
	cfg.Breaker = &BreakerConfig{FailureThreshold: 2, RecoveryTimeout: 30, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
	pool := mustPool(factory, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(nil)

	_, err := pool.Acquire(Unset())
	var createErr *ConnectionCreateError
	if !errors.As(err, &createErr) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if factory.attemptCount() != 3 {
		t.Fatalf("expected 3 attempts, got %d", factory.attemptCount())
	}
	if *pool.Stats().BreakerState != "closed" {
		t.Fatalf("expected closed, got %v", *pool.Stats().BreakerState)
	}
	_, err = pool.Acquire(Unset())
	if !errors.As(err, &createErr) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if *pool.Stats().BreakerState != "open" {
		t.Fatalf("expected open, got %v", *pool.Stats().BreakerState)
	}
}

func TestSyncResilience_CreateTimeoutDoesNotTripBreaker(t *testing.T) {
	factory := &FakeFactory{FailAlways: true}
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.AcquireTimeout = Ptr(1.0)
	cfg.Retry = RetryPolicy{MaxAttempts: 3, BaseDelay: 10, MaxDelay: 100, Multiplier: 2, Jitter: 0}
	cfg.Breaker = &BreakerConfig{FailureThreshold: 1, RecoveryTimeout: 30, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
	pool := mustPool(factory, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(nil)

	_, err := pool.Acquire(Unset())
	var timeoutErr *PoolTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected PoolTimeoutError, got %v", err)
	}
	if *pool.Stats().BreakerState != "closed" {
		t.Fatalf("expected closed, got %v", *pool.Stats().BreakerState)
	}
}

func TestSyncResilience_PoolClosedMidRetryStopsCreating(t *testing.T) {
	var pool *ConnectionPool[*FakeConn]
	downErr := errors.New("down")
	ff := FuncFactory[*FakeConn]{
		CreateFn: func() (*FakeConn, error) {
			pool.Close(nil)
			return nil, downErr
		},
	}
	cfg := quietConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 5, Multiplier: 2, Jitter: 0}
	var err error
	pool, err = NewConnectionPool[*FakeConn](ff, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close(nil)

	_, err = pool.Acquire(Unset())
	var closedErr *PoolClosedError
	if !errors.As(err, &closedErr) {
		t.Fatalf("expected PoolClosedError, got %v", err)
	}
}

// --------------------------------------------------- shutdown & observability

func TestSyncShutdown_CloseSemantics(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 1
	pool := mustPool(&FakeFactory{}, cfg, nil)

	held, _ := pool.Acquire(Unset())
	errCh := make(chan error, 1)
	go func() {
		_, err := pool.Acquire(Of(5))
		errCh <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for pool.Stats().Waiting != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	pool.Close(nil)
	select {
	case err := <-errCh:
		var closedErr *PoolClosedError
		if !errors.As(err, &closedErr) {
			t.Fatalf("expected PoolClosedError, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for blocked caller")
	}
	if held.Closed {
		t.Fatal("expected held connection untouched")
	}
	pool.Release(held)
	if !held.Closed {
		t.Fatal("expected held connection closed on release")
	}
	_, err := pool.Acquire(Unset())
	var closedErr *PoolClosedError
	if !errors.As(err, &closedErr) {
		t.Fatalf("expected PoolClosedError, got %v", err)
	}
	pool.Close(nil) // idempotent
}

func TestSyncShutdown_CloseClosesIdleConnections(t *testing.T) {
	pool := mustPool(&FakeFactory{}, quietConfig(), nil)
	c, _ := pool.Acquire(Unset())
	pool.Release(c)
	pool.Close(nil)
	if !c.Closed {
		t.Fatal("expected closed")
	}
}

func TestSyncShutdown_OpenAndClose(t *testing.T) {
	factory := &FakeFactory{}
	cfg := quietConfig()
	cfg.MinSize = 2
	pool := mustPool(factory, cfg, nil)
	if _, err := pool.Open(); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().Idle != 2 {
		t.Fatalf("expected idle 2, got %d", pool.Stats().Idle)
	}
	pool.Close(nil)
	if !pool.Closed() {
		t.Fatal("expected closed")
	}
	for _, c := range factory.Created {
		if !c.Closed {
			t.Fatal("expected all created connections closed")
		}
	}
}

func TestSyncShutdown_WarmupFailureIsNotFatal(t *testing.T) {
	factory := &FakeFactory{FailAlways: true}
	cfg := quietConfig()
	cfg.MinSize = 2
	pool := mustPool(factory, cfg, nil)
	defer pool.Close(nil)

	if _, err := pool.Open(); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0, got %d", pool.Stats().Size)
	}
}

func TestSyncShutdown_StatsAndEvents(t *testing.T) {
	rec := &Recorder{}
	pool := mustPool(&FakeFactory{}, quietConfig(), &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	var mid PoolStats
	err := pool.Use(Unset(), func(c *FakeConn) error {
		mid = pool.Stats()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if mid.Size != 1 || mid.Idle != 0 || mid.InUse != 1 {
		t.Fatalf("unexpected mid stats: %+v", mid)
	}
	s := pool.Stats()
	if s.Created != 1 || s.Acquired != 1 || s.Released != 1 || s.Idle != 1 || s.InUse != 0 {
		t.Fatalf("unexpected final stats: %+v", s)
	}
	events := rec.events()
	expected := []string{"created", "acquired", "released"}
	if len(events) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, events)
	}
	for i, e := range expected {
		if events[i] != e {
			t.Fatalf("expected %v, got %v", expected, events)
		}
	}
}

type boomListener struct {
	BasePoolListener[*FakeConn]
}

func (boomListener) OnAcquired(conn *PooledConnection[*FakeConn], waitTime float64) {
	panic("metrics down")
}

func TestSyncShutdown_BrokenListenerCannotBreakPool(t *testing.T) {
	pool := mustPool(&FakeFactory{}, quietConfig(), &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{boomListener{}}})
	defer pool.Close(nil)

	err := pool.Use(Unset(), func(c *FakeConn) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
}

func TestSyncShutdown_ConnectionCreatedDuringCloseDoesNotLeak(t *testing.T) {
	var pool *ConnectionPool[*FakeConn]
	var made []*FakeConn
	ff := FuncFactory[*FakeConn]{
		CreateFn: func() (*FakeConn, error) {
			c := newFakeConn()
			made = append(made, c)
			pool.Close(nil)
			return c, nil
		},
		CloseFn: func(c *FakeConn) error { c.Closed = true; return nil },
	}
	var err error
	pool, err = NewConnectionPool[*FakeConn](ff, quietConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close(nil)

	_, err = pool.Acquire(Unset())
	var closedErr *PoolClosedError
	if !errors.As(err, &closedErr) {
		t.Fatalf("expected PoolClosedError, got %v", err)
	}
	if len(made) != 1 {
		t.Fatalf("expected 1 made, got %d", len(made))
	}
	if !made[0].Closed {
		t.Fatal("expected made[0] closed")
	}
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0, got %d", pool.Stats().Size)
	}
}

func TestSyncShutdown_GracefulDrainWaitsForInUse(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 2
	pool := mustPool(&FakeFactory{}, cfg, nil)

	held, _ := pool.Acquire(Unset())
	released := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		pool.Release(held)
		close(released)
	}()
	pool.Close(Ptr(2.0))
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("expected returner to run")
	}
	if !held.Closed {
		t.Fatal("expected drained connection closed")
	}
	if pool.Stats().InUse != 0 {
		t.Fatalf("expected in_use 0, got %d", pool.Stats().InUse)
	}
}

func TestSyncShutdown_GracefulDrainGivesUpAfterTimeout(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 1
	pool := mustPool(&FakeFactory{}, cfg, nil)

	held, _ := pool.Acquire(Unset())
	pool.Close(Ptr(0.05))
	if pool.Stats().InUse != 1 {
		t.Fatalf("expected in_use 1, got %d", pool.Stats().InUse)
	}
	if held.Closed {
		t.Fatal("expected not yet closed")
	}
	pool.Release(held)
	if !held.Closed {
		t.Fatal("expected closed on release")
	}
}

// ---------------------------------------------------------------- concurrency

func TestSyncConcurrency_StressRespectsMaxSizeAndLeaksNothing(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 5
	cfg.AcquireTimeout = Ptr(10.0)
	cfg.MaxUses = Ptr(7)
	pool := mustPool(&FakeFactory{}, cfg, nil)
	defer pool.Close(nil)

	var mu sync.Mutex
	now, peak := 0, 0
	var errs []error
	var errMu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for j := 0; j < 60; j++ {
				err := pool.Use(Unset(), func(c *FakeConn) error {
					mu.Lock()
					now++
					if now > peak {
						peak = now
					}
					mu.Unlock()
					if c.Closed {
						return fmt.Errorf("connection closed while in use")
					}
					time.Sleep(time.Duration(rng.Float64() * float64(2*time.Millisecond)))
					mu.Lock()
					now--
					mu.Unlock()
					return nil
				})
				if err != nil {
					errMu.Lock()
					errs = append(errs, err)
					errMu.Unlock()
				}
			}
		}(int64(i))
	}
	wg.Wait()

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
	if peak > 5 {
		t.Fatalf("expected peak <= 5, got %d", peak)
	}
	s := pool.Stats()
	if s.InUse != 0 || s.Waiting != 0 {
		t.Fatalf("expected in_use/waiting 0, got %+v", s)
	}
	if s.Size != s.Idle {
		t.Fatalf("expected size == idle, got %+v", s)
	}
	if s.Acquired != 20*60 {
		t.Fatalf("expected acquired %d, got %d", 20*60, s.Acquired)
	}
	if s.Created-s.Closed != s.Idle {
		t.Fatalf("expected created-closed == idle, got %+v", s)
	}
}

// -------------------------------------------------------------- leak detection

func TestSyncLeakDetection_WarnsOnce(t *testing.T) {
	clock := NewManualClock(1000.0)
	cfg := quietConfig()
	cfg.LeakDetectionTimeout = Ptr(10.0)
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(nil)

	conn, _ := pool.Acquire(Unset())
	pool.RunMaintenance()
	if pool.Stats().Leaks != 0 {
		t.Fatalf("expected 0 leaks, got %d", pool.Stats().Leaks)
	}
	clock.Advance(11)
	pool.RunMaintenance()
	pool.RunMaintenance()
	if pool.Stats().Leaks != 1 {
		t.Fatalf("expected 1 leak, got %d", pool.Stats().Leaks)
	}
	pool.Release(conn)
}

// --------------------------------------------------------------- backpressure

func TestSyncBackpressure_ZeroWaitersFailsFastWhenFull(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 1
	cfg.MaxWaiters = Ptr(0)
	cfg.AcquireTimeout = Ptr(5.0)
	pool := mustPool(&FakeFactory{}, cfg, nil)
	defer pool.Close(nil)

	held, _ := pool.Acquire(Unset())
	_, err := pool.Acquire(Unset())
	var exhaustedErr *PoolExhaustedError
	if !errors.As(err, &exhaustedErr) {
		t.Fatalf("expected PoolExhaustedError, got %v", err)
	}
	pool.Release(held)
}

func TestSyncBackpressure_WaiterQueueIsCapped(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 1
	cfg.MaxWaiters = Ptr(1)
	cfg.AcquireTimeout = Ptr(5.0)
	pool := mustPool(&FakeFactory{}, cfg, nil)
	defer pool.Close(nil)

	held, _ := pool.Acquire(Unset())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := pool.Acquire(Of(5))
		if err == nil {
			pool.Release(c)
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for pool.Stats().Waiting < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	_, err := pool.Acquire(Of(5))
	var exhaustedErr *PoolExhaustedError
	if !errors.As(err, &exhaustedErr) {
		t.Fatalf("expected PoolExhaustedError, got %v", err)
	}
	pool.Release(held)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for waiter goroutine")
	}
}

// --------------------------------------------------------------- lifecycle edges

func TestSyncLifecycle_OpenOnClosedPoolRaises(t *testing.T) {
	pool := mustPool(&FakeFactory{}, quietConfig(), nil)
	pool.Close(nil)
	_, err := pool.Open()
	var closedErr *PoolClosedError
	if !errors.As(err, &closedErr) {
		t.Fatalf("expected PoolClosedError, got %v", err)
	}
}

func TestSyncLifecycle_OpenIsIdempotent(t *testing.T) {
	cfg := quietConfig()
	cfg.MaintenanceInterval = Ptr(0.5)
	pool := mustPool(&FakeFactory{}, cfg, nil)
	defer pool.Close(nil)

	p1, err := pool.Open()
	if err != nil {
		t.Fatal(err)
	}
	p2, err := pool.Open()
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Fatal("expected same pool instance")
	}
}

func TestSyncLifecycle_RunMaintenanceOnClosedPoolIsNoop(t *testing.T) {
	pool := mustPool(&FakeFactory{}, quietConfig(), nil)
	pool.Close(nil)
	pool.RunMaintenance() // should not panic
}

func TestSyncLifecycle_WarmupConnectionDestroyedIfClosedDuringCreate(t *testing.T) {
	var pool *ConnectionPool[*FakeConn]
	var made []*FakeConn
	ff := FuncFactory[*FakeConn]{
		CreateFn: func() (*FakeConn, error) {
			c := newFakeConn()
			made = append(made, c)
			pool.Close(nil)
			return c, nil
		},
		CloseFn: func(c *FakeConn) error { c.Closed = true; return nil },
	}
	cfg := quietConfig()
	cfg.MinSize = 1
	var err error
	pool, err = NewConnectionPool[*FakeConn](ff, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close(nil)

	if _, err := pool.Open(); err != nil {
		t.Fatal(err)
	}
	if len(made) == 0 || !made[0].Closed {
		t.Fatal("expected orphan destroyed")
	}
}

func TestSyncLifecycle_MaintenanceLoopSurvivesRunErrors(t *testing.T) {
	// Exercise RunMaintenance repeatedly via the background goroutine and ensure it
	// keeps the pool usable; errors inside listeners/factory must not crash the loop.
	factory := &FakeFactory{}
	cfg := quietConfig()
	cfg.MaintenanceInterval = Ptr(0.01)
	pool := mustPool(factory, cfg, nil)
	defer pool.Close(nil)

	if _, err := pool.Open(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	// Pool should still be functional.
	c, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	pool.Release(c)
}

func TestSyncLifecycle_DefaultOptionsAreUsedWhenNil(t *testing.T) {
	pool, err := NewConnectionPool[*FakeConn](&FakeFactory{}, quietConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close(nil)
	if pool.Name != "pool" {
		t.Fatalf("expected default name 'pool', got %q", pool.Name)
	}
	c, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	pool.Release(c)
	if pool.Stats().Size != 1 {
		t.Fatalf("expected size 1, got %d", pool.Stats().Size)
	}
}

func TestSyncLifecycle_InvalidConfigReturnsError(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 0
	_, err := NewConnectionPool[*FakeConn](&FakeFactory{}, cfg, nil)
	var cfgErr *PoolConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("expected PoolConfigError, got %v", err)
	}
}

func TestSyncLifecycle_InfiniteWaitWaiterIsServed(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 1
	cfg.AcquireTimeout = nil
	pool := mustPool(&FakeFactory{}, cfg, nil)
	defer pool.Close(nil)

	held, _ := pool.Acquire(Unset())
	gotCh := make(chan *FakeConn, 1)
	go func() {
		got, err := pool.Acquire(Infinite())
		if err == nil {
			gotCh <- got
		} else {
			close(gotCh)
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for pool.Stats().Waiting != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	pool.Release(held)
	select {
	case got := <-gotCh:
		if got != held {
			t.Fatalf("expected %v, got %v", held, got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

func TestSyncLifecycle_HealthyIdleConnectionValidatedAndReused(t *testing.T) {
	cfg := quietConfig()
	cfg.ValidateAfterIdle = Ptr(0.0)
	pool := mustPool(&FakeFactory{}, cfg, nil)
	defer pool.Close(nil)

	a, _ := pool.Acquire(Unset())
	pool.Release(a)
	b, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("expected reuse, got %v vs %v", a, b)
	}
	pool.Release(b)
}

func TestSyncLifecycle_RunMaintenanceNoopWhenNoLeakConfig(t *testing.T) {
	pool := mustPool(&FakeFactory{}, quietConfig(), nil)
	defer pool.Close(nil)

	c, _ := pool.Acquire(Unset())
	pool.RunMaintenance() // LeakDetectionTimeout nil -> checkLeaks is a no-op
	pool.Release(c)
}

// --------------------------------------------------------------- extra branches

func TestSyncExtra_UseReturnsAcquireError(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 1
	pool := mustPool(&FakeFactory{}, cfg, nil)
	defer pool.Close(nil)

	held, _ := pool.Acquire(Unset())
	err := pool.Use(Of(0), func(c *FakeConn) error { return nil })
	var timeoutErr *PoolTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected PoolTimeoutError, got %v", err)
	}
	pool.Release(held)
}

func TestSyncExtra_ReleaseWithErrorOption(t *testing.T) {
	rec := &Recorder{}
	pool := mustPool(&FakeFactory{}, quietConfig(), &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	defer pool.Close(nil)

	conn, _ := pool.Acquire(Unset())
	appErr := errors.New("boom")
	if err := pool.Release(conn, WithError(appErr)); err != nil {
		t.Fatal(err)
	}
	if conn.Closed {
		t.Fatal("expected benign error to keep connection alive")
	}
	if pool.Stats().Idle != 1 {
		t.Fatalf("expected idle 1, got %d", pool.Stats().Idle)
	}
}

func TestSyncExtra_ResetPanicDiscardsConnection(t *testing.T) {
	factory := FuncFactory[*FakeConn]{
		CreateFn: func() (*FakeConn, error) { return newFakeConn(), nil },
		ResetFn:  func(c *FakeConn) error { panic("reset exploded") },
		CloseFn:  func(c *FakeConn) error { c.Closed = true; return nil },
	}
	rec := &Recorder{}
	pool, err := NewConnectionPool[*FakeConn](factory, quietConfig(), &Options[*FakeConn]{Listeners: []PoolListener[*FakeConn]{rec}})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close(nil)

	var captured *FakeConn
	if err := pool.Use(Unset(), func(c *FakeConn) error { captured = c; return nil }); err != nil {
		t.Fatal(err)
	}
	if !captured.Closed {
		t.Fatal("expected panic-ing reset to discard the connection")
	}
	reasons := rec.closedReasons()
	if len(reasons) != 1 || reasons[0] != CloseReasonResetFailed {
		t.Fatalf("expected [reset_failed], got %v", reasons)
	}
}

func TestSyncExtra_CreatePanicIsTreatedAsCreateFailure(t *testing.T) {
	ff := FuncFactory[*FakeConn]{
		CreateFn: func() (*FakeConn, error) { panic("create exploded") },
	}
	cfg := quietConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 1, BaseDelay: 0.1, MaxDelay: 5, Multiplier: 2, Jitter: 0}
	pool, err := NewConnectionPool[*FakeConn](ff, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close(nil)

	_, err = pool.Acquire(Unset())
	var createErr *ConnectionCreateError
	if !errors.As(err, &createErr) {
		t.Fatalf("expected ConnectionCreateError, got %v", err)
	}
	if pool.Stats().CreateFailures != 1 {
		t.Fatalf("expected 1 create failure, got %d", pool.Stats().CreateFailures)
	}
}

func TestSyncExtra_ClosePanicIsLoggedNotPropagated(t *testing.T) {
	ff := FuncFactory[*FakeConn]{
		CreateFn: func() (*FakeConn, error) { return newFakeConn(), nil },
		CloseFn:  func(c *FakeConn) error { panic("close exploded") },
	}
	pool, err := NewConnectionPool[*FakeConn](ff, quietConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close(nil)

	conn, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(conn, Discard()); err != nil {
		t.Fatal(err)
	}
	if pool.Stats().Size != 0 {
		t.Fatalf("expected size 0, got %d", pool.Stats().Size)
	}
}

// panicClock is a Clock whose Monotonic panics on the Nth call (1-indexed), letting a
// test force a specific unguarded clock read inside RunMaintenance to blow up. Every
// factory/listener call inside RunMaintenance already recovers internally, so a raw
// clock read is the only panic point that can reach runMaintenanceSafe's recover.
type panicClock struct {
	Clock
	mu      sync.Mutex
	callNum int
	panicAt int
}

func (c *panicClock) Monotonic() float64 {
	c.mu.Lock()
	c.callNum++
	hit := c.panicAt != 0 && c.callNum == c.panicAt
	c.mu.Unlock()
	if hit {
		panic("clock exploded")
	}
	return c.Clock.Monotonic()
}

func TestSyncExtra_MaintenanceSweepRecoversFromPanic(t *testing.T) {
	// checkLeaks' clock read is the only Monotonic() call in RunMaintenance taken
	// outside p.mu, so it is the one safe place to force a panic without deadlocking.
	clock := &panicClock{Clock: NewManualClock(1000.0)}
	cfg := quietConfig()
	cfg.LeakDetectionTimeout = Ptr(10.0)
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(nil)

	// RunMaintenance's own sweep() call consumes one Monotonic() read (under the lock)
	// before fillToMin and checkLeaks run; arm the panic for the next one after that
	// so it lands on checkLeaks' lock-free read instead of deadlocking p.mu.
	clock.mu.Lock()
	clock.panicAt = clock.callNum + 2
	clock.mu.Unlock()

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected RunMaintenance to panic")
			}
		}()
		pool.RunMaintenance()
	}()

	// The pool's mutex must not be left locked by the panic.
	c, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	pool.Release(c)
}

func TestSyncExtra_RunMaintenanceSafeRecoversFromPanic(t *testing.T) {
	clock := &panicClock{Clock: NewManualClock(1000.0)}
	cfg := quietConfig()
	cfg.LeakDetectionTimeout = Ptr(10.0)
	pool := mustPool(&FakeFactory{}, cfg, &Options[*FakeConn]{Clock: clock})
	defer pool.Close(nil)

	clock.mu.Lock()
	clock.panicAt = clock.callNum + 2 // land on checkLeaks' lock-free read, as above
	clock.mu.Unlock()

	pool.runMaintenanceSafe() // must not panic out: logged and swallowed

	c, err := pool.Acquire(Unset())
	if err != nil {
		t.Fatal(err)
	}
	pool.Release(c)
}

func TestSyncExtra_DuplicateIdleConnectionRejectedViaRace(t *testing.T) {
	shared := newFakeConn()
	entered := make(chan struct{})
	gate := make(chan struct{})
	var creates int32
	var mu sync.Mutex

	ff := FuncFactory[*FakeConn]{
		CreateFn: func() (*FakeConn, error) {
			mu.Lock()
			creates++
			n := creates
			mu.Unlock()
			if n == 1 {
				return shared, nil
			}
			close(entered)
			<-gate
			return shared, nil
		},
		CloseFn: func(c *FakeConn) error { c.Closed = true; return nil },
	}
	cfg := quietConfig()
	cfg.MaxSize = 2
	pool, err := NewConnectionPool[*FakeConn](ff, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close(nil)

	a, err := pool.Acquire(Unset()) // create #1 -> shared checked out
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := pool.Acquire(Unset()) // reserves a 2nd slot, blocks in create #2
		errCh <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second create")
	}
	pool.Release(a) // shared becomes idle (no longer checked out)
	close(gate)     // create #2 returns shared -> registerNew sees it in idle
	select {
	case err := <-errCh:
		var createErr *ConnectionCreateError
		if !errors.As(err, &createErr) {
			t.Fatalf("expected ConnectionCreateError, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second acquire")
	}
}
