package connpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

// --- test doubles for the defensive/error branches ---

// asyncPanicFactory panics in a chosen method to exercise the pools' recover() guards.
type asyncPanicFactory struct {
	inner                               *FakeFactory
	panicCreate, panicReset, panicClose bool
}

func (f *asyncPanicFactory) Create(ctx context.Context) (*FakeConn, error) {
	if f.panicCreate {
		panic("create boom")
	}
	return f.inner.Create()
}
func (f *asyncPanicFactory) Validate(ctx context.Context, c *FakeConn) bool {
	return f.inner.Validate(c)
}
func (f *asyncPanicFactory) Reset(ctx context.Context, c *FakeConn) error {
	if f.panicReset {
		panic("reset boom")
	}
	return f.inner.Reset(c)
}
func (f *asyncPanicFactory) Close(ctx context.Context, c *FakeConn) error {
	if f.panicClose {
		panic("close boom")
	}
	return f.inner.Close(c)
}
func (f *asyncPanicFactory) IsFatalError(err error) bool { return f.inner.IsFatalError(err) }

// sleepCancelClock advances like a ManualClock but reports cancellation from SleepContext,
// simulating a context cancelled during retry backoff.
type sleepCancelClock struct{ *ManualClock }

func (c sleepCancelClock) SleepContext(ctx context.Context, seconds float64) error {
	c.ManualClock.Advance(seconds)
	return context.Canceled
}

// monotonicPanicClock panics on Monotonic, to exercise the maintenance pass's recover() guard.
type monotonicPanicClock struct{}

func (monotonicPanicClock) Monotonic() float64                          { panic("clock boom") }
func (monotonicPanicClock) Sleep(float64)                               {}
func (monotonicPanicClock) SleepContext(context.Context, float64) error { return nil }

// --- tests ---

func TestAsync_CloseIdempotent(t *testing.T) {
	p := mustAsyncPool(newAsyncFakeFactory(), quietConfig(), nil)
	ctx := context.Background()
	if err := p.Close(ctx, nil); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := p.Close(ctx, nil); err != nil { // idempotent: second close is a no-op
		t.Fatalf("second close: %v", err)
	}
}

func TestAsync_CloseDrainCancelled(t *testing.T) {
	p := mustAsyncPool(newAsyncFakeFactory(), quietConfig(), nil)
	ctx := context.Background()
	conn, err := p.Acquire(ctx, Unset())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = conn // keep it checked out so the drain loop has to wait

	cctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled -> drain breaks immediately on the ctx branch
	timeout := 5.0
	if err := p.Close(cctx, &timeout); err != nil {
		t.Fatalf("close with cancelled drain ctx: %v", err)
	}
}

func TestAsync_ResetPanicDiscardsConnection(t *testing.T) {
	f := &asyncPanicFactory{inner: &FakeFactory{}, panicReset: true}
	p := mustAsyncPool(f, quietConfig(), nil)
	ctx := context.Background()
	conn, err := p.Acquire(ctx, Unset())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := p.Release(conn); err != nil { // reset panics -> RESET_FAILED -> destroyed
		t.Fatalf("release: %v", err)
	}
	if got := p.Stats().Closed; got != 1 {
		t.Fatalf("expected 1 closed after reset panic, got %d", got)
	}
}

func TestAsync_CreateLimitClampsNegative(t *testing.T) {
	cfg := quietConfig()
	cfg.CreateTimeout = Ptr(10.0)
	p := mustAsyncPool(newAsyncFakeFactory(), cfg, nil)
	now := p.clock.Monotonic()
	past := now - 5.0 // an already-expired deadline -> remaining is negative
	limit := p.createLimit(&past)
	if limit == nil || *limit != 0.0 {
		t.Fatalf("createLimit should clamp a negative remaining to 0, got %v", limit)
	}
}

func TestAsync_CreateCancelledDuringBackoff(t *testing.T) {
	cfg := quietConfig()
	cfg.AcquireTimeout = Ptr(60.0) // plenty of time; cancellation must come from the clock
	retry := DefaultRetryPolicy()
	retry.MaxAttempts = 3
	retry.Jitter = 0.0
	cfg.Retry = retry
	f := &AsyncFakeFactory{Inner: &FakeFactory{FailAlways: true}}
	clk := sleepCancelClock{NewManualClock(1000.0)}
	p := mustAsyncPool(f, cfg, &Options[*FakeConn]{Clock: clk})

	_, err := p.Acquire(context.Background(), Unset())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from a cancelled backoff, got %v", err)
	}
	if p.Stats().Size != 0 {
		t.Fatalf("a cancelled create must not leak a slot, size=%d", p.Stats().Size)
	}
}

func TestAsync_CreatePanicIsCreateError(t *testing.T) {
	cfg := quietConfig() // NoRetry -> one attempt
	f := &asyncPanicFactory{inner: &FakeFactory{}, panicCreate: true}
	p := mustAsyncPool(f, cfg, nil)
	_, err := p.Acquire(context.Background(), Unset())
	var cce *ConnectionCreateError
	if !errors.As(err, &cce) {
		t.Fatalf("expected *ConnectionCreateError from a panicking create, got %v", err)
	}
	if p.Stats().Size != 0 {
		t.Fatalf("a failed create must not leak a slot, size=%d", p.Stats().Size)
	}
}

func TestAsync_ClosePanicIsSwallowed(t *testing.T) {
	f := &asyncPanicFactory{inner: &FakeFactory{}, panicClose: true}
	p := mustAsyncPool(f, quietConfig(), nil)
	ctx := context.Background()
	conn, err := p.Acquire(ctx, Unset())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// Discard -> destroy -> factory.Close panics -> recovered + logged, release still succeeds.
	if err := p.Release(conn, Discard()); err != nil {
		t.Fatalf("release(discard): %v", err)
	}
	if got := p.Stats().Closed; got != 1 {
		t.Fatalf("expected 1 closed despite close panic, got %d", got)
	}
}

func TestAsync_MaintenancePanicIsSwallowed(t *testing.T) {
	p := mustAsyncPool(newAsyncFakeFactory(), quietConfig(), &Options[*FakeConn]{Clock: monotonicPanicClock{}})
	// runMaintenanceSafe must recover from the panicking clock (and not leave p.mu held).
	p.runMaintenanceSafe()
	// Prove the lock was released: a follow-up locked call doesn't deadlock.
	_ = p.Closed()
}

func TestBreaker_CallFailurePath(t *testing.T) {
	b := NewCircuitBreaker(&BreakerConfig{FailureThreshold: 1, RecoveryTimeout: 30, HalfOpenMaxCalls: 1, SuccessThreshold: 1}, NewManualClock(1000.0), nil)
	boom := errors.New("boom")
	if err := b.Call(func() error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("Call should return the fn error, got %v", err)
	}
	if b.State() != BreakerOpen { // one failure at threshold 1 trips it
		t.Fatalf("breaker should be open after a failed Call, got %v", b.State())
	}
	// Now that it's open, Call must fail fast via BeforeCall without invoking fn.
	ran := false
	err := b.Call(func() error { ran = true; return nil })
	var coe *CircuitOpenError
	if !errors.As(err, &coe) {
		t.Fatalf("Call while open should return *CircuitOpenError, got %v", err)
	}
	if ran {
		t.Fatal("Call must not invoke fn while the breaker is open")
	}
}

func TestAsync_AcquireTimeoutViaContextWait(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxSize = 1
	p := mustAsyncPool(newAsyncFakeFactory(), cfg, nil)
	ctx := context.Background()
	if _, err := p.Acquire(ctx, Unset()); err != nil { // take the only slot
		t.Fatalf("first acquire: %v", err)
	}
	start := time.Now()
	_, err := p.Acquire(ctx, Of(0.05)) // must block then time out via waitSignalCtx's timer
	var pte *PoolTimeoutError
	if !errors.As(err, &pte) {
		t.Fatalf("expected *PoolTimeoutError, got %v", err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatalf("acquire returned too fast to have waited on the timer")
	}
}

func TestConfig_PropagatesRetryAndBreakerErrors(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 0} // invalid
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to surface the invalid RetryPolicy")
	}

	cfg = DefaultConfig()
	cfg.Breaker = &BreakerConfig{FailureThreshold: 0} // invalid
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to surface the invalid BreakerConfig")
	}
}
