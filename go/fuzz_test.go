package connpool

import (
	"context"
	"math"
	"math/rand"
	"testing"
)

// FuzzDelayFor checks the retry-backoff invariant: for any finite, valid policy and attempt
// count, the delay stays within [0, MaxDelay] and is never NaN. Run `go test -fuzz=FuzzDelayFor`
// to fuzz; a plain `go test` just replays the seed corpus below.
//
// ponytail: fuzzes the finite domain only. NaN/Inf config fields are skipped because
// RetryPolicy.Validate does not reject them — hardening that is a cross-language spec change,
// not part of wiring up fuzzing. (BaseDelay==0 && Multiplier>1 && large-attempts used to surface
// 0*Inf = NaN; DelayFor now special-cases BaseDelay==0 to a 0 ceiling, see TestRetry_ZeroBaseDelayNeverNaN.)
func FuzzDelayFor(f *testing.F) {
	f.Add(0.1, 5.0, 2.0, 0.5, 1, int64(1))
	f.Add(0.0, 0.0, 1.0, 0.0, 10, int64(42))
	f.Add(1.5, 100.0, 3.0, 1.0, 5, int64(7))

	f.Fuzz(func(t *testing.T, base, maxDelay, mult, jitter float64, failedAttempts int, seed int64) {
		for _, v := range []float64{base, maxDelay, mult, jitter} {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return
			}
		}
		p := RetryPolicy{MaxAttempts: 3, BaseDelay: base, MaxDelay: maxDelay, Multiplier: mult, Jitter: jitter}
		if p.Validate() != nil {
			return // only assert the invariant for policies the library accepts
		}
		d := p.DelayFor(failedAttempts, rand.New(rand.NewSource(seed)))
		if math.IsNaN(d) {
			t.Fatalf("DelayFor returned NaN for %+v attempts=%d", p, failedAttempts)
		}
		if d < 0 || d > p.MaxDelay {
			t.Fatalf("DelayFor=%v out of [0, %v] for %+v attempts=%d", d, p.MaxDelay, p, failedAttempts)
		}
	})
}

// FuzzBreakerSequence drives the circuit breaker through an arbitrary operation sequence decoded
// from the fuzz bytes and asserts it never panics and never leaves the documented
// {closed, open, half_open} state set. Single-threaded by design (the lock is exercised by the
// pool tests); this targets the transition logic.
func FuzzBreakerSequence(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5})
	f.Add([]byte{2, 2, 2, 4, 5, 0, 1})

	f.Fuzz(func(t *testing.T, ops []byte) {
		clock := NewManualClock(1000.0)
		cfg := BreakerConfig{FailureThreshold: 2, RecoveryTimeout: 1.0, HalfOpenMaxCalls: 1, SuccessThreshold: 1}
		b := NewCircuitBreaker(&cfg, clock, nil)
		for _, op := range ops {
			switch op % 6 {
			case 0:
				_ = b.BeforeCall()
			case 1:
				b.RecordSuccess()
			case 2:
				b.RecordFailure()
			case 3:
				b.RecordAbort()
			case 4:
				clock.Advance(0.5)
			case 5:
				_ = b.State()
			}
			switch b.State() {
			case BreakerClosed, BreakerOpen, BreakerHalfOpen:
			default:
				t.Fatalf("breaker reached invalid state %q", b.State())
			}
		}
	})
}

// FuzzPoolSequence drives a sync pool through an arbitrary operation sequence (decoded from the
// fuzz bytes) against a deterministic ManualClock, and asserts the pool's hard accounting
// invariants hold after every settled operation:
//
//   - 0 <= Size <= MaxSize                 (MaxSize is a hard cap)
//   - Idle + InUse == Size                 (no slot lost or double-counted at rest)
//   - InUse == connections we hold          (checkout bookkeeping matches reality)
//   - Created - Closed == Size              (conservation: every created conn is live or closed)
//
// Single-threaded on purpose: this targets the model/accounting, not the lock (the concurrency
// is exercised by the pool tests). A plain `go test` replays the seeds; `go test -fuzz=FuzzPoolSequence`
// searches for a sequence that breaks an invariant.
func FuzzPoolSequence(f *testing.F) {
	f.Add(byte(2), []byte{0, 0, 0, 3, 3, 3})
	f.Add(byte(1), []byte{0, 0, 5, 6, 3, 7})
	f.Add(byte(3), []byte{0, 1, 4, 0, 6, 5, 3, 0})

	f.Fuzz(func(t *testing.T, maxSizeByte byte, ops []byte) {
		maxSize := int(maxSizeByte%4) + 1 // 1..4
		clock := NewManualClock(1000.0)
		cfg := quietConfig()
		cfg.MaxSize = maxSize
		cfg.MinSize = 0
		cfg.MaxIdleTime = Ptr(0.5) // let maintenance actually evict, exercising destroy/size accounting
		factory := &FakeFactory{}
		p := mustPool(factory, cfg, &Options[*FakeConn]{Clock: clock, Rand: rand.New(rand.NewSource(1))})
		defer func() { _ = p.Close(nil) }()

		var held []*FakeConn

		check := func(where string) {
			t.Helper()
			s := p.Stats()
			switch {
			case s.Size < 0 || s.Idle < 0 || s.InUse < 0:
				t.Fatalf("%s: negative gauge %+v", where, s)
			case s.Size > maxSize:
				t.Fatalf("%s: Size=%d exceeds MaxSize=%d", where, s.Size, maxSize)
			case s.Idle+s.InUse != s.Size:
				t.Fatalf("%s: Idle(%d)+InUse(%d) != Size(%d)", where, s.Idle, s.InUse, s.Size)
			case s.InUse != len(held):
				t.Fatalf("%s: InUse=%d but we hold %d", where, s.InUse, len(held))
			case s.Created-s.Closed != s.Size:
				t.Fatalf("%s: Created(%d)-Closed(%d) != Size(%d)", where, s.Created, s.Closed, s.Size)
			case s.Waiting != 0:
				t.Fatalf("%s: Waiting=%d in single-threaded run", where, s.Waiting)
			}
		}

		for _, op := range ops {
			switch op % 8 {
			case 0, 1: // non-blocking acquire
				if c, err := p.Acquire(Of(0)); err == nil {
					held = append(held, c)
				}
			case 2: // release the oldest held connection
				if len(held) > 0 {
					_ = p.Release(held[0])
					held = held[1:]
				}
			case 3: // release the newest, discarding it
				if len(held) > 0 {
					_ = p.Release(held[len(held)-1], Discard())
					held = held[:len(held)-1]
				}
			case 4: // release with a caller error
				if len(held) > 0 {
					_ = p.Release(held[0], WithError(errBackendDown))
					held = held[1:]
				}
			case 5:
				clock.Advance(0.3)
			case 6:
				p.RunMaintenance()
			case 7: // double-release: a connection the pool never handed out must be rejected
				stray := newFakeConn()
				if err := p.Release(stray); err == nil {
					t.Fatalf("releasing an unknown connection must error")
				}
			}
			check("after-op")
		}

		// Drain: release everything still held; the pool must end with nothing in use.
		for _, c := range held {
			_ = p.Release(c)
		}
		held = nil
		check("drained")
	})
}

// FuzzAsyncPoolSequence is FuzzPoolSequence's twin for the async pool's divergent primitive:
// the channel-based broadcast cond in cond.go (waitSignalCtx), which sync_pool.go's
// Mutex+Condvar doesn't have. Every acquire runs with a context that this fuzzer sometimes
// cancels before the call returns, driving the "cancellation never leaks a slot" path
// documented in AsyncConnectionPool and cond.go. Single-threaded and synchronous on purpose
// (no goroutines, every Acquire/Release runs to completion before the next op): this targets
// the cancellation/accounting logic the way FuzzPoolSequence targets the sync model, not the
// scheduler. Same accounting invariants as FuzzPoolSequence, i.e. what cancellation must not
// break:
//
//   - 0 <= Size <= MaxSize                 (MaxSize is a hard cap)
//   - Idle + InUse == Size                 (no slot lost or double-counted at rest)
//   - InUse == connections we hold          (checkout bookkeeping matches reality)
//   - Created - Closed == Size              (conservation: every created conn is live or closed)
//   - Waiting == 0                          (single-threaded: nothing is actually parked waiting)
func FuzzAsyncPoolSequence(f *testing.F) {
	f.Add(byte(2), []byte{0, 0, 0, 3, 3, 3})
	f.Add(byte(1), []byte{8, 0, 8, 0, 5, 6, 3, 7})
	f.Add(byte(3), []byte{0, 1, 4, 0, 8, 6, 5, 3, 0})

	f.Fuzz(func(t *testing.T, maxSizeByte byte, ops []byte) {
		maxSize := int(maxSizeByte%4) + 1 // 1..4
		clock := NewManualClock(1000.0)
		cfg := quietConfig()
		cfg.MaxSize = maxSize
		cfg.MinSize = 0
		cfg.MaxIdleTime = Ptr(0.5) // let maintenance actually evict, exercising destroy/size accounting
		factory := newAsyncFakeFactory()
		p := mustAsyncPool(factory, cfg, &Options[*FakeConn]{Clock: clock, Rand: rand.New(rand.NewSource(1))})
		defer func() { _ = p.Close(context.Background(), nil) }()

		var held []*FakeConn

		check := func(where string) {
			t.Helper()
			s := p.Stats()
			switch {
			case s.Size < 0 || s.Idle < 0 || s.InUse < 0:
				t.Fatalf("%s: negative gauge %+v", where, s)
			case s.Size > maxSize:
				t.Fatalf("%s: Size=%d exceeds MaxSize=%d", where, s.Size, maxSize)
			case s.Idle+s.InUse != s.Size:
				t.Fatalf("%s: Idle(%d)+InUse(%d) != Size(%d)", where, s.Idle, s.InUse, s.Size)
			case s.InUse != len(held):
				t.Fatalf("%s: InUse=%d but we hold %d", where, s.InUse, len(held))
			case s.Created-s.Closed != s.Size:
				t.Fatalf("%s: Created(%d)-Closed(%d) != Size(%d)", where, s.Created, s.Closed, s.Size)
			case s.Waiting != 0:
				t.Fatalf("%s: Waiting=%d in single-threaded run", where, s.Waiting)
			}
		}

		for _, op := range ops {
			switch op % 9 {
			case 0, 1: // non-blocking acquire with a live context
				if c, err := p.Acquire(context.Background(), Of(0)); err == nil {
					held = append(held, c)
				}
			case 2: // release the oldest held connection
				if len(held) > 0 {
					_ = p.Release(held[0])
					held = held[1:]
				}
			case 3: // release the newest, discarding it
				if len(held) > 0 {
					_ = p.Release(held[len(held)-1], Discard())
					held = held[:len(held)-1]
				}
			case 4: // release with a caller error
				if len(held) > 0 {
					_ = p.Release(held[0], WithError(errBackendDown))
					held = held[1:]
				}
			case 5:
				clock.Advance(0.3)
			case 6:
				p.RunMaintenance(context.Background())
			case 7: // double-release: a connection the pool never handed out must be rejected
				stray := newFakeConn()
				if err := p.Release(stray); err == nil {
					t.Fatalf("releasing an unknown connection must error")
				}
			case 8: // acquire with an already-cancelled context: must fail without leaking a slot
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if c, err := p.Acquire(ctx, Of(0)); err == nil {
					held = append(held, c) // pool could still legally serve from idle before checking ctx
				}
			}
			check("after-op")
		}

		// Drain: release everything still held; the pool must end with nothing in use.
		for _, c := range held {
			_ = p.Release(c)
		}
		held = nil
		check("drained")
	})
}
