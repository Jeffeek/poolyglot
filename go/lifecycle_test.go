package connpool

import (
	"math/rand"
	"testing"
)

func lcConn(created float64, returned *float64, uses int, expires *float64) *PooledConnection[*FakeConn] {
	c := newPooledConnection(newFakeConn(), created, expires)
	c.LastReturnedAt = returned
	c.UseCount = uses
	return c
}

func TestLifecycle_LifetimeJitterStaysInRange(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxLifetime = Ptr(100.0)
	cfg.LifetimeJitter = 0.1
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 200; i++ {
		c := newConnection(newFakeConn(), cfg, 0.0, rng)
		if c.ExpiresAt == nil {
			t.Fatalf("expected ExpiresAt to be set")
		}
		if *c.ExpiresAt < 90 || *c.ExpiresAt > 100 {
			t.Fatalf("expected ExpiresAt in [90, 100], got %v", *c.ExpiresAt)
		}
	}
}

func TestLifecycle_NoExpiryWhenLifetimeNil(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxLifetime = nil
	c := newConnection(newFakeConn(), cfg, 0.0, rand.New(rand.NewSource(1)))
	if c.ExpiresAt != nil {
		t.Fatalf("expected nil ExpiresAt, got %v", *c.ExpiresAt)
	}
}

func TestLifecycle_RotationReasons(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxUses = Ptr(3)

	reason, ok := rotationReason(lcConn(0, nil, 0, Ptr(5.0)), cfg, 5)
	if !ok || reason != CloseReasonExpired {
		t.Fatalf("expected (expired, true), got (%v, %v)", reason, ok)
	}

	reason, ok = rotationReason(lcConn(0, nil, 3, nil), cfg, 0)
	if !ok || reason != CloseReasonMaxUses {
		t.Fatalf("expected (max_uses, true), got (%v, %v)", reason, ok)
	}

	reason, ok = rotationReason(lcConn(0, nil, 2, Ptr(9.0)), cfg, 5)
	if ok {
		t.Fatalf("expected (_, false), got (%v, %v)", reason, ok)
	}
}

func TestLifecycle_BorrowReasonFlagsIdle(t *testing.T) {
	cfg := quietConfig()
	cfg.MaxIdleTime = Ptr(10.0)

	reason, ok := borrowReason(lcConn(0, Ptr(0.0), 0, nil), cfg, 11)
	if !ok || reason != CloseReasonIdle {
		t.Fatalf("expected (idle, true), got (%v, %v)", reason, ok)
	}

	reason, ok = borrowReason(lcConn(0, Ptr(0.0), 0, nil), cfg, 5)
	if ok {
		t.Fatalf("expected (_, false), got (%v, %v)", reason, ok)
	}
}

func TestLifecycle_SweepNeverGoesBelowMinSize(t *testing.T) {
	cfg := quietConfig()
	cfg.MinSize = 2
	cfg.MaxIdleTime = Ptr(10.0)

	idle := make([]*PooledConnection[*FakeConn], 4)
	for i := range idle {
		idle[i] = lcConn(0, Ptr(0.0), 0, nil)
	}

	keep, doomed := sweep(idle, 4, cfg, 11)
	if len(keep) != 2 || len(doomed) != 2 {
		t.Fatalf("expected 2 keep / 2 doomed, got %d / %d", len(keep), len(doomed))
	}
	for _, d := range doomed {
		if d.Reason != CloseReasonIdle {
			t.Fatalf("expected idle reason, got %v", d.Reason)
		}
	}
}

func TestLifecycle_SweepAlwaysEvictsExpired(t *testing.T) {
	cfg := quietConfig()
	cfg.MinSize = 2
	cfg.MaxIdleTime = nil

	idle := []*PooledConnection[*FakeConn]{
		lcConn(0, nil, 0, Ptr(1.0)),
		lcConn(0, nil, 0, nil),
	}

	keep, doomed := sweep(idle, 2, cfg, 5)
	if len(doomed) != 1 || doomed[0].Reason != CloseReasonExpired {
		t.Fatalf("expected one expired doomed conn, got %+v", doomed)
	}
	if len(keep) != 1 {
		t.Fatalf("expected 1 keep, got %d", len(keep))
	}
}

func TestLifecycle_ValidationPolicy(t *testing.T) {
	c := lcConn(0, Ptr(0.0), 0, nil)

	cfg := quietConfig()
	cfg.ValidateAfterIdle = nil
	if needsValidation(c, cfg, 100) {
		t.Fatalf("expected no validation needed when ValidateAfterIdle is nil")
	}

	cfg.ValidateAfterIdle = Ptr(0.0)
	if !needsValidation(c, cfg, 0) {
		t.Fatalf("expected validation needed when ValidateAfterIdle is 0 and now is 0")
	}

	cfg.ValidateAfterIdle = Ptr(30.0)
	if needsValidation(c, cfg, 10) {
		t.Fatalf("expected no validation needed when idle time < threshold")
	}
	if !needsValidation(c, cfg, 30) {
		t.Fatalf("expected validation needed when idle time == threshold")
	}
}

func TestLifecycle_PopStrategies(t *testing.T) {
	a := lcConn(0, nil, 0, nil)
	b := lcConn(0, nil, 0, nil)

	idle := []*PooledConnection[*FakeConn]{a, b}
	got, rest := popIdle(idle, LIFO)
	if got != b {
		t.Fatalf("expected LIFO to pop b")
	}
	if len(rest) != 1 || rest[0] != a {
		t.Fatalf("expected rest to be [a], got %+v", rest)
	}

	idle = []*PooledConnection[*FakeConn]{a, b}
	got, rest = popIdle(idle, FIFO)
	if got != a {
		t.Fatalf("expected FIFO to pop a")
	}
	if len(rest) != 1 || rest[0] != b {
		t.Fatalf("expected rest to be [b], got %+v", rest)
	}

	got, rest = popIdle([]*PooledConnection[*FakeConn]{}, LIFO)
	if got != nil {
		t.Fatalf("expected nil from empty idle slice")
	}
	if len(rest) != 0 {
		t.Fatalf("expected empty rest from empty idle slice")
	}
}
