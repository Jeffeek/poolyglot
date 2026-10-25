package connpool

import "testing"

func TestPercentile_EmptyIsZero(t *testing.T) {
	if got := percentile([]float64{}, 0.5); got != 0.0 {
		t.Fatalf("percentile(empty) = %v, want 0", got)
	}
}

func TestPercentile_KnownQuantiles(t *testing.T) {
	vals := make([]float64, 100)
	for i := range vals {
		vals[i] = float64(i + 1)
	}
	cases := []struct {
		q    float64
		want float64
	}{
		{0.0, 1.0},
		{1.0, 100.0},
		{0.5, 50.5},
		{0.99, 99.01},
	}
	for _, c := range cases {
		if got := percentile(vals, c.q); abs(got-c.want) > 1e-6 {
			t.Errorf("percentile(q=%v) = %v, want %v", c.q, got, c.want)
		}
	}
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func TestStatsCollector_AcquireWaitPercentilesInSnapshot(t *testing.T) {
	c := newStatsCollector[*FakeConn]()
	for _, w := range []float64{0.1, 0.2, 0.3, 0.4, 100.0} {
		c.OnAcquired(nil, w)
	}
	s := c.snapshot(statsGauges{Name: "x", MaxSize: 1})
	if abs(s.AvgAcquireWait-20.2) > 1e-3 {
		t.Errorf("AvgAcquireWait = %v, want 20.2", s.AvgAcquireWait)
	}
	if s.P50AcquireWait >= 1.0 {
		t.Errorf("P50AcquireWait = %v, want < 1.0", s.P50AcquireWait)
	}
	if s.P99AcquireWait <= 50.0 {
		t.Errorf("P99AcquireWait = %v, want > 50.0", s.P99AcquireWait)
	}
}

func TestStatsCollector_CreateLatencyPercentilesInSnapshot(t *testing.T) {
	c := newStatsCollector[*FakeConn]()
	for _, d := range []float64{0.01, 0.02, 0.03, 0.04, 5.0} {
		c.OnConnectionCreated(nil, d)
	}
	s := c.snapshot(statsGauges{Name: "x", MaxSize: 1})
	want := 5.1 / 5
	if abs(s.AvgCreateLatency-want) > 1e-6 {
		t.Errorf("AvgCreateLatency = %v, want %v", s.AvgCreateLatency, want)
	}
	if s.P50CreateLatency >= 1.0 {
		t.Errorf("P50CreateLatency = %v, want < 1.0", s.P50CreateLatency)
	}
	if s.P99CreateLatency <= 1.0 {
		t.Errorf("P99CreateLatency = %v, want > 1.0", s.P99CreateLatency)
	}
}

func TestStatsCollector_WaitWindowIsBounded(t *testing.T) {
	c := newStatsCollector[*FakeConn]()
	for i := 0; i < acquireWaitWindow+500; i++ {
		c.OnAcquired(nil, 1.0)
	}
	if len(c.waits) != acquireWaitWindow {
		t.Fatalf("len(waits) = %d, want %d", len(c.waits), acquireWaitWindow)
	}
}

func TestStatsCollector_CreateWindowIsBounded(t *testing.T) {
	c := newStatsCollector[*FakeConn]()
	for i := 0; i < acquireWaitWindow+500; i++ {
		c.OnConnectionCreated(nil, 1.0)
	}
	if len(c.creates) != acquireWaitWindow {
		t.Fatalf("len(creates) = %d, want %d", len(c.creates), acquireWaitWindow)
	}
}

func TestStatsCollector_AllHooksAndSnapshot(t *testing.T) {
	c := newStatsCollector[*FakeConn]()
	c.OnCreateFailed(errBackendDown)
	c.OnReleased(nil, 1.0)
	c.OnAcquireTimeout(2.0)
	c.OnConnectionLeak(nil, 3.0)
	c.OnConnectionClosed(nil, CloseReasonExpired)
	c.OnConnectionClosed(nil, CloseReasonExpired)
	c.OnConnectionClosed(nil, CloseReasonIdle)
	c.OnBreakerStateChange(BreakerClosed, BreakerOpen)

	breaker := "closed"
	s := c.snapshot(statsGauges{Name: "p", MaxSize: 10, Size: 3, Idle: 2, InUse: 1, Waiting: 0, BreakerState: &breaker})

	if s.CreateFailures != 1 {
		t.Errorf("CreateFailures = %d, want 1", s.CreateFailures)
	}
	if s.Released != 1 {
		t.Errorf("Released = %d, want 1", s.Released)
	}
	if s.Timeouts != 1 {
		t.Errorf("Timeouts = %d, want 1", s.Timeouts)
	}
	if s.Leaks != 1 {
		t.Errorf("Leaks = %d, want 1", s.Leaks)
	}
	if s.Closed != 3 {
		t.Errorf("Closed = %d, want 3", s.Closed)
	}
	if s.BreakerState == nil || *s.BreakerState != "closed" {
		t.Errorf("BreakerState = %v, want closed", s.BreakerState)
	}
	if s.ClosedByReason["expired"] != 2 {
		t.Errorf("ClosedByReason[expired] = %d, want 2", s.ClosedByReason["expired"])
	}
	if s.ClosedByReason["idle"] != 1 {
		t.Errorf("ClosedByReason[idle] = %d, want 1", s.ClosedByReason["idle"])
	}
}

func TestStatsCollector_OnAcquiredIncrementsCount(t *testing.T) {
	c := newStatsCollector[*FakeConn]()
	c.OnAcquired(nil, 0.1)
	c.OnConnectionCreated(nil, 0.1)
	s := c.snapshot(statsGauges{Name: "x"})
	if s.Acquired != 1 {
		t.Errorf("Acquired = %d, want 1", s.Acquired)
	}
	if s.Created != 1 {
		t.Errorf("Created = %d, want 1", s.Created)
	}
}

func TestPoolStats_ToDictionary(t *testing.T) {
	c := newStatsCollector[*FakeConn]()
	s := c.snapshot(statsGauges{Name: "x", MaxSize: 2, Size: 1, Idle: 1, InUse: 0, Waiting: 0})
	d := s.ToDictionary()

	if d["name"] != "x" {
		t.Errorf(`d["name"] = %v, want "x"`, d["name"])
	}
	if d["max_size"] != 2 {
		t.Errorf(`d["max_size"] = %v, want 2`, d["max_size"])
	}
	for _, key := range []string{
		"name", "max_size", "size", "idle", "in_use", "waiting",
		"created", "create_failures", "acquired", "released", "timeouts",
		"closed", "leaks", "closed_by_reason",
		"avg_acquire_wait", "p50_acquire_wait", "p95_acquire_wait", "p99_acquire_wait",
		"avg_create_latency", "p50_create_latency", "p95_create_latency", "p99_create_latency",
		"breaker_state",
	} {
		if _, ok := d[key]; !ok {
			t.Errorf("ToDictionary() missing key %q", key)
		}
	}
	if d["breaker_state"] != nil {
		t.Errorf(`d["breaker_state"] = %v, want nil`, d["breaker_state"])
	}
}

func TestPoolStats_ToDictionary_BreakerStateSet(t *testing.T) {
	breaker := "open"
	c := newStatsCollector[*FakeConn]()
	s := c.snapshot(statsGauges{Name: "x", BreakerState: &breaker})
	d := s.ToDictionary()
	if d["breaker_state"] != "open" {
		t.Errorf(`d["breaker_state"] = %v, want "open"`, d["breaker_state"])
	}
}
