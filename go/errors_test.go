package connpool

import (
	"errors"
	"strings"
	"testing"
)

func TestErrors_MessagesAndMarkers(t *testing.T) {
	cause := errors.New("boom")
	cases := []PoolError{
		&PoolConfigError{Msg: "cfg"},
		&PoolClosedError{Msg: "closed"},
		&PoolTimeoutError{Msg: "timeout"},
		&PoolExhaustedError{Msg: "exhausted"},
		&ConnectionCreateError{Msg: "create", Cause: cause},
		&CircuitOpenError{RetryAfter: 1.5},
		&InvalidReleaseError{Msg: "release"},
	}
	for _, e := range cases {
		if e.Error() == "" {
			t.Errorf("%T returned empty Error()", e)
		}
		e.poolError() // exercise the sealed-interface marker
	}
}

func TestErrors_CircuitOpenMessage(t *testing.T) {
	e := &CircuitOpenError{RetryAfter: 2.0}
	if !strings.Contains(e.Error(), "2.0") {
		t.Errorf("CircuitOpenError message missing retry-after: %q", e.Error())
	}
}

func TestErrors_ConnectionCreateUnwrap(t *testing.T) {
	cause := errors.New("backend")
	e := &ConnectionCreateError{Msg: "create", Cause: cause}
	if !errors.Is(e, cause) {
		t.Error("ConnectionCreateError should unwrap to its cause")
	}
	var target *ConnectionCreateError
	if !errors.As(error(e), &target) {
		t.Error("errors.As should match *ConnectionCreateError")
	}
}

func TestErrors_PoolErrorInterface(t *testing.T) {
	var err error = &PoolTimeoutError{Msg: "t"}
	var pe PoolError
	if !errors.As(err, &pe) {
		t.Error("a *PoolTimeoutError should satisfy PoolError via errors.As")
	}
}

func TestEvents_BaseListenerNoops(t *testing.T) {
	var l BasePoolListener[*FakeConn]
	conn := newPooledConnection[*FakeConn](newFakeConn(), 0, nil)
	l.OnConnectionCreated(conn, 0)
	l.OnConnectionClosed(conn, CloseReasonExpired)
	l.OnCreateFailed(errBackendDown)
	l.OnAcquired(conn, 0)
	l.OnReleased(conn, 0)
	l.OnAcquireTimeout(0)
	l.OnConnectionLeak(conn, 0)
	l.OnBreakerStateChange(BreakerClosed, BreakerOpen)
}

func TestStats_CollectorBreakerStateChangeNoop(t *testing.T) {
	c := newStatsCollector[*FakeConn]()
	c.OnBreakerStateChange(BreakerClosed, BreakerOpen) // no-op, but must be covered
}
