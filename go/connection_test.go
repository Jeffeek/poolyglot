package connpool

import (
	"strings"
	"testing"
)

func TestConnection_AgeIdleAndHoldTimes(t *testing.T) {
	c := newPooledConnection[any](struct{}{}, 100.0, nil)
	if got := c.Age(110.0); got != 10.0 {
		t.Errorf("Age() = %v, want 10.0", got)
	}
	if got := c.IdleTime(105.0); got != 5.0 { // last_returned_at defaults to created_at
		t.Errorf("IdleTime() = %v, want 5.0", got)
	}
	if got := c.HoldTime(110.0); got != 0.0 { // not checked out yet
		t.Errorf("HoldTime() = %v, want 0.0", got)
	}

	c.MarkCheckedOut(120.0)
	if c.UseCount != 1 {
		t.Errorf("UseCount = %d, want 1", c.UseCount)
	}
	if got := c.HoldTime(123.0); got != 3.0 {
		t.Errorf("HoldTime() = %v, want 3.0", got)
	}
	if c.LeakReported {
		t.Error("LeakReported should be false after MarkCheckedOut")
	}

	c.MarkReturned(130.0)
	if got := c.IdleTime(131.0); got != 1.0 {
		t.Errorf("IdleTime() = %v, want 1.0", got)
	}
	if c.CheckedOutAt != nil {
		t.Error("CheckedOutAt should be nil after MarkReturned")
	}
}

func TestConnection_LeakReportedFlagRoundTrips(t *testing.T) {
	c := newPooledConnection[any](struct{}{}, 0.0, nil)
	c.LeakReported = true
	if !c.LeakReported {
		t.Error("LeakReported should be true")
	}
	c.MarkCheckedOut(1.0) // clears the flag
	if c.LeakReported {
		t.Error("MarkCheckedOut should clear LeakReported")
	}
}

func TestConnection_MetadataAccessors(t *testing.T) {
	raw := &FakeConn{}
	expires := 99.0
	c := newPooledConnection[*FakeConn](raw, 7.0, &expires)
	lastReturned := 8.0
	c.LastReturnedAt = &lastReturned

	if c.Raw != raw {
		t.Error("Raw mismatch")
	}
	if c.ID <= 0 {
		t.Errorf("ID = %d, want > 0", c.ID)
	}
	if c.CreatedAt != 7.0 {
		t.Errorf("CreatedAt = %v, want 7.0", c.CreatedAt)
	}
	if c.ExpiresAt == nil || *c.ExpiresAt != 99.0 {
		t.Errorf("ExpiresAt = %v, want 99.0", c.ExpiresAt)
	}
	if c.LastReturnedAt == nil || *c.LastReturnedAt != 8.0 {
		t.Errorf("LastReturnedAt = %v, want 8.0", c.LastReturnedAt)
	}
}

func TestConnection_StringMentionsType(t *testing.T) {
	c := newPooledConnection[any](struct{}{}, 0.0, nil)
	if !strings.Contains(c.String(), "PooledConnection") {
		t.Errorf("String() = %q, want it to contain %q", c.String(), "PooledConnection")
	}
}

func TestCloseReason_WireValues(t *testing.T) {
	cases := []struct {
		reason CloseReason
		want   string
	}{
		{CloseReasonExpired, "expired"},
		{CloseReasonMaxUses, "max_uses"},
		{CloseReasonIdle, "idle"},
		{CloseReasonUnhealthy, "unhealthy"},
		{CloseReasonError, "error"},
		{CloseReasonResetFailed, "reset_failed"},
		{CloseReasonDiscarded, "discarded"},
		{CloseReasonPoolClosed, "pool_closed"},
	}
	for _, tc := range cases {
		if got := tc.reason.Value(); got != tc.want {
			t.Errorf("%v.Value() = %q, want %q", tc.reason, got, tc.want)
		}
	}
}
