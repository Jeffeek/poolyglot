package connpool

import "testing"

// Port of java TimeoutTest.java, adapted to the Go Timeout API (computeDeadline/remaining
// take the place of Java's Timeout.resolve/computeDeadline/remaining).

func TestTimeout_UnsetResolvesToConfiguredDefault(t *testing.T) {
	u := Unset()
	if u.kind != tkUnset {
		t.Fatalf("Unset() kind = %v, want tkUnset", u.kind)
	}

	if got := computeDeadline(u, Ptr(30.0), 100.0); got == nil || *got != 130.0 {
		t.Fatalf("computeDeadline(Unset, 30.0, 100.0) = %v, want 130.0", got)
	}

	if got := computeDeadline(u, nil, 100.0); got != nil {
		t.Fatalf("computeDeadline(Unset, nil, 100.0) = %v, want nil", got)
	}
}

func TestTimeout_InfiniteResolvesToNil(t *testing.T) {
	inf := Infinite()
	if inf.kind != tkInfinite {
		t.Fatalf("Infinite() kind = %v, want tkInfinite", inf.kind)
	}

	if got := computeDeadline(inf, Ptr(30.0), 100.0); got != nil {
		t.Fatalf("computeDeadline(Infinite, 30.0, 100.0) = %v, want nil", got)
	}
}

func TestTimeout_OfResolvesToValue(t *testing.T) {
	if got := computeDeadline(Of(5.0), Ptr(30.0), 100.0); got == nil || *got != 105.0 {
		t.Fatalf("computeDeadline(Of(5.0), 30.0, 100.0) = %v, want 105.0", got)
	}

	if got := computeDeadline(Of(0.0), Ptr(30.0), 100.0); got == nil || *got != 100.0 {
		t.Fatalf("computeDeadline(Of(0.0), 30.0, 100.0) = %v, want 100.0", got)
	}
}

func TestTimeout_OfNullableMapsNilToInfinite(t *testing.T) {
	nilTimeout := OfNullable(nil)
	if nilTimeout.kind != tkInfinite {
		t.Fatalf("OfNullable(nil) kind = %v, want tkInfinite", nilTimeout.kind)
	}
	if got := computeDeadline(nilTimeout, Ptr(30.0), 100.0); got != nil {
		t.Fatalf("computeDeadline(OfNullable(nil), 30.0, 100.0) = %v, want nil", got)
	}

	seven := OfNullable(Ptr(7.0))
	if seven.kind != tkOf || seven.secs != 7.0 {
		t.Fatalf("OfNullable(7.0) = %+v, want kind=tkOf secs=7.0", seven)
	}
	if got := computeDeadline(seven, Ptr(30.0), 100.0); got == nil || *got != 107.0 {
		t.Fatalf("computeDeadline(OfNullable(7.0), 30.0, 100.0) = %v, want 107.0", got)
	}
}

func TestTimeout_ComputeDeadline(t *testing.T) {
	if got := computeDeadline(Of(5.0), Ptr(30.0), 100.0); got == nil || *got != 105.0 {
		t.Fatalf("computeDeadline(Of(5.0), 30.0, 100.0) = %v, want 105.0", got)
	}
	if got := computeDeadline(Unset(), Ptr(30.0), 100.0); got == nil || *got != 130.0 {
		t.Fatalf("computeDeadline(Unset(), 30.0, 100.0) = %v, want 130.0", got)
	}
	if got := computeDeadline(Infinite(), Ptr(30.0), 100.0); got != nil {
		t.Fatalf("computeDeadline(Infinite(), 30.0, 100.0) = %v, want nil", got)
	}
}

func TestTimeout_Remaining(t *testing.T) {
	deadline := 105.0
	if got := remaining(&deadline, 100.0); got == nil || *got != 5.0 {
		t.Fatalf("remaining(105.0, 100.0) = %v, want 5.0", got)
	}
	if got := remaining(nil, 100.0); got != nil {
		t.Fatalf("remaining(nil, 100.0) = %v, want nil", got)
	}
}
