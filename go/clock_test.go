package connpool

import (
	"context"
	"testing"
)

func TestClock_SystemMonotonicAndSleep(t *testing.T) {
	clock := NewSystemClock()
	before := clock.Monotonic()
	clock.Sleep(0.001)
	clock.Sleep(0) // no-op branch
	if clock.Monotonic() < before {
		t.Fatalf("expected monotonic time to not go backwards")
	}
}

func TestClock_SystemSleepContextCompletes(t *testing.T) {
	clock := NewSystemClock()
	if err := clock.SleepContext(context.Background(), 0.001); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClock_SystemSleepContextZeroOrNegative(t *testing.T) {
	clock := NewSystemClock()
	if err := clock.SleepContext(context.Background(), 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := clock.SleepContext(context.Background(), -1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClock_SystemSleepContextAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	clock := NewSystemClock()
	if err := clock.SleepContext(ctx, 1); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestClock_SystemSleepContextCancelledMidway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	clock := NewSystemClock()
	done := make(chan error, 1)
	go func() {
		done <- clock.SleepContext(ctx, 10)
	}()
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestClock_ManualTimeOnlyMovesWhenTold(t *testing.T) {
	clock := NewManualClock(100.0)
	if clock.Monotonic() != 100.0 {
		t.Fatalf("expected 100.0, got %v", clock.Monotonic())
	}
	clock.Advance(5)
	if clock.Monotonic() != 105.0 {
		t.Fatalf("expected 105.0, got %v", clock.Monotonic())
	}
	clock.Sleep(2) // sleeping just advances time
	if clock.Monotonic() != 107.0 {
		t.Fatalf("expected 107.0, got %v", clock.Monotonic())
	}
}

func TestClock_ManualSleepContextAdvances(t *testing.T) {
	clock := NewManualClock(0.0)
	if err := clock.SleepContext(context.Background(), 3); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if clock.Monotonic() != 3.0 {
		t.Fatalf("expected 3.0, got %v", clock.Monotonic())
	}
}

func TestClock_ManualSleepContextAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	clock := NewManualClock(0.0)
	if err := clock.SleepContext(ctx, 3); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	// Go's ManualClock still advances time even when the context is already done,
	// unlike a "check first" design — verify that behavior explicitly.
	if clock.Monotonic() != 3.0 {
		t.Fatalf("expected clock to still advance to 3.0, got %v", clock.Monotonic())
	}
}
