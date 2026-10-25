package connpool

import (
	"context"
	"time"
)

// The pools use a channel-based broadcast instead of sync.Cond because sync.Cond cannot
// wait with a timeout or honor a context. Each pool holds a `notify` channel; a state
// change closes it and installs a fresh one (signal()), waking every waiter — notify_all
// semantics. Waiters snapshot the channel under the lock, release the lock, then block on
// it (optionally with a deadline / context), re-acquire the lock and re-check state. A
// thundering herd is possible but correct; wake-up order is not strictly FIFO (matching the
// other languages).
//
// ponytail: broadcast-on-every-change (not notify-one); fine for pool-sized waiter counts.

// waitSignal blocks until ch is closed (a broadcast) or, if left is non-nil, until that
// many seconds elapse in real time — mirroring Python's Condition.wait(timeout), which also
// waits in real time regardless of the pool's logical clock.
func waitSignal(ch <-chan struct{}, left *float64) {
	if left == nil {
		<-ch
		return
	}
	t := time.NewTimer(time.Duration(*left * float64(time.Second)))
	defer t.Stop()
	select {
	case <-ch:
	case <-t.C:
	}
}

// waitSignalCtx is waitSignal plus context cancellation; it returns ctx.Err() if ctx is
// done before a broadcast or the deadline. A nil left means "no deadline".
func waitSignalCtx(ctx context.Context, ch <-chan struct{}, left *float64) error {
	if left == nil {
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	t := time.NewTimer(time.Duration(*left * float64(time.Second)))
	defer t.Stop()
	select {
	case <-ch:
		return nil
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
