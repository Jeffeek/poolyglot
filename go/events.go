package connpool

// Observability hooks. Implement PoolListener (or embed BasePoolListener and override what
// you care about) for metrics, tracing, logging. Hooks run synchronously on the calling
// goroutine, outside the pool's lock, so keep them fast. Panics raised by listeners are
// logged and swallowed - a broken metrics exporter must never break the pool.

import "log"

type PoolListener[T any] interface {
	OnConnectionCreated(conn *PooledConnection[T], duration float64)
	OnConnectionClosed(conn *PooledConnection[T], reason CloseReason)
	OnCreateFailed(err error)
	OnAcquired(conn *PooledConnection[T], waitTime float64)
	OnReleased(conn *PooledConnection[T], holdTime float64)
	OnAcquireTimeout(waited float64)
	OnConnectionLeak(conn *PooledConnection[T], heldFor float64)
	OnBreakerStateChange(old, new BreakerState)
}

// BasePoolListener is a no-op PoolListener implementation meant to be embedded so callers
// only need to override the hooks they care about.
type BasePoolListener[T any] struct{}

func (BasePoolListener[T]) OnConnectionCreated(conn *PooledConnection[T], duration float64)  {}
func (BasePoolListener[T]) OnConnectionClosed(conn *PooledConnection[T], reason CloseReason) {}
func (BasePoolListener[T]) OnCreateFailed(err error)                                         {}
func (BasePoolListener[T]) OnAcquired(conn *PooledConnection[T], waitTime float64)           {}
func (BasePoolListener[T]) OnReleased(conn *PooledConnection[T], holdTime float64)           {}
func (BasePoolListener[T]) OnAcquireTimeout(waited float64)                                  {}
func (BasePoolListener[T]) OnConnectionLeak(conn *PooledConnection[T], heldFor float64)      {}
func (BasePoolListener[T]) OnBreakerStateChange(old, new BreakerState)                       {}

type listenerGroup[T any] struct {
	listeners []PoolListener[T]
}

func newListenerGroup[T any](listeners []PoolListener[T]) *listenerGroup[T] {
	return &listenerGroup[T]{listeners: append([]PoolListener[T]{}, listeners...)}
}

// safe calls fn for every listener, recovering from and logging any panic so a broken
// listener never breaks the pool.
func (g *listenerGroup[T]) safe(fn func(PoolListener[T])) {
	for _, listener := range g.listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("connpool: pool listener %v failed: %v", listener, r)
				}
			}()
			fn(listener)
		}()
	}
}
