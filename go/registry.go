package connpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// errRegistryClosed is returned by a registry after Close.
var errRegistryClosed = errors.New("registry is closed")

// PoolRegistry hands out one ConnectionPool per key (per host, tenant, database…), created
// on demand. It is lazy: a pool is built on first use and is NOT Open()-ed, so it does no
// background maintenance unless you open it yourself (reg.Pool(key) then pool.Open()).
type PoolRegistry[K comparable, T comparable] struct {
	factoryFor func(K) ConnectionFactory[T]
	config     PoolConfig
	opts       *Options[T]
	mu         sync.Mutex
	pools      map[K]*ConnectionPool[T]
	closed     bool
}

// NewPoolRegistry builds a registry. factoryFor makes the factory for a given key; config is
// shared by every pool; opts (may be nil) is applied to each pool (its Name is overridden
// with the key).
func NewPoolRegistry[K comparable, T comparable](factoryFor func(K) ConnectionFactory[T], config PoolConfig, opts *Options[T]) *PoolRegistry[K, T] {
	return &PoolRegistry[K, T]{
		factoryFor: factoryFor,
		config:     config,
		opts:       opts,
		pools:      make(map[K]*ConnectionPool[T]),
	}
}

// Pool returns the pool for key, creating it on first use.
func (r *PoolRegistry[K, T]) Pool(key K) (*ConnectionPool[T], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errRegistryClosed
	}
	if pool, ok := r.pools[key]; ok {
		return pool, nil
	}
	opts := poolOptionsFor(r.opts, fmt.Sprint(key))
	pool, err := NewConnectionPool(r.factoryFor(key), r.config, opts)
	if err != nil {
		return nil, err
	}
	r.pools[key] = pool
	return pool, nil
}

// Use borrows a connection from the pool for key for the duration of fn.
func (r *PoolRegistry[K, T]) Use(key K, timeout Timeout, fn func(conn T) error) error {
	pool, err := r.Pool(key)
	if err != nil {
		return err
	}
	return pool.Use(timeout, fn)
}

// Close closes every pool and rejects further use.
func (r *PoolRegistry[K, T]) Close() error {
	r.mu.Lock()
	r.closed = true
	pools := r.pools
	r.pools = make(map[K]*ConnectionPool[T])
	r.mu.Unlock()
	for _, pool := range pools {
		_ = pool.Close(nil)
	}
	return nil
}

// AsyncPoolRegistry is the async twin of PoolRegistry: one AsyncConnectionPool per key.
type AsyncPoolRegistry[K comparable, T comparable] struct {
	factoryFor func(K) AsyncConnectionFactory[T]
	config     PoolConfig
	opts       *Options[T]
	mu         sync.Mutex
	pools      map[K]*AsyncConnectionPool[T]
	closed     bool
}

// NewAsyncPoolRegistry builds an async registry (see NewPoolRegistry).
func NewAsyncPoolRegistry[K comparable, T comparable](factoryFor func(K) AsyncConnectionFactory[T], config PoolConfig, opts *Options[T]) *AsyncPoolRegistry[K, T] {
	return &AsyncPoolRegistry[K, T]{
		factoryFor: factoryFor,
		config:     config,
		opts:       opts,
		pools:      make(map[K]*AsyncConnectionPool[T]),
	}
}

// Pool returns the async pool for key, creating it on first use.
func (r *AsyncPoolRegistry[K, T]) Pool(key K) (*AsyncConnectionPool[T], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errRegistryClosed
	}
	if pool, ok := r.pools[key]; ok {
		return pool, nil
	}
	opts := poolOptionsFor(r.opts, fmt.Sprint(key))
	pool, err := NewAsyncConnectionPool(r.factoryFor(key), r.config, opts)
	if err != nil {
		return nil, err
	}
	r.pools[key] = pool
	return pool, nil
}

// Use borrows a connection from the async pool for key for the duration of fn.
func (r *AsyncPoolRegistry[K, T]) Use(ctx context.Context, key K, timeout Timeout, fn func(conn T) error) error {
	pool, err := r.Pool(key)
	if err != nil {
		return err
	}
	return pool.Use(ctx, timeout, fn)
}

// Close closes every pool and rejects further use.
func (r *AsyncPoolRegistry[K, T]) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	pools := r.pools
	r.pools = make(map[K]*AsyncConnectionPool[T])
	r.mu.Unlock()
	for _, pool := range pools {
		_ = pool.Close(ctx, nil)
	}
	return nil
}

// poolOptionsFor clones base (or an empty Options) with Name set to name, so each pool in a
// registry is named after its key without mutating the shared template.
func poolOptionsFor[T comparable](base *Options[T], name string) *Options[T] {
	out := &Options[T]{Name: name}
	if base != nil {
		out.Clock = base.Clock
		out.Rand = base.Rand
		out.Listeners = base.Listeners
	}
	return out
}
