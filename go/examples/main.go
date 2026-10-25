// Command examples is a runnable demo of connpool: a small in-memory "connection"
// pooled synchronously and asynchronously. It is NOT part of the library module.
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	connpool "github.com/Jeffeek/poolyglot/go"
)

// conn is a trivial fake connection: a numbered, closeable handle.
type conn struct {
	id     int64
	closed bool
}

var ids int64

func dial() (*conn, error) { return &conn{id: atomic.AddInt64(&ids, 1)}, nil }

func main() {
	syncDemo()
	asyncDemo()
}

func syncDemo() {
	fmt.Println("== sync pool ==")
	cfg := connpool.DefaultConfig()
	cfg.MinSize = 2
	cfg.MaxSize = 4
	cfg.MaintenanceInterval = nil // keep the demo quiet

	pool, err := connpool.NewConnectionPool[*conn](
		connpool.FuncFactory[*conn]{
			CreateFn:   dial,
			ValidateFn: func(c *conn) bool { return !c.closed },
			CloseFn:    func(c *conn) error { c.closed = true; return nil },
		},
		cfg,
		&connpool.Options[*conn]{Name: "demo"},
	)
	if err != nil {
		panic(err)
	}
	if _, err := pool.Open(); err != nil { // warm up MinSize connections
		panic(err)
	}
	defer pool.Close(nil)

	for i := 0; i < 5; i++ {
		if err := pool.Use(connpool.Of(1.0), func(c *conn) error {
			fmt.Printf("  used conn #%d\n", c.id)
			return nil
		}); err != nil {
			panic(err)
		}
	}
	s := pool.Stats()
	fmt.Printf("  stats: size=%d idle=%d acquired=%d created=%d\n", s.Size, s.Idle, s.Acquired, s.Created)
}

func asyncDemo() {
	fmt.Println("== async pool ==")
	cfg := connpool.DefaultConfig()
	cfg.MaxSize = 4
	cfg.MaintenanceInterval = nil

	pool, err := connpool.NewAsyncConnectionPool[*conn](
		connpool.AsyncFuncFactory[*conn]{
			CreateFn: func(ctx context.Context) (*conn, error) { return dial() },
			CloseFn:  func(ctx context.Context, c *conn) error { c.closed = true; return nil },
		},
		cfg,
		nil,
	)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer pool.Close(context.Background(), nil)

	if err := pool.Use(ctx, connpool.Unset(), func(c *conn) error {
		fmt.Printf("  used conn #%d\n", c.id)
		return nil
	}); err != nil {
		panic(err)
	}
	s := pool.Stats()
	fmt.Printf("  stats: size=%d idle=%d acquired=%d\n", s.Size, s.Idle, s.Acquired)
}
