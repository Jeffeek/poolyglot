// Command lifecycle_logger demonstrates comprehensive connection lifecycle logging
// via PoolListener.
//
// It logs every pool event for debugging and diagnostics: understanding connection
// behavior, troubleshooting leaks, and analyzing performance.
//
// Key patterns:
//   - A PoolListener that logs all events with timestamps
//   - Correlate the connection lifecycle: created -> acquired -> released -> closed
//   - Track connection age, hold time and wait time
//   - Log breaker state changes and creation failures
//
// Use cases:
//   - Development/staging: verbose logging for debugging
//   - Production: conditional logging (only slow operations, errors, leaks)
//   - Performance analysis: identify hot connections and wait patterns
//
// Run it with:
//
//	cd go/examples && go run observability/lifecycle_logger.go
package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	connpool "github.com/Jeffeek/poolyglot/go"
)

// LifecycleLogger is a connpool.PoolListener that logs every event with detailed
// context. It tracks connection lifetimes and correlates events by connection ID.
//
// It is safe for concurrent use: listener hooks fire on whichever goroutine owns the
// connection, so counters use sync/atomic and the birth-time map uses sync.Map.
type LifecycleLogger struct {
	poolName string

	// birthTimes maps a connection ID to the instant it was created. sync.Map
	// keeps the hot path lock-free for the concurrent acquire/release scenario.
	birthTimes sync.Map

	// created, acquired, released and closed are plain counters bumped with
	// sync/atomic so concurrent goroutines never race on them.
	created  int64
	acquired int64
	released int64
	closed   int64

	// closeReasons breaks the closed connections down by reason. The map is
	// guarded by mu because it is written from many goroutines.
	mu           sync.Mutex
	closeReasons map[string]int
}

// NewLifecycleLogger creates a lifecycle logger for the named pool.
func NewLifecycleLogger(poolName string) *LifecycleLogger {
	if poolName == "" {
		poolName = "default"
	}
	return &LifecycleLogger{
		poolName:     poolName,
		closeReasons: make(map[string]int),
	}
}

// age returns the seconds elapsed since the connection was created, or 0 when the
// connection is unknown to the logger.
func (l *LifecycleLogger) age(connID int64) float64 {
	birth, ok := l.birthTimes.Load(connID)
	if !ok {
		return 0.0
	}
	return time.Since(birth.(time.Time)).Seconds()
}

// OnConnectionCreated fires when the factory successfully created a connection.
func (l *LifecycleLogger) OnConnectionCreated(conn *connpool.PooledConnection[*fakeConn], duration float64) {
	atomic.AddInt64(&l.created, 1)
	l.birthTimes.Store(conn.ID, time.Now())
	log.Printf("[%s] Connection #%d CREATED in %.3fs (uses=%d)",
		l.poolName, conn.ID, duration, conn.UseCount)
}

// OnConnectionClosed fires when a connection left the pool.
func (l *LifecycleLogger) OnConnectionClosed(conn *connpool.PooledConnection[*fakeConn], reason connpool.CloseReason) {
	atomic.AddInt64(&l.closed, 1)
	age := l.age(conn.ID)
	l.birthTimes.Delete(conn.ID)

	l.mu.Lock()
	l.closeReasons[reason.Value()]++
	l.mu.Unlock()

	log.Printf("[%s] Connection #%d CLOSED after %.1fs (reason=%s, uses=%d)",
		l.poolName, conn.ID, age, reason.Value(), conn.UseCount)
}

// OnCreateFailed fires when the factory failed to create a connection.
func (l *LifecycleLogger) OnCreateFailed(err error) {
	log.Printf("[%s] Connection CREATE FAILED: %v", l.poolName, err)
}

// OnAcquired fires when a caller acquired a connection from the pool.
func (l *LifecycleLogger) OnAcquired(conn *connpool.PooledConnection[*fakeConn], waitTime float64) {
	atomic.AddInt64(&l.acquired, 1)
	if waitTime > 0.1 {
		// Slow acquire: the pool may be undersized.
		log.Printf("[%s] Connection #%d ACQUIRED after %.3fs wait (slow: pool may be undersized)",
			l.poolName, conn.ID, waitTime)
		return
	}
	log.Printf("[%s] Connection #%d ACQUIRED (wait=%.3fs, age=%.1fs, uses=%d)",
		l.poolName, conn.ID, waitTime, l.age(conn.ID), conn.UseCount)
}

// OnReleased fires when a caller returned a connection to the pool.
func (l *LifecycleLogger) OnReleased(conn *connpool.PooledConnection[*fakeConn], holdTime float64) {
	atomic.AddInt64(&l.released, 1)
	if holdTime > 10.0 {
		// Long hold: possible leak or a slow query.
		log.Printf("[%s] Connection #%d RELEASED after %.3fs (held long: possible leak or slow query)",
			l.poolName, conn.ID, holdTime)
		return
	}
	log.Printf("[%s] Connection #%d RELEASED (held=%.3fs, age=%.1fs, uses=%d)",
		l.poolName, conn.ID, holdTime, l.age(conn.ID), conn.UseCount)
}

// OnAcquireTimeout fires when a caller timed out waiting for a connection.
func (l *LifecycleLogger) OnAcquireTimeout(waited float64) {
	log.Printf("[%s] ACQUIRE TIMEOUT after %.3fs (pool exhausted or connections taking too long)",
		l.poolName, waited)
}

// OnConnectionLeak fires when a connection was held longer than the configured
// leak_detection_timeout.
func (l *LifecycleLogger) OnConnectionLeak(conn *connpool.PooledConnection[*fakeConn], heldFor float64) {
	log.Printf("[%s] CONNECTION LEAK DETECTED: #%d held for %.1fs (age=%.1fs, uses=%d) - caller did not release!",
		l.poolName, conn.ID, heldFor, l.age(conn.ID), conn.UseCount)
}

// OnBreakerStateChange fires when the circuit breaker changed state.
func (l *LifecycleLogger) OnBreakerStateChange(old, new connpool.BreakerState) {
	switch new {
	case connpool.BreakerOpen:
		log.Printf("[%s] Circuit breaker OPENED (%s -> %s) - failing fast",
			l.poolName, old.Value(), new.Value())
	case connpool.BreakerHalfOpen:
		log.Printf("[%s] Circuit breaker HALF-OPEN (%s -> %s) - probing recovery",
			l.poolName, old.Value(), new.Value())
	case connpool.BreakerClosed:
		log.Printf("[%s] Circuit breaker CLOSED (%s -> %s) - recovered",
			l.poolName, old.Value(), new.Value())
	}
}

// summary returns a snapshot of the counters collected so far. The close-reason
// breakdown is copied under the lock so the caller can read it without racing.
func (l *LifecycleLogger) summary() (created, acquired, released, closed int64, reasons map[string]int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	reasons = make(map[string]int, len(l.closeReasons))
	for k, v := range l.closeReasons {
		reasons[k] = v
	}
	return atomic.LoadInt64(&l.created), atomic.LoadInt64(&l.acquired),
		atomic.LoadInt64(&l.released), atomic.LoadInt64(&l.closed), reasons
}

// fakeConn is a trivial fake connection: a numbered, closeable handle. The example
// needs no real database.
type fakeConn struct {
	id     int64
	closed bool
}

// connIDs hands out unique ids to the fake connections.
var connIDs int64

// createConn is the factory's Create function.
func createConn() (*fakeConn, error) {
	return &fakeConn{id: atomic.AddInt64(&connIDs, 1)}, nil
}

// Close is the factory's Close function.
func (c *fakeConn) Close() error {
	c.closed = true
	return nil
}

func main() {
	// Configure logging with timestamps (like the Python example's basicConfig).
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	fmt.Println("=== Lifecycle Logging Demo ===")
	fmt.Println()

	// Set up the pool with the lifecycle logger attached.
	logger := NewLifecycleLogger("demo_pool")
	factory := connpool.FuncFactory[*fakeConn]{
		CreateFn:   createConn,
		ValidateFn: func(c *fakeConn) bool { return !c.closed },
		CloseFn:    func(c *fakeConn) error { return c.Close() },
	}

	cfg := connpool.DefaultConfig()
	cfg.MinSize = 2                              // Warm 2 connections
	cfg.MaxSize = 4                              // Max 4 connections
	cfg.MaxUses = connpool.Ptr(5)                // Rotate after 5 uses
	cfg.MaintenanceInterval = connpool.Ptr(2.0)  // Run maintenance every 2s
	cfg.LeakDetectionTimeout = connpool.Ptr(3.0) // Warn if held >3s

	pool, err := connpool.NewConnectionPool(factory, cfg, &connpool.Options[*fakeConn]{
		Name:      "demo_pool",
		Listeners: []connpool.PoolListener[*fakeConn]{logger},
	})
	if err != nil {
		panic(err)
	}

	// Open warms min_size connections and starts the maintenance goroutine.
	if _, err := pool.Open(); err != nil {
		panic(err)
	}
	defer pool.Close(nil)

	log.Println("Pool opened (warming min_size connections)")
	time.Sleep(500 * time.Millisecond) // Let maintenance warm connections
	fmt.Println()

	log.Println("--- Scenario 1: Normal usage (single acquire/release) ---")
	if err := pool.Use(connpool.Of(5.0), func(c *fakeConn) error {
		time.Sleep(200 * time.Millisecond) // Simulate query work
		return nil
	}); err != nil {
		panic(err)
	}
	fmt.Println()

	log.Println("--- Scenario 2: Concurrent requests ---")
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := pool.Use(connpool.Of(5.0), func(c *fakeConn) error {
				time.Sleep(100 * time.Millisecond) // Simulate query work
				return nil
			}); err != nil {
				log.Printf("Error in goroutine %d: %v", idx, err)
			}
		}(i)
	}
	wg.Wait()
	fmt.Println()

	log.Println("--- Scenario 3: Leak detection (simulated) ---")
	// Acquire and hold the connection well past leak_detection_timeout (3s). The
	// maintenance goroutine sweeps every 2s, so the leak is reported on the next pass.
	conn, err := pool.Acquire(connpool.Of(5.0))
	if err != nil {
		panic(err)
	}
	log.Printf("Acquired connection #%d, sleeping for 5s (will trigger leak warning)...", conn.id)
	time.Sleep(5 * time.Second)
	if err := pool.Release(conn); err != nil {
		panic(err)
	}
	fmt.Println()

	log.Println("--- Scenario 4: Connection rotation (max_uses=5) ---")
	// Use the pool repeatedly so a connection crosses the max_uses threshold.
	for i := 0; i < 6; i++ {
		if err := pool.Use(connpool.Of(5.0), func(c *fakeConn) error {
			return nil
		}); err != nil {
			panic(err)
		}
	}

	time.Sleep(500 * time.Millisecond) // Let logs flush

	created, acquired, released, closed, reasons := logger.summary()
	fmt.Println()
	log.Println("--- Pool closing ---")
	fmt.Println()
	fmt.Println("=== Demo complete ===")
	fmt.Printf("created=%d acquired=%d released=%d closed=%d\n", created, acquired, released, closed)
	fmt.Println("close reasons:")
	for _, reason := range []string{
		connpool.CloseReasonMaxUses.Value(),
		connpool.CloseReasonExpired.Value(),
		connpool.CloseReasonIdle.Value(),
		connpool.CloseReasonUnhealthy.Value(),
		connpool.CloseReasonPoolClosed.Value(),
	} {
		if n, ok := reasons[reason]; ok {
			fmt.Printf("  %s: %d\n", reason, n)
		}
	}
	fmt.Println("Review the logs above to see the full connection lifecycle.")
}
