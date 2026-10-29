// Command prometheus_metrics demonstrates exporting pool metrics in Prometheus/StatsD format.
//
// This example shows how to bridge poolyglot's PoolListener observability hooks to external
// monitoring systems. The listener tracks counters on events and periodically samples PoolStats
// for gauge values.
//
// Key patterns:
//   - Embed BasePoolListener and override only the hooks you need
//   - Use sync/atomic for atomic counter increments
//   - Use sync.Mutex to protect map access for reason breakdowns
//   - Export gauges from PoolStats snapshot (sizes, percentiles)
//   - Map CloseReason/BreakerState to metric labels
//
// NOTE: Hooks run synchronously, so avoid blocking I/O directly in the listener.
// Instead, push events to a queue and let a background goroutine send them.
package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	connpool "github.com/Jeffeek/poolyglot/go"
	_ "modernc.org/sqlite"
)

// PrometheusMetrics is a PoolListener that tracks metrics compatible with Prometheus/StatsD.
//
// Counters are incremented on events; gauges are read from pool.Stats() periodically.
// In production, push these to your metrics backend (Prometheus pushgateway, StatsD, etc.)
// from a background goroutine to avoid blocking the pool.
type PrometheusMetrics struct {
	connpool.BasePoolListener[*sql.DB]
	poolName string

	// Counters (monotonic) - use atomic operations
	connectionsCreatedTotal  int64
	connectionsClosedTotal   int64
	createFailuresTotal      int64
	acquiresTotal            int64
	acquireTimeoutsTotal     int64
	releasesTotal            int64
	leaksDetectedTotal       int64
	breakerStateChangesTotal int64

	// Per-reason breakdown (requires mutex)
	mu                          sync.Mutex
	connectionsClosedByReason   map[string]int64
	breakerStateCurrentInternal connpool.BreakerState
}

// NewPrometheusMetrics creates a new PrometheusMetrics listener.
func NewPrometheusMetrics(poolName string) *PrometheusMetrics {
	return &PrometheusMetrics{
		poolName:                    poolName,
		connectionsClosedByReason:   make(map[string]int64),
		breakerStateCurrentInternal: connpool.BreakerClosed,
	}
}

// OnConnectionCreated increments the connections_created_total counter.
func (m *PrometheusMetrics) OnConnectionCreated(conn *connpool.PooledConnection[*sql.DB], duration float64) {
	count := atomic.AddInt64(&m.connectionsCreatedTotal, 1)
	// Example: connpool_connections_created_total{pool="default"} 42
	log.Printf("connpool_connections_created_total{pool=\"%s\"} %d", m.poolName, count)
}

// OnConnectionClosed increments the connections_closed_total counter and tracks by reason.
func (m *PrometheusMetrics) OnConnectionClosed(conn *connpool.PooledConnection[*sql.DB], reason connpool.CloseReason) {
	atomic.AddInt64(&m.connectionsClosedTotal, 1)
	reasonLabel := reason.Value() // "expired", "max_uses", "error", "evicted", "shutdown"

	m.mu.Lock()
	m.connectionsClosedByReason[reasonLabel]++
	count := m.connectionsClosedByReason[reasonLabel]
	m.mu.Unlock()

	// Example: connpool_connections_closed_total{pool="default",reason="expired"} 12
	log.Printf("connpool_connections_closed_total{pool=\"%s\",reason=\"%s\"} %d", m.poolName, reasonLabel, count)
}

// OnCreateFailed increments the create_failures_total counter.
func (m *PrometheusMetrics) OnCreateFailed(err error) {
	count := atomic.AddInt64(&m.createFailuresTotal, 1)
	// Example: connpool_create_failures_total{pool="default"} 3
	log.Printf("connpool_create_failures_total{pool=\"%s\"} %d", m.poolName, count)
}

// OnAcquired increments the acquires_total counter.
func (m *PrometheusMetrics) OnAcquired(conn *connpool.PooledConnection[*sql.DB], waitTime float64) {
	atomic.AddInt64(&m.acquiresTotal, 1)
}

// OnReleased increments the releases_total counter.
func (m *PrometheusMetrics) OnReleased(conn *connpool.PooledConnection[*sql.DB], holdTime float64) {
	atomic.AddInt64(&m.releasesTotal, 1)
}

// OnAcquireTimeout increments the acquire_timeouts_total counter.
func (m *PrometheusMetrics) OnAcquireTimeout(waited float64) {
	count := atomic.AddInt64(&m.acquireTimeoutsTotal, 1)
	log.Printf("connpool_acquire_timeouts_total{pool=\"%s\"} %d", m.poolName, count)
}

// OnConnectionLeak increments the leaks_detected_total counter.
func (m *PrometheusMetrics) OnConnectionLeak(conn *connpool.PooledConnection[*sql.DB], heldFor float64) {
	count := atomic.AddInt64(&m.leaksDetectedTotal, 1)
	log.Printf("connpool_leaks_detected_total{pool=\"%s\"} %d (connection held for %.1fs)", m.poolName, count, heldFor)
}

// OnBreakerStateChange increments the breaker_state_changes_total counter and tracks current state.
func (m *PrometheusMetrics) OnBreakerStateChange(old, new connpool.BreakerState) {
	count := atomic.AddInt64(&m.breakerStateChangesTotal, 1)
	m.mu.Lock()
	m.breakerStateCurrentInternal = new
	m.mu.Unlock()
	log.Printf("connpool_breaker_state_changes_total{pool=\"%s\"} %d (state: %s -> %s)", m.poolName, count, old.Value(), new.Value())
}

// ExportGauges converts a PoolStats snapshot to Prometheus gauge format.
//
// Call this periodically (e.g., every 15s) and push to your metrics backend.
// Returns a slice of formatted metric lines.
func (m *PrometheusMetrics) ExportGauges(stats connpool.PoolStats) []string {
	m.mu.Lock()
	breakerState := m.breakerStateCurrentInternal
	m.mu.Unlock()

	lines := []string{
		// Pool sizes
		fmt.Sprintf("connpool_size{pool=\"%s\"} %d", m.poolName, stats.Size),
		fmt.Sprintf("connpool_idle{pool=\"%s\"} %d", m.poolName, stats.Idle),
		fmt.Sprintf("connpool_in_use{pool=\"%s\"} %d", m.poolName, stats.InUse),
		fmt.Sprintf("connpool_waiting{pool=\"%s\"} %d", m.poolName, stats.Waiting),
		fmt.Sprintf("connpool_max_size{pool=\"%s\"} %d", m.poolName, stats.MaxSize),
		// Acquire-wait percentiles (seconds)
		fmt.Sprintf("connpool_acquire_wait_p50{pool=\"%s\"} %.6f", m.poolName, stats.P50AcquireWait),
		fmt.Sprintf("connpool_acquire_wait_p95{pool=\"%s\"} %.6f", m.poolName, stats.P95AcquireWait),
		fmt.Sprintf("connpool_acquire_wait_p99{pool=\"%s\"} %.6f", m.poolName, stats.P99AcquireWait),
		// Create-latency percentiles (seconds)
		fmt.Sprintf("connpool_create_latency_p50{pool=\"%s\"} %.6f", m.poolName, stats.P50CreateLatency),
		fmt.Sprintf("connpool_create_latency_p95{pool=\"%s\"} %.6f", m.poolName, stats.P95CreateLatency),
		fmt.Sprintf("connpool_create_latency_p99{pool=\"%s\"} %.6f", m.poolName, stats.P99CreateLatency),
		// Circuit breaker state (0=closed, 1=open, 2=half_open)
		fmt.Sprintf("connpool_breaker_state{pool=\"%s\"} %d", m.poolName, breakerStateValue(breakerState)),
	}
	return lines
}

// breakerStateValue maps BreakerState to numeric value for gauge.
func breakerStateValue(state connpool.BreakerState) int {
	switch state.Value() {
	case "closed":
		return 0
	case "open":
		return 1
	case "half_open":
		return 2
	default:
		return -1
	}
}

// sqlConnFactory is a ConnectionFactory that creates *sql.DB connections.
type sqlConnFactory struct {
	db *sql.DB
}

func (f *sqlConnFactory) Create() (*sql.DB, error) {
	// For demo purposes, return the shared DB handle
	// In production, you might pool sql.Conn instead
	if err := f.db.Ping(); err != nil {
		return nil, err
	}
	return f.db, nil
}

func (f *sqlConnFactory) Validate(conn *sql.DB) bool {
	return conn.Ping() == nil
}

func (f *sqlConnFactory) Close(conn *sql.DB) error {
	// Don't close the shared DB handle
	return nil
}

func (f *sqlConnFactory) Reset(conn *sql.DB) error { return nil }

func (f *sqlConnFactory) IsFatalError(err error) bool { return true }

func main() {
	// Set up SQLite database
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create demo table
	if _, err := db.Exec("CREATE TABLE demo (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	// Set up pool with metrics listener
	metrics := NewPrometheusMetrics("demo_pool")
	factory := &sqlConnFactory{db: db}
	config := connpool.DefaultConfig()
	config.MinSize = 2
	config.MaxSize = 4
	config.MaintenanceInterval = connpool.Ptr(2.0)

	pool, err := connpool.NewConnectionPool[*sql.DB](
		factory,
		config,
		&connpool.Options[*sql.DB]{
			Name:      "demo_pool",
			Listeners: []connpool.PoolListener[*sql.DB]{metrics},
		},
	)
	if err != nil {
		log.Fatalf("failed to create pool: %v", err)
	}

	// Open pool (warms up MinSize connections)
	if _, err := pool.Open(); err != nil {
		log.Fatalf("failed to open pool: %v", err)
	}
	defer pool.Close(nil)

	// Do some work
	fmt.Println("\n=== Running demo workload ===")
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			err := pool.Use(connpool.Of(5.0), func(conn *sql.DB) error {
				_, err := conn.Exec("INSERT INTO demo (value) VALUES (?)", fmt.Sprintf("row-%d", n))
				if err != nil {
					return err
				}
				time.Sleep(100 * time.Millisecond)
				return nil
			})
			if err != nil {
				log.Printf("workload error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Export gauges periodically (in production, do this from a background goroutine)
	stats := pool.Stats()
	fmt.Println("\n=== Prometheus Metrics ===")
	for _, line := range metrics.ExportGauges(stats) {
		fmt.Println(line)
	}

	fmt.Printf("\nconnpool_connections_created_total{pool=\"demo_pool\"} %d\n", atomic.LoadInt64(&metrics.connectionsCreatedTotal))
	fmt.Printf("connpool_acquires_total{pool=\"demo_pool\"} %d\n", atomic.LoadInt64(&metrics.acquiresTotal))
	fmt.Printf("connpool_releases_total{pool=\"demo_pool\"} %d\n", atomic.LoadInt64(&metrics.releasesTotal))
}
