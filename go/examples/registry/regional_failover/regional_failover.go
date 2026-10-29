// Example: Regional failover with circuit breaker via PoolRegistry.
//
// This demonstrates using PoolRegistry with circuit breakers to handle regional database
// failover. When the primary region's breaker opens (too many connection failures),
// requests automatically fail fast and can fallback to a secondary region.
//
// Key patterns:
// - PoolRegistry with one pool per region (us-east-1, us-west-2)
// - Circuit breaker guards connection creation in each pool
// - OnBreakerStateChange listener for alerting
// - Graceful degradation: try primary, fallback to secondary on breaker open
//
// Use cases:
// - Multi-region database replicas with automatic failover
// - Geographic load distribution with health-aware routing
// - Disaster recovery with circuit-breaker-driven failover
//
// Run: cd go/examples && go run registry/regional_failover.go

package main

import (
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"time"

	connpool "github.com/Jeffeek/poolyglot/go"
)

// RegionalDatabase simulates a regional database (uses SQLite, simulates failures).
type RegionalDatabase struct {
	region      string
	failureRate float64 // 0.0 = healthy, 1.0 = always fails
	dbPath      string
	mu          sync.RWMutex
}

// NewRegionalDatabase creates a new simulated regional database.
func NewRegionalDatabase(region string, failureRate float64) *RegionalDatabase {
	return &RegionalDatabase{
		region:      region,
		failureRate: failureRate,
		dbPath:      filepath.Join(os.TempDir(), fmt.Sprintf("db_%s.db", region)),
	}
}

// SetFailureRate updates the failure rate (for demo purposes).
func (rd *RegionalDatabase) SetFailureRate(rate float64) {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	rd.failureRate = rate
}

// CreateConnection creates a connection to this region's database.
func (rd *RegionalDatabase) CreateConnection() (*sql.DB, error) {
	rd.mu.RLock()
	rate := rd.failureRate
	rd.mu.RUnlock()

	// Simulate connection failures
	if rand.Float64() < rate {
		return nil, fmt.Errorf("failed to connect to %s", rd.region)
	}

	db, err := sql.Open("sqlite", rd.dbPath)
	if err != nil {
		return nil, err
	}

	// Tag connection with region for logging
	if _, err := db.Exec("PRAGMA application_id = 42"); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// BreakerAlertListener alerts when circuit breaker state changes.
type BreakerAlertListener struct {
	connpool.BasePoolListener[*sql.DB]
	region string
}

// NewBreakerAlertListener creates a new breaker alert listener.
func NewBreakerAlertListener(region string) *BreakerAlertListener {
	return &BreakerAlertListener{region: region}
}

// OnBreakerStateChange is called when the circuit breaker state changes.
func (l *BreakerAlertListener) OnBreakerStateChange(old, new connpool.BreakerState) {
	switch new {
	case connpool.BreakerOpen:
		fmt.Printf("🚨 ALERT: %s circuit breaker OPENED (region unhealthy)\n", l.region)
	case connpool.BreakerHalfOpen:
		fmt.Printf("⚠️  %s circuit breaker HALF-OPEN (probing recovery)\n", l.region)
	case connpool.BreakerClosed:
		if old != connpool.BreakerClosed {
			fmt.Printf("✅ %s circuit breaker CLOSED (region recovered)\n", l.region)
		}
	}
}

// regionalDatabases tracks databases for failure injection.
var (
	regionalDatabases   = make(map[string]*RegionalDatabase)
	regionalDatabasesMu sync.Mutex
)

// CreatePoolForRegion creates a pool for the given region with circuit breaker.
func CreatePoolForRegion(region string) (*connpool.ConnectionPool[*sql.DB], error) {
	// Simulate regional health: both start healthy
	regionalDb := NewRegionalDatabase(region, 0.0)

	regionalDatabasesMu.Lock()
	regionalDatabases[region] = regionalDb
	regionalDatabasesMu.Unlock()

	config := connpool.DefaultConfig()
	config.MinSize = 1
	config.MaxSize = 5
	config.AcquireTimeout = connpool.Ptr(3.0)
	config.MaxLifetime = connpool.Ptr(300.0)
	// Circuit breaker config: open after 3 failures, recover after 10s
	config.Breaker = &connpool.BreakerConfig{
		FailureThreshold: 3,
		RecoveryTimeout:  10.0,
		HalfOpenMaxCalls: 2,
		SuccessThreshold: 1,
	}
	// Retry once before giving up (breaker counts this as 1 failure)
	config.Retry = connpool.RetryPolicy{
		MaxAttempts: 2,
		BaseDelay:   0.1,
		MaxDelay:    5.0,
		Multiplier:  2.0,
		Jitter:      0.5,
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	factory := connpool.FuncFactory[*sql.DB]{
		CreateFn: regionalDb.CreateConnection,
		ValidateFn: func(db *sql.DB) bool {
			return db.Ping() == nil
		},
		CloseFn: func(db *sql.DB) error {
			return db.Close()
		},
		IsFatalErrorFn: func(err error) bool {
			return true // All connection errors are fatal
		},
	}

	listener := NewBreakerAlertListener(region)

	opts := &connpool.Options[*sql.DB]{
		Listeners: []connpool.PoolListener[*sql.DB]{listener},
	}

	return connpool.NewConnectionPool(factory, config, opts)
}

// QueryResult represents the result of a query with failover.
type QueryResult struct {
	Region  string
	Status  string
	Elapsed float64
	Error   error
}

// ExecuteQueryWithFailover executes a query with regional failover.
//
// Try primary region first; if breaker is open or connection fails, try secondary.
func ExecuteQueryWithFailover(pools map[string]*connpool.ConnectionPool[*sql.DB], query string) QueryResult {
	const primary = "us-east-1"
	const secondary = "us-west-2"

	start := time.Now()

	err := pools[primary].Use(connpool.Unset(), func(conn *sql.DB) error {
		_, err := conn.Exec(query)
		return err
	})

	if err == nil {
		return QueryResult{Region: primary, Status: "success", Elapsed: time.Since(start).Seconds()}
	}

	fmt.Printf("Primary region (%s) failed: %v\n", primary, err)

	errSecondary := pools[secondary].Use(connpool.Unset(), func(conn *sql.DB) error {
		_, err := conn.Exec(query)
		return err
	})

	elapsed := time.Since(start).Seconds()

	if errSecondary == nil {
		fmt.Printf("✅ Failover to %s succeeded\n", secondary)
		return QueryResult{Region: secondary, Status: "success (failover)", Elapsed: elapsed}
	}

	return QueryResult{Region: "both_failed", Status: "error", Elapsed: elapsed, Error: errSecondary}
}

func main() {
	fmt.Println("=== Regional Failover Demo ===\n")

	// Create per-region pools directly (not via registry because each region has custom breaker config)
	usEast, err := CreatePoolForRegion("us-east-1")
	if err != nil {
		panic(err)
	}
	defer usEast.Close(nil)

	usWest, err := CreatePoolForRegion("us-west-2")
	if err != nil {
		panic(err)
	}
	defer usWest.Close(nil)

	regionPools := map[string]*connpool.ConnectionPool[*sql.DB]{
		"us-east-1": usEast,
		"us-west-2": usWest,
	}

	// Initialize databases
	for region, pool := range regionPools {
		err := pool.Use(connpool.Unset(), func(conn *sql.DB) error {
			_, err := conn.Exec("CREATE TABLE IF NOT EXISTS metrics (ts INTEGER, value REAL)")
			if err != nil {
				return err
			}
			_, err = conn.Exec("INSERT INTO metrics VALUES (?, ?)", time.Now().Unix(), 42.0)
			return err
		})
		if err != nil {
			panic(fmt.Sprintf("%s init failed: %v", region, err))
		}
	}

	fmt.Println("Phase 1: Both regions healthy\n")
	for i := 0; i < 3; i++ {
		result := ExecuteQueryWithFailover(regionPools, "SELECT COUNT(*) FROM metrics")
		fmt.Printf("Request %d: %s - %s (%.3fs)\n", i+1, result.Region, result.Status, result.Elapsed)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n" + "============================================================")
	fmt.Println("Phase 2: Injecting failures into primary region (us-east-1)")
	fmt.Println("============================================================\n")

	// Simulate primary region becoming unhealthy
	regionalDatabasesMu.Lock()
	regionalDatabases["us-east-1"].SetFailureRate(1.0) // 100% failure rate
	regionalDatabasesMu.Unlock()

	fmt.Println("Attempting requests that will fail on primary...\n")
	for i := 0; i < 5; i++ {
		result := ExecuteQueryWithFailover(regionPools, "SELECT COUNT(*) FROM metrics")
		fmt.Printf("Request %d: %s - %s\n", i+1, result.Region, result.Status)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n" + "============================================================")
	fmt.Println("Phase 3: Circuit breaker opened - fast failover to secondary")
	fmt.Println("============================================================\n")

	// Now requests should fail fast and immediately use secondary
	for i := 0; i < 3; i++ {
		result := ExecuteQueryWithFailover(regionPools, "SELECT COUNT(*) FROM metrics")
		fmt.Printf("Request %d: %s - %s (%.3fs)\n", i+1, result.Region, result.Status, result.Elapsed)
		fmt.Println("  (Notice fast failover - no slow connection attempts)")
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n=== Pool Statistics ===\n")
	for _, region := range []string{"us-east-1", "us-west-2"} {
		stats := regionPools[region].Stats()
		fmt.Printf("%s:\n", region)
		if stats.BreakerState != nil {
			fmt.Printf("  Breaker state: %s\n", *stats.BreakerState)
		}
		fmt.Printf("  Total created: %d\n", stats.Created)
		fmt.Printf("  Total closed: %d\n", stats.Closed)
		fmt.Printf("  Size/max: %d/%d\n", stats.Size, stats.MaxSize)
		fmt.Printf("  Acquire wait p95: %.3fs\n\n", stats.P95AcquireWait)
	}

	fmt.Println("=== Demo complete ===")
	fmt.Println("Key takeaway: Circuit breaker enables fast failover without waiting for timeouts.")
}
