// Package main demonstrates multi-tenant connection pooling with per-tier quotas
// via PoolRegistry.
//
// This example shows how to use PoolRegistry to enforce different pool sizes per
// tenant tier. Each tenant gets an isolated pool with appropriate limits based on
// their subscription.
//
// Key patterns:
//   - PoolRegistry with a factory function that returns different configs per tenant
//   - Quota enforcement via MaxSize (caps concurrent connections)
//   - Backpressure via MaxWaiters (caps queued callers, fail fast when exhausted)
//   - Per-tenant Stats() for observability
//
// Use cases:
//   - SaaS platforms with free/pro/enterprise tiers
//   - API gateways with per-customer rate limits
//   - Multi-tenant databases with connection quotas
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	connpool "github.com/Jeffeek/poolyglot/go"
)

// TenantTier defines a subscription tier configuration.
type TenantTier struct {
	Name           string
	MaxConnections int     // Hard cap on concurrent DB connections
	MaxWaiters     int     // Cap on queued callers (backpressure)
	RequestTimeout float64 // Max seconds to wait for a connection
}

// Tier definitions
var (
	freeTier = TenantTier{
		Name:           "free",
		MaxConnections: 2,
		MaxWaiters:     5,
		RequestTimeout: 2.0,
	}
	proTier = TenantTier{
		Name:           "pro",
		MaxConnections: 10,
		MaxWaiters:     20,
		RequestTimeout: 5.0,
	}
	enterpriseTier = TenantTier{
		Name:           "enterprise",
		MaxConnections: 50,
		MaxWaiters:     100,
		RequestTimeout: 10.0,
	}
	tiers = map[string]TenantTier{
		"free":       freeTier,
		"pro":        proTier,
		"enterprise": enterpriseTier,
	}
)

// TenantDatabase simulates a per-tenant database (uses SQLite for demo).
type TenantDatabase struct {
	TenantID string
	DBPath   string
}

// NewTenantDatabase creates a new tenant database instance.
func NewTenantDatabase(tenantID string) *TenantDatabase {
	return &TenantDatabase{
		TenantID: tenantID,
		DBPath:   filepath.Join(os.TempDir(), fmt.Sprintf("tenant_%s.db", tenantID)),
	}
}

// GetTier simulates tier lookup (in production, query from a user service).
func (td *TenantDatabase) GetTier() string {
	// For demo: tenant_1 -> free, tenant_2 -> pro, tenant_3 -> enterprise
	tierMap := map[string]string{
		"tenant_1": "free",
		"tenant_2": "pro",
		"tenant_3": "enterprise",
	}
	if tier, ok := tierMap[td.TenantID]; ok {
		return tier
	}
	return "free"
}

// CreateConnection creates a SQLite connection for this tenant's database.
func (td *TenantDatabase) CreateConnection() (*sql.DB, error) {
	db, err := sql.Open("sqlite", td.DBPath)
	if err != nil {
		return nil, err
	}
	// Configure connection pool settings for the underlying driver
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	return db, nil
}

// TenantDatabase simulates a per-tenant database (uses SQLite for demo).
type RequestResult struct {
	TenantID  string
	RequestID int
	Status    string // "success" or "error"
	Err       error
	Elapsed   time.Duration
}

// simulateTenantRequest simulates a tenant's API request that needs a database
// connection.
func simulateTenantRequest(pools map[string]*connpool.ConnectionPool[*sql.DB], tenantID string, requestID int) RequestResult {
	start := time.Now()
	pool, ok := pools[tenantID]
	if !ok {
		return RequestResult{
			TenantID:  tenantID,
			RequestID: requestID,
			Status:    "error",
			Err:       fmt.Errorf("pool not found for tenant %s", tenantID),
			Elapsed:   time.Since(start),
		}
	}

	err := pool.Use(connpool.Unset(), func(db *sql.DB) error {
		// Simulate query work
		_, err := db.Exec("SELECT 1")
		return err
	})

	elapsed := time.Since(start)
	if err != nil {
		return RequestResult{
			TenantID:  tenantID,
			RequestID: requestID,
			Status:    "error",
			Err:       err,
			Elapsed:   elapsed,
		}
	}
	return RequestResult{
		TenantID:  tenantID,
		RequestID: requestID,
		Status:    "success",
		Elapsed:   elapsed,
	}
}

// ResultCollector safely collects results from concurrent goroutines.
type ResultCollector struct {
	mu      sync.Mutex
	results []RequestResult
}

// Add adds a result to the collector.
func (rc *ResultCollector) Add(result RequestResult) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.results = append(rc.results, result)
}

// Results returns all collected results.
func (rc *ResultCollector) Results() []RequestResult {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]RequestResult{}, rc.results...)
}

// ErrorBreakdown tracks error types.
type ErrorBreakdown struct {
	mu            sync.Mutex
	poolExhausted int64
	timeout       int64
	other         int64
}

// Classify classifies an error and increments the appropriate counter.
func (eb *ErrorBreakdown) Classify(err error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	var exhausted *connpool.PoolExhaustedError
	var timeout *connpool.PoolTimeoutError

	switch {
	case errors.As(err, &exhausted):
		atomic.AddInt64(&eb.poolExhausted, 1)
	case errors.As(err, &timeout):
		atomic.AddInt64(&eb.timeout, 1)
	default:
		atomic.AddInt64(&eb.other, 1)
	}
}

// Get returns the current error counts.
func (eb *ErrorBreakdown) Get() (poolExhausted, timeout, other int64) {
	return atomic.LoadInt64(&eb.poolExhausted),
		atomic.LoadInt64(&eb.timeout),
		atomic.LoadInt64(&eb.other)
}

func main() {
	// Create per-tenant pools directly (not via registry since each has custom config)
	tenantPools := make(map[string]*connpool.ConnectionPool[*sql.DB])
	tenantIDs := []string{"tenant_1", "tenant_2", "tenant_3"}

	for _, tenantID := range tenantIDs {
		tenantDB := NewTenantDatabase(tenantID)
		tier := tiers[tenantDB.GetTier()]

		fmt.Printf("Creating pool for %s (tier: %s, max_size: %d)\n",
			tenantID, tier.Name, tier.MaxConnections)

		config := connpool.DefaultConfig()
		config.MinSize = 0
		config.MaxSize = tier.MaxConnections
		config.MaxWaiters = connpool.Ptr(tier.MaxWaiters)
		config.AcquireTimeout = connpool.Ptr(tier.RequestTimeout)
		config.MaxLifetime = connpool.Ptr(600.0)
		config.ValidateAfterIdle = connpool.Ptr(30.0)
		config.MaintenanceInterval = nil

		factory := connpool.FuncFactory[*sql.DB]{
			CreateFn: func() (*sql.DB, error) {
				return tenantDB.CreateConnection()
			},
			ValidateFn: func(db *sql.DB) bool {
				return db.Ping() == nil
			},
			CloseFn: func(db *sql.DB) error {
				return db.Close()
			},
		}

		pool, err := connpool.NewConnectionPool[*sql.DB](factory, config, &connpool.Options[*sql.DB]{Name: tenantID})
		if err != nil {
			fmt.Printf("Failed to create pool for %s: %v\n", tenantID, err)
			return
		}
		tenantPools[tenantID] = pool
		defer pool.Close(nil)

		// Open and initialize the pool
		if _, err := pool.Open(); err != nil {
			fmt.Printf("Failed to open pool for %s: %v\n", tenantID, err)
			return
		}

		err = pool.Use(connpool.Unset(), func(db *sql.DB) error {
			if _, err := db.Exec("CREATE TABLE IF NOT EXISTS data (id INTEGER, value TEXT)"); err != nil {
				return err
			}
			if _, err := db.Exec("INSERT INTO data VALUES (1, 'test')"); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			fmt.Printf("Failed to initialize database for %s: %v\n", tenantID, err)
			return
		}
	}

	fmt.Println("\n=== Simulating concurrent requests ===")

	// Simulate burst of requests: 20 requests per tenant concurrently
	// tenant_1 (free, max_size=2) will see queue exhaustion
	// tenant_2 (pro, max_size=10) should handle most requests
	// tenant_3 (enterprise, max_size=50) should handle all requests
	collector := &ResultCollector{}
	var wg sync.WaitGroup

	for _, tenantID := range tenantIDs {
		for reqID := 0; reqID < 20; reqID++ {
			wg.Add(1)
			go func(tid string, rid int) {
				defer wg.Done()
				result := simulateTenantRequest(tenantPools, tid, rid)
				collector.Add(result)
			}(tenantID, reqID)
		}
	}

	wg.Wait()
	results := collector.Results()

	// Analyze results
	fmt.Println("\n=== Results Summary ===")
	for _, tenantID := range tenantIDs {
		tenantDB := NewTenantDatabase(tenantID)
		tier := tiers[tenantDB.GetTier()]

		// Filter results for this tenant
		var tenantResults []RequestResult
		var successes, errorCount int
		errorBreakdown := &ErrorBreakdown{}

		for _, r := range results {
			if r.TenantID == tenantID {
				tenantResults = append(tenantResults, r)
				if r.Status == "success" {
					successes++
				} else {
					errorCount++
					errorBreakdown.Classify(r.Err)
				}
			}
		}

		fmt.Printf("\n%s (%s tier, max_size=%d):\n",
			tenantID, tier.Name, tier.MaxConnections)
		fmt.Printf("  Total requests: %d\n", len(tenantResults))
		fmt.Printf("  Successes: %d\n", successes)
		fmt.Printf("  Errors: %d\n", errorCount)

		if errorCount > 0 {
			poolExh, timeout, other := errorBreakdown.Get()
			fmt.Printf("  Error breakdown: ")
			parts := []string{}
			if poolExh > 0 {
				parts = append(parts, fmt.Sprintf("pool_exhausted=%d", poolExh))
			}
			if timeout > 0 {
				parts = append(parts, fmt.Sprintf("timeout=%d", timeout))
			}
			if other > 0 {
				parts = append(parts, fmt.Sprintf("other=%d", other))
			}
			fmt.Printf("{%s}\n", strings.Join(parts, ", "))
		}

		// Pool stats
		pool, ok := tenantPools[tenantID]
		if !ok {
			fmt.Printf("  Failed to get pool stats: pool not found\n")
			continue
		}
		stats := pool.Stats()
		statsDict := stats.ToDictionary()

		size := int64(0)
		idle := int64(0)
		inUse := int64(0)
		acquireWaitP95 := 0.0

		if v, ok := statsDict["size"].(int64); ok {
			size = v
		}
		if v, ok := statsDict["idle"].(int64); ok {
			idle = v
		}
		if v, ok := statsDict["in_use"].(int64); ok {
			inUse = v
		}
		if v, ok := statsDict["acquire_wait_p95"].(float64); ok {
			acquireWaitP95 = v
		}

		fmt.Printf("  Pool stats: size=%d, idle=%d, in_use=%d\n", size, idle, inUse)
		fmt.Printf("  Acquire wait p95: %.3fs\n", acquireWaitP95)
	}

	fmt.Println("\n=== Demonstration complete ===")
	fmt.Println("Key takeaway: Free tier hit backpressure limits, enterprise tier handled all load.")
}
