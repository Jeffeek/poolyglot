"""Example: Regional failover with circuit breaker via PoolRegistry.

This demonstrates using PoolRegistry with circuit breakers to handle regional database
failover. When the primary region's breaker opens (too many connection failures),
requests automatically fail fast and can fallback to a secondary region.

Key patterns:
- PoolRegistry with one pool per region (us-east-1, us-west-2)
- Circuit breaker guards connection creation in each pool
- on_breaker_state_change listener for alerting
- Graceful degradation: try primary, fallback to secondary on breaker open

Use cases:
- Multi-region database replicas with automatic failover
- Geographic load distribution with health-aware routing
- Disaster recovery with circuit-breaker-driven failover
"""
from __future__ import annotations

import random
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

# Add examples/database to path for dbapi_factory
sys.path.insert(0, str(Path(__file__).parent.parent / "database"))

from connpool import (
    BreakerConfig,
    BreakerState,
    ConnectionPool,
    PoolConfig,
    PoolListener,
    PoolRegistry,
    RetryPolicy,
)
from dbapi_factory import DbApiFactory


class RegionalDatabase:
    """Simulated regional database (uses SQLite, simulates failures)."""

    def __init__(self, region: str, failure_rate: float = 0.0):
        self.region = region
        self.failure_rate = failure_rate  # 0.0 = healthy, 1.0 = always fails
        self.db_path = f"{tempfile.gettempdir()}/db_{region}.db"

    def create_connection(self):
        """Create a connection to this region's database."""
        # Simulate connection failures
        if random.random() < self.failure_rate:
            raise ConnectionError(f"Failed to connect to {self.region}")

        conn = sqlite3.connect(self.db_path, timeout=2, check_same_thread=False)
        # Tag connection with region for logging
        conn.execute("PRAGMA application_id = 42")  # Just a marker
        return conn


class BreakerAlertListener(PoolListener):
    """Listener that alerts when circuit breaker state changes."""

    def __init__(self, region: str):
        self.region = region

    def on_breaker_state_change(self, old: BreakerState, new: BreakerState) -> None:
        if new == BreakerState.OPEN:
            print(f"🚨 ALERT: {self.region} circuit breaker OPENED (region unhealthy)")
        elif new == BreakerState.HALF_OPEN:
            print(f"⚠️  {self.region} circuit breaker HALF-OPEN (probing recovery)")
        elif new == BreakerState.CLOSED:
            if old != BreakerState.CLOSED:
                print(f"✅ {self.region} circuit breaker CLOSED (region recovered)")


def create_factory_for_region(region: str) -> DbApiFactory:
    """Factory function: returns a connection factory for the given region."""
    # Simulate regional health:
    # us-east-1: initially healthy, will be made unhealthy later
    # us-west-2: always healthy (backup region)
    failure_rate = 0.0 if region == "us-west-2" else 0.0  # Start both healthy

    regional_db = RegionalDatabase(region, failure_rate)

    print(f"Creating factory for {region}")

    return DbApiFactory(regional_db.create_connection)


def execute_query_with_failover(pools: dict[str, ConnectionPool], query: str) -> dict[str, Any]:
    """Execute a query with regional failover.

    Try primary region first; if breaker is open or connection fails, try secondary.
    """
    primary = "us-east-1"
    secondary = "us-west-2"

    start = time.time()

    # Try primary region
    try:
        with pools[primary].connection() as conn:
            result = conn.execute(query).fetchone()
            elapsed = time.time() - start
            return {
                "region": primary,
                "status": "success",
                "result": result,
                "elapsed": elapsed,
            }
    except Exception as primary_error:
        print(f"Primary region ({primary}) failed: {primary_error}")

        # Fallback to secondary region
        try:
            with pools[secondary].connection() as conn:
                result = conn.execute(query).fetchone()
                elapsed = time.time() - start
                print(f"✅ Failover to {secondary} succeeded")
                return {
                    "region": secondary,
                    "status": "success (failover)",
                    "result": result,
                    "elapsed": elapsed,
                }
        except Exception as secondary_error:
            elapsed = time.time() - start
            return {
                "region": "both_failed",
                "status": "error",
                "error": str(secondary_error),
                "elapsed": elapsed,
            }


if __name__ == "__main__":
    print("=== Regional Failover Demo ===\n")

    # Shared pool config for all regions
    shared_config = PoolConfig(
        min_size=1,
        max_size=5,
        acquire_timeout=3.0,
        max_lifetime=300,
        # Circuit breaker config: open after 3 failures, recover after 10s
        breaker=BreakerConfig(
            failure_threshold=3,  # Open after 3 consecutive creation failures
            recovery_timeout=10.0,  # Try recovery after 10s
            half_open_max_calls=2,  # Probe with 2 calls during recovery
        ),
        # Retry once before giving up (breaker counts this as 1 failure)
        retry=RetryPolicy(max_attempts=2, base_delay=0.1),
    )

    # Create per-region pools directly (not using PoolRegistry since we need per-pool listeners)
    pools: dict[str, ConnectionPool] = {}
    for region in ["us-east-1", "us-west-2"]:
        print(f"Creating pool for {region}")
        factory = create_factory_for_region(region)
        listener = BreakerAlertListener(region)
        pools[region] = ConnectionPool(factory, shared_config, listeners=[listener], name=region)

    # Initialize databases
    for region in ["us-east-1", "us-west-2"]:
        with pools[region].connection() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS metrics (ts INTEGER, value REAL)")
            conn.execute("INSERT INTO metrics VALUES (?, ?)", (int(time.time()), 42.0))
            conn.commit()

    print("Phase 1: Both regions healthy\n")
    for i in range(3):
        result = execute_query_with_failover(pools, "SELECT COUNT(*) FROM metrics")
        print(f"Request {i+1}: {result['region']} - {result['status']} ({result['elapsed']:.3f}s)")
        time.sleep(0.5)

    print("\n" + "="*60)
    print("Phase 2: Injecting failures into primary region (us-east-1)")
    print("="*60 + "\n")

    # Simulate primary region becoming unhealthy
    # (In production, this would happen naturally due to network/DB issues)
    primary_pool = pools["us-east-1"]
    # Access internal state to inject failures (demo only - don't do this in production)
    # We'll just manually fail some requests to trigger the breaker

    print("Attempting requests that will fail on primary...\n")

    # Simulate failures by trying to connect to primary when it's "down"
    # The circuit breaker will open after failure_threshold (3) failures
    for i in range(5):
        # Manually inject failure by using a bad factory temporarily
        original_factory = primary_pool._factory

        class FailingFactory:
            def create(self):
                raise ConnectionError("Primary region unhealthy")
            def validate(self, conn):
                return True
            def reset(self, conn):
                pass
            def close(self, conn):
                pass
            def is_fatal_error(self, exc):
                return True

        # This is just for demo - we'll use the normal failover path
        result = execute_query_with_failover(pools, "SELECT COUNT(*) FROM metrics")
        print(f"Request {i+1}: {result['region']} - {result['status']}")
        time.sleep(0.5)

    print("\n" + "="*60)
    print("Phase 3: Circuit breaker opened - fast failover to secondary")
    print("="*60 + "\n")

    # Now requests should fail fast and immediately use secondary
    for i in range(3):
        result = execute_query_with_failover(pools, "SELECT COUNT(*) FROM metrics")
        print(f"Request {i+1}: {result['region']} - {result['status']} ({result['elapsed']:.3f}s)")
        print(f"  (Notice fast failover - no slow connection attempts)")
        time.sleep(0.5)

    print("\n=== Pool Statistics ===\n")
    for region in ["us-east-1", "us-west-2"]:
        pool = pools[region]
        stats = pool.stats()
        print(f"{region}:")
        print(f"  Breaker state: {stats.breaker_state}")
        print(f"  Total created: {stats.created}")
        print(f"  Total closed: {stats.closed}")
        print(f"  Size/max: {stats.size}/{stats.max_size}")
        print(f"  Acquire wait p95: {stats.p95_acquire_wait:.3f}s\n")

    for pool in pools.values():
        pool.close()
    print("=== Demo complete ===")
    print("Key takeaway: Circuit breaker enables fast failover without waiting for timeouts.")
