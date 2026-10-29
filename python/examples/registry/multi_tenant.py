"""Example: Multi-tenant connection pooling with per-tier quotas via PoolRegistry.

This demonstrates using PoolRegistry to enforce different pool sizes per tenant tier.
Each tenant gets an isolated pool with appropriate limits based on their subscription.

Key patterns:
- PoolRegistry with a factory function that returns different configs per tenant
- Quota enforcement via max_size (caps concurrent connections)
- Backpressure via max_waiters (caps queued callers, fail fast when exhausted)
- Per-tenant stats() for observability

Use cases:
- SaaS platforms with free/pro/enterprise tiers
- API gateways with per-customer rate limits
- Multi-tenant databases with connection quotas
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

# Add examples/database to path for dbapi_factory
sys.path.insert(0, str(Path(__file__).parent.parent / "database"))

from connpool import ConnectionPool, PoolConfig, PoolRegistry
from dbapi_factory import DbApiFactory


@dataclass
class TenantTier:
    """Subscription tier configuration."""
    name: str
    max_connections: int  # Hard cap on concurrent DB connections
    max_waiters: int      # Cap on queued callers (backpressure)
    request_timeout: float  # Max seconds to wait for a connection


# Tier definitions
TIERS = {
    "free": TenantTier("free", max_connections=2, max_waiters=5, request_timeout=2.0),
    "pro": TenantTier("pro", max_connections=10, max_waiters=20, request_timeout=5.0),
    "enterprise": TenantTier("enterprise", max_connections=50, max_waiters=100, request_timeout=10.0),
}


class TenantDatabase:
    """Simulated per-tenant database (uses SQLite for demo)."""

    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.db_path = f"{tempfile.gettempdir()}/tenant_{tenant_id}.db"

    def get_tier(self) -> str:
        """Simulate tier lookup (in production, query from a user service)."""
        # For demo: tenant_1 -> free, tenant_2 -> pro, tenant_3 -> enterprise
        tier_map = {"tenant_1": "free", "tenant_2": "pro", "tenant_3": "enterprise"}
        return tier_map.get(self.tenant_id, "free")

    def create_connection(self):
        """Create a SQLite connection for this tenant's database."""
        return sqlite3.connect(self.db_path, timeout=5, check_same_thread=False)


def create_factory_for_tenant(tenant_id: str) -> DbApiFactory:
    """Factory function: returns a connection factory for the tenant's database."""
    tenant_db = TenantDatabase(tenant_id)
    tier = TIERS[tenant_db.get_tier()]

    print(f"Creating factory for {tenant_id} (tier: {tier.name}, max_size: {tier.max_connections})")

    return DbApiFactory(tenant_db.create_connection)


def simulate_tenant_request(registry: PoolRegistry, tenant_id: str, request_id: int) -> dict:
    """Simulate a tenant's API request that needs a database connection."""
    start = time.time()
    try:
        with registry.connection(tenant_id) as conn:
            # Simulate query work
            conn.execute("SELECT 1")
            elapsed = time.time() - start
            return {
                "tenant_id": tenant_id,
                "request_id": request_id,
                "status": "success",
                "elapsed": elapsed,
            }
    except Exception as e:
        elapsed = time.time() - start
        return {
            "tenant_id": tenant_id,
            "request_id": request_id,
            "status": "error",
            "error": str(e),
            "elapsed": elapsed,
        }


if __name__ == "__main__":
    # Shared pool config for all tenants (tier-specific sizing applied via factory)
    shared_config = PoolConfig(
        min_size=0,  # Lazy: don't warm connections until first request
        max_size=10,  # Default, can be overridden per tenant
        max_waiters=20,
        acquire_timeout=5.0,
        max_lifetime=600,  # Rotate connections every 10 minutes
        validate_after_idle=30,
    )

    # Create per-tenant pool registry
    tenants = PoolRegistry(create_factory_for_tenant, shared_config)

    # Initialize tenant databases
    for tenant_id in ["tenant_1", "tenant_2", "tenant_3"]:
        with tenants.connection(tenant_id) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS data (id INTEGER, value TEXT)")
            conn.execute("INSERT INTO data VALUES (1, 'test')")
            conn.commit()

    print("\n=== Simulating concurrent requests ===")

    # Simulate burst of requests: 20 requests per tenant concurrently
    # tenant_1 (free, max_size=2) will see queue exhaustion
    # tenant_2 (pro, max_size=10) should handle most requests
    # tenant_3 (enterprise, max_size=50) should handle all requests
    tasks = []
    with ThreadPoolExecutor(max_workers=60) as executor:
        for tenant_id in ["tenant_1", "tenant_2", "tenant_3"]:
            for req_id in range(20):
                task = executor.submit(simulate_tenant_request, tenants, tenant_id, req_id)
                tasks.append(task)

        results = [future.result() for future in as_completed(tasks)]

    # Analyze results
    print("\n=== Results Summary ===")
    for tenant_id in ["tenant_1", "tenant_2", "tenant_3"]:
        tenant_results = [r for r in results if r["tenant_id"] == tenant_id]
        successes = [r for r in tenant_results if r["status"] == "success"]
        errors = [r for r in tenant_results if r["status"] == "error"]

        tier = TIERS[TenantDatabase(tenant_id).get_tier()]
        print(f"\n{tenant_id} ({tier.name} tier, max_size={tier.max_connections}):")
        print(f"  Total requests: {len(tenant_results)}")
        print(f"  Successes: {len(successes)}")
        print(f"  Errors: {len(errors)}")

        if errors:
            error_types = {}
            for err in errors:
                err_msg = err["error"]
                if "PoolExhaustedError" in err_msg:
                    error_types["pool_exhausted"] = error_types.get("pool_exhausted", 0) + 1
                elif "timeout" in err_msg.lower():
                    error_types["timeout"] = error_types.get("timeout", 0) + 1
                else:
                    error_types["other"] = error_types.get("other", 0) + 1
            print(f"  Error breakdown: {error_types}")

        # Pool stats
        pool = tenants.pool(tenant_id)
        stats = pool.stats()
        print(f"  Pool stats: size={stats.size}, idle={stats.idle}, in_use={stats.in_use}")
        print(f"  Acquire wait p95: {stats.p95_acquire_wait:.3f}s")

    # Cleanup
    tenants.close()
    print("\n=== Demonstration complete ===")
    print("Key takeaway: Free tier hit backpressure limits, enterprise tier handled all load.")
