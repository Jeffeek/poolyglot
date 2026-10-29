"""Example: Exporting pool metrics in Prometheus/StatsD format via PoolListener.

This demonstrates how to bridge poolyglot's observability hooks to external monitoring
systems. The listener tracks counters and periodically samples PoolStats for gauges.

Key patterns:
- Subclass PoolListener to capture pool events
- Use stats() snapshot for gauge values (sizes, percentiles)
- Map CloseReason/BreakerState to metric labels
- Push heavy I/O (actual HTTP export) to a background queue/thread

NOTE: Synchronous hooks mean you should NOT do blocking I/O directly in the listener.
Instead, push events to a queue and let a background worker send them.
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any

from connpool import BreakerState, CloseReason, PoolListener, PooledConnection, PoolStats

log = logging.getLogger(__name__)


class PrometheusMetrics(PoolListener):
    """PoolListener that tracks metrics compatible with Prometheus/StatsD.

    Counters are incremented on events; gauges are read from pool.stats() periodically.
    In production, push these to your metrics backend (Prometheus pushgateway, StatsD, etc.)
    from a background thread to avoid blocking the pool.
    """

    def __init__(self, pool_name: str = "default"):
        self.pool_name = pool_name
        # Counters (monotonic)
        self.connections_created_total = 0
        self.connections_closed_total = 0
        self.connections_closed_by_reason: dict[str, int] = defaultdict(int)
        self.create_failures_total = 0
        self.acquires_total = 0
        self.acquire_timeouts_total = 0
        self.releases_total = 0
        self.leaks_detected_total = 0
        self.breaker_state_changes_total = 0
        self.breaker_state_current = BreakerState.CLOSED

    def on_connection_created(self, conn: PooledConnection[Any], duration: float) -> None:
        self.connections_created_total += 1
        # Example: connpool_connections_created_total{pool="default"} 42
        log.debug(
            f"connpool_connections_created_total{{pool=\"{self.pool_name}\"}} {self.connections_created_total}"
        )

    def on_connection_closed(self, conn: PooledConnection[Any], reason: CloseReason) -> None:
        self.connections_closed_total += 1
        reason_label = reason.value  # "expired", "max_uses", "error", "evicted", "shutdown"
        self.connections_closed_by_reason[reason_label] += 1
        # Example: connpool_connections_closed_total{pool="default",reason="expired"} 12
        log.debug(
            f"connpool_connections_closed_total{{pool=\"{self.pool_name}\",reason=\"{reason_label}\"}} "
            f"{self.connections_closed_by_reason[reason_label]}"
        )

    def on_create_failed(self, error: BaseException) -> None:
        self.create_failures_total += 1
        # Example: connpool_create_failures_total{pool="default"} 3
        log.debug(f"connpool_create_failures_total{{pool=\"{self.pool_name}\"}} {self.create_failures_total}")

    def on_acquired(self, conn: PooledConnection[Any], wait_time: float) -> None:
        self.acquires_total += 1

    def on_released(self, conn: PooledConnection[Any], hold_time: float) -> None:
        self.releases_total += 1

    def on_acquire_timeout(self, waited: float) -> None:
        self.acquire_timeouts_total += 1
        log.debug(f"connpool_acquire_timeouts_total{{pool=\"{self.pool_name}\"}} {self.acquire_timeouts_total}")

    def on_connection_leak(self, conn: PooledConnection[Any], held_for: float) -> None:
        self.leaks_detected_total += 1
        log.warning(
            f"connpool_leaks_detected_total{{pool=\"{self.pool_name}\"}} {self.leaks_detected_total} "
            f"(connection held for {held_for:.1f}s)"
        )

    def on_breaker_state_change(self, old: BreakerState, new: BreakerState) -> None:
        self.breaker_state_changes_total += 1
        self.breaker_state_current = new
        log.info(
            f"connpool_breaker_state_changes_total{{pool=\"{self.pool_name}\"}} {self.breaker_state_changes_total} "
            f"(state: {old.value} -> {new.value})"
        )

    def export_gauges(self, stats: PoolStats) -> list[str]:
        """Convert PoolStats snapshot to Prometheus gauge format.

        Call this periodically (e.g., every 15s) and push to your metrics backend.
        """
        lines = [
            # Pool sizes
            f"connpool_size{{pool=\"{self.pool_name}\"}} {stats.size}",
            f"connpool_idle{{pool=\"{self.pool_name}\"}} {stats.idle}",
            f"connpool_in_use{{pool=\"{self.pool_name}\"}} {stats.in_use}",
            f"connpool_waiting{{pool=\"{self.pool_name}\"}} {stats.waiting}",
            f"connpool_max_size{{pool=\"{self.pool_name}\"}} {stats.max_size}",
            # Acquire-wait percentiles (seconds)
            f"connpool_acquire_wait_p50{{pool=\"{self.pool_name}\"}} {stats.p50_acquire_wait}",
            f"connpool_acquire_wait_p95{{pool=\"{self.pool_name}\"}} {stats.p95_acquire_wait}",
            f"connpool_acquire_wait_p99{{pool=\"{self.pool_name}\"}} {stats.p99_acquire_wait}",
            # Create-latency percentiles (seconds)
            f"connpool_create_latency_p50{{pool=\"{self.pool_name}\"}} {stats.p50_create_latency}",
            f"connpool_create_latency_p95{{pool=\"{self.pool_name}\"}} {stats.p95_create_latency}",
            f"connpool_create_latency_p99{{pool=\"{self.pool_name}\"}} {stats.p99_create_latency}",
            # Circuit breaker state (0=closed, 1=open, 2=half_open)
            f"connpool_breaker_state{{pool=\"{self.pool_name}\"}} {self._breaker_state_value(self.breaker_state_current)}",
        ]
        return lines

    @staticmethod
    def _breaker_state_value(state: BreakerState) -> int:
        """Map BreakerState to numeric value for gauge."""
        return {"closed": 0, "open": 1, "half_open": 2}[state.value]


# Example usage
if __name__ == "__main__":
    import sqlite3
    import sys
    from pathlib import Path

    # Add examples/database to path for dbapi_factory
    sys.path.insert(0, str(Path(__file__).parent.parent / "database"))

    from connpool import ConnectionPool, PoolConfig
    from dbapi_factory import DbApiFactory

    logging.basicConfig(level=logging.DEBUG, format="%(message)s")

    # Set up pool with metrics listener
    metrics = PrometheusMetrics(pool_name="demo_pool")
    factory = DbApiFactory(lambda: sqlite3.connect(":memory:", check_same_thread=False))
    config = PoolConfig(min_size=2, max_size=4, maintenance_interval=2)

    pool = ConnectionPool(factory, config, name="demo_pool", listeners=[metrics])

    with pool:
        # Do some work
        for i in range(10):
            with pool.connection() as conn:
                conn.execute("SELECT 1")
                time.sleep(0.1)

        # Export gauges periodically (in production, do this from a background thread)
        stats = pool.stats()
        print("\n=== Prometheus Metrics ===")
        for line in metrics.export_gauges(stats):
            print(line)

        print(f"\nconnpool_connections_created_total{{pool=\"demo_pool\"}} {metrics.connections_created_total}")
        print(f"connpool_acquires_total{{pool=\"demo_pool\"}} {metrics.acquires_total}")
        print(f"connpool_releases_total{{pool=\"demo_pool\"}} {metrics.releases_total}")
