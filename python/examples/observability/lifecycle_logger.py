"""Example: Comprehensive connection lifecycle logging via PoolListener.

This demonstrates using PoolListener to log every pool event for debugging and diagnostics.
Useful for understanding connection behavior, troubleshooting leaks, and analyzing performance.

Key patterns:
- PoolListener that logs all events with timestamps
- Correlate connection lifecycle: created -> acquired -> released -> closed
- Track connection age, hold time, wait time
- Log breaker state changes and failures

Use cases:
- Development/staging: verbose logging for debugging
- Production: conditional logging (only log slow operations, errors, leaks)
- Performance analysis: identify hot connections, wait patterns
"""
from __future__ import annotations

import logging
import time
from typing import Any

from connpool import BreakerState, CloseReason, PoolListener, PooledConnection

# Configure logging with timestamps
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

log = logging.getLogger("connpool.lifecycle")


class LifecycleLogger(PoolListener):
    """PoolListener that logs every event with detailed context.

    Tracks connection lifetimes and correlates events by connection ID.
    """

    def __init__(self, pool_name: str = "default"):
        self.pool_name = pool_name
        self.connection_birth_times: dict[int, float] = {}  # conn_id -> creation timestamp

    def on_connection_created(self, conn: PooledConnection[Any], duration: float) -> None:
        """Connection successfully created by the factory."""
        conn_id = conn.id
        self.connection_birth_times[conn_id] = time.time()
        log.info(
            f"[{self.pool_name}] Connection #{conn_id} CREATED in {duration:.3f}s "
            f"(lifetime={conn.age(time.time()):.1f}s, uses={conn.use_count})"
        )

    def on_connection_closed(self, conn: PooledConnection[Any], reason: CloseReason) -> None:
        """Connection closed and removed from the pool."""
        conn_id = conn.id
        birth_time = self.connection_birth_times.pop(conn_id, None)
        age = time.time() - birth_time if birth_time else 0.0

        log.info(
            f"[{self.pool_name}] Connection #{conn_id} CLOSED after {age:.1f}s "
            f"(reason={reason.value}, lifetime={conn.age(time.time()):.1f}s, uses={conn.use_count})"
        )

    def on_create_failed(self, error: BaseException) -> None:
        """Factory.create() raised an exception."""
        log.error(f"[{self.pool_name}] Connection CREATE FAILED: {type(error).__name__}: {error}")

    def on_acquired(self, conn: PooledConnection[Any], wait_time: float) -> None:
        """Connection acquired by a caller (released from idle or freshly created)."""
        conn_id = conn.id
        if wait_time > 0.1:  # Log slow acquires (>100ms wait)
            log.warning(
                f"[{self.pool_name}] Connection #{conn_id} ACQUIRED after {wait_time:.3f}s wait "
                f"(slow: pool may be undersized)"
            )
        else:
            log.debug(
                f"[{self.pool_name}] Connection #{conn_id} ACQUIRED (wait={wait_time:.3f}s, "
                f"age={conn.age(time.time()):.1f}s, uses={conn.use_count})"
            )

    def on_released(self, conn: PooledConnection[Any], hold_time: float) -> None:
        """Connection returned to the pool by a caller."""
        conn_id = conn.id
        if hold_time > 10.0:  # Log long-held connections (>10s)
            log.warning(
                f"[{self.pool_name}] Connection #{conn_id} RELEASED after {hold_time:.3f}s "
                f"(held long: possible leak or slow query)"
            )
        else:
            log.debug(
                f"[{self.pool_name}] Connection #{conn_id} RELEASED (held={hold_time:.3f}s, "
                f"age={conn.age(time.time()):.1f}s, uses={conn.use_count})"
            )

    def on_acquire_timeout(self, waited: float) -> None:
        """Caller timed out waiting for a connection."""
        log.error(
            f"[{self.pool_name}] ACQUIRE TIMEOUT after {waited:.3f}s "
            f"(pool exhausted or connections taking too long)"
        )

    def on_connection_leak(self, conn: PooledConnection[Any], held_for: float) -> None:
        """Connection held longer than leak_detection_timeout."""
        conn_id = conn.id
        log.error(
            f"[{self.pool_name}] CONNECTION LEAK DETECTED: #{conn_id} held for {held_for:.1f}s "
            f"(age={conn.age(time.time()):.1f}s, uses={conn.use_count}) - caller did not release!"
        )

    def on_breaker_state_change(self, old: BreakerState, new: BreakerState) -> None:
        """Circuit breaker changed state."""
        if new == BreakerState.OPEN:
            log.error(
                f"[{self.pool_name}] Circuit breaker OPENED "
                f"({old.value} -> {new.value}) - failing fast"
            )
        elif new == BreakerState.HALF_OPEN:
            log.warning(
                f"[{self.pool_name}] Circuit breaker HALF-OPEN "
                f"({old.value} -> {new.value}) - probing recovery"
            )
        elif new == BreakerState.CLOSED:
            log.info(
                f"[{self.pool_name}] Circuit breaker CLOSED "
                f"({old.value} -> {new.value}) - recovered"
            )


# Example usage
if __name__ == "__main__":
    import sqlite3
    import sys
    from concurrent.futures import ThreadPoolExecutor
    from pathlib import Path

    # Add examples/database to path for dbapi_factory
    sys.path.insert(0, str(Path(__file__).parent.parent / "database"))

    from connpool import ConnectionPool, PoolConfig
    from dbapi_factory import DbApiFactory

    print("=== Lifecycle Logging Demo ===\n")

    # Set up pool with lifecycle logger
    logger_listener = LifecycleLogger(pool_name="demo_pool")
    factory = DbApiFactory(lambda: sqlite3.connect(":memory:", check_same_thread=False))
    config = PoolConfig(
        min_size=2,  # Warm 2 connections
        max_size=4,  # Max 4 connections
        max_uses=5,  # Rotate after 5 uses
        maintenance_interval=2,  # Run maintenance every 2s
        leak_detection_timeout=3.0,  # Warn if held >3s
    )

    pool = ConnectionPool(factory, config, name="demo_pool", listeners=[logger_listener])

    with pool:
        log.info("Pool opened (warming min_size connections)")
        time.sleep(0.5)  # Let maintenance warm connections

        log.info("\n--- Scenario 1: Normal usage ---")
        with pool.connection() as conn:
            conn.execute("SELECT 1")
            time.sleep(0.2)  # Simulate query work

        log.info("\n--- Scenario 2: Concurrent requests ---")

        def do_query(i: int):
            with pool.connection() as conn:
                conn.execute("SELECT ?", (i,))
                time.sleep(0.1)

        with ThreadPoolExecutor(8) as executor:
            list(executor.map(do_query, range(10)))

        log.info("\n--- Scenario 3: Leak detection (simulated) ---")
        # Acquire without releasing (within context manager to avoid actual leak)
        conn = pool.acquire()
        log.info("Acquired connection, sleeping for 4s (will trigger leak warning)...")
        time.sleep(4)  # Longer than leak_detection_timeout (3s)
        pool.release(conn)

        log.info("\n--- Scenario 4: Connection rotation (max_uses=5) ---")
        # Use same connection repeatedly to trigger rotation
        for i in range(6):
            with pool.connection() as conn:
                conn.execute("SELECT 1")

        time.sleep(0.5)  # Let logs flush

    log.info("\n--- Pool closed ---")
    print("\n=== Demo complete ===")
    print("Review the logs above to see the full connection lifecycle.")
