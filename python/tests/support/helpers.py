"""Shared test doubles."""
import itertools
import threading

from connpool import (
    AsyncConnectionFactory, ConnectionFactory, NO_RETRY, PoolConfig, PoolListener,
)

_ids = itertools.count(1)


class FakeConn:
    def __init__(self):
        self.id = next(_ids)
        self.closed = False
        self.healthy = True
        self.resets = 0

    def __repr__(self):
        return f"<FakeConn {self.id}>"


class FakeFactory(ConnectionFactory[FakeConn]):
    def __init__(self):
        self.created = []
        self.attempts = 0
        self.fail_times = 0
        self.fail_always = False
        self.fatal_types = ()
        self.reset_raises = False
        self._lock = threading.Lock()

    def create(self):
        with self._lock:
            self.attempts += 1
            if self.fail_always or self.fail_times > 0:
                self.fail_times = max(0, self.fail_times - 1)
                raise ConnectionRefusedError("backend down")
            conn = FakeConn()
            self.created.append(conn)
            return conn

    def validate(self, conn):
        return conn.healthy and not conn.closed

    def reset(self, conn):
        if self.reset_raises:
            raise RuntimeError("reset failed")
        conn.resets += 1

    def close(self, conn):
        conn.closed = True

    def is_fatal_error(self, exc):
        return isinstance(exc, self.fatal_types)


class AsyncFakeFactory(AsyncConnectionFactory[FakeConn]):
    """Async view over a FakeFactory (shares its counters and failure switches)."""

    def __init__(self, inner=None, create_delay=0.0):
        self.inner = inner or FakeFactory()
        self.create_delay = create_delay

    async def create(self):
        import asyncio
        if self.create_delay:
            await asyncio.sleep(self.create_delay)
        return self.inner.create()

    async def validate(self, conn):
        return self.inner.validate(conn)

    async def reset(self, conn):
        self.inner.reset(conn)

    async def close(self, conn):
        self.inner.close(conn)

    def is_fatal_error(self, exc):
        return self.inner.is_fatal_error(exc)


class Recorder(PoolListener):
    def __init__(self):
        self.closed = []     # (conn_id, reason)
        self.events = []

    def on_connection_created(self, conn, duration):
        self.events.append("created")

    def on_connection_closed(self, conn, reason):
        self.closed.append(reason)
        self.events.append(f"closed:{reason.value}")

    def on_acquired(self, conn, wait_time):
        self.events.append("acquired")

    def on_released(self, conn, hold_time):
        self.events.append("released")

    def on_breaker_state_change(self, old, new):
        self.events.append(f"breaker:{old.value}->{new.value}")


def quiet_config(**overrides) -> PoolConfig:
    """Deterministic baseline: no jitter, no background work, no retries, no breaker."""
    base = dict(
        max_lifetime=None, max_idle_time=None, validate_after_idle=None,
        maintenance_interval=None, acquire_timeout=0.3, lifetime_jitter=0.0,
        retry=NO_RETRY, breaker=None,
    )
    base.update(overrides)
    return PoolConfig(**base)
