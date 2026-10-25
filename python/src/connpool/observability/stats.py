"""Built-in metrics: a listener that counts events, plus an immutable snapshot."""
from __future__ import annotations

import threading
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Any

from connpool.core.connection import CloseReason, PooledConnection
from connpool.observability.events import PoolListener

ACQUIRE_WAIT_WINDOW = 1024
"""How many recent acquire-wait / create-latency samples to keep. ``avg`` and all percentiles
are computed over this same window (bounded memory), so they always describe the same
population."""


def percentile(sorted_values: list[float], q: float) -> float:
    """Linear-interpolated percentile of an already-sorted list. ``q`` in [0, 1]."""
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


@dataclass(frozen=True)
class PoolStats:
    name: str
    max_size: int
    size: int            # idle + in use + being created
    idle: int            # available for checkout right now
    in_use: int          # checked out: held by a caller, OR briefly reserved by a background
                         # idle health check (validate_idle) while it pings them off-lock
    waiting: int         # callers currently blocked in acquire()
    created: int
    create_failures: int
    acquired: int
    released: int
    timeouts: int
    closed: int
    leaks: int = 0       # borrowed connections flagged as possibly leaked
    closed_by_reason: dict[str, int] = field(default_factory=dict)
    # acquire-wait aggregates, all over the last ACQUIRE_WAIT_WINDOW samples
    avg_acquire_wait: float = 0.0
    p50_acquire_wait: float = 0.0
    p95_acquire_wait: float = 0.0
    p99_acquire_wait: float = 0.0
    # create-latency aggregates (factory.create() duration), same window
    avg_create_latency: float = 0.0
    p50_create_latency: float = 0.0
    p95_create_latency: float = 0.0
    p99_create_latency: float = 0.0
    breaker_state: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


class StatsCollector(PoolListener):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._n: Counter[str] = Counter()
        self._by_reason: Counter[str] = Counter()
        self._waits: deque[float] = deque(maxlen=ACQUIRE_WAIT_WINDOW)
        self._creates: deque[float] = deque(maxlen=ACQUIRE_WAIT_WINDOW)

    def _inc(self, key: str) -> None:
        with self._lock:
            self._n[key] += 1

    def on_connection_created(self, conn: PooledConnection[Any], duration: float) -> None:
        with self._lock:
            self._n["created"] += 1
            self._creates.append(duration)

    def on_create_failed(self, error: BaseException) -> None:
        self._inc("create_failures")

    def on_connection_closed(self, conn: PooledConnection[Any], reason: CloseReason) -> None:
        with self._lock:
            self._n["closed"] += 1
            self._by_reason[reason.value] += 1

    def on_acquired(self, conn: PooledConnection[Any], wait_time: float) -> None:
        with self._lock:
            self._n["acquired"] += 1
            self._waits.append(wait_time)

    def on_released(self, conn: PooledConnection[Any], hold_time: float) -> None:
        self._inc("released")

    def on_acquire_timeout(self, waited: float) -> None:
        self._inc("timeouts")

    def on_connection_leak(self, conn: PooledConnection[Any], held_for: float) -> None:
        self._inc("leaks")

    def snapshot(self, **gauges: Any) -> PoolStats:
        with self._lock:
            n = self._n
            waits = sorted(self._waits)
            creates = sorted(self._creates)
            return PoolStats(
                created=n["created"], create_failures=n["create_failures"], acquired=n["acquired"],
                released=n["released"], timeouts=n["timeouts"], closed=n["closed"],
                leaks=n["leaks"], closed_by_reason=dict(self._by_reason),
                avg_acquire_wait=(sum(waits) / len(waits)) if waits else 0.0,
                p50_acquire_wait=percentile(waits, 0.50),
                p95_acquire_wait=percentile(waits, 0.95),
                p99_acquire_wait=percentile(waits, 0.99),
                avg_create_latency=(sum(creates) / len(creates)) if creates else 0.0,
                p50_create_latency=percentile(creates, 0.50),
                p95_create_latency=percentile(creates, 0.95),
                p99_create_latency=percentile(creates, 0.99),
                **gauges,
            )
