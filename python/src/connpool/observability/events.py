"""Observability hooks. Subclass ``PoolListener`` and override what you care about
(metrics, tracing, logging). Hooks run synchronously on the calling thread/task, outside
the pool's lock, so keep them fast. Exceptions raised by listeners are logged and
swallowed - a broken metrics exporter must never break the pool."""
from __future__ import annotations

import logging
from typing import Any, Iterable, List

from connpool.resilience.breaker import BreakerState
from connpool.core.connection import CloseReason, PooledConnection

log = logging.getLogger("connpool")


class PoolListener:
    def on_connection_created(self, conn: PooledConnection[Any], duration: float) -> None: ...
    def on_connection_closed(self, conn: PooledConnection[Any], reason: CloseReason) -> None: ...
    def on_create_failed(self, error: BaseException) -> None: ...
    def on_acquired(self, conn: PooledConnection[Any], wait_time: float) -> None: ...
    def on_released(self, conn: PooledConnection[Any], hold_time: float) -> None: ...
    def on_acquire_timeout(self, waited: float) -> None: ...
    def on_connection_leak(self, conn: PooledConnection[Any], held_for: float) -> None: ...
    def on_breaker_state_change(self, old: BreakerState, new: BreakerState) -> None: ...


class ListenerGroup:
    def __init__(self, listeners: Iterable[PoolListener]) -> None:
        self._listeners: List[PoolListener] = list(listeners)

    def dispatch(self, hook: str, *args: object) -> None:
        for listener in self._listeners:
            try:
                getattr(listener, hook)(*args)
            except Exception:  # noqa: BLE001 - listeners must never break the pool
                log.exception("pool listener %r failed in %s", listener, hook)
