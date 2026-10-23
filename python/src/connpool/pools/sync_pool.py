"""Thread-safe synchronous connection pool.

Design in one paragraph: a single Condition guards a tiny amount of state (idle deque,
checked-out map, a slot counter). *All* I/O - create, validate, reset, close - happens
outside that lock, so a slow backend never blocks unrelated callers. A "slot" is
reserved under the lock *before* creating, which is what makes ``max_size`` a hard cap.
The rules for when connections retire live in ``lifecycle``; this file is plumbing.
"""
from __future__ import annotations

import logging
import random
import threading
from collections import deque
from contextlib import contextmanager
from typing import Any, Callable, Deque, Dict, Generic, Iterator, Optional, Sequence, TypeVar

from connpool.policy import lifecycle
from connpool.core.timeouts import UNSET, Timeout, compute_deadline, remaining
from connpool.resilience.breaker import CircuitBreaker
from connpool.core.clock import Clock, SystemClock
from connpool.policy.config import PoolConfig
from connpool.core.connection import CloseReason, PooledConnection
from connpool.core.errors import (
    ConnectionCreateError, InvalidReleaseError, PoolClosedError, PoolExhaustedError, PoolTimeoutError,
)
from connpool.observability.events import ListenerGroup, PoolListener
from connpool.factories.sync_factory import CallableFactory, ConnectionFactory
from connpool.observability.stats import PoolStats, StatsCollector

T = TypeVar("T")
log = logging.getLogger("connpool")


class ConnectionPool(Generic[T]):
    """
        pool = ConnectionPool(MyFactory(), PoolConfig(max_size=20))
        with pool:                                  # optional: warm-up + background sweeper
            with pool.connection() as conn:         # acquire / release / error handling
                conn.do_something()
    """

    def __init__(
        self,
        factory: ConnectionFactory[T],
        config: Optional[PoolConfig] = None,
        *,
        listeners: Sequence[PoolListener] = (),
        clock: Optional[Clock] = None,
        rng: Optional[random.Random] = None,
        name: str = "pool",
    ) -> None:
        self.name = name
        self._factory = factory
        self._cfg = config or PoolConfig()
        self._clock = clock or SystemClock()
        self._rng = rng or random.Random()

        self._cond = threading.Condition()
        self._idle: Deque[PooledConnection[T]] = deque()
        self._checked_out: Dict[int, PooledConnection[T]] = {}  # id(raw) -> wrapper
        self._size = 0          # idle + checked out + being created/closed
        self._waiters = 0
        self._closed = False

        self._stats = StatsCollector()
        self._events = ListenerGroup([self._stats, *listeners])
        self._breaker: Optional[CircuitBreaker] = (
            CircuitBreaker(self._cfg.breaker, self._clock,
                           on_state_change=lambda o, n: self._events.dispatch("on_breaker_state_change", o, n))
            if self._cfg.breaker else None
        )
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @classmethod
    def from_callables(
        cls, create: Callable[[], T], *, close: Optional[Callable[[T], None]] = None,
        validate: Optional[Callable[[T], bool]] = None, reset: Optional[Callable[[T], None]] = None,
        config: Optional[PoolConfig] = None, **kwargs: Any,
    ) -> "ConnectionPool[T]":
        """Pool anything from plain functions."""
        return cls(CallableFactory(create, close=close, validate=validate, reset=reset), config, **kwargs)

    # ------------------------------------------------------------------ lifecycle
    def open(self) -> "ConnectionPool[T]":
        """Warm up ``min_size`` connections and start the background maintenance thread.
        Optional: without it the pool is purely lazy and does no background work."""
        with self._cond:
            if self._closed:
                raise PoolClosedError(f"pool {self.name!r} is closed")
            if self._thread is not None:
                return self
            if self._cfg.maintenance_interval is not None:
                self._thread = threading.Thread(
                    target=self._maintenance_loop, name=f"connpool-{self.name}", daemon=True)
        self._fill_to_min()
        if self._thread is not None:
            self._thread.start()
        return self

    def close(self, timeout: Optional[float] = None) -> None:
        """Close idle connections now; checked-out ones are closed when released.
        Blocked ``acquire()`` callers raise ``PoolClosedError``. Idempotent.

        ``timeout`` (seconds) drains gracefully: wait up to this long for in-use
        connections to be returned before giving up (they still close on release).
        ``None`` (default) does not wait; ``0`` returns immediately. Draining waits on
        checked-out connections only; a connection still being created is destroyed as
        soon as it is built, which may be just after this call returns."""
        with self._cond:
            if self._closed:
                return
            self._closed = True
            idle = list(self._idle)
            self._idle.clear()
            self._cond.notify_all()
            if timeout is not None:
                deadline = self._clock.monotonic() + timeout
                while self._checked_out:
                    left = deadline - self._clock.monotonic()
                    if left <= 0:
                        break
                    self._cond.wait(left)
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread() \
                and self._thread.is_alive():
            self._thread.join(timeout=5)
        for pooled in idle:
            self._destroy(pooled, CloseReason.POOL_CLOSED)

    def __enter__(self) -> "ConnectionPool[T]":
        return self.open()

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    # --------------------------------------------------------------------- usage
    @contextmanager
    def connection(self, timeout: Timeout = UNSET) -> Iterator[T]:
        """Borrow a connection for the duration of the block.

        If the block raises, the connection is discarded when the factory says the
        error is fatal or when it fails validation; otherwise it is reused."""
        conn = self.acquire(timeout)
        try:
            yield conn
        except BaseException as exc:
            self.release(conn, error=exc)
            raise
        else:
            self.release(conn)

    def acquire(self, timeout: Timeout = UNSET) -> T:
        """Get a connection. ``timeout=None`` waits forever; default is
        ``config.acquire_timeout``. Raises ``PoolTimeoutError``, ``PoolClosedError``,
        ``ConnectionCreateError`` or ``CircuitOpenError``."""
        started = self._clock.monotonic()
        deadline = compute_deadline(timeout, self._cfg.acquire_timeout, started)
        try:
            while True:
                pooled = self._take_idle_or_reserve(deadline)
                if pooled is None:                       # slot reserved -> build a new one
                    pooled, created_in = self._create(deadline)
                    self._register_new(pooled, created_in)
                    break
                reason = self._borrow_reason(pooled)
                if reason is None:
                    break
                self._destroy(pooled, reason)            # stale/dead: drop it and try again
        except PoolTimeoutError:
            self._events.dispatch("on_acquire_timeout", self._clock.monotonic() - started)
            raise
        now = self._clock.monotonic()
        pooled.mark_checked_out(now)
        self._events.dispatch("on_acquired", pooled, now - started)
        return pooled.raw

    def release(self, conn: T, *, discard: bool = False, error: Optional[BaseException] = None) -> None:
        """Return a connection. ``discard=True`` forces it to be closed instead of reused."""
        with self._cond:
            pooled = self._checked_out.pop(id(conn), None)
        if pooled is None:
            raise InvalidReleaseError("object is not checked out from this pool (double release?)")

        now = self._clock.monotonic()
        hold = pooled.hold_time(now)
        reason = self._release_reason(pooled, conn, discard, error, now)
        if reason is None and not self._park(pooled, now):
            reason = CloseReason.POOL_CLOSED
        if reason is not None:
            self._destroy(pooled, reason)
        self._events.dispatch("on_released", pooled, hold)

    def stats(self) -> PoolStats:
        with self._cond:
            size, idle, in_use, waiting = self._size, len(self._idle), len(self._checked_out), self._waiters
        return self._stats.snapshot(
            name=self.name, max_size=self._cfg.max_size, size=size, idle=idle, in_use=in_use,
            waiting=waiting, breaker_state=self._breaker.state.value if self._breaker else None)

    def run_maintenance(self) -> None:
        """One sweep: evict expired/over-idle connections, then refill to ``min_size``.
        Runs periodically in the background after ``open()``; public for tests and cron-style use."""
        with self._cond:
            if self._closed:
                return
            keep, doomed = lifecycle.sweep(self._idle, self._size, self._cfg, self._clock.monotonic())
            self._idle = deque(keep)
        for pooled, reason in doomed:
            self._destroy(pooled, reason)
        self._health_check_idle()
        self._fill_to_min()
        self._check_leaks()

    def _health_check_idle(self) -> None:
        """Validate idle connections and evict dead ones. Opt-in via ``validate_idle``;
        driven by the maintenance sweep. Each idle connection is briefly reserved (so no
        caller can take it mid-check), validated outside the lock, then returned if healthy
        or destroyed if not. Returning preserves the idle timer (no ``mark_returned``)."""
        if not self._cfg.validate_idle:
            return
        with self._cond:
            batch = list(self._idle)
            self._idle.clear()
            for pooled in batch:
                self._checked_out[id(pooled.raw)] = pooled
        for pooled in batch:
            healthy = self._is_healthy(pooled.raw)
            with self._cond:
                self._checked_out.pop(id(pooled.raw), None)
                keep = healthy and not self._closed
                if keep:
                    self._idle.append(pooled)
                    self._cond.notify()
            if not keep:
                self._destroy(pooled, CloseReason.UNHEALTHY if not healthy else CloseReason.POOL_CLOSED)

    def _check_leaks(self) -> None:
        """Warn about connections borrowed longer than ``leak_detection_timeout``.
        Warn-only: the pool never reclaims a connection the caller may still be using."""
        timeout = self._cfg.leak_detection_timeout
        if timeout is None:
            return
        now = self._clock.monotonic()
        leaked = []
        with self._cond:
            for pooled in self._checked_out.values():
                if not pooled.leak_reported and pooled.hold_time(now) >= timeout:
                    pooled.leak_reported = True
                    leaked.append((pooled, pooled.hold_time(now)))
        for pooled, held in leaked:
            log.warning("pool %r: possible connection leak: %r held for %.1fs", self.name, pooled, held)
            self._events.dispatch("on_connection_leak", pooled, held)

    # ----------------------------------------------------------------- internals
    def _take_idle_or_reserve(self, deadline: Optional[float]) -> Optional[PooledConnection[T]]:
        """Under the lock: return an idle connection, or reserve a slot (return None), or wait."""
        with self._cond:
            while True:
                if self._closed:
                    raise PoolClosedError(f"pool {self.name!r} is closed")
                pooled = lifecycle.pop_idle(self._idle, self._cfg.idle_strategy)
                if pooled is not None:
                    self._checked_out[id(pooled.raw)] = pooled
                    return pooled
                if self._size < self._cfg.max_size:
                    self._size += 1
                    return None
                left = remaining(deadline, self._clock.monotonic())
                if left is not None and left <= 0:
                    raise PoolTimeoutError(
                        f"pool {self.name!r}: no connection available (max_size={self._cfg.max_size})")
                if self._cfg.max_waiters is not None and self._waiters >= self._cfg.max_waiters:
                    raise PoolExhaustedError(
                        f"pool {self.name!r}: waiter queue full (max_waiters={self._cfg.max_waiters})")
                self._waiters += 1
                try:
                    self._cond.wait(left)
                finally:
                    self._waiters -= 1

    def _borrow_reason(self, pooled: PooledConnection[T]) -> Optional[CloseReason]:
        now = self._clock.monotonic()
        reason = lifecycle.borrow_reason(pooled, self._cfg, now)
        if reason is None and lifecycle.needs_validation(pooled, self._cfg, now):
            if not self._is_healthy(pooled.raw):
                return CloseReason.UNHEALTHY
        return reason

    def _release_reason(
        self, pooled: PooledConnection[T], conn: T, discard: bool,
        error: Optional[BaseException], now: float,
    ) -> Optional[CloseReason]:
        if discard:
            return CloseReason.DISCARDED
        if error is not None and (self._factory.is_fatal_error(error) or not self._is_healthy(conn)):
            return CloseReason.ERROR
        reason = lifecycle.rotation_reason(pooled, self._cfg, now)
        if reason is not None:
            return reason
        try:
            self._factory.reset(conn)
        except Exception:  # noqa: BLE001
            log.warning("pool %r: reset failed; discarding %r", self.name, pooled, exc_info=True)
            return CloseReason.RESET_FAILED
        return None

    def _is_healthy(self, raw: T) -> bool:
        try:
            return bool(self._factory.validate(raw))
        except Exception:  # noqa: BLE001
            log.debug("pool %r: validate raised", self.name, exc_info=True)
            return False

    def _park(self, pooled: PooledConnection[T], now: float) -> bool:
        """Put a healthy connection into the idle queue. False if the pool is closed."""
        with self._cond:
            if self._closed:
                return False
            pooled.mark_returned(now)
            self._idle.append(pooled)
            self._cond.notify()
            return True

    def _create(self, deadline: Optional[float]) -> tuple[PooledConnection[T], float]:
        """Build a connection in an already-reserved slot; frees the slot on failure.
        Returns the connection and how long the factory took (the caller fires
        ``on_connection_created`` once the connection is actually adopted)."""
        started = self._clock.monotonic()
        try:
            raw = self._create_raw(deadline)
        except BaseException:
            self._free_slot()
            raise
        now = self._clock.monotonic()
        pooled = lifecycle.new_connection(raw, self._cfg, now, self._rng)
        return pooled, now - started

    def _register_new(self, pooled: PooledConnection[T], created_in: float) -> None:
        """Mark a freshly created connection as checked out, or clean up and raise.

        Guards two edge cases: the pool was closed while we were creating, and the
        factory returned an object the pool already manages (which would corrupt the
        checked-out map and leak a slot)."""
        with self._cond:
            if self._closed:
                action = "closed"
            elif id(pooled.raw) in self._checked_out or any(p.raw is pooled.raw for p in self._idle):
                action = "duplicate"                     # already tracked (idle or checked out)
            else:
                self._checked_out[id(pooled.raw)] = pooled
                action = "ok"
        if action == "duplicate":
            # The same object we already manage: not a new connection, so it is not
            # counted as created, and we never close it (its real owner still holds it).
            self._free_slot()
            raise ConnectionCreateError(
                f"pool {self.name!r}: factory.create() returned an object the pool already manages; "
                "each create() must return a distinct connection")
        self._events.dispatch("on_connection_created", pooled, created_in)
        if action == "closed":                           # built but unusable: created + closed
            self._destroy(pooled, CloseReason.POOL_CLOSED)
            raise PoolClosedError(f"pool {self.name!r} is closed")

    def _create_raw(self, deadline: Optional[float]) -> T:
        """factory.create() wrapped in circuit breaker + retry/backoff.

        The breaker sees one logical creation per call: fail-fast when open, and a single
        success/failure recorded for the whole retry sequence - individual retry attempts
        do not each count against ``failure_threshold``."""
        if self._breaker:
            self._breaker.before_call()                  # CircuitOpenError: fail fast, no retry
        try:
            raw = self._create_with_retry(deadline)
        except ConnectionCreateError:                    # the backend actually failed
            if self._breaker:
                self._breaker.record_failure()
            raise
        except BaseException:                            # timeout / closed / cancelled: not a
            if self._breaker:                            # backend fault, so don't trip the breaker
                self._breaker.record_abort()
            raise
        if self._breaker:
            self._breaker.record_success()
        return raw

    def _create_with_retry(self, deadline: Optional[float]) -> T:
        retry, attempt = self._cfg.retry, 0
        while True:
            if self._closed:
                raise PoolClosedError(f"pool {self.name!r} is closed")
            attempt += 1
            try:
                raw = self._factory.create()
            except Exception as exc:
                self._events.dispatch("on_create_failed", exc)
                if attempt >= retry.max_attempts or not retry.should_retry(exc):
                    raise ConnectionCreateError(
                        f"pool {self.name!r}: could not create connection after {attempt} attempt(s): {exc}"
                    ) from exc
                delay = retry.delay_for(attempt, self._rng)
                left = remaining(deadline, self._clock.monotonic())
                if left is not None and delay >= left:
                    raise PoolTimeoutError(
                        f"pool {self.name!r}: acquire deadline reached while retrying creation") from exc
                self._clock.sleep(delay)
                continue
            return raw

    def _free_slot(self) -> None:
        with self._cond:
            self._size -= 1
            self._cond.notify()

    def _destroy(self, pooled: PooledConnection[T], reason: CloseReason) -> None:
        try:
            self._factory.close(pooled.raw)
        except Exception:  # noqa: BLE001
            log.warning("pool %r: error closing %r", self.name, pooled, exc_info=True)
        finally:
            with self._cond:
                self._checked_out.pop(id(pooled.raw), None)
                self._size -= 1
                self._cond.notify()
        self._events.dispatch("on_connection_closed", pooled, reason)

    def _fill_to_min(self) -> None:
        """Best-effort warm-up; stops quietly on the first failure."""
        while True:
            with self._cond:
                if self._closed or self._size >= self._cfg.min_size:
                    return
                self._size += 1
            try:
                pooled, created_in = self._create(deadline=None)
            except Exception as exc:  # noqa: BLE001
                log.warning("pool %r: warm-up failed: %s", self.name, exc)
                return
            self._events.dispatch("on_connection_created", pooled, created_in)
            if not self._park(pooled, self._clock.monotonic()):
                self._destroy(pooled, CloseReason.POOL_CLOSED)

    def _maintenance_loop(self) -> None:
        interval = self._cfg.maintenance_interval
        while not self._stop.wait(interval):
            try:
                self.run_maintenance()
            except Exception:  # noqa: BLE001
                log.exception("pool %r: maintenance pass failed", self.name)

    def __repr__(self) -> str:
        s = self.stats()
        return f"<ConnectionPool {self.name!r} size={s.size}/{s.max_size} idle={s.idle} in_use={s.in_use}>"

    def __del__(self) -> None:
        # A pool dropped without close() leaks its connections. Nudge the user like the
        # stdlib does for unclosed files/sockets. Guarded: __init__ may not have finished.
        if not getattr(self, "_closed", True) and getattr(self, "_size", 0) > 0:
            import warnings
            warnings.warn(f"pool {self.name!r} was not closed; {self._size} connection(s) may leak",
                          ResourceWarning, stacklevel=2, source=self)
