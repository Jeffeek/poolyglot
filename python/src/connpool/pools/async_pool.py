"""asyncio connection pool. Same model and same rules (``lifecycle``) as the sync pool.

Differences worth knowing:
  * ``create_timeout`` is enforced here (``asyncio.timeout``); sync factories enforce their own.
  * Listener hooks stay synchronous (keep them fast).
  * Requires Python 3.11+ (``asyncio.timeout``).
"""
from __future__ import annotations

import asyncio
import logging
import random
from collections import deque
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Deque, Dict, Generic, Optional, Sequence, TypeVar

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
from connpool.factories.async_factory import AsyncCallableFactory, AsyncConnectionFactory
from connpool.observability.stats import PoolStats, StatsCollector

T = TypeVar("T")
log = logging.getLogger("connpool")


class AsyncConnectionPool(Generic[T]):
    """
        pool = AsyncConnectionPool(MyAsyncFactory(), PoolConfig(max_size=20))
        async with pool:
            async with pool.connection() as conn:
                await conn.do_something()
    """

    def __init__(
        self,
        factory: AsyncConnectionFactory[T],
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

        self._cond = asyncio.Condition()
        self._idle: Deque[PooledConnection[T]] = deque()
        self._checked_out: Dict[int, PooledConnection[T]] = {}
        self._size = 0
        self._waiters = 0
        self._closed = False

        self._stats = StatsCollector()
        self._events = ListenerGroup([self._stats, *listeners])
        self._breaker: Optional[CircuitBreaker] = (
            CircuitBreaker(self._cfg.breaker, self._clock,
                           on_state_change=lambda o, n: self._events.dispatch("on_breaker_state_change", o, n))
            if self._cfg.breaker else None
        )
        self._task: Optional["asyncio.Task[None]"] = None
        self._opened = False

    @classmethod
    def from_callables(
        cls, create: Callable[[], Any], *, close: Optional[Callable[[T], Any]] = None,
        validate: Optional[Callable[[T], Any]] = None, reset: Optional[Callable[[T], Any]] = None,
        config: Optional[PoolConfig] = None, **kwargs: Any,
    ) -> "AsyncConnectionPool[T]":
        return cls(AsyncCallableFactory(create, close=close, validate=validate, reset=reset), config, **kwargs)

    # ------------------------------------------------------------------ lifecycle
    async def open(self) -> "AsyncConnectionPool[T]":
        if self._closed:
            raise PoolClosedError(f"pool {self.name!r} is closed")
        if self._opened:
            return self
        self._opened = True
        await self._fill_to_min()
        # _fill_to_min awaits, so close() may have run meanwhile. Re-check under the
        # single-threaded loop (no await between here and create_task): otherwise the
        # task is started after close() already saw _task is None and would never be
        # cancelled - a maintenance loop leaked past shutdown.
        if self._cfg.maintenance_interval is not None and not self._closed:
            self._task = asyncio.create_task(self._maintenance_loop(), name=f"connpool-{self.name}")
        return self

    async def close(self, timeout: Optional[float] = None) -> None:
        """Close idle connections now; checked-out ones close on release. Idempotent.
        ``timeout`` (seconds) drains gracefully: wait up to this long for in-use
        connections to return. ``None`` (default) does not wait; ``0`` returns at once.
        Draining waits on checked-out connections only; one still being created is
        destroyed as soon as it is built, possibly just after this call returns."""
        async with self._cond:
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
                    try:
                        async with asyncio.timeout(left):
                            await self._cond.wait()
                    except TimeoutError:
                        break
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        for pooled in idle:
            await self._destroy(pooled, CloseReason.POOL_CLOSED)

    async def __aenter__(self) -> "AsyncConnectionPool[T]":
        return await self.open()

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    # --------------------------------------------------------------------- usage
    @asynccontextmanager
    async def connection(self, timeout: Timeout = UNSET) -> AsyncIterator[T]:
        conn = await self.acquire(timeout)
        try:
            yield conn
        except BaseException as exc:
            await self.release(conn, error=exc)
            raise
        else:
            await self.release(conn)

    async def acquire(self, timeout: Timeout = UNSET) -> T:
        started = self._clock.monotonic()
        deadline = compute_deadline(timeout, self._cfg.acquire_timeout, started)
        try:
            while True:
                pooled = await self._take_idle_or_reserve(deadline)
                if pooled is None:
                    pooled, created_in = await self._create(deadline)
                    try:
                        await self._register_new(pooled, created_in)
                    except (ConnectionCreateError, PoolClosedError):
                        raise                # _register_new already cleaned up its own cases
                    except BaseException:    # cancelled while registering: don't leak the new conn
                        await self._destroy(pooled, CloseReason.ERROR)
                        raise
                    break
                try:
                    reason = await self._borrow_reason(pooled)
                except BaseException:        # e.g. cancelled mid-validation: don't leak it
                    await self._destroy(pooled, CloseReason.ERROR)
                    raise
                if reason is None:
                    break
                await self._destroy(pooled, reason)
        except PoolTimeoutError:
            self._events.dispatch("on_acquire_timeout", self._clock.monotonic() - started)
            raise
        now = self._clock.monotonic()
        pooled.mark_checked_out(now)
        self._events.dispatch("on_acquired", pooled, now - started)
        return pooled.raw

    async def release(self, conn: T, *, discard: bool = False, error: Optional[BaseException] = None) -> None:
        async with self._cond:
            pooled = self._checked_out.pop(id(conn), None)
        if pooled is None:
            raise InvalidReleaseError("object is not checked out from this pool (double release?)")

        now = self._clock.monotonic()
        hold = pooled.hold_time(now)
        try:
            reason = await self._release_reason(pooled, conn, discard, error, now)
            if reason is None and not await self._park(pooled, now):
                reason = CloseReason.POOL_CLOSED
        except BaseException:                # cancelled during reset/validate: close, don't leak
            await self._destroy(pooled, CloseReason.ERROR)
            raise
        if reason is not None:
            await self._destroy(pooled, reason)
        self._events.dispatch("on_released", pooled, hold)

    def stats(self) -> PoolStats:
        # Single-threaded event loop: reading state without the lock is consistent.
        return self._stats.snapshot(
            name=self.name, max_size=self._cfg.max_size, size=self._size, idle=len(self._idle),
            in_use=len(self._checked_out), waiting=self._waiters,
            breaker_state=self._breaker.state.value if self._breaker else None)

    async def run_maintenance(self) -> None:
        async with self._cond:
            if self._closed:
                return
            keep, doomed = lifecycle.sweep(self._idle, self._size, self._cfg, self._clock.monotonic())
            self._idle = deque(keep)
        for pooled, reason in doomed:
            await self._destroy(pooled, reason)
        await self._health_check_idle()
        await self._fill_to_min()
        self._check_leaks()

    async def _health_check_idle(self) -> None:
        """Validate idle connections and evict dead ones. Opt-in via ``validate_idle``;
        driven by the maintenance sweep. Each idle connection is briefly reserved (so no
        caller can take it mid-check), validated outside the lock, then returned if healthy
        or destroyed if not. Returning preserves the idle timer (no ``mark_returned``)."""
        if not self._cfg.validate_idle:
            return
        async with self._cond:
            batch = list(self._idle)
            self._idle.clear()
            for pooled in batch:
                self._checked_out[id(pooled.raw)] = pooled
        for pooled in batch:
            healthy = await self._is_healthy(pooled.raw)
            async with self._cond:
                self._checked_out.pop(id(pooled.raw), None)
                keep = healthy and not self._closed
                if keep:
                    self._idle.append(pooled)
                    self._cond.notify()
            if not keep:
                await self._destroy(pooled, CloseReason.UNHEALTHY if not healthy else CloseReason.POOL_CLOSED)

    def _check_leaks(self) -> None:
        """Warn about connections borrowed longer than ``leak_detection_timeout`` (warn-only).
        Safe to read ``_checked_out`` without the lock: the event loop is single-threaded."""
        timeout = self._cfg.leak_detection_timeout
        if timeout is None:
            return
        now = self._clock.monotonic()
        for pooled in list(self._checked_out.values()):
            if not pooled.leak_reported and pooled.hold_time(now) >= timeout:
                pooled.leak_reported = True
                held = pooled.hold_time(now)
                log.warning("pool %r: possible connection leak: %r held for %.1fs", self.name, pooled, held)
                self._events.dispatch("on_connection_leak", pooled, held)

    # ----------------------------------------------------------------- internals
    async def _take_idle_or_reserve(self, deadline: Optional[float]) -> Optional[PooledConnection[T]]:
        async with self._cond:
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
                    async with asyncio.timeout(left):
                        await self._cond.wait()
                except TimeoutError:
                    pass                      # loop re-checks state, then raises PoolTimeoutError
                finally:
                    self._waiters -= 1

    async def _borrow_reason(self, pooled: PooledConnection[T]) -> Optional[CloseReason]:
        now = self._clock.monotonic()
        reason = lifecycle.borrow_reason(pooled, self._cfg, now)
        if reason is None and lifecycle.needs_validation(pooled, self._cfg, now):
            if not await self._is_healthy(pooled.raw):
                return CloseReason.UNHEALTHY
        return reason

    async def _release_reason(
        self, pooled: PooledConnection[T], conn: T, discard: bool,
        error: Optional[BaseException], now: float,
    ) -> Optional[CloseReason]:
        if discard:
            return CloseReason.DISCARDED
        if error is not None and (self._factory.is_fatal_error(error) or not await self._is_healthy(conn)):
            return CloseReason.ERROR
        reason = lifecycle.rotation_reason(pooled, self._cfg, now)
        if reason is not None:
            return reason
        try:
            await self._factory.reset(conn)
        except Exception:  # noqa: BLE001
            log.warning("pool %r: reset failed; discarding %r", self.name, pooled, exc_info=True)
            return CloseReason.RESET_FAILED
        return None

    async def _is_healthy(self, raw: T) -> bool:
        try:
            return bool(await self._factory.validate(raw))
        except Exception:  # noqa: BLE001
            log.debug("pool %r: validate raised", self.name, exc_info=True)
            return False

    async def _park(self, pooled: PooledConnection[T], now: float) -> bool:
        async with self._cond:
            if self._closed:
                return False
            pooled.mark_returned(now)
            self._idle.append(pooled)
            self._cond.notify()
            return True

    async def _create(self, deadline: Optional[float]) -> tuple[PooledConnection[T], float]:
        """Returns the connection and how long the factory took; the caller fires
        ``on_connection_created`` once the connection is actually adopted."""
        started = self._clock.monotonic()
        try:
            raw = await self._create_raw(deadline)
        except BaseException:
            await self._free_slot()
            raise
        now = self._clock.monotonic()
        pooled = lifecycle.new_connection(raw, self._cfg, now, self._rng)
        return pooled, now - started

    def _create_limit(self, deadline: Optional[float]) -> Optional[float]:
        limits = [x for x in (self._cfg.create_timeout, remaining(deadline, self._clock.monotonic()))
                  if x is not None]
        return max(0.0, min(limits)) if limits else None

    async def _register_new(self, pooled: PooledConnection[T], created_in: float) -> None:
        """Async twin of the sync ``_register_new``: guards close-during-create and a
        factory that returns an object the pool already manages."""
        async with self._cond:
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
            await self._free_slot()
            raise ConnectionCreateError(
                f"pool {self.name!r}: factory.create() returned an object the pool already manages; "
                "each create() must return a distinct connection")
        self._events.dispatch("on_connection_created", pooled, created_in)
        if action == "closed":                           # built but unusable: created + closed
            await self._destroy(pooled, CloseReason.POOL_CLOSED)
            raise PoolClosedError(f"pool {self.name!r} is closed")

    async def _create_raw(self, deadline: Optional[float]) -> T:
        """The breaker sees one logical creation per call: fail-fast when open, a single
        success/failure for the whole retry sequence (see the sync pool for rationale)."""
        if self._breaker:
            self._breaker.before_call()
        try:
            raw = await self._create_with_retry(deadline)
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

    async def _create_with_retry(self, deadline: Optional[float]) -> T:
        retry, attempt = self._cfg.retry, 0
        while True:
            if self._closed:
                raise PoolClosedError(f"pool {self.name!r} is closed")
            attempt += 1
            try:
                async with asyncio.timeout(self._create_limit(deadline)):
                    raw = await self._factory.create()
            except Exception as exc:
                # A timeout whose binding limit was the acquire deadline is caller
                # impatience, not a backend fault: surface it as PoolTimeoutError so the
                # breaker records an abort, not a failure. A create_timeout firing *is* a
                # backend fault and flows through the normal create-failure path below.
                left = remaining(deadline, self._clock.monotonic())
                if isinstance(exc, TimeoutError) and left is not None and left <= 0:
                    raise PoolTimeoutError(
                        f"pool {self.name!r}: acquire deadline reached while creating connection") from exc
                self._events.dispatch("on_create_failed", exc)
                if attempt >= retry.max_attempts or not retry.should_retry(exc):
                    raise ConnectionCreateError(
                        f"pool {self.name!r}: could not create connection after {attempt} attempt(s): "
                        f"{exc or type(exc).__name__}") from exc
                delay = retry.delay_for(attempt, self._rng)
                if left is not None and delay >= left:
                    raise PoolTimeoutError(
                        f"pool {self.name!r}: acquire deadline reached while retrying creation") from exc
                await self._clock.async_sleep(delay)
                continue
            return raw

    async def _free_slot(self) -> None:
        async with self._cond:
            self._size -= 1
            self._cond.notify()

    async def _destroy(self, pooled: PooledConnection[T], reason: CloseReason) -> None:
        """Dispose of a connection exactly once. Shielded, so a cancellation that lands while
        ``factory.close()`` is awaiting I/O (or while the accounting lock is contended) can
        neither interrupt backend disposal, skip the slot decrement, nor drop the
        ``on_connection_closed`` event; the original exception still propagates to the caller
        unchanged. Every disposal path (acquire, release, maintenance, shutdown) goes through
        here, so they all share this guarantee - there is no interruptible disposal to leak a
        half-closed resource or unbalance the ``created``/``closed`` accounting."""
        await asyncio.shield(self._dispose(pooled, reason))

    async def _dispose(self, pooled: PooledConnection[T], reason: CloseReason) -> None:
        try:
            await self._factory.close(pooled.raw)
        except Exception:  # noqa: BLE001
            log.warning("pool %r: error closing %r", self.name, pooled, exc_info=True)
        finally:
            async with self._cond:
                self._checked_out.pop(id(pooled.raw), None)
                self._size -= 1
                self._cond.notify()
        self._events.dispatch("on_connection_closed", pooled, reason)

    async def _fill_to_min(self) -> None:
        while True:
            async with self._cond:
                if self._closed or self._size >= self._cfg.min_size:
                    return
                self._size += 1
            try:
                pooled, created_in = await self._create(deadline=None)
            except Exception as exc:  # noqa: BLE001
                log.warning("pool %r: warm-up failed: %s", self.name, exc)
                return
            self._events.dispatch("on_connection_created", pooled, created_in)
            if not await self._park(pooled, self._clock.monotonic()):
                await self._destroy(pooled, CloseReason.POOL_CLOSED)

    async def _maintenance_loop(self) -> None:
        interval = self._cfg.maintenance_interval
        assert interval is not None             # the task is only started when it is set
        while True:
            await asyncio.sleep(interval)
            try:
                await self.run_maintenance()
            except Exception:  # noqa: BLE001
                log.exception("pool %r: maintenance pass failed", self.name)

    def __repr__(self) -> str:
        s = self.stats()
        return f"<AsyncConnectionPool {self.name!r} size={s.size}/{s.max_size} idle={s.idle} in_use={s.in_use}>"

    def __del__(self) -> None:
        # Unlike the sync pool we cannot await close() here; just warn (stdlib style).
        if not getattr(self, "_closed", True) and getattr(self, "_size", 0) > 0:
            import warnings
            warnings.warn(f"pool {self.name!r} was not closed; {self._size} connection(s) may leak",
                          ResourceWarning, stacklevel=2, source=self)
