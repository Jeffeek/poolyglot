"""Circuit breaker (thread-safe; also safe to use from asyncio because the lock is
never held across an ``await``).

    CLOSED --(N consecutive failures)--> OPEN --(recovery_timeout)--> HALF_OPEN
       ^                                   ^                              |
       |                                   +------ any probe fails -------+
       +------------ enough probes succeed ---------------------------------+
"""
from __future__ import annotations

import enum
import threading
from dataclasses import dataclass
from typing import Callable, Optional, Tuple, TypeVar

from connpool.core.clock import Clock, SystemClock
from connpool.core.errors import CircuitOpenError, PoolConfigError

R = TypeVar("R")


class BreakerState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True)
class BreakerConfig:
    failure_threshold: int = 5      # consecutive failures that trip the breaker
    recovery_timeout: float = 30.0  # seconds to stay OPEN before probing
    half_open_max_calls: int = 1    # concurrent probe calls allowed while HALF_OPEN
    success_threshold: int = 1      # probe successes needed to close again

    def __post_init__(self) -> None:
        if self.failure_threshold < 1 or self.half_open_max_calls < 1 or self.success_threshold < 1:
            raise PoolConfigError("breaker thresholds must be >= 1")
        if self.recovery_timeout < 0:
            raise PoolConfigError("breaker.recovery_timeout must be >= 0")


_Transition = Optional[Tuple[BreakerState, BreakerState]]


class CircuitBreaker:
    """Usage: ``before_call()`` -> do work -> ``record_success()`` / ``record_failure()``."""

    def __init__(
        self,
        config: Optional[BreakerConfig] = None,
        clock: Optional[Clock] = None,
        on_state_change: Optional[Callable[[BreakerState, BreakerState], None]] = None,
    ) -> None:
        self._cfg = config or BreakerConfig()
        self._clock = clock or SystemClock()
        self._on_change = on_state_change
        self._lock = threading.Lock()
        self._state = BreakerState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._probes_inflight = 0
        self._probe_successes = 0

    # -- public API ---------------------------------------------------------
    @property
    def state(self) -> BreakerState:
        with self._lock:
            transition = self._refresh()
            state = self._state
        self._emit(transition)
        return state

    def before_call(self) -> None:
        """Raise ``CircuitOpenError`` if the call must not be attempted."""
        error: Optional[CircuitOpenError] = None
        with self._lock:
            transition = self._refresh()
            if self._state is BreakerState.OPEN:
                wait = self._cfg.recovery_timeout - (self._clock.monotonic() - self._opened_at)
                error = CircuitOpenError(max(0.0, wait))
            elif self._state is BreakerState.HALF_OPEN:
                if self._probes_inflight >= self._cfg.half_open_max_calls:
                    error = CircuitOpenError(0.0)
                else:
                    self._probes_inflight += 1
        self._emit(transition)
        if error:
            raise error

    def record_success(self) -> None:
        transition: _Transition = None
        with self._lock:
            if self._state is BreakerState.CLOSED:
                self._failures = 0
            elif self._state is BreakerState.HALF_OPEN:
                self._probes_inflight = max(0, self._probes_inflight - 1)
                self._probe_successes += 1
                if self._probe_successes >= self._cfg.success_threshold:
                    transition = self._set_state(BreakerState.CLOSED)
        self._emit(transition)

    def record_failure(self) -> None:
        transition: _Transition = None
        with self._lock:
            if self._state is BreakerState.CLOSED:
                self._failures += 1
                if self._failures >= self._cfg.failure_threshold:
                    transition = self._set_state(BreakerState.OPEN)
            elif self._state is BreakerState.HALF_OPEN:
                transition = self._set_state(BreakerState.OPEN)
            # OPEN: a late failure from a call started before tripping; ignore.
        self._emit(transition)

    def record_abort(self) -> None:
        """The call neither succeeded nor failed (e.g. cancelled or timed out before the
        backend answered). Release a half-open probe slot without counting it either way."""
        with self._lock:
            if self._state is BreakerState.HALF_OPEN:
                self._probes_inflight = max(0, self._probes_inflight - 1)

    def call(self, fn: Callable[[], R]) -> R:
        """Convenience wrapper for arbitrary callables."""
        self.before_call()
        try:
            result = fn()
        except BaseException:
            self.record_failure()
            raise
        self.record_success()
        return result

    def reset(self) -> None:
        with self._lock:
            transition = self._set_state(BreakerState.CLOSED)
        self._emit(transition)

    # -- internals (call with the lock held) -----------------------------------
    def _refresh(self) -> _Transition:
        if (
            self._state is BreakerState.OPEN
            and self._clock.monotonic() - self._opened_at >= self._cfg.recovery_timeout
        ):
            return self._set_state(BreakerState.HALF_OPEN)
        return None

    def _set_state(self, new: BreakerState) -> _Transition:
        old = self._state
        if old is new:
            return None
        self._state = new
        self._failures = 0
        self._probes_inflight = 0
        self._probe_successes = 0
        if new is BreakerState.OPEN:
            self._opened_at = self._clock.monotonic()
        return (old, new)

    def _emit(self, transition: _Transition) -> None:
        # Fired after the lock is released so callbacks may safely re-enter the breaker.
        if transition and self._on_change:
            self._on_change(*transition)
