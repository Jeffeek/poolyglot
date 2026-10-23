"""Exception hierarchy. Catch ``PoolError`` to handle anything the pool raises."""
from __future__ import annotations


class PoolError(Exception):
    """Base class for every error raised by connpool."""


class PoolConfigError(PoolError, ValueError):
    """Invalid pool / breaker / retry configuration."""


class PoolClosedError(PoolError):
    """The pool has been closed; no more connections can be acquired."""


class PoolTimeoutError(PoolError, TimeoutError):
    """No connection became available before the acquire deadline."""


class PoolExhaustedError(PoolError):
    """The pool is full and the waiter queue is at ``max_waiters``; shedding load."""


class ConnectionCreateError(PoolError):
    """The factory failed to create a connection (after all retries).

    The original exception is available as ``__cause__``.
    """


class CircuitOpenError(PoolError):
    """The circuit breaker is rejecting calls to protect a failing backend."""

    def __init__(self, retry_after: float) -> None:
        super().__init__(f"circuit breaker is open; retry in ~{retry_after:.1f}s")
        self.retry_after = retry_after


class InvalidReleaseError(PoolError):
    """Released an object that is not checked out from this pool (double release?)."""
