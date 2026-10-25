"""Synchronous factory contract: the ONE seam between the pool and the outside world.

To pool *anything* (DB driver, HTTP session, SSH client, socket, gRPC channel...)
implement ``ConnectionFactory`` - or just pass callables via ``CallableFactory``.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Generic, TypeVar

T = TypeVar("T")


class ConnectionFactory(ABC, Generic[T]):
    """Override ``create``; override the rest only when the default isn't right."""

    @abstractmethod
    def create(self) -> T:
        """Open a new connection. Should enforce its own connect timeout."""

    def validate(self, conn: T) -> bool:
        """Cheap liveness check (e.g. ``SELECT 1``). Return False/raise = dead."""
        return True

    def reset(self, conn: T) -> None:
        """Called on release to scrub session state (e.g. ``rollback()``)."""

    def close(self, conn: T) -> None:
        """Dispose of a connection. Default: call ``conn.close()`` if present."""
        closer = getattr(conn, "close", None)
        if callable(closer):
            closer()

    def is_fatal_error(self, exc: BaseException) -> bool:
        """True if ``exc`` (raised by user code using the connection) proves the
        connection is broken, so it is discarded without further checks."""
        return False


class CallableFactory(ConnectionFactory[T]):
    """Build a factory from plain functions."""

    def __init__(
        self,
        create: Callable[[], T],
        *,
        close: Callable[[T], None] | None = None,
        validate: Callable[[T], bool] | None = None,
        reset: Callable[[T], None] | None = None,
        is_fatal_error: Callable[[BaseException], bool] | None = None,
    ) -> None:
        self._create, self._close, self._validate = create, close, validate
        self._reset, self._is_fatal = reset, is_fatal_error

    def create(self) -> T:
        return self._create()

    def validate(self, conn: T) -> bool:
        return True if self._validate is None else bool(self._validate(conn))

    def reset(self, conn: T) -> None:
        if self._reset:
            self._reset(conn)

    def close(self, conn: T) -> None:
        if self._close:
            self._close(conn)
        else:
            super().close(conn)

    def is_fatal_error(self, exc: BaseException) -> bool:
        return bool(self._is_fatal and self._is_fatal(exc))
