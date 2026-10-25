"""Asynchronous factory contract: async twin of ``sync_factory``."""
from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, Generic, TypeVar, cast

T = TypeVar("T")


async def _maybe_await(value: Any | Awaitable[Any]) -> Any:
    return await value if inspect.isawaitable(value) else value


class AsyncConnectionFactory(ABC, Generic[T]):
    """Async twin of ``ConnectionFactory``."""

    @abstractmethod
    async def create(self) -> T: ...

    async def validate(self, conn: T) -> bool:
        return True

    async def reset(self, conn: T) -> None:
        return None

    async def close(self, conn: T) -> None:
        closer = getattr(conn, "close", None)
        if callable(closer):
            await _maybe_await(closer())

    def is_fatal_error(self, exc: BaseException) -> bool:
        return False


class AsyncCallableFactory(AsyncConnectionFactory[T]):
    """Callables may be sync or async - both work."""

    def __init__(
        self,
        create: Callable[[], Any],
        *,
        close: Callable[[T], Any] | None = None,
        validate: Callable[[T], Any] | None = None,
        reset: Callable[[T], Any] | None = None,
        is_fatal_error: Callable[[BaseException], bool] | None = None,
    ) -> None:
        self._create, self._close, self._validate = create, close, validate
        self._reset, self._is_fatal = reset, is_fatal_error

    async def create(self) -> T:
        return cast(T, await _maybe_await(self._create()))

    async def validate(self, conn: T) -> bool:
        return True if self._validate is None else bool(await _maybe_await(self._validate(conn)))

    async def reset(self, conn: T) -> None:
        if self._reset:
            await _maybe_await(self._reset(conn))

    async def close(self, conn: T) -> None:
        if self._close:
            await _maybe_await(self._close(conn))
        else:
            await super().close(conn)

    def is_fatal_error(self, exc: BaseException) -> bool:
        return bool(self._is_fatal and self._is_fatal(exc))
