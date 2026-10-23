"""One pool per key (e.g. per host, per tenant, per database) created on demand.

Both registries are *lazy*: a pool is built on first use and is **not** ``open()``-ed,
so it does no background maintenance unless you open it yourself (``registry.pool(key).open()``).
"""
from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Callable, Dict, Generic, Hashable, Iterator, Optional, TypeVar

from connpool.policy.config import PoolConfig
from connpool.core.timeouts import UNSET, Timeout
from connpool.factories.async_factory import AsyncConnectionFactory
from connpool.factories.sync_factory import ConnectionFactory
from connpool.pools.async_pool import AsyncConnectionPool
from connpool.pools.sync_pool import ConnectionPool

K = TypeVar("K", bound=Hashable)
T = TypeVar("T")


class PoolRegistry(Generic[K, T]):
    """
        hosts = PoolRegistry(lambda host: HttpFactory(host), PoolConfig(max_size=4))
        with hosts.connection("api.example.com") as conn: ...
    """

    def __init__(self, factory_for: Callable[[K], ConnectionFactory[T]],
                 config: Optional[PoolConfig] = None, **pool_kwargs: object) -> None:
        self._factory_for, self._config, self._kwargs = factory_for, config, pool_kwargs
        self._pools: Dict[K, ConnectionPool[T]] = {}
        self._lock = threading.Lock()
        self._closed = False

    def pool(self, key: K) -> ConnectionPool[T]:
        with self._lock:
            if self._closed:
                raise RuntimeError("registry is closed")
            pool = self._pools.get(key)
            if pool is None:
                pool = self._pools[key] = ConnectionPool(
                    self._factory_for(key), self._config, name=str(key), **self._kwargs)  # type: ignore[arg-type]
            return pool

    @contextmanager
    def connection(self, key: K, timeout: Timeout = UNSET) -> Iterator[T]:
        with self.pool(key).connection(timeout) as conn:
            yield conn

    def close(self) -> None:
        with self._lock:
            self._closed = True
            pools, self._pools = list(self._pools.values()), {}
        for pool in pools:
            pool.close()

    def __enter__(self) -> "PoolRegistry[K, T]":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class AsyncPoolRegistry(Generic[K, T]):
    """Async twin of ``PoolRegistry``: one ``AsyncConnectionPool`` per key.

        hosts = AsyncPoolRegistry(lambda host: HttpFactory(host), PoolConfig(max_size=4))
        async with hosts.connection("api.example.com") as conn: ...
    """

    def __init__(self, factory_for: Callable[[K], AsyncConnectionFactory[T]],
                 config: Optional[PoolConfig] = None, **pool_kwargs: object) -> None:
        self._factory_for, self._config, self._kwargs = factory_for, config, pool_kwargs
        self._pools: Dict[K, AsyncConnectionPool[T]] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def pool(self, key: K) -> AsyncConnectionPool[T]:
        async with self._lock:
            if self._closed:
                raise RuntimeError("registry is closed")
            pool = self._pools.get(key)
            if pool is None:
                pool = self._pools[key] = AsyncConnectionPool(
                    self._factory_for(key), self._config, name=str(key), **self._kwargs)  # type: ignore[arg-type]
            return pool

    @asynccontextmanager
    async def connection(self, key: K, timeout: Timeout = UNSET) -> AsyncIterator[T]:
        pool = await self.pool(key)
        async with pool.connection(timeout) as conn:
            yield conn

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            pools, self._pools = list(self._pools.values()), {}
        for pool in pools:
            await pool.close()

    async def __aenter__(self) -> "AsyncPoolRegistry[K, T]":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()
