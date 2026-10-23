"""Asyncio example (runs as-is): swap FakeClient for asyncpg / aiohttp / redis.asyncio / etc."""
import asyncio

from connpool import AsyncConnectionPool, BreakerConfig, PoolConfig, RetryPolicy


class FakeClient:
    async def ping(self):
        await asyncio.sleep(0)
        return True

    async def call(self, n):
        await asyncio.sleep(0.01)
        return n * 2

    async def close(self):
        pass


async def main():
    pool = AsyncConnectionPool.from_callables(
        FakeClient,                                            # sync or async callables both work
        validate=lambda c: c.ping(),
        config=PoolConfig(
            min_size=1, max_size=5, acquire_timeout=2, create_timeout=3,
            retry=RetryPolicy(max_attempts=3, base_delay=0.2),
            breaker=BreakerConfig(failure_threshold=5, recovery_timeout=20),
        ),
    )
    async with pool:
        async def one(n):
            async with pool.connection() as client:
                return await client.call(n)

        print(sum(await asyncio.gather(*(one(i) for i in range(100)))))
        print(pool.stats())


asyncio.run(main())
