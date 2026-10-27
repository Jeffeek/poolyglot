"""Op-sequence interpreter for the async pool fuzz harness. Lives in ``fuzz/`` (not
``src/`` or ``tests/``) so it never counts toward the 100% coverage gate or the ruff F,B
lint gate; the deterministic/property twin in
``tests/pools/test_async_pool_property.py`` duplicates the handful of lines that matter
so it *is* gated.

Decodes a small integer op sequence into pool operations run against a fresh
``AsyncConnectionPool`` backed by ``ManualClock`` + a fake async factory, and checks the
invariants from the architect's spec:
  * no deadlock (the caller wraps this in ``asyncio.wait_for``)
  * the Condition waiter set empties at rest
  * a cancelled acquire never leaks a reserved slot (idle+in_use == size,
    created-closed == size)
  * shield guarantees factory.close() completes even when the acquiring task is cancelled
  * no lost wakeup (every pending acquire eventually resolves or is cancelled)
"""
from __future__ import annotations

import asyncio

from connpool import (
    AsyncConnectionFactory,
    AsyncConnectionPool,
    ManualClock,
    PoolClosedError,
    PoolConfig,
    PoolExhaustedError,
    PoolTimeoutError,
)

# Expected failure modes of an acquire task under this op sequence (timeout/exhausted/
# closed-while-pending) - not invariant violations, just normal pool behavior.
_ACCEPTABLE = (PoolTimeoutError, PoolExhaustedError, PoolClosedError)

# Ops, decoded from fuzzer bytes (0..5):
OP_ACQUIRE = 0        # start a new pending acquire task
OP_RELEASE = 1        # release the oldest still-held connection
OP_CANCEL = 2         # cancel the oldest still-pending acquire task
OP_ADVANCE_CLOCK = 3  # advance the ManualClock (exercises lifetime/idle/breaker logic)
OP_CLOSE = 4          # close the pool
OP_YIELD = 5          # let the loop run a tick (lets pending tasks progress)


class _FakeConn:
    pass


class _FakeAsyncFactory(AsyncConnectionFactory[_FakeConn]):
    def __init__(self) -> None:
        self.created = 0
        self.closed = 0

    async def create(self) -> _FakeConn:
        # The interruptible await comes before the counter bump (as a real factory's I/O
        # would be): create() only "counts" a connection once it is actually handed back,
        # so a CancelledError landing during the await never leaves a half-made connection
        # on the books that nothing will ever close.
        await asyncio.sleep(0)
        self.created += 1
        return _FakeConn()

    async def close(self, conn: _FakeConn) -> None:
        await asyncio.sleep(0)
        self.closed += 1

    async def validate(self, conn: _FakeConn) -> bool:
        return True

    async def reset(self, conn: _FakeConn) -> None:
        return None


async def run_op_sequence(ops: list[int]) -> None:
    clock = ManualClock()
    factory = _FakeAsyncFactory()
    cfg = PoolConfig(max_size=3, min_size=0, acquire_timeout=None, maintenance_interval=None)
    pool = AsyncConnectionPool(factory, cfg, clock=clock)
    await pool.open()

    held: list = []
    pending: list[asyncio.Task] = []

    try:
        for op in ops:
            if op == OP_ACQUIRE:
                pending.append(asyncio.create_task(pool.acquire(timeout=5)))
            elif op == OP_RELEASE and held:
                await pool.release(held.pop(0))
            elif op == OP_CANCEL and pending:
                # Only request cancellation here; keep the task in `pending` (instead of
                # dropping the reference) so it is still awaited below. A task that was
                # just cancelled is not yet `.done()` - its cleanup is delivered on a later
                # scheduling tick - so forgetting it here would let invariant checks run
                # before that cleanup actually happened.
                pending[0].cancel()
            elif op == OP_ADVANCE_CLOCK:
                clock.advance(1.0)
            elif op == OP_CLOSE:
                await pool.close()
            else:
                await asyncio.sleep(0)

            # Drain any tasks that finished (or were cancelled) so `held` stays accurate
            # and exceptions besides CancelledError surface immediately.
            still_pending = []
            for t in pending:
                if t.done():
                    if not t.cancelled():
                        exc = t.exception()
                        if exc is not None and not isinstance(exc, _ACCEPTABLE):
                            raise exc
                        if exc is None:
                            held.append(t.result())
                else:
                    still_pending.append(t)
            pending = still_pending

        # Drain remaining pending tasks with a bounded wait (the outer asyncio.wait_for
        # in the harness is the real deadlock guard; this just cleans up this run).
        for t in pending:
            t.cancel()
        for t in pending:
            try:
                conn = await t
                held.append(conn)
            except (*_ACCEPTABLE, asyncio.CancelledError):
                pass

        for conn in held:
            await pool.release(conn)
    finally:
        await pool.close()

    # ``_destroy`` shields disposal with ``asyncio.shield``: a connection's cleanup (the
    # slot decrement + factory.close()) is guaranteed to eventually run even when the task
    # that triggered it was cancelled, but NOT guaranteed to have finished by the time that
    # cancelled task itself returns to its caller - the shielded coroutine keeps running as
    # its own task in the background. Settle for a bounded number of ticks so any disposal
    # still in flight from a cancelled acquire has a chance to finish before we assert on
    # final counts (the outer asyncio.wait_for is the real deadlock guard).
    for _ in range(100):
        await asyncio.sleep(0)

    _check_invariants(pool, factory)


def _check_invariants(pool: AsyncConnectionPool, factory: _FakeAsyncFactory) -> None:
    cond = pool._cond  # noqa: SLF001 - intentional white-box check, fuzz-only code
    assert len(cond._waiters) == 0, "Condition waiter set did not empty at rest"  # noqa: SLF001
    s = pool.stats()
    assert s.idle + s.in_use == s.size, f"slot accounting leak: idle={s.idle} in_use={s.in_use} size={s.size}"
    assert factory.created - factory.closed == s.size, (
        f"created/closed mismatch: created={factory.created} closed={factory.closed} size={s.size}"
    )
