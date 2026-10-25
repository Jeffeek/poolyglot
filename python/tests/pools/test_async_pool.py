import asyncio
import unittest

from connpool import (
    AsyncCallableFactory,
    AsyncConnectionPool,
    BreakerConfig,
    CircuitOpenError,
    CloseReason,
    ConnectionCreateError,
    InvalidReleaseError,
    ManualClock,
    PoolClosedError,
    PoolExhaustedError,
    PoolTimeoutError,
    RetryPolicy,
)
from tests.support.helpers import AsyncFakeFactory, FakeConn, FakeFactory, Recorder, quiet_config


class AsyncPoolTests(unittest.IsolatedAsyncioTestCase):
    def make(self, factory=None, clock=None, **cfg):
        self.inner = FakeFactory()
        self.factory = factory or AsyncFakeFactory(self.inner)
        self.inner = getattr(self.factory, "inner", self.inner)
        self.rec = Recorder()
        self.clock = clock
        pool = AsyncConnectionPool(self.factory, quiet_config(**cfg), listeners=[self.rec], clock=clock)
        self.addAsyncCleanup(pool.close)
        return pool

    async def test_reuse_and_stats(self):
        pool = self.make()
        async with pool.connection() as a:
            pass
        async with pool.connection() as b:
            pass
        self.assertIs(a, b)
        s = pool.stats()
        self.assertEqual((s.created, s.acquired, s.idle, s.in_use), (1, 2, 1, 0))

    async def test_timeout_when_exhausted(self):
        pool = self.make(max_size=1)
        held = await pool.acquire()
        with self.assertRaises(PoolTimeoutError):
            await pool.acquire(timeout=0.05)
        self.assertEqual(pool.stats().timeouts, 1)
        await pool.release(held)

    async def test_waiter_woken_by_release(self):
        pool = self.make(max_size=1)
        held = await pool.acquire()
        waiter = asyncio.create_task(pool.acquire(timeout=2))
        await asyncio.sleep(0.02)
        self.assertEqual(pool.stats().waiting, 1)
        await pool.release(held)
        self.assertIs(await waiter, held)

    async def test_double_release_rejected(self):
        pool = self.make()
        conn = await pool.acquire()
        await pool.release(conn)
        with self.assertRaises(InvalidReleaseError):
            await pool.release(conn)

    async def test_rotation_by_lifetime(self):
        pool = self.make(clock=ManualClock(), max_lifetime=10)
        a = await pool.acquire(); await pool.release(a)
        self.clock.advance(11)
        b = await pool.acquire()
        self.assertIsNot(a, b)
        self.assertEqual(self.rec.closed, [CloseReason.EXPIRED])

    async def test_validation_and_error_discard(self):
        pool = self.make(validate_after_idle=0)
        a = await pool.acquire(); await pool.release(a)
        a.healthy = False
        b = await pool.acquire()
        self.assertIsNot(a, b)
        self.inner.fatal_types = (ConnectionResetError,)
        with self.assertRaises(ConnectionResetError):
            async with pool.connection() as c:
                raise ConnectionResetError()
        self.assertTrue(c.closed)

    async def test_retry_and_breaker(self):
        self.inner = FakeFactory(); self.inner.fail_always = True
        pool = self.make(AsyncFakeFactory(self.inner), clock=ManualClock(), acquire_timeout=60,
                         retry=RetryPolicy(max_attempts=2, base_delay=1, jitter=0),
                         breaker=BreakerConfig(failure_threshold=2, recovery_timeout=30))
        # The breaker counts one failure per logical acquire, not per retry attempt:
        # each acquire burns 2 create attempts but records a single breaker failure.
        with self.assertRaises(ConnectionCreateError):
            await pool.acquire()                    # failure #1 -> still closed
        self.assertEqual(self.inner.attempts, 2)
        self.assertEqual(pool.stats().breaker_state, "closed")
        with self.assertRaises(ConnectionCreateError):
            await pool.acquire()                    # failure #2 -> trips the breaker
        self.assertEqual(self.inner.attempts, 4)
        with self.assertRaises(CircuitOpenError):
            await pool.acquire()                    # now fails fast, no new attempts
        self.assertEqual(self.inner.attempts, 4)
        self.assertEqual(pool.stats().size, 0)
        self.clock.advance(31)
        self.inner.fail_always = False
        await pool.release(await pool.acquire())
        self.assertEqual(pool.stats().breaker_state, "closed")

    async def test_create_timeout_is_enforced(self):
        pool = self.make(AsyncFakeFactory(create_delay=5), create_timeout=0.05)
        with self.assertRaises(ConnectionCreateError):
            await pool.acquire()
        self.assertEqual(pool.stats().size, 0)

    async def test_cancel_while_waiting_does_not_leak(self):
        pool = self.make(max_size=1, acquire_timeout=None)
        held = await pool.acquire()
        waiter = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0.02)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(pool.stats().waiting, 0)
        await pool.release(held)
        self.assertIs(await pool.acquire(timeout=1), held)

    async def test_cancel_during_create_frees_slot(self):
        pool = self.make(AsyncFakeFactory(create_delay=5), max_size=1, create_timeout=None)
        task = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0.02)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(pool.stats().size, 0)

    async def test_close_wakes_waiters(self):
        pool = self.make(max_size=1, acquire_timeout=None)
        held = await pool.acquire()
        waiter = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0.02)
        await pool.close()
        with self.assertRaises(PoolClosedError):
            await waiter
        await pool.release(held)
        self.assertTrue(held.closed)

    async def test_maintenance_task_and_context_manager(self):
        factory = AsyncFakeFactory()
        async with AsyncConnectionPool(factory, quiet_config(min_size=2, maintenance_interval=0.02)) as pool:
            self.assertEqual(pool.stats().idle, 2)
        self.assertTrue(pool.closed)
        self.assertTrue(all(c.closed for c in factory.inner.created))

    async def test_open_racing_close_does_not_leak_maintenance_task(self):
        # close() can complete (seeing _task is None) while open() is still awaiting
        # _fill_to_min; open() must not then start a maintenance loop that outlives close().
        pool = AsyncConnectionPool(AsyncFakeFactory(), quiet_config(maintenance_interval=0.01))
        gate = asyncio.Event()
        orig_fill = pool._fill_to_min

        async def slow_fill():
            await gate.wait()
            await orig_fill()

        pool._fill_to_min = slow_fill  # type: ignore[method-assign]
        open_task = asyncio.create_task(pool.open())
        await asyncio.sleep(0)         # let open() reach the gate
        await pool.close()             # runs fully; _task is still None
        gate.set()
        await open_task

        self.assertTrue(pool.closed)
        self.assertIsNone(pool._task)
        await asyncio.sleep(0.03)      # a maintenance interval would have ticked
        lingering = [t for t in asyncio.all_tasks()
                     if t is not asyncio.current_task() and not t.done()]
        self.assertEqual(lingering, [])

    async def test_cancellation_during_disposal_still_completes(self):
        # Cancelling a caller while factory.close() is mid-I/O must not interrupt backend
        # disposal, skip the slot decrement, or drop on_connection_closed. Disposal is
        # shielded: the caller sees CancelledError at once, the close finishes in the
        # background, and created/closed accounting stays balanced.
        started, resume, closed = asyncio.Event(), asyncio.Event(), []

        async def slow_close(c):
            started.set()
            await resume.wait()
            closed.append(c)

        factory = AsyncCallableFactory(lambda: object(), close=slow_close)
        rec = Recorder()
        pool = AsyncConnectionPool(factory, quiet_config(max_size=2), listeners=[rec])
        self.addAsyncCleanup(pool.close)

        conn = await pool.acquire()
        releasing = asyncio.create_task(pool.release(conn, discard=True))
        await started.wait()                       # now suspended inside factory.close()
        releasing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await releasing                        # caller observes cancellation promptly
        self.assertEqual(closed, [])               # disposal paused, not yet finished

        resume.set()                               # let the shielded disposal complete
        for _ in range(100):                       # bounded; converges once disposal runs
            if pool.stats().size == 0 and rec.closed:
                break
            await asyncio.sleep(0)

        self.assertEqual(closed, [conn])                        # backend close completed
        self.assertEqual(rec.closed, [CloseReason.DISCARDED])   # event fired despite cancel
        s = pool.stats()
        self.assertEqual((s.size, s.closed), (0, 1))            # accounting balanced

    async def test_concurrency_stress(self):
        pool = self.make(max_size=4, acquire_timeout=10, max_uses=9)
        state = {"now": 0, "peak": 0}

        async def worker():
            for _ in range(40):
                async with pool.connection() as conn:
                    state["now"] += 1
                    state["peak"] = max(state["peak"], state["now"])
                    await asyncio.sleep(0)
                    self.assertFalse(conn.closed)
                    state["now"] -= 1

        await asyncio.gather(*(worker() for _ in range(25)))
        s = pool.stats()
        self.assertLessEqual(state["peak"], 4)
        self.assertEqual((s.in_use, s.waiting, s.acquired), (0, 0, 25 * 40))
        self.assertEqual(s.created - s.closed, s.idle)

    async def test_from_callables_accepts_sync_and_async(self):
        closed = []

        async def make():
            return object()

        pool = AsyncConnectionPool.from_callables(make, close=closed.append, config=quiet_config())
        async with pool.connection():
            pass
        await pool.close()
        self.assertEqual(len(closed), 1)

    async def test_duplicate_connection_from_factory_is_rejected(self):
        shared = object()
        pool = AsyncConnectionPool.from_callables(lambda: shared, config=quiet_config())
        self.addAsyncCleanup(pool.close)
        first = await pool.acquire()
        with self.assertRaises(ConnectionCreateError):
            await pool.acquire()
        self.assertEqual(pool.stats().size, 1)                 # duplicate slot freed
        self.assertEqual(pool.stats().created, 1)              # duplicate not counted as created
        await pool.release(first)

    async def test_validate_idle_evicts_dead_connections(self):
        pool = self.make(validate_idle=True, max_size=2)
        a = await pool.acquire(); b = await pool.acquire()
        await pool.release(a); await pool.release(b)
        self.assertEqual(pool.stats().idle, 2)
        a.healthy = False                                  # a goes silently dead while idle
        await pool.run_maintenance()                       # background health check reaps it
        self.assertEqual(pool.stats().idle, 1)
        self.assertIn(CloseReason.UNHEALTHY, self.rec.closed)
        async with pool.connection() as c:
            self.assertIs(c, b)                            # the healthy survivor is reused

    async def test_leak_detection_warns_once(self):
        clock = ManualClock()
        pool = self.make(clock=clock, leak_detection_timeout=10)
        conn = await pool.acquire()
        clock.advance(11)
        await pool.run_maintenance()
        await pool.run_maintenance()
        self.assertEqual(pool.stats().leaks, 1)
        await pool.release(conn)

    async def test_graceful_drain_waits_for_in_use(self):
        pool = self.make(max_size=2)
        held = await pool.acquire()

        async def returner():
            await asyncio.sleep(0.05)
            await pool.release(held)

        task = asyncio.create_task(returner())
        await pool.close(timeout=2)
        await task
        self.assertTrue(held.closed)
        self.assertEqual(pool.stats().in_use, 0)

    async def test_drain_timeout_gives_up(self):
        pool = self.make(max_size=1)
        held = await pool.acquire()
        await pool.close(timeout=0.05)
        self.assertEqual(pool.stats().in_use, 1)
        self.assertFalse(held.closed)
        await pool.release(held)
        self.assertTrue(held.closed)

    # ---------------------------------------------------------------- edge coverage
    async def test_open_edges(self):
        pool = self.make(maintenance_interval=0.5)
        self.assertIs(await pool.open(), await pool.open())     # idempotent
        await pool.close()
        with self.assertRaises(PoolClosedError):
            await pool.open()                                   # reopening a closed pool

    async def test_drain_zero_timeout_returns_immediately(self):
        pool = self.make(max_size=1)
        held = await pool.acquire()
        await pool.close(timeout=0)                             # left <= 0: don't wait
        self.assertEqual(pool.stats().in_use, 1)
        await pool.release(held)

    async def test_run_maintenance_on_closed_pool_is_noop(self):
        pool = self.make()
        await pool.close()
        await pool.run_maintenance()

    async def test_maintenance_evicts_idle(self):
        clock = ManualClock()
        pool = self.make(clock=clock, max_idle_time=10)
        a = await pool.acquire(); await pool.release(a)
        clock.advance(11)
        await pool.run_maintenance()
        self.assertEqual(pool.stats().idle, 0)
        self.assertEqual(self.rec.closed, [CloseReason.IDLE])

    async def test_max_waiters_fails_fast(self):
        pool = self.make(max_size=1, max_waiters=0, acquire_timeout=5)
        held = await pool.acquire()
        with self.assertRaises(PoolExhaustedError):
            await pool.acquire()
        await pool.release(held)

    async def test_release_discard_and_close_error_do_not_leak(self):
        async def bad_close(c):
            raise OSError("close boom")
        pool = self.make(AsyncCallableFactory(FakeConn, close=bad_close))
        await pool.release(await pool.acquire(), discard=True)  # discard + close() raises
        self.assertEqual(pool.stats().size, 0)
        self.assertEqual(self.rec.closed, [CloseReason.DISCARDED])

    async def test_release_reset_error_is_reset_failed(self):
        def bad_reset(c):
            raise RuntimeError("reset boom")
        pool = self.make(AsyncCallableFactory(FakeConn, reset=bad_reset))
        await pool.release(await pool.acquire())
        self.assertEqual(self.rec.closed, [CloseReason.RESET_FAILED])

    async def test_release_reset_baseexception_destroys(self):
        class Boom(BaseException):
            pass

        def bad_reset(c):
            raise Boom()
        pool = self.make(AsyncCallableFactory(FakeConn, reset=bad_reset))
        a = await pool.acquire()
        with self.assertRaises(Boom):
            await pool.release(a)
        self.assertEqual(self.rec.closed, [CloseReason.ERROR])

    async def test_checkout_validation_error_is_unhealthy(self):
        def bad_validate(c):
            raise RuntimeError("ping boom")
        pool = self.make(AsyncCallableFactory(FakeConn, validate=bad_validate), validate_after_idle=0)
        a = await pool.acquire(); await pool.release(a)
        b = await pool.acquire()
        self.assertIsNot(a, b)
        self.assertEqual(self.rec.closed, [CloseReason.UNHEALTHY])

    async def test_borrow_validation_baseexception_is_not_swallowed(self):
        class Boom(BaseException):
            pass

        def bad_validate(c):
            raise Boom()
        pool = self.make(AsyncCallableFactory(FakeConn, validate=bad_validate), validate_after_idle=0)
        a = await pool.acquire(); await pool.release(a)
        with self.assertRaises(Boom):
            await pool.acquire()                               # BaseException propagates, conn dropped
        self.assertEqual(self.rec.closed, [CloseReason.ERROR])

    async def test_create_timeout_during_retry_does_not_trip_breaker(self):
        inner = FakeFactory(); inner.fail_always = True
        pool = self.make(AsyncFakeFactory(inner), clock=ManualClock(), acquire_timeout=1,
                         retry=RetryPolicy(max_attempts=3, base_delay=10, jitter=0),
                         breaker=BreakerConfig(failure_threshold=1))
        with self.assertRaises(PoolTimeoutError):
            await pool.acquire()
        self.assertEqual(pool.stats().breaker_state, "closed")

    async def test_create_timeout_counts_as_breaker_failure(self):
        # A slow backend (create_timeout fires, acquire deadline still has room) is a real
        # fault: it is reported as ConnectionCreateError and counts toward the breaker.
        pool = self.make(AsyncFakeFactory(create_delay=5), create_timeout=0.05,
                         acquire_timeout=5, breaker=BreakerConfig(failure_threshold=1))
        with self.assertRaises(ConnectionCreateError):
            await pool.acquire()
        self.assertEqual(pool.stats().breaker_state, "open")

    async def test_acquire_deadline_timeout_does_not_trip_breaker(self):
        # create() is slow and the acquire deadline (not create_timeout) is the binding
        # limit, so the timeout is caller impatience: PoolTimeoutError, breaker untouched.
        pool = self.make(AsyncFakeFactory(create_delay=5), create_timeout=None,
                         acquire_timeout=0.05, breaker=BreakerConfig(failure_threshold=1))
        with self.assertRaises(PoolTimeoutError):
            await pool.acquire()
        self.assertEqual(pool.stats().breaker_state, "closed")

    async def test_cancel_while_registering_does_not_leak(self):
        # Cancellation delivered while _register_new waits on the (contended) condition lock,
        # after the connection was already built. The half-registered connection must be
        # closed exactly once, accounting must return to zero, nothing may stay untracked,
        # the cancellation must propagate, and the pool must remain usable.
        built, proceed, closes = asyncio.Event(), asyncio.Event(), []

        async def create():
            conn = FakeConn()
            built.set()                 # connection built; _create is about to return
            await proceed.wait()        # hold until the test is holding _cond
            return conn

        async def closer(conn):
            closes.append(conn)
            conn.closed = True

        pool = AsyncConnectionPool(AsyncCallableFactory(create, close=closer),
                                   quiet_config(max_size=2))
        self.addAsyncCleanup(pool.close)

        task = asyncio.create_task(pool.acquire())
        await built.wait()                              # parked inside create(); slot reserved
        await pool._cond.acquire()                      # force _register_new's lock to block
        proceed.set()                                   # create() returns -> task hits that lock
        while not any(not f.done() for f in (pool._cond._lock._waiters or ())):
            await asyncio.sleep(0)                      # until the task is queued on the lock
        task.cancel()                                   # cancel while it waits for the lock
        pool._cond.release()                            # let the shielded cleanup proceed

        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(closes), 1)                       # closed exactly once
        self.assertEqual(pool.stats().size, 0)                 # slot accounting restored
        self.assertEqual(pool.stats().idle, 0)
        self.assertEqual(len(pool._checked_out), 0)            # nothing left untracked
        self.assertEqual(pool.stats().created, 0)              # never adopted -> not counted
        async with pool.connection() as c:                     # pool still usable
            self.assertFalse(c.closed)

    async def test_pool_closed_mid_retry(self):
        pool = None

        async def create():
            await pool.close()
            raise ConnectionError("down")
        pool = self.make(AsyncCallableFactory(create),
                         retry=RetryPolicy(max_attempts=3, base_delay=0, jitter=0))
        with self.assertRaises(PoolClosedError):
            await pool.acquire()

    async def test_connection_created_during_close_is_destroyed(self):
        pool = None
        made = []

        async def create():
            c = FakeConn(); made.append(c); await pool.close(); return c
        pool = self.make(AsyncCallableFactory(create, close=lambda c: setattr(c, "closed", True)))
        with self.assertRaises(PoolClosedError):
            await pool.acquire()
        self.assertTrue(made and made[0].closed)

    async def test_warmup_failure_is_not_fatal(self):
        inner = FakeFactory(); inner.fail_always = True
        pool = self.make(AsyncFakeFactory(inner), min_size=2)
        await pool.open()
        self.assertEqual(pool.stats().size, 0)

    async def test_warmup_connection_destroyed_if_closed_during_create(self):
        pool = None
        made = []

        async def create():
            c = FakeConn(); made.append(c); await pool.close(); return c
        pool = AsyncConnectionPool(
            AsyncCallableFactory(create, close=lambda c: setattr(c, "closed", True)),
            quiet_config(min_size=1))
        await pool.open()
        self.assertTrue(made and made[0].closed)

    async def test_maintenance_loop_runs_and_survives_errors(self):
        pool = self.make(maintenance_interval=0.01)
        calls = []

        async def boom():
            calls.append(1); raise RuntimeError("sweep broke")
        pool.run_maintenance = boom                            # type: ignore[method-assign]
        await pool.open()
        while not calls:
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.02)                              # kept looping despite the error
        self.assertGreaterEqual(len(calls), 1)

    async def test_repr_mentions_sizes(self):
        self.assertIn("AsyncConnectionPool", repr(self.make()))

    async def test_del_warns_when_not_closed(self):
        pool = AsyncConnectionPool(AsyncFakeFactory(), quiet_config())
        await pool.acquire()
        with self.assertWarns(ResourceWarning):
            pool.__del__()
        await pool.close()


if __name__ == "__main__":
    unittest.main()
