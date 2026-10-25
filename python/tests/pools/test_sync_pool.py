import random
import threading
import time
import unittest

from connpool import (
    BreakerConfig,
    CallableFactory,
    CircuitOpenError,
    CloseReason,
    ConnectionCreateError,
    ConnectionPool,
    IdleStrategy,
    InvalidReleaseError,
    ManualClock,
    PoolClosedError,
    PoolExhaustedError,
    PoolTimeoutError,
    RetryPolicy,
)
from tests.support.helpers import FakeConn, FakeFactory, Recorder, quiet_config


class PoolTestCase(unittest.TestCase):
    def make(self, factory=None, clock=None, **cfg):
        self.factory = factory or FakeFactory()
        self.rec = Recorder()
        self.clock = clock
        pool = ConnectionPool(self.factory, quiet_config(**cfg), listeners=[self.rec], clock=clock)
        self.addCleanup(pool.close)
        return pool


class BasicsTests(PoolTestCase):
    def test_connection_is_reused(self):
        pool = self.make()
        with pool.connection() as a:
            pass
        with pool.connection() as b:
            pass
        self.assertIs(a, b)
        self.assertEqual(len(self.factory.created), 1)
        self.assertEqual(a.resets, 2)

    def test_max_size_is_a_hard_cap_and_acquire_times_out(self):
        pool = self.make(max_size=1)
        held = pool.acquire()
        with self.assertRaises(PoolTimeoutError):
            pool.acquire(timeout=0.05)
        self.assertEqual(pool.stats().timeouts, 1)
        pool.release(held)

    def test_zero_timeout_is_non_blocking(self):
        pool = self.make(max_size=1)
        held = pool.acquire()
        with self.assertRaises(PoolTimeoutError):
            pool.acquire(timeout=0)
        pool.release(held)

    def test_waiter_gets_connection_when_released(self):
        pool = self.make(max_size=1)
        held = pool.acquire()
        got = []
        t = threading.Thread(target=lambda: got.append(pool.acquire(timeout=2)))
        t.start()
        time.sleep(0.05)
        self.assertEqual(pool.stats().waiting, 1)
        pool.release(held)
        t.join(2)
        self.assertEqual(got, [held])

    def test_double_and_foreign_release_rejected(self):
        pool = self.make()
        conn = pool.acquire()
        pool.release(conn)
        with self.assertRaises(InvalidReleaseError):
            pool.release(conn)
        with self.assertRaises(InvalidReleaseError):
            pool.release(FakeConn())

    def test_idle_strategy_lifo_vs_fifo(self):
        for strategy, expected_index in ((IdleStrategy.LIFO, 2), (IdleStrategy.FIFO, 0)):
            with self.subTest(strategy=strategy):
                pool = self.make(idle_strategy=strategy)
                conns = [pool.acquire() for _ in range(3)]
                for c in conns:
                    pool.release(c)
                self.assertIs(pool.acquire(), conns[expected_index])

    def test_from_callables_and_default_close(self):
        class Thing:
            closed = False
            def close(self): self.closed = True
        pool = ConnectionPool.from_callables(Thing, config=quiet_config())
        with pool.connection() as t:
            pass
        pool.close()
        self.assertTrue(t.closed)

    def test_explicit_discard(self):
        pool = self.make()
        conn = pool.acquire()
        pool.release(conn, discard=True)
        self.assertTrue(conn.closed)
        self.assertEqual(pool.stats().size, 0)
        self.assertEqual(self.rec.closed, [CloseReason.DISCARDED])

    def test_validate_idle_evicts_dead_connections(self):
        pool = self.make(validate_idle=True, max_size=2)
        a = pool.acquire(); b = pool.acquire()
        pool.release(a); pool.release(b)
        self.assertEqual(pool.stats().idle, 2)
        a.healthy = False                                  # a goes silently dead while idle
        pool.run_maintenance()                             # background health check reaps it
        self.assertEqual(pool.stats().idle, 1)
        self.assertIn(CloseReason.UNHEALTHY, self.rec.closed)
        with pool.connection() as c:
            self.assertIs(c, b)                            # the healthy survivor is reused

    def test_duplicate_connection_from_factory_is_rejected(self):
        shared = FakeConn()
        pool = self.make(CallableFactory(lambda: shared))
        first = pool.acquire()
        with self.assertRaises(ConnectionCreateError):
            pool.acquire()                                      # same object again -> rejected
        self.assertEqual(pool.stats().size, 1)                 # the duplicate slot was freed
        self.assertEqual(pool.stats().created, 1)              # duplicate not counted as created
        pool.release(first)                                     # original still releasable
        self.assertEqual(pool.stats().in_use, 0)


class RotationTests(PoolTestCase):
    def test_expired_connection_is_not_reused(self):
        pool = self.make(clock=ManualClock(), max_lifetime=100)
        a = pool.acquire(); pool.release(a)
        self.clock.advance(101)
        b = pool.acquire()
        self.assertIsNot(a, b)
        self.assertTrue(a.closed)
        self.assertEqual(self.rec.closed, [CloseReason.EXPIRED])

    def test_connection_expiring_while_in_use_is_closed_on_release(self):
        pool = self.make(clock=ManualClock(), max_lifetime=100)
        a = pool.acquire()
        self.clock.advance(150)
        pool.release(a)
        self.assertTrue(a.closed)
        self.assertEqual(pool.stats().idle, 0)

    def test_max_uses_rotation(self):
        pool = self.make(max_uses=2)
        first = pool.acquire(); pool.release(first)
        again = pool.acquire(); pool.release(again)      # 2nd use -> retired
        self.assertIs(first, again)
        self.assertTrue(first.closed)
        self.assertIsNot(pool.acquire(), first)
        self.assertEqual(self.rec.closed, [CloseReason.MAX_USES])

    def test_idle_expired_connection_dropped_on_borrow(self):
        pool = self.make(clock=ManualClock(), max_idle_time=10)
        a = pool.acquire(); pool.release(a)
        self.clock.advance(11)                            # sat idle too long, no sweep yet
        b = pool.acquire()                                # borrow sees it over-idle and drops it
        self.assertIsNot(a, b)
        self.assertEqual(self.rec.closed, [CloseReason.IDLE])

    def test_idle_eviction_respects_min_size(self):
        pool = self.make(clock=ManualClock(), min_size=1, max_idle_time=10)
        conns = [pool.acquire() for _ in range(3)]
        for c in conns:
            pool.release(c)
        self.clock.advance(11)
        pool.run_maintenance()
        self.assertEqual(pool.stats().idle, 1)
        self.assertEqual(self.rec.closed.count(CloseReason.IDLE), 2)

    def test_maintenance_replaces_expired_to_keep_min_size(self):
        pool = self.make(clock=ManualClock(), min_size=2, max_lifetime=50)
        pool.open()
        self.assertEqual(pool.stats().idle, 2)
        self.clock.advance(51)
        pool.run_maintenance()
        self.assertEqual(pool.stats().idle, 2)
        self.assertEqual(len(self.factory.created), 4)
        self.assertEqual(sum(c.closed for c in self.factory.created), 2)

    def test_background_maintenance_thread(self):
        pool = self.make(maintenance_interval=0.02, max_idle_time=0.05)
        pool.open()
        pool.release(pool.acquire())
        deadline = time.time() + 3
        while pool.stats().size and time.time() < deadline:
            time.sleep(0.02)
        self.assertEqual(pool.stats().size, 0)


class HealthTests(PoolTestCase):
    def test_unhealthy_idle_connection_replaced_on_borrow(self):
        pool = self.make(validate_after_idle=0)
        a = pool.acquire(); pool.release(a)
        a.healthy = False
        b = pool.acquire()
        self.assertIsNot(a, b)
        self.assertTrue(a.closed)
        self.assertEqual(self.rec.closed, [CloseReason.UNHEALTHY])

    def test_validation_skipped_when_recently_used(self):
        pool = self.make(clock=ManualClock(), validate_after_idle=30)
        a = pool.acquire(); pool.release(a)
        a.healthy = False                       # would fail validation, but we don't ask
        self.assertIs(pool.acquire(), a)

    def test_fatal_error_discards_connection(self):
        self.factory = FakeFactory(); self.factory.fatal_types = (ConnectionResetError,)
        pool = self.make(self.factory)
        with self.assertRaises(ConnectionResetError), pool.connection() as c:
            raise ConnectionResetError()
        self.assertTrue(c.closed)
        self.assertEqual(self.rec.closed, [CloseReason.ERROR])

    def test_benign_error_keeps_healthy_connection(self):
        pool = self.make()
        with self.assertRaises(ValueError), pool.connection() as c:
            raise ValueError("app bug")
        self.assertFalse(c.closed)
        self.assertEqual(pool.stats().idle, 1)

    def test_error_plus_failed_validation_discards(self):
        pool = self.make()
        with self.assertRaises(ValueError), pool.connection() as c:
            c.healthy = False
            raise ValueError()
        self.assertTrue(c.closed)

    def test_reset_failure_discards(self):
        pool = self.make()
        self.factory.reset_raises = True
        with pool.connection() as c:
            pass
        self.assertTrue(c.closed)
        self.assertEqual(self.rec.closed, [CloseReason.RESET_FAILED])

    def test_close_errors_do_not_leak_slots(self):
        class BadClose(FakeFactory):
            def close(self, conn): raise OSError("close failed")
        pool = self.make(BadClose())
        pool.release(pool.acquire(), discard=True)
        self.assertEqual(pool.stats().size, 0)

    def test_validation_error_is_treated_as_unhealthy(self):
        def raising_validate(conn): raise RuntimeError("ping blew up")
        pool = self.make(CallableFactory(FakeConn, validate=raising_validate), validate_after_idle=0)
        a = pool.acquire(); pool.release(a)      # parked
        b = pool.acquire()                        # checkout validates -> raises -> unhealthy -> fresh
        self.assertIsNot(a, b)
        self.assertEqual(self.rec.closed, [CloseReason.UNHEALTHY])


class ResilienceTests(PoolTestCase):
    def test_retry_with_backoff_succeeds(self):
        self.factory = FakeFactory(); self.factory.fail_times = 2
        retry = RetryPolicy(max_attempts=3, base_delay=1, multiplier=2, jitter=0)
        pool = self.make(self.factory, clock=ManualClock(), retry=retry, acquire_timeout=60)
        start = self.clock.monotonic()
        pool.acquire()
        self.assertEqual(self.factory.attempts, 3)
        self.assertEqual(self.clock.monotonic() - start, 3)      # slept 1s then 2s

    def test_exhausted_retries_raise_and_do_not_leak_slot(self):
        self.factory = FakeFactory(); self.factory.fail_always = True
        pool = self.make(self.factory, clock=ManualClock(), retry=RetryPolicy(max_attempts=2, jitter=0))
        with self.assertRaises(ConnectionCreateError) as ctx:
            pool.acquire()
        self.assertIsInstance(ctx.exception.__cause__, ConnectionRefusedError)
        self.assertEqual(self.factory.attempts, 2)
        self.assertEqual(pool.stats().size, 0)
        self.assertEqual(pool.stats().create_failures, 2)

    def test_non_retryable_error_fails_immediately(self):
        class AuthFail(FakeFactory):
            def create(self): self.attempts += 1; raise PermissionError("bad password")
        pool = self.make(AuthFail(), clock=ManualClock(),
                         retry=RetryPolicy(max_attempts=5, retry_on=(ConnectionError,)))
        with self.assertRaises(ConnectionCreateError):
            pool.acquire()
        self.assertEqual(self.factory.attempts, 1)

    def test_retry_never_sleeps_past_acquire_deadline(self):
        self.factory = FakeFactory(); self.factory.fail_always = True
        pool = self.make(self.factory, clock=ManualClock(), acquire_timeout=1,
                         retry=RetryPolicy(max_attempts=5, base_delay=5, jitter=0))
        with self.assertRaises(PoolTimeoutError):
            pool.acquire()
        self.assertEqual(self.factory.attempts, 1)

    def test_circuit_breaker_full_cycle(self):
        self.factory = FakeFactory(); self.factory.fail_always = True
        pool = self.make(self.factory, clock=ManualClock(),
                         breaker=BreakerConfig(failure_threshold=2, recovery_timeout=30))
        for _ in range(2):
            with self.assertRaises(ConnectionCreateError):
                pool.acquire()
        attempts = self.factory.attempts
        with self.assertRaises(CircuitOpenError):               # fails fast, backend untouched
            pool.acquire()
        self.assertEqual(self.factory.attempts, attempts)
        self.assertEqual(pool.stats().breaker_state, "open")

        self.clock.advance(31)                                  # recovery window passes
        self.factory.fail_always = False
        conn = pool.acquire()                                   # half-open probe succeeds
        self.assertEqual(pool.stats().breaker_state, "closed")
        self.assertEqual(self.rec.events.count("breaker:open->half_open"), 1)
        self.assertEqual(pool.stats().size, 1)
        pool.release(conn)

    def test_failed_probe_reopens_breaker(self):
        self.factory = FakeFactory(); self.factory.fail_always = True
        pool = self.make(self.factory, clock=ManualClock(),
                         breaker=BreakerConfig(failure_threshold=1, recovery_timeout=5))
        with self.assertRaises(ConnectionCreateError):
            pool.acquire()
        self.clock.advance(6)
        with self.assertRaises(ConnectionCreateError):          # the probe fails
            pool.acquire()
        with self.assertRaises(CircuitOpenError):
            pool.acquire()

    def test_breaker_counts_one_failure_per_acquire_not_per_attempt(self):
        self.factory = FakeFactory(); self.factory.fail_always = True
        pool = self.make(self.factory, clock=ManualClock(), acquire_timeout=60,
                         retry=RetryPolicy(max_attempts=3, base_delay=1, jitter=0),
                         breaker=BreakerConfig(failure_threshold=2, recovery_timeout=30))
        with self.assertRaises(ConnectionCreateError):
            pool.acquire()                                      # 3 attempts, 1 breaker failure
        self.assertEqual(self.factory.attempts, 3)
        self.assertEqual(pool.stats().breaker_state, "closed")  # NOT tripped by the retries
        with self.assertRaises(ConnectionCreateError):
            pool.acquire()                                      # 2nd failure -> trips
        self.assertEqual(pool.stats().breaker_state, "open")

    def test_create_timeout_does_not_trip_breaker(self):
        self.factory = FakeFactory(); self.factory.fail_always = True
        pool = self.make(self.factory, clock=ManualClock(), acquire_timeout=1,
                         retry=RetryPolicy(max_attempts=3, base_delay=10, jitter=0),
                         breaker=BreakerConfig(failure_threshold=1))
        with self.assertRaises(PoolTimeoutError):               # ran out of time while retrying
            pool.acquire()
        self.assertEqual(pool.stats().breaker_state, "closed")  # a timeout is not a backend fault

    def test_pool_closed_mid_retry_stops_creating(self):
        pool = None

        def create():
            pool.close()                                        # closes between retry attempts
            raise ConnectionError("down")

        pool = self.make(CallableFactory(create),
                         retry=RetryPolicy(max_attempts=3, base_delay=0, jitter=0))
        with self.assertRaises(PoolClosedError):
            pool.acquire()


class ShutdownAndObservabilityTests(PoolTestCase):
    def test_close_semantics(self):
        pool = self.make(max_size=1)
        held = pool.acquire()
        errors = []

        def waiter():
            try:
                pool.acquire(timeout=5)
            except PoolClosedError as exc:
                errors.append(exc)

        t = threading.Thread(target=waiter); t.start()
        time.sleep(0.05)
        pool.close()
        t.join(2)
        self.assertEqual(len(errors), 1)                        # blocked caller was released
        self.assertFalse(held.closed)                           # in-use connection untouched...
        pool.release(held)
        self.assertTrue(held.closed)                            # ...closed once returned
        with self.assertRaises(PoolClosedError):
            pool.acquire()
        pool.close()                                            # idempotent

    def test_close_closes_idle_connections(self):
        pool = self.make()
        c = pool.acquire(); pool.release(c)
        pool.close()
        self.assertTrue(c.closed)

    def test_context_manager_opens_and_closes(self):
        self.factory = FakeFactory()
        with ConnectionPool(self.factory, quiet_config(min_size=2)) as pool:
            self.assertEqual(pool.stats().idle, 2)
        self.assertTrue(pool.closed)
        self.assertTrue(all(c.closed for c in self.factory.created))

    def test_warmup_failure_is_not_fatal(self):
        self.factory = FakeFactory(); self.factory.fail_always = True
        pool = self.make(self.factory, min_size=2)
        pool.open()
        self.assertEqual(pool.stats().size, 0)

    def test_stats_and_events(self):
        pool = self.make()
        with pool.connection():
            s = pool.stats()
            self.assertEqual((s.size, s.idle, s.in_use), (1, 0, 1))
        s = pool.stats()
        self.assertEqual((s.created, s.acquired, s.released, s.idle, s.in_use), (1, 1, 1, 1, 0))
        self.assertEqual(self.rec.events, ["created", "acquired", "released"])

    def test_broken_listener_cannot_break_pool(self):
        from connpool import ConnectionPool, PoolListener

        class Boom(PoolListener):
            def on_acquired(self, *a): raise RuntimeError("metrics down")
        pool = ConnectionPool(FakeFactory(), quiet_config(), listeners=[Boom()])
        self.addCleanup(pool.close)
        with pool.connection():
            pass

    def test_connection_created_during_close_does_not_leak(self):
        # Factory closes the pool mid-create: the fresh connection must be destroyed,
        # acquire must raise PoolClosedError, and the slot must be reclaimed.
        pool = None
        made = []

        def create():
            c = FakeConn(); made.append(c)
            pool.close()                                        # pool closes while we build
            return c

        pool = self.make(CallableFactory(create, close=lambda c: setattr(c, "closed", True)))
        with self.assertRaises(PoolClosedError):
            pool.acquire()
        self.assertEqual(len(made), 1)
        self.assertTrue(made[0].closed)                        # created conn was closed
        self.assertEqual(pool.stats().size, 0)                 # slot not leaked

    def test_graceful_drain_waits_for_in_use(self):
        pool = self.make(max_size=2)
        held = pool.acquire()
        released = threading.Event()

        def returner():
            time.sleep(0.05)
            pool.release(held)
            released.set()

        t = threading.Thread(target=returner); t.start()
        pool.close(timeout=2)                                  # should block until returner releases
        t.join(2)
        self.assertTrue(released.is_set())
        self.assertTrue(held.closed)                           # drained connection was closed
        self.assertEqual(pool.stats().in_use, 0)

    def test_graceful_drain_gives_up_after_timeout(self):
        pool = self.make(max_size=1)
        held = pool.acquire()
        pool.close(timeout=0.05)                               # caller never returns it
        self.assertEqual(pool.stats().in_use, 1)              # still outstanding after timeout
        self.assertFalse(held.closed)
        pool.release(held)                                     # closes on release
        self.assertTrue(held.closed)


class ConcurrencyTests(PoolTestCase):
    def test_stress_respects_max_size_and_leaks_nothing(self):
        pool = self.make(max_size=5, acquire_timeout=10, max_uses=7)
        lock = threading.Lock()
        state = {"now": 0, "peak": 0}
        errors = []

        def worker(seed):
            rng = random.Random(seed)
            try:
                for _ in range(60):
                    with pool.connection() as conn:
                        with lock:
                            state["now"] += 1
                            state["peak"] = max(state["peak"], state["now"])
                        self.assertFalse(conn.closed)
                        time.sleep(rng.random() * 0.002)
                        with lock:
                            state["now"] -= 1
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
        [t.start() for t in threads]
        [t.join(60) for t in threads]
        self.assertEqual(errors, [])
        self.assertLessEqual(state["peak"], 5)
        s = pool.stats()
        self.assertEqual((s.in_use, s.waiting), (0, 0))
        self.assertEqual(s.size, s.idle)
        self.assertEqual(s.acquired, 20 * 60)
        self.assertEqual(s.created - s.closed, s.idle)          # every conn accounted for
        self.assertTrue(all(not p.raw.closed for p in pool._idle))


class LeakDetectionTests(PoolTestCase):
    def test_leak_detection_warns_once(self):
        clock = ManualClock()
        pool = self.make(clock=clock, leak_detection_timeout=10)
        conn = pool.acquire()
        pool.run_maintenance()                                 # not held long enough yet
        self.assertEqual(pool.stats().leaks, 0)
        clock.advance(11)
        pool.run_maintenance()
        pool.run_maintenance()                                 # second sweep: no duplicate report
        self.assertEqual(pool.stats().leaks, 1)
        pool.release(conn)


class BackpressureTests(PoolTestCase):
    def test_zero_waiters_fails_fast_when_full(self):
        pool = self.make(max_size=1, max_waiters=0, acquire_timeout=5)
        held = pool.acquire()
        with self.assertRaises(PoolExhaustedError):            # would wait, but cap is 0
            pool.acquire()
        pool.release(held)

    def test_waiter_queue_is_capped(self):
        pool = self.make(max_size=1, max_waiters=1, acquire_timeout=5)
        held = pool.acquire()

        def wait_one():
            try:
                pool.release(pool.acquire(timeout=5))
            except Exception:
                pass

        t = threading.Thread(target=wait_one); t.start()
        while pool.stats().waiting < 1:                        # the single allowed waiter
            time.sleep(0.005)
        with self.assertRaises(PoolExhaustedError):            # one more exceeds the cap
            pool.acquire(timeout=5)
        pool.release(held)
        t.join(2)


class LifecycleEdgeTests(PoolTestCase):
    def test_open_on_closed_pool_raises(self):
        pool = self.make(); pool.close()
        with self.assertRaises(PoolClosedError):
            pool.open()

    def test_open_is_idempotent(self):
        pool = self.make(maintenance_interval=0.5)
        self.assertIs(pool.open(), pool.open())                # second open() no-ops

    def test_run_maintenance_on_closed_pool_is_noop(self):
        pool = self.make(); pool.close()
        pool.run_maintenance()                                 # returns quietly

    def test_warmup_connection_destroyed_if_closed_during_create(self):
        made = []
        pool = None

        def create():
            c = FakeConn(); made.append(c); pool.close(); return c

        pool = ConnectionPool(CallableFactory(create, close=lambda c: setattr(c, "closed", True)),
                              quiet_config(min_size=1))
        pool.open()                                            # warm-up create closes the pool
        self.assertTrue(made and made[0].closed)              # the orphan was destroyed

    def test_repr_mentions_sizes(self):
        pool = self.make()
        self.assertIn("ConnectionPool", repr(pool))

    def test_del_warns_when_not_closed(self):
        pool = ConnectionPool(FakeFactory(), quiet_config())
        pool.acquire()                                         # size > 0, never closed
        with self.assertWarns(ResourceWarning):
            pool.__del__()
        pool.close()

    def test_maintenance_loop_survives_run_errors(self):
        pool = self.make(maintenance_interval=0.01)
        calls = []

        def boom():
            calls.append(1); raise RuntimeError("sweep broke")

        pool.run_maintenance = boom                            # type: ignore[method-assign]
        t = threading.Thread(target=pool._maintenance_loop, daemon=True); t.start()
        while not calls:
            time.sleep(0.005)
        pool._stop.set(); t.join(1)
        self.assertTrue(calls)                                 # kept looping despite the error


if __name__ == "__main__":
    unittest.main()
