import unittest

from connpool import (
    BreakerConfig,
    BreakerState,
    CircuitBreaker,
    CircuitOpenError,
    ManualClock,
    PoolConfigError,
)


class BreakerTests(unittest.TestCase):
    def make(self, **kw):
        self.clock = ManualClock()
        self.changes = []
        cfg = BreakerConfig(**{"failure_threshold": 3, "recovery_timeout": 10, **kw})
        return CircuitBreaker(cfg, self.clock, lambda o, n: self.changes.append((o, n)))

    def fail(self, b, n=1):
        for _ in range(n):
            b.before_call()
            b.record_failure()

    def test_opens_after_consecutive_failures(self):
        b = self.make()
        self.fail(b, 3)
        self.assertIs(b.state, BreakerState.OPEN)
        with self.assertRaises(CircuitOpenError) as ctx:
            b.before_call()
        self.assertAlmostEqual(ctx.exception.retry_after, 10, delta=0.01)

    def test_success_resets_failure_streak(self):
        b = self.make()
        self.fail(b, 2)
        b.before_call(); b.record_success()
        self.fail(b, 2)
        self.assertIs(b.state, BreakerState.CLOSED)

    def test_half_open_then_closes_on_success(self):
        b = self.make()
        self.fail(b, 3)
        self.clock.advance(10)
        self.assertIs(b.state, BreakerState.HALF_OPEN)
        b.before_call(); b.record_success()
        self.assertIs(b.state, BreakerState.CLOSED)
        self.assertEqual([n.value for _, n in self.changes], ["open", "half_open", "closed"])

    def test_half_open_failure_reopens(self):
        b = self.make()
        self.fail(b, 3)
        self.clock.advance(10)
        b.before_call(); b.record_failure()
        self.assertIs(b.state, BreakerState.OPEN)

    def test_half_open_limits_concurrent_probes(self):
        b = self.make(half_open_max_calls=1)
        self.fail(b, 3)
        self.clock.advance(10)
        b.before_call()                       # the one probe
        with self.assertRaises(CircuitOpenError):
            b.before_call()                   # second is rejected

    def test_success_threshold(self):
        b = self.make(success_threshold=2, half_open_max_calls=2)
        self.fail(b, 3)
        self.clock.advance(10)
        b.before_call(); b.record_success()
        self.assertIs(b.state, BreakerState.HALF_OPEN)
        b.before_call(); b.record_success()
        self.assertIs(b.state, BreakerState.CLOSED)

    def test_call_wrapper_and_reset(self):
        b = self.make(failure_threshold=1)
        with self.assertRaises(ZeroDivisionError):
            b.call(lambda: 1 / 0)
        self.assertIs(b.state, BreakerState.OPEN)
        b.reset()
        self.assertEqual(b.call(lambda: 42), 42)

    def test_callback_may_reenter_breaker(self):
        clock = ManualClock()
        b = CircuitBreaker(BreakerConfig(failure_threshold=1), clock, lambda o, n: b.state)
        b.before_call(); b.record_failure()   # would deadlock if callback ran under the lock

    def test_record_abort_frees_probe_without_counting(self):
        b = self.make(half_open_max_calls=1)
        self.fail(b, 3)
        self.clock.advance(10)
        b.before_call()                       # take the one probe slot
        b.record_abort()                      # neither success nor failure: slot released
        self.assertIs(b.state, BreakerState.HALF_OPEN)   # still probing, not reopened
        b.before_call()                       # a fresh probe is allowed again
        b.record_success()
        self.assertIs(b.state, BreakerState.CLOSED)

    def test_abort_in_closed_state_is_a_noop(self):
        b = self.make()
        b.record_abort()                      # no probe to release; must not raise or change state
        self.assertIs(b.state, BreakerState.CLOSED)

    def test_reset_when_already_closed_is_a_noop(self):
        b = self.make()
        b.reset()                             # CLOSED -> CLOSED: no transition emitted
        self.assertIs(b.state, BreakerState.CLOSED)
        self.assertEqual(self.changes, [])

    def test_invalid_config(self):
        with self.assertRaises(PoolConfigError):
            BreakerConfig(failure_threshold=0)
        with self.assertRaises(PoolConfigError):
            BreakerConfig(recovery_timeout=-1)


if __name__ == "__main__":
    unittest.main()
