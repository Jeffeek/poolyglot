import random
import unittest
from collections import deque

from connpool import CloseReason, IdleStrategy, PoolConfig, PooledConnection
from connpool.policy import lifecycle


def conn(created=0.0, returned=None, uses=0, expires=None):
    c = PooledConnection(raw=object(), created_at=created, expires_at=expires, last_returned_at=returned)
    c.use_count = uses
    return c


class LifecycleTests(unittest.TestCase):
    def test_lifetime_jitter_stays_in_range(self):
        cfg = PoolConfig(max_lifetime=100, lifetime_jitter=0.1)
        rng = random.Random(7)
        for _ in range(200):
            c = lifecycle.new_connection(object(), cfg, 0.0, rng)
            self.assertTrue(90 <= c.expires_at <= 100)

    def test_rotation_reasons(self):
        cfg = PoolConfig(max_uses=3)
        self.assertIs(lifecycle.rotation_reason(conn(expires=5), cfg, 5), CloseReason.EXPIRED)
        self.assertIs(lifecycle.rotation_reason(conn(uses=3), cfg, 0), CloseReason.MAX_USES)
        self.assertIsNone(lifecycle.rotation_reason(conn(uses=2, expires=9), cfg, 5))

    def test_sweep_never_goes_below_min_size(self):
        cfg = PoolConfig(min_size=2, max_idle_time=10)
        idle = [conn(returned=0) for _ in range(4)]
        keep, doomed = lifecycle.sweep(idle, total_size=4, cfg=cfg, now=11)
        self.assertEqual((len(keep), len(doomed)), (2, 2))
        self.assertTrue(all(r is CloseReason.IDLE for _, r in doomed))

    def test_sweep_always_evicts_expired(self):
        cfg = PoolConfig(min_size=2, max_idle_time=None)
        keep, doomed = lifecycle.sweep([conn(expires=1), conn()], total_size=2, cfg=cfg, now=5)
        self.assertEqual([r for _, r in doomed], [CloseReason.EXPIRED])
        self.assertEqual(len(keep), 1)

    def test_validation_policy(self):
        c = conn(returned=0)
        self.assertFalse(lifecycle.needs_validation(c, PoolConfig(validate_after_idle=None), 100))
        self.assertTrue(lifecycle.needs_validation(c, PoolConfig(validate_after_idle=0), 0))
        self.assertFalse(lifecycle.needs_validation(c, PoolConfig(validate_after_idle=30), 10))
        self.assertTrue(lifecycle.needs_validation(c, PoolConfig(validate_after_idle=30), 30))

    def test_pop_strategies(self):
        a, b = conn(), conn()
        d = deque([a, b])
        self.assertIs(lifecycle.pop_idle(d, IdleStrategy.LIFO), b)
        d = deque([a, b])
        self.assertIs(lifecycle.pop_idle(d, IdleStrategy.FIFO), a)
        self.assertIsNone(lifecycle.pop_idle(deque(), IdleStrategy.LIFO))


if __name__ == "__main__":
    unittest.main()
