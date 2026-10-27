import math
import random
import unittest

from connpool import PoolConfigError, RetryPolicy


class RetryPolicyTests(unittest.TestCase):
    def test_invalid_config(self):
        for bad in (dict(max_attempts=0), dict(base_delay=-1), dict(multiplier=0.5), dict(jitter=2.0)):
            with self.subTest(bad=bad), self.assertRaises(PoolConfigError):
                RetryPolicy(**bad)

    def test_retry_policy_backoff(self):
        p = RetryPolicy(max_attempts=5, base_delay=1, max_delay=5, multiplier=2, jitter=0)
        rng = random.Random(0)
        self.assertEqual([p.delay_for(n, rng) for n in (1, 2, 3, 4)], [1, 2, 4, 5])
        full = RetryPolicy(base_delay=4, jitter=1.0)
        self.assertTrue(all(0 <= full.delay_for(1, rng) <= 4 for _ in range(100)))

    def test_zero_base_delay_no_nan(self):
        p = RetryPolicy(base_delay=0, multiplier=2, jitter=0.5)
        rng = random.Random(0)
        for n in (1, 2, 100, 1000):
            d = p.delay_for(n, rng)
            self.assertTrue(math.isfinite(d))
            self.assertEqual(d, 0.0)

    def test_retry_on_filter(self):
        p = RetryPolicy(retry_on=(ConnectionError,))
        self.assertTrue(p.should_retry(ConnectionResetError()))
        self.assertFalse(p.should_retry(ValueError()))


if __name__ == "__main__":
    unittest.main()
