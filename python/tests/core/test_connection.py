import unittest

from connpool import PooledConnection


class PooledConnectionTests(unittest.TestCase):
    def test_age_idle_and_hold_times(self):
        c = PooledConnection(raw=object(), created_at=100.0)
        self.assertEqual(c.age(110.0), 10.0)
        self.assertEqual(c.idle_time(105.0), 5.0)     # last_returned_at defaults to created_at
        self.assertEqual(c.hold_time(110.0), 0.0)     # not checked out yet

        c.mark_checked_out(120.0)
        self.assertEqual(c.use_count, 1)
        self.assertEqual(c.hold_time(123.0), 3.0)

        c.mark_returned(130.0)
        self.assertEqual(c.idle_time(131.0), 1.0)
        self.assertIsNone(c.checked_out_at)

    def test_repr(self):
        c = PooledConnection(raw=object(), created_at=0.0)
        self.assertIn("PooledConnection", repr(c))


if __name__ == "__main__":
    unittest.main()
