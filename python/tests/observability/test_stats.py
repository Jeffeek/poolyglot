import unittest

from connpool.observability.stats import ACQUIRE_WAIT_WINDOW, StatsCollector, percentile


class PercentileTests(unittest.TestCase):
    def test_empty_is_zero(self):
        self.assertEqual(percentile([], 0.5), 0.0)

    def test_known_quantiles(self):
        vals = [float(i) for i in range(1, 101)]          # 1..100, already sorted
        self.assertAlmostEqual(percentile(vals, 0.0), 1.0)
        self.assertAlmostEqual(percentile(vals, 1.0), 100.0)
        self.assertAlmostEqual(percentile(vals, 0.5), 50.5)      # linear interpolation
        self.assertAlmostEqual(percentile(vals, 0.99), 99.01)


class CollectorTests(unittest.TestCase):
    def test_acquire_wait_percentiles_in_snapshot(self):
        c = StatsCollector()
        for w in [0.1, 0.2, 0.3, 0.4, 100.0]:             # one slow outlier
            c.on_acquired(conn=None, wait_time=w)
        s = c.snapshot(name="x", max_size=1, size=0, idle=0, in_use=0, waiting=0)
        self.assertAlmostEqual(s.avg_acquire_wait, 20.2)
        self.assertLess(s.p50_acquire_wait, 1.0)                 # median unaffected by the outlier
        self.assertGreater(s.p99_acquire_wait, 50.0)            # tail catches it

    def test_create_latency_percentiles_in_snapshot(self):
        c = StatsCollector()
        for d in [0.01, 0.02, 0.03, 0.04, 5.0]:            # one slow create
            c.on_connection_created(conn=None, duration=d)
        s = c.snapshot(name="x", max_size=1, size=0, idle=0, in_use=0, waiting=0)
        self.assertAlmostEqual(s.avg_create_latency, 5.1 / 5)
        self.assertLess(s.p50_create_latency, 1.0)              # median unaffected by the outlier
        self.assertGreater(s.p99_create_latency, 1.0)          # tail catches the slow one

    def test_wait_window_is_bounded(self):
        c = StatsCollector()
        for _ in range(ACQUIRE_WAIT_WINDOW + 500):
            c.on_acquired(conn=None, wait_time=1.0)
        self.assertEqual(len(c._waits), ACQUIRE_WAIT_WINDOW)

    def test_snapshot_as_dict(self):
        c = StatsCollector()
        s = c.snapshot(name="x", max_size=2, size=1, idle=1, in_use=0, waiting=0)
        d = s.as_dict()
        self.assertEqual(d["name"], "x")
        self.assertEqual(d["max_size"], 2)
        self.assertIn("p99_acquire_wait", d)


if __name__ == "__main__":
    unittest.main()
