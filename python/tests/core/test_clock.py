import time
import unittest

from connpool import ManualClock, SystemClock


class SystemClockTests(unittest.IsolatedAsyncioTestCase):
    def test_monotonic_and_sleep(self):
        clock = SystemClock()
        before = clock.monotonic()
        clock.sleep(0.001)
        self.assertGreaterEqual(clock.monotonic(), before)

    async def test_async_sleep(self):
        await SystemClock().async_sleep(0.001)


class ManualClockTests(unittest.IsolatedAsyncioTestCase):
    def test_time_only_moves_when_told(self):
        clock = ManualClock(start=100.0)
        self.assertEqual(clock.monotonic(), 100.0)
        clock.advance(5)
        self.assertEqual(clock.monotonic(), 105.0)
        clock.sleep(2)                       # sleeping just advances time
        self.assertEqual(clock.monotonic(), 107.0)

    async def test_async_sleep_advances(self):
        clock = ManualClock(start=0.0)
        await clock.async_sleep(3)
        self.assertEqual(clock.monotonic(), 3.0)


if __name__ == "__main__":
    unittest.main()
