import unittest

from connpool import AsyncCallableFactory, AsyncPoolRegistry, CallableFactory, PoolRegistry
from tests.support.helpers import quiet_config


class RegistryTests(unittest.TestCase):
    def test_one_pool_per_key(self):
        made = []

        def factory_for(key):
            made.append(key)
            return CallableFactory(lambda: (key, object()))

        with PoolRegistry(factory_for, quiet_config()) as reg:
            with reg.connection("a") as a1:
                pass
            with reg.connection("a") as a2:
                pass
            with reg.connection("b") as b:
                pass
            self.assertIs(a1, a2)
            self.assertEqual(sorted(made), ["a", "b"])
            self.assertEqual(b[0], "b")
        with self.assertRaises(RuntimeError):
            reg.pool("a")


class AsyncRegistryTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_pool_per_key(self):
        made = []

        def factory_for(key):
            made.append(key)
            return AsyncCallableFactory(lambda: (key, object()))

        async with AsyncPoolRegistry(factory_for, quiet_config()) as reg:
            async with reg.connection("a") as a1:
                pass
            async with reg.connection("a") as a2:
                pass
            async with reg.connection("b") as b:
                pass
            self.assertIs(a1, a2)
            self.assertEqual(sorted(made), ["a", "b"])
            self.assertEqual(b[0], "b")
        with self.assertRaises(RuntimeError):
            await reg.pool("a")


if __name__ == "__main__":
    unittest.main()
