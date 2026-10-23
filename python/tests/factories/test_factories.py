import unittest

from connpool import (
    AsyncCallableFactory, AsyncConnectionFactory, CallableFactory, ConnectionFactory,
)


class Conn:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class SyncFactoryTests(unittest.TestCase):
    def test_base_class_defaults(self):
        class Minimal(ConnectionFactory):
            def create(self):
                return Conn()

        f = Minimal()
        c = f.create()
        self.assertTrue(f.validate(c))                 # default: always healthy
        self.assertFalse(f.is_fatal_error(OSError()))  # default: nothing is fatal
        f.reset(c)                                      # default: no-op
        f.close(c)                                       # default: calls conn.close()
        self.assertTrue(c.closed)

    def test_base_close_without_close_method(self):
        class Minimal(ConnectionFactory):
            def create(self):
                return object()

        Minimal().close(object())                        # no .close() attr: silently fine

    def test_callable_factory_all_callbacks(self):
        resets = []
        f = CallableFactory(
            Conn,
            validate=lambda c: True,
            reset=lambda c: resets.append(c),
            is_fatal_error=lambda e: isinstance(e, ValueError),
        )
        c = f.create()
        self.assertTrue(f.validate(c))
        f.reset(c)
        self.assertEqual(resets, [c])
        self.assertTrue(f.is_fatal_error(ValueError()))
        self.assertFalse(f.is_fatal_error(OSError()))

    def test_callable_factory_defaults_when_callbacks_omitted(self):
        f = CallableFactory(Conn)                         # no validate/reset/is_fatal
        c = f.create()
        self.assertTrue(f.validate(c))                    # None validate -> True
        f.reset(c)                                         # None reset -> no-op
        self.assertFalse(f.is_fatal_error(OSError()))     # None is_fatal -> False
        f.close(c)                                          # falls back to conn.close()
        self.assertTrue(c.closed)


class AsyncFactoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_base_class_defaults(self):
        class Minimal(AsyncConnectionFactory):
            async def create(self):
                return Conn()

        f = Minimal()
        c = await f.create()
        self.assertTrue(await f.validate(c))
        self.assertIsNone(await f.reset(c))
        self.assertFalse(f.is_fatal_error(OSError()))
        await f.close(c)                                   # awaits conn.close() (sync here)
        self.assertTrue(c.closed)

    async def test_base_close_without_close_method(self):
        class Minimal(AsyncConnectionFactory):
            async def create(self):
                return object()

        await Minimal().close(object())

    async def test_callable_factory_all_callbacks_sync_and_async(self):
        resets = []

        async def areset(c):
            resets.append(c)

        f = AsyncCallableFactory(
            Conn,
            validate=lambda c: True,                       # sync callable is fine
            reset=areset,                                  # async callable is fine
            is_fatal_error=lambda e: isinstance(e, ValueError),
        )
        c = await f.create()
        self.assertTrue(await f.validate(c))
        await f.reset(c)
        self.assertEqual(resets, [c])
        self.assertTrue(f.is_fatal_error(ValueError()))

    async def test_callable_factory_defaults_when_callbacks_omitted(self):
        f = AsyncCallableFactory(Conn)
        c = await f.create()
        self.assertTrue(await f.validate(c))               # None validate -> True
        await f.reset(c)                                    # None reset -> no-op
        self.assertFalse(f.is_fatal_error(OSError()))
        await f.close(c)                                     # falls back to conn.close()
        self.assertTrue(c.closed)


if __name__ == "__main__":
    unittest.main()
