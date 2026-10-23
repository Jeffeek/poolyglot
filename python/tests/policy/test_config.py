import unittest

from connpool import PoolConfig, PoolConfigError


class ConfigTests(unittest.TestCase):
    def test_defaults_are_valid(self):
        PoolConfig()

    def test_rejects_bad_values(self):
        for bad in (dict(max_size=0), dict(min_size=11), dict(min_size=-1), dict(acquire_timeout=-1),
                    dict(max_lifetime=0), dict(lifetime_jitter=1.5), dict(max_uses=0),
                    dict(validate_after_idle=-1), dict(maintenance_interval=0),
                    dict(leak_detection_timeout=0), dict(leak_detection_timeout=-5)):
            with self.subTest(bad=bad), self.assertRaises(PoolConfigError):
                PoolConfig(**bad)

    def test_replace_returns_validated_copy(self):
        cfg = PoolConfig(max_size=5)
        self.assertEqual(cfg.replace(max_size=8).max_size, 8)
        self.assertEqual(cfg.max_size, 5)
        with self.assertRaises(PoolConfigError):
            cfg.replace(min_size=99)


if __name__ == "__main__":
    unittest.main()
