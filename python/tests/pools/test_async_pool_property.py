"""Deterministic stdlib twin of ``fuzz/fuzz_async_pool.py`` (no Atheris needed). Runs
seeded random op sequences against the async pool's divergent primitives
(``asyncio.Condition`` waiter coordination, ``asyncio.shield``-guarded disposal,
cancellation via ``task.cancel()``) and checks the same invariants, so this file is
gated by ``unittest discover -s tests`` / the 100% coverage gate even though the
Atheris harness itself cannot run on this platform."""
from __future__ import annotations

import asyncio
import random
import unittest

from fuzz.async_pool_ops import OP_ACQUIRE, OP_ADVANCE_CLOCK, OP_CANCEL, OP_CLOSE, OP_RELEASE, run_op_sequence

_OPS = (OP_ACQUIRE, OP_RELEASE, OP_CANCEL, OP_ADVANCE_CLOCK, OP_CLOSE)


class AsyncPoolPropertyTests(unittest.TestCase):
    """Seeded random op sequences stand in for Atheris's coverage-guided corpus: not
    exhaustive, but exercises the same cancellation / condition-wakeup invariants
    deterministically on every platform."""

    def test_seeded_op_sequences_hold_invariants(self):
        rng = random.Random(1234)
        for _ in range(30):
            ops = [rng.choice(_OPS) for _ in range(rng.randint(1, 20))]
            with self.subTest(ops=ops):
                asyncio.run(asyncio.wait_for(run_op_sequence(ops), timeout=10))

    def test_empty_sequence(self):
        asyncio.run(asyncio.wait_for(run_op_sequence([]), timeout=10))


if __name__ == "__main__":
    unittest.main()
