"""Atheris (libFuzzer) coverage-guided harness for the async pool's divergent primitives:
``asyncio.Condition`` waiter coordination, ``asyncio.shield``-guarded disposal, and
cancellation via ``task.cancel()`` / ``CancelledError``.

Outside ``src/`` and ``tests/`` on purpose: Atheris is not importable on win32 (confirmed
here) and needs clang/libFuzzer, so this file can never be picked up by
``coverage run --source=src`` or ``unittest discover -s tests`` and can never drag the
100% gate or add a runtime dependency. The deterministic twin of this fuzzer (no Atheris
needed) lives in ``tests/pools/test_async_pool_property.py`` and runs in the normal gate.

Manual run (Linux/clang):
    pip install atheris
    python fuzz/fuzz_async_pool.py -max_total_time=60
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import atheris  # noqa: E402  (import after sys.path setup; not installed on win32)

from fuzz.async_pool_ops import run_op_sequence  # noqa: E402


def test_one_input(data: bytes) -> None:
    fdp = atheris.FuzzedDataProvider(data)
    ops = []
    # Cap the sequence length so one input can't run forever under the fuzzer's watchdog.
    while fdp.remaining_bytes() > 0 and len(ops) < 64:
        ops.append(fdp.ConsumeIntInRange(0, 5))
    try:
        asyncio.run(asyncio.wait_for(run_op_sequence(ops), timeout=10))
    except TimeoutError as exc:
        raise AssertionError(f"deadlock: op sequence {ops} did not finish in time") from exc


if __name__ == "__main__":
    atheris.Setup(sys.argv, test_one_input)
    atheris.Fuzz()
