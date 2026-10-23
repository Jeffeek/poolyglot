"""Time source abstraction so every time-dependent rule is deterministic in tests."""
from __future__ import annotations

import asyncio
import time
from typing import Protocol


class Clock(Protocol):
    def monotonic(self) -> float: ...
    def sleep(self, seconds: float) -> None: ...
    async def async_sleep(self, seconds: float) -> None: ...


class SystemClock:
    """Real time. The default."""

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    async def async_sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class ManualClock:
    """Fake clock for tests: time moves only when you say so; sleeping just advances it."""

    def __init__(self, start: float = 1000.0) -> None:
        self._now = start

    def monotonic(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds

    def sleep(self, seconds: float) -> None:
        self._now += seconds

    async def async_sleep(self, seconds: float) -> None:
        self._now += seconds
        await asyncio.sleep(0)
