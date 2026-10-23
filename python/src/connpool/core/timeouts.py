"""Tiny shared helpers (internal)."""
from __future__ import annotations

import enum
from typing import Optional, Union


class _Unset(enum.Enum):
    UNSET = "unset"


UNSET = _Unset.UNSET
"""Sentinel: "use the configured default" (``None`` already means "wait forever")."""

Timeout = Union[float, None, _Unset]


def compute_deadline(timeout: Timeout, default: Optional[float], now: float) -> Optional[float]:
    if isinstance(timeout, _Unset):
        timeout = default
    return None if timeout is None else now + timeout


def remaining(deadline: Optional[float], now: float) -> Optional[float]:
    return None if deadline is None else deadline - now
