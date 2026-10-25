"""Tiny shared helpers (internal)."""
from __future__ import annotations

import enum
from typing import Union


class _Unset(enum.Enum):
    UNSET = "unset"


UNSET = _Unset.UNSET
"""Sentinel: "use the configured default" (``None`` already means "wait forever")."""

Timeout = Union[float, None, _Unset]


def compute_deadline(timeout: Timeout, default: float | None, now: float) -> float | None:
    if isinstance(timeout, _Unset):
        timeout = default
    return None if timeout is None else now + timeout


def remaining(deadline: float | None, now: float) -> float | None:
    return None if deadline is None else deadline - now
