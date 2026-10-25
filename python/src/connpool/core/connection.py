"""Bookkeeping wrapper around a raw connection."""
from __future__ import annotations

import enum
import itertools
from dataclasses import dataclass, field
from typing import Generic, TypeVar

T = TypeVar("T")
_ids = itertools.count(1)


class CloseReason(str, enum.Enum):
    """Why a connection left the pool."""

    EXPIRED = "expired"            # max_lifetime reached (rotation)
    MAX_USES = "max_uses"          # max_uses reached (rotation)
    IDLE = "idle"                  # sat idle longer than max_idle_time
    UNHEALTHY = "unhealthy"        # failed validation
    ERROR = "error"                # caller's block raised and the connection is suspect
    RESET_FAILED = "reset_failed"  # factory.reset() raised on release
    DISCARDED = "discarded"        # caller asked for it
    POOL_CLOSED = "pool_closed"


@dataclass(eq=False)
class PooledConnection(Generic[T]):
    """A raw connection plus the metadata the pool needs to manage it."""

    raw: T
    created_at: float
    expires_at: float | None = None
    last_returned_at: float | None = None
    checked_out_at: float | None = None
    use_count: int = 0
    leak_reported: bool = False
    id: int = field(default_factory=lambda: next(_ids))

    def __post_init__(self) -> None:
        if self.last_returned_at is None:
            self.last_returned_at = self.created_at

    def age(self, now: float) -> float:
        return now - self.created_at

    def idle_time(self, now: float) -> float:
        return now - (self.last_returned_at or self.created_at)

    def hold_time(self, now: float) -> float:
        return 0.0 if self.checked_out_at is None else now - self.checked_out_at

    def mark_checked_out(self, now: float) -> None:
        self.checked_out_at = now
        self.use_count += 1
        self.leak_reported = False

    def mark_returned(self, now: float) -> None:
        self.checked_out_at = None
        self.last_returned_at = now

    def __repr__(self) -> str:
        return f"<PooledConnection #{self.id} uses={self.use_count}>"
