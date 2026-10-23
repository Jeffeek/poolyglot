"""The pure "functional core": every rule about *when a connection must go away*.

No locks, no I/O, no clocks of its own - just functions of (connection, config, now).
Both the sync and async pools call these, so the rules live in exactly one place and
can be unit-tested without threads or event loops.
"""
from __future__ import annotations

import random
from collections import deque
from typing import Any, Deque, Iterable, List, Optional, Tuple, TypeVar

from connpool.policy.config import IdleStrategy, PoolConfig
from connpool.core.connection import CloseReason, PooledConnection

T = TypeVar("T")


def new_connection(raw: T, cfg: PoolConfig, now: float, rng: random.Random) -> PooledConnection[T]:
    expires_at = None
    if cfg.max_lifetime is not None:
        expires_at = now + cfg.max_lifetime * (1.0 - cfg.lifetime_jitter * rng.random())
    return PooledConnection(raw=raw, created_at=now, expires_at=expires_at)


def pop_idle(idle: Deque[PooledConnection[T]], strategy: IdleStrategy) -> Optional[PooledConnection[T]]:
    if not idle:
        return None
    return idle.pop() if strategy is IdleStrategy.LIFO else idle.popleft()


def rotation_reason(c: PooledConnection[Any], cfg: PoolConfig, now: float) -> Optional[CloseReason]:
    """Connections that must be retired no matter what (lifetime / use-count rotation)."""
    if c.expires_at is not None and now >= c.expires_at:
        return CloseReason.EXPIRED
    if cfg.max_uses is not None and c.use_count >= cfg.max_uses:
        return CloseReason.MAX_USES
    return None


def idle_too_long(c: PooledConnection[Any], cfg: PoolConfig, now: float) -> bool:
    return cfg.max_idle_time is not None and c.idle_time(now) >= cfg.max_idle_time


def needs_validation(c: PooledConnection[Any], cfg: PoolConfig, now: float) -> bool:
    return cfg.validate_after_idle is not None and c.idle_time(now) >= cfg.validate_after_idle


def borrow_reason(c: PooledConnection[Any], cfg: PoolConfig, now: float) -> Optional[CloseReason]:
    """Static checks applied to an idle connection about to be handed out
    (validation, which needs I/O, is done by the pool afterwards)."""
    reason = rotation_reason(c, cfg, now)
    if reason is None and idle_too_long(c, cfg, now):
        reason = CloseReason.IDLE
    return reason


def sweep(
    idle: Iterable[PooledConnection[T]], total_size: int, cfg: PoolConfig, now: float
) -> Tuple[List[PooledConnection[T]], List[Tuple[PooledConnection[T], CloseReason]]]:
    """Split idle connections into (keep, evict). Rotation victims always go;
    idle-timeout victims go only while the pool stays at or above ``min_size``."""
    keep: List[PooledConnection[T]] = []
    doomed: List[Tuple[PooledConnection[T], CloseReason]] = []
    remaining = total_size
    for c in idle:
        reason = rotation_reason(c, cfg, now)
        if reason is None and idle_too_long(c, cfg, now) and remaining > cfg.min_size:
            reason = CloseReason.IDLE
        if reason is None:
            keep.append(c)
        else:
            doomed.append((c, reason))
            remaining -= 1
    return keep, doomed
