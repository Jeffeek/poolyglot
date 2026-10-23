"""Pool configuration: one immutable, validated object."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field, replace
from typing import Any, Optional

from connpool.resilience.breaker import BreakerConfig
from connpool.core.errors import PoolConfigError
from connpool.resilience.retry import RetryPolicy


class IdleStrategy(str, enum.Enum):
    LIFO = "lifo"  # reuse the hottest connection; cold ones age out and get evicted
    FIFO = "fifo"  # reuse the oldest idle connection; spreads load / rotates evenly


@dataclass(frozen=True)
class PoolConfig:
    # --- sizing ---
    min_size: int = 0                       # kept warm by open() and the maintenance pass
    max_size: int = 10                      # hard cap (idle + in use + being created)

    # --- timeouts (seconds; None = no limit) ---
    acquire_timeout: Optional[float] = 30.0   # default wait in acquire()
    create_timeout: Optional[float] = 10.0    # enforced by AsyncConnectionPool only; sync
                                              # factories must enforce their own connect timeout
    max_waiters: Optional[int] = None         # backpressure: cap callers blocked in acquire()
                                              # (None = unlimited; 0 = never wait when full)

    # --- rotation / recycling ---
    max_lifetime: Optional[float] = 1800.0  # retire connections after this age
    lifetime_jitter: float = 0.1            # shave up to 10% off each lifetime so a
                                            # whole pool never expires at the same instant
    max_idle_time: Optional[float] = 600.0  # evict idle connections (down to min_size)
    max_uses: Optional[int] = None          # retire after N checkouts
    idle_strategy: IdleStrategy = IdleStrategy.LIFO

    # --- health ---
    validate_after_idle: Optional[float] = 30.0  # validate on checkout if idle >= this
                                                 # (0 = always, None = never)
    maintenance_interval: Optional[float] = 30.0  # background sweep period (None = off)
    leak_detection_timeout: Optional[float] = None  # warn when a borrowed connection is held
                                                    # longer than this (None = off); needs open()
    validate_idle: bool = False             # validate idle connections during the maintenance
                                            # sweep and evict dead ones (needs open(); adds I/O)

    # --- resilience ---
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    breaker: Optional[BreakerConfig] = field(default_factory=BreakerConfig)  # None = off

    def __post_init__(self) -> None:
        def need(ok: bool, msg: str) -> None:
            if not ok:
                raise PoolConfigError(msg)

        need(self.max_size >= 1, "max_size must be >= 1")
        need(0 <= self.min_size <= self.max_size, "min_size must be within [0, max_size]")
        need(self.acquire_timeout is None or self.acquire_timeout >= 0, "acquire_timeout must be >= 0")
        for name in ("create_timeout", "max_lifetime", "max_idle_time", "maintenance_interval",
                     "leak_detection_timeout"):
            value = getattr(self, name)
            need(value is None or value > 0, f"{name} must be > 0 or None")
        need(self.validate_after_idle is None or self.validate_after_idle >= 0,
             "validate_after_idle must be >= 0 or None")
        need(0.0 <= self.lifetime_jitter <= 1.0, "lifetime_jitter must be within [0, 1]")
        need(self.max_uses is None or self.max_uses >= 1, "max_uses must be >= 1 or None")
        need(self.max_waiters is None or self.max_waiters >= 0, "max_waiters must be >= 0 or None")

    def replace(self, **changes: Any) -> "PoolConfig":
        """Return a modified copy (the config itself is immutable)."""
        return replace(self, **changes)
