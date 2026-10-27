"""Retry policy for connection creation: exponential backoff with jitter."""
from __future__ import annotations

import random
from dataclasses import dataclass

from connpool.core.errors import PoolConfigError


@dataclass(frozen=True)
class RetryPolicy:
    """How to retry ``factory.create()``.

    ``jitter`` in [0, 1]: 0 = deterministic delays, 1 = "full jitter"
    (uniform in [0, delay]). Spreading retries avoids thundering herds.
    ``retry_on`` narrows what is retryable (e.g. don't retry auth failures).
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 5.0
    multiplier: float = 2.0
    jitter: float = 0.5
    retry_on: tuple[type[BaseException], ...] = (Exception,)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise PoolConfigError("retry.max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0 or self.multiplier < 1:
            raise PoolConfigError("retry delays must be >= 0 and multiplier >= 1")
        if not 0.0 <= self.jitter <= 1.0:
            raise PoolConfigError("retry.jitter must be within [0, 1]")

    def should_retry(self, exc: BaseException) -> bool:
        return isinstance(exc, self.retry_on)

    def delay_for(self, failed_attempts: int, rng: random.Random) -> float:
        """Delay before the next attempt, given how many attempts have failed so far."""
        ceiling = (
            0.0
            if self.base_delay == 0.0
            else min(self.max_delay, self.base_delay * self.multiplier ** (failed_attempts - 1))
        )
        return ceiling * (1.0 - self.jitter + self.jitter * rng.random())


NO_RETRY = RetryPolicy(max_attempts=1)
