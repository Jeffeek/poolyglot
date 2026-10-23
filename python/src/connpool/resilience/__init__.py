"""Failure handling: circuit breaker and retry/backoff. Depends on ``core`` only."""
from connpool.resilience.breaker import BreakerConfig, BreakerState, CircuitBreaker
from connpool.resilience.retry import NO_RETRY, RetryPolicy

__all__ = ["BreakerConfig", "BreakerState", "CircuitBreaker", "NO_RETRY", "RetryPolicy"]
