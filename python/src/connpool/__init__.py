"""connpool - a generic connection pool for Python (sync + asyncio), standard library only.

Layers (each only depends on the ones above it):

    core           errors, clocks, connection bookkeeping
    resilience     circuit breaker, retry/backoff
    policy         PoolConfig + the pure retirement rules
    factories      ConnectionFactory: the seam users implement to pool anything
    observability  listeners + stats
    pools          ConnectionPool, AsyncConnectionPool, PoolRegistry

This module is the *public API facade*: everything users need can be imported straight
from ``connpool`` so that internal modules can move without breaking anyone.
"""
from connpool.core import (
    CircuitOpenError,
    Clock,
    CloseReason,
    ConnectionCreateError,
    InvalidReleaseError,
    ManualClock,
    PoolClosedError,
    PoolConfigError,
    PooledConnection,
    PoolError,
    PoolExhaustedError,
    PoolTimeoutError,
    SystemClock,
)
from connpool.factories import (
    AsyncCallableFactory,
    AsyncConnectionFactory,
    CallableFactory,
    ConnectionFactory,
)
from connpool.observability import PoolListener, PoolStats
from connpool.policy import IdleStrategy, PoolConfig
from connpool.pools import AsyncConnectionPool, AsyncPoolRegistry, ConnectionPool, PoolRegistry
from connpool.resilience import (
    NO_RETRY,
    BreakerConfig,
    BreakerState,
    CircuitBreaker,
    RetryPolicy,
)

__version__ = "1.0.0"

__all__ = [
    # pools
    "AsyncConnectionPool", "AsyncPoolRegistry", "ConnectionPool", "PoolRegistry",
    # configuration
    "BreakerConfig", "IdleStrategy", "NO_RETRY", "PoolConfig", "RetryPolicy",
    # what you implement
    "AsyncCallableFactory", "AsyncConnectionFactory", "CallableFactory", "ConnectionFactory",
    # observability
    "BreakerState", "CloseReason", "PoolListener", "PoolStats", "PooledConnection",
    # errors
    "CircuitOpenError", "ConnectionCreateError", "InvalidReleaseError", "PoolClosedError",
    "PoolConfigError", "PoolError", "PoolExhaustedError", "PoolTimeoutError",
    # standalone building blocks / testing
    "CircuitBreaker", "Clock", "ManualClock", "SystemClock",
]
