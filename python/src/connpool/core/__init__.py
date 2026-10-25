"""Foundation layer: errors, clocks, connection bookkeeping, timeout helpers.
Depends on nothing else in connpool."""
from connpool.core.clock import Clock, ManualClock, SystemClock
from connpool.core.connection import CloseReason, PooledConnection
from connpool.core.errors import (
    CircuitOpenError,
    ConnectionCreateError,
    InvalidReleaseError,
    PoolClosedError,
    PoolConfigError,
    PoolError,
    PoolExhaustedError,
    PoolTimeoutError,
)

__all__ = [
    "CircuitOpenError", "Clock", "CloseReason", "ConnectionCreateError", "InvalidReleaseError",
    "ManualClock", "PoolClosedError", "PoolConfigError", "PoolError", "PoolExhaustedError",
    "PoolTimeoutError", "PooledConnection", "SystemClock",
]
