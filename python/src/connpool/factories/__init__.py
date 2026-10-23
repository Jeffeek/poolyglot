"""The seam to the outside world: what users implement to pool *any* kind of connection.
Has no dependency on the rest of connpool."""
from connpool.factories.async_factory import AsyncCallableFactory, AsyncConnectionFactory
from connpool.factories.sync_factory import CallableFactory, ConnectionFactory

__all__ = ["AsyncCallableFactory", "AsyncConnectionFactory", "CallableFactory", "ConnectionFactory"]
