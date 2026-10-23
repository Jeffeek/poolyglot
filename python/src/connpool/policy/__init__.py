"""Configuration and the pure rules that decide when a connection must retire.
Depends on ``core`` and ``resilience``."""
from connpool.policy.config import IdleStrategy, PoolConfig

__all__ = ["IdleStrategy", "PoolConfig"]
