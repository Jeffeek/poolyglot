"""HTTP keep-alive connections, one pool per host (needs network; illustration only).

Why per-host pooling?
- Avoids head-of-line blocking: one slow host won't starve requests to fast hosts
- Each host gets its own connection limit (max_size per pool)
- PoolRegistry creates pools lazily on first use

Pattern: PoolRegistry keyed by hostname
- Key = hostname (e.g., "api.github.com", "example.com")
- Value = pool of HTTPS connections to that host
- Config is shared across all hosts (max_size=8 per host)
"""
import http.client

from connpool import ConnectionFactory, PoolConfig, PoolRegistry


class HttpsFactory(ConnectionFactory[http.client.HTTPSConnection]):
    """Factory for HTTPS connections to a specific host."""

    def __init__(self, host):
        self.host = host

    def create(self):
        conn = http.client.HTTPSConnection(self.host, timeout=5)
        conn.connect()  # Fail in create(), not in the caller's request
        return conn

    def validate(self, conn):
        return conn.sock is not None  # Server hasn't closed the keep-alive socket

    def is_fatal_error(self, exc):
        return isinstance(exc, (http.client.HTTPException, ConnectionError, TimeoutError))


# Create registry: one pool per host, shared config
hosts = PoolRegistry(HttpsFactory, PoolConfig(max_size=8, max_lifetime=300, validate_after_idle=5))

if __name__ == "__main__":
    with hosts:
        # First request to example.com creates a pool lazily
        with hosts.connection("example.com") as conn:
            conn.request("GET", "/")
            status = conn.getresponse().status
            print(f"example.com: HTTP {status}")

        # Pool stats (per-host)
        pool = hosts.pool("example.com")
        stats = pool.stats()
        print(f"\nPool stats for example.com:")
        print(f"  Size: {stats['size']}/{stats['max_size']}")
        print(f"  Created: {stats['created']}, Closed: {stats['closed']}")
        print(f"  Acquire wait p95: {stats['acquire_wait_p95']:.3f}s")
