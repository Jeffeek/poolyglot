"""HTTP keep-alive connections, one pool per host (needs network; illustration only)."""
import http.client

from connpool import ConnectionFactory, PoolConfig, PoolRegistry


class HttpsFactory(ConnectionFactory[http.client.HTTPSConnection]):
    def __init__(self, host):
        self.host = host

    def create(self):
        conn = http.client.HTTPSConnection(self.host, timeout=5)
        conn.connect()                       # fail in create(), not in the caller's request
        return conn

    def validate(self, conn):
        return conn.sock is not None         # server hasn't closed the keep-alive socket

    def is_fatal_error(self, exc):
        return isinstance(exc, (http.client.HTTPException, ConnectionError, TimeoutError))


hosts = PoolRegistry(HttpsFactory, PoolConfig(max_size=8, max_lifetime=300, validate_after_idle=5))

if __name__ == "__main__":
    with hosts:
        with hosts.connection("example.com") as conn:
            conn.request("GET", "/")
            print(conn.getresponse().status)
