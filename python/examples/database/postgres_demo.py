"""Same DbApiFactory, different database (needs `pip install psycopg` and a server;
shown for illustration, not executed in the test suite).

MySQL:   DbApiFactory(lambda: pymysql.connect(host=..., connect_timeout=5))
Oracle:  DbApiFactory(lambda: oracledb.connect(...), ping_sql="SELECT 1 FROM DUAL")
"""
import psycopg

from connpool import ConnectionPool, PoolConfig, RetryPolicy
from dbapi_factory import DbApiFactory

factory = DbApiFactory(
    lambda: psycopg.connect("postgresql://app:secret@db.internal/app", connect_timeout=5),
    fatal_errors=(psycopg.OperationalError, psycopg.InterfaceError),
)
config = PoolConfig(
    min_size=2, max_size=20, max_lifetime=1800,
    retry=RetryPolicy(max_attempts=4, base_delay=0.25, retry_on=(psycopg.OperationalError,)),
)

if __name__ == "__main__":
    with ConnectionPool(factory, config, name="postgres") as pool:
        with pool.connection() as conn:
            print(conn.execute("SELECT version()").fetchone())
