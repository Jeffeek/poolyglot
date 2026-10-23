"""Runnable demo of DbApiFactory using sqlite3 (standard library, so no install needed).
SQLite is used purely as a convenient stand-in: swap the connect lambda for any other
driver (see postgres_demo.py) and nothing else changes.

    PYTHONPATH=src python examples/database/sqlite_demo.py
"""
import os
import sqlite3
import tempfile
from concurrent.futures import ThreadPoolExecutor

from connpool import ConnectionPool, PoolConfig
from dbapi_factory import DbApiFactory

path = os.path.join(tempfile.mkdtemp(), "demo.db")
factory = DbApiFactory(lambda: sqlite3.connect(path, timeout=5, check_same_thread=False))

with ConnectionPool(factory, PoolConfig(min_size=2, max_size=4, max_uses=500), name="sqlite") as pool:
    with pool.connection() as db:
        db.execute("CREATE TABLE hits (n INTEGER)")
        db.commit()

    def work(i):
        with pool.connection() as db:
            db.execute("INSERT INTO hits VALUES (?)", (i,))
            db.commit()

    with ThreadPoolExecutor(16) as ex:
        list(ex.map(work, range(200)))

    with pool.connection() as db:
        print("rows:", db.execute("SELECT COUNT(*) FROM hits").fetchone()[0])
    print(pool.stats())
