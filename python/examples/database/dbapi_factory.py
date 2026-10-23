"""One factory for ANY relational database.

Every mainstream Python driver implements PEP 249 (DB-API 2.0): sqlite3, psycopg,
PyMySQL, mysqlclient, oracledb, pyodbc, ... They all expose ``connect()``, ``cursor()``,
``rollback()`` and ``close()``, so a single adapter is enough. The pool itself knows
nothing about databases; this file lives in examples/ precisely because it is *client* code.
"""
from typing import Any, Callable, Tuple, Type

from connpool import ConnectionFactory


class DbApiFactory(ConnectionFactory[Any]):
    def __init__(
        self,
        connect: Callable[[], Any],
        *,
        ping_sql: str = "SELECT 1",                      # Oracle needs "SELECT 1 FROM DUAL"
        fatal_errors: Tuple[Type[BaseException], ...] = (),
    ) -> None:
        self._connect, self._ping_sql, self._fatal = connect, ping_sql, fatal_errors

    def create(self) -> Any:
        return self._connect()

    def validate(self, conn: Any) -> bool:
        cur = conn.cursor()
        try:
            cur.execute(self._ping_sql)
            cur.fetchall()
            return True
        finally:
            cur.close()

    def reset(self, conn: Any) -> None:
        conn.rollback()                                  # never leak an open transaction

    def is_fatal_error(self, exc: BaseException) -> bool:
        # Optional fast path. Without it the pool still validates the connection after
        # any error and discards it if the ping fails.
        return isinstance(exc, self._fatal)
