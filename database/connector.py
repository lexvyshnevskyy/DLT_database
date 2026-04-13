from __future__ import annotations

from typing import Any, Iterable, Optional

import mysql.connector
from mysql.connector import Error, pooling


class DbConnector:
    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 3306,
        user: str = "ubuntu",
        password: str = "raspberry",
        database: str = "exp",
        pool_name: str = "DbConnector",
        pool_size: int = 5,
        logger=None,
    ) -> None:
        self.logger = logger
        self.config = {
            "host": host,
            "port": int(port),
            "user": user,
            "password": password,
            "database": database,
        }

        self.pool = pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            **self.config,
        )
        self.conn = None
        self.cur = None
        self.connect()

    def _log(self, level: str, message: str) -> None:
        if self.logger is None:
            print(message)
            return
        log_fn = getattr(self.logger, level, None)
        if callable(log_fn):
            log_fn(message)
        else:
            self.logger.info(message)

    def connect(self) -> None:
        try:
            self.conn = self.pool.get_connection()
            self.cur = self.conn.cursor()
            self.cur.execute(f"USE `{self.config['database']}`")
            self._log(
                "info",
                f"[DB] Connection fetched from pool '{self.pool.pool_name}' (size: {self.pool.pool_size}).",
            )
        except Error as exc:
            self._log("error", f"[DB] Connection failed: {exc}")
            self.conn = None
            self.cur = None

    def ensure_connection(self) -> None:
        try:
            if self.conn is None or not self.conn.is_connected():
                self._log("warning", "[DB] Connection lost; fetching a new one from pool.")
                if self.conn is not None:
                    self.conn.close()
                self.connect()
        except Error:
            self._log("warning", "[DB] Connection check failed; reconnecting.")
            self.connect()

    def execute(self, query: str, params: Optional[Iterable[Any]] = None) -> bool:
        self.ensure_connection()
        if self.cur is None or self.conn is None:
            return False
        try:
            self.cur.execute(query, params or ())
            self.conn.commit()
            return True
        except Error as exc:
            self._log("error", f"[DB] Query error: {exc}")
            return False

    def fetchall(self):
        if self.cur is None:
            return []
        try:
            return self.cur.fetchall()
        except Error as exc:
            self._log("error", f"[DB] Fetch error: {exc}")
            return []

    def close(self) -> None:
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None and self.conn.is_connected():
            self.conn.close()
            self._log("info", "[DB] Connection returned to pool.")

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
