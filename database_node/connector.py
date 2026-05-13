from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, List, Optional

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

    @staticmethod
    def _split_sql_statements(sql: str) -> List[str]:
        """Split a schema SQL file into executable statements.

        mysql-connector-python 9.x / C extension cursors do not support
        cursor.execute(..., multi=True).  The schema used by this package is
        simple DDL, so splitting on semicolons outside strings/comments is
        enough and works with both CMySQLCursor and pure Python cursors.
        """
        statements: List[str] = []
        current: List[str] = []
        quote: Optional[str] = None
        escape = False
        in_line_comment = False
        in_block_comment = False
        i = 0

        while i < len(sql):
            ch = sql[i]
            nxt = sql[i + 1] if i + 1 < len(sql) else ''

            if in_line_comment:
                if ch == '\n':
                    in_line_comment = False
                    current.append(ch)
                i += 1
                continue

            if in_block_comment:
                if ch == '*' and nxt == '/':
                    in_block_comment = False
                    i += 2
                else:
                    i += 1
                continue

            if quote is None:
                if ch == '-' and nxt == '-':
                    in_line_comment = True
                    i += 2
                    continue
                if ch == '#':
                    in_line_comment = True
                    i += 1
                    continue
                if ch == '/' and nxt == '*':
                    in_block_comment = True
                    i += 2
                    continue
                if ch in ("'", '"', '`'):
                    quote = ch
                    current.append(ch)
                    i += 1
                    continue
                if ch == ';':
                    statement = ''.join(current).strip()
                    if statement:
                        statements.append(statement)
                    current = []
                    i += 1
                    continue
            else:
                current.append(ch)
                if escape:
                    escape = False
                    i += 1
                    continue
                if ch == '\\' and quote != '`':
                    escape = True
                    i += 1
                    continue
                if ch == quote:
                    quote = None
                i += 1
                continue

            current.append(ch)
            i += 1

        statement = ''.join(current).strip()
        if statement:
            statements.append(statement)
        return statements

    def initialize_schema(self, schema_path: str) -> bool:
        self.ensure_connection()
        if self.cur is None or self.conn is None:
            return False

        sql = Path(schema_path).read_text(encoding='utf-8')
        statements = self._split_sql_statements(sql)
        if not statements:
            self._log("warning", f"[DB] Schema file has no SQL statements: {schema_path}")
            return True

        try:
            for statement in statements:
                self.cur.execute(statement)
            self.conn.commit()
            self._log("info", f"[DB] Schema initialized from: {schema_path} ({len(statements)} statements).")
            return True
        except Error as exc:
            self._log("error", f"[DB] Schema initialization failed: {exc}")
            self.conn.rollback()
            return False

    def close(self) -> None:
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None and self.conn.is_connected():
            self.conn.close()
            self._log("info", "[DB] Connection returned to pool.")

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
