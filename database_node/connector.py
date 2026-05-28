from __future__ import annotations

import threading
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
            pool_size: int = 10,
            logger=None,
    ) -> None:
        self.logger = logger
        self.config = {
            "host": host,
            "port": int(port),
            "user": user,
            "password": password,
            "database": database,
            # Important for the query service: every SELECT must see the latest
            # committed measurements instead of staying in an old InnoDB snapshot.
            "autocommit": True,
        }
        self._local = threading.local()
        self._all_connections_lock = threading.Lock()
        self._all_connections = []

        self.pool = pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            **self.config,
        )
        self.connect()

    @property
    def conn(self):
        self.ensure_connection()
        return self._local.conn

    @property
    def cur(self):
        self.ensure_connection()
        return self._local.cur

    def _log(self, level: str, message: str) -> None:
        """Log DB helper messages without ever breaking query execution.

        rclpy/rcutils can raise "Logger severity cannot be changed between calls"
        when one helper function dynamically calls logger.info(), logger.warning(),
        logger.error(), etc. from the same Python call site.  This connector is
        used while opening per-thread DB connections, so a logging exception here
        can make a perfectly valid SQL request fail before it reaches MySQL.

        Keep one fixed ROS severity at this call site and put the intended level
        into the message text.  If the ROS logger itself still fails for any
        reason, fall back to stdout and continue.
        """
        level_name = str(level or "info").upper()
        text = message if level_name == "INFO" else f"[{level_name}] {message}"

        if self.logger is None:
            print(text, flush=True)
            return

        try:
            # Always use the same severity from this source line.
            self.logger.info(text)
        except Exception:
            print(text, flush=True)

    def _close_local(self) -> None:
        cur = getattr(self._local, 'cur', None)
        conn = getattr(self._local, 'conn', None)
        if cur is not None:
            try:
                cur.close()
            except Error:
                pass
        if conn is not None:
            try:
                if conn.is_connected():
                    conn.close()
            except Error:
                pass
        self._local.cur = None
        self._local.conn = None

    def connect(self) -> None:
        try:
            conn = self.pool.get_connection()
            # Buffered cursor prevents "Unread result found" when a handler uses fetchone()
            # and later reuses the same thread-local connection for another query.
            cur = conn.cursor(buffered=True)
            cur.execute(f"USE `{self.config['database']}`")
            self._local.conn = conn
            self._local.cur = cur
            with self._all_connections_lock:
                self._all_connections.append(conn)
            self._log(
                "info",
                f"[DB] Thread-local connection fetched from pool '{self.pool.pool_name}' "
                f"(size: {self.pool.pool_size}).",
            )
        except Error as exc:
            self._log("error", f"[DB] Connection failed: {exc}")
            self._local.conn = None
            self._local.cur = None

    def ensure_connection(self) -> None:
        conn = getattr(self._local, 'conn', None)
        try:
            if conn is None or not conn.is_connected():
                self._log("warning", "[DB] Thread-local connection lost; fetching a new one from pool.")
                self._close_local()
                self.connect()
        except Error:
            self._log("warning", "[DB] Connection check failed; reconnecting.")
            self._close_local()
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
            try:
                self.conn.rollback()
            except Error:
                pass
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
        # Close current thread-local connection first, then any worker thread
        # connections still tracked by this connector.
        self._close_local()
        with self._all_connections_lock:
            connections = list(self._all_connections)
            self._all_connections.clear()
        for conn in connections:
            try:
                if conn.is_connected():
                    conn.close()
            except Error:
                pass
        self._log("info", "[DB] Connections returned to pool.")

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
