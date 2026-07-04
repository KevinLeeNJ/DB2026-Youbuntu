from __future__ import annotations

import sqlite3

from benchmark.tpcc.core.backend import Backend, BackendError


class SqliteBackend(Backend):
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.execute("pragma journal_mode=WAL;")
        self.conn.execute("pragma synchronous=NORMAL;")

    def execute(self, sql: str) -> str:
        try:
            cursor = self.conn.execute(sql)
        except sqlite3.DatabaseError as exc:
            raise BackendError(str(exc)) from exc
        rows = cursor.fetchall()
        if not rows:
            return ""
        return "\n".join(
            "|".join("" if value is None else str(value) for value in row)
            for row in rows
        )

    def begin(self) -> None:
        self.conn.execute("begin;")

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def close(self) -> None:
        self.conn.close()
