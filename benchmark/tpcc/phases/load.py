from __future__ import annotations

import os
from pathlib import Path

from benchmark.tpcc.core.backend import Backend

TABLES = [
    "warehouse",
    "district",
    "customer",
    "history",
    "new_orders",
    "orders",
    "order_line",
    "item",
    "stock",
]


def execute_sql_file(backend: Backend, path: Path) -> None:
    for statement in path.read_text().split(";"):
        sql = statement.strip()
        if sql:
            backend.execute(sql + ";")


def rmdb_load_path(path: Path, relative_to: Path | None = None) -> str:
    path_to_emit = Path(path)
    if relative_to is not None:
        path_to_emit = Path(os.path.relpath(path_to_emit, relative_to))
    text = path_to_emit.as_posix()
    if path_to_emit.is_absolute() or text.startswith("./") or text.startswith("../"):
        return text
    return f"./{text}"


def load_all(
    backend: Backend, data_dir: Path, schema_dir: Path, backend_name: str, rmdb_db_dir: Path | None = None
) -> None:
    if backend_name == "sqlite":
        execute_sql_file(backend, schema_dir / "sqlite_schema.sql")
        execute_sql_file(backend, schema_dir / "sqlite_indexes.sql")
    else:
        execute_sql_file(backend, schema_dir / "rmdb_schema.sql")
        execute_sql_file(backend, schema_dir / "rmdb_indexes.sql")
    for table in TABLES:
        if backend_name == "sqlite":
            raise RuntimeError("sqlite load must use --sqlite-import")
        backend.execute(
            f"load {rmdb_load_path(data_dir / (table + '.csv'), relative_to=rmdb_db_dir)} into {table};"
        )
    if backend_name == "rmdb":
        backend.execute("set output_file off")
