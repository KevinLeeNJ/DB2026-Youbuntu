#!/usr/bin/env python3
"""Run the opt-in process-level crash matrix for WAL and checkpoint recovery.

This is intentionally a standalone diagnostic runner.  It is not registered with
CMake/ctest because each case starts and kills a real rmdb process.

The binary must be built with RMDB_ENABLE_FAULT_INJECTION defined, for example in
a separate build tree with CXXFLAGS=-DRMDB_ENABLE_FAULT_INJECTION=1.
"""

from __future__ import annotations

import argparse
import os
import re
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
PORT = 8765


class SqlClient:
    def __init__(self) -> None:
        self.sock = socket.create_connection(("127.0.0.1", PORT), timeout=10)
        self.sock.settimeout(10)

    def close(self) -> None:
        self.sock.close()

    def sql(self, statement: str) -> str:
        self.sock.sendall(statement.encode("utf-8") + b"\0")
        chunks = []
        while True:
            data = self.sock.recv(8192)
            if not data:
                raise RuntimeError(f"server 在回复前关闭连接: {statement}")
            chunks.append(data)
            if b"\0" in data:
                return (
                    b"".join(chunks)
                    .split(b"\0", 1)[0]
                    .decode("utf-8", errors="replace")
                )

    def ok(self, statement: str) -> str:
        reply = self.sql(statement)
        if any(word in reply.lower() for word in ("failure", "error", "abort")):
            raise RuntimeError(f"SQL 执行失败: {statement}\n{reply}")
        return reply


class Server:
    def __init__(self, binary: Path, work_dir: Path, db_name: str) -> None:
        self.binary = binary
        self.work_dir = work_dir
        self.db_name = db_name
        self.proc: subprocess.Popen[bytes] | None = None

    def start(
        self, log_name: str, point: str | None = None, action: str = "abort"
    ) -> None:
        self._assert_port_free()
        env = os.environ.copy()
        if point is None:
            env.pop("RMDB_FAULT_POINT", None)
            env.pop("RMDB_FAULT_ACTION", None)
        else:
            env["RMDB_FAULT_POINT"] = point
            env["RMDB_FAULT_ACTION"] = action
        out = open(self.work_dir / f"{log_name}.out", "wb")
        err = open(self.work_dir / f"{log_name}.err", "wb")
        self.proc = subprocess.Popen(
            [str(self.binary), self.db_name],
            cwd=self.work_dir,
            stdout=out,
            stderr=err,
            env=env,
        )
        self._wait_for_port()

    def wait_exit(self) -> int:
        if self.proc is None:
            raise RuntimeError("server 尚未启动")
        return self.proc.wait(timeout=10)

    def kill(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)

    def stop(self) -> None:
        if self.proc is None or self.proc.poll() is not None:
            return
        self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            self.proc.wait(timeout=5)

    @staticmethod
    def _assert_port_free() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.2)
        try:
            sock.connect(("127.0.0.1", PORT))
        except OSError:
            return
        finally:
            sock.close()
        raise RuntimeError(f"端口 {PORT} 已被占用")

    @staticmethod
    def _wait_for_port() -> None:
        deadline = time.monotonic() + 10
        last_error: OSError | None = None
        while time.monotonic() < deadline:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.2)
            try:
                sock.connect(("127.0.0.1", PORT))
                return
            except OSError as exc:
                last_error = exc
                time.sleep(0.01)
            finally:
                sock.close()
        raise RuntimeError(f"server 未在端口 {PORT} 就绪: {last_error}")


def prepare_db(server: Server) -> None:
    server.start("prepare")
    client = SqlClient()
    try:
        client.ok("create table kv (id int, v int);")
        client.ok("create index kv(id);")
        client.ok("begin;")
        client.ok("insert into kv values (1, 10);")
        client.ok("commit;")
    finally:
        client.close()
        server.stop()


def query_value(server: Server) -> int:
    server.start("verify")
    client = SqlClient()
    try:
        reply = client.ok("select v from kv where id = 1;")
    finally:
        client.close()
        server.stop()
    values = re.findall(r"\|\s*(-?\d+)\s*\|", reply)
    if not values:
        raise RuntimeError(f"恢复后没有读到 kv.id=1: {reply}")
    return int(values[0])


def commit_case(server: Server, point: str, expected: int) -> None:
    prepare_db(server)
    server.start("fault", point)
    client = SqlClient()
    try:
        client.ok("begin;")
        client.ok("update kv set v = 20 where id = 1;")
        try:
            client.ok("commit;")
        except (OSError, RuntimeError):
            pass
    finally:
        client.close()
    server.wait_exit()
    actual = query_value(server)
    if actual != expected:
        raise RuntimeError(f"{point}: 恢复值为 {actual}，期望 {expected}")


def checkpoint_case(server: Server, point: str, action: str = "abort") -> None:
    prepare_db(server)
    server.start("fault", point, action)
    client = SqlClient()
    try:
        try:
            client.ok("create static_checkpoint;")
        except (OSError, RuntimeError):
            pass
    finally:
        client.close()
    if action == "throw":
        server.stop()
    else:
        server.wait_exit()
    actual = query_value(server)
    if actual != 10:
        raise RuntimeError(f"{point}/{action}: 恢复值为 {actual}，期望 10")


def recovery_case(server: Server, point: str) -> None:
    prepare_db(server)
    server.start("dirty")
    client = SqlClient()
    try:
        client.ok("begin;")
        client.ok("update kv set v = 20 where id = 1;")
        client.ok("commit;")
    finally:
        client.close()
    server.kill()

    try:
        server.start("recovery_fault", point)
    except RuntimeError:
        pass
    else:
        server.wait_exit()
    actual = query_value(server)
    if actual != 20:
        raise RuntimeError(f"{point}: redo 后恢复值为 {actual}，期望 20")


def run_case(name: str, binary: Path, root: Path) -> None:
    work_dir = root / name
    work_dir.mkdir()
    server = Server(binary, work_dir, "fault_db")
    if name == "before_commit_wal":
        commit_case(server, name, 10)
    elif name in {
        "after_commit_wal_write",
        "after_commit_wal_sync",
        "before_tuple_publication",
        "mid_tuple_publication",
        "after_tuple_publication",
        "before_published_csn_store",
    }:
        commit_case(server, name, 20)
    elif name in {
        "before_checkpoint_data_sync",
        "after_checkpoint_data_sync",
        "before_wal_truncate",
    }:
        checkpoint_case(server, name)
    elif name == "before_checkpoint_data_sync_throw":
        checkpoint_case(server, "before_checkpoint_data_sync", "throw")
    elif name in {"mid_recovery_redo", "mid_index_rebuild"}:
        recovery_case(server, name)
    else:
        raise ValueError(name)


CASES = [
    "before_commit_wal",
    "after_commit_wal_write",
    "after_commit_wal_sync",
    "before_tuple_publication",
    "mid_tuple_publication",
    "after_tuple_publication",
    "before_published_csn_store",
    "before_checkpoint_data_sync",
    "after_checkpoint_data_sync",
    "before_wal_truncate",
    "before_checkpoint_data_sync_throw",
    "mid_recovery_redo",
    "mid_index_rebuild",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary", type=Path, default=ROOT_DIR / "build" / "bin" / "rmdb"
    )
    parser.add_argument(
        "--case", choices=CASES, action="append", help="只运行指定 case，可重复传入"
    )
    args = parser.parse_args()
    if not args.binary.is_file():
        parser.error(f"找不到 rmdb: {args.binary}")
    selected = args.case or CASES
    with tempfile.TemporaryDirectory(prefix="rmdb-fault-matrix-") as temp_dir:
        root = Path(temp_dir)
        for name in selected:
            started = time.monotonic()
            run_case(name, args.binary.resolve(), root)
            print(f"PASS {name} ({time.monotonic() - started:.2f}s)")
    print(f"{len(selected)}/{len(selected)} fault-injection cases passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
