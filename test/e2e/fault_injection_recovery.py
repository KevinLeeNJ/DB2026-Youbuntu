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
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
PORT = 8765

sys.path.insert(0, str(ROOT_DIR / "test" / "protocol"))
from live_wire_protocol_test import COMMAND_OK, ERROR, TRANSACTION_ABORT, WireClient


class SqlClient:
    def __init__(self) -> None:
        self.wire = WireClient(PORT)

    def close(self) -> None:
        self.wire.close()

    def ok(self, statement: str) -> None:
        tag, payload = self.wire.stream_raw(statement)
        if tag == COMMAND_OK and payload == b"":
            return
        diagnostic = payload.decode("utf-8", errors="replace")
        if tag in (ERROR, TRANSACTION_ABORT):
            raise RuntimeError(f"SQL 执行失败: {statement}\n{diagnostic}")
        raise RuntimeError(
            f"SQL 返回了意外的 Wire v3 frame tag={tag}: {statement}\n{diagnostic}"
        )

    def query(self, statement: str) -> list[list[object]]:
        _, rows = self.wire.query(statement)
        return rows


class Server:
    def __init__(self, binary: Path, work_dir: Path, db_name: str) -> None:
        self.binary = binary
        self.work_dir = work_dir
        self.db_name = db_name
        self.proc: subprocess.Popen[bytes] | None = None

    def start(
        self,
        log_name: str,
        point: str | None = None,
        action: str = "abort",
        skip: int = 0,
    ) -> None:
        self._assert_port_free()
        env = os.environ.copy()
        if point is None:
            env.pop("RMDB_FAULT_POINT", None)
            env.pop("RMDB_FAULT_ACTION", None)
            env.pop("RMDB_FAULT_SKIP", None)
        else:
            env["RMDB_FAULT_POINT"] = point
            env["RMDB_FAULT_ACTION"] = action
            if skip > 0:
                env["RMDB_FAULT_SKIP"] = str(skip)
            else:
                env.pop("RMDB_FAULT_SKIP", None)
            # The fault matrix validates the strongest WAL boundary. The
            # regular benchmark remains PROCESS_CRASH by default.
            env["RMDB_DURABILITY_MODE"] = "strict"
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
        client.ok("insert into kv values (2, 10);")
        client.ok("commit;")
    finally:
        client.close()
        server.stop()


def query_values(server: Server) -> dict[int, int]:
    server.start("verify")
    client = SqlClient()
    try:
        rows = client.query("select id, v from kv order by id;")
    finally:
        client.close()
        server.stop()
    if not rows:
        raise RuntimeError("恢复后没有读到 kv")
    return {int(row[0]): int(row[1]) for row in rows}


def query_value(server: Server) -> int:
    values = query_values(server)
    if 1 not in values:
        raise RuntimeError(f"恢复后没有读到 kv.id=1: {values}")
    return values[1]


def commit_case(server: Server, point: str, expected: int) -> None:
    prepare_db(server)
    server.start("fault", point)
    client = SqlClient()
    try:
        try:
            # Use one autocommit statement so the selected fault point always
            # belongs to the transaction being checked, not to BEGIN itself.
            # Two rows make a partially published transaction observable.
            client.ok("begin;")
            client.ok("update kv set v = 20 where id = 1;")
            client.ok("update kv set v = 20 where id = 2;")
            client.ok("commit;")
        except (EOFError, OSError, RuntimeError):
            pass
    finally:
        client.close()
    server.wait_exit()
    actual = query_values(server)
    expected_rows = {1: expected, 2: expected}
    if actual != expected_rows:
        raise RuntimeError(f"{point}: 恢复值为 {actual}，期望 {expected_rows}")


def checkpoint_case(server: Server, point: str, action: str = "abort") -> None:
    prepare_db(server)
    server.start("fault", point, action)
    client = SqlClient()
    try:
        try:
            client.ok("create static_checkpoint;")
        except (EOFError, OSError, RuntimeError):
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


def checkpoint_truncate_case(server: Server) -> None:
    prepare_db(server)
    # Startup recovery resets the WAL once before accepting connections. Skip
    # that occurrence so this case crashes at the explicit checkpoint below.
    server.start("fault", "after_wal_ftruncate", skip=1)
    client = SqlClient()
    try:
        try:
            client.ok("begin;")
            client.ok("update kv set v = v + 1 where id = 1;")
            client.ok("update kv set v = v + 1 where id = 2;")
            client.ok("commit;")
            client.ok("create static_checkpoint;")
        except (EOFError, OSError, RuntimeError):
            pass
    finally:
        client.close()
    server.wait_exit()

    # The first post-checkpoint commit must persist both the new WAL epoch and
    # the preceding file-size change before its successful response.
    server.start("post_truncate_commit")
    client = SqlClient()
    try:
        client.ok("begin;")
        client.ok("update kv set v = v + 10 where id = 1;")
        client.ok("update kv set v = v + 10 where id = 2;")
        client.ok("commit;")
    finally:
        client.close()
        server.kill()

    actual = query_values(server)
    expected = {1: 21, 2: 21}
    if actual != expected:
        raise RuntimeError(
            f"after_wal_ftruncate: 恢复值为 {actual}，期望 {expected}"
        )


def recovery_case(server: Server, point: str) -> None:
    prepare_db(server)
    server.start("dirty")
    client = SqlClient()
    winner = None
    try:
        if point == "mid_recovery_undo":
            # Keep a loser transaction active while a second transaction
            # commits. The winner's WAL flush makes the loser's UPDATE
            # durable enough for recovery undo to exercise the real path.
            client.ok("begin;")
            client.ok("update kv set v = 20 where id = 1;")
            winner = SqlClient()
            winner.ok("begin;")
            winner.ok("update kv set v = 30 where id = 2;")
            winner.ok("commit;")
        else:
            client.ok("begin;")
            client.ok("update kv set v = 20 where id = 1;")
            client.ok("commit;")
    finally:
        client.close()
        if winner is not None:
            winner.close()
    server.kill()

    try:
        server.start("recovery_fault", point)
    except RuntimeError:
        pass
    else:
        server.wait_exit()
    actual = query_values(server)
    expected = {1: 10, 2: 30} if point == "mid_recovery_undo" else {1: 20, 2: 10}
    if actual != expected:
        raise RuntimeError(f"{point}: recovery 后恢复值为 {actual}，期望 {expected}")


def run_case(name: str, binary: Path, root: Path) -> None:
    work_dir = root / name
    work_dir.mkdir()
    server = Server(binary, work_dir, "fault_db")
    if name == "before_commit_wal":
        commit_case(server, name, 10)
    elif name in {"after_commit_log_append", "during_wal_pwrite"}:
        # The COMMIT record has not reached the OS page cache yet.
        commit_case(server, name, 10)
    elif name in {
        "after_commit_wal_write",
        "before_wal_fsync",
        "after_wal_fsync",
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
    elif name == "after_wal_ftruncate":
        checkpoint_truncate_case(server)
    elif name == "before_checkpoint_data_sync_throw":
        checkpoint_case(server, "before_checkpoint_data_sync", "throw")
    elif name in {
        "mid_recovery_redo",
        "mid_recovery_undo",
        "mid_index_rebuild",
        "before_recovery_wal_reset",
    }:
        recovery_case(server, name)
    else:
        raise ValueError(name)


CASES = [
    "before_commit_wal",
    "after_commit_log_append",
    "during_wal_pwrite",
    "after_commit_wal_write",
    "before_wal_fsync",
    "after_wal_fsync",
    "after_commit_wal_sync",
    "before_tuple_publication",
    "mid_tuple_publication",
    "after_tuple_publication",
    "before_published_csn_store",
    "before_checkpoint_data_sync",
    "after_checkpoint_data_sync",
    "before_wal_truncate",
    "after_wal_ftruncate",
    "before_checkpoint_data_sync_throw",
    "mid_recovery_redo",
    "mid_recovery_undo",
    "mid_index_rebuild",
    "before_recovery_wal_reset",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary", type=Path, default=ROOT_DIR / "build" / "bin" / "rmdb"
    )
    parser.add_argument(
        "--case", choices=CASES, action="append", help="只运行指定 case，可重复传入"
    )
    parser.add_argument(
        "--repeat", type=int, default=1, help="每个 case 使用独立数据库重复执行的次数"
    )
    args = parser.parse_args()
    if not args.binary.is_file():
        parser.error(f"找不到 rmdb: {args.binary}")
    if args.repeat < 1:
        parser.error("--repeat 必须大于 0")
    selected = args.case or CASES
    for repetition in range(1, args.repeat + 1):
        with tempfile.TemporaryDirectory(prefix="rmdb-fault-matrix-") as temp_dir:
            root = Path(temp_dir)
            for name in selected:
                started = time.monotonic()
                run_case(name, args.binary.resolve(), root)
                print(
                    f"PASS repetition={repetition} {name} "
                    f"({time.monotonic() - started:.2f}s)"
                )
    print(
        f"{len(selected) * args.repeat}/{len(selected) * args.repeat} fault-injection cases passed"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
