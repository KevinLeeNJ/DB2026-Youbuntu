#!/usr/bin/env python3
"""真实 server 级别的 checkpoint/crash recovery 近似测试矩阵。"""

from __future__ import annotations

import argparse
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
import struct

ROOT_DIR = Path(__file__).resolve().parents[2]
SERVER_BIN = ROOT_DIR / "build" / "bin" / "rmdb"
PORT = 8765

# Reuse the black-box Wire v3 codec so this recovery matrix exercises the same
# handshake and frame contract as the live protocol suite.
sys.path.insert(0, str(ROOT_DIR / "test" / "protocol"))
import live_wire_protocol_test as wire  # noqa: E402


class SqlClient(wire.WireClient):
    def __init__(self, port: int = PORT):
        super().__init__(port)

    def sql(self, statement: str, expect_response: bool = True) -> str:
        self.write_frame(wire.EXEC_STREAM, statement.encode("utf-8"))
        if not expect_response:
            if statement.strip().lower() == "crash":
                LiveServer.crash_active()
            return ""

        tag, payload = self.read_frame()
        if tag == wire.COMMAND_OK:
            wire.require(payload == b"", "COMMAND_OK carried a payload")
            return "COMMAND_OK"
        if tag in (wire.ERROR, wire.TRANSACTION_ABORT):
            kind = "TRANSACTION_ABORT" if tag == wire.TRANSACTION_ABORT else "ERROR"
            detail = payload.decode("utf-8", "replace")
            raise AssertionError(f"{kind}: {statement}\n{detail}")
        wire.require(tag == wire.META, f"unexpected response tag {tag:#x}: {statement}")

        schema = self._read_schema(payload)
        rows = []
        while True:
            tag, payload = self.read_frame()
            if tag == wire.ROW:
                offset = 0
                row = []
                for _, value_type in schema:
                    value, offset = wire.parse_cell(payload, offset, value_type)
                    row.append(value)
                wire.require(offset == len(payload), "ROW has trailing bytes")
                rows.append(row)
                continue
            if tag in (wire.ERROR, wire.TRANSACTION_ABORT):
                kind = "TRANSACTION_ABORT" if tag == wire.TRANSACTION_ABORT else "ERROR"
                detail = payload.decode("utf-8", "replace")
                raise AssertionError(f"{kind}: {statement}\n{detail}")
            wire.require(
                tag == wire.RESULT_END and len(payload) == 8,
                f"query did not end with RESULT_END: {statement}",
            )
            wire.require(
                struct.unpack("!Q", payload)[0] == len(rows),
                "RESULT_END row count is incorrect",
            )
            return self._format_rows(rows)

    @staticmethod
    def _read_schema(payload: bytes) -> list[tuple[str, int]]:
        wire.require(len(payload) >= 2, "truncated META")
        column_count = struct.unpack_from("!H", payload)[0]
        wire.require(column_count > 0, "META must have at least one column")
        offset = 2
        schema = []
        for _ in range(column_count):
            wire.require(offset + 2 <= len(payload), "truncated META name length")
            name_size = struct.unpack_from("!H", payload, offset)[0]
            offset += 2
            wire.require(offset + name_size + 1 <= len(payload), "truncated META column")
            name = payload[offset : offset + name_size].decode("utf-8")
            offset += name_size
            schema.append((name, payload[offset]))
            offset += 1
        wire.require(offset == len(payload), "META has trailing bytes")
        return schema

    @staticmethod
    def _format_rows(rows: list[list[object | None]]) -> str:
        # Keep the two legacy parsing affordances used by this matrix and by
        # crash_repro_strong.py: integer cells begin a '| ... |' row and an
        # empty result has the exact Total record(s): 0 marker.
        lines = [
            "| " + " | ".join("NULL" if value is None else str(value) for value in row) + " |"
            for row in rows
        ]
        lines.append(f"Total record(s): {len(rows)}")
        return "\n".join(lines)

    def ok(self, statement: str) -> str:
        return self.sql(statement)


@dataclass
class TestResult:
    name: str
    ok: bool
    detail: str
    recovery_seconds: float | None = None


class LiveServer:
    _active_server: LiveServer | None = None

    def __init__(self, work_dir: Path, db_name: str):
        self.work_dir = work_dir
        self.db_name = db_name
        self.proc: subprocess.Popen[bytes] | None = None

    def start(self, log_name: str) -> float:
        assert_port_free()
        start = time.monotonic()
        out = open(self.work_dir / f"{log_name}.out", "wb")
        err = open(self.work_dir / f"{log_name}.err", "wb")
        self.proc = subprocess.Popen(
            [str(SERVER_BIN), self.db_name], cwd=self.work_dir, stdout=out, stderr=err
        )
        wait_for_server()
        LiveServer._active_server = self
        return time.monotonic() - start

    @classmethod
    def crash_active(cls) -> None:
        server = cls._active_server
        if server is None or server.proc is None or server.proc.poll() is not None:
            raise AssertionError("没有可崩溃的 live server")
        server.proc.send_signal(signal.SIGKILL)

    def wait_crashed(self) -> int:
        if self.proc is None:
            raise AssertionError("server 尚未启动")
        status = self.proc.wait(timeout=10)
        if LiveServer._active_server is self:
            LiveServer._active_server = None
        return status

    def stop(self) -> None:
        if self.proc is None or self.proc.poll() is not None:
            if LiveServer._active_server is self:
                LiveServer._active_server = None
            return
        self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=3)
        if LiveServer._active_server is self:
            LiveServer._active_server = None


def assert_port_free() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.2)
    try:
        sock.connect(("127.0.0.1", PORT))
    except OSError:
        return
    finally:
        sock.close()
    raise AssertionError(f"端口 {PORT} 已被占用")


def wait_for_server() -> None:
    deadline = time.monotonic() + 10
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        client: SqlClient | None = None
        try:
            client = SqlClient()
            client.readiness_probe()
            return
        except (OSError, EOFError, wire.ProtocolFailure) as exc:
            last_error = exc
            time.sleep(0.005)
        finally:
            if client is not None:
                client.close()
    raise AssertionError(f"server 未通过 Wire SQL readiness: {last_error}")


def parse_first_int(output: str) -> int:
    for line in output.splitlines():
        match = re.match(r"^\|\s*(-?\d+)\s*\|", line)
        if match:
            return int(match.group(1))
    raise AssertionError(f"无法从输出解析整数:\n{output}")


def assert_count(client: SqlClient, expected: int) -> None:
    output = client.sql("select count(*) from kv;")
    actual = parse_first_int(output)
    if actual != expected:
        raise AssertionError(
            f"行数不符合预期: expected={expected}, actual={actual}\n{output}"
        )


def assert_value(client: SqlClient, row_id: int, expected_v: int) -> None:
    output = client.sql(f"select v from kv where id = {row_id};")
    actual = parse_first_int(output)
    if actual != expected_v:
        raise AssertionError(
            f"id={row_id} 的 v 不符合预期: expected={expected_v}, actual={actual}\n{output}"
        )


def setup_schema(with_index: bool) -> None:
    client = SqlClient()
    try:
        client.ok("create table kv (id int, v int);")
        if with_index:
            client.ok("create index kv(id);")
    finally:
        client.close()


def insert_range(start_id: int, end_id: int, checkpoint_every: int = 0) -> None:
    client = SqlClient()
    try:
        for row_id in range(start_id, end_id + 1):
            client.ok("begin;")
            client.ok(f"insert into kv values ({row_id}, {row_id * 10});")
            client.ok("commit;")
            if checkpoint_every > 0 and row_id % checkpoint_every == 0:
                client.ok("create static_checkpoint;")
    finally:
        client.close()


def insert_range_multi(total_rows: int, threads: int) -> None:
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker(worker_id: int) -> None:
        try:
            client = SqlClient()
            try:
                for row_id in range(worker_id + 1, total_rows + 1, threads):
                    client.ok("begin;")
                    client.ok(f"insert into kv values ({row_id}, {row_id * 10});")
                    client.ok("commit;")
            finally:
                client.close()
        except BaseException as exc:  # noqa: BLE001 - 测试线程需要把所有异常带回主线程
            with lock:
                errors.append(exc)

    workers = [threading.Thread(target=worker, args=(idx,)) for idx in range(threads)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    if errors:
        raise AssertionError(f"多线程 workload 失败: {errors[0]}")


def leave_uncommitted_and_crash(uncommitted_id: int) -> None:
    client = SqlClient()
    try:
        client.ok("begin;")
        client.ok(f"insert into kv values ({uncommitted_id}, {uncommitted_id * 10});")
        client.ok("update kv set v = 999 where id = 1;")
        client.ok("delete from kv where id = 2;")
        client.sql("crash", expect_response=False)
    finally:
        client.close()


def run_case(
    name: str,
    work_root: Path,
    rows: int,
    *,
    with_index: bool = False,
    multi_thread: bool = False,
    threads: int = 4,
    checkpoint_every: int = 0,
) -> TestResult:
    work_dir = work_root / name
    work_dir.mkdir()
    server = LiveServer(work_dir, "checkpoint_test")
    recovery_seconds: float | None = None
    try:
        server.start("server-before-crash")
        setup_schema(with_index)
        if multi_thread:
            insert_range_multi(rows, threads)
        else:
            insert_range(1, rows, checkpoint_every)
        pre_crash_client = SqlClient()
        try:
            assert_count(pre_crash_client, rows)
        finally:
            pre_crash_client.close()
        leave_uncommitted_and_crash(rows + 1)
        status = server.wait_crashed()
        if status == 0:
            raise AssertionError("crash 命令预期应让 server 非 0 退出")

        recovery_seconds = server.start("server-after-crash")
        client = SqlClient()
        try:
            assert_count(client, rows)
            assert_value(client, 1, 10)
            assert_value(client, 2, 20)
            assert_value(client, rows, rows * 10)
        finally:
            client.close()
        return TestResult(
            name, True, f"功能通过，恢复耗时 {recovery_seconds:.4f}s", recovery_seconds
        )
    except BaseException as exc:  # noqa: BLE001 - 汇总测试结果
        return TestResult(name, False, str(exc), recovery_seconds)
    finally:
        server.stop()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="运行真实 server crash recovery 近似测试矩阵"
    )
    parser.add_argument("--skip-build", action="store_true", help="跳过 make build")
    parser.add_argument("--small-rows", type=int, default=30)
    parser.add_argument("--large-rows", type=int, default=2000)
    parser.add_argument("--huge-rows", type=int, default=10000)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument(
        "--only",
        choices=[
            "single",
            "multi",
            "index",
            "large",
            "without_checkpoint",
            "with_checkpoint",
        ],
        help="只运行一个测试点，便于定位",
    )
    args = parser.parse_args()

    if not args.skip_build:
        subprocess.run(["make", "-C", str(ROOT_DIR), "build"], check=True)
    if not SERVER_BIN.exists():
        print(f"缺少 server 二进制: {SERVER_BIN}", file=sys.stderr)
        return 1

    work_root = Path(tempfile.mkdtemp(prefix="rmdb-live-matrix."))
    print(f"工作目录: {work_root}")
    results: list[TestResult] = []
    try:
        without_checkpoint: TestResult | None = None
        with_checkpoint: TestResult | None = None

        if args.only in (None, "single"):
            results.append(
                run_case(
                    "crash_recovery_single_thread_test", work_root, args.small_rows
                )
            )
        if args.only in (None, "multi"):
            results.append(
                run_case(
                    "crash_recovery_multi_thread_test",
                    work_root,
                    args.small_rows,
                    multi_thread=True,
                    threads=args.threads,
                )
            )
        if args.only in (None, "index"):
            results.append(
                run_case(
                    "crash_recovery_index_test",
                    work_root,
                    args.large_rows,
                    with_index=True,
                )
            )
        if args.only in (None, "large"):
            results.append(
                run_case(
                    "crash_recovery_large_data_test",
                    work_root,
                    args.large_rows,
                    multi_thread=True,
                    threads=args.threads,
                )
            )
        if args.only in (None, "without_checkpoint"):
            without_checkpoint = run_case(
                "crash_recovery_without_checkpoint", work_root, args.huge_rows
            )
            results.append(without_checkpoint)
        if args.only in (None, "with_checkpoint"):
            checkpoint_every = max(1, args.huge_rows // 8)
            with_checkpoint = run_case(
                "crash_recovery_with_checkpoint",
                work_root,
                args.huge_rows,
                checkpoint_every=checkpoint_every,
            )
            results.append(with_checkpoint)

        print()
        print("测试结果:")
        for result in results:
            status = "PASS" if result.ok else "FAIL"
            print(f"- {status} {result.name}: {result.detail}")

        if (
            without_checkpoint is not None
            and with_checkpoint is not None
            and without_checkpoint.ok
            and with_checkpoint.ok
        ):
            t1 = without_checkpoint.recovery_seconds or 0.0
            t2 = with_checkpoint.recovery_seconds or 0.0
            ratio = (t2 / t1) if t1 > 0 else float("inf")
            perf_status = "PASS" if ratio <= 0.70 else "WARN"
            print(
                f"- {perf_status} checkpoint 恢复耗时比例: t2/t1 = {ratio:.3f} ({t2:.4f}s / {t1:.4f}s)"
            )

        return 0 if all(result.ok for result in results) else 1
    finally:
        shutil.rmtree(work_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
