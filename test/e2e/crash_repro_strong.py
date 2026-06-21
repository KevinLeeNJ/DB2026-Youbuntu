#!/usr/bin/env python3
"""强场景 crash recovery 复现脚本。

针对「日志不截断 + 非幂等 undo/redo」的边界场景构造确定性复现：
  A. loser update 跨轮覆盖 committed update（不依赖 RID 复用，最确定）
  B. loser delete 跨轮覆盖 committed update
  C. loser insert 跨轮 + RID 复用（insert-crash-insert 变体）
  D. 多轮纯 committed insert-crash（基线对照，应通过）
  E. 索引 + 多轮 loser（index_test 核心）

复用 live_crash_recovery_matrix 的 LiveServer / SqlClient 框架。
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import live_crash_recovery_matrix as m  # noqa: F401  复用框架与常量
from live_crash_recovery_matrix import (
    LiveServer,
    SqlClient,
    TestResult,
    parse_first_int,
)


def query_v(client: SqlClient, row_id: int) -> int | None:
    """返回 id=row_id 的 v 值；行不存在时返回 None。"""
    out = client.sql(f"select v from kv where id = {row_id};")
    if "Total record(s): 0" in out:
        return None
    return parse_first_int(out)


def query_count(client: SqlClient) -> int:
    return parse_first_int(client.sql("select count(*) from kv;"))


def crash_and_wait(server: LiveServer, client: SqlClient) -> None:
    client.sql("crash", expect_response=False)
    client.close()
    server.wait_crashed()


def result(name: str, ok: bool, detail: str) -> TestResult:
    return TestResult(name, ok, detail)


# ---------------------------------------------------------------------------
# 场景 A: loser update 跨轮覆盖 committed update
#   round1: insert id=1 v=10 committed; begin; update id=1 v=999; crash(loser)
#   recover1: undo loser update -> v 恢复 10
#   round2: update id=1 v=20 committed; crash(干净)
#   recover2: 日志未截断 -> analyze 仍把 round1 loser 视为 active ->
#             redo T_loser(999)+T_committed(20) -> v=20; undo T_loser(old=10) -> v=10  BUG
#   预期 v=20
# ---------------------------------------------------------------------------
def scenario_A_loser_update_overwrites_committed(work_root: Path) -> TestResult:
    name = "A_loser_update_overwrites_committed"
    work_dir = work_root / name
    work_dir.mkdir()
    server = LiveServer(work_dir, "repro_db")
    try:
        server.start("s1")
        c = SqlClient()
        c.ok("create table kv (id int, v int);")
        c.ok("begin;")
        c.ok("insert into kv values (1, 10);")
        c.ok("commit;")
        c.ok("begin;")
        c.ok("update kv set v = 999 where id = 1;")
        crash_and_wait(server, c)

        server.start("s2")
        c = SqlClient()
        v1 = query_v(c, 1)
        c.close()
        if v1 != 10:
            return result(
                name,
                False,
                f"recover1 后 v={v1}, 期望 10 (基础 undo loser update 已坏)",
            )

        c = SqlClient()
        c.ok("begin;")
        c.ok("update kv set v = 20 where id = 1;")
        c.ok("commit;")
        crash_and_wait(server, c)

        server.start("s3")
        c = SqlClient()
        v2 = query_v(c, 1)
        c.close()
        if v2 != 20:
            return result(
                name,
                False,
                f"recover2 后 v={v2}, 期望 20 (旧 loser update 的 undo 覆盖了 committed update)",
            )
        return result(name, True, f"v={v2} 正确，loser undo 未污染 committed update")
    finally:
        server.stop()


# ---------------------------------------------------------------------------
# 场景 B: loser delete 跨轮覆盖 committed update
#   round1: insert id=1,2 committed; begin; delete id=2; crash(loser)
#   recover1: undo delete -> id=2 恢复
#   round2: update id=2 v=99 committed; crash(干净)
#   recover2: redo T_loser delete(id=2)+T_committed update(id=2,99);
#             undo T_loser delete(old=20) -> 覆盖 99 为 20  BUG
#   预期 v=99
# ---------------------------------------------------------------------------
def scenario_B_loser_delete_overwrites_committed(work_root: Path) -> TestResult:
    name = "B_loser_delete_overwrites_committed"
    work_dir = work_root / name
    work_dir.mkdir()
    server = LiveServer(work_dir, "repro_db")
    try:
        server.start("s1")
        c = SqlClient()
        c.ok("create table kv (id int, v int);")
        c.ok("begin;")
        c.ok("insert into kv values (1, 10);")
        c.ok("insert into kv values (2, 20);")
        c.ok("commit;")
        c.ok("begin;")
        c.ok("delete from kv where id = 2;")
        crash_and_wait(server, c)

        server.start("s2")
        c = SqlClient()
        v1 = query_v(c, 2)
        c.close()
        if v1 != 20:
            return result(
                name,
                False,
                f"recover1 后 id=2 v={v1}, 期望 20 (基础 undo loser delete 已坏)",
            )

        c = SqlClient()
        c.ok("begin;")
        c.ok("update kv set v = 99 where id = 2;")
        c.ok("commit;")
        crash_and_wait(server, c)

        server.start("s3")
        c = SqlClient()
        v2 = query_v(c, 2)
        c.close()
        if v2 != 99:
            return result(
                name,
                False,
                f"recover2 后 id=2 v={v2}, 期望 99 (旧 loser delete 的 undo 覆盖了 committed update)",
            )
        return result(
            name, True, f"v={v2} 正确，loser delete undo 未污染 committed update"
        )
    finally:
        server.stop()


# ---------------------------------------------------------------------------
# 场景 C: loser insert 跨轮 + RID 复用（insert-crash-insert 变体）
#   round1: insert id=1 committed; begin; insert id=2; crash(loser)
#   recover1: undo insert id=2 -> RID 释放
#   round2: insert id=2 committed (可能复用同一 RID); crash(干净)
#   recover2: 日志未截断 -> round1 loser insert id=2 仍 active ->
#             undo insert id=2 -> 删除 committed 的 id=2  BUG
#   预期 count=2 (id=1,2 都在)
# ---------------------------------------------------------------------------
def scenario_C_loser_insert_rid_reuse(work_root: Path) -> TestResult:
    name = "C_loser_insert_rid_reuse"
    work_dir = work_root / name
    work_dir.mkdir()
    server = LiveServer(work_dir, "repro_db")
    try:
        server.start("s1")
        c = SqlClient()
        c.ok("create table kv (id int, v int);")
        c.ok("begin;")
        c.ok("insert into kv values (1, 10);")
        c.ok("commit;")
        c.ok("begin;")
        c.ok("insert into kv values (2, 20);")
        crash_and_wait(server, c)

        server.start("s2")
        c = SqlClient()
        cnt1 = query_count(c)
        c.close()
        if cnt1 != 1:
            return result(
                name,
                False,
                f"recover1 后 count={cnt1}, 期望 1 (基础 undo loser insert 已坏)",
            )

        c = SqlClient()
        c.ok("begin;")
        c.ok("insert into kv values (2, 20);")
        c.ok("commit;")
        crash_and_wait(server, c)

        server.start("s3")
        c = SqlClient()
        cnt2 = query_count(c)
        v2 = query_v(c, 2)
        c.close()
        if v2 is None:
            return result(
                name,
                False,
                f"recover2 后 count={cnt2}, id=2 不存在 (旧 loser insert 的 undo 删除了 committed 的 id=2)",
            )
        if cnt2 != 2 or v2 != 20:
            return result(
                name,
                False,
                f"recover2 后 count={cnt2} v(id=2)={v2}, 期望 count=2 v=20 (旧 loser insert 的 undo 删除了 committed 的 id=2)",
            )
        return result(name, True, f"count={cnt2} v={v2} 正确")
    finally:
        server.stop()


# ---------------------------------------------------------------------------
# 场景 D: 多轮纯 committed insert-crash（基线对照，应通过）
#   每轮 insert 一批 committed 行 -> crash -> recover，无 loser
#   验证日志不截断下纯 committed 多轮 redo 是否稳定
# ---------------------------------------------------------------------------
def scenario_D_multi_round_committed_insert(work_root: Path) -> TestResult:
    name = "D_multi_round_committed_insert"
    work_dir = work_root / name
    work_dir.mkdir()
    server = LiveServer(work_dir, "repro_db")
    try:
        server.start("s1")
        c = SqlClient()
        c.ok("create table kv (id int, v int);")
        c.close()

        rounds = [(1, 10), (11, 20), (21, 30)]
        for start, end in rounds:
            c = SqlClient()
            for i in range(start, end + 1):
                c.ok("begin;")
                c.ok(f"insert into kv values ({i}, {i * 10});")
                c.ok("commit;")
            crash_and_wait(server, c)
            server.start(f"s_{start}")
            c = SqlClient()
            cnt = query_count(c)
            c.close()
            if cnt != end:
                return result(
                    name, False, f"插入到 {end} 后 recover count={cnt}, 期望 {end}"
                )

        c = SqlClient()
        cnt = query_count(c)
        ok_all = all(query_v(c, i) == i * 10 for i in (1, 15, 30))
        c.close()
        if cnt != 30 or not ok_all:
            return result(
                name, False, f"最终 count={cnt} 值校验={ok_all}, 期望 count=30 且值正确"
            )
        return result(name, True, f"count={cnt} 多轮 committed 稳定")
    finally:
        server.stop()


# ---------------------------------------------------------------------------
# 场景 E: 索引 + 多轮 loser（index_test 核心）
#   带索引，复现场景 A 的 loser update 跨轮，并校验索引查询与表一致
# ---------------------------------------------------------------------------
def scenario_E_index_multi_round_loser(work_root: Path) -> TestResult:
    name = "E_index_multi_round_loser"
    work_dir = work_root / name
    work_dir.mkdir()
    server = LiveServer(work_dir, "repro_db")
    try:
        server.start("s1")
        c = SqlClient()
        c.ok("create table kv (id int, v int);")
        c.ok("create index kv(id);")
        c.ok("begin;")
        c.ok("insert into kv values (1, 10);")
        c.ok("commit;")
        c.ok("begin;")
        c.ok("update kv set v = 999 where id = 1;")
        crash_and_wait(server, c)

        server.start("s2")
        c = SqlClient()
        v1 = query_v(c, 1)
        cnt1 = query_count(c)
        c.close()
        if v1 != 10 or cnt1 != 1:
            return result(
                name, False, f"recover1 后 v={v1} count={cnt1}, 期望 v=10 count=1"
            )

        c = SqlClient()
        c.ok("begin;")
        c.ok("update kv set v = 20 where id = 1;")
        c.ok("commit;")
        crash_and_wait(server, c)

        server.start("s3")
        c = SqlClient()
        v2 = query_v(c, 1)
        cnt2 = query_count(c)
        c.close()
        if v2 != 20 or cnt2 != 1:
            return result(
                name,
                False,
                f"recover2 后 v={v2} count={cnt2}, 期望 v=20 count=1 (索引场景下 loser undo 覆盖 committed 或索引不一致)",
            )
        return result(name, True, f"v={v2} count={cnt2} 正确，索引与表一致")
    finally:
        server.stop()


SCENARIOS = {
    "A": scenario_A_loser_update_overwrites_committed,
    "B": scenario_B_loser_delete_overwrites_committed,
    "C": scenario_C_loser_insert_rid_reuse,
    "D": scenario_D_multi_round_committed_insert,
    "E": scenario_E_index_multi_round_loser,
}


def main() -> int:
    ap = argparse.ArgumentParser(description="强场景 crash recovery 复现")
    ap.add_argument("--only", choices=list(SCENARIOS.keys()) + ["all"], default="all")
    ap.add_argument("--keep-workdir", action="store_true")
    args = ap.parse_args()

    work_root = Path(tempfile.mkdtemp(prefix="rmdb-repro."))
    print(f"工作目录: {work_root}")
    selected = list(SCENARIOS.keys()) if args.only == "all" else [args.only]
    results = [SCENARIOS[k](work_root) for k in selected]

    print()
    print("复现结果:")
    for r in results:
        status = "PASS" if r.ok else "FAIL"
        print(f"- {status} {r.name}: {r.detail}")

    if args.keep_workdir:
        print(f"保留工作目录: {work_root}")
    else:
        shutil.rmtree(work_root, ignore_errors=True)
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
