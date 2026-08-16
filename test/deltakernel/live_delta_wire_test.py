#!/usr/bin/env python3
"""Fresh-format DeltaKernel Wire v3 acceptance."""

import os
import struct
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "protocol"))
import live_wire_protocol_test as wire


def require(condition, message):
    if not condition:
        raise wire.ProtocolFailure(message)


def test_durable_checkpoint_restart(binary):
    server = wire.Server(binary, {"RMDB_STORAGE_ENGINE": None}, require_sql_readiness=False)
    try:
        server.start()
        client = wire.WireClient(server.port)
        client.command("CREATE TABLE t(k INT, v INT);")
        client.command("INSERT INTO t VALUES(1, 7);")
        client.command("CREATE STATIC_CHECKPOINT;")
        client.command("UPDATE t SET v = 11 WHERE k = 1;")
        client.close()
        server.stop(crash=True)
        server.start()
        client = wire.WireClient(server.port)
        schema, rows = client.query("SELECT v FROM t WHERE k = 1;")
        require(schema == [("v", wire.INT32)] and rows == [[11]], "restart lost typed Delta value")
        client.close()
    finally:
        server.cleanup()


def test_failed_prepare_preserves_dictionary(binary):
    server = wire.Server(binary, {"RMDB_STORAGE_ENGINE": None}, require_sql_readiness=False)
    try:
        server.start()
        client = wire.WireClient(server.port)
        client.command("CREATE TABLE t(k INT, v INT);")
        client.command("INSERT INTO t VALUES(1, 7);")
        statements = [(1, True, [wire.INT32], "SELECT v FROM t WHERE k = $1;")]
        schemas = client.prepare(statements)
        require(schemas == {1: [("v", wire.INT32)]}, "Delta PREPARE_OK schema mismatch")
        client.command("CREATE TABLE unrelated(id INT);")

        sql = b"INSERT INTO t VALUES($1, $2);"
        payload = struct.pack("!H H B H B B I", 1, 2, 0, 2, wire.CHAR, wire.INT32, len(sql)) + sql
        client.write_frame(wire.PREPARE_SET, payload)
        tag, diagnostic = client.read_frame()
        require(tag == wire.ERROR and diagnostic, "wrong Delta parameter type did not fail PREPARE_SET")

        result = client.batch([(1, [1])], {1: [wire.INT32]}, schemas)
        require(result == (1, 0, 0xFFFF, "", [(0, [[7]])]), "failed PREPARE_SET replaced prior dictionary")
        client.close()
    finally:
        server.cleanup()


def test_typed_insert_select_batch(binary):
    server = wire.Server(binary, {"RMDB_STORAGE_ENGINE": None}, require_sql_readiness=False)
    try:
        server.start()
        client = wire.WireClient(server.port)
        client.command("CREATE TABLE t(k INT, amount FLOAT, note CHAR(16));")
        statements = [
            (1, False, [wire.INT32, wire.FLOAT32, wire.CHAR], "INSERT INTO t VALUES($1, $2, $3);"),
            (2, True, [wire.INT32], "SELECT amount, note FROM t WHERE k = $1;"),
        ]
        schemas = client.prepare(statements)
        parameter_types = {1: [wire.INT32, wire.FLOAT32, wire.CHAR], 2: [wire.INT32]}
        bits = 0x40490FDB
        result = client.batch(
            [(1, [7, bits, "a\0b"]), (1, [8, bits, ""]), (2, [7]), (2, [8])], parameter_types, schemas
        )
        require(
            result == (4, 0, 0xFFFF, "", [(2, [[bits, "a\0b"]]), (3, [[bits, ""]])]),
            "Delta batch did not preserve exact CHAR bytes",
        )
        _, rows = client.query("SELECT note FROM t WHERE k = 7;")
        require(rows == [["a\0b"]], "Delta EXEC_STREAM truncated embedded-NUL CHAR")
        _, rows = client.query("SELECT note FROM t WHERE k = 8;")
        require(rows == [[""]], "Delta EXEC_STREAM changed empty CHAR")
        client.close()
    finally:
        server.cleanup()


def test_batch_error_discards_private_delta(binary):
    server = wire.Server(binary, {"RMDB_STORAGE_ENGINE": None}, require_sql_readiness=False)
    try:
        server.start()
        client = wire.WireClient(server.port)
        client.command("CREATE TABLE t(k INT, note CHAR(3));")
        statements = [(1, False, [wire.INT32, wire.CHAR], "INSERT INTO t VALUES($1, $2);")]
        schemas = client.prepare(statements)
        client.command("BEGIN;")
        result = client.batch([(1, [1, "ok"]), (1, [2, "too-long"])], {1: [wire.INT32, wire.CHAR]}, schemas)
        executed, status, failed, diagnostic, rows = result
        require((executed, status, failed, rows) == (1, 2, 1, []), "Delta batch failure framing mismatch")
        require(diagnostic, "Delta batch failure omitted diagnostic")
        _, rows = client.query("SELECT * FROM t;")
        require(rows == [], "AUTO_ABORT did not discard the private TxnDelta")
        client.close()
    finally:
        server.cleanup()


def test_si_conflict_reuse_checkpoint_restart(binary):
    server = wire.Server(binary, {"RMDB_STORAGE_ENGINE": None}, require_sql_readiness=False)
    try:
        server.start()
        setup = wire.WireClient(server.port)
        setup.command("CREATE TABLE t(k INT, v INT);")
        setup.command("INSERT INTO t VALUES(1, 1);")
        setup.close()
        winner = wire.WireClient(server.port)
        loser = wire.WireClient(server.port)
        winner.command("BEGIN;")
        loser.command("BEGIN;")
        winner.command("UPDATE t SET v = 11 WHERE k = 1;")
        loser.command("UPDATE t SET v = 22 WHERE k = 1;")
        winner.command("COMMIT;")
        tag, diagnostic = loser.stream_raw("COMMIT;")
        require(tag == wire.TRANSACTION_ABORT and diagnostic, "stale Delta commit did not abort")
        _, rows = loser.query("SELECT v FROM t WHERE k = 1;")
        require(rows == [[11]], "aborted connection did not observe winner or remain reusable")
        winner.command("CREATE STATIC_CHECKPOINT;")
        winner.close()
        loser.close()
        server.stop(crash=True)
        server.start()
        client = wire.WireClient(server.port)
        schema, rows = client.query("SELECT v FROM t WHERE k = 1;")
        require(schema == [("v", wire.INT32)] and rows == [[11]], "checkpoint/restart lost SI winner")
        client.close()
    finally:
        server.cleanup()


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: live_delta_wire_test.py <rmdb>")
    for test in (
        test_durable_checkpoint_restart,
        test_failed_prepare_preserves_dictionary,
        test_typed_insert_select_batch,
        test_batch_error_discards_private_delta,
        test_si_conflict_reuse_checkpoint_restart,
    ):
        test(sys.argv[1])


if __name__ == "__main__":
    try:
        main()
    except (wire.ProtocolFailure, OSError, EOFError) as error:
        print("live Delta Wire test failed:", error, file=sys.stderr)
        raise SystemExit(1)
