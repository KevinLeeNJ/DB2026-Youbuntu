#!/usr/bin/env python3
"""Black-box RMDB Wire v3 integration smoke test.

This intentionally speaks the published protocol directly.  It does not use
the legacy text client or in-process executor fixtures, so regressions in the
server's network/result-sink path are observable by CTest.
"""

import os
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time


META = 0x01
ROW = 0x02
COMMAND_OK = 0x10
RESULT_END = 0x11
TRANSACTION_ABORT = 0x12
ERROR = 0x13
PREPARE_OK = 0x14
BATCH_RESULT = 0x15
EXEC_STREAM = 0x20
PREPARE_SET = 0x21
EXEC_BATCH = 0x22

INT32 = 0x01
FLOAT32 = 0x02
CHAR = 0x03


class ProtocolFailure(AssertionError):
    pass


def require(condition, message):
    if not condition:
        raise ProtocolFailure(message)


def recv_exact(sock, size):
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise EOFError("peer closed the connection")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def cell(value_type, value):
    if value is None:
        return b"\0"
    if value_type == INT32:
        return b"\1" + struct.pack("!i", value)
    if value_type == FLOAT32:
        return b"\1" + struct.pack("!I", value)
    if value_type == CHAR:
        encoded = value.encode("utf-8")
        return b"\1" + struct.pack("!I", len(encoded)) + encoded
    raise ValueError("unknown cell type")


def parse_cell(payload, offset, value_type):
    require(offset < len(payload), "truncated typed cell")
    present = payload[offset]
    offset += 1
    if present == 0:
        return None, offset
    require(present == 1, "invalid typed-cell present flag")
    if value_type == INT32:
        require(offset + 4 <= len(payload), "truncated INT32 cell")
        return struct.unpack_from("!i", payload, offset)[0], offset + 4
    if value_type == FLOAT32:
        require(offset + 4 <= len(payload), "truncated FLOAT32 cell")
        return struct.unpack_from("!I", payload, offset)[0], offset + 4
    if value_type == CHAR:
        require(offset + 4 <= len(payload), "truncated CHAR cell length")
        size = struct.unpack_from("!I", payload, offset)[0]
        offset += 4
        require(offset + size <= len(payload), "truncated CHAR cell")
        return payload[offset : offset + size].decode("utf-8"), offset + size
    raise ProtocolFailure("unknown type in response")


class WireClient:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=2)
        self.sock.settimeout(5)
        handshake = b"RMDB\x00\x03\x00\x00"
        self.sock.sendall(handshake)
        require(recv_exact(self.sock, len(handshake)) == handshake, "Wire v3 handshake was not echoed")

    def close(self):
        self.sock.close()

    def write_frame(self, tag, payload=b"", flags=0):
        self.sock.sendall(struct.pack("!IBBH", len(payload), tag, flags, 0) + payload)

    def read_frame(self):
        size, tag, flags, reserved = struct.unpack("!IBBH", recv_exact(self.sock, 8))
        require(flags == 0 and reserved == 0, "server returned nonzero frame flags or reserved bits")
        require(size <= 1 << 20, "server exceeded frame payload limit")
        return tag, recv_exact(self.sock, size)

    def stream_raw(self, sql):
        self.write_frame(EXEC_STREAM, sql.encode("utf-8"))
        return self.read_frame()

    def command(self, sql):
        tag, payload = self.stream_raw(sql)
        require(tag == COMMAND_OK and payload == b"", "expected COMMAND_OK for: " + sql)

    def query(self, sql):
        tag, payload = self.stream_raw(sql)
        if tag in (ERROR, TRANSACTION_ABORT):
            raise ProtocolFailure("query failed: " + payload.decode("utf-8", "replace"))
        require(tag == META, "query must start with META: " + sql)
        require(len(payload) >= 2, "truncated META")
        column_count = struct.unpack_from("!H", payload)[0]
        require(column_count > 0, "META must have at least one column")
        offset = 2
        schema = []
        for _ in range(column_count):
            require(offset + 2 <= len(payload), "truncated META name length")
            name_size = struct.unpack_from("!H", payload, offset)[0]
            offset += 2
            require(offset + name_size + 1 <= len(payload), "truncated META column")
            name = payload[offset : offset + name_size].decode("utf-8")
            offset += name_size
            schema.append((name, payload[offset]))
            offset += 1
        require(offset == len(payload), "META has trailing bytes")

        rows = []
        while True:
            tag, payload = self.read_frame()
            if tag == ROW:
                offset = 0
                row = []
                for _, value_type in schema:
                    value, offset = parse_cell(payload, offset, value_type)
                    row.append(value)
                require(offset == len(payload), "ROW has trailing bytes")
                rows.append(row)
                continue
            require(tag == RESULT_END and len(payload) == 8, "query must end with RESULT_END")
            require(struct.unpack("!Q", payload)[0] == len(rows), "RESULT_END row count is incorrect")
            return schema, rows

    def readiness_probe(self):
        tag, payload = self.stream_raw("show tables;")
        if tag == COMMAND_OK:
            require(payload == b"", "show tables COMMAND_OK carried a payload")
            return
        require(tag == META, "show tables returned neither COMMAND_OK nor META")
        while True:
            tag, payload = self.read_frame()
            if tag == ROW:
                continue
            require(tag == RESULT_END and len(payload) == 8, "show tables query has no RESULT_END")
            return

    def prepare(self, statements):
        payload = bytearray(struct.pack("!H", len(statements)))
        for statement_id, query, parameter_types, sql in statements:
            encoded_sql = sql.encode("utf-8")
            payload += struct.pack("!H B H", statement_id, int(query), len(parameter_types))
            payload += bytes(parameter_types)
            payload += struct.pack("!I", len(encoded_sql)) + encoded_sql
        self.write_frame(PREPARE_SET, bytes(payload))
        tag, payload = self.read_frame()
        require(tag == PREPARE_OK, "PREPARE_SET did not return PREPARE_OK")
        offset = 0
        require(len(payload) >= 2, "truncated PREPARE_OK")
        count = struct.unpack_from("!H", payload)[0]
        offset = 2
        require(count == len(statements), "PREPARE_OK statement count mismatch")
        schemas = {}
        for statement_id, query, _, _ in statements:
            require(offset + 4 <= len(payload), "truncated PREPARE_OK entry")
            returned_id, column_count = struct.unpack_from("!HH", payload, offset)
            offset += 4
            require(returned_id == statement_id, "PREPARE_OK changed statement order or id")
            columns = []
            for _ in range(column_count):
                require(offset + 2 <= len(payload), "truncated prepared column name")
                name_size = struct.unpack_from("!H", payload, offset)[0]
                offset += 2
                require(offset + name_size + 1 <= len(payload), "truncated prepared column")
                name = payload[offset : offset + name_size].decode("utf-8")
                offset += name_size
                columns.append((name, payload[offset]))
                offset += 1
            require(bool(columns) == bool(query), "prepared command/query schema mismatch")
            schemas[statement_id] = columns
        require(offset == len(payload), "PREPARE_OK has trailing bytes")
        return schemas

    def batch(self, operations, parameter_types, schemas):
        payload = bytearray(struct.pack("!H", len(operations)))
        for statement_id, values in operations:
            payload += struct.pack("!H", statement_id)
            expected = parameter_types[statement_id]
            require(len(expected) == len(values), "batch parameter count mismatch")
            for value_type, value in zip(expected, values):
                payload += cell(value_type, value)
        self.write_frame(EXEC_BATCH, bytes(payload), flags=1)
        tag, payload = self.read_frame()
        require(tag == BATCH_RESULT, "EXEC_BATCH did not return BATCH_RESULT")
        require(len(payload) >= 11, "truncated BATCH_RESULT")
        executed, status, failed, diagnostic_size = struct.unpack_from("!HBHI", payload)
        offset = 9
        require(offset + diagnostic_size + 2 <= len(payload), "truncated BATCH_RESULT diagnostic")
        diagnostic = payload[offset : offset + diagnostic_size].decode("utf-8", "replace")
        offset += diagnostic_size
        result_count = struct.unpack_from("!H", payload, offset)[0]
        offset += 2
        results = []
        for _ in range(result_count):
            require(offset + 6 <= len(payload), "truncated batch query result")
            operation_index, row_count = struct.unpack_from("!HI", payload, offset)
            offset += 6
            statement_id = operations[operation_index][0]
            schema = schemas[statement_id]
            rows = []
            for _ in range(row_count):
                row = []
                for _, value_type in schema:
                    value, offset = parse_cell(payload, offset, value_type)
                    row.append(value)
                rows.append(row)
            results.append((operation_index, rows))
        require(offset == len(payload), "BATCH_RESULT has trailing bytes")
        return executed, status, failed, diagnostic, results


class Server:
    def __init__(self, binary):
        self.binary = os.path.abspath(binary)
        self.root = tempfile.mkdtemp(prefix="rmdb-live-wire-")
        self.port = find_free_port()
        self.process = None

    def start(self):
        env = os.environ.copy()
        env["RMDB_PORT"] = str(self.port)
        # Live protocol tests exercise normal server behavior.  Do not inherit
        # diagnostic checkpoint knobs from the caller: a tiny threshold could
        # checkpoint the buffered ABORT before this suite deliberately kills
        # the process.
        env.pop("RMDB_AUTO_CHECKPOINT_BYTES", None)
        env.pop("RMDB_CHECKPOINT_PREFLUSH_BYTES", None)
        env.pop("RMDB_CHECKPOINT_PREFLUSH_PAGES", None)
        self.process = subprocess.Popen([self.binary, "db"], cwd=self.root, env=env, stdout=subprocess.DEVNULL,
                                        stderr=subprocess.DEVNULL, start_new_session=True)
        deadline = time.monotonic() + 20
        last_error = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise ProtocolFailure("rmdb exited before becoming ready")
            try:
                client = WireClient(self.port)
                client.readiness_probe()
                client.close()
                return
            except (OSError, EOFError, ProtocolFailure) as error:
                last_error = error
                time.sleep(0.1)
        raise ProtocolFailure("rmdb never passed Wire SQL readiness: " + str(last_error))

    def stop(self, crash=False):
        if self.process is None or self.process.poll() is not None:
            return
        os.killpg(self.process.pid, signal.SIGKILL if crash else signal.SIGINT)
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=5)

    def cleanup(self):
        self.stop()
        shutil.rmtree(self.root, ignore_errors=True)


def test_stream_prepare_float_and_auto_abort(port):
    client = WireClient(port)
    client.command("CREATE TABLE stock (s_w_id INT, s_i_id INT, s_ytd FLOAT);")
    client.command("CREATE TABLE wire_values (id INT, amount FLOAT, note CHAR(20));")
    client.command("INSERT INTO stock VALUES (1, 2, 1.5);")

    client.command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;")
    statements = [
        (1, False, [INT32, INT32], "UPDATE stock SET s_ytd = s_ytd WHERE s_w_id = $1 AND s_i_id = $2;"),
        (2, False, [FLOAT32, INT32, INT32],
         "UPDATE stock SET s_ytd = s_ytd + $1 WHERE s_w_id = $2 AND s_i_id = $3;"),
        (3, False, [FLOAT32, INT32, INT32],
         "UPDATE stock SET s_ytd = s_ytd - $1 WHERE s_w_id = $2 AND s_i_id = $3;"),
        (4, True, [INT32, INT32], "SELECT s_ytd FROM stock WHERE s_w_id = $1 AND s_i_id = $2;"),
        (5, False, [INT32, FLOAT32, CHAR], "INSERT INTO wire_values VALUES ($1, $2, $3);"),
        (6, True, [INT32], "SELECT amount, note FROM wire_values WHERE id = $1;"),
        (7, False, [], "BEGIN;"),
        (8, False, [FLOAT32, INT32, INT32],
         "UPDATE stock SET s_ytd = s_ytd + $1 WHERE s_w_id = $2 AND s_i_id = $3;"),
        (9, False, [FLOAT32, INT32, INT32],
         "UPDATE stock SET s_ytd = s_ytd / $1 WHERE s_w_id = $2 AND s_i_id = $3;"),
    ]
    schemas = client.prepare(statements)
    parameter_types = {statement_id: parameter_types for statement_id, _, parameter_types, _ in statements}

    executed, status, failed, diagnostic, results = client.batch(
        [(1, [1, 2]), (2, [0x3E800000, 1, 2]), (3, [0x3F000000, 1, 2]), (4, [1, 2])], parameter_types, schemas)
    require((executed, status, failed, diagnostic) == (4, 0, 0xFFFF, ""), "prepared UPDATE batch failed")
    require(results == [(3, [[0x3FA00000]])], "prepared FLOAT result did not preserve raw bits")

    sentinel = 0x40490FDB
    executed, status, failed, diagnostic, results = client.batch([(5, [7, sentinel, "wire-v3"]), (6, [7])],
                                                                   parameter_types, schemas)
    require((executed, status, failed, diagnostic) == (2, 0, 0xFFFF, ""), "typed parameter batch failed")
    require(results == [(1, [[sentinel, "wire-v3"]])], "typed FLOAT/CHAR batch result mismatch")

    executed, status, failed, diagnostic, results = client.batch(
        [(7, []), (8, [0x3F800000, 1, 2]), (9, [0, 1, 2])], parameter_types, schemas)
    require(executed == 2 and status == 2 and failed == 2 and results == [],
            "AUTO_ABORT must return an error batch with no partial query results")
    require(diagnostic, "AUTO_ABORT error must include a diagnostic")
    _, rows = client.query("SELECT s_ytd FROM stock WHERE s_w_id = 1 AND s_i_id = 2;")
    require(rows == [[0x3FA00000]], "AUTO_ABORT did not roll back the preceding prepared update")
    client.close()


def test_snapshot_write_conflict(port):
    setup = WireClient(port)
    setup.command("CREATE TABLE si_probe (id INT, value INT);")
    setup.command("INSERT INTO si_probe VALUES (1, 10);")
    setup.close()

    winner = WireClient(port)
    victim = WireClient(port)
    try:
        winner.command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;")
        victim.command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;")
        winner.command("BEGIN;")
        victim.command("BEGIN;")
        winner.command("UPDATE si_probe SET value = value + 1 WHERE id = 1;")
        winner.command("COMMIT;")
        tag, diagnostic = victim.stream_raw("UPDATE si_probe SET value = value + 1 WHERE id = 1;")
        require(tag == TRANSACTION_ABORT, "stale SNAPSHOT ISOLATION writer must abort at UPDATE")
        require(diagnostic, "SI transaction abort must include a diagnostic")
        _, rows = winner.query("SELECT value FROM si_probe WHERE id = 1;")
        require(rows == [[11]], "SI conflict victim changed the committed value")
    finally:
        winner.close()
        victim.close()


def test_stream_result_sink_ssi(port):
    setup = WireClient(port)
    setup.command("CREATE TABLE ssi_probe (id INT, value INT);")
    setup.command("INSERT INTO ssi_probe VALUES (1, 10);")
    setup.command("INSERT INTO ssi_probe VALUES (2, 20);")
    setup.close()

    reader = WireClient(port)
    writer = WireClient(port)
    try:
        reader.command("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        writer.command("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        reader.command("BEGIN;")
        writer.command("BEGIN;")

        # Both SELECTs go through EXEC_STREAM's result sink.  The first is an
        # empty range (predicate tracking); the second is a nonempty record read.
        _, empty_rows = reader.query("SELECT value FROM ssi_probe WHERE id = 999;")
        require(empty_rows == [], "SSI empty-range setup query was not empty")
        _, rows = writer.query("SELECT value FROM ssi_probe WHERE id = 1;")
        require(rows == [[10]], "SSI nonempty setup query returned the wrong row")

        reader.command("UPDATE ssi_probe SET value = value + 1 WHERE id = 1;")
        tag, diagnostic = writer.stream_raw("INSERT INTO ssi_probe VALUES (999, 999);")
        require(tag == TRANSACTION_ABORT, "SSI writer victim must abort at the conflict statement")
        require(diagnostic, "SSI transaction abort must include a diagnostic")

        _, rows = reader.query("SELECT value FROM ssi_probe WHERE id = 999;")
        require(rows == [], "SSI victim rollback left an inserted row behind")
        reader.command("COMMIT;")
    finally:
        reader.close()
        writer.close()


def test_empty_integer_aggregates_use_typed_zero(port):
    client = WireClient(port)
    try:
        client.command("CREATE TABLE aggregate_empty_probe (id INT, value INT);")
        client.command("INSERT INTO aggregate_empty_probe VALUES (1, NULL);")

        schema, rows = client.query(
            "SELECT SUM(value) AS total, MIN(value) AS lowest, MAX(value) AS highest "
            "FROM aggregate_empty_probe WHERE id = 999;"
        )
        require(schema == [("total", INT32), ("lowest", INT32), ("highest", INT32)],
                "empty integer aggregates must retain INT32 schema")
        require(rows == [[0, 0, 0]], "empty integer aggregates must use the evaluator's typed-zero convention")

        _, rows = client.query(
            "SELECT SUM(value) AS total, MIN(value) AS lowest, MAX(value) AS highest "
            "FROM aggregate_empty_probe WHERE id = 1;"
        )
        require(rows == [[None, None, None]], "nonempty all-NULL aggregate input must remain NULL")
    finally:
        client.close()


def test_direct_column_update(port):
    client = WireClient(port)
    try:
        client.command(
            "CREATE TABLE wire_copy ("
            "id INT, i_src INT, i_dst INT, f_src FLOAT, f_dst FLOAT, "
            "c_src CHAR(8), c_dst CHAR(8), n_src INT, n_dst INT);"
        )
        client.command("INSERT INTO wire_copy VALUES (2, 7, 3, 1.5, 9.5, 'source', 'dest', NULL, 8);")
        client.command(
            "UPDATE wire_copy SET "
            "i_src = i_dst, i_dst = i_src, f_dst = f_src, c_dst = c_src, n_dst = n_src "
            "WHERE id = (1 + 1);"
        )

        schema, rows = client.query(
            "SELECT i_src, i_dst, f_dst, c_dst, n_dst FROM wire_copy WHERE id = 2;"
        )
        require(
            schema == [
                ("i_src", INT32),
                ("i_dst", INT32),
                ("f_dst", FLOAT32),
                ("c_dst", CHAR),
                ("n_dst", INT32),
            ],
            "direct column UPDATE changed typed result schema",
        )
        require(
            rows == [[3, 7, 0x3FC00000, "source", None]],
            "direct column UPDATE did not copy old-row INT/FLOAT/CHAR/NULL values",
        )

        tag, diagnostic = client.stream_raw("UPDATE wire_copy SET i_dst = c_src WHERE id = 2;")
        require(tag == ERROR, "incompatible direct column UPDATE must return ERROR")
        require(diagnostic, "incompatible direct column UPDATE must include a diagnostic")
    finally:
        client.close()


def test_chained_update(port):
    client = WireClient(port)
    try:
        client.command("CREATE TABLE wire_chain (id INT, a INT, b INT, c INT, d INT, f FLOAT, n INT);")
        client.command("INSERT INTO wire_chain VALUES (2, 10, 20, 30, 40, 16777216.0, NULL);")
        client.command(
            "UPDATE wire_chain SET "
            "a = a - 1 + 91, b = b + 3, c = c + 4, d = d + 5 "
            "WHERE id = (1 + 1);"
        )
        client.command("UPDATE wire_chain SET f = f + 1 - 1, n = n - 1 + 91 WHERE id = 2;")

        schema, rows = client.query("SELECT a, b, c, d, f, n FROM wire_chain WHERE id = 2;")
        require(
            schema == [
                ("a", INT32),
                ("b", INT32),
                ("c", INT32),
                ("d", INT32),
                ("f", FLOAT32),
                ("n", INT32),
            ],
            "chained UPDATE changed typed result schema",
        )
        require(
            rows == [[100, 23, 34, 45, 0x4B7FFFFF, None]],
            "chained UPDATE was not left-associative binary32 or did not propagate NULL",
        )

        statements = [
            (100, False, [INT32, INT32], "UPDATE wire_chain SET a = a - $1 + $2 WHERE id = 2;"),
            (101, True, [INT32], "SELECT a FROM wire_chain WHERE id = $1;"),
        ]
        schemas = client.prepare(statements)
        parameter_types = {statement_id: types for statement_id, _, types, _ in statements}
        executed, status, failed, diagnostic, results = client.batch(
            [(100, [10, 5]), (101, [2])], parameter_types, schemas
        )
        require(
            (executed, status, failed, diagnostic) == (2, 0, 0xFFFF, ""),
            "prepared chained UPDATE batch failed",
        )
        require(results == [(1, [[95]])], "prepared chained UPDATE produced a misordered first result")

        executed, status, failed, diagnostic, results = client.batch(
            [(100, [3, 20]), (101, [2])], parameter_types, schemas
        )
        require(
            (executed, status, failed, diagnostic) == (2, 0, 0xFFFF, ""),
            "repeated prepared chained UPDATE batch failed",
        )
        require(results == [(1, [[112]])], "prepared chained UPDATE reused stale scalar terms")

        tag, diagnostic = client.stream_raw("UPDATE wire_chain SET a = b + 1 + 2 WHERE id = 2;")
        require(tag == ERROR and diagnostic, "cross-column chained UPDATE must return diagnostic ERROR")
        tag, diagnostic = client.stream_raw("UPDATE wire_chain SET a = a + 1 + 'bad' WHERE id = 2;")
        require(tag == ERROR and diagnostic, "incompatible chained UPDATE term must return diagnostic ERROR")

        client.command("CREATE TABLE wire_chain_atomic (id INT, a INT, source INT, b INT);")
        client.command("CREATE INDEX wire_chain_atomic(a);")
        client.command("INSERT INTO wire_chain_atomic VALUES (1, 1, 5, 10);")
        client.command("INSERT INTO wire_chain_atomic VALUES (2, 2, 5, 20);")
        tag, diagnostic = client.stream_raw(
            "UPDATE wire_chain_atomic SET a = source, b = b + 1 + 1 WHERE id >= 1;"
        )
        require(tag == ERROR and diagnostic, "indexed chained UPDATE collision must return diagnostic ERROR")
        _, rows = client.query("SELECT a, b FROM wire_chain_atomic WHERE id = 1;")
        require(rows == [[1, 10]], "failed chained UPDATE did not roll back its earlier row")
        _, rows = client.query("SELECT a, b FROM wire_chain_atomic WHERE id = 2;")
        require(rows == [[2, 20]], "failed chained UPDATE changed the conflicting row")
    finally:
        client.close()


def test_crash_recovery_smoke(server):
    client = WireClient(server.port)
    client.command("CREATE TABLE durable_wire (id INT);")
    client.command("INSERT INTO durable_wire VALUES (77);")
    client.close()
    server.stop(crash=True)
    server.start()
    client = WireClient(server.port)
    _, rows = client.query("SELECT id FROM durable_wire WHERE id = 77;")
    require(rows == [[77]], "committed row was lost after SIGKILL and recovery")
    client.close()


def test_abort_ack_then_sigkill_recovers_indexed_undo(server):
    client = WireClient(server.port)
    try:
        client.command("CREATE TABLE abort_crash_wire (id INT, key_col INT, value INT);")
        client.command("CREATE INDEX abort_crash_wire(key_col);")
        client.command("INSERT INTO abort_crash_wire VALUES (1, 10, 100);")
        client.command("INSERT INTO abort_crash_wire VALUES (2, 20, 200);")

        client.command("BEGIN;")
        client.command("INSERT INTO abort_crash_wire VALUES (3, 30, 300);")
        client.command("UPDATE abort_crash_wire SET key_col = 11 WHERE id = 1;")
        client.command("DELETE FROM abort_crash_wire WHERE id = 2;")

        # A separate committed transaction advances the WAL durable frontier
        # past this transaction's DML records without flushing its later ABORT
        # record.  Thus recovery must undo durable loser changes after kill -9,
        # rather than passing merely because none of the aborted work reached
        # the WAL file.
        flusher = WireClient(server.port)
        try:
            flusher.command("INSERT INTO abort_crash_wire VALUES (4, 40, 400);")
        finally:
            flusher.close()

        # COMMAND_OK is the ABORT acknowledgement.  Kill the process only
        # after it has been received, so any unflushed abort record is left to
        # crash recovery rather than a graceful shutdown.
        client.command("ABORT;")
    finally:
        client.close()

    server.stop(crash=True)
    server.start()

    verifier = WireClient(server.port)
    try:
        # The complete table result proves the heap is back at its pre-ABORT
        # state; the equality predicates exercise the secondary index's old
        # and new keys after recovery.
        _, rows = verifier.query("SELECT id, key_col, value FROM abort_crash_wire ORDER BY id;")
        require(rows == [[1, 10, 100], [2, 20, 200], [4, 40, 400]],
                "SIGKILL after ABORT acknowledgement left heap rows behind or changed")
        _, rows = verifier.query("SELECT id FROM abort_crash_wire WHERE key_col = 10;")
        require(rows == [[1]], "recovery lost the pre-ABORT secondary-index entry")
        _, rows = verifier.query("SELECT id FROM abort_crash_wire WHERE key_col = 20;")
        require(rows == [[2]], "recovery did not restore the deleted row's secondary-index entry")
        _, rows = verifier.query("SELECT id FROM abort_crash_wire WHERE key_col = 11;")
        require(rows == [], "recovery retained the aborted UPDATE secondary-index entry")
        _, rows = verifier.query("SELECT id FROM abort_crash_wire WHERE key_col = 30;")
        require(rows == [], "recovery retained the aborted INSERT secondary-index entry")
        _, rows = verifier.query("SELECT id FROM abort_crash_wire WHERE key_col = 40;")
        require(rows == [[4]], "recovery lost the WAL-stabilizing committed row")
    finally:
        verifier.close()


OBSERVABILITY_FIELDS = {
    "obs_abort": {"t_ms", "seq", "shrinking", "upgrade", "deadlock", "ww", "ssi", "unique"},
    "obs_lock": {"t_ms", "seq", "kind", "immediate_conflict", "wait_enqueued", "wait_granted",
                 "wait_cancelled", "wait_ns", "queue_depth_max", "cycle_checks", "cycle_victims"},
    "obs_ckpt": {"t_ms", "seq", "attempt", "preflush", "success", "drain_timeout", "deadline",
                 "final_data_fail", "initial_ns", "preblock_ns", "block_ns", "drain_ns", "final_wal_ns",
                 "final_data_ns", "meta_ns", "manifest_ns", "truncate_ns", "begin_blocked", "begin_wait_ns"},
    "obs_bpm": {"t_ms", "seq", "fetch_miss", "inflight_wait", "inflight_wait_ns", "no_victim",
                "eviction_clean", "eviction_dirty", "page_reads", "page_writes"},
}


def read_observability_snapshots(log_path):
    try:
        with open(log_path, "r", encoding="utf-8") as log_file:
            lines = log_file.read().splitlines()
    except FileNotFoundError:
        return {}

    snapshots = {}
    for line in lines:
        match = re.search(r"\b(obs_abort|obs_lock|obs_ckpt|obs_bpm) (.*)$", line)
        if not match:
            continue
        line_kind, payload = match.groups()
        values = {}
        for token in payload.split():
            key, separator, value = token.partition("=")
            require(separator and key and value, "malformed " + line_kind + " observability field")
            values[key] = value
        require(set(values) == OBSERVABILITY_FIELDS[line_kind], line_kind + " omitted or added observability fields")
        for key, value in values.items():
            if key != "kind":
                require(value.isdigit(), line_kind + " field " + key + " is not an unsigned integer")
        sequence = int(values["seq"])
        snapshot = snapshots.setdefault(sequence, {"obs_lock": {}})
        if line_kind == "obs_lock":
            require(values["kind"] in ("record", "unique"), "obs_lock emitted an unknown kind")
            snapshot["obs_lock"][values["kind"]] = values
        else:
            snapshot[line_kind] = values
    return snapshots


def wait_for_observability_snapshot(log_path, after_sequence):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        snapshots = read_observability_snapshots(log_path)
        for sequence in sorted(snapshots):
            snapshot = snapshots[sequence]
            if sequence <= after_sequence:
                continue
            if ("obs_abort" in snapshot and "obs_ckpt" in snapshot and "obs_bpm" in snapshot and
                    set(snapshot["obs_lock"]) == {"record", "unique"}):
                for line_kind, values in snapshot.items():
                    if line_kind == "obs_lock":
                        for lock_values in values.values():
                            require(int(lock_values["seq"]) == sequence,
                                    "SIGUSR1 observability lines did not share one sequence")
                    else:
                        require(int(values["seq"]) == sequence,
                                "SIGUSR1 observability lines did not share one sequence")
                return sequence, snapshot
        time.sleep(0.05)
    raise ProtocolFailure("SIGUSR1 did not publish complete observability lines")


def force_snapshot_write_conflict(port):
    setup = WireClient(port)
    setup.command("CREATE TABLE obs_si_probe (id INT, value INT);")
    setup.command("INSERT INTO obs_si_probe VALUES (1, 10);")
    setup.close()

    winner = WireClient(port)
    victim = WireClient(port)
    try:
        winner.command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;")
        victim.command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;")
        winner.command("BEGIN;")
        victim.command("BEGIN;")
        winner.command("UPDATE obs_si_probe SET value = value + 1 WHERE id = 1;")
        winner.command("COMMIT;")
        tag, diagnostic = victim.stream_raw("UPDATE obs_si_probe SET value = value + 1 WHERE id = 1;")
        require(tag == TRANSACTION_ABORT and diagnostic,
                "observability probe did not create a SNAPSHOT ISOLATION write conflict")
    finally:
        winner.close()
        victim.close()


def test_sigusr1_observability(server):
    log_path = os.path.join(server.root, "rmdb.log")
    os.kill(server.process.pid, signal.SIGUSR1)
    first_sequence, first = wait_for_observability_snapshot(log_path, 0)

    force_snapshot_write_conflict(server.port)
    os.kill(server.process.pid, signal.SIGUSR1)
    second_sequence, second = wait_for_observability_snapshot(log_path, first_sequence)

    require(second_sequence > first_sequence, "consecutive SIGUSR1 requests did not produce increasing sequences")
    require(int(second["obs_abort"]["ww"]) == int(first["obs_abort"]["ww"]) + 1,
            "one EXEC_STREAM SNAPSHOT ISOLATION write conflict must increment ww exactly once")


def main():
    require(len(sys.argv) == 2, "usage: live_wire_protocol_test.py <rmdb-binary>")
    server = Server(sys.argv[1])
    try:
        server.start()
        test_stream_prepare_float_and_auto_abort(server.port)
        test_snapshot_write_conflict(server.port)
        test_stream_result_sink_ssi(server.port)
        test_empty_integer_aggregates_use_typed_zero(server.port)
        test_direct_column_update(server.port)
        test_chained_update(server.port)
        test_crash_recovery_smoke(server)
        test_abort_ack_then_sigkill_recovers_indexed_undo(server)
        test_sigusr1_observability(server)
        print("live Wire v3 server baseline: PASS")
        return 0
    finally:
        server.cleanup()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ProtocolFailure, EOFError, OSError, struct.error) as error:
        print("live Wire v3 server baseline: FAIL: " + str(error), file=sys.stderr)
        sys.exit(1)
