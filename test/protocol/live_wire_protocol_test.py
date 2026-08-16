#!/usr/bin/env python3
"""Black-box RMDB Wire v3 integration smoke test.

This intentionally speaks the published protocol directly.  It does not use
the legacy text client or in-process executor fixtures, so regressions in the
server's network/result-sink path are observable by CTest.
"""

import os
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
        require(
            recv_exact(self.sock, len(handshake)) == handshake,
            "Wire v3 handshake was not echoed",
        )

    def close(self):
        self.sock.close()

    def write_frame(self, tag, payload=b"", flags=0):
        self.sock.sendall(struct.pack("!IBBH", len(payload), tag, flags, 0) + payload)

    def read_frame(self):
        size, tag, flags, reserved = struct.unpack("!IBBH", recv_exact(self.sock, 8))
        require(
            flags == 0 and reserved == 0,
            "server returned nonzero frame flags or reserved bits",
        )
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
            require(
                tag == RESULT_END and len(payload) == 8,
                "query must end with RESULT_END",
            )
            require(
                struct.unpack("!Q", payload)[0] == len(rows),
                "RESULT_END row count is incorrect",
            )
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
            require(
                tag == RESULT_END and len(payload) == 8,
                "show tables query has no RESULT_END",
            )
            return

    def prepare(self, statements):
        payload = bytearray(struct.pack("!H", len(statements)))
        for statement_id, query, parameter_types, sql in statements:
            encoded_sql = sql.encode("utf-8")
            payload += struct.pack(
                "!H B H", statement_id, int(query), len(parameter_types)
            )
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
            require(
                returned_id == statement_id, "PREPARE_OK changed statement order or id"
            )
            columns = []
            for _ in range(column_count):
                require(offset + 2 <= len(payload), "truncated prepared column name")
                name_size = struct.unpack_from("!H", payload, offset)[0]
                offset += 2
                require(
                    offset + name_size + 1 <= len(payload), "truncated prepared column"
                )
                name = payload[offset : offset + name_size].decode("utf-8")
                offset += name_size
                columns.append((name, payload[offset]))
                offset += 1
            require(
                bool(columns) == bool(query), "prepared command/query schema mismatch"
            )
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
        require(
            offset + diagnostic_size + 2 <= len(payload),
            "truncated BATCH_RESULT diagnostic",
        )
        diagnostic = payload[offset : offset + diagnostic_size].decode(
            "utf-8", "replace"
        )
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
    def __init__(
        self,
        binary,
        env_overrides=None,
        require_sql_readiness=True,
        stderr=subprocess.DEVNULL,
    ):
        self.binary = os.path.abspath(binary)
        self.root = tempfile.mkdtemp(prefix="rmdb-live-wire-")
        self.port = find_free_port()
        self.process = None
        self.env_overrides = env_overrides or {}
        self.require_sql_readiness = require_sql_readiness
        self.stderr = stderr

    def start(self):
        env = os.environ.copy()
        env["RMDB_PORT"] = str(self.port)
        for key, value in self.env_overrides.items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        self.process = subprocess.Popen(
            [self.binary, "db"],
            cwd=self.root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=self.stderr,
            start_new_session=True,
        )
        deadline = time.monotonic() + 20
        last_error = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise ProtocolFailure("rmdb exited before becoming ready")
            try:
                client = WireClient(self.port)
                if self.require_sql_readiness:
                    client.readiness_probe()
                client.close()
                return
            except (OSError, EOFError, ProtocolFailure) as error:
                last_error = error
                time.sleep(0.1)
        raise ProtocolFailure(
            "rmdb never passed Wire SQL readiness: " + str(last_error)
        )

    def stop(self, crash=False, stop_signal=signal.SIGINT):
        if self.process is None or self.process.poll() is not None:
            return
        os.killpg(self.process.pid, signal.SIGKILL if crash else stop_signal)
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
        (
            1,
            False,
            [INT32, INT32],
            "UPDATE stock SET s_ytd = s_ytd WHERE s_w_id = $1 AND s_i_id = $2;",
        ),
        (
            2,
            False,
            [FLOAT32, INT32, INT32],
            "UPDATE stock SET s_ytd = s_ytd + $1 WHERE s_w_id = $2 AND s_i_id = $3;",
        ),
        (
            3,
            False,
            [FLOAT32, INT32, INT32],
            "UPDATE stock SET s_ytd = s_ytd - $1 WHERE s_w_id = $2 AND s_i_id = $3;",
        ),
        (
            4,
            True,
            [INT32, INT32],
            "SELECT s_ytd FROM stock WHERE s_w_id = $1 AND s_i_id = $2;",
        ),
        (
            5,
            False,
            [INT32, FLOAT32, CHAR],
            "INSERT INTO wire_values VALUES ($1, $2, $3);",
        ),
        (6, True, [INT32], "SELECT amount, note FROM wire_values WHERE id = $1;"),
        (7, False, [], "BEGIN;"),
        (
            8,
            False,
            [FLOAT32, INT32, INT32],
            "UPDATE stock SET s_ytd = s_ytd + $1 WHERE s_w_id = $2 AND s_i_id = $3;",
        ),
        (
            9,
            False,
            [FLOAT32, INT32, INT32],
            "UPDATE stock SET s_ytd = s_ytd + $1 WHERE s_w_id = $2 AND s_i_id = $3;",
        ),
    ]
    schemas = client.prepare(statements)
    parameter_types = {
        statement_id: parameter_types
        for statement_id, _, parameter_types, _ in statements
    }

    executed, status, failed, diagnostic, results = client.batch(
        [(1, [1, 2]), (2, [0x3E800000, 1, 2]), (3, [0x3F000000, 1, 2]), (4, [1, 2])],
        parameter_types,
        schemas,
    )
    require(
        (executed, status, failed, diagnostic) == (4, 0, 0xFFFF, ""),
        "prepared UPDATE batch failed",
    )
    require(
        results == [(3, [[0x3FA00000]])],
        "prepared FLOAT result did not preserve raw bits",
    )

    sentinel = 0x40490FDB
    second_float = 0xBF000000
    executed, status, failed, diagnostic, results = client.batch(
        [(5, [7, sentinel, "wire-v3"]), (6, [7])], parameter_types, schemas
    )
    require(
        (executed, status, failed, diagnostic) == (2, 0, 0xFFFF, ""),
        "typed parameter batch failed",
    )
    require(
        results == [(1, [[sentinel, "wire-v3"]])],
        "typed FLOAT/CHAR batch result mismatch",
    )

    executed, status, failed, diagnostic, results = client.batch(
        [(5, [8, second_float, "second"]), (6, [7]), (6, [8])], parameter_types, schemas
    )
    require(
        (executed, status, failed, diagnostic) == (3, 0, 0xFFFF, ""),
        "consecutive prepared INSERT batch failed",
    )
    require(
        results == [(1, [[sentinel, "wire-v3"]]), (2, [[second_float, "second"]])],
        "prepared INSERT reused stale CHAR/FLOAT parameters",
    )

    executed, status, failed, diagnostic, results = client.batch(
        [(5, [9, None, None]), (6, [9]), (6, [7])], parameter_types, schemas
    )
    require(
        (executed, status, failed, diagnostic) == (3, 0, 0xFFFF, ""),
        "prepared NULL/multi-query batch failed",
    )
    require(
        results == [(1, [[None, None]]), (2, [[sentinel, "wire-v3"]])],
        "BATCH_RESULT did not preserve exact NULL cells or query operation order",
    )

    executed, status, failed, diagnostic, results = client.batch(
        [(7, []), (8, [0x3F800000, 1, 2]), (9, [0x7FC00000, 1, 2])], parameter_types, schemas
    )
    require(
        executed == 2 and status == 2 and failed == 2 and results == [],
        "AUTO_ABORT must return an error batch with no partial query results",
    )
    require(diagnostic, "AUTO_ABORT error must include a diagnostic")
    _, rows = client.query("SELECT s_ytd FROM stock WHERE s_w_id = 1 AND s_i_id = 2;")
    require(
        rows == [[0x3FA00000]],
        "AUTO_ABORT did not roll back the preceding prepared update",
    )
    executed, status, failed, diagnostic, results = client.batch(
        [(2, [0x3F800000, 1, 2])], parameter_types, schemas
    )
    require(
        (executed, status, failed, diagnostic, results) == (1, 0, 0xFFFF, "", []),
        "AUTO_ABORT did not clear the explicit transaction before the next operation",
    )
    client.close()


def test_batch_result_payload_limit_rolls_back_without_partial_results(port):
    client = WireClient(port)
    client.command(
        "CREATE TABLE batch_limit_rows (id INT, payload CHAR(255));"
    )
    client.command("CREATE TABLE batch_limit_marker (id INT, value INT);")
    client.command("INSERT INTO batch_limit_marker VALUES (1, 10);")
    payload = "x" * 255
    for row_id in range(46):
        client.command(
            "INSERT INTO batch_limit_rows VALUES "
            f"({row_id}, '{payload}');"
        )

    statements = [
        (1, False, [], "BEGIN;"),
        (
            2,
            False,
            [INT32, INT32],
            "UPDATE batch_limit_marker SET value = value + $1 WHERE id = $2;",
        ),
        (
            3,
            True,
            [],
            "SELECT a.payload, b.payload FROM batch_limit_rows a JOIN batch_limit_rows b;",
        ),
    ]
    schemas = client.prepare(statements)
    parameter_types = {
        statement_id: declared_types
        for statement_id, _, declared_types, _ in statements
    }
    executed, status, failed, diagnostic, results = client.batch(
        [(1, []), (2, [1, 1]), (3, [])], parameter_types, schemas
    )
    require(
        executed == 2 and status == 2 and failed == 2 and results == [],
        "oversized BATCH_RESULT must fail at the query with no partial results",
    )
    require(
        "protocol limit" in diagnostic,
        "oversized BATCH_RESULT must report the payload limit",
    )
    _, rows = client.query("SELECT value FROM batch_limit_marker WHERE id = 1;")
    require(
        rows == [[10]],
        "oversized BATCH_RESULT did not roll back the preceding update",
    )
    client.close()


def test_prepared_select_fast_route(server):
    client = WireClient(server.port)
    try:
        client.command("CREATE TABLE prepared_route (id INT, note CHAR(16));")
        client.command("INSERT INTO prepared_route VALUES (1, 'one');")
        client.command("INSERT INTO prepared_route VALUES (2, 'two');")
        client.command("INSERT INTO prepared_route VALUES (3, 'three');")
        statements = [
            (
                200,
                True,
                [INT32, INT32, INT32],
                "SELECT id, note FROM prepared_route "
                "WHERE id >= $1 AND id <= $1 LIMIT $2 OFFSET $3;",
            ),
        ]
        schemas = client.prepare(statements)
        parameter_types = {200: [INT32, INT32, INT32]}
        executed, status, failed, diagnostic, results = client.batch(
            [(200, [2, 1, 0]), (200, [3, 1, 0])], parameter_types, schemas
        )
        require(
            (executed, status, failed, diagnostic) == (2, 0, 0xFFFF, ""),
            "prepared SELECT fast-route batch failed",
        )
        require(
            results == [(0, [[2, "two"]]), (1, [[3, "three"]])],
            "prepared SELECT reused stale repeated parameters",
        )

    finally:
        client.close()


def test_prepared_and_explicit_transaction_ddl_rejection(port):
    client = WireClient(port)
    try:
        client.command("CREATE TABLE ddl_guard_probe (id INT);")
        statements = [
            (210, True, [INT32], "SELECT id FROM ddl_guard_probe WHERE id = $1;")
        ]
        schemas = client.prepare(statements)
        parameter_types = {210: [INT32]}

        sql = b"CREATE TABLE forbidden_prepared_ddl (id INT);"
        payload = struct.pack("!H H B H I", 1, 211, 0, 0, len(sql)) + sql
        client.write_frame(PREPARE_SET, payload)
        tag, diagnostic = client.read_frame()
        require(tag == ERROR and diagnostic, "PREPARE_SET must reject structural DDL")

        executed, status, failed, diagnostic, results = client.batch(
            [(210, [1])], parameter_types, schemas
        )
        require(
            (executed, status, failed, diagnostic, results)
            == (1, 0, 0xFFFF, "", [(0, [])]),
            "failed PREPARE_SET replaced the prior dictionary",
        )

        client.command("BEGIN;")
        tag, diagnostic = client.stream_raw(
            "CREATE TABLE forbidden_transaction_ddl (id INT);"
        )
        require(
            tag == ERROR and diagnostic,
            "explicit transaction structural DDL must return ERROR",
        )
        client.command("INSERT INTO ddl_guard_probe VALUES (7);")
        client.command("COMMIT;")
        _, rows = client.query("SELECT id FROM ddl_guard_probe WHERE id = 7;")
        require(
            rows == [[7]], "DDL rejection aborted or corrupted the explicit transaction"
        )
    finally:
        client.close()


def test_prepared_transaction_end_commands_share_wire_lifecycle(port):
    client = WireClient(port)
    try:
        client.command("CREATE TABLE prepared_txn_end_probe (id INT);")
        statements = [
            (212, False, [], "BEGIN;"),
            (213, False, [], "INSERT INTO prepared_txn_end_probe VALUES (1);"),
            (214, False, [], "COMMIT;"),
            (215, False, [], "INSERT INTO prepared_txn_end_probe VALUES (2);"),
            (216, False, [], "ROLLBACK;"),
            (217, False, [], "INSERT INTO prepared_txn_end_probe VALUES (3);"),
            (218, False, [], "INSERT INTO prepared_txn_end_probe VALUES (4);"),
            (219, False, [], "ABORT;"),
            (220, False, [], "INSERT INTO prepared_txn_end_probe VALUES (5);"),
        ]
        parameter_types = {statement_id: [] for statement_id, _, _, _ in statements}
        schemas = client.prepare(statements)

        def run(operations, message):
            result = client.batch(operations, parameter_types, schemas)
            require(result == (len(operations), 0, 0xFFFF, "", []), message)

        run([(212, []), (213, []), (214, [])], "prepared COMMIT batch failed")
        _, rows = client.query("SELECT id FROM prepared_txn_end_probe WHERE id = 1;")
        require(rows == [[1]], "prepared COMMIT did not persist its write")

        run([(212, []), (215, []), (216, [])], "prepared ROLLBACK batch failed")
        _, rows = client.query("SELECT id FROM prepared_txn_end_probe WHERE id = 2;")
        require(rows == [], "prepared ROLLBACK did not discard its write")
        run([(212, []), (217, []), (214, [])], "connection was not reusable after prepared ROLLBACK")

        run([(212, []), (218, []), (219, [])], "prepared ABORT batch failed")
        _, rows = client.query("SELECT id FROM prepared_txn_end_probe WHERE id = 4;")
        require(rows == [], "prepared ABORT did not discard its write")
        run([(220, [])], "connection was not reusable after prepared ABORT")
        _, rows = client.query("SELECT id FROM prepared_txn_end_probe WHERE id = 5;")
        require(rows == [[5]], "connection was not reusable after prepared ABORT")
    finally:
        client.close()


def test_prepared_update_is_cold_hot_and_unsupported_shape_falls_back(server):
    client = WireClient(server.port)
    try:
        client.command("CREATE TABLE compiled_fallback_probe (id INT, value INT);")
        client.command("CREATE INDEX compiled_fallback_probe(id);")
        client.command("INSERT INTO compiled_fallback_probe VALUES (1, 10);")
        client.command("INSERT INTO compiled_fallback_probe VALUES (2, 20);")
        statements = [
            (
                220,
                False,
                [INT32, INT32],
                "UPDATE compiled_fallback_probe SET value = value + $1 WHERE id = $2;",
            ),
        ]
        parameter_types = {220: [INT32, INT32]}
        schemas = client.prepare(statements)
        executed, status, failed, diagnostic, results = client.batch(
            [(220, [5, 1])], parameter_types, schemas
        )
        require(
            (executed, status, failed, diagnostic, results) == (1, 0, 0xFFFF, "", []),
            "cold prepared UPDATE failed",
        )

        schemas = client.prepare(statements)
        executed, status, failed, diagnostic, results = client.batch(
            [(220, [7, 1])], parameter_types, schemas
        )
        require(
            (executed, status, failed, diagnostic, results) == (1, 0, 0xFFFF, "", []),
            "hot prepared UPDATE failed",
        )
        _, rows = client.query(
            "SELECT value FROM compiled_fallback_probe WHERE id = 1;"
        )
        require(rows == [[22]], "cold/hot prepared UPDATE changed semantics")

        fallback_statements = [
            (
                221,
                False,
                [INT32],
                "DELETE FROM compiled_fallback_probe WHERE id = $1;",
            ),
        ]
        fallback_parameter_types = {221: [INT32]}
        fallback_schemas = client.prepare(fallback_statements)
        _, rows = client.query(
            "SELECT value FROM compiled_fallback_probe WHERE id = 2;"
        )
        require(rows == [[20]], "PREPARE_SET executed unsupported DELETE")
        executed, status, failed, diagnostic, results = client.batch(
            [(221, [2])], fallback_parameter_types, fallback_schemas
        )
        require(
            (executed, status, failed, diagnostic, results) == (1, 0, 0xFFFF, "", []),
            "unsupported prepared DELETE fallback failed",
        )
        _, rows = client.query(
            "SELECT value FROM compiled_fallback_probe WHERE id = 2;"
        )
        require(rows == [], "unsupported prepared DELETE fallback changed semantics")

    finally:
        client.close()


def test_prepared_revalidation_switches_to_generic_fallback_after_index_change(server):
    client_a = WireClient(server.port)
    client_b = WireClient(server.port)
    try:
        client_a.command("CREATE TABLE prepared_revalidate_probe (a INT, b INT, value INT);")
        client_a.command("INSERT INTO prepared_revalidate_probe VALUES (1, 7, 10);")
        client_a.command("INSERT INTO prepared_revalidate_probe VALUES (2, 7, 20);")
        statements = [(240, True, [INT32], "SELECT value FROM prepared_revalidate_probe WHERE b = $1;")]
        schemas = client_a.prepare(statements)
        parameter_types = {240: [INT32]}
        client_b.command("CREATE INDEX prepared_revalidate_probe(a, b);")
        executed, status, failed, diagnostic, results = client_a.batch(
            [(240, [7])], parameter_types, schemas
        )
        require(
            (executed, status, failed, diagnostic, results) == (1, 0, 0xFFFF, "", [(0, [[10], [20]])]),
            "revalidated prepared SELECT did not use its generic fallback after composite index planning changed",
        )
    finally:
        client_a.close()
        client_b.close()


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
        tag, diagnostic = victim.stream_raw(
            "UPDATE si_probe SET value = value + 1 WHERE id = 1;"
        )
        require(
            tag == TRANSACTION_ABORT,
            "stale SNAPSHOT ISOLATION writer must abort at UPDATE",
        )
        require(diagnostic, "SI transaction abort must include a diagnostic")
        _, rows = winner.query("SELECT value FROM si_probe WHERE id = 1;")
        require(rows == [[11]], "SI conflict victim changed the committed value")
    finally:
        winner.close()
        victim.close()


def test_active_snapshot_delete_conflict_aborts_immediately(port):
    setup = WireClient(port)
    setup.command("CREATE TABLE active_si_delete (id INT, value INT);")
    setup.command("CREATE INDEX active_si_delete(id);")
    setup.command("INSERT INTO active_si_delete VALUES (1, 10);")
    setup.command("INSERT INTO active_si_delete VALUES (2, 20);")
    setup.close()

    winner = WireClient(port)
    victim = WireClient(port)
    verifier = WireClient(port)
    try:
        winner.command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;")
        victim.command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;")
        winner.command("BEGIN;")
        victim.command("BEGIN;")
        winner.command("DELETE FROM active_si_delete WHERE id = 1;")

        victim.sock.settimeout(1)
        tag, diagnostic = victim.stream_raw("DELETE FROM active_si_delete WHERE id = 1;")
        victim.sock.settimeout(5)
        require(
            tag == TRANSACTION_ABORT,
            "active SNAPSHOT ISOLATION DELETE conflict must abort before owner completion",
        )
        require(
            b"write-write conflict" in diagnostic or b"stale" in diagnostic,
            "active SI DELETE abort must report its conflict",
        )

        victim.command("BEGIN;")
        _, rows = victim.query("SELECT value FROM active_si_delete WHERE id = 2;")
        require(rows == [[20]], "DELETE conflict did not fully end the victim transaction")
        victim.command("COMMIT;")

        winner.command("COMMIT;")
        _, rows = verifier.query("SELECT id, value FROM active_si_delete;")
        require(
            rows == [[2, 20]],
            "active SI DELETE conflict did not preserve exactly the winner's delete",
        )
    finally:
        winner.close()
        victim.close()
        verifier.close()


def test_prepared_snapshot_write_conflict(server):
    setup = WireClient(server.port)
    setup.command("CREATE TABLE prepared_si_probe (id INT, value INT);")
    setup.command("CREATE INDEX prepared_si_probe(id);")
    setup.command("INSERT INTO prepared_si_probe VALUES (1, 10);")
    setup.close()

    winner = WireClient(server.port)
    victim = WireClient(server.port)
    try:
        statements = [
            (
                230,
                False,
                [INT32, INT32],
                "UPDATE prepared_si_probe SET value = value + $1 WHERE id = $2;",
            ),
            (
                231,
                True,
                [INT32],
                "SELECT value FROM prepared_si_probe WHERE id = $1;",
            ),
        ]
        parameter_types = {230: [INT32, INT32], 231: [INT32]}
        winner.command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;")
        victim.command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;")
        winner_schemas = winner.prepare(statements)
        victim_schemas = victim.prepare(statements)
        winner.command("BEGIN;")
        victim.command("BEGIN;")

        executed, status, failed, diagnostic, results = winner.batch(
            [(230, [1, 1])], parameter_types, winner_schemas
        )
        require(
            (executed, status, failed, diagnostic, results) == (1, 0, 0xFFFF, "", []),
            "winner prepared SI UPDATE failed",
        )
        winner.command("COMMIT;")

        executed, status, failed, diagnostic, results = victim.batch(
            [(231, [1]), (230, [1, 1])], parameter_types, victim_schemas
        )
        require(
            executed == 1
            and status == 1
            and failed == 1
            and diagnostic
            and results == [],
            "stale prepared SI point UPDATE did not AUTO_ABORT without partial results",
        )
        _, rows = victim.query("SELECT value FROM prepared_si_probe WHERE id = 1;")
        require(rows == [[11]], "stale prepared SI UPDATE changed the committed value")

        victim.command("BEGIN;")
        executed, status, failed, diagnostic, results = victim.batch(
            [(230, [2, 1])], parameter_types, victim_schemas
        )
        require(
            (executed, status, failed, diagnostic, results)
            == (1, 0, 0xFFFF, "", []),
            "connection could not reuse prepared SI point UPDATE after AUTO_ABORT",
        )
        victim.command("COMMIT;")
        _, rows = victim.query("SELECT value FROM prepared_si_probe WHERE id = 1;")
        require(rows == [[13]], "post-AUTO_ABORT prepared point UPDATE did not commit")

    finally:
        winner.close()
        victim.close()


def test_prepared_composite_float_char_point_update(server):
    client = WireClient(server.port)
    try:
        client.command(
            "CREATE TABLE prepared_composite_probe "
            "(f_key FLOAT, c_key CHAR(8), value FLOAT);"
        )
        client.command(
            "CREATE INDEX prepared_composite_probe(f_key, c_key);"
        )
        client.command(
            "INSERT INTO prepared_composite_probe VALUES (1.5, 'ab', 2.25);"
        )
        client.command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;")
        statements = [
            (
                232,
                False,
                [FLOAT32, FLOAT32, CHAR],
                "UPDATE prepared_composite_probe SET value = value + $1 "
                "WHERE f_key = $2 AND c_key = $3;",
            ),
            (
                233,
                True,
                [FLOAT32, CHAR],
                "SELECT value, c_key FROM prepared_composite_probe "
                "WHERE f_key = $1 AND c_key = $2;",
            ),
        ]
        schemas = client.prepare(statements)
        parameter_types = {
            statement_id: types for statement_id, _, types, _ in statements
        }

        executed, status, failed, diagnostic, results = client.batch(
            [(232, [0x3F000000, 0x3FC00000, "ab"]), (233, [0x3FC00000, "ab"])],
            parameter_types,
            schemas,
        )
        require(
            (executed, status, failed, diagnostic)
            == (2, 0, 0xFFFF, ""),
            "composite FLOAT+CHAR prepared point UPDATE failed",
        )
        require(
            results == [(1, [[0x40300000, "ab"]])],
            "composite point UPDATE changed FLOAT32 bits or CHAR length/padding",
        )

        executed, status, failed, diagnostic, results = client.batch(
            [
                (232, [0x3F000000, 0x3FC00000, None]),
                (232, [0x3F000000, None, "ab"]),
                (233, [0x3FC00000, "ab"]),
            ],
            parameter_types,
            schemas,
        )
        require(
            (executed, status, failed, diagnostic)
            == (3, 0, 0xFFFF, ""),
            "NULL composite point keys did not complete as proven no-candidate updates",
        )
        require(
            results == [(2, [[0x40300000, "ab"]])],
            "NULL composite point key mutated the indexed row",
        )

    finally:
        client.close()


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
        require(
            tag == TRANSACTION_ABORT,
            "SSI writer victim must abort at the conflict statement",
        )
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
        require(
            schema == [("total", INT32), ("lowest", INT32), ("highest", INT32)],
            "empty integer aggregates must retain INT32 schema",
        )
        require(
            rows == [[0, 0, 0]],
            "empty integer aggregates must use the evaluator's typed-zero convention",
        )

        _, rows = client.query(
            "SELECT SUM(value) AS total, MIN(value) AS lowest, MAX(value) AS highest "
            "FROM aggregate_empty_probe WHERE id = 1;"
        )
        require(
            rows == [[None, None, None]],
            "nonempty all-NULL aggregate input must remain NULL",
        )
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
        client.command(
            "INSERT INTO wire_copy VALUES (2, 7, 3, 1.5, 9.5, 'source', 'dest', NULL, 8);"
        )
        client.command(
            "UPDATE wire_copy SET "
            "i_src = i_dst, i_dst = i_src, f_dst = f_src, c_dst = c_src, n_dst = n_src "
            "WHERE id = (1 + 1);"
        )

        schema, rows = client.query(
            "SELECT i_src, i_dst, f_dst, c_dst, n_dst FROM wire_copy WHERE id = 2;"
        )
        require(
            schema
            == [
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

        tag, diagnostic = client.stream_raw(
            "UPDATE wire_copy SET i_dst = c_src WHERE id = 2;"
        )
        require(tag == ERROR, "incompatible direct column UPDATE must return ERROR")
        require(
            diagnostic, "incompatible direct column UPDATE must include a diagnostic"
        )
    finally:
        client.close()


def test_chained_update(port):
    client = WireClient(port)
    try:
        client.command(
            "CREATE TABLE wire_chain (id INT, a INT, b INT, c INT, d INT, f FLOAT, n INT);"
        )
        client.command(
            "INSERT INTO wire_chain VALUES (2, 10, 20, 30, 40, 16777216.0, NULL);"
        )
        client.command(
            "UPDATE wire_chain SET "
            "a = a - 1 + 91, b = b + 3, c = c + 4, d = d + 5 "
            "WHERE id = (1 + 1);"
        )
        client.command(
            "UPDATE wire_chain SET f = f + 1 - 1, n = n - 1 + 91 WHERE id = 2;"
        )

        schema, rows = client.query(
            "SELECT a, b, c, d, f, n FROM wire_chain WHERE id = 2;"
        )
        require(
            schema
            == [
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
            (
                100,
                False,
                [INT32, INT32],
                "UPDATE wire_chain SET a = a - $1 + $2 WHERE id = 2;",
            ),
            (101, True, [INT32], "SELECT a FROM wire_chain WHERE id = $1;"),
        ]
        schemas = client.prepare(statements)
        parameter_types = {
            statement_id: types for statement_id, _, types, _ in statements
        }
        executed, status, failed, diagnostic, results = client.batch(
            [(100, [10, 5]), (101, [2])], parameter_types, schemas
        )
        require(
            (executed, status, failed, diagnostic) == (2, 0, 0xFFFF, ""),
            "prepared chained UPDATE batch failed",
        )
        require(
            results == [(1, [[95]])],
            "prepared chained UPDATE produced a misordered first result",
        )

        executed, status, failed, diagnostic, results = client.batch(
            [(100, [3, 20]), (101, [2])], parameter_types, schemas
        )
        require(
            (executed, status, failed, diagnostic) == (2, 0, 0xFFFF, ""),
            "repeated prepared chained UPDATE batch failed",
        )
        require(
            results == [(1, [[112]])],
            "prepared chained UPDATE reused stale scalar terms",
        )

        tag, diagnostic = client.stream_raw(
            "UPDATE wire_chain SET a = b + 1 + 2 WHERE id = 2;"
        )
        require(
            tag == ERROR and diagnostic,
            "cross-column chained UPDATE must return diagnostic ERROR",
        )
        tag, diagnostic = client.stream_raw(
            "UPDATE wire_chain SET a = a + 1 + 'bad' WHERE id = 2;"
        )
        require(
            tag == ERROR and diagnostic,
            "incompatible chained UPDATE term must return diagnostic ERROR",
        )

        client.command(
            "CREATE TABLE wire_chain_atomic (id INT, a INT, source INT, b INT);"
        )
        client.command("CREATE INDEX wire_chain_atomic(a);")
        client.command("INSERT INTO wire_chain_atomic VALUES (1, 1, 5, 10);")
        client.command("INSERT INTO wire_chain_atomic VALUES (2, 2, 5, 20);")
        tag, diagnostic = client.stream_raw(
            "UPDATE wire_chain_atomic SET a = source, b = b + 1 + 1 WHERE id >= 1;"
        )
        require(
            tag == ERROR and diagnostic,
            "indexed chained UPDATE collision must return diagnostic ERROR",
        )
        _, rows = client.query("SELECT a, b FROM wire_chain_atomic WHERE id = 1;")
        require(
            rows == [[1, 10]], "failed chained UPDATE did not roll back its earlier row"
        )
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
        client.command(
            "CREATE TABLE abort_crash_wire (id INT, key_col INT, value INT);"
        )
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
        _, rows = verifier.query(
            "SELECT id, key_col, value FROM abort_crash_wire ORDER BY id;"
        )
        require(
            rows == [[1, 10, 100], [2, 20, 200], [4, 40, 400]],
            "SIGKILL after ABORT acknowledgement left heap rows behind or changed",
        )
        _, rows = verifier.query("SELECT id FROM abort_crash_wire WHERE key_col = 10;")
        require(rows == [[1]], "recovery lost the pre-ABORT secondary-index entry")
        _, rows = verifier.query("SELECT id FROM abort_crash_wire WHERE key_col = 20;")
        require(
            rows == [[2]],
            "recovery did not restore the deleted row's secondary-index entry",
        )
        _, rows = verifier.query("SELECT id FROM abort_crash_wire WHERE key_col = 11;")
        require(
            rows == [], "recovery retained the aborted UPDATE secondary-index entry"
        )
        _, rows = verifier.query("SELECT id FROM abort_crash_wire WHERE key_col = 30;")
        require(
            rows == [], "recovery retained the aborted INSERT secondary-index entry"
        )
        _, rows = verifier.query("SELECT id FROM abort_crash_wire WHERE key_col = 40;")
        require(rows == [[4]], "recovery lost the WAL-stabilizing committed row")
    finally:
        verifier.close()


def test_unique_index_auto_abort_then_sigkill_has_no_residue(server):
    client = WireClient(server.port)
    try:
        client.command(
            "CREATE TABLE empty_write_abort_wire (id INT, key_col INT, value INT);"
        )
        # The first index accepts the losing INSERT before the second index
        # rejects its duplicate key. The executor has appended DML WAL but has
        # not yet added a WriteRecord when it rolls that partial work back.
        client.command("CREATE INDEX empty_write_abort_wire(id);")
        client.command("CREATE INDEX empty_write_abort_wire(key_col);")
        client.command("INSERT INTO empty_write_abort_wire VALUES (1, 10, 100);")
        client.command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;")
        statements = [
            (101, False, [], "BEGIN;"),
            (
                102,
                False,
                [INT32, INT32, INT32],
                "INSERT INTO empty_write_abort_wire VALUES ($1, $2, $3);",
            ),
        ]
        schemas = client.prepare(statements)
        parameter_types = {
            statement_id: types for statement_id, _, types, _ in statements
        }

        wal_path = os.path.join(server.root, "db", "db.log")
        wal_size_before = os.path.getsize(wal_path)
        executed, status, failed, diagnostic, results = client.batch(
            [(101, []), (102, [2, 10, 200])], parameter_types, schemas
        )
        require(
            (executed, status, failed) == (1, 1, 1),
            "duplicate second-index key must return an AUTO_ABORT transaction abort",
        )
        require(
            diagnostic and results == [],
            "AUTO_ABORT transaction abort must carry a diagnostic and no partial results",
        )

        # A broken write_set-only fast path could otherwise pass the recovery
        # check by losing both the unflushed INSERT WAL and its in-memory
        # partial work at SIGKILL. Prove the acknowledged abort advanced the
        # real WAL file before any later transaction can flush it incidentally.
        wal_size_after_abort = os.path.getsize(wal_path)
        require(
            wal_size_after_abort > wal_size_before,
            "AUTO_ABORT acknowledgement did not publish the losing transaction's WAL",
        )

        # The failed batch has ended its transaction, so the same connection
        # must immediately observe only the pre-transaction row.
        _, rows = client.query(
            "SELECT id, key_col, value FROM empty_write_abort_wire ORDER BY id;"
        )
        require(
            rows == [[1, 10, 100]],
            "AUTO_ABORT left partial heap work visible before SIGKILL",
        )

        # Advance the durable frontier after the failed ACK as an independent
        # guard that the complete loser chain is present at crash time.
        flusher = WireClient(server.port)
        try:
            flusher.command("INSERT INTO empty_write_abort_wire VALUES (3, 30, 300);")
        finally:
            flusher.close()
    finally:
        client.close()

    # BATCH_RESULT/TRANSACTION_ABORT acknowledges that rollback is complete.
    server.stop(crash=True)
    server.start()

    verifier = WireClient(server.port)
    try:
        _, rows = verifier.query(
            "SELECT id, key_col, value FROM empty_write_abort_wire ORDER BY id;"
        )
        require(
            rows == [[1, 10, 100], [3, 30, 300]],
            "unique-index AUTO_ABORT left a heap row after SIGKILL",
        )
        _, rows = verifier.query("SELECT id FROM empty_write_abort_wire WHERE id = 2;")
        require(
            rows == [],
            "unique-index abort left the first secondary-index entry after SIGKILL",
        )
        _, rows = verifier.query(
            "SELECT id FROM empty_write_abort_wire WHERE key_col = 10;"
        )
        require(
            rows == [[1]],
            "unique-index abort damaged the conflicting secondary index after SIGKILL",
        )
        _, rows = verifier.query(
            "SELECT id FROM empty_write_abort_wire WHERE key_col = 30;"
        )
        require(rows == [[3]], "recovery lost the post-abort WAL-flushing transaction")
    finally:
        verifier.close()


def test_sigint_drains_connected_client_and_restarts(server):
    client = WireClient(server.port)
    try:
        server.stop()
        require(
            server.process.poll() is not None,
            "SIGINT did not stop server while a client was connected",
        )
        client.sock.settimeout(2)
        try:
            require(client.sock.recv(1) == b"", "SIGINT left connected client socket open")
        except OSError:
            pass
    finally:
        client.close()

    server.start()
    verifier = WireClient(server.port)
    try:
        verifier.readiness_probe()
    finally:
        verifier.close()


def test_connection_churn_then_sigterm_restarts(server):
    for _ in range(64):
        client = WireClient(server.port)
        try:
            client.readiness_probe()
        finally:
            client.close()

    client = WireClient(server.port)
    try:
        server.stop(stop_signal=signal.SIGTERM)
        require(
            server.process.poll() is not None,
            "SIGTERM did not stop server after connection churn",
        )
        client.sock.settimeout(2)
        require(client.sock.recv(1) == b"", "SIGTERM left connected client socket open")
    finally:
        client.close()

    server.start()
    verifier = WireClient(server.port)
    try:
        verifier.readiness_probe()
    finally:
        verifier.close()


def main():
    require(len(sys.argv) == 2, "usage: live_wire_protocol_test.py <rmdb-binary>")
    server = Server(sys.argv[1], {"RMDB_STORAGE_ENGINE": "legacy"})
    try:
        server.start()
        test_sigint_drains_connected_client_and_restarts(server)
        test_connection_churn_then_sigterm_restarts(server)
        test_stream_prepare_float_and_auto_abort(server.port)
        test_batch_result_payload_limit_rolls_back_without_partial_results(
            server.port
        )
        test_prepared_select_fast_route(server)
        test_prepared_and_explicit_transaction_ddl_rejection(server.port)
        test_prepared_transaction_end_commands_share_wire_lifecycle(server.port)
        test_prepared_update_is_cold_hot_and_unsupported_shape_falls_back(server)
        test_prepared_revalidation_switches_to_generic_fallback_after_index_change(server)
        test_snapshot_write_conflict(server.port)
        test_active_snapshot_delete_conflict_aborts_immediately(server.port)
        test_prepared_snapshot_write_conflict(server)
        test_prepared_composite_float_char_point_update(server)
        test_stream_result_sink_ssi(server.port)
        test_empty_integer_aggregates_use_typed_zero(server.port)
        test_direct_column_update(server.port)
        test_chained_update(server.port)
        test_crash_recovery_smoke(server)
        test_unique_index_auto_abort_then_sigkill_has_no_residue(server)
        test_abort_ack_then_sigkill_recovers_indexed_undo(server)
    finally:
        server.cleanup()

    print("live Wire v3 server baseline: PASS")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ProtocolFailure, EOFError, OSError, struct.error) as error:
        print("live Wire v3 server baseline: FAIL: " + str(error), file=sys.stderr)
        sys.exit(1)
