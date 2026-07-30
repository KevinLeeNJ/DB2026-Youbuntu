#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVER_BIN="$ROOT_DIR/build/bin/rmdb"
DB_NAME="live_crash_checkpoint_db"
PORT=8765
DO_BUILD=1

if [[ "${1:-}" == "--skip-build" ]]; then
    DO_BUILD=0
fi

if [[ "$DO_BUILD" -eq 1 ]]; then
    make -C "$ROOT_DIR" build
fi

if [[ ! -x "$SERVER_BIN" ]]; then
    echo "missing server binary: $SERVER_BIN" >&2
    echo "run: make build" >&2
    exit 1
fi

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/rmdb-live-crash.XXXXXX")"
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -INT "$SERVER_PID" 2>/dev/null || true
        for _ in {1..20}; do
            if ! kill -0 "$SERVER_PID" 2>/dev/null; then
                break
            fi
            sleep 0.1
        done
        if kill -0 "$SERVER_PID" 2>/dev/null; then
            kill -TERM "$SERVER_PID" 2>/dev/null || true
        fi
        for _ in {1..20}; do
            if ! kill -0 "$SERVER_PID" 2>/dev/null; then
                break
            fi
            sleep 0.1
        done
        if kill -0 "$SERVER_PID" 2>/dev/null; then
            kill -KILL "$SERVER_PID" 2>/dev/null || true
        fi
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    if [[ "${KEEP_RMDB_LIVE_CRASH_WORKDIR:-0}" == "1" ]]; then
        echo "kept workspace: $WORK_DIR"
    else
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup EXIT

assert_port_free() {
    python3 - "$PORT" <<'PY'
import socket
import sys

port = int(sys.argv[1])
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(0.2)
try:
    sock.connect(("127.0.0.1", port))
except OSError:
    sys.exit(0)
else:
    print(f"port {port} is already in use", file=sys.stderr)
    sys.exit(1)
finally:
    sock.close()
PY
}

wait_for_server() {
    python3 - "$PORT" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
deadline = time.time() + 10
last_error = None
while time.time() < deadline:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.2)
    try:
        sock.connect(("127.0.0.1", port))
        sock.close()
        sys.exit(0)
    except OSError as exc:
        last_error = exc
        time.sleep(0.1)
    finally:
        sock.close()

print(f"server did not become ready on port {port}: {last_error}", file=sys.stderr)
sys.exit(1)
PY
}

start_server() {
    local log_name="$1"
    assert_port_free
    pushd "$WORK_DIR" >/dev/null
    "$SERVER_BIN" "$DB_NAME" >"$log_name.out" 2>"$log_name.err" &
    SERVER_PID=$!
    popd >/dev/null
    wait_for_server
}

stop_server_gracefully() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -INT "$SERVER_PID"
        for _ in {1..50}; do
            if ! kill -0 "$SERVER_PID" 2>/dev/null; then
                break
            fi
            sleep 0.1
        done
        if kill -0 "$SERVER_PID" 2>/dev/null; then
            kill -TERM "$SERVER_PID" 2>/dev/null || true
        fi
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
    fi
}

run_crash_workload() {
    python3 - "$ROOT_DIR" "$PORT" "$SERVER_PID" <<'PY'
import os
import signal
import sys

root_dir = sys.argv[1]
port = int(sys.argv[2])
server_pid = int(sys.argv[3])
sys.path.insert(0, root_dir + "/test/protocol")

from live_wire_protocol_test import WireClient

client = WireClient(port)
try:
    for sql in [
        "create table t (id int, v int);",
        "insert into t values (1, 10);",
        "insert into t values (2, 20);",
        "create static_checkpoint;",
        "begin;",
        "insert into t values (3, 30);",
        "update t set v = 999 where id = 1;",
        "delete from t where id = 2;",
    ]:
        client.command(sql)

    os.kill(server_pid, signal.SIGKILL)
finally:
    client.close()
PY
}

verify_recovery_and_append() {
    python3 - "$ROOT_DIR" "$PORT" <<'PY'
import sys

root_dir = sys.argv[1]
port = int(sys.argv[2])
sys.path.insert(0, root_dir + "/test/protocol")

from live_wire_protocol_test import WireClient

client = WireClient(port)
try:
    _, recovered = client.query("select * from t;")
    recovered_rows = sorted(tuple(row) for row in recovered)
    expected_recovered = [(1, 10), (2, 20)]
    if recovered_rows != expected_recovered:
        raise AssertionError(
            f"expected recovered rows {expected_recovered}, got {recovered_rows}"
        )

    client.command("insert into t values (4, 40);")

    _, after_append = client.query("select * from t;")
    after_append_rows = sorted(tuple(row) for row in after_append)
    expected_after_append = [(1, 10), (2, 20), (4, 40)]
    if after_append_rows != expected_after_append:
        raise AssertionError(
            f"expected rows after append {expected_after_append}, got {after_append_rows}"
        )

    print("Recovered rows after crash:", recovered_rows)
    print()
    print("Rows after post-recovery append:", after_append_rows)
finally:
    client.close()
PY
}

echo "workspace: $WORK_DIR"
echo "phase 1: start server, create checkpoint, leave transaction uncommitted, crash"
start_server "server-before-crash"
run_crash_workload

set +e
wait "$SERVER_PID"
CRASH_STATUS=$?
set -e
SERVER_PID=""

if [[ "$CRASH_STATUS" -eq 0 ]]; then
    echo "expected crash command to terminate server with non-zero status" >&2
    exit 1
fi

echo "phase 2: restart server on the same database and verify recovery"
start_server "server-after-crash"
verify_recovery_and_append
stop_server_gracefully

echo "live crash checkpoint test passed"
