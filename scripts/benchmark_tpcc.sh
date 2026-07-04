#!/usr/bin/env bash
# TPC-C benchmark with crash-restart consistency verification.
#
# Flow: start rmdb -> load + run -> kill -9 -> restart rmdb (wait for WAL
# recovery) -> run consistency check against the recovered database.
#
# The consistency phase reads baseline counts and the committed new_order total
# from the result.json written by the run phase, so it does not depend on any
# in-memory state across the crash/restart.

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_LOG="$ROOT_DIR/benchmark/tpcc/rmdb-server.log"

BINARY="$ROOT_DIR/build/bin/rmdb"
DB_DIR="tpcc_benchmark_db"
PORT=8765
WAREHOUSES=1
WORKERS=16
WARMUP=30
MEASURE=360
ROUNDS=3
PROGRESS_INTERVAL=5
DATA_DIR="$ROOT_DIR/benchmark/tpcc/data"
JSON_OUT="$ROOT_DIR/benchmark/tpcc/result.json"
RMDB_DB_DIR=""
RESTART_TIMEOUT=120
REGENERATE_DATA=0
DATA_ARGS="--reuse-data-dir"

usage() {
    cat <<EOF
Usage: $0 [options]
  --binary PATH            rmdb binary (default: build/bin/rmdb)
  --db-dir PATH            database directory (default: tpcc_benchmark_db)
  --port N                 server port (default: 8765)
  --warehouses N           TPC-C warehouses (default: 1)
  --workers N              concurrent workers (default: 16)
  --warmup N               warmup seconds (default: 30)
  --measure N              measure seconds (default: 360)
  --rounds N               benchmark rounds (default: 3)
  --progress-interval N    progress print interval seconds (default: 5)
  --data-dir PATH          CSV data directory
  --json-out PATH          result.json path
  --rmdb-db-dir PATH       passed through to tpcc_run
  --restart-timeout N      seconds to wait for rmdb recovery after restart (default: 120)
  --regenerate-data        rebuild CSV data instead of reusing
  --overwrite-data-dir     alias for regenerate (passed to tpcc_run)
  --reuse-data-dir         reuse CSV data (passed to tpcc_run, default)
  -h, --help               show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --binary) BINARY="$2"; shift 2 ;;
        --db-dir) DB_DIR="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --warehouses) WAREHOUSES="$2"; shift 2 ;;
        --workers) WORKERS="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --measure) MEASURE="$2"; shift 2 ;;
        --rounds) ROUNDS="$2"; shift 2 ;;
        --progress-interval) PROGRESS_INTERVAL="$2"; shift 2 ;;
        --data-dir) DATA_DIR="$2"; shift 2 ;;
        --json-out) JSON_OUT="$2"; shift 2 ;;
        --rmdb-db-dir) RMDB_DB_DIR="$2"; shift 2 ;;
        --restart-timeout) RESTART_TIMEOUT="$2"; shift 2 ;;
        --regenerate-data) REGENERATE_DATA=1; shift ;;
        --overwrite-data-dir) DATA_ARGS="--overwrite-data-dir"; shift ;;
        --reuse-data-dir) DATA_ARGS="--reuse-data-dir"; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ "$REGENERATE_DATA" -eq 1 ]]; then
    rm -rf "$DATA_DIR"
    DATA_ARGS="--overwrite-data-dir"
fi

# Auto-detect whether the CSV test data set is present in $DATA_DIR; if it is
# missing or incomplete, run the datagen phase before load/run so `make
# benchmark` works on a fresh checkout without a manual datagen step.
DATA_COMPLETE=0
if (cd "$ROOT_DIR" && python3 - "$DATA_DIR" <<'PY'
import sys
from pathlib import Path
from benchmark.tpcc.phases.load import TABLES
from benchmark.tpcc.phases.datagen import complete_csv_set
sys.exit(0 if complete_csv_set(Path(sys.argv[1]), TABLES) else 1)
PY
); then
    DATA_COMPLETE=1
fi

PHASES="load run"
if [[ "$DATA_COMPLETE" -eq 0 ]]; then
    echo "[benchmark] $DATA_DIR 缺少或不完整的 CSV 测试数据，自动生成"
    PHASES="datagen load run"
    # Force overwrite so datagen proceeds even if some partial CSVs exist.
    DATA_ARGS="--overwrite-data-dir"
fi

if [[ ! -x "$BINARY" ]]; then
    echo "missing server binary: $BINARY" >&2
    echo "run: make build" >&2
    exit 1
fi

rm -rf "$DB_DIR" "$JSON_OUT" "$SERVER_LOG"

SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -KILL "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

wait_port() {
    local timeout="$1"
    python3 - "$PORT" "$timeout" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
timeout = int(sys.argv[2])
deadline = time.time() + timeout
last_error = None
while time.time() < deadline:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.5)
    try:
        sock.connect(("127.0.0.1", port))
        sock.close()
        sys.exit(0)
    except OSError as exc:
        last_error = exc
        time.sleep(0.2)
    finally:
        sock.close()
print(
    f"server did not become ready on port {port} within {timeout}s: {last_error}",
    file=sys.stderr,
)
sys.exit(1)
PY
}

RMDB_DB_ARG=()
[[ -n "$RMDB_DB_DIR" ]] && RMDB_DB_ARG=(--rmdb-db-dir "$RMDB_DB_DIR")

echo "[benchmark] 启动 rmdb server (db=$DB_DIR)"
"$BINARY" "$DB_DIR" >> "$SERVER_LOG" 2>&1 &
SERVER_PID=$!
wait_port 30

echo "[benchmark] $PHASES: warehouses=$WAREHOUSES workers=$WORKERS warmup=${WARMUP}s measure=${MEASURE}s rounds=$ROUNDS"
python3 -m benchmark.tpcc.tpcc_run $PHASES \
    --backend rmdb \
    --port "$PORT" \
    --warehouses "$WAREHOUSES" \
    --workers "$WORKERS" \
    --warmup "$WARMUP" \
    --measure "$MEASURE" \
    --rounds "$ROUNDS" \
    --progress-interval "$PROGRESS_INTERVAL" \
    --data-dir "$DATA_DIR" \
    --json-out "$JSON_OUT" \
    "${RMDB_DB_ARG[@]}" \
    $DATA_ARGS

echo "[benchmark] kill -9 rmdb server (pid=$SERVER_PID) 触发崩溃恢复"
kill -KILL "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

echo "[benchmark] 重启 rmdb server，等待恢复就绪 (最长 ${RESTART_TIMEOUT}s)"
"$BINARY" "$DB_DIR" >> "$SERVER_LOG" 2>&1 &
SERVER_PID=$!
wait_port "$RESTART_TIMEOUT"

echo "[benchmark] 一致性检查 (基于 $JSON_OUT)"
set +e
python3 -m benchmark.tpcc.tpcc_run consistency \
    --backend rmdb \
    --port "$PORT" \
    --result-json "$JSON_OUT"
consistency_status=$?
set -e

kill -KILL "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

exit "$consistency_status"
