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
WAREHOUSES=50
WORKERS=50
WARMUP=30
MEASURE=150
ROUNDS=3
PROGRESS_INTERVAL=5
DATA_DIR="$ROOT_DIR/benchmark/tpcc/data"
JSON_OUT="$ROOT_DIR/benchmark/tpcc/result.json"
RMDB_DB_DIR=""
RESTART_TIMEOUT=600
REGENERATE_DATA=0
THINK_MS=0
RECONNECT_EACH_TXN=0
ISOLATION="read-committed"
GO_BINARY="$ROOT_DIR/build/bin/tpcc-go"

usage() {
    cat <<EOF
Usage: $0 [options]
  --binary PATH            rmdb binary (default: build/bin/rmdb)
  --db-dir PATH            database directory (default: tpcc_benchmark_db)
  --port N                 server port (default: 8765)
  --warehouses N           TPC-C warehouses (default: 50)
  --workers N              concurrent workers (default: 50)
  --warmup N               warmup seconds (default: 30)
  --measure N              measure seconds (default: 150)
  --rounds N               benchmark rounds (default: 3)
  --progress-interval N    progress print interval seconds (default: 5)
  --data-dir PATH          CSV data directory
  --json-out PATH          result.json path
  --rmdb-db-dir PATH       RMDB directory used to resolve CSV load paths
  --restart-timeout N      seconds to wait for rmdb recovery after restart (default: 600)
  --think-ms N             pause N milliseconds between transactions (default: 0)
  --reconnect-each-txn 0|1 reconnect each worker after every transaction (default: 0)
  --isolation LEVEL        read-committed or snapshot-isolation (default: read-committed)
  --go-binary PATH         Go runner binary (default: build/bin/tpcc-go)
  --regenerate-data        rebuild CSV data instead of reusing
  --overwrite-data-dir     alias for regenerate
  --reuse-data-dir         reuse existing CSV data (default)
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
        --think-ms) THINK_MS="$2"; shift 2 ;;
        --reconnect-each-txn) RECONNECT_EACH_TXN="$2"; shift 2 ;;
        --isolation) ISOLATION="$2"; shift 2 ;;
        --go-binary) GO_BINARY="$2"; shift 2 ;;
        --regenerate-data|--overwrite-data-dir) REGENERATE_DATA=1; shift ;;
        --reuse-data-dir) shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ ! "$THINK_MS" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "--think-ms must be a non-negative number" >&2
    exit 2
fi
if [[ "$RECONNECT_EACH_TXN" != "0" && "$RECONNECT_EACH_TXN" != "1" ]]; then
    echo "--reconnect-each-txn must be 0 or 1" >&2
    exit 2
fi
if [[ "$ISOLATION" != "read-committed" && "$ISOLATION" != "snapshot-isolation" ]]; then
    echo "--isolation must be read-committed or snapshot-isolation" >&2
    exit 2
fi
if [[ ! -x "$GO_BINARY" ]]; then
    echo "missing Go benchmark binary: $GO_BINARY" >&2
    exit 1
fi
if [[ ! "$ROUNDS" =~ ^[1-9][0-9]*$ ]]; then
    echo "--rounds must be a positive integer" >&2
    exit 2
fi

if [[ "$REGENERATE_DATA" -eq 1 ]]; then
    rm -rf "$DATA_DIR"
fi

# Auto-detect whether the CSV test data set is present. The Go runner owns
# generation as well as the high-concurrency workload.
if ! "$GO_BINARY" --command data-ready --data-dir "$DATA_DIR" --warehouses "$WAREHOUSES" --seed 1; then
    echo "[benchmark] $DATA_DIR 缺少或不完整的 CSV 测试数据，自动生成"
    "$GO_BINARY" --command datagen --warehouses "$WAREHOUSES" --data-dir "$DATA_DIR" --seed 1 --overwrite-data-dir
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
    "$GO_BINARY" --command wait-ready --port "$PORT" --wait-timeout "${timeout}s"
}

GO_RECONNECT_ARGS=()
if [[ "$RECONNECT_EACH_TXN" == "1" ]]; then
    GO_RECONNECT_ARGS=(--reconnect-each-txn)
fi

rm -rf "$DB_DIR" "$JSON_OUT"
echo "[benchmark] official-equivalent: one ${WARMUP}s warmup + $ROUNDS continuous ${MEASURE}s windows"
"$BINARY" "$DB_DIR" >> "$SERVER_LOG" 2>&1 &
SERVER_PID=$!
wait_port 30
"$GO_BINARY" --command load \
    --port "$PORT" --isolation "$ISOLATION" \
    --data-dir "$DATA_DIR" --schema-dir "$ROOT_DIR/benchmark/tpcc/schema" \
    --rmdb-db-dir "${RMDB_DB_DIR:-$DB_DIR}"
"$GO_BINARY" --mode official-equivalent --port "$PORT" --isolation "$ISOLATION" \
    --workers "$WORKERS" --warmup "$WARMUP" --measure "$MEASURE" --rounds "$ROUNDS" \
    --progress-interval "$PROGRESS_INTERVAL" --warehouse-policy official-terminal-home \
    --think "${THINK_MS}ms" --json-out "$JSON_OUT" "${GO_RECONNECT_ARGS[@]}"
echo "[benchmark] official-equivalent: SIGKILL and recovery check"
kill -KILL "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""
"$BINARY" "$DB_DIR" >> "$SERVER_LOG" 2>&1 &
SERVER_PID=$!
wait_port "$RESTART_TIMEOUT"
"$GO_BINARY" --command consistency --port "$PORT" --isolation "$ISOLATION" \
    --consistency-stage post-recovery-official-equivalent --result-json "$JSON_OUT"
echo "[benchmark] official-equivalent complete: result=$JSON_OUT"
