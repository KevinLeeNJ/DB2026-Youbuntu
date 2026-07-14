#!/usr/bin/env bash
# Run a TPC-C workload and kill the RMDB server at random points while it is
# serving transactions. Each cycle restarts the same database and runs the
# benchmark consistency checker before the next crash.

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="$ROOT_DIR/build/bin/rmdb"
GO_BINARY="$ROOT_DIR/build/bin/tpcc-go"
DB_DIR="tpcc_random_kill_db"
PORT=8765
WORKERS=16
WARMUP=2
MEASURE=45
CYCLES=5
MIN_KILL_DELAY=3
MAX_KILL_DELAY=8
RESTART_TIMEOUT=120
DATA_DIR="$ROOT_DIR/benchmark/tpcc/data"
SCHEMA_DIR="$ROOT_DIR/benchmark/tpcc/schema"
WORK_DIR=""
SERVER_LOG=""
ISOLATION="read-committed"
SEED=1

usage() {
    cat <<EOF
Usage: $0 [options]
  --binary PATH            rmdb binary (default: build/bin/rmdb)
  --go-binary PATH         Go benchmark binary (default: build/bin/tpcc-go)
  --db-dir PATH            database directory (default: tpcc_random_kill_db)
  --port N                 server port (default: 8765)
  --workers N               concurrent workers (default: 16)
  --warmup N               workload warmup seconds (default: 2)
  --measure N               workload measurement seconds (default: 45)
  --cycles N                random kill/recovery cycles (default: 5)
  --min-kill-delay N        minimum seconds before a kill (default: 3)
  --max-kill-delay N        maximum seconds before a kill (default: 8)
  --restart-timeout N       recovery wait timeout seconds (default: 120)
  --data-dir PATH           TPC-C CSV data directory
  --schema-dir PATH         TPC-C schema directory
  --work-dir PATH           logs and temporary JSON files directory
  --server-log PATH         server log path
  --isolation LEVEL         read-committed or snapshot-isolation
  --seed N                  random seed (default: 1)
  -h, --help                show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --binary) BINARY="$2"; shift 2 ;;
        --go-binary) GO_BINARY="$2"; shift 2 ;;
        --db-dir) DB_DIR="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --workers) WORKERS="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --measure) MEASURE="$2"; shift 2 ;;
        --cycles) CYCLES="$2"; shift 2 ;;
        --min-kill-delay) MIN_KILL_DELAY="$2"; shift 2 ;;
        --max-kill-delay) MAX_KILL_DELAY="$2"; shift 2 ;;
        --restart-timeout) RESTART_TIMEOUT="$2"; shift 2 ;;
        --data-dir) DATA_DIR="$2"; shift 2 ;;
        --schema-dir) SCHEMA_DIR="$2"; shift 2 ;;
        --work-dir) WORK_DIR="$2"; shift 2 ;;
        --server-log) SERVER_LOG="$2"; shift 2 ;;
        --isolation) ISOLATION="$2"; shift 2 ;;
        --seed) SEED="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ ! "$WORKERS" =~ ^[1-9][0-9]*$ || ! "$CYCLES" =~ ^[1-9][0-9]*$ || ! "$MEASURE" =~ ^[1-9][0-9]*$ ]]; then
    echo "workers, measure, and cycles must be positive integers" >&2
    exit 2
fi
if [[ ! "$MIN_KILL_DELAY" =~ ^[0-9]+([.][0-9]+)?$ || ! "$MAX_KILL_DELAY" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "kill delays must be non-negative numbers" >&2
    exit 2
fi
if [[ "$ISOLATION" != "read-committed" && "$ISOLATION" != "snapshot-isolation" ]]; then
    echo "--isolation must be read-committed or snapshot-isolation" >&2
    exit 2
fi
if [[ ! -x "$BINARY" || ! -x "$GO_BINARY" ]]; then
    echo "missing rmdb or tpcc-go binary" >&2
    exit 1
fi

if [[ -z "$WORK_DIR" ]]; then
    WORK_DIR="$(mktemp -d /tmp/rmdb-tpcc-random-kill.XXXXXX)"
    REMOVE_WORK_DIR=1
else
    mkdir -p "$WORK_DIR"
    REMOVE_WORK_DIR=0
fi
if [[ -z "$SERVER_LOG" ]]; then
    SERVER_LOG="$WORK_DIR/rmdb-server.log"
fi
mkdir -p "$(dirname "$DB_DIR")" "$(dirname "$SERVER_LOG")"
rm -rf "$DB_DIR"
rm -f "$SERVER_LOG" "$WORK_DIR"/baseline.json "$WORK_DIR"/run-*.json
ACK_FILE="$WORK_DIR/crash-acked-transactions.log"
rm -f "$ACK_FILE"

SERVER_PID=""
WORKLOAD_PID=""
cleanup() {
    if [[ -n "$WORKLOAD_PID" ]] && kill -0 "$WORKLOAD_PID" 2>/dev/null; then
        kill -TERM "$WORKLOAD_PID" 2>/dev/null || true
        wait "$WORKLOAD_PID" 2>/dev/null || true
    fi
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        sleep 1
        kill -KILL "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    if [[ "$REMOVE_WORK_DIR" -eq 1 ]]; then
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup EXIT INT TERM

wait_port() {
    "$GO_BINARY" --command wait-port --port "$PORT" --wait-timeout "${1}s"
}

start_server() {
    "$BINARY" "$DB_DIR" >> "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
    if ! wait_port "$1"; then
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "[random-kill] server exited before becoming ready (pid=$SERVER_PID)" >&2
            tail -80 "$SERVER_LOG" >&2 || true
        fi
        return 1
    fi
}

stop_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

echo "[random-kill] starting server and loading TPC-C data"
start_server 30
"$GO_BINARY" --command load \
    --port "$PORT" \
    --isolation "$ISOLATION" \
    --data-dir "$DATA_DIR" \
    --schema-dir "$SCHEMA_DIR" \
    --rmdb-db-dir "$DB_DIR"
"$GO_BINARY" --command oracle-init --port "$PORT" --isolation "$ISOLATION"

# The consistency checker needs only the initial profile/counts. Generate it
# before crash cycles; the random-kill workload itself may not reach its normal
# JSON write because its clients lose their connections when the server dies.
"$GO_BINARY" \
    --port "$PORT" \
    --isolation "$ISOLATION" \
    --workers 1 \
    --warmup 0 \
    --measure 1 \
    --rounds 1 \
    --progress-interval 0 \
    --json-out "$WORK_DIR/baseline.json"
stop_server

for ((cycle = 1; cycle <= CYCLES; cycle++)); do
    echo "[random-kill] cycle $cycle/$CYCLES: restart workload server"
    start_server "$RESTART_TIMEOUT"
    RUN_JSON="$WORK_DIR/run-$cycle.json"
    rm -f "$RUN_JSON"
    "$GO_BINARY" \
        --port "$PORT" \
        --isolation "$ISOLATION" \
        --workers "$WORKERS" \
        --warmup "$WARMUP" \
        --measure "$MEASURE" \
        --rounds 1 \
        --progress-interval 0 \
        --warehouse-policy terminal-home \
        --oracle-ack-file "$ACK_FILE" \
        --oracle-id-prefix "$cycle" \
        --json-out "$RUN_JSON" \
        >"$WORK_DIR/run-$cycle.out" 2>"$WORK_DIR/run-$cycle.err" &
    WORKLOAD_PID=$!

    delay="$(python3 - "$SEED" "$cycle" "$MIN_KILL_DELAY" "$MAX_KILL_DELAY" <<'PY'
import random
import sys

seed, cycle, lower, upper = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
if upper < lower:
    raise SystemExit("max kill delay must be >= min kill delay")
rng = random.Random(seed + cycle * 7919)
print(f"{rng.uniform(lower, upper):.3f}")
PY
)"
    if awk "BEGIN { exit !($delay >= $MEASURE + $WARMUP) }"; then
        echo "random kill delay $delay exceeds workload duration" >&2
        exit 2
    fi
    echo "[random-kill] cycle $cycle/$CYCLES: kill -9 after ${delay}s (server pid=$SERVER_PID)"
    sleep "$delay"
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "[random-kill] server died before scheduled kill -9 (pid=$SERVER_PID)" >&2
        tail -80 "$SERVER_LOG" >&2 || true
        exit 1
    fi
    kill -KILL "$SERVER_PID"
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
    if kill -0 "$WORKLOAD_PID" 2>/dev/null; then
        # The Go clients normally observe the closed sockets and exit. Bound
        # the wait so one lost connection cannot stall the recovery test.
        for _ in {1..50}; do
            kill -0 "$WORKLOAD_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -TERM "$WORKLOAD_PID" 2>/dev/null || true
    fi
    wait "$WORKLOAD_PID" 2>/dev/null || true
    WORKLOAD_PID=""

    echo "[random-kill] cycle $cycle/$CYCLES: kill once more during recovery"
    "$BINARY" "$DB_DIR" >> "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
    sleep 0.02
    kill -KILL "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""

    echo "[random-kill] cycle $cycle/$CYCLES: recover and check consistency/oracle"
    start_server "$RESTART_TIMEOUT"
    "$GO_BINARY" --command consistency \
        --port "$PORT" \
        --isolation "$ISOLATION" \
        --consistency-stage "random-kill-cycle-$cycle" \
        --result-json "$WORK_DIR/baseline.json"
    "$GO_BINARY" --command oracle-verify \
        --port "$PORT" \
        --isolation "$ISOLATION" \
        --oracle-ack-file "$ACK_FILE"
    stop_server
done

echo "[random-kill] passed $CYCLES random crash/recovery cycles"
echo "[random-kill] work directory: $WORK_DIR"
