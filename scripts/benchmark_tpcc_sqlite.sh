#!/usr/bin/env bash
# Run the TPC-C benchmark against the SQLite reference backend.
#
# This mirrors the workload parameters of benchmark_tpcc.sh and validates the
# database after load and after the transaction run. SQLite does not provide
# the RMDB server/WAL crash-recovery phase, so post-recovery is not attempted.

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$ROOT_DIR/benchmark/tpcc/data"
SQLITE_PATH="$ROOT_DIR/benchmark/tpcc/tpcc.sqlite"
JSON_OUT="$ROOT_DIR/benchmark/tpcc/sqlite-result.json"
WAREHOUSES=8
WORKERS=16
WARMUP=10
MEASURE=60
ROUNDS=1
PROGRESS_INTERVAL=5
REGENERATE_DATA=0
SKIP_CONSISTENCY=0

usage() {
    cat <<EOF
Usage: $0 [options]
  --sqlite-path PATH       SQLite database (default: benchmark/tpcc/tpcc.sqlite)
  --data-dir PATH          CSV data directory (default: benchmark/tpcc/data)
  --json-out PATH          result JSON (default: benchmark/tpcc/sqlite-result.json)
  --warehouses N           (default: 8)
  --workers N              (default: 16)
  --warmup N               warmup seconds (default: 10)
  --measure N              measurement seconds (default: 60)
  --rounds N               (default: 1)
  --progress-interval N    (default: 5)
  --regenerate-data        regenerate CSV data before loading
  --skip-consistency       skip post-load and post-transaction checks
  -h, --help               show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sqlite-path) SQLITE_PATH="$2"; shift 2 ;;
        --data-dir) DATA_DIR="$2"; shift 2 ;;
        --json-out) JSON_OUT="$2"; shift 2 ;;
        --warehouses) WAREHOUSES="$2"; shift 2 ;;
        --workers) WORKERS="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --measure) MEASURE="$2"; shift 2 ;;
        --rounds) ROUNDS="$2"; shift 2 ;;
        --progress-interval) PROGRESS_INTERVAL="$2"; shift 2 ;;
        --regenerate-data) REGENERATE_DATA=1; shift ;;
        --skip-consistency) SKIP_CONSISTENCY=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ "$REGENERATE_DATA" -eq 1 ]]; then
    DATA_ARGS=(--overwrite-data-dir)
else
    DATA_ARGS=(--reuse-data-dir)
fi

if [[ "$SKIP_CONSISTENCY" -eq 1 ]]; then
    CONSISTENCY_ARGS=(--skip-consistency)
else
    CONSISTENCY_ARGS=()
fi

echo "[benchmark-sqlite] data policy: ${DATA_ARGS[*]}"

echo "[benchmark-sqlite] removing previous SQLite runtime files"
rm -f "$SQLITE_PATH" "$SQLITE_PATH-shm" "$SQLITE_PATH-wal"
rm -f "$JSON_OUT"

echo "[benchmark-sqlite] running: warehouses=$WAREHOUSES workers=$WORKERS " \
     "warmup=${WARMUP}s measure=${MEASURE}s rounds=$ROUNDS"
python3 -m benchmark.tpcc.tpcc_run all \
    --backend sqlite \
    --warehouses "$WAREHOUSES" \
    --workers "$WORKERS" \
    --warmup "$WARMUP" \
    --measure "$MEASURE" \
    --rounds "$ROUNDS" \
    --progress-interval "$PROGRESS_INTERVAL" \
    --data-dir "$DATA_DIR" \
    --sqlite-path "$SQLITE_PATH" \
    --json-out "$JSON_OUT" \
    "${DATA_ARGS[@]}" \
    "${CONSISTENCY_ARGS[@]}"

echo "[benchmark-sqlite] complete: result=$JSON_OUT"
