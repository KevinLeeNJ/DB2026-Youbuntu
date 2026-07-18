#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_FILE="$ROOT_DIR/JIT_PERFORMANCE_RESULTS.md"
RUN_ROOT="${JIT_RUN_ROOT:-/tmp/rmdb-jit-runs}"
MIN_FREE_BYTES="${JIT_MIN_FREE_BYTES:-2147483648}"

PHASE=""
LABEL=""
RESULT_JSON=""
METRICS_JSON=""
PROFILE_SUMMARY=""
SELF_TEST=0
ARTIFACTS=()

usage() {
    cat <<EOF
Usage: $0 --phase N --label NAME [options] -- COMMAND [ARG...]
       $0 --self-test

Options:
  --result-json PATH   Extract TPC-C summary fields from PATH before cleanup.
  --metrics-json PATH  Extract RMDB phase metrics from PATH before cleanup.
  --profile-summary PATH  Record a bounded perf/flamegraph summary before cleanup.
  --artifact PATH      Register a generated path for allowlisted cleanup (repeatable).
  --self-test          Exercise success, failure, and interrupted reporting.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --phase) PHASE="$2"; shift 2 ;;
        --label) LABEL="$2"; shift 2 ;;
        --result-json) RESULT_JSON="$2"; shift 2 ;;
        --metrics-json) METRICS_JSON="$2"; shift 2 ;;
        --profile-summary) PROFILE_SUMMARY="$2"; shift 2 ;;
        --artifact) ARTIFACTS+=("$2"); shift 2 ;;
        --self-test) SELF_TEST=1; shift ;;
        --) shift; break ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ "$SELF_TEST" -eq 1 ]]; then
    SELF_TEST_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
    "$0" --phase 0 --label "self-test-success-$SELF_TEST_ID" -- bash -c 'exit 0'
    set +e
    "$0" --phase 0 --label "self-test-failure-$SELF_TEST_ID" -- bash -c 'exit 7'
    FAILURE_RC=$?
    "$0" --phase 0 --label "self-test-interrupted-$SELF_TEST_ID" -- sleep 30 &
    INTERRUPTED_PID=$!
    sleep 1
    kill -TERM "$INTERRUPTED_PID"
    wait "$INTERRUPTED_PID"
    INTERRUPTED_RC=$?
    set -e
    [[ "$FAILURE_RC" -eq 7 ]] || { echo "failure self-test returned $FAILURE_RC" >&2; exit 1; }
    [[ "$INTERRUPTED_RC" -eq 143 ]] || { echo "interrupt self-test returned $INTERRUPTED_RC" >&2; exit 1; }
    rg -q "self-test-success-$SELF_TEST_ID.*pass" "$RESULTS_FILE"
    rg -q "self-test-failure-$SELF_TEST_ID.*fail" "$RESULTS_FILE"
    rg -q "self-test-interrupted-$SELF_TEST_ID.*interrupted" "$RESULTS_FILE"
    echo "JIT validation wrapper self-test passed"
    exit 0
fi

if [[ -z "$PHASE" || -z "$LABEL" || $# -eq 0 ]]; then
    usage >&2
    exit 2
fi

mkdir -p "$RUN_ROOT"
RUN_ID="phase${PHASE}-$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
RUN_DIR="$RUN_ROOT/$RUN_ID"
mkdir -p "$RUN_DIR"
COMMAND_LOG="$RUN_DIR/command.log"
MANIFEST="$RUN_DIR/cleanup.manifest"
FRAGMENT="$RUN_DIR/result.md"
touch "$MANIFEST"

for artifact in "${ARTIFACTS[@]}"; do
    realpath -m "$artifact" >> "$MANIFEST"
done

AVAILABLE_BEFORE="$(df -B1 --output=avail "$ROOT_DIR" | tail -1 | tr -d ' ')"
ROOT_BYTES_BEFORE="$(du -sb "$ROOT_DIR" | awk '{print $1}')"
if (( AVAILABLE_BEFORE < MIN_FREE_BYTES )); then
    echo "insufficient disk space: $AVAILABLE_BEFORE bytes available, need $MIN_FREE_BYTES" >&2
    exit 1
fi

STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
START_EPOCH="$(date +%s)"
COMMIT="$(git -C "$ROOT_DIR" rev-parse --short HEAD)"
DIRTY="$(git -C "$ROOT_DIR" status --porcelain --untracked-files=no | wc -l | tr -d ' ')"
CPU="$(lscpu | sed -n 's/^Model name:[[:space:]]*//p' | head -1)"
BUILD_TYPE="$(sed -n 's/^CMAKE_BUILD_TYPE:STRING=//p' "$ROOT_DIR/build/CMakeCache.txt" 2>/dev/null || true)"
COMMAND_TEXT="$(printf '%q ' "$@")"

STATUS="running"
EXIT_CODE=0
CHILD_PID=""
REPORTED=0
FINALIZED=0

append_fragment() {
    local source="$1"
    local lock_file="$RUN_ROOT/results.lock"
    (
        flock -x 9
        cat "$source" >> "$RESULTS_FILE"
    ) 9>"$lock_file"
}

report_before_cleanup() {
    if [[ "$REPORTED" -eq 1 ]]; then
        return
    fi
    local ended_at duration tpcc metrics profile
    ended_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    duration=$(( $(date +%s) - START_EPOCH ))
    tpcc="N/A (no readable TPC-C result JSON)"
    if [[ -n "$RESULT_JSON" && -s "$RESULT_JSON" ]] && jq -e . "$RESULT_JSON" >/dev/null 2>&1; then
        tpcc="$(jq -c '[.rounds[].tpmc] as $values | ($values | add / length) as $mean | {median_tpmc, tpmc_cv_pct: (if $mean == 0 then 0 else (([$values[] | . - $mean | . * .] | add / length | sqrt) / $mean * 100) end), rounds: [.rounds[] | {tpmc, abort_rate, latency_ms}], config}' "$RESULT_JSON")"
    fi
    metrics="N/A (no readable phase metrics JSON)"
    if [[ -n "$METRICS_JSON" && -s "$METRICS_JSON" ]] && jq -e . "$METRICS_JSON" >/dev/null 2>&1; then
        metrics="$(jq -c . "$METRICS_JSON")"
    fi
    profile="N/A (no readable perf/flamegraph summary)"
    if [[ -n "$PROFILE_SUMMARY" && -s "$PROFILE_SUMMARY" ]]; then
        profile="$(sed -n '1,120p' "$PROFILE_SUMMARY" | sed 's/[[:space:]]*$//')"
    fi
    local error_summary="N/A (command passed)"
    if [[ "$STATUS" != "pass" ]]; then
        error_summary="$(tail -5 "$COMMAND_LOG" | tr '\n' '|' | sed 's/|$//')"
    fi
    {
        echo
        echo "### $RUN_ID | $LABEL | $STATUS"
        echo
        echo "- Context: phase=$PHASE; started=$STARTED_AT; ended=$ended_at; duration_s=$duration; commit=$COMMIT; dirty_tracked_files=$DIRTY; build=$BUILD_TYPE; cpu=$CPU"
        echo "- Command: \`$COMMAND_TEXT\`"
        echo "- Modes: RMDB_JIT=${RMDB_JIT:-unset}; RMDB_STATEMENT_CACHE=${RMDB_STATEMENT_CACHE:-unset}; workers=${TPCC_WORKERS:-N/A}; warehouses=${TPCC_WAREHOUSES:-N/A}; warmup_s=${TPCC_WARMUP:-N/A}; measure_s=${TPCC_MEASURE:-N/A}; rounds=${TPCC_ROUNDS:-N/A}; seed=${TPCC_RUN_SEED:-N/A}"
        echo "- Result: status=$STATUS; exit_code=$EXIT_CODE; log=$COMMAND_LOG"
        echo "- Error summary: $error_summary"
        echo "- TPC-C: $tpcc"
        echo "- Phase metrics: $metrics"
        echo "- CPU/profile:"
        echo '```text'
        echo "$profile"
        echo '```'
        echo "- Cache/JIT metrics: N/A before their implementation phase."
        echo "- Cleanup preflight: available_bytes=$AVAILABLE_BEFORE; repository_bytes=$ROOT_BYTES_BEFORE; status=pending"
    } > "$FRAGMENT"
    append_fragment "$FRAGMENT"
    REPORTED=1
}

path_is_allowlisted() {
    local path="$1"
    case "$path" in
        "$RUN_DIR"/*|"$ROOT_DIR"/jit_*|"$ROOT_DIR"/benchmark/tpcc/jit_*) return 0 ;;
        *) return 1 ;;
    esac
}

cleanup_artifacts() {
    local cleanup_status="pass" path
    while IFS= read -r path; do
        [[ -n "$path" ]] || continue
        if ! path_is_allowlisted "$path"; then
            echo "refusing to clean non-allowlisted path: $path" >> "$COMMAND_LOG"
            cleanup_status="fail"
            continue
        fi
        rm -rf -- "$path" || cleanup_status="fail"
    done < "$MANIFEST"

    local available_after root_bytes_after cleanup_fragment
    available_after="$(df -B1 --output=avail "$ROOT_DIR" | tail -1 | tr -d ' ')"
    root_bytes_after="$(du -sb "$ROOT_DIR" | awk '{print $1}')"
    cleanup_fragment="$RUN_DIR/cleanup-result.md"
    {
        echo "- Cleanup result for $RUN_ID: status=$cleanup_status; available_bytes=$available_after; repository_bytes=$root_bytes_after; registered_artifacts=$(wc -l < "$MANIFEST" | tr -d ' ')"
    } > "$cleanup_fragment"
    append_fragment "$cleanup_fragment"
    [[ "$cleanup_status" == "pass" ]]
}

finalize() {
    if [[ "$FINALIZED" -eq 1 ]]; then
        return
    fi
    FINALIZED=1
    report_before_cleanup
    if ! cleanup_artifacts; then
        EXIT_CODE=1
    fi
    rm -rf -- "$RUN_DIR"
}

handle_signal() {
    local signal="$1" code="$2"
    STATUS="interrupted($signal)"
    EXIT_CODE="$code"
    if [[ -n "$CHILD_PID" ]] && kill -0 "$CHILD_PID" 2>/dev/null; then
        kill -TERM -- "-$CHILD_PID" 2>/dev/null || true
    fi
}

trap 'handle_signal INT 130' INT
trap 'handle_signal TERM 143' TERM
trap finalize EXIT

set +e
setsid -- "$@" > "$COMMAND_LOG" 2>&1 &
CHILD_PID=$!
wait "$CHILD_PID"
COMMAND_RC=$?
set -e

if [[ "$STATUS" == "running" ]]; then
    EXIT_CODE="$COMMAND_RC"
    if [[ "$COMMAND_RC" -eq 0 ]]; then
        STATUS="pass"
    else
        STATUS="fail"
    fi
fi

finalize
exit "$EXIT_CODE"
