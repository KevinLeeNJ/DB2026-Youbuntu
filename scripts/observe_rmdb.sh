#!/usr/bin/env bash
# Sample one RMDB process and the block device hosting its database directory.
# This intentionally uses procfs/sysfs only so a benchmark host does not need
# pidstat or iostat installed.

set -Eeuo pipefail

PID=""
PHASE=""
OUTPUT=""
DB_DIR=""

usage() {
    cat <<EOF
Usage: $0 --pid PID --phase run|recovery --output PATH --db-dir PATH

Writes one CSV row per second. The file is created by the caller; this script
appends samples and writes the header only when the file is empty. The final
three columns report the sampled process RSS and non-negative minor/major
page-fault deltas, which help identify recovery mmap RSS growth.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pid) PID="$2"; shift 2 ;;
        --phase) PHASE="$2"; shift 2 ;;
        --output) OUTPUT="$2"; shift 2 ;;
        --db-dir) DB_DIR="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ "$PID" =~ ^[1-9][0-9]*$ ]] || { echo "--pid must be a PID" >&2; exit 2; }
[[ "$PHASE" == "run" || "$PHASE" == "recovery" ]] || { echo "--phase must be run or recovery" >&2; exit 2; }
[[ -n "$OUTPUT" && -n "$DB_DIR" ]] || { echo "--output and --db-dir are required" >&2; exit 2; }

mkdir -p "$(dirname "$OUTPUT")"
if [[ ! -s "$OUTPUT" ]]; then
    printf '%s\n' 'wall_time_utc,phase,pid,process_cpu_percent,process_read_bytes_delta,process_write_bytes_delta,voluntary_ctxt_switches_delta,nonvoluntary_ctxt_switches_delta,block_device,block_read_sectors_delta,block_write_sectors_delta,block_io_time_ms_delta,db_log_bytes,checkpoint_offset,retained_wal_bytes,rss_bytes,minor_faults_delta,major_faults_delta' >> "$OUTPUT"
fi

resolve_block_device() {
    local source resolved name target
    source="$(df -P "$DB_DIR" 2>/dev/null | awk 'NR == 2 { print $1 }')"
    [[ "$source" == /dev/* ]] || { printf '%s\n' unavailable; return; }
    resolved="$(readlink -f "$source" 2>/dev/null || printf '%s' "$source")"
    name="${resolved##*/}"
    [[ -e "/sys/class/block/$name/dev" ]] || { printf '%s\n' unavailable; return; }
    target="$(readlink -f "/sys/class/block/$name" 2>/dev/null || true)"
    [[ -n "$target" ]] || { printf '%s\n' unavailable; return; }
    printf '%s\n' "${target##*/}"
}

read_process_counters() {
    local stat_line rest utime stime minflt majflt rss_pages read_bytes write_bytes voluntary nonvoluntary
    stat_line="$(<"/proc/$PID/stat")" || return 1
    # The executable name may contain spaces or parentheses, so discard it via
    # the final ')' before selecting fields 14 and 15 (utime/stime).
    rest="${stat_line##*) }"
    utime="$(awk '{print $12}' <<<"$rest")"
    stime="$(awk '{print $13}' <<<"$rest")"
    # Fields 10 and 12 in /proc/PID/stat are minflt and majflt. They become
    # fields 8 and 10 after removing the possibly space-containing comm field.
    minflt="$(awk '{print $8}' <<<"$rest")"
    majflt="$(awk '{print $10}' <<<"$rest")"
    rss_pages="$(awk '{print $2}' "/proc/$PID/statm")"
    read_bytes="$(awk '/^read_bytes:/ {print $2}' "/proc/$PID/io")"
    write_bytes="$(awk '/^write_bytes:/ {print $2}' "/proc/$PID/io")"
    voluntary="$(awk '/^voluntary_ctxt_switches:/ {print $2}' "/proc/$PID/status")"
    nonvoluntary="$(awk '/^nonvoluntary_ctxt_switches:/ {print $2}' "/proc/$PID/status")"
    printf '%s %s %s %s %s %s %s %s\n' "$((utime + stime))" "${read_bytes:-0}" "${write_bytes:-0}" \
        "${voluntary:-0}" "${nonvoluntary:-0}" "$(( ${rss_pages:-0} * page_size ))" "${minflt:-0}" "${majflt:-0}"
}

nonnegative_delta() {
    local current="$1" previous="$2" delta
    delta=$((current - previous))
    (( delta >= 0 )) && printf '%s\n' "$delta" || printf '0\n'
}

read_disk_counters() {
    local device="$1"
    [[ "$device" != unavailable ]] || { printf '%s\n' '0 0 0'; return; }
    awk -v device="$device" '$3 == device { print $6, $10, $13; found = 1; exit } END { if (!found) print "0 0 0" }' /proc/diskstats
}

db_log_bytes() {
    local path total=0 size
    shopt -s nullglob
    for path in "$DB_DIR"/db.log "$DB_DIR"/db.log.*; do
        [[ -f "$path" ]] || continue
        size="$(stat -c '%s' "$path" 2>/dev/null || printf '0')"
        total=$((total + size))
    done
    shopt -u nullglob
    printf '%s\n' "$total"
}

checkpoint_offset() {
    local offset
    [[ -r "$DB_DIR/db.restart" ]] || return 0
    IFS= read -r offset < "$DB_DIR/db.restart" || true
    [[ "$offset" =~ ^[0-9]+$ ]] || return 0
    printf '%s\n' "$offset"
}

clk_tck="$(getconf CLK_TCK)"
page_size="$(getconf PAGESIZE)"
block_device="$(resolve_block_device)"
prev_process=""
prev_disk=""
prev_block_device=""
prev_ns=""

while kill -0 "$PID" 2>/dev/null; do
    # The benchmark starts us immediately after fork, before a fresh RMDB may
    # have created DB_DIR. Resolve every sample so a later mount/device change
    # also establishes a fresh disk-counter baseline.
    block_device="$(resolve_block_device)"
    process="$(read_process_counters)" || break
    disk="$(read_disk_counters "$block_device")"
    now_ns="$(date +%s%N)"
    log_bytes="$(db_log_bytes)"
    restart_offset="$(checkpoint_offset)"
    retained_wal_bytes=""
    if [[ -n "$restart_offset" && "$restart_offset" -le "$log_bytes" ]]; then
        retained_wal_bytes=$((log_bytes - restart_offset))
    fi
    read -r process_ticks process_read process_write voluntary nonvoluntary rss_bytes minor_faults major_faults <<< "$process"
    read -r disk_read disk_write disk_io_ms <<< "$disk"

    cpu_percent=0
    process_read_delta=0
    process_write_delta=0
    voluntary_delta=0
    nonvoluntary_delta=0
    minor_faults_delta=0
    major_faults_delta=0
    disk_read_delta=0
    disk_write_delta=0
    disk_io_delta=0
    if [[ -n "$prev_process" && -n "$prev_disk" && -n "$prev_ns" ]]; then
        read -r prev_ticks prev_read prev_write prev_voluntary prev_nonvoluntary prev_rss prev_minor_faults prev_major_faults <<< "$prev_process"
        read -r prev_disk_read prev_disk_write prev_disk_io <<< "$prev_disk"
        elapsed_ns=$((now_ns - prev_ns))
        if (( elapsed_ns > 0 )); then
            cpu_percent="$(awk -v ticks="$((process_ticks - prev_ticks))" -v hz="$clk_tck" -v elapsed="$elapsed_ns" 'BEGIN { printf "%.2f", (ticks * 100000000000.0) / (hz * elapsed) }')"
        fi
        process_read_delta=$((process_read - prev_read))
        process_write_delta=$((process_write - prev_write))
        voluntary_delta=$((voluntary - prev_voluntary))
        nonvoluntary_delta=$((nonvoluntary - prev_nonvoluntary))
        minor_faults_delta="$(nonnegative_delta "$minor_faults" "$prev_minor_faults")"
        major_faults_delta="$(nonnegative_delta "$major_faults" "$prev_major_faults")"
        if [[ "$block_device" == "$prev_block_device" ]]; then
            disk_read_delta=$((disk_read - prev_disk_read))
            disk_write_delta=$((disk_write - prev_disk_write))
            disk_io_delta=$((disk_io_ms - prev_disk_io))
        fi
    fi
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$PHASE" "$PID" "$cpu_percent" \
        "$process_read_delta" "$process_write_delta" "$voluntary_delta" "$nonvoluntary_delta" \
        "$block_device" "$disk_read_delta" "$disk_write_delta" "$disk_io_delta" "$log_bytes" "$restart_offset" \
        "$retained_wal_bytes" "$rss_bytes" "$minor_faults_delta" "$major_faults_delta" >> "$OUTPUT"
    prev_process="$process"
    prev_disk="$disk"
    prev_block_device="$block_device"
    prev_ns="$now_ns"
    sleep 1
done
