#!/usr/bin/env bash
# Sample one RMDB process and the block device hosting its database directory.
# This intentionally uses procfs/sysfs only so a benchmark host does not need
# pidstat or iostat installed.

set -Eeuo pipefail

PID=""
PHASE=""
OUTPUT=""
DB_DIR=""
MAX_ROWS=20000
export TZ=UTC

usage() {
    cat <<EOF
Usage: $0 --pid PID --phase run|recovery --output PATH --db-dir PATH

Writes one CSV row every 250 ms. The file is created by the caller; this script
appends samples and writes the header only when the file is empty. The final
eight columns retain the root-process RSS/fault view and add a process-tree
snapshot from /proc/PID/status, so recovery workers or helpers cannot be
mistaken for unmeasured memory.
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
[[ -n "${EPOCHREALTIME-}" ]] || { echo "[observe] Bash 5+ with EPOCHREALTIME is required" >&2; exit 2; }

mkdir -p "$(dirname "$OUTPUT")"
if [[ ! -s "$OUTPUT" ]]; then
    printf '%s\n' 'wall_time_utc,phase,sample_kind,pid,process_cpu_percent,process_read_bytes_delta,process_write_bytes_delta,voluntary_ctxt_switches_delta,nonvoluntary_ctxt_switches_delta,block_device,block_read_sectors_delta,block_write_sectors_delta,block_io_time_ms_delta,db_log_bytes,checkpoint_offset,retained_wal_bytes,rss_bytes,minor_faults_delta,major_faults_delta,process_tree_vm_rss_sum_bytes_conservative_not_additive,process_tree_rss_anon_bytes,process_tree_rss_file_bytes,process_tree_rss_shmem_bytes,process_tree_threads' >> "$OUTPUT"
fi

read_starttime() {
    local stat_line rest
    [[ -r "/proc/$1/stat" ]] || return 1
    stat_line="$(<"/proc/$1/stat")" || return 1
    rest="${stat_line##*) }"
    local -a fields
    read -r -a fields <<< "$rest"
    [[ "${fields[19]:-}" =~ ^[0-9]+$ ]] || return 1
    printf '%s\n' "${fields[19]}"
}

declare -A PROCESS_TREE_SEEN=()
declare -A PROCESS_TREE_STARTTIME=()
declare -A PROCESS_TREE_PARENT=()
declare -a PROCESS_TREE_PIDS=()
read_process_identity() {
    local pid="$1" expected_parent="$2" key value ignored starttime ppid=0
    starttime="$(read_starttime "$pid")" || return 1
    while IFS=$' \t' read -r key value ignored; do
        [[ "$key" == "PPid:" ]] && { ppid="${value:-0}"; break; }
    done < "/proc/$pid/status"
    [[ -z "$expected_parent" || "$ppid" == "$expected_parent" ]] || return 1
    printf '%s %s\n' "$starttime" "$ppid"
}
process_tree_pids() {
    local pid="$1" expected_parent="$2" child children='' task_children identity starttime ppid
    [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return
    [[ -z "${PROCESS_TREE_SEEN[$pid]-}" ]] || return
    identity="$(read_process_identity "$pid" "$expected_parent")" || return
    read -r starttime ppid <<< "$identity"
    PROCESS_TREE_SEEN[$pid]=1
    PROCESS_TREE_STARTTIME[$pid]="$starttime"
    PROCESS_TREE_PARENT[$pid]="$ppid"
    PROCESS_TREE_PIDS+=("$pid")
    for task_children in /proc/$pid/task/*/children; do
        [[ -r "$task_children" ]] || continue
        children=''
        IFS= read -r children < "$task_children" || true
        for child in $children; do
            process_tree_pids "$child" "$pid"
        done
    done
}

read_process_tree_memory() {
    local root="$1" pid status key value ignored starttime ppid vm_rss rss_anon rss_file rss_shmem threads
    local total_vm_rss=0 total_rss_anon=0 total_rss_file=0 total_rss_shmem=0 total_threads=0
    PROCESS_TREE_SEEN=()
    PROCESS_TREE_STARTTIME=()
    PROCESS_TREE_PARENT=()
    PROCESS_TREE_PIDS=()
    process_tree_pids "$root" ""
    for pid in "${PROCESS_TREE_PIDS[@]}"; do
        status="/proc/$pid/status"
        [[ -r "$status" ]] || continue
        vm_rss=0 rss_anon=0 rss_file=0 rss_shmem=0 threads=0
        while IFS=$' \t' read -r key value ignored; do
            case "$key" in
                VmRSS:) vm_rss="${value:-0}" ;;
                RssAnon:) rss_anon="${value:-0}" ;;
                RssFile:) rss_file="${value:-0}" ;;
                RssShmem:) rss_shmem="${value:-0}" ;;
                Threads:) threads="${value:-0}" ;;
            esac
        done < "$status"
        read -r starttime ppid <<< "$(read_process_identity "$pid" "${PROCESS_TREE_PARENT[$pid]-}")" || continue
        [[ "$starttime" == "${PROCESS_TREE_STARTTIME[$pid]-}" ]] || continue
        total_vm_rss=$((total_vm_rss + ${vm_rss:-0} * 1024))
        total_rss_anon=$((total_rss_anon + ${rss_anon:-0} * 1024))
        total_rss_file=$((total_rss_file + ${rss_file:-0} * 1024))
        total_rss_shmem=$((total_rss_shmem + ${rss_shmem:-0} * 1024))
        total_threads=$((total_threads + ${threads:-0}))
    done
    printf '%s %s %s %s %s\n' "$total_vm_rss" "$total_rss_anon" "$total_rss_file" "$total_rss_shmem" "$total_threads"
}

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
    local stat_line rest key value ignored utime stime minflt majflt rss_pages read_bytes write_bytes voluntary nonvoluntary
    stat_line="$(<"/proc/$PID/stat")" || return 1
    # The executable name may contain spaces or parentheses, so discard it via
    # the final ')' before selecting fields 14 and 15 (utime/stime).
    rest="${stat_line##*) }"
    local -a fields
    read -r -a fields <<< "$rest"
    utime="${fields[11]:-0}"
    stime="${fields[12]:-0}"
    # Fields 10 and 12 in /proc/PID/stat are minflt and majflt. They become
    # fields 8 and 10 after removing the possibly space-containing comm field.
    minflt="${fields[7]:-0}"
    majflt="${fields[9]:-0}"
    read -r _ rss_pages _ < "/proc/$PID/statm" || return 1
    read_bytes=0 write_bytes=0
    while IFS=$' \t' read -r key value ignored; do
        case "$key" in
            read_bytes:) read_bytes="${value:-0}" ;;
            write_bytes:) write_bytes="${value:-0}" ;;
        esac
    done < "/proc/$PID/io"
    voluntary=0 nonvoluntary=0
    while IFS=$' \t' read -r key value ignored; do
        case "$key" in
            voluntary_ctxt_switches:) voluntary="${value:-0}" ;;
            nonvoluntary_ctxt_switches:) nonvoluntary="${value:-0}" ;;
        esac
    done < "/proc/$PID/status"
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

epoch_realtime_us() {
    local value="$1" seconds fraction
    seconds="${value%%.*}"
    fraction="${value#*.}000000"
    printf '%s\n' "$((10#$seconds * 1000000 + 10#${fraction:0:6}))"
}

wall_time_utc() {
    local value="$1" fraction formatted
    fraction="${value#*.}000000"
    printf -v formatted '%(%Y-%m-%dT%H:%M:%S)T' -1
    printf '%s.%sZ\n' "$formatted" "${fraction:0:6}"
}

clk_tck="$(getconf CLK_TCK)"
page_size="$(getconf PAGESIZE)"
root_starttime="$(read_starttime "$PID")" || { echo "[observe] root pid disappeared before sampling" >&2; exit 1; }
block_device="$(resolve_block_device)"
prev_slow_process=""
prev_slow_disk=""
prev_slow_block_device=""
prev_slow_us=""
sample_rows=0
slow_sample=0
cached_disk="0 0 0"
cached_log_bytes=0
cached_restart_offset=""
cached_retained_wal_bytes=""
cached_cpu_percent=0
cached_process_read_delta=0
cached_process_write_delta=0
cached_voluntary_delta=0
cached_nonvoluntary_delta=0
cached_minor_faults_delta=0
cached_major_faults_delta=0
cached_disk_read_delta=0
cached_disk_write_delta=0
cached_disk_io_delta=0

while kill -0 "$PID" 2>/dev/null; do
    if [[ "$(read_starttime "$PID")" != "$root_starttime" ]]; then
        echo "[observe] root pid $PID changed starttime or disappeared; stopping" >&2
        break
    fi
    if (( sample_rows >= MAX_ROWS )); then
        echo "[observe] reached bounded CSV limit of $MAX_ROWS rows; stopping" >&2
        break
    fi
    process="$(read_process_counters)" || break
    process_tree_memory="$(read_process_tree_memory "$PID")"
    if (( sample_rows % 4 == 0 )); then
        # The only external probes are amortized to at most once per second.
        # Fast 250 ms samples use procfs reads and shell builtins only.
        block_device="$(resolve_block_device)"
        cached_disk="$(read_disk_counters "$block_device")"
        slow_now_us="$(epoch_realtime_us "$EPOCHREALTIME")"
        cached_log_bytes="$(db_log_bytes)"
        cached_restart_offset="$(checkpoint_offset)"
        cached_retained_wal_bytes=""
        if [[ -n "$cached_restart_offset" && "$cached_restart_offset" -le "$cached_log_bytes" ]]; then
            cached_retained_wal_bytes=$((cached_log_bytes - cached_restart_offset))
        fi
        slow_sample=1
    else
        slow_sample=0
    fi
    sample_now="$EPOCHREALTIME"
    sample_wall_time_utc="$(wall_time_utc "$sample_now")"
    read -r process_ticks process_read process_write voluntary nonvoluntary rss_bytes minor_faults major_faults <<< "$process"
    read -r process_tree_vm_rss process_tree_rss_anon process_tree_rss_file process_tree_rss_shmem process_tree_threads <<< "$process_tree_memory"
    read -r disk_read disk_write disk_io_ms <<< "$cached_disk"

    if (( slow_sample )); then
        cached_cpu_percent=0
        cached_process_read_delta=0 cached_process_write_delta=0
        cached_voluntary_delta=0 cached_nonvoluntary_delta=0
        cached_minor_faults_delta=0 cached_major_faults_delta=0
        cached_disk_read_delta=0 cached_disk_write_delta=0 cached_disk_io_delta=0
        if [[ -n "$prev_slow_process" && -n "$prev_slow_disk" && -n "$prev_slow_us" ]]; then
        read -r prev_ticks prev_read prev_write prev_voluntary prev_nonvoluntary prev_rss prev_minor_faults prev_major_faults <<< "$prev_slow_process"
        read -r prev_disk_read prev_disk_write prev_disk_io <<< "$prev_slow_disk"
        elapsed_us=$((slow_now_us - prev_slow_us))
        if (( elapsed_us > 0 )); then
            cached_cpu_percent="$(awk -v ticks="$((process_ticks - prev_ticks))" -v hz="$clk_tck" -v elapsed="$elapsed_us" 'BEGIN { printf "%.2f", (ticks * 100000000.0) / (hz * elapsed) }')"
        fi
        cached_process_read_delta=$((process_read - prev_read))
        cached_process_write_delta=$((process_write - prev_write))
        cached_voluntary_delta=$((voluntary - prev_voluntary))
        cached_nonvoluntary_delta=$((nonvoluntary - prev_nonvoluntary))
        cached_minor_faults_delta="$(nonnegative_delta "$minor_faults" "$prev_minor_faults")"
        cached_major_faults_delta="$(nonnegative_delta "$major_faults" "$prev_major_faults")"
        if [[ "$block_device" == "$prev_slow_block_device" ]]; then
            cached_disk_read_delta=$((disk_read - prev_disk_read))
            cached_disk_write_delta=$((disk_write - prev_disk_write))
            cached_disk_io_delta=$((disk_io_ms - prev_disk_io))
        fi
        fi
        prev_slow_process="$process"
        prev_slow_disk="$cached_disk"
        prev_slow_block_device="$block_device"
        prev_slow_us="$slow_now_us"
    fi
    if (( slow_sample )); then
        sample_kind=slow
        output_cpu_percent="$cached_cpu_percent"
        output_process_read_delta="$cached_process_read_delta" output_process_write_delta="$cached_process_write_delta"
        output_voluntary_delta="$cached_voluntary_delta" output_nonvoluntary_delta="$cached_nonvoluntary_delta"
        output_disk_read_delta="$cached_disk_read_delta" output_disk_write_delta="$cached_disk_write_delta" output_disk_io_delta="$cached_disk_io_delta"
        output_minor_faults_delta="$cached_minor_faults_delta" output_major_faults_delta="$cached_major_faults_delta"
    else
        sample_kind=fast
        output_cpu_percent=0 output_process_read_delta=0 output_process_write_delta=0 output_voluntary_delta=0
        output_nonvoluntary_delta=0 output_disk_read_delta=0 output_disk_write_delta=0 output_disk_io_delta=0
        output_minor_faults_delta=0 output_major_faults_delta=0
    fi
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$sample_wall_time_utc" "$PHASE" "$sample_kind" "$PID" "$output_cpu_percent" \
        "$output_process_read_delta" "$output_process_write_delta" "$output_voluntary_delta" "$output_nonvoluntary_delta" \
        "$block_device" "$output_disk_read_delta" "$output_disk_write_delta" "$output_disk_io_delta" "$cached_log_bytes" "$cached_restart_offset" \
        "$cached_retained_wal_bytes" "$rss_bytes" "$output_minor_faults_delta" "$output_major_faults_delta" \
        "$process_tree_vm_rss" "$process_tree_rss_anon" "$process_tree_rss_file" "$process_tree_rss_shmem" \
        "$process_tree_threads" >> "$OUTPUT"
    ((sample_rows += 1))
    sleep 0.25
done
