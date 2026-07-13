#!/usr/bin/env bash
#
# perf_flamegraph.sh — 捕获 rmdb 运行数据并生成火焰图
#
# 流程：
#   1. 检查 FlameGraph 工具集，缺失则 clone brendangregg/FlameGraph
#   2. 自动定位 rmdb 进程 PID
#   3. perf record 采集 CPU 采样
#   4. perf script + stackcollapse-perf.pl + flamegraph.pl 生成 SVG 火焰图
#
# 用法：
#   ./scripts/perf_flamegraph.sh [选项]
#
# 选项：
#   -d, --duration SEC    采样时长（秒），默认 30
#   -p, --pid PID         指定 rmdb PID，省略则自动定位
#   -w, --width PX        火焰图画布宽度，默认 4000（越大帧越宽、名字越清晰）
#   -m, --minwidth PX     最小可见帧宽度，默认 0.5（更小的帧不绘制）
#   --demangle            对 C++ mangled 符号做 demangle（更易读）
#   --reuse               跳过采样，复用已有 perf_out/perf.data 只重跑渲染
#
# 示例：
#   ./scripts/perf_flamegraph.sh                       # 默认采样 30s
#   ./scripts/perf_flamegraph.sh -d 60                 # 采样 60s
#   ./scripts/perf_flamegraph.sh -d 60 -p 12345        # 指定 PID 采样 60s
#   ./scripts/perf_flamegraph.sh --reuse --demangle -w 6000   # 复用数据、demangle、放大画布重渲染
#
# 依赖：perf、perl、git。perf 采集需要 root 权限，脚本会自动 sudo。

set -euo pipefail

DURATION=15
PID_ARG=""
WIDTH=2000
MINWIDTH=0.5
DEMANGLE=0
REUSE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        -d|--duration)  DURATION="$2"; shift 2 ;;
        -p|--pid)       PID_ARG="$2"; shift 2 ;;
        -w|--width)     WIDTH="$2"; shift 2 ;;
        -m|--minwidth)  MINWIDTH="$2"; shift 2 ;;
        --demangle)     DEMANGLE=1; shift ;;
        --reuse)        REUSE=1; shift ;;
        -h|--help)
            sed -n '2,30p' "$0"; exit 0 ;;
        *) err "未知参数：$1"; exit 1 ;;
    esac
done

# FlameGraph 工具集目录：与脚本平级，便于复用
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
FLAMEGRAPH_DIR="${PROJECT_ROOT}/FlameGraph"
OUT_DIR="${PROJECT_ROOT}/perf_out"

# 注意：log 必须输出到 stderr，否则会被 $(do_record) 当作返回值捕获
log() { printf '\033[1;34m[flamegraph]\033[0m %s\n' "$*" >&2; }
err() { printf '\033[1;31m[flamegraph error]\033[0m %s\n' "$*" >&2; }

# ── 1. 检查/克隆 FlameGraph 工具集 ──────────────────────────────────────────
ensure_flamegraph() {
    if [[ -x "${FLAMEGRAPH_DIR}/flamegraph.pl" && -x "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" ]]; then
        log "FlameGraph 工具集已存在：${FLAMEGRAPH_DIR}"
        return 0
    fi
    log "FlameGraph 工具集缺失，开始 clone..."
    if ! command -v git >/dev/null 2>&1; then
        err "缺少 git，请先安装"; exit 1
    fi
    git clone --depth 1 https://github.com/brendangregg/FlameGraph.git "${FLAMEGRAPH_DIR}"
    log "clone 完成：${FLAMEGRAPH_DIR}"
}

# ── 2. 定位 rmdb PID ────────────────────────────────────────────────────────
find_rmdb_pid() {
    if [[ -n "${PID_ARG}" ]]; then
        if kill -0 "${PID_ARG}" 2>/dev/null; then
            echo "${PID_ARG}"
            return 0
        fi
        err "指定的 PID ${PID_ARG} 不存在"
        exit 1
    fi
    # pgrep 优先按进程名匹配（排除 grep 自身与脚本进程）
    local pid
    pid="$(pgrep -x rmdb | head -n1 || true)"
    if [[ -z "${pid}" ]]; then
        err "未找到 rmdb 进程。请先启动：./build/bin/rmdb <dbdir>"
        err "或手动指定 PID：./scripts/perf_flamegraph.sh ${DURATION} <pid>"
        exit 1
    fi
    echo "${pid}"
}

# ── 3. perf record 采集 ─────────────────────────────────────────────────────
do_record() {
    local pid="$1"
    local perf_data="${OUT_DIR}/perf.data"
    log "目标 PID=${pid}，采样 ${DURATION}s（CPU 采样，频率 99Hz）"
    log "输出：${perf_data}"

    # -g 启用调用栈；--call-graph dwarf 更可靠地展开 C++ 用户态栈
    local perf_cmd=(perf record -F 99 -p "${pid}" -g --call-graph dwarf -o "${perf_data}")
    ( set -x; sudo "${perf_cmd[@]}" -- sleep "${DURATION}" ) || {
        err "perf record 失败（退出码 $?）"
        err "可能原因：1) 进程已退出 2) perf_event_paranoid 限制 3) 磁盘空间不足"
        exit 1
    }
    echo "${perf_data}"
}

# ── 4. 生成火焰图 ────────────────────────────────────────────────────────────
generate_flamegraph() {
    local perf_data="$1"
    local folded="${perf_data%.data}.folded"
    local svg="${perf_data%.data}.svg"

    log "perf script 解析采样数据..."
    # sudo 是因为 perf.data 通常由 root 写入，普通用户无读权限
    sudo perf script -i "${perf_data}" \
        | "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" \
        > "${folded}"
    log "折叠栈完成：${folded}"

    if [[ "${DEMANGLE}" -eq 1 ]]; then
        command -v c++filt >/dev/null 2>&1 || { err "缺少 c++filt（binutils），无法 demangle"; exit 1; }
        local demangled="${folded%.folded}.demangled.folded"
        # folded 行格式：a;b;c 123。用哨兵分隔符保护行结构，
        # 一次性喂给 c++filt 做批量 demangle（比逐行子进程快得多）。
        #   \x1e 替换分号（帧分隔），\x1f 分隔栈与样本数
        awk '{
            n = split($0, parts, " ")
            samples = parts[n]
            stack = substr($0, 1, length($0) - length(samples) - 1)
            gsub(/;/, "\x1e", stack)
            print stack "\x1f" samples
        }' "${folded}" | c++filt | awk '{
            gsub(/\x1e/, ";")
            gsub(/\x1f/, " ")
            print
        }' > "${demangled}"
        folded="${demangled}"
        log "C++ demangle 完成：${folded}"
    fi

    "${FLAMEGRAPH_DIR}/flamegraph.pl" \
        --width "${WIDTH}" \
        --minwidth "${MINWIDTH}" \
        --title "rmdb flamegraph (pid ${TARGET_PID:-?}, ${DURATION}s)" \
        "${folded}" > "${svg}"
    log "火焰图生成完成：${svg}（画布宽 ${WIDTH}px，最小帧 ${MINWIDTH}px）"
}

main() {
    command -v perf >/dev/null 2>&1 || { err "缺少 perf"; exit 1; }
    command -v perl >/dev/null 2>&1 || { err "缺少 perl"; exit 1; }

    mkdir -p "${OUT_DIR}"

    ensure_flamegraph

    local perf_data="${OUT_DIR}/perf.data"
    if [[ "${REUSE}" -eq 1 ]]; then
        [[ -f "${perf_data}" ]] || { err "--reuse 指定但 ${perf_data} 不存在"; exit 1; }
        log "复用已有采样数据：${perf_data}"
        TARGET_PID=""
    else
        TARGET_PID="$(find_rmdb_pid)"
        log "已定位 rmdb PID=${TARGET_PID}（$(ps -o comm= -p "${TARGET_PID}" 2>/dev/null || echo '?')）"
        perf_data="$(do_record "${TARGET_PID}")"
    fi
    generate_flamegraph "${perf_data}"

    local svg="${perf_data%.data}.svg"
    log "完成。用浏览器打开：${svg}"
    log "产物目录：${OUT_DIR}"
}

main "$@"
