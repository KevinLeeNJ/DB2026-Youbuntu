#!/usr/bin/env bash
# Compile and run the standalone WAL stabilization microbenchmark without
# modifying the repository build tree.  The binary is removed on exit.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/wal-sync-microbench-build.XXXXXX")
cleanup() {
    rm -f -- "$build_dir/wal_sync_microbench"
    rmdir -- "$build_dir"
}
trap cleanup EXIT

if ! command -v g++ >/dev/null 2>&1; then
    echo "error: g++ is required" >&2
    exit 127
fi

g++ -std=c++17 -O2 -Wall -Wextra -Wpedantic "$script_dir/wal_sync_microbench.cpp" \
    -o "$build_dir/wal_sync_microbench"
exec "$build_dir/wal_sync_microbench" "$@"
