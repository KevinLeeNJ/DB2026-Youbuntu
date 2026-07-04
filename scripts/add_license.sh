#!/bin/bash
# add_license.sh - Add/update license headers for .cpp/.h files
#
# Modes:
#   commits   (default) Process files changed between TARGET_COMMIT..HEAD
#   workspace           Process working tree changes (staged, unstaged, untracked)
#
# For new files (did not exist at the base ref):
#   Copyright uses "Team Youbuntu" only.
# For modified files (existed at the base ref):
#   A new "Copyright (c) 2026 Team Youbuntu" line is inserted after the existing line.

set -euo pipefail

TARGET_COMMIT="c8cad8bec64454889dfab7e5d84118ea75cff989"

# License header block for brand-new files (Team Youbuntu only).
# The trailing blank line is intentional: existing files have a blank line between license and code.
read -r -d '' LICENSE_NEW <<'LICEOF' || true
/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

LICEOF

MODE="${1:-commits}"

usage() {
    echo "Usage: $0 [commits|workspace]"
    echo "  commits   - Process files changed between $TARGET_COMMIT..HEAD (default)"
    echo "  workspace - Process working tree changes only"
    exit 1
}

if [ "$MODE" != "commits" ] && [ "$MODE" != "workspace" ]; then
    usage
fi

# ---------------------------------------------------------------------------
# Collect target files
# ---------------------------------------------------------------------------
collect_files() {
    if [ "$MODE" = "workspace" ]; then
        # Staged, unstaged modified, and untracked .cpp/.h files
        {
            git diff --name-only HEAD -- '*.cpp' '*.h' 2>/dev/null || true
            git diff --name-only --cached -- '*.cpp' '*.h' 2>/dev/null || true
            git ls-files --others --exclude-standard -- '*.cpp' '*.h' 2>/dev/null || true
        } | sort -u
    else
        # Files that differ between TARGET_COMMIT and HEAD
        git diff --name-only "$TARGET_COMMIT"..HEAD -- '*.cpp' '*.h' 2>/dev/null || true
    fi
}

# ---------------------------------------------------------------------------
# Determine whether a file is "new" relative to the base ref
# ---------------------------------------------------------------------------
is_new() {
    local file="$1"
    local base_ref
    if [ "$MODE" = "workspace" ]; then
        base_ref="HEAD"
    else
        base_ref="$TARGET_COMMIT"
    fi
    # Returns 0 (true) if the file does NOT exist at base_ref
    ! git cat-file -e "$base_ref:$file" 2>/dev/null
}

# ---------------------------------------------------------------------------
# Process a single file
# ---------------------------------------------------------------------------
process_file() {
    local file="$1"

    # Skip if file does not exist on disk (e.g. deleted in working tree)
    [ -f "$file" ] || return 0

    # Idempotent: skip if "Team Youbuntu" is already present
    if grep -qF 'Team Youbuntu' "$file"; then
        echo "  [SKIP] $file (already has Team Youbuntu)"
        return 0
    fi

    if is_new "$file"; then
        echo "  [NEW]  $file"
        if grep -qF '/* Copyright' "$file"; then
            # File already has a copyright header (likely copied from existing code).
            # Replace the copyright line with Team Youbuntu only.
            sed -i '1,10s|/\* Copyright (c) [0-9]\{4\} .*|/* Copyright (c) 2026 Team Youbuntu|' "$file"
        else
            # No existing copyright — prepend the full license block.
            local tmpfile
            tmpfile=$(mktemp)
            {
                printf '%s\n\n' "$LICENSE_NEW"
                cat "$file"
            } > "$tmpfile"
            mv "$tmpfile" "$file"
        fi
    else
        echo "  [MOD]  $file"
        if grep -qF '/* Copyright' "$file"; then
            # Insert a new "Team Youbuntu" copyright line after the Renmin University line.
            sed -i '/\/\* Copyright (c) [0-9]\{4\} Renmin University of China/ a\   Copyright (c) 2026 Team Youbuntu' "$file"
        else
            echo "  [WARN] $file: modified file has no copyright header — skipping"
            return 0
        fi
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    echo "=== add_license.sh (mode: $MODE) ==="

    local files
    mapfile -t files < <(collect_files)

    if [ ${#files[@]} -eq 0 ]; then
        echo "No .cpp/.h files to process."
        return 0
    fi

    echo "Processing ${#files[@]} file(s)..."
    local count=0
    for f in "${files[@]}"; do
        process_file "$f"
        ((++count))
    done

    echo "Processed $count file(s)."
}

main
