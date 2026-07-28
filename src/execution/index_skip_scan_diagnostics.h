/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace index_skip_scan_diagnostics {

// Diagnostics are deliberately opt-in: the ranking path pays only one
// predictable branch per prepared operation and skip-scan invocation. The
// enabled path may take a mutex because it is for attribution, not ranking.
inline bool enabled() noexcept {
    static const bool value = [] {
        const char* configured = std::getenv("RMDB_SKIP_SCAN_DIAGNOSTICS");
        if (configured == nullptr) {
            return false;
        }
        const std::string_view text(configured);
        return !text.empty() && text != "0" && text != "false" && text != "off";
    }();
    return value;
}

inline thread_local std::uint16_t active_statement_id = 0;
inline thread_local std::uint64_t active_plan_hash = 0;

class StatementScope {
public:
    StatementScope(std::uint16_t statement_id, std::uint64_t plan_hash)
        : previous_statement_id_(active_statement_id), previous_plan_hash_(active_plan_hash) {
        active_statement_id = statement_id;
        active_plan_hash = plan_hash;
    }

    ~StatementScope() {
        active_statement_id = previous_statement_id_;
        active_plan_hash = previous_plan_hash_;
    }

    StatementScope(const StatementScope&) = delete;
    StatementScope& operator=(const StatementScope&) = delete;

private:
    std::uint16_t previous_statement_id_;
    std::uint64_t previous_plan_hash_;
};

inline std::uint16_t current_statement_id() {
    return active_statement_id;
}

inline std::uint64_t current_plan_hash() {
    return active_plan_hash;
}

struct Snapshot {
    std::uint16_t statement_id{0};
    std::uint64_t plan_hash{0};
    std::string table_name;
    std::string index_name;
    std::size_t prefix_column_count{0};
    std::uint64_t invocations{0};
    std::uint64_t prefixes{0};
    std::uint64_t ranges{0};
    std::uint64_t descents{0};
    std::uint64_t build_ns{0};
    std::uint64_t build_ns_max{0};
};

using SnapshotKey = std::tuple<std::uint16_t, std::uint64_t, std::string, std::string, std::size_t>;

struct State {
    std::mutex latch;
    std::map<SnapshotKey, Snapshot> snapshots;
    std::set<std::string> prepared_plans;
};

inline State& state() {
    static State instance;
    return instance;
}

inline void observe_build(std::uint16_t statement_id, std::uint64_t plan_hash, const std::string& table_name,
                          const std::string& index_name, std::size_t prefix_column_count, std::uint64_t prefixes,
                          std::uint64_t ranges, std::uint64_t descents, std::uint64_t build_ns) noexcept {
    try {
        State& shared = state();
        std::lock_guard<std::mutex> guard(shared.latch);
        const SnapshotKey key{statement_id, plan_hash, table_name, index_name, prefix_column_count};
        auto [it, inserted] = shared.snapshots.try_emplace(key);
        Snapshot& snapshot = it->second;
        if (inserted) {
            snapshot.statement_id = statement_id;
            snapshot.plan_hash = plan_hash;
            snapshot.table_name = table_name;
            snapshot.index_name = index_name;
            snapshot.prefix_column_count = prefix_column_count;
        }
        ++snapshot.invocations;
        snapshot.prefixes += prefixes;
        snapshot.ranges += ranges;
        snapshot.descents += descents;
        snapshot.build_ns += build_ns;
        snapshot.build_ns_max = std::max(snapshot.build_ns_max, build_ns);
    } catch (...) {
        // Diagnostics must never alter execution behavior.
    }
}

inline std::vector<Snapshot> snapshots() noexcept {
    try {
        State& shared = state();
        std::lock_guard<std::mutex> guard(shared.latch);
        std::vector<Snapshot> result;
        result.reserve(shared.snapshots.size());
        for (const auto& [key, snapshot] : shared.snapshots) {
            (void)key;
            result.push_back(snapshot);
        }
        return result;
    } catch (...) {
        return {};
    }
}

// PREPARE_SET is connection-local, so many connections may submit
// the same dictionary. Return true only for the first process-wide occurrence
// of a mapping to keep rmdb.log useful.
inline bool register_prepared_plan(const std::string& mapping) noexcept {
    try {
        State& shared = state();
        std::lock_guard<std::mutex> guard(shared.latch);
        return shared.prepared_plans.emplace(mapping).second;
    } catch (...) {
        return false;
    }
}

} // namespace index_skip_scan_diagnostics
