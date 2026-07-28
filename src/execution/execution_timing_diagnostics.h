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

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <mutex>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace execution_timing_diagnostics {

inline bool configured_enabled(const char* configured) {
    if (configured == nullptr) {
        return false;
    }
    const std::string_view text(configured);
    return !text.empty() && text != "0" && text != "false" && text != "off";
}

inline bool enabled() {
    static const bool value = configured_enabled(std::getenv("RMDB_EXECUTION_TIMING_DIAGNOSTICS"));
    return value;
}

enum class Route : std::uint8_t { Fallback, PreparedPlan, PreparedRuntime };

enum class FallbackReason : std::uint8_t { None, Null, Schema, Shape, Bind };

inline const char* route_name(Route route) {
    switch (route) {
    case Route::Fallback:
        return "fallback";
    case Route::PreparedPlan:
        return "prepared-plan";
    case Route::PreparedRuntime:
        return "prepared-runtime";
    }
    return "unknown";
}

inline const char* fallback_reason_name(FallbackReason reason) {
    switch (reason) {
    case FallbackReason::None:
        return "none";
    case FallbackReason::Null:
        return "null";
    case FallbackReason::Schema:
        return "schema";
    case FallbackReason::Shape:
        return "shape";
    case FallbackReason::Bind:
        return "bind";
    }
    return "unknown";
}

struct Sample {
    Route route{Route::Fallback};
    FallbackReason fallback_reason{FallbackReason::None};
    std::uint64_t total_ns{0};
    std::uint64_t bind_ns{0};
    std::uint64_t clone_bind_ns{0};
    std::uint64_t analyze_ns{0};
    std::uint64_t plan_ns{0};
    std::uint64_t instantiate_ns{0};
    std::uint64_t portal_run_ns{0};
    std::uint64_t executor_constructed{0};
    std::uint64_t executor_reused{0};
};

struct Snapshot : Sample {
    std::uint16_t statement_id{0};
    std::uint64_t plan_hash{0};
    std::uint64_t invocations{0};
    std::uint64_t failures{0};
    std::uint64_t aborts{0};
    std::uint64_t errors{0};
};

using SnapshotKey = std::tuple<std::uint16_t, std::uint64_t, Route, FallbackReason>;

struct State {
    std::mutex latch;
    std::map<SnapshotKey, Snapshot> snapshots;
};

inline State& state() {
    static State instance;
    return instance;
}

inline void observe(std::uint16_t statement_id, std::uint64_t plan_hash, const Sample& sample, bool failed,
                    bool aborted = false, bool errored = false) noexcept {
    try {
        State& shared = state();
        std::lock_guard<std::mutex> guard(shared.latch);
        const SnapshotKey key{statement_id, plan_hash, sample.route, sample.fallback_reason};
        auto [it, inserted] = shared.snapshots.try_emplace(key);
        Snapshot& snapshot = it->second;
        if (inserted) {
            snapshot.statement_id = statement_id;
            snapshot.plan_hash = plan_hash;
            snapshot.route = sample.route;
            snapshot.fallback_reason = sample.fallback_reason;
        }
        ++snapshot.invocations;
        snapshot.failures += failed ? 1 : 0;
        snapshot.aborts += aborted ? 1 : 0;
        snapshot.errors += errored ? 1 : 0;
        snapshot.total_ns += sample.total_ns;
        snapshot.bind_ns += sample.bind_ns;
        snapshot.clone_bind_ns += sample.clone_bind_ns;
        snapshot.analyze_ns += sample.analyze_ns;
        snapshot.plan_ns += sample.plan_ns;
        snapshot.instantiate_ns += sample.instantiate_ns;
        snapshot.portal_run_ns += sample.portal_run_ns;
        snapshot.executor_constructed += sample.executor_constructed;
        snapshot.executor_reused += sample.executor_reused;
    } catch (...) {
        // Opt-in diagnostics must never change transaction or protocol behavior.
    }
}

inline void observe_abort(std::uint16_t statement_id, std::uint64_t plan_hash, Route route = Route::Fallback,
                          FallbackReason fallback_reason = FallbackReason::None) noexcept {
    try {
        State& shared = state();
        std::lock_guard<std::mutex> guard(shared.latch);
        Snapshot& snapshot = shared.snapshots[{statement_id, plan_hash, route, fallback_reason}];
        snapshot.statement_id = statement_id;
        snapshot.plan_hash = plan_hash;
        snapshot.route = route;
        snapshot.fallback_reason = fallback_reason;
        ++snapshot.aborts;
    } catch (...) {
        // Opt-in diagnostics must never change transaction or protocol behavior.
    }
}

inline void observe_error(std::uint16_t statement_id, std::uint64_t plan_hash, Route route = Route::Fallback,
                          FallbackReason fallback_reason = FallbackReason::None) noexcept {
    try {
        State& shared = state();
        std::lock_guard<std::mutex> guard(shared.latch);
        Snapshot& snapshot = shared.snapshots[{statement_id, plan_hash, route, fallback_reason}];
        snapshot.statement_id = statement_id;
        snapshot.plan_hash = plan_hash;
        snapshot.route = route;
        snapshot.fallback_reason = fallback_reason;
        ++snapshot.errors;
    } catch (...) {
        // Opt-in diagnostics must never change transaction or protocol behavior.
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

class StageTimer {
public:
    explicit StageTimer(std::uint64_t* destination)
        : destination_(destination), started_(std::chrono::steady_clock::now()) {}

    ~StageTimer() {
        *destination_ += static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started_).count());
    }

    StageTimer(const StageTimer&) = delete;
    StageTimer& operator=(const StageTimer&) = delete;

private:
    std::uint64_t* destination_;
    std::chrono::steady_clock::time_point started_;
};

class OperationScope {
public:
    OperationScope(std::uint16_t statement_id, std::uint64_t plan_hash, Route route = Route::Fallback,
                   FallbackReason fallback_reason = FallbackReason::None)
        : statement_id_(statement_id), plan_hash_(plan_hash), started_(std::chrono::steady_clock::now()) {
        sample_.route = route;
        sample_.fallback_reason = fallback_reason;
    }

    ~OperationScope() {
        if (!recorded_) {
            finish_sample(true);
        }
    }

    OperationScope(const OperationScope&) = delete;
    OperationScope& operator=(const OperationScope&) = delete;

    Sample& sample() {
        return sample_;
    }

    void finish() {
        finish_sample(false);
    }

    void finish_abort() {
        finish_sample(true, true, false);
    }

    void finish_error() {
        finish_sample(true, false, true);
    }

private:
    void finish_sample(bool failed, bool aborted = false, bool errored = false) {
        if (recorded_) {
            return;
        }
        sample_.total_ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started_).count());
        observe(statement_id_, plan_hash_, sample_, failed, aborted, errored);
        recorded_ = true;
    }

    std::uint16_t statement_id_;
    std::uint64_t plan_hash_;
    std::chrono::steady_clock::time_point started_;
    Sample sample_;
    bool recorded_{false};
};

} // namespace execution_timing_diagnostics
