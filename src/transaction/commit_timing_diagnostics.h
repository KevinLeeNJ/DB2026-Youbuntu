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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string_view>

namespace commit_timing_diagnostics {

inline bool configured_enabled(const char* configured) {
    if (configured == nullptr) {
        return false;
    }
    const std::string_view text(configured);
    return !text.empty() && text != "0" && text != "false" && text != "off";
}

inline bool enabled() {
    static const bool value = configured_enabled(std::getenv("RMDB_COMMIT_TIMING_DIAGNOSTICS"));
    return value;
}

enum class Stage {
    PREPARE_PUBLICATION,
    TIMESTAMP_CSN,
    WAL,
    TUPLE_PUBLICATION,
    FRONTIER_PUBLICATION,
    FRONTIER_WAIT,
    CLEANUP,
};

struct Sample {
    std::uint64_t total_ns{0};
    std::uint64_t prepare_publication_ns{0};
    std::uint64_t timestamp_csn_ns{0};
    std::uint64_t wal_ns{0};
    std::uint64_t tuple_publication_ns{0};
    std::uint64_t frontier_publication_ns{0};
    std::uint64_t frontier_wait_ns{0};
    std::uint64_t cleanup_ns{0};
};

struct Snapshot : Sample {
    std::uint64_t invocations{0};
    std::uint64_t failures{0};
};

struct State {
    std::atomic<std::uint64_t> invocations{0};
    std::atomic<std::uint64_t> failures{0};
    std::atomic<std::uint64_t> total_ns{0};
    std::atomic<std::uint64_t> prepare_publication_ns{0};
    std::atomic<std::uint64_t> timestamp_csn_ns{0};
    std::atomic<std::uint64_t> wal_ns{0};
    std::atomic<std::uint64_t> tuple_publication_ns{0};
    std::atomic<std::uint64_t> frontier_publication_ns{0};
    std::atomic<std::uint64_t> frontier_wait_ns{0};
    std::atomic<std::uint64_t> cleanup_ns{0};
};

inline State& state() {
    static State instance;
    return instance;
}

inline void observe(const Sample& sample, bool failed) noexcept {
    State& shared = state();
    shared.invocations.fetch_add(1, std::memory_order_relaxed);
    shared.failures.fetch_add(failed ? 1 : 0, std::memory_order_relaxed);
    shared.total_ns.fetch_add(sample.total_ns, std::memory_order_relaxed);
    shared.prepare_publication_ns.fetch_add(sample.prepare_publication_ns, std::memory_order_relaxed);
    shared.timestamp_csn_ns.fetch_add(sample.timestamp_csn_ns, std::memory_order_relaxed);
    shared.wal_ns.fetch_add(sample.wal_ns, std::memory_order_relaxed);
    shared.tuple_publication_ns.fetch_add(sample.tuple_publication_ns, std::memory_order_relaxed);
    shared.frontier_publication_ns.fetch_add(sample.frontier_publication_ns, std::memory_order_relaxed);
    shared.frontier_wait_ns.fetch_add(sample.frontier_wait_ns, std::memory_order_relaxed);
    shared.cleanup_ns.fetch_add(sample.cleanup_ns, std::memory_order_relaxed);
}

inline Snapshot snapshot() {
    State& shared = state();
    Snapshot result;
    result.invocations = shared.invocations.load(std::memory_order_relaxed);
    result.failures = shared.failures.load(std::memory_order_relaxed);
    result.total_ns = shared.total_ns.load(std::memory_order_relaxed);
    result.prepare_publication_ns = shared.prepare_publication_ns.load(std::memory_order_relaxed);
    result.timestamp_csn_ns = shared.timestamp_csn_ns.load(std::memory_order_relaxed);
    result.wal_ns = shared.wal_ns.load(std::memory_order_relaxed);
    result.tuple_publication_ns = shared.tuple_publication_ns.load(std::memory_order_relaxed);
    result.frontier_publication_ns = shared.frontier_publication_ns.load(std::memory_order_relaxed);
    result.frontier_wait_ns = shared.frontier_wait_ns.load(std::memory_order_relaxed);
    result.cleanup_ns = shared.cleanup_ns.load(std::memory_order_relaxed);
    return result;
}

inline void reset_for_test() {
    State& shared = state();
    shared.invocations.store(0, std::memory_order_relaxed);
    shared.failures.store(0, std::memory_order_relaxed);
    shared.total_ns.store(0, std::memory_order_relaxed);
    shared.prepare_publication_ns.store(0, std::memory_order_relaxed);
    shared.timestamp_csn_ns.store(0, std::memory_order_relaxed);
    shared.wal_ns.store(0, std::memory_order_relaxed);
    shared.tuple_publication_ns.store(0, std::memory_order_relaxed);
    shared.frontier_publication_ns.store(0, std::memory_order_relaxed);
    shared.frontier_wait_ns.store(0, std::memory_order_relaxed);
    shared.cleanup_ns.store(0, std::memory_order_relaxed);
}

template <bool Enabled> class OperationScope;

template <> class OperationScope<false> {
public:
    void finish() {}
};

template <> class OperationScope<true> {
public:
    OperationScope() : started_(std::chrono::steady_clock::now()) {}

    ~OperationScope() {
        if (!recorded_) {
            finish_sample(true);
        }
    }

    OperationScope(const OperationScope&) = delete;
    OperationScope& operator=(const OperationScope&) = delete;

    void add(Stage stage, std::uint64_t elapsed_ns) {
        switch (stage) {
        case Stage::PREPARE_PUBLICATION:
            sample_.prepare_publication_ns += elapsed_ns;
            break;
        case Stage::TIMESTAMP_CSN:
            sample_.timestamp_csn_ns += elapsed_ns;
            break;
        case Stage::WAL:
            sample_.wal_ns += elapsed_ns;
            break;
        case Stage::TUPLE_PUBLICATION:
            sample_.tuple_publication_ns += elapsed_ns;
            break;
        case Stage::FRONTIER_PUBLICATION:
            sample_.frontier_publication_ns += elapsed_ns;
            break;
        case Stage::FRONTIER_WAIT:
            sample_.frontier_wait_ns += elapsed_ns;
            break;
        case Stage::CLEANUP:
            sample_.cleanup_ns += elapsed_ns;
            break;
        }
    }

    void finish() {
        finish_sample(false);
    }

private:
    void finish_sample(bool failed) {
        sample_.total_ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started_).count());
        observe(sample_, failed);
        recorded_ = true;
    }

    std::chrono::steady_clock::time_point started_;
    Sample sample_;
    bool recorded_{false};
};

template <bool Enabled> class StageTimer;

template <> class StageTimer<false> {
public:
    StageTimer(OperationScope<false>& scope, Stage stage) {
        (void)scope;
        (void)stage;
    }
};

template <> class StageTimer<true> {
public:
    StageTimer(OperationScope<true>& scope, Stage stage)
        : scope_(scope), stage_(stage), started_(std::chrono::steady_clock::now()) {}

    ~StageTimer() {
        const auto elapsed = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started_).count());
        scope_.add(stage_, elapsed);
    }

    StageTimer(const StageTimer&) = delete;
    StageTimer& operator=(const StageTimer&) = delete;

private:
    OperationScope<true>& scope_;
    Stage stage_;
    std::chrono::steady_clock::time_point started_;
};

} // namespace commit_timing_diagnostics
