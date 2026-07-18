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

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>
#endif

namespace phase_metrics {

enum class Phase : size_t {
    LEXER,
    NORMALIZE,
    PARSER,
    ANALYZER,
    PLANNER,
    PORTAL_INSTANTIATE,
    EXECUTOR,
    PREDICATE,
    JOIN_COMPARE,
    PROJECTION_COPY,
    UPDATE_ARITHMETIC,
    AGGREGATE_TRANSITION,
    COUNT,
};

struct Snapshot {
    uint64_t samples;
    uint64_t sampled_ns;
    uint64_t sampled_cycles;
};

class Registry {
public:
    static Registry& instance() {
        static Registry registry;
        return registry;
    }

    bool enabled() const {
        return enabled_;
    }

    void record(Phase phase, uint64_t elapsed_ns, uint64_t elapsed_cycles) {
        const auto index = static_cast<size_t>(phase);
        samples_[index].fetch_add(1, std::memory_order_relaxed);
        sampled_ns_[index].fetch_add(elapsed_ns, std::memory_order_relaxed);
        sampled_cycles_[index].fetch_add(elapsed_cycles, std::memory_order_relaxed);
    }

    Snapshot snapshot(Phase phase) const {
        const auto index = static_cast<size_t>(phase);
        return Snapshot{samples_[index].load(std::memory_order_relaxed),
                        sampled_ns_[index].load(std::memory_order_relaxed),
                        sampled_cycles_[index].load(std::memory_order_relaxed)};
    }

    void reset() {
        for (auto& value : samples_) {
            value.store(0, std::memory_order_relaxed);
        }
        for (auto& value : sampled_ns_) {
            value.store(0, std::memory_order_relaxed);
        }
        for (auto& value : sampled_cycles_) {
            value.store(0, std::memory_order_relaxed);
        }
    }

private:
    Registry() : enabled_(std::getenv("RMDB_PHASE_METRICS_PATH") != nullptr) {}

    bool enabled_;
    std::array<std::atomic<uint64_t>, static_cast<size_t>(Phase::COUNT)> samples_{};
    std::array<std::atomic<uint64_t>, static_cast<size_t>(Phase::COUNT)> sampled_ns_{};
    std::array<std::atomic<uint64_t>, static_cast<size_t>(Phase::COUNT)> sampled_cycles_{};
};

inline uint64_t read_cycles() {
#if defined(__x86_64__) || defined(_M_X64)
    return __rdtsc();
#else
    return 0;
#endif
}

class ScopedSample {
public:
    explicit ScopedSample(Phase phase, uint32_t sample_rate = 1) : phase_(phase) {
        auto& registry = Registry::instance();
        if (!registry.enabled()) {
            return;
        }
        auto& counter = counters_[static_cast<size_t>(phase)];
        active_ = (++counter % sample_rate) == 0;
        if (active_) {
            start_ = Clock::now();
            start_cycles_ = read_cycles();
        }
    }

    ~ScopedSample() {
        if (!active_) {
            return;
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start_).count();
        Registry::instance().record(phase_, static_cast<uint64_t>(elapsed), read_cycles() - start_cycles_);
    }

    ScopedSample(const ScopedSample&) = delete;
    ScopedSample& operator=(const ScopedSample&) = delete;

private:
    using Clock = std::chrono::steady_clock;

    Phase phase_;
    bool active_{false};
    Clock::time_point start_{};
    uint64_t start_cycles_{0};
    inline static thread_local std::array<uint32_t, static_cast<size_t>(Phase::COUNT)> counters_{};
};

inline constexpr const char* phase_name(Phase phase) {
    switch (phase) {
    case Phase::LEXER:
        return "lexer";
    case Phase::NORMALIZE:
        return "normalize";
    case Phase::PARSER:
        return "parser";
    case Phase::ANALYZER:
        return "analyzer";
    case Phase::PLANNER:
        return "planner";
    case Phase::PORTAL_INSTANTIATE:
        return "portal_instantiate";
    case Phase::EXECUTOR:
        return "executor";
    case Phase::PREDICATE:
        return "predicate";
    case Phase::JOIN_COMPARE:
        return "join_compare";
    case Phase::PROJECTION_COPY:
        return "projection_copy";
    case Phase::UPDATE_ARITHMETIC:
        return "update_arithmetic";
    case Phase::AGGREGATE_TRANSITION:
        return "aggregate_transition";
    case Phase::COUNT:
        break;
    }
    return "unknown";
}

inline constexpr uint32_t sample_rate(Phase phase) {
    switch (phase) {
    case Phase::LEXER:
    case Phase::PREDICATE:
    case Phase::JOIN_COMPARE:
    case Phase::PROJECTION_COPY:
    case Phase::UPDATE_ARITHMETIC:
    case Phase::AGGREGATE_TRANSITION:
        return 1024;
    case Phase::PARSER:
    case Phase::ANALYZER:
    case Phase::PLANNER:
    case Phase::PORTAL_INSTANTIATE:
    case Phase::EXECUTOR:
        return 64;
    default:
        return 1;
    }
}

} // namespace phase_metrics
