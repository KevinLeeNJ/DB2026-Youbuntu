/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. */

#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <shared_mutex>

// This records observed acquisition latency (including scheduling and work
// before the lock is granted). It deliberately keeps the normal single
// blocking lock acquisition, so it is a low-perturbation contention proxy,
// not an exact mutex-wait measurement.
class ShardAcquisitionMetrics {
public:
    static constexpr size_t kShardCount = 64;

    struct Config {
        bool enabled{false};
        uint8_t sample_log2{0};
        uint64_t slow_ns{1000};

        // Invalid input is disabled rather than reported with an exception:
        // production instances are constructed before main() starts.
        static Config Parse(const char* sample_value, const char* slow_value) noexcept {
            if (sample_value == nullptr) {
                return {};
            }
            uint64_t parsed_sample = 0;
            if (!parse_unsigned(sample_value, &parsed_sample) || parsed_sample > 20) {
                return {};
            }
            Config config;
            config.enabled = true;
            config.sample_log2 = static_cast<uint8_t>(parsed_sample);
            if (slow_value != nullptr) {
                uint64_t parsed_slow = 0;
                if (!parse_unsigned(slow_value, &parsed_slow)) {
                    return {};
                }
                config.slow_ns = static_cast<uint64_t>(parsed_slow);
            }
            return config;
        }

        static Config FromEnvironment(const char* sample_log2_name, const char* slow_ns_name) noexcept {
            return Parse(std::getenv(sample_log2_name), std::getenv(slow_ns_name));
        }

    private:
        static bool parse_unsigned(const char* text, uint64_t* value) noexcept {
            if (text == nullptr || *text == '\0') {
                return false;
            }
            uint64_t parsed = 0;
            for (const char* cursor = text; *cursor != '\0'; ++cursor) {
                if (*cursor < '0' || *cursor > '9') {
                    return false;
                }
                const uint64_t digit = static_cast<uint64_t>(*cursor - '0');
                if (parsed > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
                    return false;
                }
                parsed = parsed * 10 + digit;
            }
            *value = parsed;
            return true;
        }
    };

    struct Snapshot {
        uint64_t sampled_acquisitions{};
        uint64_t slow_acquisitions{};
        uint64_t sampled_elapsed_ns{};
        uint64_t sampled_max_ns{};
    };

    ShardAcquisitionMetrics() = default;

    explicit ShardAcquisitionMetrics(Config config) : config_(sanitize(config)) {}

    bool enabled() const noexcept { return config_.enabled; }
    const Config& config() const noexcept { return config_; }

    template <typename Mutex>
    std::unique_lock<Mutex> acquire_exclusive(Mutex& mutex, size_t shard) {
        if (!config_.enabled || !sample()) {
            return std::unique_lock<Mutex>(mutex);
        }
        const auto begin = Clock::now();
        std::unique_lock<Mutex> lock(mutex);
        record(shard, elapsed_ns(begin));
        return lock;
    }

    template <typename Mutex>
    std::shared_lock<Mutex> acquire_shared(Mutex& mutex, size_t shard) {
        if (!config_.enabled || !sample()) {
            return std::shared_lock<Mutex>(mutex);
        }
        const auto begin = Clock::now();
        std::shared_lock<Mutex> lock(mutex);
        record(shard, elapsed_ns(begin));
        return lock;
    }

    Snapshot snapshot(size_t shard) const noexcept {
        const Counters& counters = counters_[shard];
        return {counters.sampled_acquisitions.load(std::memory_order_relaxed),
                counters.slow_acquisitions.load(std::memory_order_relaxed),
                counters.sampled_elapsed_ns.load(std::memory_order_relaxed),
                counters.sampled_max_ns.load(std::memory_order_relaxed)};
    }

private:
    using Clock = std::chrono::steady_clock;
    struct alignas(64) Counters {
        std::atomic<uint64_t> sampled_acquisitions{0};
        std::atomic<uint64_t> slow_acquisitions{0};
        std::atomic<uint64_t> sampled_elapsed_ns{0};
        std::atomic<uint64_t> sampled_max_ns{0};
    };

    static Config sanitize(Config config) noexcept {
        if (!config.enabled || config.sample_log2 > 20) {
            return {};
        }
        return config;
    }

    static uint64_t mix(uint64_t value) noexcept {
        value += 0x9e3779b97f4a7c15ULL;
        value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
        value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
        return value ^ (value >> 31);
    }

    static uint64_t next_unique_seed() noexcept {
        static std::atomic<uint64_t> next{0x6a09e667f3bcc909ULL};
        return mix(next.fetch_add(0x9e3779b97f4a7c15ULL, std::memory_order_relaxed));
    }

    uint64_t stream_salt() const noexcept {
        uint64_t salt = stream_salt_.load(std::memory_order_relaxed);
        if (salt != 0) {
            return salt;
        }
        const uint64_t candidate = next_unique_seed() | 1;
        stream_salt_.compare_exchange_strong(salt, candidate, std::memory_order_relaxed);
        return salt == 0 ? candidate : salt;
    }

    bool sample() const noexcept {
        thread_local uint64_t thread_state = 0;
        if (thread_state == 0) {
            thread_state = next_unique_seed() | 1;
        }
        const uint64_t random = mix(thread_state ^ stream_salt());
        const uint64_t state = thread_state;
        thread_state = mix(state) | 1;
        const uint64_t mask = (uint64_t{1} << config_.sample_log2) - 1;
        return (random & mask) == 0;
    }

    static uint64_t elapsed_ns(Clock::time_point begin) noexcept {
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin);
        return static_cast<uint64_t>(elapsed.count());
    }

    void record(size_t shard, uint64_t elapsed) noexcept {
        Counters& counters = counters_[shard];
        counters.sampled_acquisitions.fetch_add(1, std::memory_order_relaxed);
        counters.sampled_elapsed_ns.fetch_add(elapsed, std::memory_order_relaxed);
        if (elapsed >= config_.slow_ns) {
            counters.slow_acquisitions.fetch_add(1, std::memory_order_relaxed);
        }
        uint64_t maximum = counters.sampled_max_ns.load(std::memory_order_relaxed);
        while (maximum < elapsed &&
               !counters.sampled_max_ns.compare_exchange_weak(maximum, elapsed, std::memory_order_relaxed)) {
        }
    }

    Config config_;
    mutable std::atomic<uint64_t> stream_salt_{0};
    std::array<Counters, kShardCount> counters_{};
};
