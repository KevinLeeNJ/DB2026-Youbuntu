/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <atomic>
#include <climits>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unistd.h>

class FaultInjector {
public:
    static void Point(const char* name) {
#ifndef RMDB_ENABLE_FAULT_INJECTION
        (void)name;
        return;
#else
        const char* configured_point = std::getenv("RMDB_FAULT_POINT");
        const char* configured_action = std::getenv("RMDB_FAULT_ACTION");
        if (configured_point == nullptr || configured_action == nullptr || std::string(configured_point) != name) {
            return;
        }

        int skip_count = 0;
        if (const char* skip = std::getenv("RMDB_FAULT_SKIP"); skip != nullptr) {
            char* end = nullptr;
            long parsed = std::strtol(skip, &end, 10);
            if (end != skip && *end == '\0' && parsed > 0) {
                skip_count = parsed > INT_MAX ? INT_MAX : static_cast<int>(parsed);
            }
        }
        if (point_occurrences_.fetch_add(1, std::memory_order_relaxed) < skip_count) {
            return;
        }

        const std::string action(configured_action);
        if (action == "abort") {
            std::abort();
        }
        if (action == "_exit") {
            ::_exit(137);
        }
        if (action == "throw") {
            throw std::runtime_error("fault injection at " + std::string(name));
        }
        if (action == "block") {
            std::unique_lock<std::mutex> lock(block_mutex_);
            block_cv_.wait(lock, [] { return block_released_.load(std::memory_order_acquire); });
        }
#endif
    }

    static void ReleaseBlockedPoint() {
        block_released_.store(true, std::memory_order_release);
        block_cv_.notify_all();
    }

    // Fault-enabled tests may configure different points sequentially in one
    // process. Occurrence counts are scoped to one injected run, not to the
    // lifetime of the test binary.
    static void ResetForTest() noexcept {
        point_occurrences_.store(0, std::memory_order_relaxed);
        block_released_.store(false, std::memory_order_release);
    }

private:
    inline static std::atomic<bool> block_released_{false};
    inline static std::atomic<int> point_occurrences_{0};
    inline static std::mutex block_mutex_;
    inline static std::condition_variable block_cv_;
};
