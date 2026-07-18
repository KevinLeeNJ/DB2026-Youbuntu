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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "jit/jit_ir.h"

namespace jit {

enum class JitMode { OFF, AUTO, FORCE };

enum class JitEntryState { COLD, OBSERVING, QUEUED, COMPILING, READY, FAILED_COOLDOWN, EVICTED };

struct JitManagerConfig {
    size_t max_entries{256};
    size_t max_code_bytes{16U * 1024U * 1024U};
    size_t max_queue_size{64};
    uint64_t min_executions{32};
    uint64_t min_tuple_evaluations{256};
    uint64_t min_interpreted_ns{100000};
    std::chrono::seconds failure_cooldown{60};
};

struct JitObservation {
    uint64_t tuple_evaluations{0};
    uint64_t interpreted_ns{0};
};

struct JitManagerStats {
    size_t entry_count{0};
    size_t queued_count{0};
    size_t code_bytes{0};
    uint64_t cache_hits{0};
    uint64_t fallbacks{0};
    uint64_t compile_attempts{0};
    uint64_t compile_failures{0};
    uint64_t evictions{0};
};

class JitManager {
public:
    using CompileFunction = std::function<JitCompileResult(const JitProgram&)>;
    using CatalogGenerationFunction = std::function<uint64_t()>;

    class ExecutionScope {
    public:
        ExecutionScope() = default;
        ~ExecutionScope();

        ExecutionScope(const ExecutionScope&) = delete;
        ExecutionScope& operator=(const ExecutionScope&) = delete;
        ExecutionScope(ExecutionScope&& other) noexcept;
        ExecutionScope& operator=(ExecutionScope&& other) noexcept;

    private:
        friend class JitManager;

        explicit ExecutionScope(JitManager* manager) : manager_(manager) {}
        void reset();

        JitManager* manager_{nullptr};
    };

    JitManager(JitManagerConfig config, CatalogGenerationFunction catalog_generation, CompileFunction compile);
    ~JitManager();

    JitManager(const JitManager&) = delete;
    JitManager& operator=(const JitManager&) = delete;

    std::shared_ptr<const JitCode> observe(const JitProgram& program, JitMode mode, JitObservation observation = {});
    ExecutionScope enter_execution();
    void discard(const JitProgram& program);
    bool wait_until_idle(std::chrono::milliseconds timeout);
    void shutdown_and_drain();
    JitManagerStats stats() const;

private:
    struct Entry;

    std::shared_ptr<const JitCode> compile_immediately(const std::shared_ptr<Entry>& entry);
    void worker_loop();
    void publish_compile_result(const std::shared_ptr<Entry>& entry, JitCompileResult result,
                                std::chrono::nanoseconds compile_time);
    void evict_locked(const std::shared_ptr<Entry>& protected_entry,
                      std::vector<std::shared_ptr<const JitCode>>* released_code);
    void leave_execution();

    JitManagerConfig config_;
    CatalogGenerationFunction catalog_generation_;
    CompileFunction compile_;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace jit
