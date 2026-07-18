/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "common/config.h"
#include "compiled/program_template.h"
#include "jit/jit_types.h"

namespace jit {

inline constexpr uint32_t POINT_PROGRAM_NATIVE_HELPER_ABI_VERSION = 1;

struct PointProgramJitKey {
    parser::TokenShapeKey shape;
    uint64_t statement_generation{0};
    uint64_t planner_generation{0};
    uint64_t catalog_generation{0};
    compiled::ProgramKind kind{compiled::ProgramKind::POINT_SELECT};
    uint32_t ir_version{0};
    uint32_t abi_version{0};
    uint32_t helper_abi_version{POINT_PROGRAM_NATIVE_HELPER_ABI_VERSION};

    friend bool operator==(const PointProgramJitKey& lhs, const PointProgramJitKey& rhs) {
        return lhs.shape == rhs.shape && lhs.statement_generation == rhs.statement_generation &&
               lhs.planner_generation == rhs.planner_generation && lhs.catalog_generation == rhs.catalog_generation &&
               lhs.kind == rhs.kind && lhs.ir_version == rhs.ir_version && lhs.abi_version == rhs.abi_version &&
               lhs.helper_abi_version == rhs.helper_abi_version;
    }

    friend bool operator!=(const PointProgramJitKey& lhs, const PointProgramJitKey& rhs) {
        return !(lhs == rhs);
    }
};

struct PointProgramJitKeyHash {
    size_t operator()(const PointProgramJitKey& key) const noexcept;
};

PointProgramJitKey MakePointProgramJitKey(const compiled::ProgramTemplate& program_template);

struct PointProgramJitConfig {
    size_t max_entries{rmdb_config::kJitMaxEntries};
    size_t max_code_bytes{rmdb_config::kJitMaxCodeBytes};
    size_t max_queue_size{rmdb_config::kJitMaxQueueSize};
    uint64_t min_executions{rmdb_config::kJitMinExecutions};
    uint64_t min_interpreted_ns{rmdb_config::kJitMinInterpretedNs};
    std::chrono::seconds failure_cooldown{rmdb_config::kJitFailureCooldownSeconds};
};

struct PointProgramJitStats {
    size_t entry_count{0};
    size_t queued_count{0};
    size_t code_bytes{0};
    uint64_t interpreter_executions{0};
    uint64_t native_executions{0};
    uint64_t native_cache_hits{0};
    uint64_t compile_attempts{0};
    uint64_t compile_failures{0};
    uint64_t evictions{0};
};

class PointProgramJitManager {
public:
    using CompileFunction = std::function<JitCompileResult(compiled::ProgramTemplatePtr)>;
    using IdentityCurrentFunction = std::function<bool(const compiled::ProgramTemplateIdentity&)>;

    PointProgramJitManager(PointProgramJitConfig config, IdentityCurrentFunction identity_current,
                           CompileFunction compile);
    ~PointProgramJitManager();

    PointProgramJitManager(const PointProgramJitManager&) = delete;
    PointProgramJitManager& operator=(const PointProgramJitManager&) = delete;

    std::shared_ptr<const JitCode> Acquire(compiled::ProgramTemplatePtr program_template, rmdb_config::JitMode mode);
    void ObserveInterpreter(compiled::ProgramTemplatePtr program_template, uint64_t elapsed_ns,
                            rmdb_config::JitMode mode);
    void RecordNativeExecution() noexcept;

    bool WaitUntilIdle(std::chrono::milliseconds timeout);
    void ShutdownAndDrain();
    PointProgramJitStats Stats() const;

private:
    struct Entry;
    struct Impl;

    bool IdentityCurrent(const compiled::ProgramTemplateIdentity& identity) const noexcept;
    JitCompileResult Compile(const compiled::ProgramTemplatePtr& program_template) const noexcept;
    void RemoveStale(const PointProgramJitKey& key);
    std::shared_ptr<const JitCode> CompileImmediately(const std::shared_ptr<Entry>& entry);
    void WorkerLoop();
    void PublishCompileResult(const std::shared_ptr<Entry>& entry, JitCompileResult result);
    void EvictLocked(const std::shared_ptr<Entry>& protected_entry,
                     std::vector<std::shared_ptr<const JitCode>>* released_code);

    PointProgramJitConfig config_;
    IdentityCurrentFunction identity_current_;
    CompileFunction compile_;
    std::unique_ptr<Impl> impl_;
};

} // namespace jit
