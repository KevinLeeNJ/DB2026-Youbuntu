/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
*/

#pragma once

#include <exception>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "compiled/program_runtime.h"
#include "execution/result_sink.h"
#include "execution/runtime/delete_runtime.h"
#include "execution/runtime/insert_runtime.h"
#include "execution/runtime/point_lookup_runtime.h"

struct PointIndexRuntimeBinding {
    std::string table_name;
    std::vector<std::string> index_col_names;
    std::vector<uint32_t> tuple_offsets;
};

struct DatabaseProgramBindings {
    uint64_t catalog_generation{0};
    std::vector<PointIndexRuntimeBinding> point_indexes;
    const UpdateRuntimeInfo* update_info{nullptr};
    const DeleteRuntimeInfo* delete_info{nullptr};
    const InsertRuntimeInfo* insert_info{nullptr};
    ResultSink* result_sink{nullptr};
};

class DatabaseProgramRuntime : public compiled::ProgramRuntime {
public:
    DatabaseProgramRuntime(SmManager* sm_manager, Context* context, DatabaseProgramBindings bindings)
        : sm_manager_(sm_manager), context_(context), bindings_(std::move(bindings)) {}

    compiled::ExecStatus MakePointKey(uint32_t index_id, const compiled::RuntimeValue& value,
                                      compiled::RuntimeValue* key) noexcept override;
    compiled::ExecStatus PointLookup(const compiled::RuntimeValue& key, compiled::RuntimeValue* row,
                                     compiled::RuntimeValue* tuple) noexcept override;
    compiled::ExecStatus PrepareUpdate(const compiled::RuntimeValue& row, compiled::RuntimeValue* current_tuple,
                                       compiled::RuntimeValue* prepared) noexcept override;
    compiled::ExecStatus CommitUpdate(const compiled::RuntimeValue& prepared,
                                      const compiled::RuntimeValue& proposed_tuple) noexcept override;
    compiled::ExecStatus DeleteRow(const compiled::RuntimeValue& row) noexcept override;
    compiled::ExecStatus InsertRow(const compiled::RuntimeValue& tuple, compiled::RuntimeValue* row) noexcept override;
    compiled::ExecStatus EmitRow(const compiled::RuntimeValue& tuple) noexcept override;

    compiled::ExecStatus FinishResult() noexcept;
    void RethrowPending() const;

    bool fallback_allowed() const {
        return fallback_allowed_;
    }
    bool has_pending_exception() const {
        return pending_exception_ != nullptr;
    }

private:
    struct RowCapability {
        Rid rid;
        std::string table_name;
        int table_fd{-1};
        uint64_t catalog_generation{0};
    };

    struct PreparedUpdateCapability {
        PreparedUpdate token;
        Rid rid;
        int table_fd{-1};
        uint64_t catalog_generation{0};
    };

    compiled::ExecStatus Sticky() const noexcept;
    compiled::ExecStatus Fail(compiled::ExecStatus status, const char* message,
                              std::exception_ptr error = nullptr) noexcept;
    compiled::ExecStatus Fallback(const char* message) noexcept;
    compiled::ExecStatus Capture(compiled::ExecStatus status, const char* helper) noexcept;
    bool CatalogMatches() const noexcept;
    bool BeginStateful() noexcept;
    static uint64_t NewCapability();
    uint64_t RegisterRowCapability(const Rid& rid, const std::string& table_name, int table_fd,
                                   uint64_t catalog_generation);
    const RowCapability& ResolveRowCapability(const compiled::RuntimeValue& row, const std::string& table_name,
                                              int table_fd) const;
    bool HasOutstandingUpdate(int table_fd, const Rid& rid) const;

    SmManager* sm_manager_;
    Context* context_;
    DatabaseProgramBindings bindings_;
    std::unordered_map<uint64_t, RowCapability> row_capabilities_;
    std::unordered_map<uint64_t, PreparedUpdateCapability> prepared_updates_;
    std::exception_ptr pending_exception_;
    bool fallback_allowed_{true};
};
