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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "execution/prepared_execution_binding.h"
#include "execution/prepared_insert_binding.h"
#include "system/sm.h"
#include "optimizer/plan.h"

enum class PreparedStatementKind {
    Select,
    Insert,
    Update,
    Unsupported,
};

enum class PreparedPlanFallbackReason {
    None,
    NullPlan,
    UnsupportedStatement,
    UnsupportedShape,
    InvalidParameterLayout,
};

struct PreparedParameterSlot {
    std::size_t ordinal;
    ColType type;
};

struct PreparedLimitOffsetLayout {
    std::optional<std::size_t> limit_ordinal;
    std::optional<std::size_t> offset_ordinal;
};

class PreparedPlanDescriptor final {
public:
    static std::unique_ptr<const PreparedPlanDescriptor>
    Build(std::unique_ptr<Plan> plan, PreparedStatementKind statement_kind, std::vector<std::string> output_names,
          std::vector<ColMeta> result_schema, std::string database_identity, std::uint64_t catalog_generation);

    PreparedPlanDescriptor(const PreparedPlanDescriptor&) = delete;
    PreparedPlanDescriptor& operator=(const PreparedPlanDescriptor&) = delete;
    PreparedPlanDescriptor(PreparedPlanDescriptor&&) = delete;
    PreparedPlanDescriptor& operator=(PreparedPlanDescriptor&&) = delete;

    const Plan* plan() const noexcept {
        return plan_.get();
    }

    const DMLPlan* dml_plan() const noexcept {
        return dynamic_cast<const DMLPlan*>(plan_.get());
    }

    PreparedStatementKind statement_kind() const noexcept {
        return statement_kind_;
    }

    const std::vector<std::string>& output_names() const noexcept {
        return output_names_;
    }

    const std::vector<ColMeta>& result_schema() const noexcept {
        return result_schema_;
    }

    const std::vector<PreparedParameterSlot>& parameter_layout() const noexcept {
        return parameter_layout_;
    }

    const PreparedLimitOffsetLayout& limit_offset_layout() const noexcept {
        return limit_offset_layout_;
    }

    const PreparedInsertExecutable* insert_executable() const noexcept {
        return insert_executable_ == nullptr ? nullptr : insert_executable_.get();
    }

    const PreparedSelectExecutable* select_executable() const noexcept {
        return select_executable_ == nullptr ? nullptr : select_executable_.get();
    }

    const PreparedUpdateExecutable* update_executable() const noexcept {
        return update_executable_ == nullptr ? nullptr : update_executable_.get();
    }

    const std::string& database_identity() const noexcept {
        return database_identity_;
    }

    std::uint64_t catalog_generation() const noexcept {
        return catalog_generation_;
    }

    bool eligible() const noexcept {
        return fallback_reason_ == PreparedPlanFallbackReason::None;
    }

    PreparedPlanFallbackReason fallback_reason() const noexcept {
        return fallback_reason_;
    }

private:
    PreparedPlanDescriptor(std::unique_ptr<const Plan> plan, PreparedStatementKind statement_kind,
                           std::vector<std::string> output_names, std::vector<ColMeta> result_schema,
                           std::string database_identity, std::uint64_t catalog_generation);

    std::unique_ptr<const Plan> plan_;
    const PreparedStatementKind statement_kind_;
    const std::vector<std::string> output_names_;
    const std::vector<ColMeta> result_schema_;
    std::vector<PreparedParameterSlot> parameter_layout_;
    PreparedLimitOffsetLayout limit_offset_layout_;
    std::unique_ptr<const PreparedInsertExecutable> insert_executable_;
    std::unique_ptr<const PreparedSelectExecutable> select_executable_;
    std::unique_ptr<const PreparedUpdateExecutable> update_executable_;
    const std::string database_identity_;
    const std::uint64_t catalog_generation_;
    PreparedPlanFallbackReason fallback_reason_ = PreparedPlanFallbackReason::None;
};
