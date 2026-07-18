/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of the Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <functional>
#include <optional>

#include "jit/jit_ir.h"
#include "jit/jit_manager.h"

class Context;

namespace jit {

class PredicateKernel {
public:
    PredicateKernel() = default;
    PredicateKernel(PlanTag plan_tag, const std::vector<Condition>& conditions, JitTupleLayout tuple0,
                    std::optional<JitTupleLayout> tuple1, uint64_t catalog_generation, Context* context = nullptr);

    explicit operator bool() const {
        return program_ != nullptr;
    }

    std::optional<bool> evaluate(const char* tuple0, uint32_t tuple0_len, const char* tuple1 = nullptr,
                                 uint32_t tuple1_len = 0) const;

private:
    std::shared_ptr<const JitProgram> program_;
    JitParamBlock params_;
    mutable std::shared_ptr<const JitCode> code_;
    mutable uint32_t pending_evaluations_{0};
    mutable bool observed_once_{false};
};

void initialize_predicate_jit(std::function<uint64_t()> catalog_generation);
bool predicate_jit_available();
JitManager::ExecutionScope enter_predicate_jit_execution();
JitManagerStats predicate_jit_stats();
void shutdown_predicate_jit();

} // namespace jit
