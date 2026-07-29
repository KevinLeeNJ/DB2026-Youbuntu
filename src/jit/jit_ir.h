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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/common.h"
#include "jit/jit_types.h"
#include "system/sm.h"
#include "optimizer/plan.h"

namespace jit {

inline constexpr uint32_t JIT_IR_VERSION = 1;
inline constexpr uint32_t JIT_ABI_VERSION = 1;

struct JitDigest {
    uint64_t high{0};
    uint64_t low{0};

    friend bool operator==(const JitDigest& lhs, const JitDigest& rhs) {
        return lhs.high == rhs.high && lhs.low == rhs.low;
    }

    friend bool operator!=(const JitDigest& lhs, const JitDigest& rhs) {
        return !(lhs == rhs);
    }
};

struct JitCodeKey {
    JitDigest digest;
    std::string canonical_bytes;

    friend bool operator==(const JitCodeKey& lhs, const JitCodeKey& rhs) {
        return lhs.digest == rhs.digest && lhs.canonical_bytes == rhs.canonical_bytes;
    }

    friend bool operator!=(const JitCodeKey& lhs, const JitCodeKey& rhs) {
        return !(lhs == rhs);
    }
};

enum class JitOperandSource : uint8_t { TUPLE0, TUPLE1, PARAMETER };

struct JitOperand {
    JitOperandSource source{JitOperandSource::TUPLE0};
    ColType type{TYPE_INT};
    uint32_t offset{0};
    uint32_t len{0};
    uint32_t parameter_index{0};
};

struct JitPredicateInstruction {
    CompOp op{OP_EQ};
    JitOperand lhs;
    JitOperand rhs;
};

struct JitParameterDesc {
    ColType type{TYPE_INT};
    uint32_t max_len{0};
};

struct JitTupleLayout {
    uint32_t tuple_len{0};
    std::vector<ColMeta> columns;
};

struct JitProgram {
    uint32_t ir_version{JIT_IR_VERSION};
    uint32_t abi_version{JIT_ABI_VERSION};
    PlanTag plan_tag{T_Invalid};
    uint64_t catalog_generation{0};
    uint32_t helper_version{1};
    uint32_t cpu_feature_tier{0};
    std::string architecture;
    JitTupleLayout tuple0;
    std::optional<JitTupleLayout> tuple1;
    std::vector<JitParameterDesc> parameters;
    std::vector<JitPredicateInstruction> predicates;
    JitCodeKey key;
};

struct JitValue {
    ColType type{TYPE_INT};
    int32_t int_value{0};
    double float_value{0.0};
    const char* bytes{nullptr};
    uint32_t bytes_len{0};
};

struct JitParamBlock {
    std::vector<JitValue> values;
    std::vector<std::string> owned_bytes;
};

struct JitBindResult {
    std::optional<JitParamBlock> params;
    std::string error;

    explicit operator bool() const {
        return params.has_value();
    }
};

struct JitCallFrame {
    const char* tuple0{nullptr};
    uint32_t tuple0_len{0};
    const char* tuple1{nullptr};
    uint32_t tuple1_len{0};
    const JitValue* params{nullptr};
    uint32_t param_count{0};
    bool match{false};
};

struct JitBuildResult {
    std::optional<JitProgram> program;
    JitParamBlock params;
    std::string error;

    explicit operator bool() const {
        return program.has_value();
    }
};

struct JitVerifyResult {
    bool valid{false};
    std::string error;

    explicit operator bool() const {
        return valid;
    }
};

JitBuildResult build_predicate_program(PlanTag plan_tag, const std::vector<Condition>& conditions,
                                       JitTupleLayout tuple0, std::optional<JitTupleLayout> tuple1,
                                       uint64_t catalog_generation);
JitBuildResult build_predicate_program(const ScanPlan& plan, uint64_t catalog_generation);
JitBuildResult build_predicate_program(const FilterPlan& plan, JitTupleLayout input, uint64_t catalog_generation);
JitBuildResult build_predicate_program(const JoinPlan& plan, JitTupleLayout left, JitTupleLayout right,
                                       uint64_t catalog_generation);

JitVerifyResult verify_program(const JitProgram& program);
JitBindResult bind_parameters(const JitProgram& program, const std::vector<Condition>& conditions);
JitStatus interpret_predicate(const JitProgram& program, JitCallFrame* frame);

} // namespace jit
