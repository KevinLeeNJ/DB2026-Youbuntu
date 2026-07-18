/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "jit/jit_ir.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <utility>

namespace jit {
namespace {

constexpr uint32_t kMaxPredicates = 1024;
constexpr uint32_t kMaxParameters = 256;
constexpr uint32_t kMaxTupleLength = 1024 * 1024;

void append_u32(std::string& output, uint32_t value) {
    for (uint32_t shift = 0; shift < 32; shift += 8) {
        output.push_back(static_cast<char>((value >> shift) & 0xffU));
    }
}

void append_u64(std::string& output, uint64_t value) {
    for (uint32_t shift = 0; shift < 64; shift += 8) {
        output.push_back(static_cast<char>((value >> shift) & 0xffU));
    }
}

void append_string(std::string& output, const std::string& value) {
    append_u32(output, static_cast<uint32_t>(value.size()));
    output.append(value);
}

void append_layout(std::string& output, const JitTupleLayout& layout) {
    append_u32(output, layout.tuple_len);
    append_u32(output, static_cast<uint32_t>(layout.columns.size()));
    for (const auto& column : layout.columns) {
        append_string(output, column.tab_name);
        append_string(output, column.name);
        append_u32(output, static_cast<uint32_t>(column.type));
        append_u32(output, static_cast<uint32_t>(column.len));
        append_u32(output, static_cast<uint32_t>(column.offset));
    }
}

void append_operand(std::string& output, const JitOperand& operand) {
    output.push_back(static_cast<char>(operand.source));
    append_u32(output, static_cast<uint32_t>(operand.type));
    append_u32(output, operand.offset);
    append_u32(output, operand.len);
    append_u32(output, operand.parameter_index);
}

JitDigest stable_digest(const std::string& input) {
    uint64_t low = 1469598103934665603ULL;
    uint64_t high = 1099511628211ULL;
    for (unsigned char byte : input) {
        low ^= byte;
        low *= 1099511628211ULL;
        high ^= byte + 0x9dU;
        high *= 14029467366897019727ULL;
        high ^= high >> 29;
    }
    return {high, low};
}

std::string host_architecture() {
#if defined(__x86_64__) || defined(_M_X64)
    return "x86_64";
#elif defined(__aarch64__)
    return "aarch64";
#else
    return "unsupported";
#endif
}

std::string serialize_program(const JitProgram& program) {
    std::string output;
    output.reserve(128 + program.predicates.size() * 32);
    output.append("RMDB-JIT-PREDICATE", 18);
    append_u32(output, program.ir_version);
    append_u32(output, program.abi_version);
    append_u32(output, static_cast<uint32_t>(program.plan_tag));
    append_u64(output, program.catalog_generation);
    append_u32(output, program.helper_version);
    append_u32(output, program.cpu_feature_tier);
    append_string(output, program.architecture);
    append_layout(output, program.tuple0);
    output.push_back(program.tuple1.has_value() ? 1 : 0);
    if (program.tuple1.has_value()) {
        append_layout(output, *program.tuple1);
    }
    append_u32(output, static_cast<uint32_t>(program.parameters.size()));
    for (const auto& parameter : program.parameters) {
        append_u32(output, static_cast<uint32_t>(parameter.type));
        append_u32(output, parameter.max_len);
    }
    append_u32(output, static_cast<uint32_t>(program.predicates.size()));
    for (const auto& predicate : program.predicates) {
        append_u32(output, static_cast<uint32_t>(predicate.op));
        append_operand(output, predicate.lhs);
        append_operand(output, predicate.rhs);
    }
    return output;
}

bool is_numeric(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT;
}

bool can_compare(ColType lhs, ColType rhs) {
    return lhs == rhs || (is_numeric(lhs) && is_numeric(rhs)) ||
           ((lhs == TYPE_STRING || lhs == TYPE_DATETIME) && (rhs == TYPE_STRING || rhs == TYPE_DATETIME));
}

bool valid_fixed_length(ColType type, uint32_t len) {
    return (type == TYPE_INT && len == sizeof(int32_t)) || (type == TYPE_FLOAT && len == sizeof(double)) ||
           ((type == TYPE_STRING || type == TYPE_DATETIME) && len > 0);
}

const ColMeta* find_column(const JitTupleLayout& layout, const TabCol& target) {
    auto found = std::find_if(layout.columns.begin(), layout.columns.end(), [&](const ColMeta& column) {
        return column.tab_name == target.tab_name && column.name == target.col_name;
    });
    return found == layout.columns.end() ? nullptr : &*found;
}

bool column_fits(const ColMeta& column, const JitTupleLayout& layout) {
    return column.offset >= 0 && column.len >= 0 &&
           static_cast<uint64_t>(column.offset) + column.len <= layout.tuple_len;
}

std::optional<JitOperand> make_column_operand(const TabCol& column, const JitTupleLayout& tuple0,
                                              const std::optional<JitTupleLayout>& tuple1, std::string* error) {
    if (const ColMeta* meta = find_column(tuple0, column); meta != nullptr) {
        if (!column_fits(*meta, tuple0)) {
            *error = "column layout is outside tuple0";
            return std::nullopt;
        }
        return JitOperand{JitOperandSource::TUPLE0, meta->type, static_cast<uint32_t>(meta->offset),
                          static_cast<uint32_t>(meta->len), 0};
    }
    if (tuple1.has_value()) {
        if (const ColMeta* meta = find_column(*tuple1, column); meta != nullptr) {
            if (!column_fits(*meta, *tuple1)) {
                *error = "column layout is outside tuple1";
                return std::nullopt;
            }
            return JitOperand{JitOperandSource::TUPLE1, meta->type, static_cast<uint32_t>(meta->offset),
                              static_cast<uint32_t>(meta->len), 0};
        }
    }
    *error = "condition refers to an unknown column";
    return std::nullopt;
}

bool compare_result(CompOp op, int comparison) {
    switch (op) {
    case OP_EQ:
        return comparison == 0;
    case OP_NE:
        return comparison != 0;
    case OP_LT:
        return comparison < 0;
    case OP_GT:
        return comparison > 0;
    case OP_LE:
        return comparison <= 0;
    case OP_GE:
        return comparison >= 0;
    }
    return false;
}

const char* tuple_data(const JitCallFrame& frame, JitOperandSource source) {
    return source == JitOperandSource::TUPLE0 ? frame.tuple0 : frame.tuple1;
}

uint32_t tuple_length(const JitCallFrame& frame, JitOperandSource source) {
    return source == JitOperandSource::TUPLE0 ? frame.tuple0_len : frame.tuple1_len;
}

JitStatus read_numeric(const JitOperand& operand, const JitCallFrame& frame, double* value) {
    if (operand.source == JitOperandSource::PARAMETER) {
        if (operand.parameter_index >= frame.param_count) {
            return JitStatus::INVALID_INPUT;
        }
        const JitValue& parameter = frame.params[operand.parameter_index];
        if (parameter.type != operand.type) {
            return JitStatus::INVALID_INPUT;
        }
        *value = operand.type == TYPE_INT ? static_cast<double>(parameter.int_value) : parameter.float_value;
        return JitStatus::OK;
    }
    const char* tuple = tuple_data(frame, operand.source);
    if (tuple == nullptr || static_cast<uint64_t>(operand.offset) + operand.len > tuple_length(frame, operand.source)) {
        return JitStatus::INVALID_INPUT;
    }
    if (operand.type == TYPE_INT) {
        int32_t integer = 0;
        std::memcpy(&integer, tuple + operand.offset, sizeof(integer));
        *value = static_cast<double>(integer);
    } else {
        std::memcpy(value, tuple + operand.offset, sizeof(*value));
    }
    return JitStatus::OK;
}

JitStatus read_bytes(const JitOperand& operand, const JitCallFrame& frame, const char** data, uint32_t* len) {
    if (operand.source == JitOperandSource::PARAMETER) {
        if (operand.parameter_index >= frame.param_count) {
            return JitStatus::INVALID_INPUT;
        }
        const JitValue& parameter = frame.params[operand.parameter_index];
        if (parameter.type != operand.type || (parameter.bytes == nullptr && parameter.bytes_len != 0)) {
            return JitStatus::INVALID_INPUT;
        }
        *data = parameter.bytes;
        *len = parameter.bytes_len;
        return JitStatus::OK;
    }
    const char* tuple = tuple_data(frame, operand.source);
    if (tuple == nullptr || static_cast<uint64_t>(operand.offset) + operand.len > tuple_length(frame, operand.source)) {
        return JitStatus::INVALID_INPUT;
    }
    *data = tuple + operand.offset;
    const void* terminator = std::memchr(*data, '\0', operand.len);
    *len = terminator == nullptr ? operand.len : static_cast<uint32_t>(static_cast<const char*>(terminator) - *data);
    return JitStatus::OK;
}

} // namespace

JitBuildResult build_predicate_program(PlanTag plan_tag, const std::vector<Condition>& conditions,
                                       JitTupleLayout tuple0, std::optional<JitTupleLayout> tuple1,
                                       uint64_t catalog_generation) {
    if (plan_tag != T_SeqScan && plan_tag != T_IndexScan && plan_tag != T_IndexSkipScan && plan_tag != T_Filter &&
        plan_tag != T_NestLoop) {
        return {{}, {}, "predicate IR requires a scan, filter, or join plan"};
    }
    JitProgram program;
    program.plan_tag = plan_tag;
    program.catalog_generation = catalog_generation;
    program.architecture = host_architecture();
    program.tuple0 = std::move(tuple0);
    program.tuple1 = std::move(tuple1);
    program.predicates.reserve(conditions.size());
    program.parameters.reserve(conditions.size());

    for (const auto& condition : conditions) {
        std::string error;
        auto lhs = make_column_operand(condition.lhs_col, program.tuple0, program.tuple1, &error);
        if (!lhs.has_value()) {
            return {{}, {}, std::move(error)};
        }
        JitOperand rhs;
        if (condition.is_rhs_val) {
            const uint32_t index = static_cast<uint32_t>(program.parameters.size());
            const uint32_t max_len =
                condition.rhs_val.type == TYPE_STRING || condition.rhs_val.type == TYPE_DATETIME ? lhs->len : 0;
            program.parameters.push_back({condition.rhs_val.type, max_len});
            rhs = {JitOperandSource::PARAMETER, condition.rhs_val.type, 0, max_len, index};
        } else {
            auto operand = make_column_operand(condition.rhs_col, program.tuple0, program.tuple1, &error);
            if (!operand.has_value()) {
                return {{}, {}, std::move(error)};
            }
            rhs = *operand;
        }
        if (!can_compare(lhs->type, rhs.type)) {
            return {{}, {}, "condition has incompatible operand types"};
        }
        program.predicates.push_back({condition.op, *lhs, rhs});
    }
    program.key.canonical_bytes = serialize_program(program);
    program.key.digest = stable_digest(program.key.canonical_bytes);
    JitVerifyResult verification = verify_program(program);
    if (!verification) {
        return {{}, {}, std::move(verification.error)};
    }
    JitBindResult binding = bind_parameters(program, conditions);
    if (!binding) {
        return {{}, {}, std::move(binding.error)};
    }
    JitBuildResult result;
    result.params = std::move(*binding.params);
    result.program = std::move(program);
    return result;
}

JitBuildResult build_predicate_program(const ScanPlan& plan, uint64_t catalog_generation) {
    return build_predicate_program(plan.tag, plan.conds_, {static_cast<uint32_t>(plan.len_), plan.cols_}, std::nullopt,
                                   catalog_generation);
}

JitBuildResult build_predicate_program(const FilterPlan& plan, JitTupleLayout input, uint64_t catalog_generation) {
    return build_predicate_program(plan.tag, plan.conds_, std::move(input), std::nullopt, catalog_generation);
}

JitBuildResult build_predicate_program(const JoinPlan& plan, JitTupleLayout left, JitTupleLayout right,
                                       uint64_t catalog_generation) {
    return build_predicate_program(plan.tag, plan.conds_, std::move(left), std::move(right), catalog_generation);
}

JitVerifyResult verify_program(const JitProgram& program) {
    if (program.ir_version != JIT_IR_VERSION || program.abi_version != JIT_ABI_VERSION || program.helper_version != 1 ||
        program.architecture.empty()) {
        return {false, "unsupported JIT IR or ABI version"};
    }
    if (program.tuple0.tuple_len > kMaxTupleLength ||
        (program.tuple1.has_value() && program.tuple1->tuple_len > kMaxTupleLength) ||
        program.predicates.size() > kMaxPredicates || program.parameters.size() > kMaxParameters) {
        return {false, "JIT program exceeds resource limits"};
    }
    auto verify_operand = [&](const JitOperand& operand) -> JitVerifyResult {
        if (operand.source == JitOperandSource::PARAMETER) {
            if (operand.parameter_index >= program.parameters.size() ||
                program.parameters[operand.parameter_index].type != operand.type) {
                return {false, "predicate references an invalid parameter"};
            }
            if ((operand.type == TYPE_STRING || operand.type == TYPE_DATETIME) &&
                operand.len > program.parameters[operand.parameter_index].max_len) {
                return {false, "byte parameter length exceeds its descriptor"};
            }
            return {true, {}};
        }
        const JitTupleLayout* layout = operand.source == JitOperandSource::TUPLE0 ? &program.tuple0 : nullptr;
        if (operand.source == JitOperandSource::TUPLE1) {
            layout = program.tuple1.has_value() ? &*program.tuple1 : nullptr;
        }
        if (layout == nullptr || !valid_fixed_length(operand.type, operand.len) ||
            static_cast<uint64_t>(operand.offset) + operand.len > layout->tuple_len) {
            return {false, "predicate references bytes outside a tuple layout"};
        }
        return {true, {}};
    };
    for (const auto& predicate : program.predicates) {
        JitVerifyResult lhs = verify_operand(predicate.lhs);
        JitVerifyResult rhs = verify_operand(predicate.rhs);
        if (!lhs) {
            return lhs;
        }
        if (!rhs) {
            return rhs;
        }
        if (!can_compare(predicate.lhs.type, predicate.rhs.type)) {
            return {false, "predicate has incompatible operand types"};
        }
    }
    const std::string bytes = serialize_program(program);
    if (program.key.canonical_bytes != bytes || program.key.digest != stable_digest(bytes)) {
        return {false, "JIT program key does not match its canonical encoding"};
    }
    return {true, {}};
}

JitBindResult bind_parameters(const JitProgram& program, const std::vector<Condition>& conditions) {
    JitParamBlock block;
    block.values.reserve(program.parameters.size());
    block.owned_bytes.reserve(program.parameters.size());
    for (const auto& condition : conditions) {
        if (!condition.is_rhs_val) {
            continue;
        }
        const Value& value = condition.rhs_val;
        const size_t parameter_index = block.values.size();
        if (parameter_index >= program.parameters.size() || value.type != program.parameters[parameter_index].type) {
            return {{}, "parameter type does not match the JIT program"};
        }
        JitValue bound;
        bound.type = value.type;
        if (value.type == TYPE_INT) {
            bound.int_value = value.int_val;
        } else if (value.type == TYPE_FLOAT) {
            bound.float_value = value.float_val;
        } else {
            if (value.str_val.size() > program.parameters[parameter_index].max_len) {
                return {{}, "string parameter exceeds the JIT program length"};
            }
            block.owned_bytes.push_back(value.str_val);
            bound.bytes = block.owned_bytes.back().data();
            bound.bytes_len = static_cast<uint32_t>(block.owned_bytes.back().size());
        }
        block.values.push_back(bound);
    }
    if (block.values.size() != program.parameters.size()) {
        return {{}, "parameter count does not match the JIT program"};
    }
    return {std::move(block), {}};
}

JitStatus interpret_predicate(const JitProgram& program, JitCallFrame* frame) {
    if (frame == nullptr || !verify_program(program)) {
        return JitStatus::INVALID_INPUT;
    }
    if (frame->param_count != program.parameters.size() || (frame->param_count != 0 && frame->params == nullptr) ||
        frame->tuple0_len < program.tuple0.tuple_len ||
        (program.tuple1.has_value() && (frame->tuple1 == nullptr || frame->tuple1_len < program.tuple1->tuple_len))) {
        return JitStatus::INVALID_INPUT;
    }
    frame->match = false;
    for (const auto& predicate : program.predicates) {
        bool predicate_match = false;
        if (is_numeric(predicate.lhs.type) && is_numeric(predicate.rhs.type)) {
            double lhs = 0;
            double rhs = 0;
            JitStatus status = read_numeric(predicate.lhs, *frame, &lhs);
            if (status != JitStatus::OK) {
                return status;
            }
            status = read_numeric(predicate.rhs, *frame, &rhs);
            if (status != JitStatus::OK) {
                return status;
            }
            if (std::isnan(lhs) || std::isnan(rhs)) {
                predicate_match = predicate.op == OP_NE;
            } else {
                const int comparison = lhs < rhs ? -1 : lhs > rhs ? 1 : 0;
                predicate_match = compare_result(predicate.op, comparison);
            }
        } else {
            const char* lhs = nullptr;
            const char* rhs = nullptr;
            uint32_t lhs_len = 0;
            uint32_t rhs_len = 0;
            JitStatus status = read_bytes(predicate.lhs, *frame, &lhs, &lhs_len);
            if (status != JitStatus::OK) {
                return status;
            }
            status = read_bytes(predicate.rhs, *frame, &rhs, &rhs_len);
            if (status != JitStatus::OK) {
                return status;
            }
            const uint32_t common = std::min(lhs_len, rhs_len);
            int comparison = common == 0 ? 0 : std::memcmp(lhs, rhs, common);
            if (comparison == 0) {
                comparison = lhs_len < rhs_len ? -1 : lhs_len > rhs_len ? 1 : 0;
            }
            predicate_match = compare_result(predicate.op, comparison);
        }
        if (!predicate_match) {
            return JitStatus::OK;
        }
    }
    frame->match = true;
    return JitStatus::OK;
}

} // namespace jit
