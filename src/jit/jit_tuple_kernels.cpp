/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "jit/jit_tuple_kernels.h"

#include <algorithm>
#include <cstring>

#include "common/config.h"
#include "execution/execution_common.h"

namespace jit {
namespace {

const ColMeta* find_column(const TabMeta& table, const TabCol& column) {
    auto found = std::find_if(table.cols.begin(), table.cols.end(), [&](const ColMeta& meta) {
        return meta.tab_name == column.tab_name && meta.name == column.col_name;
    });
    return found == table.cols.end() ? nullptr : &*found;
}

bool is_numeric(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT;
}

bool can_cast(ColType target, ColType source) {
    return target == source || (target == TYPE_INT && source == TYPE_FLOAT) ||
           (target == TYPE_FLOAT && source == TYPE_INT) ||
           ((target == TYPE_STRING && source == TYPE_DATETIME) || (target == TYPE_DATETIME && source == TYPE_STRING));
}

} // namespace

bool tuple_jit_enabled() {
    return rmdb_config::jit_mode != rmdb_config::JitMode::OFF;
}

ProjectionKernel::ProjectionKernel(const std::vector<ColMeta>& output_columns,
                                   const std::vector<size_t>& selected_columns,
                                   const std::vector<ColMeta>& input_columns) {
    if (output_columns.size() != selected_columns.size()) {
        return;
    }
    for (size_t index = 0; index < output_columns.size(); ++index) {
        if (selected_columns[index] >= input_columns.size() || output_columns[index].len < 0 ||
            input_columns[selected_columns[index]].len < 0) {
            return;
        }
        const CopySpan next{static_cast<size_t>(input_columns[selected_columns[index]].offset),
                            static_cast<size_t>(output_columns[index].offset),
                            static_cast<size_t>(output_columns[index].len)};
        if (!spans_.empty()) {
            CopySpan& previous = spans_.back();
            if (previous.source_offset + previous.length == next.source_offset &&
                previous.destination_offset + previous.length == next.destination_offset) {
                previous.length += next.length;
                continue;
            }
        }
        spans_.push_back(next);
    }
    valid_ = true;
}

void ProjectionKernel::project(const char* input, char* output) const {
    for (const CopySpan& span : spans_) {
        std::memcpy(output + span.destination_offset, input + span.source_offset, span.length);
    }
}

UpdateKernel::UpdateKernel(const TabMeta& table, const std::vector<SetClause>& clauses) {
    assignments_.reserve(clauses.size());
    for (const SetClause& clause : clauses) {
        const ColMeta* target = find_column(table, clause.lhs);
        if (target == nullptr) {
            return;
        }
        ColMeta source{};
        if (clause.is_self_ref) {
            const ColMeta* found = find_column(table, clause.rhs_col);
            if (found == nullptr) {
                return;
            }
            source = *found;
        }
        assignments_.push_back({*target, source, clause});
    }
    valid_ = true;
}

JitStatus UpdateKernel::update(char* destination, const char* source) const {
    if (!valid_ || destination == nullptr || source == nullptr) {
        return JitStatus::INVALID_INPUT;
    }
    for (const Assignment& assignment : assignments_) {
        if (assignment.clause.is_self_ref && assignment.clause.op == UpdateOp::SELF_DIV &&
            is_numeric(assignment.source.type) && is_numeric(assignment.clause.rhs.type)) {
            const double delta = assignment.clause.rhs.type == TYPE_INT
                                     ? static_cast<double>(assignment.clause.rhs.int_val)
                                     : assignment.clause.rhs.float_val;
            if (delta == 0.0) {
                return JitStatus::DIVISION_BY_ZERO;
            }
        }
    }
    for (const Assignment& assignment : assignments_) {
        const SetClause& clause = assignment.clause;
        char* data = destination + assignment.target.offset;
        if (clause.is_self_ref) {
            const ColMeta& rhs = assignment.source;
            if (clause.op == UpdateOp::ASSIGNMENT) {
                if (assignment.target.type == TYPE_INT && rhs.type == TYPE_FLOAT) {
                    write_unaligned(data, static_cast<int>(read_unaligned<double>(source + rhs.offset)));
                } else if (assignment.target.type == TYPE_FLOAT && rhs.type == TYPE_INT) {
                    write_unaligned(data, static_cast<double>(read_unaligned<int>(source + rhs.offset)));
                } else if (assignment.target.type == TYPE_STRING || assignment.target.type == TYPE_DATETIME) {
                    if (rhs.type != TYPE_STRING && rhs.type != TYPE_DATETIME) {
                        return JitStatus::INVALID_INPUT;
                    }
                    std::memset(data, 0, assignment.target.len);
                    std::memcpy(data, source + rhs.offset, std::min(assignment.target.len, rhs.len));
                } else if (assignment.target.type == TYPE_INT && rhs.type == TYPE_INT) {
                    write_unaligned(data, read_unaligned<int>(source + rhs.offset));
                } else if (assignment.target.type == TYPE_FLOAT && rhs.type == TYPE_FLOAT) {
                    write_unaligned(data, read_unaligned<double>(source + rhs.offset));
                } else {
                    return JitStatus::INVALID_INPUT;
                }
                continue;
            }
            if (!is_numeric(rhs.type) || !is_numeric(clause.rhs.type) || !is_numeric(assignment.target.type)) {
                return JitStatus::INVALID_INPUT;
            }
            const double base = rhs.type == TYPE_INT ? static_cast<double>(read_unaligned<int>(source + rhs.offset))
                                                     : read_unaligned<double>(source + rhs.offset);
            const double delta =
                clause.rhs.type == TYPE_INT ? static_cast<double>(clause.rhs.int_val) : clause.rhs.float_val;
            double result = base;
            switch (clause.op) {
            case UpdateOp::SELF_ADD:
                result += delta;
                break;
            case UpdateOp::SELF_SUB:
                result -= delta;
                break;
            case UpdateOp::SELF_MUL:
                result *= delta;
                break;
            case UpdateOp::SELF_DIV:
                result /= delta;
                break;
            case UpdateOp::ASSIGNMENT:
                break;
            }
            if (assignment.target.type == TYPE_INT) {
                write_unaligned(data, static_cast<int>(result));
            } else {
                write_unaligned(data, result);
            }
            continue;
        }
        if (!can_cast(assignment.target.type, clause.rhs.type)) {
            return JitStatus::INVALID_INPUT;
        }
        switch (assignment.target.type) {
        case TYPE_INT:
            write_unaligned(data,
                            clause.rhs.type == TYPE_INT ? clause.rhs.int_val : static_cast<int>(clause.rhs.float_val));
            break;
        case TYPE_FLOAT:
            write_unaligned(data, clause.rhs.type == TYPE_FLOAT ? clause.rhs.float_val
                                                                : static_cast<double>(clause.rhs.int_val));
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            std::memset(data, 0, assignment.target.len);
            std::memcpy(data, clause.rhs.str_val.c_str(),
                        std::min(assignment.target.len, (int)clause.rhs.str_val.size()));
            break;
        }
    }
    return JitStatus::OK;
}

} // namespace jit
