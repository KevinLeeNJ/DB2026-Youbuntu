/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
*/

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/common.h"

enum class PointProgramKind { Select, Update, Delete, Insert };

// A compiled condition deliberately contains no Value. Literal values are
// supplied by the current Query/DMLPlan when a program is used.
struct CompiledCondition {
    TabCol lhs_col;
    CompOp op = OP_EQ;
    bool rhs_is_value = false;
    TabCol rhs_col;
    ColType rhs_type = TYPE_INT;
};

struct CompiledSetOp {
    TabCol lhs_col;
    bool is_self_ref = false;
    TabCol rhs_col;
    UpdateOp op = UpdateOp::ASSIGNMENT;
    ColType rhs_type = TYPE_INT;
};

// Immutable, schema-generation-scoped metadata for a point DML program.
// Runtime Context/Transaction, RID, table/index handles, and page pointers
// must never be stored here.
struct CompiledPointProgram {
    PointProgramKind kind = PointProgramKind::Update;
    std::uint64_t catalog_generation = 0;
    std::string table_name;
    std::vector<std::string> index_col_names;
    std::vector<size_t> key_condition_positions;
    std::vector<CompiledCondition> conditions;
    std::vector<CompiledSetOp> set_ops;
};

using CompiledPointProgramPtr = std::shared_ptr<const CompiledPointProgram>;
