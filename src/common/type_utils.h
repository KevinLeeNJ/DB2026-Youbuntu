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

#include "defs.h"

namespace rmdb::common {

/// 类型转换矩阵的唯一来源（Phase 6 收口）。
/// 规则：同类型可转；INT<->FLOAT 可转；STRING<->DATETIME 可转；其余不可转。
inline bool can_cast(ColType lhs_type, ColType rhs_type) {
    if (lhs_type == rhs_type) {
        return true;
    }
    if ((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) || (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT)) {
        return true;
    }
    if ((lhs_type == TYPE_STRING && rhs_type == TYPE_DATETIME) ||
        (lhs_type == TYPE_DATETIME && rhs_type == TYPE_STRING)) {
        return true;
    }
    return false;
}

/// 数值类型判断（INT/FLOAT）。
inline bool is_numeric_type(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT;
}

/// 可分组类型判断（数值与大字符串；DATETIME 也可分组）。
inline bool is_groupable_type(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT || type == TYPE_STRING || type == TYPE_DATETIME;
}

} // namespace rmdb::common

namespace rmdb {
using common::can_cast;
using common::is_groupable_type;
using common::is_numeric_type;
} // namespace rmdb
