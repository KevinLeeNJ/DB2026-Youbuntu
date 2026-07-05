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

#include <cstring>
#include <string>

#include "common/common.h"
#include "common/type_utils.h"
#include "errors.h"
#include "execution_defs.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

namespace rmdb::exec {

/// 表达式求值器。
/// 当前仅提取条件比较的 type dispatch / op-switch 逻辑，
/// 消除 AbstractExecutor::compare 中的类型分支重复。
/// get_col_offset 虚回调仍保留在 executor 中（ledger 登记后续收敛）。
class ExpressionEvaluator {
public:
    /// 比较一条记录是否满足指定条件。
    /// lhs_col_meta / rhs_col_meta 由调用方通过 schema 解析后传入。
    static bool eval_condition(const Condition& cond, const RmRecord& rec, const ColMeta& lhs_col_meta,
                               const ColMeta* rhs_col_meta) {
        const char* lhs_data = rec.data + lhs_col_meta.offset;
        ColType lhs_type = lhs_col_meta.type;
        ColType rhs_type;
        const char* rhs_data = nullptr;
        if (!cond.is_rhs_val) {
            rhs_data = rec.data + rhs_col_meta->offset;
            rhs_type = rhs_col_meta->type;
        } else {
            rhs_type = cond.rhs_val.type;
        }
        if (!can_cast(lhs_type, rhs_type)) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
        return compare_typed(cond.op, lhs_data, lhs_type, lhs_col_meta.len, rhs_data, rhs_type,
                             rhs_col_meta ? rhs_col_meta->len : 0, cond.rhs_val, cond.is_rhs_val);
    }

private:
    static bool compare_typed(CompOp op, const char* lhs_data, ColType lhs_type, int lhs_len, const char* rhs_data,
                              ColType rhs_type, int rhs_len, const Value& rhs_val, bool is_rhs_val) {
        switch (lhs_type) {
        case TYPE_INT:
        case TYPE_FLOAT: {
            float lhs_val = lhs_type == TYPE_INT ? static_cast<float>(*reinterpret_cast<const int*>(lhs_data))
                                                 : *reinterpret_cast<const float*>(lhs_data);
            float rhs_val_f;
            if (is_rhs_val) {
                rhs_val_f = rhs_type == TYPE_INT ? static_cast<float>(rhs_val.int_val) : rhs_val.float_val;
            } else {
                rhs_val_f = rhs_type == TYPE_INT ? static_cast<float>(*reinterpret_cast<const int*>(rhs_data))
                                                 : *reinterpret_cast<const float*>(rhs_data);
            }
            return apply_op(op, lhs_val, rhs_val_f);
        }
        case TYPE_STRING:
        case TYPE_DATETIME: {
            std::string lhs_str(lhs_data, strnlen(lhs_data, lhs_len));
            std::string rhs_str = is_rhs_val ? rhs_val.str_val : std::string(rhs_data, strnlen(rhs_data, rhs_len));
            return apply_op(op, lhs_str, rhs_str);
        }
        }
        return false;
    }

    template <typename T> static bool apply_op(CompOp op, const T& lhs, const T& rhs) {
        switch (op) {
        case OP_EQ:
            return lhs == rhs;
        case OP_NE:
            return lhs != rhs;
        case OP_LT:
            return lhs < rhs;
        case OP_GT:
            return lhs > rhs;
        case OP_LE:
            return lhs <= rhs;
        case OP_GE:
            return lhs >= rhs;
        }
        return false;
    }
};

} // namespace rmdb::exec
