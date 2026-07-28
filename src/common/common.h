/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "defs.h"
#include "errors.h"
#include "record/rm_defs.h"

struct TabCol {
    std::string tab_name;
    std::string col_name;

    friend bool operator<(const TabCol& x, const TabCol& y) {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }
};

struct Value {
    ColType type = TYPE_INT; // type of value
    // 0 means a literal. Prepared-plan inspection retains the original 1-based
    // placeholder ordinal without turning it into a runtime value.
    std::size_t parameter_ordinal = 0;
    union {
        int int_val = 0; // int value
        float float_val; // SQL FLOAT value
    };
    std::string str_val; // string value

    // SQL NULL 与列类型无关：is_null 为真时 type 只表示"被赋给哪种列"，
    // int_val/float_val/str_val 均无意义，原始字节按全零处理。
    bool is_null = false;

    std::shared_ptr<RmRecord> raw; // raw record buffer

    void set_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void set_null() {
        is_null = true;
    }

    void init_raw(int len) {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        if (is_null) {
            memset(raw->data, 0, len);
        } else if (type == TYPE_INT) {
            assert(len == sizeof(int));
            write_unaligned(raw->data, int_val);
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(float));
            write_float(raw->data, float_val);
        } else if (type == TYPE_STRING || type == TYPE_DATETIME) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        }
    }
};

enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };

enum class AggType { COUNT, MAX, MIN, SUM, AVG };

enum class QueryExprType { COLUMN, AGGREGATE, VALUE };

inline CompOp swap_comp_op(CompOp op) {
    switch (op) {
    case OP_EQ:
    case OP_NE:
        return op;
    case OP_LT:
        return OP_GT;
    case OP_GT:
        return OP_LT;
    case OP_LE:
        return OP_GE;
    case OP_GE:
        return OP_LE;
    }
    throw InternalError("Unexpected comparison operator");
}

struct AggExpr {
    AggType type = AggType::COUNT;
    bool is_star = false;
    bool is_distinct = false;
    TabCol col;
    std::string display_name;
};

struct QueryExpr {
    QueryExprType type = QueryExprType::COLUMN;
    TabCol col;
    AggExpr agg;
    Value value;
    std::string display_name;
};

struct SelectItem {
    QueryExpr expr;
    std::string alias;
    std::string output_name;
};

struct OrderByItem {
    QueryExpr expr;
    bool is_desc = false;
    std::string order_name;
};

struct HavingCondition {
    QueryExpr lhs;
    CompOp op = OP_EQ;
    bool is_rhs_val = false;
    QueryExpr rhs_expr;
    Value rhs_val;
};

/* `IS NULL` / `IS NOT NULL`：不读数据字节，只看 lhs 列的 NULL 位 */
enum class NullTest { NONE, IS_NULL, IS_NOT_NULL };

struct Condition {
    TabCol lhs_col;  // left-hand side column
    CompOp op;       // comparison operator
    bool is_rhs_val; // true if right-hand side is a value (not a column)
    TabCol rhs_col;  // right-hand side column
    Value rhs_val;   // right-hand side value
    std::string rhs_display;
    NullTest null_test = NullTest::NONE;
};

/**
 * @brief 条件的右值是否是可用于索引键 / 点查的普通字面量。
 *
 * NULL 不进索引键，所以 `IS [NOT] NULL` 和 `col = NULL` 都必须留给执行器按
 * 三值逻辑过滤，不能参与索引前缀匹配或点查键的构造。
 */
inline bool is_indexable_value_condition(const Condition& cond) {
    return cond.is_rhs_val && cond.null_test == NullTest::NONE && !cond.rhs_val.is_null;
}

/* eval_condition_nulls 的结果：条件已被 NULL 语义定死，或需要按原有二值语义
   继续比较数据字节。 */
enum class NullEval { DECIDED_TRUE, DECIDED_FALSE, COMPARE };

/**
 * @brief 比较前的 NULL 短路，四个条件求值器（执行器 compare、row_mutation、
 *        以及 SSI 的两处）共用同一份三值逻辑。
 *
 * 语义：`IS [NOT] NULL` 直接由 lhs 的 NULL 位定值；否则任一操作数为 NULL 时
 * 整个比较为假——包括 `<>`，SQL 里 `NULL <> x` 是 UNKNOWN 而不是 TRUE。
 *
 * 各 null 地址由调用方从自己缓存的列地址里取（见 defs.h 的布局说明），
 * null_byte < 0 的列（旧格式文件）会被 is_null_at 直接短路掉。
 */
inline NullEval eval_condition_nulls(const Condition& cond, const char* tuple, int lhs_null_byte, uint8_t lhs_null_mask,
                                     int rhs_null_byte, uint8_t rhs_null_mask) {
    const bool lhs_null = is_null_at(tuple, lhs_null_byte, lhs_null_mask);
    if (cond.null_test != NullTest::NONE) {
        return lhs_null == (cond.null_test == NullTest::IS_NULL) ? NullEval::DECIDED_TRUE : NullEval::DECIDED_FALSE;
    }
    if (lhs_null || (cond.is_rhs_val && cond.rhs_val.is_null)) {
        return NullEval::DECIDED_FALSE;
    }
    if (!cond.is_rhs_val && is_null_at(tuple, rhs_null_byte, rhs_null_mask)) {
        return NullEval::DECIDED_FALSE;
    }
    return NullEval::COMPARE;
}

enum class UpdateOp { SELF_ADD, SELF_SUB, SELF_MUL, SELF_DIV, ASSIGNMENT };

struct UpdateTerm {
    Value rhs;
    UpdateOp op = UpdateOp::SELF_ADD;
};

struct SetClause {
    TabCol lhs;
    Value rhs;
    bool is_self_ref = false;
    TabCol rhs_col;
    UpdateOp op = UpdateOp::ASSIGNMENT;
    // Ordered scalar +/- terms after the legacy first operation. Keeping the
    // first term in the existing fields preserves prepared/cache compatibility
    // for the ranking-critical single-operation UPDATE shape.
    std::vector<UpdateTerm> additional_terms;
};
