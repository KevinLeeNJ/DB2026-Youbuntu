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
    ColType type; // type of value
    bool is_null = false;
    union {
        int int_val;      // int value
        double float_val; // SQL FLOAT value
    };
    std::string str_val; // string value

    std::shared_ptr<RmRecord> raw; // raw record buffer

    void set_int(int int_val_) {
        type = TYPE_INT;
        is_null = false;
        int_val = int_val_;
    }

    void set_float(double float_val_) {
        type = TYPE_FLOAT;
        is_null = false;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        is_null = false;
        str_val = std::move(str_val_);
    }

    void set_null() {
        type = TYPE_INT;
        is_null = true;
        int_val = 0;
    }

    void init_raw(int len) {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        if (is_null) {
            memset(raw->data, 0, len);
        } else if (type == TYPE_INT) {
            assert(len == sizeof(int));
            *(int*)(raw->data) = int_val;
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(double));
            *(double*)(raw->data) = float_val;
        } else if (type == TYPE_STRING || type == TYPE_DATETIME) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        }
    }
};

enum CompOp {
    OP_EQ,
    OP_NE,
    OP_LT,
    OP_GT,
    OP_LE,
    OP_GE,
    OP_LIKE,
    OP_IN,
    OP_BETWEEN,
    OP_IS_NULL,
    OP_IS_NOT_NULL,
    OP_EXISTS
};

enum class AggType { COUNT, MAX, MIN, SUM, AVG };

enum class WindowFuncType { ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM, AVG };

enum class QueryExprType { COLUMN, AGGREGATE, VALUE, ARITHMETIC, LOGICAL, CASE_EXPR, PREDICATE, SUBQUERY, WINDOW };

enum class QueryArithmeticOp { ADD, SUB, MUL, DIV };
enum class QueryLogicalOp { AND, OR, NOT };
enum class QueryQuantifier { NONE, ANY, ALL };
enum class QuerySetOperator { UNION, INTERSECT, EXCEPT };

class Query;
class Plan;

inline bool is_swappable_comp_op(CompOp op) {
    return op == OP_EQ || op == OP_NE || op == OP_LT || op == OP_GT || op == OP_LE || op == OP_GE;
}

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
    case OP_LIKE:
    case OP_IN:
    case OP_BETWEEN:
        throw InternalError("Cannot swap non-comparison predicate");
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
    WindowFuncType window_func = WindowFuncType::ROW_NUMBER;
    std::vector<std::shared_ptr<QueryExpr>> window_args;
    std::vector<std::shared_ptr<QueryExpr>> window_partition_by;
    std::vector<std::shared_ptr<QueryExpr>> window_order_by;
    std::vector<bool> window_order_desc;
    std::vector<int> window_nulls_order;
    std::string window_result_name;
    QueryArithmeticOp arithmetic_op = QueryArithmeticOp::ADD;
    QueryLogicalOp logical_op = QueryLogicalOp::AND;
    CompOp predicate_op = OP_EQ;
    QueryQuantifier quantifier = QueryQuantifier::NONE;
    bool negated = false;
    std::shared_ptr<QueryExpr> lhs;
    std::shared_ptr<QueryExpr> rhs;
    std::shared_ptr<QueryExpr> rhs_upper;
    std::vector<std::shared_ptr<QueryExpr>> operands;
    std::vector<std::pair<std::shared_ptr<QueryExpr>, std::shared_ptr<QueryExpr>>> case_when;
    std::shared_ptr<QueryExpr> else_expr;
    std::vector<std::shared_ptr<QueryExpr>> rhs_values;
    std::shared_ptr<Query> subquery;
    std::shared_ptr<Plan> subquery_plan;
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
    int nulls_order = 0;
};

struct HavingCondition {
    QueryExpr lhs;
    CompOp op = OP_EQ;
    bool is_rhs_val = false;
    QueryExpr rhs_expr;
    Value rhs_val;
    Value rhs_upper;
    std::vector<Value> rhs_vals;
    bool has_rhs_upper = false;
    bool negated = false;
};

struct Condition {
    TabCol lhs_col;  // left-hand side column
    CompOp op;       // comparison operator
    bool is_rhs_val; // true if right-hand side is a value (not a column)
    TabCol rhs_col;  // right-hand side column
    Value rhs_val;   // right-hand side value
    std::string rhs_display;
    Value rhs_upper;
    std::vector<Value> rhs_vals;
    bool has_rhs_upper = false;
    bool negated = false;
    bool is_join_on = false;
};

enum class UpdateOp { SELF_ADD, SELF_SUB, SELF_MUL, SELF_DIV, ASSIGNMENT };

struct SetClause {
    TabCol lhs;
    Value rhs;
    bool is_self_ref = false;
    TabCol rhs_col;
    UpdateOp op = UpdateOp::ASSIGNMENT;
    std::shared_ptr<QueryExpr> rhs_expr;
};
