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

#include "execution_defs.h"
#include "execution_scalar.h"
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
public:
    Rid _abstract_rid;

    Context* context_ = nullptr;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const {
        return 0;
    };

    virtual const std::vector<ColMeta>& cols() const {
        static const std::vector<ColMeta> no_cols;
        return no_cols;
    };

    virtual std::string getType() {
        return "AbstractExecutor";
    };

    virtual void beginTuple() {};

    virtual void nextTuple() {};

    virtual bool is_end() const {
        return true;
    };

    virtual Rid& rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    // NULL values produced by an outer join are transient execution metadata;
    // ordinary table records have an empty bitmap, which means all columns are
    // non-NULL.
    virtual const std::vector<bool>& nulls() const {
        static const std::vector<bool> no_nulls;
        return no_nulls;
    }

    virtual ColMeta get_col_offset(const TabCol& target) {
        (void)target;
        return ColMeta();
    };

    virtual void set_counting_enabled(bool enabled) {
        (void)enabled;
    }

    virtual void set_key_conditions(std::vector<Condition> /*key_conds*/) {
        // no-op default; only IndexScanExecutor overrides
    }

    virtual std::string scan_table_name() const {
        return "";
    }

    virtual std::vector<Condition> scan_conditions() const {
        return {};
    }

    virtual void record_current_read_for_ssi() {}

    // Returns true if this executor yields rows in ascending order of `col`
    // (i.e. an index-ordered scan whose range is monotonic on `col`), so that
    // a min(col) aggregate can be answered from the first matching row alone.
    // Default: not supported.
    virtual bool provides_min_order(const TabCol& /*col*/) const {
        return false;
    }

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta>& rec_cols, const TabCol& target) const {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta& col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

protected:
    /**
     * @brief 比较条件cond与记录rec是否匹配
     * @param cond 条件
     * @param rec 记录
     * @return true if rec matches cond, false otherwise
     */
    static execution_scalar::CellValue read_cell(const char* data, const ColMeta& col) {
        execution_scalar::CellValue value;
        value.type = col.type;
        switch (col.type) {
        case TYPE_INT:
            value.int_val = *reinterpret_cast<const int*>(data);
            break;
        case TYPE_FLOAT:
            value.float_val = *reinterpret_cast<const double*>(data);
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            value.str_val = execution_scalar::trim_string(data, col.len);
            break;
        }
        return value;
    }

    bool is_null(const std::vector<bool>& nulls, const TabCol& target) const {
        auto pos = get_col(cols(), target);
        size_t index = static_cast<size_t>(pos - cols().begin());
        return index < nulls.size() && nulls[index];
    }

    static bool compare_order(CompOp op, int cmp) {
        switch (op) {
        case OP_EQ:
            return cmp == 0;
        case OP_NE:
            return cmp != 0;
        case OP_LT:
            return cmp < 0;
        case OP_GT:
            return cmp > 0;
        case OP_LE:
            return cmp <= 0;
        case OP_GE:
            return cmp >= 0;
        default:
            throw InternalError("Unexpected comparison operator");
        }
    }

    bool compare(const Condition& cond, const RmRecord& rec) {
        static const std::vector<bool> no_nulls;
        return compare(cond, rec, no_nulls);
    }

    bool compare(const Condition& cond, const RmRecord& rec, const std::vector<bool>& nulls) {
        ColMeta lhs_col_meta = get_col_offset(cond.lhs_col);
        bool lhs_is_null = !nulls.empty() && is_null(nulls, cond.lhs_col);
        if (cond.op == OP_IS_NULL) {
            return lhs_is_null;
        }
        if (cond.op == OP_IS_NOT_NULL) {
            return !lhs_is_null;
        }
        if (cond.op == OP_EXISTS) {
            return false;
        }
        if (lhs_is_null) {
            return false;
        }

        const auto lhs = read_cell(rec.data + lhs_col_meta.offset, lhs_col_meta);
        auto value_from_literal = [](const Value& value) {
            execution_scalar::CellValue result;
            result.type = value.type;
            if (value.type == TYPE_INT) {
                result.int_val = value.int_val;
            } else if (value.type == TYPE_FLOAT) {
                result.float_val = value.float_val;
            } else {
                result.str_val = value.str_val;
            }
            return result;
        };

        auto matches_simple = [&](const execution_scalar::CellValue& rhs, CompOp op) {
            if (!can_cast(lhs.type, rhs.type)) {
                throw IncompatibleTypeError(coltype2str(lhs.type), coltype2str(rhs.type));
            }
            if (op == OP_LIKE) {
                if ((lhs.type != TYPE_STRING && lhs.type != TYPE_DATETIME) ||
                    (rhs.type != TYPE_STRING && rhs.type != TYPE_DATETIME)) {
                    throw IncompatibleTypeError(coltype2str(lhs.type), coltype2str(rhs.type));
                }
                bool matches = execution_scalar::like_match(lhs.str_val, rhs.str_val);
                return cond.negated ? !matches : matches;
            }
            return compare_order(op, execution_scalar::compare_cells(lhs, rhs));
        };

        if (cond.op == OP_IN) {
            bool matched = false;
            bool has_null = false;
            for (const auto& rhs_val : cond.rhs_vals) {
                if (rhs_val.is_null) {
                    has_null = true;
                    continue;
                }
                if (matches_simple(value_from_literal(rhs_val), OP_EQ)) {
                    matched = true;
                    break;
                }
            }
            if (has_null && !matched) {
                return false;
            }
            return cond.negated ? !matched : matched;
        }

        if (cond.op == OP_BETWEEN) {
            if (!cond.has_rhs_upper) {
                throw InternalError("BETWEEN predicate is missing its upper bound");
            }
            if (cond.rhs_val.is_null || cond.rhs_upper.is_null) {
                return false;
            }
            bool matches = matches_simple(value_from_literal(cond.rhs_val), OP_GE) &&
                           matches_simple(value_from_literal(cond.rhs_upper), OP_LE);
            return cond.negated ? !matches : matches;
        }

        execution_scalar::CellValue rhs;
        if (cond.is_rhs_val) {
            if (cond.rhs_val.is_null) {
                return false;
            }
            rhs = value_from_literal(cond.rhs_val);
        } else {
            ColMeta rhs_col_meta = get_col_offset(cond.rhs_col);
            if (!nulls.empty() && is_null(nulls, cond.rhs_col)) {
                return false;
            }
            rhs = read_cell(rec.data + rhs_col_meta.offset, rhs_col_meta);
        }
        return matches_simple(rhs, cond.op);
    }
    /**
     * @brief 判断两个列类型是否可以进行转换
     * @param lhs 左侧列类型
     * @param rhs 右侧列类型
     * @return true if can cast, false otherwise
     */
    static inline bool can_cast(const ColType& lhs, const ColType& rhs) {
        if (lhs == rhs)
            return true;
        if (lhs == TYPE_INT && rhs == TYPE_FLOAT)
            return true;
        if (lhs == TYPE_FLOAT && rhs == TYPE_INT)
            return true;
        if ((lhs == TYPE_STRING && rhs == TYPE_DATETIME) || (lhs == TYPE_DATETIME && rhs == TYPE_STRING))
            return true;
        return false;
    }
};
