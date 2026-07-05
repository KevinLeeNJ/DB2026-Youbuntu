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
#include "expression_evaluator.h"
#include "common/common.h"
#include "common/context.h"
#include "common/type_utils.h"
#include "index/ix.h"
#include "system/sm_meta.h"

namespace rmdb::exec {
class AbstractExecutor {
public:
    Rid _abstract_rid;

    Context* context_ = nullptr;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const {
        return 0;
    };

    virtual const std::vector<ColMeta>& cols() const {
        std::vector<ColMeta>* _cols = nullptr;
        return *_cols;
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

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta>& rec_cols, const TabCol& target) {
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
    bool compare(const Condition& cond, const RmRecord& rec) {
        ColMeta lhs_col_meta = get_col_offset(cond.lhs_col);
        ColMeta rhs_col_meta;
        const ColMeta* rhs_col_meta_ptr = nullptr;
        if (!cond.is_rhs_val) {
            rhs_col_meta = get_col_offset(cond.rhs_col);
            rhs_col_meta_ptr = &rhs_col_meta;
        }
        return ExpressionEvaluator::eval_condition(cond, rec, lhs_col_meta, rhs_col_meta_ptr);
    }
};

} // namespace rmdb::exec

namespace rmdb {
using exec::AbstractExecutor;
} // namespace rmdb
