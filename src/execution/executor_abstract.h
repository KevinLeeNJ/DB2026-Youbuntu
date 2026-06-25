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
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
public:
    Rid _abstract_rid;

    Context* context_;

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
        char* lhs_data = rec.data + lhs_col_meta.offset;
        ColType lhs_type, rhs_type;
        lhs_type = get_col_offset(cond.lhs_col).type;
        char* rhs_data = nullptr;
        if (!cond.is_rhs_val) {
            rhs_col_meta = get_col_offset(cond.rhs_col);
            rhs_data = rec.data + rhs_col_meta.offset;
            rhs_type = rhs_col_meta.type;
        } else {
            rhs_type = cond.rhs_val.type;
        }
        if (can_cast(lhs_type, rhs_type) == false) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
        switch (lhs_type) {
        case TYPE_INT:
        case TYPE_FLOAT: {
            float lhs_val = lhs_type == TYPE_INT ? (float)*(int*)lhs_data : *(float*)lhs_data;
            float rhs_val;
            if (cond.is_rhs_val) {
                rhs_val = rhs_type == TYPE_INT ? (float)cond.rhs_val.int_val : cond.rhs_val.float_val;
            } else {
                rhs_val = rhs_type == TYPE_INT ? (float)*(int*)rhs_data : *(float*)rhs_data;
            }
            switch (cond.op) {
            case OP_EQ:
                return lhs_val == rhs_val;
            case OP_NE:
                return lhs_val != rhs_val;
            case OP_LT:
                return lhs_val < rhs_val;
            case OP_GT:
                return lhs_val > rhs_val;
            case OP_LE:
                return lhs_val <= rhs_val;
            case OP_GE:
                return lhs_val >= rhs_val;
            }
        }
        case TYPE_STRING: {
            std::string lhs_val(lhs_data, strnlen(lhs_data, lhs_col_meta.len));
            std::string rhs_val =
                cond.is_rhs_val ? cond.rhs_val.str_val : std::string(rhs_data, strnlen(rhs_data, rhs_col_meta.len));
            switch (cond.op) {
            case OP_EQ:
                return lhs_val == rhs_val;
            case OP_NE:
                return lhs_val != rhs_val;
            case OP_LT:
                return lhs_val < rhs_val;
            case OP_GT:
                return lhs_val > rhs_val;
            case OP_LE:
                return lhs_val <= rhs_val;
            case OP_GE:
                return lhs_val >= rhs_val;
            }
        }
        }
        return false;
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
        return false;
    }
};
