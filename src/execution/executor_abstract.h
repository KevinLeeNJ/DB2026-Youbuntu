/* Copyright (c) 2023 Renmin University of China
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

    static std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta>& rec_cols, const TabCol& target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta& col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    static bool is_numeric(ColType type) {
        return type == TYPE_INT || type == TYPE_FLOAT;
    }

    static bool eval_conds(const std::vector<Condition>& conds, const std::vector<ColMeta>& cols, const RmRecord& rec) {
        for (auto& cond : conds) {
            auto lhs_it = get_col(cols, cond.lhs_col);
            auto& lhs_col = *lhs_it;
            char* lhs_data = rec.data + lhs_col.offset;

            int cmp = 0;
            if (cond.is_rhs_val) {
                // 支持 INT 和 FLOAT 之间的跨类型比较，统一提升为 FLOAT
                if (is_numeric(lhs_col.type) && is_numeric(cond.rhs_val.type)) {
                    float lhs_val = (lhs_col.type == TYPE_INT) ? static_cast<float>(*reinterpret_cast<int*>(lhs_data))
                                                               : *reinterpret_cast<float*>(lhs_data);
                    float rhs_val = (cond.rhs_val.type == TYPE_INT) ? static_cast<float>(cond.rhs_val.int_val)
                                                                    : cond.rhs_val.float_val;
                    cmp = (lhs_val < rhs_val) ? -1 : (lhs_val > rhs_val) ? 1 : 0;
                } else if (lhs_col.type == TYPE_INT) {
                    int lhs_val = *reinterpret_cast<int*>(lhs_data);
                    cmp = (lhs_val < cond.rhs_val.int_val) ? -1 : (lhs_val > cond.rhs_val.int_val) ? 1 : 0;
                } else if (lhs_col.type == TYPE_FLOAT) {
                    float lhs_val = *reinterpret_cast<float*>(lhs_data);
                    cmp = (lhs_val < cond.rhs_val.float_val) ? -1 : (lhs_val > cond.rhs_val.float_val) ? 1 : 0;
                } else {
                    cmp = strncmp(lhs_data, cond.rhs_val.str_val.c_str(), lhs_col.len);
                }
            } else {
                auto rhs_it = get_col(cols, cond.rhs_col);
                char* rhs_data = rec.data + rhs_it->offset;
                // 支持 INT 和 FLOAT 之间的跨类型比较
                if (is_numeric(lhs_col.type) && is_numeric(rhs_it->type)) {
                    float lhs_val = (lhs_col.type == TYPE_INT) ? static_cast<float>(*reinterpret_cast<int*>(lhs_data))
                                                               : *reinterpret_cast<float*>(lhs_data);
                    float rhs_val = (rhs_it->type == TYPE_INT) ? static_cast<float>(*reinterpret_cast<int*>(rhs_data))
                                                               : *reinterpret_cast<float*>(rhs_data);
                    cmp = (lhs_val < rhs_val) ? -1 : (lhs_val > rhs_val) ? 1 : 0;
                } else if (lhs_col.type == TYPE_INT) {
                    int lhs_val = *reinterpret_cast<int*>(lhs_data);
                    int rhs_val = *reinterpret_cast<int*>(rhs_data);
                    cmp = (lhs_val < rhs_val) ? -1 : (lhs_val > rhs_val) ? 1 : 0;
                } else if (lhs_col.type == TYPE_FLOAT) {
                    float lhs_val = *reinterpret_cast<float*>(lhs_data);
                    float rhs_val = *reinterpret_cast<float*>(rhs_data);
                    cmp = (lhs_val < rhs_val) ? -1 : (lhs_val > rhs_val) ? 1 : 0;
                } else {
                    cmp = strncmp(lhs_data, rhs_data, lhs_col.len);
                }
            }

            switch (cond.op) {
            case OP_EQ:
                if (cmp != 0)
                    return false;
                break;
            case OP_NE:
                if (cmp == 0)
                    return false;
                break;
            case OP_LT:
                if (cmp >= 0)
                    return false;
                break;
            case OP_GT:
                if (cmp <= 0)
                    return false;
                break;
            case OP_LE:
                if (cmp > 0)
                    return false;
                break;
            case OP_GE:
                if (cmp < 0)
                    return false;
                break;
            default:
                return false;
            }
        }
        return true;
    }

    static bool eval_conds(const std::vector<Condition>& conds, const std::vector<ColMeta>& cols,
                           const RmRecord& left_rec, const RmRecord& right_rec, size_t left_len) {
        for (auto& cond : conds) {
            auto lhs_it = get_col(cols, cond.lhs_col);
            auto& lhs_col = *lhs_it;
            char* lhs_data = (static_cast<size_t>(lhs_col.offset) < left_len)
                                 ? left_rec.data + lhs_col.offset
                                 : right_rec.data + (lhs_col.offset - static_cast<int>(left_len));

            int cmp = 0;
            if (cond.is_rhs_val) {
                // 支持 INT 和 FLOAT 之间的跨类型比较，统一提升为 FLOAT
                if (is_numeric(lhs_col.type) && is_numeric(cond.rhs_val.type)) {
                    float lhs_val = (lhs_col.type == TYPE_INT) ? static_cast<float>(*reinterpret_cast<int*>(lhs_data))
                                                               : *reinterpret_cast<float*>(lhs_data);
                    float rhs_val = (cond.rhs_val.type == TYPE_INT) ? static_cast<float>(cond.rhs_val.int_val)
                                                                    : cond.rhs_val.float_val;
                    cmp = (lhs_val < rhs_val) ? -1 : (lhs_val > rhs_val) ? 1 : 0;
                } else if (lhs_col.type == TYPE_INT) {
                    int lhs_val = *reinterpret_cast<int*>(lhs_data);
                    cmp = (lhs_val < cond.rhs_val.int_val) ? -1 : (lhs_val > cond.rhs_val.int_val) ? 1 : 0;
                } else if (lhs_col.type == TYPE_FLOAT) {
                    float lhs_val = *reinterpret_cast<float*>(lhs_data);
                    cmp = (lhs_val < cond.rhs_val.float_val) ? -1 : (lhs_val > cond.rhs_val.float_val) ? 1 : 0;
                } else {
                    cmp = strncmp(lhs_data, cond.rhs_val.str_val.c_str(), lhs_col.len);
                }
            } else {
                auto rhs_it = get_col(cols, cond.rhs_col);
                auto& rhs_col = *rhs_it;
                char* rhs_data = (static_cast<size_t>(rhs_col.offset) < left_len)
                                     ? left_rec.data + rhs_col.offset
                                     : right_rec.data + (rhs_col.offset - static_cast<int>(left_len));
                // 支持 INT 和 FLOAT 之间的跨类型比较
                if (is_numeric(lhs_col.type) && is_numeric(rhs_col.type)) {
                    float lhs_val = (lhs_col.type == TYPE_INT) ? static_cast<float>(*reinterpret_cast<int*>(lhs_data))
                                                               : *reinterpret_cast<float*>(lhs_data);
                    float rhs_val = (rhs_col.type == TYPE_INT) ? static_cast<float>(*reinterpret_cast<int*>(rhs_data))
                                                               : *reinterpret_cast<float*>(rhs_data);
                    cmp = (lhs_val < rhs_val) ? -1 : (lhs_val > rhs_val) ? 1 : 0;
                } else if (lhs_col.type == TYPE_INT) {
                    int lhs_val = *reinterpret_cast<int*>(lhs_data);
                    int rhs_val = *reinterpret_cast<int*>(rhs_data);
                    cmp = (lhs_val < rhs_val) ? -1 : (lhs_val > rhs_val) ? 1 : 0;
                } else if (lhs_col.type == TYPE_FLOAT) {
                    float lhs_val = *reinterpret_cast<float*>(lhs_data);
                    float rhs_val = *reinterpret_cast<float*>(rhs_data);
                    cmp = (lhs_val < rhs_val) ? -1 : (lhs_val > rhs_val) ? 1 : 0;
                } else {
                    cmp = strncmp(lhs_data, rhs_data, lhs_col.len);
                }
            }

            switch (cond.op) {
            case OP_EQ:
                if (cmp != 0)
                    return false;
                break;
            case OP_NE:
                if (cmp == 0)
                    return false;
                break;
            case OP_LT:
                if (cmp >= 0)
                    return false;
                break;
            case OP_GT:
                if (cmp <= 0)
                    return false;
                break;
            case OP_LE:
                if (cmp > 0)
                    return false;
                break;
            case OP_GE:
                if (cmp < 0)
                    return false;
                break;
            default:
                return false;
            }
        }
        return true;
    }
};