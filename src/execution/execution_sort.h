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
#include <algorithm>
#include <cstring>
#include <type_traits>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
private:
    struct SortKey {
        ColMeta col;
        bool is_desc = false;
    };

    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    std::vector<SortKey> sort_keys_;
    std::vector<RmRecord> tuples_;
    size_t cursor_ = 0;
    bool materialized_ = false;

    static std::string read_string(const char* data, int len) {
        return std::string(data, strnlen(data, len));
    }

    static int compare_cell(const RmRecord& lhs, const RmRecord& rhs, const ColMeta& col) {
        const char* lhs_data = lhs.data + col.offset;
        const char* rhs_data = rhs.data + col.offset;
        switch (col.type) {
        case TYPE_INT: {
            int lhs_val = *reinterpret_cast<const int*>(lhs_data);
            int rhs_val = *reinterpret_cast<const int*>(rhs_data);
            if (lhs_val < rhs_val) {
                return -1;
            }
            if (lhs_val > rhs_val) {
                return 1;
            }
            return 0;
        }
        case TYPE_FLOAT: {
            float lhs_val = *reinterpret_cast<const float*>(lhs_data);
            float rhs_val = *reinterpret_cast<const float*>(rhs_data);
            if (lhs_val < rhs_val) {
                return -1;
            }
            if (lhs_val > rhs_val) {
                return 1;
            }
            return 0;
        }
        case TYPE_STRING: {
            const auto lhs_val = read_string(lhs_data, col.len);
            const auto rhs_val = read_string(rhs_data, col.len);
            if (lhs_val < rhs_val) {
                return -1;
            }
            if (lhs_val > rhs_val) {
                return 1;
            }
            return 0;
        }
        }
        throw InternalError("Unexpected column type in SortExecutor");
    }

    static std::vector<ColMeta>::const_iterator find_col_by_name(const std::vector<ColMeta>& cols,
                                                                 const std::string& name) {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta& col) {
            return col.name == name || (!col.tab_name.empty() && (col.tab_name + "." + col.name) == name);
        });
        if (pos == cols.end()) {
            throw ColumnNotFoundError(name);
        }
        return pos;
    }

    ColMeta resolve_col(const TabCol& target) const {
        auto pos = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta& col) {
            if (!target.tab_name.empty()) {
                return col.tab_name == target.tab_name && col.name == target.col_name;
            }
            return col.name == target.col_name;
        });
        if (pos == cols_.end()) {
            throw ColumnNotFoundError(target.col_name);
        }
        return *pos;
    }

    void add_sort_key(const ColMeta& col, bool is_desc) {
        sort_keys_.push_back({col, is_desc});
    }

    void materialize_and_sort() {
        tuples_.clear();
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto rec = prev_->Next();
            if (rec != nullptr) {
                tuples_.emplace_back(*rec);
            }
        }
        std::stable_sort(tuples_.begin(), tuples_.end(), [&](const RmRecord& lhs, const RmRecord& rhs) {
            for (const auto& key : sort_keys_) {
                int cmp = compare_cell(lhs, rhs, key.col);
                if (cmp == 0) {
                    continue;
                }
                return key.is_desc ? (cmp > 0) : (cmp < 0);
            }
            return false;
        });
        materialized_ = true;
    }

    template <typename OrderByItemT> void build_from_order_by_items(const std::vector<OrderByItemT>& order_by_items) {
        for (const auto& item : order_by_items) {
            const auto& expr = item.expr;
            const auto* resolved = static_cast<const ColMeta*>(nullptr);
            auto try_resolve_by_name = [&](const std::string& name) -> bool {
                if (name.empty()) {
                    return false;
                }
                try {
                    resolved = &(*find_col_by_name(cols_, name));
                    return true;
                } catch (const ColumnNotFoundError&) {
                    return false;
                }
            };

            if (static_cast<int>(expr.type) == 0) {
                try {
                    ColMeta col = resolve_col(expr.col);
                    add_sort_key(col, item.is_desc);
                    continue;
                } catch (const ColumnNotFoundError&) {
                    try_resolve_by_name(expr.col.col_name);
                }
            } else {
                try_resolve_by_name(expr.display_name) || try_resolve_by_name(expr.agg.display_name);
            }

            if (resolved == nullptr) {
                throw ColumnNotFoundError(expr.display_name.empty() ? expr.col.col_name : expr.display_name);
            }
            add_sort_key(*resolved, item.is_desc);
        }
    }

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
        add_sort_key(resolve_col(sel_cols), is_desc);
    }

    template <typename OrderByItemT, typename = std::enable_if_t<!std::is_same_v<OrderByItemT, TabCol>>>
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<OrderByItemT>& order_by_items) {
        prev_ = std::move(prev);
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
        build_from_order_by_items(order_by_items);
    }

    void beginTuple() override {
        if (!materialized_) {
            materialize_and_sort();
        }
        cursor_ = 0;
    }

    void nextTuple() override {
        if (!is_end()) {
            ++cursor_;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(tuples_[cursor_]);
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    bool is_end() const override {
        return cursor_ >= tuples_.size();
    }

    std::string getType() override {
        return "SortExecutor";
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    ColMeta get_col_offset(const TabCol& target) override {
        return resolve_col(target);
    }
};
