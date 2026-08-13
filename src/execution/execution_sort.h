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
#include <algorithm>
#include <cstring>
#include <numeric>
#include <queue>
#include <type_traits>

#include "execution_defs.h"
#include "execution_manager.h"
#include "execution_scalar.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
private:
    using CompareFn = int (*)(const RmRecord&, const RmRecord&, const ColMeta&);

    struct SortKey {
        ColMeta col;
        bool is_desc = false;
        int nulls_order = 0;
        CompareFn compare_fn = nullptr;
    };

    struct MaterializedTuple {
        RmRecord record;
        std::vector<bool> nulls;
        size_t ordinal = 0;
    };

    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    std::vector<SortKey> sort_keys_;
    std::vector<RmRecord> tuples_;
    std::vector<std::vector<bool>> tuple_nulls_;
    size_t cursor_ = 0;
    bool materialized_ = false;
    int limit_ = -1;

    static int compare_int_cell(const RmRecord& lhs, const RmRecord& rhs, const ColMeta& col) {
        const char* lhs_data = lhs.data + col.offset;
        const char* rhs_data = rhs.data + col.offset;
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

    static int compare_float_cell(const RmRecord& lhs, const RmRecord& rhs, const ColMeta& col) {
        const char* lhs_data = lhs.data + col.offset;
        const char* rhs_data = rhs.data + col.offset;
        double lhs_val = *reinterpret_cast<const double*>(lhs_data);
        double rhs_val = *reinterpret_cast<const double*>(rhs_data);
        if (lhs_val < rhs_val) {
            return -1;
        }
        if (lhs_val > rhs_val) {
            return 1;
        }
        return 0;
    }

    static int compare_string_cell(const RmRecord& lhs, const RmRecord& rhs, const ColMeta& col) {
        const char* lhs_data = lhs.data + col.offset;
        const char* rhs_data = rhs.data + col.offset;
        const auto lhs_val = execution_scalar::trim_string_view(lhs_data, col.len);
        const auto rhs_val = execution_scalar::trim_string_view(rhs_data, col.len);
        if (lhs_val < rhs_val) {
            return -1;
        }
        if (lhs_val > rhs_val) {
            return 1;
        }
        return 0;
    }

    static CompareFn bind_compare_fn(ColType type) {
        switch (type) {
        case TYPE_INT:
            return &compare_int_cell;
        case TYPE_FLOAT:
            return &compare_float_cell;
        case TYPE_STRING:
        case TYPE_DATETIME:
            return &compare_string_cell;
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
        sort_keys_.push_back({col, is_desc, 0, bind_compare_fn(col.type)});
    }

    void add_sort_key(const ColMeta& col, bool is_desc, int nulls_order) {
        sort_keys_.push_back({col, is_desc, nulls_order, bind_compare_fn(col.type)});
    }

    int compare_records(const RmRecord& lhs, const RmRecord& rhs) const {
        for (const auto& key : sort_keys_) {
            int cmp = key.compare_fn(lhs, rhs, key.col);
            if (cmp != 0) {
                return key.is_desc ? -cmp : cmp;
            }
        }
        return 0;
    }

    int compare_materialized(const MaterializedTuple& lhs, const MaterializedTuple& rhs) const {
        for (size_t key_index = 0; key_index < sort_keys_.size(); ++key_index) {
            const auto& key = sort_keys_[key_index];
            auto col_pos = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta& col) {
                return col.offset == key.col.offset && col.name == key.col.name;
            });
            size_t null_index = col_pos == cols_.end() ? cols_.size() : static_cast<size_t>(col_pos - cols_.begin());
            bool lhs_null = null_index < lhs.nulls.size() && lhs.nulls[null_index];
            bool rhs_null = null_index < rhs.nulls.size() && rhs.nulls[null_index];
            if (lhs_null || rhs_null) {
                if (lhs_null && rhs_null) {
                    continue;
                }
                bool nulls_first = key.nulls_order == 1 || (key.nulls_order == 0 && !key.is_desc);
                return lhs_null == nulls_first ? -1 : 1;
            }
            int cmp = key.compare_fn(lhs.record, rhs.record, key.col);
            if (cmp != 0) {
                return key.is_desc ? -cmp : cmp;
            }
        }
        if (lhs.ordinal < rhs.ordinal) {
            return -1;
        }
        if (lhs.ordinal > rhs.ordinal) {
            return 1;
        }
        return 0;
    }

    bool comes_before(const MaterializedTuple& lhs, const MaterializedTuple& rhs) const {
        return compare_materialized(lhs, rhs) < 0;
    }

    void materialize_all() {
        tuples_.clear();
        tuple_nulls_.clear();
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto rec = prev_->Next();
            if (rec != nullptr) {
                tuples_.emplace_back(*rec);
                tuple_nulls_.push_back(prev_->nulls());
            }
        }
    }

    void materialize_top_k(size_t top_k) {
        struct HeapCompare {
            const SortExecutor* self = nullptr;

            bool operator()(const MaterializedTuple& lhs, const MaterializedTuple& rhs) const {
                return self->comes_before(lhs, rhs);
            }
        };

        tuples_.clear();
        tuple_nulls_.clear();
        if (top_k == 0) {
            return;
        }

        std::priority_queue<MaterializedTuple, std::vector<MaterializedTuple>, HeapCompare> heap(HeapCompare{this});
        size_t ordinal = 0;
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto rec = prev_->Next();
            if (rec == nullptr) {
                continue;
            }

            MaterializedTuple tuple{*rec, prev_->nulls(), ordinal++};
            if (heap.size() < top_k) {
                heap.push(std::move(tuple));
                continue;
            }
            if (comes_before(tuple, heap.top())) {
                heap.pop();
                heap.push(std::move(tuple));
            }
        }

        std::vector<MaterializedTuple> top_tuples;
        top_tuples.reserve(heap.size());
        while (!heap.empty()) {
            top_tuples.push_back(heap.top());
            heap.pop();
        }
        std::sort(top_tuples.begin(), top_tuples.end(),
                  [&](const MaterializedTuple& lhs, const MaterializedTuple& rhs) { return comes_before(lhs, rhs); });
        tuples_.reserve(top_tuples.size());
        tuple_nulls_.reserve(top_tuples.size());
        for (const auto& tuple : top_tuples) {
            tuples_.push_back(tuple.record);
            tuple_nulls_.push_back(tuple.nulls);
        }
    }

    void materialize_and_sort() {
        if (limit_ >= 0 && !sort_keys_.empty()) {
            materialize_top_k(static_cast<size_t>(limit_));
        } else {
            materialize_all();
        }
        std::vector<size_t> order(tuples_.size());
        std::iota(order.begin(), order.end(), 0);
        std::stable_sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs) {
            return compare_materialized(MaterializedTuple{tuples_[lhs], tuple_nulls_[lhs], lhs},
                                        MaterializedTuple{tuples_[rhs], tuple_nulls_[rhs], rhs}) < 0;
        });
        std::vector<RmRecord> sorted_tuples;
        std::vector<std::vector<bool>> sorted_nulls;
        sorted_tuples.reserve(order.size());
        sorted_nulls.reserve(order.size());
        for (size_t index : order) {
            sorted_tuples.push_back(std::move(tuples_[index]));
            sorted_nulls.push_back(std::move(tuple_nulls_[index]));
        }
        tuples_ = std::move(sorted_tuples);
        tuple_nulls_ = std::move(sorted_nulls);
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
                    add_sort_key(col, item.is_desc, item.nulls_order);
                    continue;
                } catch (const ColumnNotFoundError&) {
                    try_resolve_by_name(item.order_name) || try_resolve_by_name(expr.col.col_name);
                }
            } else {
                try_resolve_by_name(item.order_name) || try_resolve_by_name(expr.display_name) ||
                    try_resolve_by_name(expr.agg.display_name);
            }

            if (resolved == nullptr) {
                throw ColumnNotFoundError(!item.order_name.empty()
                                              ? item.order_name
                                              : (expr.display_name.empty() ? expr.col.col_name : expr.display_name));
            }
            add_sort_key(*resolved, item.is_desc, item.nulls_order);
        }
    }

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc, int limit = -1) {
        prev_ = std::move(prev);
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
        limit_ = limit;
        add_sort_key(resolve_col(sel_cols), is_desc);
    }

    template <typename OrderByItemT, typename = std::enable_if_t<!std::is_same_v<OrderByItemT, TabCol>>>
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<OrderByItemT>& order_by_items,
                 int limit = -1) {
        prev_ = std::move(prev);
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
        limit_ = limit;
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

    const std::vector<bool>& nulls() const override {
        static const std::vector<bool> no_nulls;
        return is_end() ? no_nulls : tuple_nulls_[cursor_];
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
