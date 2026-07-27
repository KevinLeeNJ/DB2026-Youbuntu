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
        CompareFn compare_fn = nullptr;
    };

    struct MaterializedTuple {
        RmRecord record;
        size_t ordinal = 0;
    };

    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    std::vector<SortKey> sort_keys_;
    std::vector<RmRecord> tuples_;
    size_t cursor_ = 0;
    bool materialized_ = false;
    int limit_ = -1;

    static std::unique_ptr<RmRecord> copy_view(TupleView view) {
        if (!view) {
            return nullptr;
        }
        auto record = std::make_unique<RmRecord>(static_cast<int>(view.size));
        std::memcpy(record->data, view.data, view.size);
        return record;
    }

    static int compare_int_cell(const RmRecord& lhs, const RmRecord& rhs, const ColMeta& col) {
        const char* lhs_data = lhs.data + col.offset;
        const char* rhs_data = rhs.data + col.offset;
        int lhs_val = read_unaligned<int>(lhs_data);
        int rhs_val = read_unaligned<int>(rhs_data);
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
        float lhs_val = read_float(lhs_data);
        float rhs_val = read_float(rhs_data);
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
        sort_keys_.push_back({col, is_desc, bind_compare_fn(col.type)});
    }

    // NULL 排在所有非 NULL 之后（即视为最大值），两个 NULL 相等。这个约定使
    // ASC 时 NULLS LAST、DESC 时 NULLS FIRST，与 PostgreSQL 的默认行为一致；
    // 引擎不提供显式的 NULLS FIRST/LAST 语法。
    static int compare_sort_key(const SortKey& key, const RmRecord& lhs, const RmRecord& rhs) {
        const bool lhs_null = is_null(lhs.data, key.col);
        const bool rhs_null = is_null(rhs.data, key.col);
        if (lhs_null || rhs_null) {
            return lhs_null == rhs_null ? 0 : (lhs_null ? 1 : -1);
        }
        return key.compare_fn(lhs, rhs, key.col);
    }

    int compare_records(const RmRecord& lhs, const RmRecord& rhs) const {
        for (const auto& key : sort_keys_) {
            int cmp = compare_sort_key(key, lhs, rhs);
            if (cmp != 0) {
                return key.is_desc ? -cmp : cmp;
            }
        }
        return 0;
    }

    int compare_materialized(const MaterializedTuple& lhs, const MaterializedTuple& rhs) const {
        int cmp = compare_records(lhs.record, rhs.record);
        if (cmp != 0) {
            return cmp;
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
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto rec = copy_view(prev_->current());
            if (rec == nullptr) {
                rec = prev_->Next();
            }
            if (rec != nullptr) {
                tuples_.emplace_back(*rec);
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
        if (top_k == 0) {
            return;
        }

        std::priority_queue<MaterializedTuple, std::vector<MaterializedTuple>, HeapCompare> heap(HeapCompare{this});
        size_t ordinal = 0;
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto rec = copy_view(prev_->current());
            if (rec == nullptr) {
                rec = prev_->Next();
            }
            if (rec == nullptr) {
                continue;
            }

            MaterializedTuple tuple{*rec, ordinal++};
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
        for (const auto& tuple : top_tuples) {
            tuples_.push_back(tuple.record);
        }
    }

    void materialize_and_sort() {
        if (limit_ >= 0 && !sort_keys_.empty()) {
            materialize_top_k(static_cast<size_t>(limit_));
        } else {
            materialize_all();
        }
        std::stable_sort(tuples_.begin(), tuples_.end(), [&](const RmRecord& lhs, const RmRecord& rhs) {
            for (const auto& key : sort_keys_) {
                int cmp = compare_sort_key(key, lhs, rhs);
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
            add_sort_key(*resolved, item.is_desc);
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

    TupleView current() const override {
        if (is_end()) {
            return {};
        }
        return TupleView{tuples_[cursor_].data, static_cast<uint32_t>(tuples_[cursor_].size)};
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
