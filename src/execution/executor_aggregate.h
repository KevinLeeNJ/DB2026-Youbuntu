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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "errors.h"
#include "execution_defs.h"
#include "execution_scalar.h"
#include "executor_abstract.h"
#ifdef RMDB_ENABLE_JIT
#include "jit/jit_aggregate.h"
#endif

class AggregateExecutor : public AbstractExecutor {
private:
    enum class LocalAggType { COUNT = 0, MAX = 1, MIN = 2, SUM = 3, AVG = 4 };
    enum class OperandKind { GROUP_COL, AGG_RESULT, VALUE };

    using CellValue = execution_scalar::CellValue;
    using CellValueHash = execution_scalar::CellValueHash;

    struct GroupKey {
        std::vector<CellValue> values;

        bool operator==(const GroupKey& other) const {
            return values == other.values;
        }
    };

    struct GroupKeyHash {
        size_t operator()(const GroupKey& key) const {
            size_t seed = 0;
            CellValueHash hash_cell;
            for (const auto& value : key.values) {
                execution_scalar::hash_combine(seed, hash_cell(value));
            }
            return seed;
        }
    };

    struct AggregateSpec {
        LocalAggType type = LocalAggType::COUNT;
        bool is_star = false;
        TabCol col;
        std::string output_name;
        ColType input_type = TYPE_INT;
        int input_len = static_cast<int>(sizeof(int));
        ColMeta input_col;
    };

    struct AggregateState {
        int64_t count = 0;
        double sum = 0.0;
        bool has_value = false;
        CellValue value;
    };

    struct GroupState {
        std::vector<CellValue> group_values;
        std::vector<AggregateState> aggregate_states;
    };

    struct HavingOperand {
        OperandKind kind = OperandKind::VALUE;
        size_t index = 0;
        CellValue literal;
    };

    struct HavingSpec {
        HavingOperand lhs;
        CompOp op = OP_EQ;
        HavingOperand rhs;
    };

    template <typename T, typename = void> struct has_member_val : std::false_type {};

    template <typename T>
    struct has_member_val<T, std::void_t<decltype(std::declval<const T&>().val)>> : std::true_type {};

    template <typename T, typename = void> struct has_member_value : std::false_type {};

    template <typename T>
    struct has_member_value<T, std::void_t<decltype(std::declval<const T&>().value)>> : std::true_type {};

    template <typename T, typename = void> struct has_member_agg : std::false_type {};

    template <typename T>
    struct has_member_agg<T, std::void_t<decltype(std::declval<const T&>().agg)>> : std::true_type {};

    std::unique_ptr<AbstractExecutor> prev_;
    Context* context_ = nullptr;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    std::vector<ColMeta> group_cols_;
    std::vector<AggregateSpec> aggregates_;
    std::vector<HavingSpec> having_conds_;
    std::vector<GroupState> groups_;
    size_t cursor_ = 0;
    bool materialized_ = false;
    bool has_group_by_ = false;
    mutable std::unique_ptr<RmRecord> current_output_;
#ifdef RMDB_ENABLE_JIT
    std::unique_ptr<jit::AggregateKernel> jit_aggregate_;
#endif

    static std::string trim_string(const char* data, int len) {
        return execution_scalar::trim_string(data, len);
    }

    static LocalAggType normalize_agg_type(int type_code) {
        switch (type_code) {
        case 0:
            return LocalAggType::COUNT;
        case 1:
            return LocalAggType::MAX;
        case 2:
            return LocalAggType::MIN;
        case 3:
            return LocalAggType::SUM;
        case 4:
            return LocalAggType::AVG;
        default:
            throw InternalError("Unexpected aggregate type");
        }
    }

    CellValue read_cell(const RmRecord& rec, const ColMeta& col) const {
        return read_cell(TupleView{rec.data, static_cast<uint32_t>(rec.size)}, col);
    }

    CellValue read_cell(TupleView tuple, const ColMeta& col) const {
        CellValue value;
        value.type = col.type;
        const char* data = tuple.data + col.offset;
        switch (col.type) {
        case TYPE_INT:
            value.int_val = read_unaligned<int>(data);
            break;
        case TYPE_FLOAT:
            value.float_val = read_unaligned<double>(data);
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            value.str_val = trim_string(data, col.len);
            break;
        }
        return value;
    }

    static int compare_cells(const CellValue& lhs, const CellValue& rhs) {
        return execution_scalar::compare_cells(lhs, rhs);
    }

    static bool compare_with_op(CompOp op, int cmp) {
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
        }
        throw InternalError("Unexpected comparison operator");
    }

    static CellValue zero_value(ColType type) {
        return execution_scalar::zero_value(type);
    }

    template <typename ExprT> static const Value& get_expr_literal_value(const ExprT& expr) {
        if constexpr (has_member_val<ExprT>::value) {
            return expr.val;
        } else if constexpr (has_member_value<ExprT>::value) {
            return expr.value;
        } else {
            static_assert(has_member_val<ExprT>::value || has_member_value<ExprT>::value,
                          "Expression type must expose val or value");
        }
    }

    void write_cell(char* dest, const ColMeta& col, const CellValue& value) const {
        switch (col.type) {
        case TYPE_INT:
            write_unaligned(dest + col.offset, value.int_val);
            break;
        case TYPE_FLOAT:
            write_unaligned(dest + col.offset, value.float_val);
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            std::memset(dest + col.offset, 0, col.len);
            std::memcpy(dest + col.offset, value.str_val.data(), std::min<int>(col.len, value.str_val.size()));
            break;
        }
    }

    ColMeta find_input_col(const TabCol& target) const {
        return prev_->get_col_offset(target);
    }

    void append_output_col(ColMeta col, const std::string& output_name) {
        col.offset = static_cast<int>(len_);
        if (!output_name.empty()) {
            col.name = output_name;
            col.tab_name.clear();
        }
        len_ += static_cast<size_t>(col.len);
        cols_.push_back(col);
    }

    template <typename GroupByT> static const TabCol& extract_group_col(const GroupByT& item) {
        if constexpr (std::is_same_v<GroupByT, TabCol>) {
            return item;
        } else {
            return item.col;
        }
    }

    template <typename AggExprT> AggregateSpec make_aggregate_spec_direct(const AggExprT& agg_expr) const {
        AggregateSpec spec;
        spec.type = normalize_agg_type(static_cast<int>(agg_expr.type));
        spec.is_star = agg_expr.is_star;
        spec.col = agg_expr.col;
        spec.output_name = agg_expr.display_name;
        if (spec.type == LocalAggType::COUNT) {
            spec.input_type = TYPE_INT;
            spec.input_len = static_cast<int>(sizeof(int));
        } else {
            spec.input_col = find_input_col(spec.col);
            spec.input_type = spec.input_col.type;
            spec.input_len = spec.input_col.len;
        }
        return spec;
    }

    template <typename AggItemT> AggregateSpec make_aggregate_spec(const AggItemT& agg_item) const {
        if constexpr (has_member_agg<AggItemT>::value) {
            return make_aggregate_spec_direct(agg_item.agg);
        } else {
            return make_aggregate_spec_direct(agg_item);
        }
    }

    template <typename ExprT> HavingOperand make_operand_from_expr(const ExprT& expr) const {
        HavingOperand operand;
        const int expr_type = static_cast<int>(expr.type);
        if (expr_type == 2) {
            const Value& literal = get_expr_literal_value(expr);
            operand.kind = OperandKind::VALUE;
            operand.literal = zero_value(literal.type);
            operand.literal.type = literal.type;
            if (literal.type == TYPE_INT) {
                operand.literal.int_val = literal.int_val;
            } else if (literal.type == TYPE_FLOAT) {
                operand.literal.float_val = literal.float_val;
            } else {
                operand.literal.str_val = literal.str_val;
            }
            return operand;
        }

        if (expr_type == 0) {
            for (size_t i = 0; i < group_cols_.size(); ++i) {
                if (group_cols_[i].tab_name == expr.col.tab_name && group_cols_[i].name == expr.col.col_name) {
                    operand.kind = OperandKind::GROUP_COL;
                    operand.index = i;
                    return operand;
                }
                if (group_cols_[i].name == expr.col.col_name) {
                    operand.kind = OperandKind::GROUP_COL;
                    operand.index = i;
                    return operand;
                }
            }
            throw ColumnNotFoundError(expr.col.col_name);
        }

        for (size_t i = 0; i < aggregates_.size(); ++i) {
            const auto& agg = aggregates_[i];
            if (!expr.display_name.empty() && agg.output_name == expr.display_name) {
                operand.kind = OperandKind::AGG_RESULT;
                operand.index = i;
                return operand;
            }
            if (agg.is_star == expr.agg.is_star && agg.col.tab_name == expr.agg.col.tab_name &&
                agg.col.col_name == expr.agg.col.col_name &&
                static_cast<int>(agg.type) == static_cast<int>(normalize_agg_type(static_cast<int>(expr.agg.type)))) {
                operand.kind = OperandKind::AGG_RESULT;
                operand.index = i;
                return operand;
            }
        }
        throw ColumnNotFoundError(expr.display_name);
    }

    template <typename HavingCondT> HavingSpec make_having_spec(const HavingCondT& cond) const {
        HavingSpec spec;
        spec.lhs = make_operand_from_expr(cond.lhs);
        spec.op = cond.op;
        if (cond.is_rhs_value) {
            spec.rhs.kind = OperandKind::VALUE;
            spec.rhs.literal.type = cond.rhs_val.type;
            if (cond.rhs_val.type == TYPE_INT) {
                spec.rhs.literal.int_val = cond.rhs_val.int_val;
            } else if (cond.rhs_val.type == TYPE_FLOAT) {
                spec.rhs.literal.float_val = cond.rhs_val.float_val;
            } else {
                spec.rhs.literal.str_val = cond.rhs_val.str_val;
            }
        } else {
            spec.rhs = make_operand_from_expr(cond.rhs_expr);
        }
        return spec;
    }

    AggregateState init_aggregate_state(const AggregateSpec& spec) const {
        AggregateState state;
        state.value = zero_value(spec.input_type);
        return state;
    }

    void update_aggregate_state(AggregateState& state, const AggregateSpec& spec, const RmRecord& rec) const {
        update_aggregate_state(state, spec, TupleView{rec.data, static_cast<uint32_t>(rec.size)});
    }

    void update_aggregate_state(AggregateState& state, const AggregateSpec& spec, TupleView tuple) const {
        phase_metrics::ScopedSample metrics_sample(
            phase_metrics::Phase::AGGREGATE_TRANSITION,
            phase_metrics::sample_rate(phase_metrics::Phase::AGGREGATE_TRANSITION));
        CellValue current_value;
        if (!spec.is_star && spec.type != LocalAggType::COUNT) {
            current_value = read_cell(tuple, spec.input_col);
        }

        switch (spec.type) {
        case LocalAggType::COUNT:
            ++state.count;
            break;
        case LocalAggType::SUM:
            state.sum += (current_value.type == TYPE_INT) ? static_cast<double>(current_value.int_val)
                                                          : static_cast<double>(current_value.float_val);
            break;
        case LocalAggType::AVG:
            state.sum += (current_value.type == TYPE_INT) ? static_cast<double>(current_value.int_val)
                                                          : static_cast<double>(current_value.float_val);
            ++state.count;
            break;
        case LocalAggType::MAX:
            if (!state.has_value || compare_cells(current_value, state.value) > 0) {
                state.value = current_value;
                state.has_value = true;
            }
            break;
        case LocalAggType::MIN:
            if (!state.has_value || compare_cells(current_value, state.value) < 0) {
                state.value = current_value;
                state.has_value = true;
            }
            break;
        }
    }

    std::vector<CellValue> finalize_aggregate_values(const GroupState& group_state) const {
        std::vector<CellValue> aggregate_values;
        aggregate_values.reserve(aggregates_.size());
        for (size_t i = 0; i < aggregates_.size(); ++i) {
            aggregate_values.push_back(finalize_aggregate(aggregates_[i], group_state.aggregate_states[i]));
        }
        return aggregate_values;
    }

    CellValue finalize_aggregate(const AggregateSpec& spec, const AggregateState& state) const {
        switch (spec.type) {
        case LocalAggType::COUNT: {
            CellValue value;
            value.type = TYPE_INT;
            value.int_val = static_cast<int>(state.count);
            return value;
        }
        case LocalAggType::SUM: {
            CellValue value;
            value.type = spec.input_type;
            if (spec.input_type == TYPE_INT) {
                value.int_val = static_cast<int>(state.sum);
            } else {
                value.float_val = state.sum;
            }
            return value;
        }
        case LocalAggType::AVG: {
            CellValue value;
            value.type = TYPE_FLOAT;
            value.float_val = state.count == 0 ? 0.0 : state.sum / static_cast<double>(state.count);
            return value;
        }
        case LocalAggType::MAX:
        case LocalAggType::MIN:
            if (state.has_value) {
                return state.value;
            }
            return zero_value(spec.input_type);
        }
        throw InternalError("Unexpected aggregate type");
    }

    CellValue resolve_having_operand(const HavingOperand& operand, const std::vector<CellValue>& group_values,
                                     const std::vector<CellValue>& aggregate_values) const {
        switch (operand.kind) {
        case OperandKind::GROUP_COL:
            return group_values.at(operand.index);
        case OperandKind::AGG_RESULT:
            return aggregate_values.at(operand.index);
        case OperandKind::VALUE:
            return operand.literal;
        }
        throw InternalError("Unexpected HAVING operand kind");
    }

    bool passes_having(const std::vector<CellValue>& group_values,
                       const std::vector<CellValue>& aggregate_values) const {
        for (const auto& cond : having_conds_) {
            CellValue lhs = resolve_having_operand(cond.lhs, group_values, aggregate_values);
            CellValue rhs = resolve_having_operand(cond.rhs, group_values, aggregate_values);
            if (!compare_with_op(cond.op, compare_cells(lhs, rhs))) {
                return false;
            }
        }
        return true;
    }

    bool passes_having(const GroupState& group_state) const {
        std::vector<CellValue> aggregate_values = finalize_aggregate_values(group_state);
        return passes_having(group_state.group_values, aggregate_values);
    }

    bool can_count_star_by_cursor_only() const {
        return !has_group_by_ && having_conds_.empty() && aggregates_.size() == 1 &&
               aggregates_[0].type == LocalAggType::COUNT && aggregates_[0].is_star &&
               !prev_->scan_table_name().empty();
    }

    // min(col) can stop at the first row if the child provides ascending order
    // on col. Only applies to a single min() with no GROUP BY / HAVING.
    bool can_use_min_index_shortcut() const {
        if (has_group_by_ || !having_conds_.empty() || aggregates_.size() != 1) {
            return false;
        }
        const auto& agg = aggregates_[0];
        if (agg.type != LocalAggType::MIN || agg.is_star) {
            return false;
        }
        return prev_->provides_min_order(agg.col);
    }

    std::unique_ptr<RmRecord> materialize_group_result(const GroupState& group_state) const {
        std::vector<CellValue> aggregate_values = finalize_aggregate_values(group_state);
        RmRecord rec(static_cast<int>(len_));
        std::memset(rec.data, 0, len_);
        for (size_t i = 0; i < group_state.group_values.size(); ++i) {
            write_cell(rec.data, cols_[i], group_state.group_values[i]);
        }
        for (size_t i = 0; i < aggregate_values.size(); ++i) {
            write_cell(rec.data, cols_[group_state.group_values.size() + i], aggregate_values[i]);
        }
        return std::make_unique<RmRecord>(rec);
    }

    GroupState make_group_state(const std::vector<CellValue>& group_values) const {
        GroupState state;
        state.group_values = group_values;
        state.aggregate_states.reserve(aggregates_.size());
        for (const auto& aggregate : aggregates_) {
            state.aggregate_states.push_back(init_aggregate_state(aggregate));
        }
        return state;
    }

    void materialize_groups() {
        groups_.clear();
        if (has_group_by_) {
            std::unordered_map<GroupKey, size_t, GroupKeyHash> group_indexes;
            for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
                TupleView tuple = prev_->current();
                std::unique_ptr<RmRecord> fallback;
                if (!tuple) {
                    fallback = prev_->Next();
                    if (fallback != nullptr) {
                        tuple = TupleView{fallback->data, static_cast<uint32_t>(fallback->size)};
                    }
                }
                if (!tuple) {
                    continue;
                }

                GroupKey key;
                key.values.reserve(group_cols_.size());
                for (const auto& col : group_cols_) {
                    key.values.push_back(read_cell(tuple, col));
                }

                auto [it, inserted] = group_indexes.emplace(key, groups_.size());
                if (inserted) {
                    groups_.push_back(make_group_state(key.values));
                }
                for (size_t i = 0; i < aggregates_.size(); ++i) {
                    update_aggregate_state(groups_[it->second].aggregate_states[i], aggregates_[i], tuple);
                }
            }

            size_t kept = 0;
            for (size_t i = 0; i < groups_.size(); ++i) {
                if (!passes_having(groups_[i])) {
                    continue;
                }
                if (kept != i) {
                    groups_[kept] = std::move(groups_[i]);
                }
                ++kept;
            }
            groups_.resize(kept);
            return;
        }

        GroupState global_state = make_group_state({});
        if (can_count_star_by_cursor_only()) {
            for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
                ++global_state.aggregate_states[0].count;
            }
            groups_.push_back(std::move(global_state));
            return;
        }

        // min(col) index-order shortcut: when the child yields rows in ascending
        // order of the aggregated column (e.g. an index range scan whose leading
        // column is col), the first visible matching row is the minimum, so we can
        // stop after consuming it instead of scanning the whole range.
        if (can_use_min_index_shortcut()) {
            for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
                TupleView tuple = prev_->current();
                std::unique_ptr<RmRecord> fallback;
                if (!tuple) {
                    fallback = prev_->Next();
                    if (fallback != nullptr) {
                        tuple = TupleView{fallback->data, static_cast<uint32_t>(fallback->size)};
                    }
                }
                if (!tuple) {
                    continue;
                }
                update_aggregate_state(global_state.aggregate_states[0], aggregates_[0], tuple);
                groups_.push_back(std::move(global_state));
                return;
            }
            // No visible rows: result is the zero/empty value.
            if (passes_having(global_state)) {
                groups_.push_back(std::move(global_state));
            }
            return;
        }

#ifdef RMDB_ENABLE_JIT
        if (jit_aggregate_ != nullptr && jit_aggregate_->valid()) {
            for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
                TupleView tuple = prev_->current();
                std::unique_ptr<RmRecord> fallback;
                if (!tuple) {
                    fallback = prev_->Next();
                    if (fallback != nullptr) {
                        tuple = TupleView{fallback->data, static_cast<uint32_t>(fallback->size)};
                    }
                }
                if (tuple) {
                    const auto status = jit_aggregate_->update(tuple.data, tuple.size);
                    if (status != jit::JitStatus::OK) {
                        jit_aggregate_.reset();
                        break;
                    }
                }
            }
            if (jit_aggregate_ != nullptr) {
                const auto& slots = jit_aggregate_->slots();
                for (size_t i = 0; i < aggregates_.size(); ++i) {
                    auto& state = global_state.aggregate_states[i];
                    const auto& slot = slots[i];
                    state.count = slot.count;
                    state.sum = slot.sum;
                    state.has_value = slot.has_value;
                    if (aggregates_[i].input_type == TYPE_INT) {
                        state.value.type = TYPE_INT;
                        state.value.int_val = static_cast<int>(slot.value);
                    } else {
                        state.value.type = TYPE_FLOAT;
                        state.value.float_val = slot.value;
                    }
                }
                if (passes_having(global_state)) {
                    groups_.push_back(std::move(global_state));
                }
                return;
            }
            global_state = make_group_state({});
        }
#endif

        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            TupleView tuple = prev_->current();
            std::unique_ptr<RmRecord> fallback;
            if (!tuple) {
                fallback = prev_->Next();
                if (fallback != nullptr) {
                    tuple = TupleView{fallback->data, static_cast<uint32_t>(fallback->size)};
                }
            }
            if (!tuple) {
                continue;
            }
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                update_aggregate_state(global_state.aggregate_states[i], aggregates_[i], tuple);
            }
        }

        if (passes_having(global_state)) {
            groups_.push_back(std::move(global_state));
        }
    }

    void init_group_cols(const std::vector<TabCol>& group_by_cols) {
        has_group_by_ = !group_by_cols.empty();
        for (const auto& group_by_col : group_by_cols) {
            ColMeta input_col = find_input_col(group_by_col);
            group_cols_.push_back(input_col);
            append_output_col(input_col, "");
        }
    }

    template <typename GroupByT> void init_group_cols(const std::vector<GroupByT>& group_by_cols) {
        std::vector<TabCol> cols;
        cols.reserve(group_by_cols.size());
        for (const auto& group_by_col : group_by_cols) {
            cols.push_back(extract_group_col(group_by_col));
        }
        init_group_cols(cols);
    }

    template <typename AggExprT> void init_aggregate_cols(const std::vector<AggExprT>& aggregate_exprs) {
        for (const auto& aggregate_expr : aggregate_exprs) {
            AggregateSpec spec = make_aggregate_spec(aggregate_expr);
            aggregates_.push_back(spec);

            ColMeta output_col;
            output_col.tab_name.clear();
            output_col.name = spec.output_name;
            switch (spec.type) {
            case LocalAggType::COUNT:
                output_col.type = TYPE_INT;
                output_col.len = static_cast<int>(sizeof(int));
                break;
            case LocalAggType::SUM:
                output_col.type = spec.input_type;
                output_col.len = spec.input_len;
                break;
            case LocalAggType::AVG:
                output_col.type = TYPE_FLOAT;
                output_col.len = static_cast<int>(sizeof(double));
                break;
            case LocalAggType::MAX:
            case LocalAggType::MIN:
                output_col.type = spec.input_type;
                output_col.len = spec.input_len;
                break;
            }
            append_output_col(output_col, output_col.name);
        }
    }

    template <typename HavingCondT> void init_having_conds(const std::vector<HavingCondT>& having_conds) {
        having_conds_.reserve(having_conds.size());
        for (const auto& having_cond : having_conds) {
            having_conds_.push_back(make_having_spec(having_cond));
        }
    }

public:
    template <typename GroupByT, typename AggExprT, typename HavingCondT>
    AggregateExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<GroupByT>& group_by_cols,
                      const std::vector<AggExprT>& aggregate_exprs, const std::vector<HavingCondT>& having_conds,
                      Context* context = nullptr)
        : prev_(std::move(prev)), context_(context) {
        init_group_cols(group_by_cols);
        init_aggregate_cols(aggregate_exprs);
        init_having_conds(having_conds);
#ifdef RMDB_ENABLE_JIT
        if (!has_group_by_ && having_conds_.empty() && !can_count_star_by_cursor_only() &&
            !can_use_min_index_shortcut() && jit::aggregate_jit_enabled()) {
            std::vector<jit::AggregateDescriptor> descriptors;
            descriptors.reserve(aggregates_.size());
            for (const auto& aggregate : aggregates_) {
                jit::AggregateOp op = jit::AggregateOp::COUNT;
                switch (aggregate.type) {
                case LocalAggType::COUNT:
                    op = jit::AggregateOp::COUNT;
                    break;
                case LocalAggType::SUM:
                    op = jit::AggregateOp::SUM;
                    break;
                case LocalAggType::MIN:
                    op = jit::AggregateOp::MIN;
                    break;
                case LocalAggType::MAX:
                    op = jit::AggregateOp::MAX;
                    break;
                case LocalAggType::AVG:
                    op = jit::AggregateOp::AVG;
                    break;
                }
                descriptors.push_back({op, aggregate.input_type, static_cast<uint32_t>(aggregate.input_col.offset),
                                       static_cast<uint32_t>(aggregate.input_col.len), aggregate.is_star});
            }
            jit_aggregate_ = std::make_unique<jit::AggregateKernel>(std::move(descriptors));
        }
#endif
    }

    void beginTuple() override {
        if (!materialized_) {
            materialize_groups();
            materialized_ = true;
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
        return materialize_group_result(groups_[cursor_]);
    }

    TupleView current() const override {
        if (is_end()) {
            return {};
        }
        // Aggregate results are materialized per call; current() keeps a
        // reusable compatibility buffer so downstream executors can borrow it.
        if (current_output_ == nullptr || current_output_->size != static_cast<int>(len_)) {
            current_output_ = std::make_unique<RmRecord>(static_cast<int>(len_));
        }
        auto result = materialize_group_result(groups_[cursor_]);
        memcpy(current_output_->data, result->data, len_);
        return TupleView{current_output_->data, static_cast<uint32_t>(len_)};
    }

    bool is_end() const override {
        return cursor_ >= groups_.size();
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    std::string getType() override {
        return "AggregateExecutor";
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    ColMeta get_col_offset(const TabCol& target) override {
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
};
