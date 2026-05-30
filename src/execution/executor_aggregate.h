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
#include "executor_abstract.h"

class AggregateExecutor : public AbstractExecutor {
private:
    enum class LocalAggType { COUNT = 0, MAX = 1, MIN = 2, SUM = 3, AVG = 4 };
    enum class OperandKind { GROUP_COL, AGG_RESULT, VALUE };

    struct CellValue {
        ColType type = TYPE_INT;
        int int_val = 0;
        float float_val = 0.0f;
        std::string str_val;

        bool operator==(const CellValue& other) const {
            if (type != other.type) {
                return false;
            }
            switch (type) {
            case TYPE_INT:
                return int_val == other.int_val;
            case TYPE_FLOAT:
                return float_val == other.float_val;
            case TYPE_STRING:
                return str_val == other.str_val;
            }
            return false;
        }
    };

    struct CellValueHash {
        size_t operator()(const CellValue& value) const {
            size_t seed = std::hash<int>()(static_cast<int>(value.type));
            switch (value.type) {
            case TYPE_INT:
                seed ^= std::hash<int>()(value.int_val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                break;
            case TYPE_FLOAT:
                seed ^= std::hash<float>()(value.float_val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                break;
            case TYPE_STRING:
                seed ^= std::hash<std::string>()(value.str_val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                break;
            }
            return seed;
        }
    };

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
                seed ^= hash_cell(value) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
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

    template <typename T, typename = void> struct has_member_expr : std::false_type {};

    template <typename T>
    struct has_member_expr<T, std::void_t<decltype(std::declval<const T&>().expr)>> : std::true_type {};

    template <typename T, typename = void> struct has_member_agg : std::false_type {};

    template <typename T>
    struct has_member_agg<T, std::void_t<decltype(std::declval<const T&>().agg)>> : std::true_type {};

    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    std::vector<ColMeta> group_cols_;
    std::vector<AggregateSpec> aggregates_;
    std::vector<HavingSpec> having_conds_;
    std::vector<RmRecord> results_;
    size_t cursor_ = 0;
    bool materialized_ = false;
    bool has_group_by_ = false;

    static std::string trim_string(const char* data, int len) {
        return std::string(data, strnlen(data, len));
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
        CellValue value;
        value.type = col.type;
        const char* data = rec.data + col.offset;
        switch (col.type) {
        case TYPE_INT:
            value.int_val = *reinterpret_cast<const int*>(data);
            break;
        case TYPE_FLOAT:
            value.float_val = *reinterpret_cast<const float*>(data);
            break;
        case TYPE_STRING:
            value.str_val = trim_string(data, col.len);
            break;
        }
        return value;
    }

    static int compare_cells(const CellValue& lhs, const CellValue& rhs) {
        if (lhs.type == TYPE_INT && rhs.type == TYPE_FLOAT) {
            float lhs_val = static_cast<float>(lhs.int_val);
            if (lhs_val < rhs.float_val) {
                return -1;
            }
            if (lhs_val > rhs.float_val) {
                return 1;
            }
            return 0;
        }
        if (lhs.type == TYPE_FLOAT && rhs.type == TYPE_INT) {
            float rhs_val = static_cast<float>(rhs.int_val);
            if (lhs.float_val < rhs_val) {
                return -1;
            }
            if (lhs.float_val > rhs_val) {
                return 1;
            }
            return 0;
        }
        if (lhs.type != rhs.type) {
            throw IncompatibleTypeError(coltype2str(lhs.type), coltype2str(rhs.type));
        }
        switch (lhs.type) {
        case TYPE_INT:
            if (lhs.int_val < rhs.int_val) {
                return -1;
            }
            if (lhs.int_val > rhs.int_val) {
                return 1;
            }
            return 0;
        case TYPE_FLOAT:
            if (lhs.float_val < rhs.float_val) {
                return -1;
            }
            if (lhs.float_val > rhs.float_val) {
                return 1;
            }
            return 0;
        case TYPE_STRING:
            if (lhs.str_val < rhs.str_val) {
                return -1;
            }
            if (lhs.str_val > rhs.str_val) {
                return 1;
            }
            return 0;
        }
        throw InternalError("Unexpected cell type");
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
        CellValue value;
        value.type = type;
        if (type == TYPE_INT) {
            value.int_val = 0;
        } else if (type == TYPE_FLOAT) {
            value.float_val = 0.0f;
        }
        return value;
    }

    void write_cell(char* dest, const ColMeta& col, const CellValue& value) const {
        switch (col.type) {
        case TYPE_INT:
            *reinterpret_cast<int*>(dest + col.offset) = value.int_val;
            break;
        case TYPE_FLOAT:
            *reinterpret_cast<float*>(dest + col.offset) = value.float_val;
            break;
        case TYPE_STRING:
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
            ColMeta input_col = find_input_col(spec.col);
            spec.input_type = input_col.type;
            spec.input_len = input_col.len;
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
            operand.kind = OperandKind::VALUE;
            operand.literal = zero_value(expr.val.type);
            operand.literal.type = expr.val.type;
            if (expr.val.type == TYPE_INT) {
                operand.literal.int_val = expr.val.int_val;
            } else if (expr.val.type == TYPE_FLOAT) {
                operand.literal.float_val = expr.val.float_val;
            } else {
                operand.literal.str_val = expr.val.str_val;
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
        CellValue current_value;
        if (!spec.is_star) {
            current_value = read_cell(rec, find_input_col(spec.col));
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
                value.float_val = static_cast<float>(state.sum);
            }
            return value;
        }
        case LocalAggType::AVG: {
            CellValue value;
            value.type = TYPE_FLOAT;
            value.float_val =
                state.count == 0 ? 0.0f : static_cast<float>(state.sum / static_cast<double>(state.count));
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

    void emit_group_result(const GroupState& group_state) {
        std::vector<CellValue> aggregate_values;
        aggregate_values.reserve(aggregates_.size());
        for (size_t i = 0; i < aggregates_.size(); ++i) {
            aggregate_values.push_back(finalize_aggregate(aggregates_[i], group_state.aggregate_states[i]));
        }
        if (!passes_having(group_state.group_values, aggregate_values)) {
            return;
        }

        RmRecord rec(static_cast<int>(len_));
        std::memset(rec.data, 0, len_);
        for (size_t i = 0; i < group_state.group_values.size(); ++i) {
            write_cell(rec.data, cols_[i], group_state.group_values[i]);
        }
        for (size_t i = 0; i < aggregate_values.size(); ++i) {
            write_cell(rec.data, cols_[group_state.group_values.size() + i], aggregate_values[i]);
        }
        results_.push_back(rec);
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

    void materialize_results() {
        results_.clear();
        if (has_group_by_) {
            std::unordered_map<GroupKey, GroupState, GroupKeyHash> groups;
            std::vector<GroupKey> group_order;
            for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
                auto rec = prev_->Next();
                if (rec == nullptr) {
                    continue;
                }

                GroupKey key;
                key.values.reserve(group_cols_.size());
                for (const auto& col : group_cols_) {
                    key.values.push_back(read_cell(*rec, col));
                }

                auto [it, inserted] = groups.emplace(key, make_group_state(key.values));
                if (inserted) {
                    group_order.push_back(key);
                }
                for (size_t i = 0; i < aggregates_.size(); ++i) {
                    update_aggregate_state(it->second.aggregate_states[i], aggregates_[i], *rec);
                }
            }

            for (const auto& key : group_order) {
                emit_group_result(groups.at(key));
            }
            return;
        }

        GroupState global_state = make_group_state({});
        bool saw_input = false;
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto rec = prev_->Next();
            if (rec == nullptr) {
                continue;
            }
            saw_input = true;
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                update_aggregate_state(global_state.aggregate_states[i], aggregates_[i], *rec);
            }
        }

        if (saw_input || !aggregates_.empty() || !has_group_by_) {
            emit_group_result(global_state);
        }
    }

    void init_group_cols(const std::vector<TabCol>& group_by_cols) {
        has_group_by_ = !group_by_cols.empty();
        for (const auto& group_by_col : group_by_cols) {
            ColMeta input_col = find_input_col(group_by_col);
            group_cols_.push_back(input_col);
            append_output_col(input_col, input_col.name);
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
                output_col.len = static_cast<int>(sizeof(float));
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
                      const std::vector<AggExprT>& aggregate_exprs, const std::vector<HavingCondT>& having_conds)
        : prev_(std::move(prev)) {
        init_group_cols(group_by_cols);
        init_aggregate_cols(aggregate_exprs);
        init_having_conds(having_conds);
    }

    void beginTuple() override {
        if (!materialized_) {
            materialize_results();
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
        return std::make_unique<RmRecord>(results_[cursor_]);
    }

    bool is_end() const override {
        return cursor_ >= results_.size();
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
