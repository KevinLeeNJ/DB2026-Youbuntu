/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/common.h"
#include "execution_defs.h"
#include "execution_scalar.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

class AggregateExecutor;

namespace aggregate_execution {

enum class AggregateType { COUNT = 0, MAX = 1, MIN = 2, SUM = 3, AVG = 4 };
enum class HavingOperandKind { GROUP_COL, AGG_RESULT, VALUE };

struct AggregateSpec {
    AggregateType type = AggregateType::COUNT;
    bool is_star = false;
    TabCol col;
    std::string output_name;
    ColType input_type = TYPE_INT;
    int input_len = static_cast<int>(sizeof(int));
    ColMeta input_col;
};

struct HavingOperand {
    HavingOperandKind kind = HavingOperandKind::VALUE;
    size_t index = 0;
    execution_scalar::CellValue literal;
};

struct HavingSpec {
    HavingOperand lhs;
    CompOp op = OP_EQ;
    HavingOperand rhs;
};

// Generation-independent, immutable execution metadata. It contains no child
// executor, transaction, cursor, page guard, or catalog handle.
class AggregateDescriptor {
public:
    AggregateDescriptor() = default;

    AggregateDescriptor(std::vector<ColMeta> output_cols, size_t output_len, std::vector<ColMeta> group_cols,
                        std::vector<AggregateSpec> aggregates, std::vector<HavingSpec> having)
        : output_cols_(std::move(output_cols)), output_len_(output_len), group_cols_(std::move(group_cols)),
          aggregates_(std::move(aggregates)), having_(std::move(having)) {}

    const std::vector<ColMeta>& output_cols() const noexcept {
        return output_cols_;
    }
    size_t output_len() const noexcept {
        return output_len_;
    }
    const std::vector<ColMeta>& group_cols() const noexcept {
        return group_cols_;
    }
    const std::vector<AggregateSpec>& aggregates() const noexcept {
        return aggregates_;
    }
    const std::vector<HavingSpec>& having() const noexcept {
        return having_;
    }
    bool has_group_by() const noexcept {
        return !group_cols_.empty();
    }

private:
    friend class ::AggregateExecutor;

    std::vector<ColMeta> output_cols_;
    size_t output_len_{0};
    std::vector<ColMeta> group_cols_;
    std::vector<AggregateSpec> aggregates_;
    std::vector<HavingSpec> having_;
};

struct AggregateValueState {
    int64_t count = 0;
    double sum = 0.0;
    bool has_value = false;
    execution_scalar::CellValue value;
};

struct AggregateGroupState {
    std::vector<execution_scalar::CellValue> group_values;
    std::vector<AggregateValueState> aggregate_states;
};

// All mutable state is request-owned so an AggregateDescriptor can be shared
// by prepared execution graphs without sharing cursors or intermediate rows.
class AggregateRequestState {
private:
    friend class ::AggregateExecutor;

    std::vector<AggregateGroupState> groups_;
    size_t cursor_{0};
    bool materialized_{false};
    mutable std::unique_ptr<RmRecord> current_output_;
};

} // namespace aggregate_execution
