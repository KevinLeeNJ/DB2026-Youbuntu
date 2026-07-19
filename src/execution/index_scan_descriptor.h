/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "execution_defs.h"
#include "index/ix_scan.h"
#include "system/sm.h"

class IndexScanDescriptor {
public:
    struct ColumnAddress {
        int offset{0};
        int len{0};
        ColType type{TYPE_INT};
    };

    struct ConditionLayout {
        ColumnAddress lhs;
        ColumnAddress rhs;
    };

    struct CompiledIndexCondition {
        size_t index_col_ordinal;
        size_t condition_position;
        CompOp op;
        Value literal;
    };

    static IndexScanDescriptor Build(SmManager* sm_manager, std::string tab_name, std::vector<Condition> conditions,
                                     std::vector<std::string> index_col_names,
                                     ScanDirection direction = ScanDirection::Forward) {
        IndexScanDescriptor descriptor;
        descriptor.catalog_generation_ = sm_manager->get_catalog_generation();
        descriptor.tab_name_ = std::move(tab_name);
        descriptor.index_col_names_ = std::move(index_col_names);
        descriptor.direction_ = direction;

        auto& table = sm_manager->db_.get_table(descriptor.tab_name_);
        descriptor.cols_ = table.cols;
        descriptor.tuple_len_ = descriptor.cols_.back().offset + descriptor.cols_.back().len;
        descriptor.index_meta_ = *table.get_index_meta(descriptor.index_col_names_);
        descriptor.index_name_ =
            sm_manager->get_ix_manager()->get_index_name(descriptor.tab_name_, descriptor.index_meta_.cols);

        for (auto& condition : conditions) {
            if (condition.lhs_col.tab_name != descriptor.tab_name_) {
                assert(!condition.is_rhs_val && condition.rhs_col.tab_name == descriptor.tab_name_);
                std::swap(condition.lhs_col, condition.rhs_col);
                condition.op = swap_comp_op(condition.op);
            }
        }
        descriptor.conditions_ = std::move(conditions);

        descriptor.condition_layouts_.reserve(descriptor.conditions_.size());
        for (size_t condition_position = 0; condition_position < descriptor.conditions_.size(); ++condition_position) {
            const auto& condition = descriptor.conditions_[condition_position];
            ConditionLayout layout;
            layout.lhs = descriptor.resolve_column(condition.lhs_col);
            if (condition.is_rhs_val) {
                layout.rhs.type = condition.rhs_val.type;
            } else {
                layout.rhs = descriptor.resolve_column(condition.rhs_col);
            }
            descriptor.condition_layouts_.push_back(layout);
        }

        descriptor.compiled_index_conditions_.reserve(descriptor.conditions_.size());
        for (size_t condition_position = 0; condition_position < descriptor.conditions_.size(); ++condition_position) {
            const auto& condition = descriptor.conditions_[condition_position];
            if (!condition.is_rhs_val || condition.lhs_col.tab_name != descriptor.tab_name_ || condition.op == OP_NE) {
                continue;
            }
            auto index_col = std::find_if(descriptor.index_meta_.cols.begin(), descriptor.index_meta_.cols.end(),
                                          [&](const ColMeta& col) { return col.name == condition.lhs_col.col_name; });
            if (index_col == descriptor.index_meta_.cols.end()) {
                continue;
            }
            descriptor.compiled_index_conditions_.push_back(
                {static_cast<size_t>(index_col - descriptor.index_meta_.cols.begin()), condition_position, condition.op,
                 condition.rhs_val});
        }
        return descriptor;
    }

    uint64_t catalog_generation() const noexcept {
        return catalog_generation_;
    }
    const std::string& table_name() const noexcept {
        return tab_name_;
    }
    const std::vector<Condition>& conditions() const noexcept {
        return conditions_;
    }
    const std::vector<std::string>& index_column_names() const noexcept {
        return index_col_names_;
    }
    const IndexMeta& index_meta() const noexcept {
        return index_meta_;
    }
    const std::vector<ColMeta>& columns() const noexcept {
        return cols_;
    }
    size_t tuple_len() const noexcept {
        return tuple_len_;
    }
    const std::string& index_name() const noexcept {
        return index_name_;
    }
    ScanDirection direction() const noexcept {
        return direction_;
    }
    const std::vector<ConditionLayout>& condition_layouts() const noexcept {
        return condition_layouts_;
    }
    const std::vector<CompiledIndexCondition>& compiled_index_conditions() const noexcept {
        return compiled_index_conditions_;
    }

private:
    ColumnAddress resolve_column(const TabCol& target) const {
        auto column = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta& candidate) {
            return candidate.tab_name == target.tab_name && candidate.name == target.col_name;
        });
        if (column == cols_.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return ColumnAddress{column->offset, column->len, column->type};
    }

    uint64_t catalog_generation_{0};
    std::string tab_name_;
    std::vector<Condition> conditions_;
    std::vector<std::string> index_col_names_;
    IndexMeta index_meta_;
    std::vector<ColMeta> cols_;
    size_t tuple_len_{0};
    std::string index_name_;
    ScanDirection direction_{ScanDirection::Forward};
    std::vector<ConditionLayout> condition_layouts_;
    std::vector<CompiledIndexCondition> compiled_index_conditions_;
};
