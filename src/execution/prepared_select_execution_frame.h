/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/executor_filter.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_limit.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/parameter_frame.h"
#include "execution/prepared_execution_binding.h"

// Connection-local reusable executor state for the canonical prepared SELECT
// shape. The PreparedSelectExecutable metadata must outlive this frame.
class PreparedSelectExecutionFrame final {
public:
    class OperationLease final {
    public:
        OperationLease() = default;
        OperationLease(const OperationLease&) = delete;
        OperationLease& operator=(const OperationLease&) = delete;

        OperationLease(OperationLease&& other) noexcept : frame_(std::exchange(other.frame_, nullptr)) {}

        OperationLease& operator=(OperationLease&& other) noexcept {
            if (this != &other) {
                close();
                frame_ = std::exchange(other.frame_, nullptr);
            }
            return *this;
        }

        ~OperationLease() {
            close();
        }

        AbstractExecutor& root() const {
            assert(frame_ != nullptr);
            return *frame_->root_;
        }

        void close() noexcept {
            if (frame_ != nullptr) {
                frame_->close_operation();
                frame_ = nullptr;
            }
        }

    private:
        friend class PreparedSelectExecutionFrame;
        explicit OperationLease(PreparedSelectExecutionFrame* frame) noexcept : frame_(frame) {}

        PreparedSelectExecutionFrame* frame_ = nullptr;
    };

    static std::unique_ptr<PreparedSelectExecutionFrame> Build(const PreparedSelectExecutable& executable) {
        if (executable.scan.sm_manager == nullptr || executable.scan.table == nullptr ||
            executable.scan.table_handle == nullptr ||
            (executable.scan.uses_index &&
             (executable.scan.index == nullptr || executable.scan.index_handle == nullptr))) {
            return nullptr;
        }

        auto frame = std::unique_ptr<PreparedSelectExecutionFrame>(new PreparedSelectExecutionFrame());
        std::unique_ptr<AbstractExecutor> root;
        auto scan_conditions = copy_static_conditions(executable.scan.conditions);
        if (executable.scan.uses_index) {
            auto scan = std::make_unique<IndexScanExecutor>(
                executable.scan.sm_manager, executable.scan.table_name, *executable.scan.table,
                executable.scan.table_handle, *executable.scan.index, executable.scan.index_handle,
                executable.scan.index_name, std::move(scan_conditions), nullptr,
                executable.scan.scan_backward ? ScanDirection::Backward : ScanDirection::Forward);
            frame->add_condition_target(scan.get(), executable.scan.conditions);
            root = std::move(scan);
        } else {
            auto scan = std::make_unique<SeqScanExecutor>(executable.scan.sm_manager, executable.scan.table_name,
                                                          *executable.scan.table, executable.scan.table_handle,
                                                          std::move(scan_conditions), nullptr);
            frame->add_condition_target(scan.get(), executable.scan.conditions);
            root = std::move(scan);
        }

        for (const auto& layer : executable.layers) {
            switch (layer.kind) {
            case PreparedSelectLayerKind::Filter: {
                auto filter =
                    std::make_unique<FilterExecutor>(std::move(root), copy_static_conditions(layer.conditions));
                frame->add_condition_target(filter.get(), layer.conditions);
                root = std::move(filter);
                break;
            }
            case PreparedSelectLayerKind::Projection:
                root = std::make_unique<ProjectionExecutor>(std::move(root), layer.projection_ordinals,
                                                            layer.projection_names);
                break;
            case PreparedSelectLayerKind::Limit: {
                const size_t initial_limit = layer.limit < 0 ? 0 : static_cast<size_t>(layer.limit);
                const size_t initial_offset = layer.offset < 0 ? 0 : static_cast<size_t>(layer.offset);
                auto limit = std::make_unique<LimitExecutor>(std::move(root), initial_limit, initial_offset);
                frame->limit_targets_.push_back(LimitTarget{limit.get(), &layer});
                root = std::move(limit);
                break;
            }
            }
        }

        frame->root_ = std::move(root);
        frame->close_operation();
        return frame;
    }

    PreparedSelectExecutionFrame(const PreparedSelectExecutionFrame&) = delete;
    PreparedSelectExecutionFrame& operator=(const PreparedSelectExecutionFrame&) = delete;

    ~PreparedSelectExecutionFrame() {
        close_operation();
    }

    OperationLease begin_operation(const ParameterFrame& parameters, Context* context) {
        if (active_) {
            throw InternalError("prepared SELECT execution frame is already active");
        }
        if (context == nullptr) {
            throw InternalError("prepared SELECT execution frame requires a Context");
        }

        // Always dispose of stale cursor/page state and request-local pointers
        // before staging the next operation. Staging may throw, but it mutates
        // only the inactive side of each condition double buffer.
        close_operation();
        try {
            for (auto& target : condition_targets_) {
                stage_conditions(target, parameters);
            }
            for (auto& target : limit_targets_) {
                stage_limit(target, parameters);
            }
        } catch (...) {
            for (auto& target : condition_targets_) {
                clear_staged_parameters(target);
            }
            throw;
        }

        // All potentially throwing validation is complete. The remaining
        // swaps and lifecycle hooks are no-throw, so a node can never observe a
        // partially rebound operation.
        for (auto& target : condition_targets_) {
            target.apply(target.executor, target.staged_conditions);
        }
        for (auto& target : limit_targets_) {
            target.executor->replace_prepared_bounds(target.staged_limit, target.staged_offset);
        }
        root_->begin_operation(context);
        active_ = true;
        return OperationLease(this);
    }

private:
    using ApplyConditions = void (*)(AbstractExecutor*, std::vector<Condition>&) noexcept;
    using ClearParameters = void (*)(AbstractExecutor*, const std::vector<size_t>&) noexcept;

    struct ConditionTarget {
        AbstractExecutor* executor = nullptr;
        const std::vector<PreparedConditionBinding>* bindings = nullptr;
        std::vector<Condition> staged_conditions;
        std::vector<size_t> parameter_condition_indexes;
        ApplyConditions apply = nullptr;
        ClearParameters clear = nullptr;
    };

    struct LimitTarget {
        LimitExecutor* executor = nullptr;
        const PreparedSelectLayer* binding = nullptr;
        size_t staged_limit = 0;
        size_t staged_offset = 0;
    };

    PreparedSelectExecutionFrame() = default;

    static std::vector<Condition> copy_static_conditions(const std::vector<PreparedConditionBinding>& bindings) {
        std::vector<Condition> conditions;
        conditions.reserve(bindings.size());
        for (const auto& binding : bindings) {
            conditions.push_back(binding.condition);
        }
        return conditions;
    }

    template <typename Executor>
    static void apply_conditions(AbstractExecutor* executor, std::vector<Condition>& conditions) noexcept {
        static_cast<Executor*>(executor)->replace_prepared_conditions(conditions);
    }

    template <typename Executor>
    static void clear_parameters(AbstractExecutor* executor, const std::vector<size_t>& indexes) noexcept {
        static_cast<Executor*>(executor)->clear_prepared_parameters(indexes);
    }

    template <typename Executor>
    void add_condition_target(Executor* executor, const std::vector<PreparedConditionBinding>& bindings) {
        ConditionTarget target;
        target.executor = executor;
        target.bindings = &bindings;
        // Both sides of the double buffer use the executor's normalized
        // condition shape (notably the index scan's lhs/rhs normalization).
        // Per-operation rebinding changes only parameter Value objects.
        target.staged_conditions = executor->prepared_conditions_ref();
        target.parameter_condition_indexes.reserve(bindings.size());
        for (size_t i = 0; i < bindings.size(); ++i) {
            if (bindings[i].condition.is_rhs_val && bindings[i].condition.rhs_val.parameter_ordinal != 0) {
                target.parameter_condition_indexes.push_back(i);
            }
        }
        target.apply = &apply_conditions<Executor>;
        target.clear = &clear_parameters<Executor>;
        condition_targets_.push_back(std::move(target));
    }

    static void stage_conditions(ConditionTarget& target, const ParameterFrame& parameters) {
        assert(target.bindings != nullptr);
        assert(target.staged_conditions.size() == target.bindings->size());
        for (size_t index : target.parameter_condition_indexes) {
            const auto& binding = (*target.bindings)[index];
            Value bound_value =
                parameters.bind(binding.condition.rhs_val.parameter_ordinal, binding.condition.rhs_val.type);
            if (!bound_value.is_null) {
                bound_value.init_raw(binding.rhs_raw_length);
            }
            target.staged_conditions[index].rhs_val = std::move(bound_value);
        }
    }

    static void stage_limit(LimitTarget& target, const ParameterFrame& parameters) {
        assert(target.binding != nullptr);
        int runtime_limit = target.binding->limit;
        int runtime_offset = target.binding->offset;
        if (target.binding->limit_parameter_ordinal != 0) {
            const Value value = parameters.bind(target.binding->limit_parameter_ordinal, TYPE_INT);
            if (value.is_null || value.int_val < 0) {
                throw RMDBError("LIMIT must be a non-NULL, non-negative INT32");
            }
            runtime_limit = value.int_val;
        }
        if (target.binding->offset_parameter_ordinal != 0) {
            const Value value = parameters.bind(target.binding->offset_parameter_ordinal, TYPE_INT);
            if (value.is_null || value.int_val < 0) {
                throw RMDBError("OFFSET must be a non-NULL, non-negative INT32");
            }
            runtime_offset = value.int_val;
        }
        if (runtime_limit < 0 || runtime_offset < 0 ||
            runtime_limit > std::numeric_limits<int>::max() - runtime_offset) {
            throw RMDBError("LIMIT plus OFFSET exceeds INT32 range");
        }
        target.staged_limit = static_cast<size_t>(runtime_limit);
        target.staged_offset = static_cast<size_t>(runtime_offset);
    }

    void close_operation() noexcept {
        if (root_ != nullptr) {
            // Close every cursor before clearing the condition values which SSI
            // and index bounds may have referenced during this operation.
            root_->end_operation();
        }
        for (auto& target : condition_targets_) {
            target.clear(target.executor, target.parameter_condition_indexes);
            clear_staged_parameters(target);
        }
        active_ = false;
    }

    static void clear_staged_parameters(ConditionTarget& target) noexcept {
        for (size_t index : target.parameter_condition_indexes) {
            if (index >= target.staged_conditions.size()) {
                continue;
            }
            Value& value = target.staged_conditions[index].rhs_val;
            value.raw.reset();
            value.parameter_ordinal = 0;
            value.is_null = true;
            value.int_val = 0;
            if (value.str_val.capacity() > kRetainedParameterBytes) {
                std::string{}.swap(value.str_val);
            } else {
                value.str_val.clear();
            }
        }
    }

    static constexpr size_t kRetainedParameterBytes = 256;

    std::unique_ptr<AbstractExecutor> root_;
    std::vector<ConditionTarget> condition_targets_;
    std::vector<LimitTarget> limit_targets_;
    bool active_ = false;
};
