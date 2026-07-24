/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "execution/runtime/prepared_select_pipeline.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "common/context.h"
#include "execution/executor_projection.h"
#include "execution/result_sink.h"

namespace prepared_select_pipeline {
namespace {

bool is_enabled_value(const char* value) noexcept {
    return value != nullptr && std::strcmp(value, "1") == 0;
}

const PreparedProjectionNode* projection_node(const PreparedSelectDescriptor& descriptor) noexcept {
    if (descriptor.nodes().empty()) {
        return nullptr;
    }
    return std::get_if<PreparedProjectionNode>(&descriptor.nodes().back());
}

class SsiReadTrackingGuard {
public:
    explicit SsiReadTrackingGuard(Context* context) : context_(context) {
        if (context_ != nullptr) {
            old_value_ = context_->enable_ssi_read_tracking_;
            context_->enable_ssi_read_tracking_ = true;
        }
    }

    ~SsiReadTrackingGuard() {
        if (context_ != nullptr) {
            context_->enable_ssi_read_tracking_ = old_value_;
        }
    }

    SsiReadTrackingGuard(const SsiReadTrackingGuard&) = delete;
    SsiReadTrackingGuard& operator=(const SsiReadTrackingGuard&) = delete;

private:
    Context* context_;
    bool old_value_{false};
};

} // namespace

bool Enabled() noexcept {
    return is_enabled_value(std::getenv("ENABLE_PREPARED_SELECT_PIPELINE"));
}

bool IsEligible(const PreparedSelectDescriptor& descriptor) noexcept {
    const auto& nodes = descriptor.nodes();
    if ((nodes.size() != 2 && nodes.size() != 3) || !std::holds_alternative<PreparedIndexScanNode>(nodes[0]) ||
        !std::holds_alternative<PreparedProjectionNode>(nodes.back())) {
        return false;
    }
    if (nodes.size() == 3 && !std::holds_alternative<PreparedFilterNode>(nodes[1])) {
        return false;
    }

    const auto* projection = projection_node(descriptor);
    if (projection->preserve_column_names) {
        return !projection->columns.empty() && projection->items.empty();
    }
    return projection->columns.empty() && !projection->items.empty() &&
           std::all_of(projection->items.begin(), projection->items.end(),
                       [](const PreparedProjectionItem& item) { return item.expr.type == QueryExprType::COLUMN; });
}

RunStatus Run(const PreparedSelectDescriptor& descriptor, const parser::OwnedTokenStream& lexical,
              SmManager* sm_manager, Context* context) {
    if (!Enabled() || sm_manager == nullptr || context == nullptr || !descriptor.Matches(sm_manager) ||
        !IsEligible(descriptor)) {
        return RunStatus::FALLBACK;
    }

    if (!descriptor.pipeline_layout().has_value()) {
        return RunStatus::FALLBACK;
    }
    const auto& layout = *descriptor.pipeline_layout();
    if (layout.output_columns.empty()) {
        return RunStatus::FALLBACK;
    }

    auto lease = descriptor.AcquirePipelineScan(lexical, sm_manager, context);
    if (lease == nullptr) {
        return RunStatus::FALLBACK;
    }

    // Reuse the complete prepared chain so index visibility, filter predicate
    // evaluation, SSI tracking and projection storage keep their existing
    // semantics and request-local reset lifecycle.
    auto& input = lease->input();
    auto& projection_executor = lease->projection();
    SsiReadTrackingGuard ssi_read_tracking_guard(context);
    ResultSink result_sink(sm_manager, context, layout.output_columns, descriptor.output_names());

    input.beginTuple();
    while (!input.is_end()) {
        const TupleView input_tuple = input.current();
        if (!input_tuple) {
            throw InternalError("prepared SELECT pipeline produced an empty tuple");
        }
        const TupleView projected_tuple = projection_executor.ProjectForPipeline(input_tuple);
        if (!projected_tuple) {
            throw InternalError("prepared SELECT pipeline projection produced an empty tuple");
        }
        result_sink.Emit(projected_tuple);
        input.nextTuple();
    }
    result_sink.Finish();
    return RunStatus::HANDLED;
}

} // namespace prepared_select_pipeline
