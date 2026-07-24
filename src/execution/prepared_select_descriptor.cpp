/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "execution/prepared_select_descriptor.h"

#include <algorithm>
#include <atomic>
#include <mutex>

#include "execution/executor_aggregate.h"
#include "execution/executor_filter.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_projection.h"

namespace {

uint32_t condition_max_length(const Condition& condition, const std::vector<ColMeta>& columns) {
    auto found = std::find_if(columns.begin(), columns.end(), [&](const ColMeta& column) {
        return column.tab_name == condition.lhs_col.tab_name && column.name == condition.lhs_col.col_name;
    });
    return found == columns.end() ? 0 : static_cast<uint32_t>(found->len);
}

bool register_conditions(const std::vector<Condition>& conditions, const std::vector<ColMeta>& columns,
                         PreparedParameterLayout* parameters) {
    for (const auto& condition : conditions) {
        if (condition.is_rhs_val &&
            !parameters->Register(condition.rhs_val, condition_max_length(condition, columns))) {
            return false;
        }
    }
    return true;
}

const ColMeta* resolve_projection_column(const std::vector<ColMeta>& columns, const TabCol& target) {
    const auto found = std::find_if(columns.begin(), columns.end(), [&](const ColMeta& column) {
        return column.tab_name == target.tab_name && column.name == target.col_name;
    });
    return found == columns.end() ? nullptr : &*found;
}

std::optional<PreparedSelectPipelineLayout> build_pipeline_layout(const std::vector<ColMeta>& input_columns,
                                                                  const PreparedProjectionNode& projection) {
    const size_t output_count = projection.preserve_column_names ? projection.columns.size() : projection.items.size();
    if (output_count == 0) {
        return std::nullopt;
    }

    PreparedSelectPipelineLayout layout;
    layout.output_columns.reserve(output_count);
    for (size_t i = 0; i < output_count; ++i) {
        const TabCol& target = projection.preserve_column_names ? projection.columns[i] : projection.items[i].expr.col;
        const auto* source = resolve_projection_column(input_columns, target);
        if (source == nullptr) {
            return std::nullopt;
        }

        auto output = *source;
        if (!projection.preserve_column_names) {
            output.name = projection.items[i].display_name;
            output.tab_name.clear();
        }
        output.offset =
            layout.output_columns.empty() ? 0 : layout.output_columns.back().offset + layout.output_columns.back().len;
        layout.output_columns.push_back(std::move(output));
    }
    return layout;
}

std::vector<std::string> projection_output_names(const ProjectionPlan& projection) {
    if (!projection.output_names_.empty()) {
        return projection.output_names_;
    }
    std::vector<std::string> names;
    names.reserve(projection.select_items_.size());
    for (const auto& item : projection.select_items_) {
        if (!item.output_name.empty()) {
            names.push_back(item.output_name);
        } else if (!item.alias.empty()) {
            names.push_back(item.alias);
        } else if (!item.expr.display_name.empty()) {
            names.push_back(item.expr.display_name);
        } else {
            names.push_back(item.expr.col.col_name);
        }
    }
    return names;
}

} // namespace

class PreparedSelectExecutorChain {
public:
    static std::unique_ptr<PreparedSelectExecutorChain> Build(const std::vector<PreparedSelectNode>& nodes,
                                                              SmManager* sm_manager) {
        auto chain = std::make_unique<PreparedSelectExecutorChain>();
        std::unique_ptr<AbstractExecutor> executor;
        for (const auto& node : nodes) {
            if (const auto* scan = std::get_if<PreparedIndexScanNode>(&node)) {
                auto next = std::make_unique<IndexScanExecutor>(sm_manager, scan->descriptor, nullptr);
                chain->scan_ = next.get();
                executor = std::move(next);
            } else if (const auto* filter = std::get_if<PreparedFilterNode>(&node)) {
                if (executor == nullptr) {
                    throw InternalError("prepared filter has no child executor");
                }
                auto next = std::make_unique<FilterExecutor>(std::move(executor), filter->conditions, true);
                chain->filter_ = next.get();
                executor = std::move(next);
            } else if (const auto* aggregate = std::get_if<PreparedAggregateNode>(&node)) {
                if (executor == nullptr || aggregate->descriptor == nullptr) {
                    throw InternalError("prepared aggregate has no descriptor or child executor");
                }
                auto next = std::make_unique<AggregateExecutor>(std::move(executor), aggregate->descriptor, nullptr);
                chain->aggregate_ = next.get();
                executor = std::move(next);
            } else if (const auto* projection = std::get_if<PreparedProjectionNode>(&node)) {
                if (executor == nullptr) {
                    throw InternalError("prepared projection has no child executor");
                }
                if (projection->preserve_column_names) {
                    auto next = std::make_unique<ProjectionExecutor>(std::move(executor), projection->columns);
                    chain->projection_ = next.get();
                    executor = std::move(next);
                } else {
                    auto next = std::make_unique<ProjectionExecutor>(std::move(executor), projection->items);
                    chain->projection_ = next.get();
                    executor = std::move(next);
                }
            }
        }
        if (chain->scan_ == nullptr || executor == nullptr) {
            throw InternalError("prepared SELECT executor chain is incomplete");
        }
        chain->root_ = std::move(executor);
        return chain;
    }

    bool Reset(compiled::ParameterFrame frame, const PreparedParameterLayout& parameters, Context* context) {
        frame_ = std::move(frame);
        if (!scan_->ResetPreparedRequest(parameters, *frame_, context)) {
            return false;
        }
        if (filter_ != nullptr && !filter_->ResetPreparedRequest(parameters, *frame_, context)) {
            return false;
        }
        if (aggregate_ != nullptr) {
            aggregate_->ResetPreparedRequest(context);
        }
        if (projection_ != nullptr) {
            projection_->ResetPreparedRequest(context);
        }
        return true;
    }

    void ResetForPool() noexcept {
        if (projection_ != nullptr) {
            projection_->ResetForPreparedPool();
        }
        if (aggregate_ != nullptr) {
            aggregate_->ResetForPreparedPool();
        }
        if (filter_ != nullptr) {
            filter_->ResetForPreparedPool();
        }
        scan_->ResetForPreparedPool();
        frame_.reset();
    }

    AbstractExecutor* root() const noexcept {
        return root_.get();
    }

    AbstractExecutor* pipeline_input() const noexcept {
        return filter_ != nullptr ? static_cast<AbstractExecutor*>(filter_) : static_cast<AbstractExecutor*>(scan_);
    }

    ProjectionExecutor* pipeline_projection() const noexcept {
        return projection_;
    }

private:
    std::unique_ptr<AbstractExecutor> root_;
    IndexScanExecutor* scan_{nullptr};
    FilterExecutor* filter_{nullptr};
    AggregateExecutor* aggregate_{nullptr};
    ProjectionExecutor* projection_{nullptr};
    std::optional<compiled::ParameterFrame> frame_;
};

class PreparedSelectExecutorPool {
public:
    PreparedSelectExecutorPool() {
        available_.reserve(kMaxAvailableChains);
    }

    std::unique_ptr<PreparedSelectExecutorChain> Acquire(const std::vector<PreparedSelectNode>& nodes,
                                                         SmManager* sm_manager) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!available_.empty()) {
                auto chain = std::move(available_.back());
                available_.pop_back();
                reused_.fetch_add(1, std::memory_order_relaxed);
                return chain;
            }
        }
        auto chain = PreparedSelectExecutorChain::Build(nodes, sm_manager);
        constructed_.fetch_add(1, std::memory_order_relaxed);
        return chain;
    }

    void Release(std::unique_ptr<PreparedSelectExecutorChain> chain) noexcept {
        if (chain == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (available_.size() < kMaxAvailableChains) {
            available_.push_back(std::move(chain));
        }
    }

    PreparedSelectPoolStats Stats() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return {constructed_.load(std::memory_order_relaxed), reused_.load(std::memory_order_relaxed),
                static_cast<uint64_t>(available_.size())};
    }

private:
    static constexpr size_t kMaxAvailableChains = 16;
    mutable std::mutex mutex_;
    std::vector<std::unique_ptr<PreparedSelectExecutorChain>> available_;
    std::atomic<uint64_t> constructed_{0};
    std::atomic<uint64_t> reused_{0};
};

namespace {

class PreparedSelectPipelineLeaseImpl final : public PreparedSelectPipelineLease {
public:
    explicit PreparedSelectPipelineLeaseImpl(std::shared_ptr<PreparedSelectExecutorPool> pool)
        : pool_(std::move(pool)) {}

    void Attach(std::unique_ptr<PreparedSelectExecutorChain> chain) noexcept {
        chain_ = std::move(chain);
    }

    ~PreparedSelectPipelineLeaseImpl() override {
        if (chain_ != nullptr) {
            chain_->ResetForPool();
            pool_->Release(std::move(chain_));
        }
    }

    AbstractExecutor& input() noexcept override {
        return *chain_->pipeline_input();
    }

    ProjectionExecutor& projection() noexcept override {
        return *chain_->pipeline_projection();
    }

private:
    std::shared_ptr<PreparedSelectExecutorPool> pool_;
    std::unique_ptr<PreparedSelectExecutorChain> chain_;
};

class PreparedSelectExecutorLease final : public AbstractExecutor {
public:
    PreparedSelectExecutorLease(std::shared_ptr<PreparedSelectExecutorPool> pool, Context* context)
        : pool_(std::move(pool)) {
        context_ = context;
    }

    void Attach(std::unique_ptr<PreparedSelectExecutorChain> chain) noexcept {
        chain_ = std::move(chain);
    }

    ~PreparedSelectExecutorLease() override {
        if (chain_ != nullptr) {
            chain_->ResetForPool();
            pool_->Release(std::move(chain_));
        }
    }

    size_t tupleLen() const override {
        return root()->tupleLen();
    }
    const std::vector<ColMeta>& cols() const override {
        return root()->cols();
    }
    std::string getType() override {
        return root()->getType();
    }
    void beginTuple() override {
        root()->beginTuple();
    }
    void nextTuple() override {
        root()->nextTuple();
    }
    bool is_end() const override {
        return root()->is_end();
    }
    Rid& rid() override {
        return root()->rid();
    }
    std::unique_ptr<RmRecord> Next() override {
        return root()->Next();
    }
    TupleView current() const override {
        return root()->current();
    }
    ColMeta get_col_offset(const TabCol& target) override {
        return root()->get_col_offset(target);
    }
    void set_counting_enabled(bool enabled) override {
        root()->set_counting_enabled(enabled);
    }
    void set_key_conditions(std::vector<Condition> key_conds) override {
        root()->set_key_conditions(std::move(key_conds));
    }
    void bind_lookup_key(const TabCol& target, LookupKeyView key) override {
        root()->bind_lookup_key(target, key);
    }
    std::string scan_table_name() const override {
        return root()->scan_table_name();
    }
    std::string_view scan_table_name_view() const override {
        return root()->scan_table_name_view();
    }
    std::vector<Condition> scan_conditions() const override {
        return root()->scan_conditions();
    }
    const std::vector<Condition>& scan_conditions_ref() const override {
        return root()->scan_conditions_ref();
    }
    void record_current_read_for_ssi() override {
        root()->record_current_read_for_ssi();
    }
    bool provides_min_order(const TabCol& col) const override {
        return root()->provides_min_order(col);
    }
    uint64_t catalog_generation() const override {
        return root()->catalog_generation();
    }

private:
    AbstractExecutor* root() const noexcept {
        return chain_->root();
    }

    std::shared_ptr<PreparedSelectExecutorPool> pool_;
    std::unique_ptr<PreparedSelectExecutorChain> chain_;
};

} // namespace

std::shared_ptr<const PreparedSelectDescriptor> PreparedSelectDescriptor::Build(const Plan& plan,
                                                                                SmManager* sm_manager) {
    if (sm_manager == nullptr || plan.tag != T_select) {
        return nullptr;
    }
    const auto& select = static_cast<const DMLPlan&>(plan);
    if (select.subplan_ == nullptr || select.subplan_->tag != T_Projection) {
        return nullptr;
    }
    const auto& projection = static_cast<const ProjectionPlan&>(*select.subplan_);
    const Plan* child = projection.subplan_.get();
    if (child == nullptr) {
        return nullptr;
    }

    const AggregatePlan* aggregate = nullptr;
    if (child->tag == T_Aggregate) {
        aggregate = static_cast<const AggregatePlan*>(child);
        child = aggregate->subplan_.get();
    }

    const FilterPlan* filter = nullptr;
    if (child->tag == T_Filter) {
        filter = static_cast<const FilterPlan*>(child);
        child = filter->subplan_.get();
    }
    if (child == nullptr || child->tag != T_IndexScan) {
        return nullptr;
    }
    const auto& scan = static_cast<const ScanPlan&>(*child);

    PreparedProjectionNode projection_node;
    projection_node.preserve_column_names = projection.preserve_col_names_;
    projection_node.columns.reserve(projection.select_items_.size());
    projection_node.items.reserve(projection.select_items_.size());
    for (const auto& item : projection.select_items_) {
        if (item.expr.type != QueryExprType::COLUMN &&
            (aggregate == nullptr || item.expr.type != QueryExprType::AGGREGATE)) {
            return nullptr;
        }
        if (projection.preserve_col_names_) {
            if (item.expr.type != QueryExprType::COLUMN) {
                return nullptr;
            }
            projection_node.columns.push_back(item.expr.col);
        } else {
            projection_node.items.push_back(PreparedProjectionItem{
                item.expr, !item.output_name.empty() ? item.output_name
                                                     : (!item.alias.empty() ? item.alias : item.expr.display_name)});
        }
    }

    auto descriptor = std::shared_ptr<PreparedSelectDescriptor>(new PreparedSelectDescriptor());
    descriptor->owner_ = sm_manager;
    descriptor->catalog_generation_ = sm_manager->get_catalog_generation();
    auto scan_descriptor =
        IndexScanDescriptor::Build(sm_manager, scan.tab_name_, scan.conds_, scan.index_col_names_,
                                   scan.scan_backward_ ? ScanDirection::Backward : ScanDirection::Forward);
    const auto scan_columns = scan_descriptor.columns();
    if (!register_conditions(scan_descriptor.conditions(), scan_columns, &descriptor->parameters_)) {
        return nullptr;
    }
    descriptor->nodes_.push_back(PreparedIndexScanNode{std::move(scan_descriptor)});
    if (filter != nullptr) {
        if (!register_conditions(filter->conds_, scan_columns, &descriptor->parameters_)) {
            return nullptr;
        }
        descriptor->nodes_.push_back(PreparedFilterNode{filter->conds_});
    }
    if (aggregate != nullptr) {
        // HAVING literals are part of the normalized statement parameter set,
        // but AggregateDescriptor intentionally contains finalized CellValues.
        // Until HAVING gains request-local value slots, reject that capability
        // instead of freezing the first execution's literal into shared state.
        for (const auto& condition : aggregate->having_conds_) {
            if (condition.is_rhs_val && condition.rhs_val.lexical_slot >= 0) {
                return nullptr;
            }
        }
        auto aggregate_descriptor = AggregateExecutor::BuildDescriptor(scan_columns, aggregate->group_by_cols_,
                                                                       aggregate->agg_exprs_, aggregate->having_conds_);
        descriptor->nodes_.push_back(PreparedAggregateNode{std::move(aggregate_descriptor)});
    }
    descriptor->nodes_.push_back(std::move(projection_node));
    descriptor->output_names_ = projection_output_names(projection);
    descriptor->executor_pool_ = std::make_shared<PreparedSelectExecutorPool>();

    const auto* prepared_projection = std::get_if<PreparedProjectionNode>(&descriptor->nodes_.back());
    if (aggregate == nullptr && prepared_projection != nullptr) {
        auto pipeline_layout = build_pipeline_layout(scan_columns, *prepared_projection);
        if (pipeline_layout.has_value()) {
            descriptor->pipeline_layout_ = std::move(*pipeline_layout);
        }
    }
    return descriptor;
}

bool PreparedSelectDescriptor::Matches(const SmManager* sm_manager) const noexcept {
    return sm_manager != nullptr && sm_manager == owner_ && catalog_generation_ == sm_manager->get_catalog_generation();
}

PreparedSelectPoolStats PreparedSelectDescriptor::pool_stats() const noexcept {
    return executor_pool_ == nullptr ? PreparedSelectPoolStats{} : executor_pool_->Stats();
}

std::unique_ptr<PreparedSelectPipelineLease>
PreparedSelectDescriptor::AcquirePipelineScan(const parser::OwnedTokenStream& lexical, SmManager* sm_manager,
                                              Context* context) const {
    if (!Matches(sm_manager) || context == nullptr || executor_pool_ == nullptr || !pipeline_layout_.has_value()) {
        return nullptr;
    }
    auto frame = parameters_.Bind(lexical);
    if (!frame.has_value()) {
        return nullptr;
    }

    auto chain = executor_pool_->Acquire(nodes_, sm_manager);
    try {
        if (!chain->Reset(std::move(*frame), parameters_, context)) {
            chain->ResetForPool();
            executor_pool_->Release(std::move(chain));
            return nullptr;
        }
        auto lease = std::make_unique<PreparedSelectPipelineLeaseImpl>(executor_pool_);
        lease->Attach(std::move(chain));
        return lease;
    } catch (...) {
        chain->ResetForPool();
        executor_pool_->Release(std::move(chain));
        throw;
    }
}

std::unique_ptr<AbstractExecutor> PreparedSelectDescriptor::Instantiate(const parser::OwnedTokenStream& lexical,
                                                                        SmManager* sm_manager, Context* context) const {
    if (!Matches(sm_manager)) {
        return nullptr;
    }
    auto frame = parameters_.Bind(lexical);
    if (!frame.has_value()) {
        return nullptr;
    }

    if (executor_pool_ == nullptr) {
        return nullptr;
    }
    auto chain = executor_pool_->Acquire(nodes_, sm_manager);
    bool reset = false;
    try {
        reset = chain->Reset(std::move(*frame), parameters_, context);
    } catch (...) {
        chain->ResetForPool();
        executor_pool_->Release(std::move(chain));
        throw;
    }
    if (!reset) {
        chain->ResetForPool();
        executor_pool_->Release(std::move(chain));
        return nullptr;
    }
    std::unique_ptr<PreparedSelectExecutorLease> lease;
    try {
        lease = std::make_unique<PreparedSelectExecutorLease>(executor_pool_, context);
    } catch (...) {
        chain->ResetForPool();
        executor_pool_->Release(std::move(chain));
        throw;
    }
    lease->Attach(std::move(chain));
    return lease;
}
