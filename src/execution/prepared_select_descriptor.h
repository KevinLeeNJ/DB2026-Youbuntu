/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "execution/aggregate_descriptor.h"
#include "execution/index_scan_descriptor.h"
#include "execution/prepared_parameter_binding.h"
#include "optimizer/plan.h"

class AbstractExecutor;
class Context;

struct PreparedIndexScanNode {
    IndexScanDescriptor descriptor;
};

struct PreparedFilterNode {
    std::vector<Condition> conditions;
};

struct PreparedAggregateNode {
    std::shared_ptr<const aggregate_execution::AggregateDescriptor> descriptor;
};

struct PreparedProjectionItem {
    QueryExpr expr;
    std::string display_name;
};

struct PreparedProjectionNode {
    bool preserve_column_names{false};
    std::vector<TabCol> columns;
    std::vector<PreparedProjectionItem> items;
};

using PreparedSelectNode =
    std::variant<PreparedIndexScanNode, PreparedFilterNode, PreparedAggregateNode, PreparedProjectionNode>;

// Generation-scoped immutable execution graph. Instantiate creates only
// request-local executors, conditions, parameter values, cursors and guards.
class PreparedSelectDescriptor {
public:
    static std::shared_ptr<const PreparedSelectDescriptor> Build(const Plan& plan, SmManager* sm_manager);

    bool Matches(const SmManager* sm_manager) const noexcept;
    std::unique_ptr<AbstractExecutor> Instantiate(const parser::OwnedTokenStream& lexical, SmManager* sm_manager,
                                                  Context* context) const;

    const std::vector<std::string>& output_names() const noexcept {
        return output_names_;
    }
    const std::vector<PreparedSelectNode>& nodes() const noexcept {
        return nodes_;
    }
    const PreparedParameterLayout& parameters() const noexcept {
        return parameters_;
    }

private:
    uint64_t catalog_generation_{0};
    std::vector<PreparedSelectNode> nodes_;
    std::vector<std::string> output_names_;
    PreparedParameterLayout parameters_;
};
