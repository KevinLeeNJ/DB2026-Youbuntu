/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "cache/statement_template_cache.h"

#include <algorithm>

namespace cache {
namespace {

class LiteralBinder {
public:
    explicit LiteralBinder(const parser::OwnedTokenStream& stream)
        : parameters_(stream.parameters), used_(stream.parameters.size(), false) {}

    bool bind(ast::Value& value) {
        const auto* parameter = take(value.parameter_slot);
        if (parameter == nullptr)
            return false;
        switch (value.type) {
        case ast::AstType::IntLit:
            if (parameter->type != parser::TokenType::VALUE_INT)
                return false;
            static_cast<ast::IntLit&>(value).val = static_cast<int>(parameter->int_value);
            break;
        case ast::AstType::FloatLit:
            if (parameter->type != parser::TokenType::VALUE_FLOAT && parameter->type != parser::TokenType::VALUE_INT) {
                return false;
            }
            static_cast<ast::FloatLit&>(value).val = parameter->type == parser::TokenType::VALUE_INT
                                                         ? static_cast<double>(parameter->int_value)
                                                         : parameter->float_value;
            break;
        case ast::AstType::StringLit:
            if (parameter->type != parser::TokenType::VALUE_STRING)
                return false;
            static_cast<ast::StringLit&>(value).val = parameter->text;
            break;
        case ast::AstType::BoolLit:
            if (parameter->type != parser::TokenType::VALUE_BOOL)
                return false;
            static_cast<ast::BoolLit&>(value).val = parameter->bool_value;
            break;
        default:
            return false;
        }
        value.display_text = parameter->text;
        return true;
    }

    bool bind(Value& value) {
        const auto* parameter = take(value.lexical_slot);
        if (parameter == nullptr)
            return false;
        switch (value.type) {
        case TYPE_INT:
            if (parameter->type != parser::TokenType::VALUE_INT && parameter->type != parser::TokenType::VALUE_FLOAT)
                return false;
            value.int_val = parameter->type == parser::TokenType::VALUE_INT ? static_cast<int>(parameter->int_value)
                                                                            : static_cast<int>(parameter->float_value);
            break;
        case TYPE_FLOAT:
            if (parameter->type != parser::TokenType::VALUE_FLOAT && parameter->type != parser::TokenType::VALUE_INT) {
                return false;
            }
            value.float_val = parameter->type == parser::TokenType::VALUE_INT
                                  ? static_cast<double>(parameter->int_value)
                                  : parameter->float_value;
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            if (parameter->type != parser::TokenType::VALUE_STRING)
                return false;
            value.str_val = parameter->text;
            break;
        default:
            return false;
        }
        value.raw.reset();
        return true;
    }

    bool done() const {
        return std::all_of(used_.begin(), used_.end(), [](bool used) { return used; });
    }

private:
    const parser::LexicalParam* take(int slot) {
        size_t index;
        if (slot >= 0) {
            index = static_cast<size_t>(slot);
        } else {
            while (position_ < used_.size() && used_[position_])
                ++position_;
            index = position_++;
        }
        if (index >= parameters_.size())
            return nullptr;
        used_[index] = true;
        return &parameters_[index];
    }

    const std::vector<parser::LexicalParam>& parameters_;
    size_t position_{0};
    std::vector<bool> used_;
};

bool bind_expr(ast::Expr& expression, LiteralBinder& binder);

bool bind_binary(ast::BinaryExpr& expression, LiteralBinder& binder) {
    return bind_expr(*expression.lhs, binder) && bind_expr(*expression.rhs, binder);
}

bool bind_expr(ast::Expr& expression, LiteralBinder& binder) {
    switch (expression.type) {
    case ast::AstType::IntLit:
    case ast::AstType::FloatLit:
    case ast::AstType::StringLit:
    case ast::AstType::BoolLit:
        return binder.bind(static_cast<ast::Value&>(expression));
    case ast::AstType::Col:
        return true;
    case ast::AstType::AggExpr:
        return true;
    default:
        return false;
    }
}

bool bind_tree(ast::TreeNode& node, LiteralBinder& binder) {
    switch (node.type) {
    case ast::AstType::InsertStmt: {
        for (auto& value : static_cast<ast::InsertStmt&>(node).vals) {
            if (!binder.bind(*value))
                return false;
        }
        return true;
    }
    case ast::AstType::DeleteStmt: {
        for (auto& condition : static_cast<ast::DeleteStmt&>(node).conds) {
            if (!bind_binary(*condition, binder))
                return false;
        }
        return true;
    }
    case ast::AstType::UpdateStmt: {
        auto& update = static_cast<ast::UpdateStmt&>(node);
        for (auto& clause : update.set_clauses) {
            if (clause->val != nullptr && !binder.bind(*clause->val))
                return false;
        }
        for (auto& condition : update.conds) {
            if (!bind_binary(*condition, binder))
                return false;
        }
        return true;
    }
    case ast::AstType::SelectStmt: {
        auto& select = static_cast<ast::SelectStmt&>(node);
        for (auto& item : select.select_items) {
            if (!bind_expr(*item->expr, binder))
                return false;
        }
        for (auto& condition : select.conds) {
            if (!bind_binary(*condition, binder))
                return false;
        }
        for (auto& join : select.jointree) {
            for (auto& condition : join->conds) {
                if (!bind_binary(*condition, binder))
                    return false;
            }
        }
        for (auto& condition : select.having_conds) {
            if (!bind_expr(*condition->lhs, binder) || !bind_expr(*condition->rhs, binder))
                return false;
        }
        for (auto& item : select.order_by_items) {
            if (!bind_expr(*item->expr, binder))
                return false;
        }
        return true;
    }
    case ast::AstType::ExplainAnalyze:
        return bind_tree(*static_cast<ast::ExplainAnalyze&>(node).select, binder);
    case ast::AstType::SelectFromUnionStmt: {
        auto& wrapper = static_cast<ast::SelectFromUnionStmt&>(node);
        for (auto& branch : wrapper.union_stmt->branches) {
            if (!bind_tree(*branch, binder))
                return false;
        }
        for (auto& item : wrapper.order_by_items) {
            if (!bind_expr(*item->expr, binder))
                return false;
        }
        return true;
    }
    default:
        return true;
    }
}

bool bind_query_expr(QueryExpr& expression, LiteralBinder& binder) {
    return expression.type != QueryExprType::VALUE || binder.bind(expression.value);
}

bool bind_query_semantic(Query& query, const parser::OwnedTokenStream& lexical) {
    LiteralBinder binder(lexical);
    for (auto& item : query.select_items) {
        if (!bind_query_expr(item.expr, binder))
            return false;
    }
    for (auto& condition : query.conds) {
        if (condition.is_rhs_val && !binder.bind(condition.rhs_val))
            return false;
    }
    for (auto& condition : query.having_conds) {
        if (!bind_query_expr(condition.lhs, binder) ||
            (!condition.is_rhs_val && !bind_query_expr(condition.rhs_expr, binder)) ||
            (condition.is_rhs_val && !binder.bind(condition.rhs_val))) {
            return false;
        }
    }
    for (auto& item : query.order_by_items) {
        if (!bind_query_expr(item.expr, binder))
            return false;
    }
    for (auto& clause : query.set_clauses) {
        if (!binder.bind(clause.rhs))
            return false;
    }
    for (auto& value : query.values) {
        if (!binder.bind(value))
            return false;
    }
    return binder.done();
}

bool bind_plan_conditions(std::vector<Condition>& conditions, LiteralBinder& binder) {
    for (auto& condition : conditions) {
        if (condition.is_rhs_val && !binder.bind(condition.rhs_val))
            return false;
    }
    return true;
}

bool bind_plan_expr(QueryExpr& expression, LiteralBinder& binder) {
    return expression.type != QueryExprType::VALUE || binder.bind(expression.value);
}

bool bind_plan_node(Plan& plan, LiteralBinder& binder) {
    switch (plan.tag) {
    case T_Projection: {
        auto& projection = static_cast<ProjectionPlan&>(plan);
        for (auto& item : projection.select_items_) {
            if (!bind_plan_expr(item.expr, binder))
                return false;
        }
        return bind_plan_node(*projection.subplan_, binder);
    }
    case T_Aggregate: {
        auto& aggregate = static_cast<AggregatePlan&>(plan);
        if (!bind_plan_node(*aggregate.subplan_, binder))
            return false;
        for (auto& condition : aggregate.having_conds_) {
            if (!bind_plan_expr(condition.lhs, binder) ||
                (!condition.is_rhs_val && !bind_plan_expr(condition.rhs_expr, binder)) ||
                (condition.is_rhs_val && !binder.bind(condition.rhs_val)))
                return false;
        }
        return true;
    }
    case T_Sort: {
        auto& sort = static_cast<SortPlan&>(plan);
        if (!bind_plan_node(*sort.subplan_, binder))
            return false;
        for (auto& item : sort.order_by_items_) {
            if (!bind_plan_expr(item.expr, binder))
                return false;
        }
        return true;
    }
    case T_Limit:
        return bind_plan_node(*static_cast<LimitPlan&>(plan).subplan_, binder);
    case T_Filter: {
        auto& filter = static_cast<FilterPlan&>(plan);
        if (!bind_plan_conditions(filter.conds_, binder))
            return false;
        return bind_plan_node(*filter.subplan_, binder);
    }
    case T_SeqScan:
    case T_IndexScan:
    case T_IndexSkipScan:
        return bind_plan_conditions(static_cast<ScanPlan&>(plan).conds_, binder);
    case T_NestLoop:
    case T_SortMerge: {
        auto& join = static_cast<JoinPlan&>(plan);
        if (!bind_plan_conditions(join.conds_, binder))
            return false;
        return bind_plan_node(*join.left_, binder) && bind_plan_node(*join.right_, binder);
    }
    case T_Union: {
        auto& union_plan = static_cast<UnionPlan&>(plan);
        for (auto& branch : union_plan.branches_) {
            if (!bind_plan_node(*branch, binder))
                return false;
        }
        return true;
    }
    case T_Insert: {
        auto& dml = static_cast<DMLPlan&>(plan);
        for (auto& value : dml.values_) {
            if (!binder.bind(value))
                return false;
        }
        return true;
    }
    case T_Update: {
        auto& dml = static_cast<DMLPlan&>(plan);
        if (!bind_plan_conditions(dml.conds_, binder))
            return false;
        for (auto& clause : dml.set_clauses_) {
            if (!binder.bind(clause.rhs))
                return false;
        }
        return dml.subplan_ == nullptr || bind_plan_node(*dml.subplan_, binder);
    }
    case T_Delete: {
        auto& dml = static_cast<DMLPlan&>(plan);
        return bind_plan_conditions(dml.conds_, binder) &&
               (dml.subplan_ == nullptr || bind_plan_node(*dml.subplan_, binder));
    }
    case T_select:
    case T_ExplainAnalyze: {
        auto& dml = static_cast<DMLPlan&>(plan);
        return dml.subplan_ == nullptr || bind_plan_node(*dml.subplan_, binder);
    }
    default:
        return true;
    }
}

} // namespace

bool StatementTemplateCache::lookup(const parser::TokenShapeKey& key, uint64_t catalog_generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++stats_.lookups;
    auto found = entries_.find(key);
    if (found == entries_.end() || found->second.catalog_generation != catalog_generation || found->second.key != key) {
        ++stats_.misses;
        return false;
    }
    found->second.last_use = ++clock_;
    ++stats_.hits;
    return true;
}

std::unique_ptr<ast::TreeNode> StatementTemplateCache::lookup_ast(const parser::TokenShapeKey& key,
                                                                  uint64_t catalog_generation,
                                                                  const parser::OwnedTokenStream* lexical) {
    std::shared_ptr<const ast::TreeNode> skeleton;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.lookups;
        auto found = entries_.find(key);
        if (found == entries_.end() || found->second.catalog_generation != catalog_generation ||
            found->second.key != key || found->second.skeleton == nullptr) {
            ++stats_.misses;
            return nullptr;
        }
        found->second.last_use = ++clock_;
        ++stats_.hits;
        skeleton = found->second.skeleton;
    }
    auto result = ast::clone_tree(*skeleton);
    if (lexical != nullptr) {
        LiteralBinder binder(*lexical);
        if (!bind_tree(*result, binder) || !binder.done())
            return nullptr;
    }
    return result;
}

std::unique_ptr<Query> StatementTemplateCache::lookup_query(const parser::TokenShapeKey& key,
                                                            uint64_t catalog_generation,
                                                            const parser::OwnedTokenStream* lexical) {
    std::shared_ptr<const Query> query;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.lookups;
        auto found = entries_.find(key);
        if (found == entries_.end() || found->second.catalog_generation != catalog_generation ||
            found->second.key != key || found->second.query == nullptr) {
            ++stats_.misses;
            return nullptr;
        }
        found->second.last_use = ++clock_;
        ++stats_.hits;
        query = found->second.query;
    }
    auto result = clone_query(*query);
    if (lexical != nullptr && result->parse != nullptr) {
        LiteralBinder binder(*lexical);
        if (!bind_tree(*result->parse, binder) || !binder.done())
            return nullptr;
        if (!bind_query_semantic(*result, *lexical))
            return nullptr;
    }
    return result;
}

std::unique_ptr<Plan> StatementTemplateCache::lookup_plan(const parser::TokenShapeKey& key, uint64_t catalog_generation,
                                                          SmManager* sm_manager,
                                                          const parser::OwnedTokenStream* lexical) {
    std::shared_ptr<const Plan> plan;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.lookups;
        auto found = entries_.find(key);
        if (found == entries_.end() || found->second.catalog_generation != catalog_generation ||
            found->second.key != key || found->second.plan == nullptr) {
            ++stats_.misses;
            return nullptr;
        }
        found->second.last_use = ++clock_;
        ++stats_.hits;
        plan = found->second.plan;
    }
    auto result = clone_plan(*plan, sm_manager);
    if (lexical != nullptr) {
        LiteralBinder binder(*lexical);
        if (!bind_plan_node(*result, binder) || !binder.done())
            return nullptr;
    }
    return result;
}

FullTemplateLookup StatementTemplateCache::lookup_full(const parser::TokenShapeKey& key, uint64_t catalog_generation,
                                                       SmManager* sm_manager, const parser::OwnedTokenStream* lexical) {
    ast::AstType statement_type{ast::AstType::Help};
    std::shared_ptr<const Plan> plan;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.lookups;
        auto found = entries_.find(key);
        if (found == entries_.end() || found->second.catalog_generation != catalog_generation ||
            found->second.key != key || found->second.skeleton == nullptr || found->second.plan == nullptr) {
            ++stats_.misses;
            return {};
        }
        found->second.last_use = ++clock_;
        ++stats_.hits;
        statement_type = found->second.skeleton->type;
        plan = found->second.plan;
    }

    FullTemplateLookup result{statement_type, clone_plan(*plan, sm_manager)};
    if (!result) {
        return {};
    }
    if (lexical == nullptr) {
        return result;
    }
    LiteralBinder plan_binder(*lexical);
    if (!bind_plan_node(*result.plan, plan_binder) || !plan_binder.done()) {
        return {};
    }
    return result;
}

void StatementTemplateCache::publish(const parser::TokenShapeKey& key, uint64_t catalog_generation,
                                     std::shared_ptr<const ast::TreeNode> skeleton, std::shared_ptr<const Query> query,
                                     std::shared_ptr<const Plan> plan) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto previous = entries_.find(key);
    if (previous != entries_.end() && skeleton == nullptr) {
        skeleton = previous->second.skeleton;
    }
    if (previous != entries_.end() && query == nullptr) {
        query = previous->second.query;
    }
    if (previous != entries_.end() && plan == nullptr) {
        plan = previous->second.plan;
    }
    Entry entry{key, catalog_generation, ++clock_, std::move(skeleton), std::move(query), std::move(plan)};
    entries_[key] = std::move(entry);
    ++stats_.publishes;
    if (entries_.size() <= capacity_) {
        return;
    }
    auto victim = std::min_element(entries_.begin(), entries_.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.second.last_use < rhs.second.last_use;
    });
    entries_.erase(victim);
    ++stats_.evictions;
}

StatementTemplateStats StatementTemplateCache::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
}

void StatementTemplateCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.clear();
}

} // namespace cache
