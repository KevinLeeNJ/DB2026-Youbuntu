/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "parser/token_stream.h"
#include "parser/ast.h"
#include "analyze/analyze.h"
#include "common/config.h"
#include "optimizer/plan.h"

class PreparedSelectDescriptor;

namespace cache {

using StatementCacheMode = rmdb_config::StatementCacheMode;

struct StatementTemplateStats {
    uint64_t lookups{0};
    uint64_t hits{0};
    uint64_t misses{0};
    uint64_t publishes{0};
    uint64_t evictions{0};
};

struct FullTemplateLookup {
    ast::AstType statement_type{ast::AstType::Help};
    std::unique_ptr<Plan> plan;
    std::shared_ptr<const PreparedSelectDescriptor> prepared_select;

    explicit operator bool() const {
        return plan != nullptr || prepared_select != nullptr;
    }
};

class StatementTemplateCache {
public:
    explicit StatementTemplateCache(size_t capacity = rmdb_config::kStatementTemplateCacheCapacity)
        : capacity_(capacity) {}

    bool lookup(const parser::TokenShapeKey& key, uint64_t catalog_generation);
    std::unique_ptr<ast::TreeNode> lookup_ast(const parser::TokenShapeKey& key, uint64_t catalog_generation,
                                              const parser::OwnedTokenStream* lexical = nullptr);
    std::unique_ptr<Query> lookup_query(const parser::TokenShapeKey& key, uint64_t catalog_generation,
                                        const parser::OwnedTokenStream* lexical = nullptr);
    std::unique_ptr<Plan> lookup_plan(const parser::TokenShapeKey& key, uint64_t catalog_generation,
                                      SmManager* sm_manager, const parser::OwnedTokenStream* lexical = nullptr);
    FullTemplateLookup lookup_full(const parser::TokenShapeKey& key, uint64_t catalog_generation, SmManager* sm_manager,
                                   const parser::OwnedTokenStream* lexical = nullptr);
    void publish(const parser::TokenShapeKey& key, uint64_t catalog_generation,
                 std::shared_ptr<const ast::TreeNode> skeleton = nullptr, std::shared_ptr<const Query> query = nullptr,
                 std::shared_ptr<const Plan> plan = nullptr,
                 std::shared_ptr<const PreparedSelectDescriptor> prepared_select = nullptr);
    StatementTemplateStats stats() const;
    void clear();

private:
    struct Entry {
        parser::TokenShapeKey key;
        uint64_t catalog_generation{0};
        uint64_t last_use{0};
        std::shared_ptr<const ast::TreeNode> skeleton;
        std::shared_ptr<const Query> query;
        std::shared_ptr<const Plan> plan;
        std::shared_ptr<const PreparedSelectDescriptor> prepared_select;
    };

    size_t capacity_;
    mutable std::mutex mutex_;
    std::unordered_map<parser::TokenShapeKey, Entry, parser::TokenShapeKeyHash> entries_;
    uint64_t clock_{0};
    StatementTemplateStats stats_;
};

} // namespace cache
