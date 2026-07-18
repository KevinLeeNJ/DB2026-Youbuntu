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
#include <cstdlib>
#include <string_view>

namespace cache {

StatementCacheMode configured_statement_cache_mode() {
    const char* value = std::getenv("RMDB_STATEMENT_CACHE");
    if (value == nullptr || std::string_view(value) == "off") {
        return value == nullptr ? StatementCacheMode::SHADOW : StatementCacheMode::OFF;
    }
    const std::string_view mode(value);
    if (mode == "parser") {
        return StatementCacheMode::PARSER;
    }
    if (mode == "analyzer") {
        return StatementCacheMode::ANALYZER;
    }
    if (mode == "full") {
        return StatementCacheMode::FULL;
    }
    return StatementCacheMode::SHADOW;
}

bool StatementTemplateCache::lookup(const parser::TokenShapeKey& key, uint64_t catalog_generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++stats_.lookups;
    auto found = entries_.find(map_key(key));
    if (found == entries_.end() || found->second.catalog_generation != catalog_generation || found->second.key != key) {
        ++stats_.misses;
        return false;
    }
    found->second.last_use = ++clock_;
    ++stats_.hits;
    return true;
}

std::unique_ptr<ast::TreeNode> StatementTemplateCache::lookup_ast(const parser::TokenShapeKey& key,
                                                                  uint64_t catalog_generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++stats_.lookups;
    auto found = entries_.find(map_key(key));
    if (found == entries_.end() || found->second.catalog_generation != catalog_generation || found->second.key != key ||
        found->second.skeleton == nullptr) {
        ++stats_.misses;
        return nullptr;
    }
    found->second.last_use = ++clock_;
    ++stats_.hits;
    return ast::clone_tree(*found->second.skeleton);
}

std::unique_ptr<Query> StatementTemplateCache::lookup_query(const parser::TokenShapeKey& key,
                                                            uint64_t catalog_generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++stats_.lookups;
    auto found = entries_.find(map_key(key));
    if (found == entries_.end() || found->second.catalog_generation != catalog_generation || found->second.key != key ||
        found->second.query == nullptr) {
        ++stats_.misses;
        return nullptr;
    }
    found->second.last_use = ++clock_;
    ++stats_.hits;
    return clone_query(*found->second.query);
}

void StatementTemplateCache::publish(const parser::TokenShapeKey& key, uint64_t catalog_generation,
                                     std::shared_ptr<const ast::TreeNode> skeleton,
                                     std::shared_ptr<const Query> query) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto previous = entries_.find(map_key(key));
    if (previous != entries_.end() && skeleton == nullptr) {
        skeleton = previous->second.skeleton;
    }
    if (previous != entries_.end() && query == nullptr) {
        query = previous->second.query;
    }
    Entry entry{key, catalog_generation, ++clock_, std::move(skeleton), std::move(query)};
    entries_[map_key(key)] = std::move(entry);
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
