/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
*/

#include "point_lookup_runtime.h"

#include <algorithm>
#include <cstring>

#include "index/ix.h"
#include "system/sm.h"

namespace {

void WritePointKeyPart(char* dest, const Value& value, const ColMeta& col) {
    memset(dest, 0, col.len);
    switch (col.type) {
    case TYPE_INT: {
        const int converted = value.type == TYPE_FLOAT ? static_cast<int>(value.float_val) : value.int_val;
        memcpy(dest, &converted, col.len);
        break;
    }
    case TYPE_FLOAT: {
        const double converted = value.type == TYPE_INT ? static_cast<double>(value.int_val) : value.float_val;
        memcpy(dest, &converted, col.len);
        break;
    }
    case TYPE_STRING:
    case TYPE_DATETIME:
        memcpy(dest, value.str_val.data(), std::min(static_cast<size_t>(col.len), value.str_val.size()));
        break;
    }
}

} // namespace

PointLookupResult PointLookupRuntime::Lookup(const PointLookupRequest& request, SmManager* sm_manager,
                                             Context* context) {
    if (request.table_name == nullptr || request.index_col_names == nullptr || request.conditions == nullptr ||
        request.condition_positions == nullptr || sm_manager == nullptr) {
        return {PointLookupStatus::FALLBACK, std::nullopt};
    }
    if (context != nullptr && context->txn_ != nullptr &&
        context->txn_->get_isolation_level() != IsolationLevel::READ_COMMITTED) {
        return {PointLookupStatus::FALLBACK, std::nullopt};
    }

    auto& tab = sm_manager->db_.get_table(*request.table_name);
    auto index_it = tab.get_index_meta(*request.index_col_names);
    if (index_it == tab.indexes.end() || request.condition_positions->size() != index_it->cols.size()) {
        return {PointLookupStatus::FALLBACK, std::nullopt};
    }
    const IndexMeta& index = *index_it;
    const std::string index_name = sm_manager->get_ix_manager()->get_index_name(*request.table_name, index.cols);
    auto ih_it = sm_manager->ihs_.find(index_name);
    if (ih_it == sm_manager->ihs_.end()) {
        return {PointLookupStatus::FALLBACK, std::nullopt};
    }

    std::vector<char> key(index.col_tot_len);
    int key_offset = 0;
    for (size_t i = 0; i < index.cols.size(); ++i) {
        const size_t condition_position = (*request.condition_positions)[i];
        if (condition_position >= request.conditions->size()) {
            return {PointLookupStatus::FALLBACK, std::nullopt};
        }
        const auto& condition = (*request.conditions)[condition_position];
        if (!condition.is_rhs_val) {
            return {PointLookupStatus::FALLBACK, std::nullopt};
        }
        WritePointKeyPart(key.data() + key_offset, condition.rhs_val, index.cols[i]);
        key_offset += index.cols[i].len;
    }

    const auto lookup = ih_it->second->lookup_unique(key.data());
    if (lookup.status == UniqueLookupStatus::Duplicate) {
        return {PointLookupStatus::FALLBACK, std::nullopt};
    }
    std::optional<Rid> point_rid;
    if (lookup.status == UniqueLookupStatus::Unique) {
        point_rid = lookup.rid;
    }
    if (context != nullptr && context->txn_ != nullptr && context->txn_mgr_ != nullptr &&
        sm_manager->has_historical_index_keys(*request.table_name, index_name)) {
        std::optional<Rid> historical_rid;
        for (const Rid& candidate_rid :
             sm_manager->get_historical_index_key_rids(*request.table_name, index_name, key)) {
            if (!historical_rid.has_value()) {
                historical_rid = candidate_rid;
            } else if (*historical_rid != candidate_rid) {
                return {PointLookupStatus::FALLBACK, std::nullopt};
            }
        }
        if (historical_rid.has_value()) {
            if (point_rid.has_value() && *point_rid != *historical_rid) {
                return {PointLookupStatus::FALLBACK, std::nullopt};
            }
            point_rid = historical_rid;
        }
    }
    if (!point_rid.has_value()) {
        return {PointLookupStatus::NOT_FOUND, std::nullopt};
    }
    return {PointLookupStatus::FOUND, point_rid};
}

PointLookupResult PointLookupRuntime::LookupEncoded(const std::string& table_name,
                                                    const std::vector<std::string>& index_col_names, const char* key,
                                                    size_t key_size, SmManager* sm_manager, Context* context,
                                                    const std::string* validated_index_name) {
    if (sm_manager == nullptr || key == nullptr) {
        return {PointLookupStatus::FALLBACK, std::nullopt};
    }
    if (context != nullptr && context->txn_ != nullptr &&
        context->txn_->get_isolation_level() != IsolationLevel::READ_COMMITTED) {
        return {PointLookupStatus::FALLBACK, std::nullopt};
    }

    auto& tab = sm_manager->db_.get_table(table_name);
    auto index_it = tab.get_index_meta(index_col_names);
    if (index_it == tab.indexes.end() || key_size != static_cast<size_t>(index_it->col_tot_len)) {
        return {PointLookupStatus::FALLBACK, std::nullopt};
    }
    const IndexMeta& index = *index_it;
    const std::string computed_index_name = validated_index_name == nullptr
                                                ? sm_manager->get_ix_manager()->get_index_name(table_name, index.cols)
                                                : std::string{};
    const std::string& index_name = validated_index_name == nullptr ? computed_index_name : *validated_index_name;
    auto ih_it = sm_manager->ihs_.find(index_name);
    if (ih_it == sm_manager->ihs_.end()) {
        return {PointLookupStatus::FALLBACK, std::nullopt};
    }

    const auto lookup = ih_it->second->lookup_unique(key);
    if (lookup.status == UniqueLookupStatus::Duplicate) {
        return {PointLookupStatus::FALLBACK, std::nullopt};
    }
    std::optional<Rid> point_rid;
    if (lookup.status == UniqueLookupStatus::Unique) {
        point_rid = lookup.rid;
    }
    if (context != nullptr && context->txn_ != nullptr && context->txn_mgr_ != nullptr &&
        sm_manager->has_historical_index_keys(table_name, index_name)) {
        const std::vector<char> owned_key(key, key + key_size);
        std::optional<Rid> historical_rid;
        for (const Rid& candidate_rid : sm_manager->get_historical_index_key_rids(table_name, index_name, owned_key)) {
            if (!historical_rid.has_value()) {
                historical_rid = candidate_rid;
            } else if (*historical_rid != candidate_rid) {
                return {PointLookupStatus::FALLBACK, std::nullopt};
            }
        }
        if (historical_rid.has_value()) {
            if (point_rid.has_value() && *point_rid != *historical_rid) {
                return {PointLookupStatus::FALLBACK, std::nullopt};
            }
            point_rid = historical_rid;
        }
    }
    if (!point_rid.has_value()) {
        return {PointLookupStatus::NOT_FOUND, std::nullopt};
    }
    return {PointLookupStatus::FOUND, point_rid};
}
