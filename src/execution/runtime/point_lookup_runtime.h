/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
*/

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "common/common.h"
#include "common/context.h"
#include "record/rm_defs.h"

class SmManager;

enum class PointLookupStatus { FOUND, NOT_FOUND, FALLBACK };

struct PointLookupResult {
    PointLookupStatus status{PointLookupStatus::FALLBACK};
    std::optional<Rid> rid;
};

struct PointLookupRequest {
    const std::string* table_name;
    const std::vector<std::string>* index_col_names;
    const std::vector<Condition>* conditions;
    const std::vector<size_t>* condition_positions;
};

class PointLookupRuntime {
public:
    static PointLookupResult Lookup(const PointLookupRequest& request, SmManager* sm_manager, Context* context);
    static PointLookupResult LookupEncoded(const std::string& table_name,
                                           const std::vector<std::string>& index_col_names, const char* key,
                                           size_t key_size, SmManager* sm_manager, Context* context);
};
