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

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "execution/row_mutation.h"

class IxIndexHandle;
class RmFileHandle;
class SmManager;
struct IndexMeta;
struct TabMeta;

struct PreparedConditionBinding {
    Condition condition;
    int rhs_raw_length = 0;
};

struct PreparedSetClauseBinding {
    SetClause clause;
    ColType target_type = TYPE_INT;
    int target_length = 0;
};

struct PreparedScanExecutable {
    SmManager* sm_manager = nullptr;
    std::string table_name;
    const TabMeta* table = nullptr;
    RmFileHandle* table_handle = nullptr;
    bool uses_index = false;
    const IndexMeta* index = nullptr;
    IxIndexHandle* index_handle = nullptr;
    std::string index_name;
    bool scan_backward = false;
    std::vector<PreparedConditionBinding> conditions;
};

enum class PreparedSelectLayerKind {
    Filter,
    Projection,
    Limit,
};

struct PreparedSelectLayer {
    PreparedSelectLayerKind kind = PreparedSelectLayerKind::Filter;
    std::vector<PreparedConditionBinding> conditions;
    std::vector<std::size_t> projection_ordinals;
    std::vector<std::string> projection_names;
    int limit = 0;
    int offset = 0;
    std::size_t limit_parameter_ordinal = 0;
    std::size_t offset_parameter_ordinal = 0;
};

struct PreparedSelectExecutable {
    PreparedScanExecutable scan;
    // Inner-to-outer order, so runtime construction is a single linear pass.
    std::vector<PreparedSelectLayer> layers;
};

struct PreparedPointKeyPart {
    std::size_t condition_index = 0;
    std::size_t parameter_ordinal = 0;
    ColType target_type = TYPE_INT;
    int target_length = 0;
    int key_offset = 0;
};

struct PreparedPointUpdateExecutable {
    const IndexMeta* index = nullptr;
    IxIndexHandle* index_handle = nullptr;
    std::string index_name;
    int key_length = 0;
    std::vector<PreparedPointKeyPart> key_parts;
    bool lock_only = false;
};

struct PreparedUpdateExecutable {
    PreparedScanExecutable scan;
    std::vector<PreparedConditionBinding> conditions;
    std::vector<PreparedSetClauseBinding> set_clauses;
    std::vector<BoundMutationCondition> bound_conditions;
    std::vector<BoundMutationSetClause> bound_set_clauses;
    std::vector<RowMutationIndex> indexes;
    std::vector<bool> affected_index_bitmap;
    std::optional<PreparedPointUpdateExecutable> point_update;
};
