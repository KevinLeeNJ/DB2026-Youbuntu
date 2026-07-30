/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "common/common.h"

class IxIndexHandle;
class RmFileHandle;
class SmManager;
struct IndexMeta;
struct TabMeta;

struct PreparedInsertValueBinding {
    std::size_t parameter_ordinal = 0;
    ColType type = TYPE_INT;
    int length = 0;
    Value constant;
};

struct PreparedInsertIndexBinding {
    const IndexMeta* metadata = nullptr;
    IxIndexHandle* handle = nullptr;
    std::string name;
};

// Immutable and catalog-generation-scoped. Handles are owned by SmManager;
// PREPARE_SET invalidation and the catalog guard bound their lifetime.
struct PreparedInsertExecutable {
    SmManager* sm_manager = nullptr;
    std::string table_name;
    const TabMeta* table = nullptr;
    RmFileHandle* table_handle = nullptr;
    std::vector<PreparedInsertValueBinding> values;
    std::vector<PreparedInsertIndexBinding> indexes;
};
