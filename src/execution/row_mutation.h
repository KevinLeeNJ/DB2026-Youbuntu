/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
*/

#pragma once

#include <cstdint>
#include <vector>

#include "execution_common.h"
#include "index/ix.h"
#include "system/sm.h"

struct RowMutationIndex {
    const IndexMeta* meta;
    IxIndexHandle* handle;
    std::string name;
};

struct BoundMutationColumn {
    uint32_t offset;
    uint32_t len;
    ColType type;
    // 元组内该列 NULL 位的地址；null_byte < 0 表示该列不可能为 NULL
    int null_byte = -1;
    uint8_t null_mask = 0;
};

struct BoundMutationCondition {
    BoundMutationColumn lhs;
    BoundMutationColumn rhs;
};

struct BoundMutationSetClause {
    BoundMutationColumn lhs;
    BoundMutationColumn rhs;
};

std::vector<BoundMutationCondition> BindMutationConditions(const TabMeta& tab,
                                                           const std::vector<Condition>& conditions);
std::vector<BoundMutationSetClause> BindMutationSetClauses(const TabMeta& tab,
                                                           const std::vector<SetClause>& set_clauses);

struct RowMutationRuntimeInfo {
    SmManager* sm_manager;
    const std::string* tab_name;
    const TabMeta* tab;
    RmFileHandle* fh;
    const std::vector<Condition>* conditions;
    const std::vector<BoundMutationCondition>* bound_conditions;
    const std::vector<RowMutationIndex>* indexes;
};

struct UpdateRuntimeInfo : RowMutationRuntimeInfo {
    const std::vector<SetClause>* set_clauses;
    const std::vector<BoundMutationSetClause>* bound_set_clauses;
    const std::vector<bool>* affected_index_bitmap;
};

struct DeleteRuntimeInfo : RowMutationRuntimeInfo {};

class RowMutationEngine {
public:
    // Evaluates the bound statement predicate against a snapshot-visible row.
    static bool MatchesTarget(const RmRecord& visible_record, const RowMutationRuntimeInfo& info);

    // Performs the same predicate, X-lock, and post-lock tuple-meta checks as a
    // real mutation without creating a tuple version or any mutation side effects.
    static bool LockOnly(const Rid& rid, RmRecord& visible_record, const RowMutationRuntimeInfo& info,
                         Context* context);

    // Returns false when the row no longer belongs to the statement's target
    // set (including the READ COMMITTED post-lock recheck).
    static bool UpdateOne(const Rid& rid, RmRecord& visible_record, const UpdateRuntimeInfo& info, Context* context);

    // Returns false when the row no longer belongs to the statement's target
    // set. A successful delete is represented by true.
    static bool DeleteOne(const Rid& rid, RmRecord& visible_record, const DeleteRuntimeInfo& info, Context* context);
};
