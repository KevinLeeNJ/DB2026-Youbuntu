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
#include <memory>
#include <optional>
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
void MakeRowMutationIndexKey(const IndexMeta& index, const char* record_data, std::vector<char>& out);
std::vector<char> MakeRowMutationIndexKey(const IndexMeta& index, const char* record_data);

struct RowMutationKeyScratch {
    void PrepareUpdate(size_t index_count) {
        if (old_keys.size() < index_count) {
            old_keys.resize(index_count);
        }
        if (new_keys.size() < index_count) {
            new_keys.resize(index_count);
        }
    }

    void PrepareSingle(size_t index_count) {
        if (keys.size() < index_count) {
            keys.resize(index_count);
        }
    }

    std::vector<std::vector<char>> old_keys;
    std::vector<std::vector<char>> new_keys;
    std::vector<std::vector<char>> keys;
};

class RowMutationKeyScratchLease {
public:
    RowMutationKeyScratchLease();
    ~RowMutationKeyScratchLease();
    RowMutationKeyScratchLease(const RowMutationKeyScratchLease&) = delete;
    RowMutationKeyScratchLease& operator=(const RowMutationKeyScratchLease&) = delete;

    RowMutationKeyScratch& get() const {
        return *scratch_;
    }

private:
    RowMutationKeyScratch* scratch_{nullptr};
};

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

using MutationFaultHook = void (*)(const char* point);
void SetMutationFaultHookForTesting(MutationFaultHook hook) noexcept;
#ifndef NDEBUG
void MutationFaultPoint(const char* point);
#else
inline void MutationFaultPoint(const char*) {}
#endif

class PreparedUpdate {
public:
    PreparedUpdate(const PreparedUpdate&) = delete;
    PreparedUpdate& operator=(const PreparedUpdate&) = delete;
    PreparedUpdate(PreparedUpdate&&) noexcept = default;
    PreparedUpdate& operator=(PreparedUpdate&&) noexcept = default;

    const Rid& rid() const {
        return rid_;
    }

    const RmRecord& old_record() const {
        return *old_record_;
    }

private:
    friend class RowMutationEngine;

    PreparedUpdate(const Rid& rid, std::unique_ptr<RmRecord> old_record, TupleMeta old_meta, txn_id_t txn_id,
                   int table_fd, uint64_t catalog_generation)
        : rid_(rid), old_record_(std::move(old_record)), old_meta_(old_meta), txn_id_(txn_id), table_fd_(table_fd),
          catalog_generation_(catalog_generation) {}

    Rid rid_;
    std::unique_ptr<RmRecord> old_record_;
    TupleMeta old_meta_;
    txn_id_t txn_id_{INVALID_TXN_ID};
    int table_fd_{-1};
    uint64_t catalog_generation_{0};
    bool consumed_{false};
};

class RowMutationEngine {
public:
    // Returns no value when the row no longer belongs to the statement's
    // target set. The returned tuple is owned and reflects the RC post-lock
    // re-read when one was required.
    static std::optional<PreparedUpdate> PrepareUpdate(const Rid& rid, const UpdateRuntimeInfo& info, Context* context);

    static std::unique_ptr<RmRecord> ComputeLegacyUpdate(const PreparedUpdate& prepared, const UpdateRuntimeInfo& info);

    static void CommitUpdate(PreparedUpdate&& prepared, RmRecord& proposed, const UpdateRuntimeInfo& info,
                             Context* context);

    // Returns false when the row no longer belongs to the statement's target
    // set (including the READ COMMITTED post-lock recheck).
    static bool UpdateOne(const Rid& rid, RmRecord& visible_record, const UpdateRuntimeInfo& info, Context* context);

    // Returns false when the row no longer belongs to the statement's target
    // set. A successful delete is represented by true.
    static bool DeleteOne(const Rid& rid, RmRecord& visible_record, const DeleteRuntimeInfo& info, Context* context);
};
