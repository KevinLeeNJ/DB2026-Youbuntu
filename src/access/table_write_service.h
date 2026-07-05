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

#include <string>
#include <vector>

#include "common/common.h"
#include "common/context.h"
#include "record/rm_file_handle.h"
#include "recovery/log_manager.h"
#include "system/schema_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"

namespace rmdb::access {

/// 统一写路径服务。insert / delete / update / load 的写协议集中在此实现。
/// 执行器只调用本服务的接口，不再直接调 add_log_to_buffer / set_tuple_meta /
/// insert_entry / delete_entry。详见 docs/refactor/write-protocol-contract.md。
///
/// Phase 4 保持行为不变：
/// - 决策 G：不调用 set_page_lsn（独立 PR 修复）。
/// - 决策 H 选项 A：保持当前锁语义，delete/update 行级 X 锁，insert 无事务锁。
/// - txn=nullptr（recovery / load 无事务场景）：跳过锁 / WAL / Undo / MVCC。
class TableWriteService {
public:
    TableWriteService(SchemaManager* schema_mgr, LockManager* lock_mgr, LogManager* log_mgr,
                      TransactionManager* txn_mgr);

    // === 单行写操作 ===

    /// 插入一条记录。返回新插入行的 rid。
    /// txn 可为 nullptr（无事务场景，跳过锁/WAL/Undo/MVCC）。
    Rid insert(const std::string& tab_name, const RmRecord& rec, Transaction* txn, Context* ctx);

    /// 删除 rid 指向的行。conds 用于加锁前谓词预检与 RC 重读后谓词复检。
    /// 返回是否实际删除（false=skip）。
    bool remove(const std::string& tab_name, const Rid& rid, const std::vector<Condition>& conds, Transaction* txn,
                Context* ctx);

    /// 更新 rid 指向的行。set_clauses 描述如何由旧值计算新值（与原 UpdateExecutor
    /// 的 update_record 语义一致），conds 用于加锁前谓词预检与 RC 重读后谓词复检。
    /// 返回是否实际更新（false=skip）。
    bool update(const std::string& tab_name, const Rid& rid, const std::vector<SetClause>& set_clauses,
                const std::vector<Condition>& conds, Transaction* txn, Context* ctx);

    // === 批量插入（LOAD DATA 用）===
    /// 无事务批量插入。内部用 PinnedInserter 走批量路径，跳过锁/WAL/Undo/MVCC。
    /// 失败时抛 RMDBError。调用方负责 flush（LoadDataService 在结束时 flush）。
    void bulk_insert(const std::string& tab_name, const std::vector<std::vector<char>>& rows, Context* ctx);

private:
    SchemaManager* schema_mgr_;
    LockManager* lock_mgr_;
    LogManager* log_mgr_;
    TransactionManager* txn_mgr_;

    // 由 old_rec 应用 set_clauses 计算 new_rec（迁移自 UpdateExecutor::update_record）。
    static void apply_set_clauses(const TabMeta& tab, const std::vector<SetClause>& set_clauses, RmRecord* rec,
                                  const RmRecord& old_rec);

    // 谓词匹配（迁移自 AbstractExecutor::compare，独立实现避免依赖执行器）。
    static bool record_matches_conds(const TabMeta& tab, const std::vector<Condition>& conds, const RmRecord& rec);

    // 管理器解析：优先取自 Context（与原 executor 一致），回退到服务成员。
    inline LockManager* resolve_lock_mgr(Context* ctx) const;
    inline LogManager* resolve_log_mgr(Context* ctx) const;
    inline TransactionManager* resolve_txn_mgr(Context* ctx) const;
};

} // namespace rmdb::access
