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

#include <memory>
#include <string>
#include <vector>

#include "access/cursor/index_cursor.h"
#include "access/cursor/table_cursor.h"
#include "common/common.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

namespace rmdb::system {
class SchemaManager;
}

namespace rmdb::access {

/// 执行层访问数据的统一门面。
/// 持有 SchemaManager*，创建 TableCursor / IndexCursor，并提供 SSI 转发。
/// execution/ 依赖 TableAccess，不再直接接触 RmScan/IxScan/RmFileHandle/IxIndexHandle。
class TableAccess {
public:
    explicit TableAccess(rmdb::system::SchemaManager* schema_manager);

    // --- cursor 工厂 ---
    std::unique_ptr<TableCursor> open_table_scan(const std::string& tab_name);
    std::unique_ptr<IndexCursor> open_index_scan(const std::string& tab_name, const std::vector<ColMeta>& index_cols);
    std::unique_ptr<IndexCursor> open_index_scan(const std::string& tab_name,
                                                 const std::vector<std::string>& index_cols);

    // --- 表级元数据 / 单点访问 ---
    int record_size(const std::string& tab_name);
    bool is_record(const std::string& tab_name, const Rid& rid);
    TupleMeta get_tuple_meta(const std::string& tab_name, const Rid& rid);
    std::unique_ptr<RmRecord> get_record(const std::string& tab_name, const Rid& rid,
                                         rmdb::statement::StatementContext* context);
    std::unique_ptr<RmRecord> get_visible_record(const std::string& tab_name, const Rid& rid,
                                                 rmdb::statement::StatementContext* context);

    // --- SSI 转发（替代执行器直接持有 fh_ 调用 TransactionManager）---
    /// 谓词读冲突检测（转发 TransactionManager::CheckPredicateInvisibleWrites）。
    bool check_predicate_invisible_writes(rmdb::statement::StatementContext* context, txn_id_t reader,
                                          const std::string& tab_name, const std::vector<Condition>& conds,
                                          const std::vector<ColMeta>& cols);

    /// insert 的 SSI deleted-tuple-candidate 冲突检测。
    bool deleted_tuple_candidates_conflict_with_insert(const std::string& tab_name, const RmRecord& inserted_rec,
                                                       rmdb::statement::StatementContext* context);

    /// 索引历史键冲突检测。
    bool historical_index_key_conflicts_with_txn(const std::string& tab_name, const Rid& rid, const IndexMeta& index,
                                                 const std::vector<char>& key,
                                                 rmdb::statement::StatementContext* context);

private:
    rmdb::system::SchemaManager* schema_manager_;
};

} // namespace rmdb::access
