/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_common.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "executor_expr.h"
#include "index/ix.h"
#include "system/sm.h"
#include <algorithm>

/**
 * @brief 更新指定 RID 集合中满足条件的记录。
 *
 * 更新会重新读取可见版本并验证谓词，计算新记录后检查旧/新记录的 SSI 写冲突，
 * 再同步维护 MVCC undo、WAL 和所有受影响的索引键。
 */
class UpdateExecutor : public AbstractExecutor {
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle* fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager* sm_manager_;
    QueryExprEvaluator::SubqueryRunner subquery_runner_;

    /**
     * @brief 按索引列顺序从记录中拼接复合索引键。
     * @param index 索引元数据。
     * @param rec_data 记录数据首地址。
     * @return 固定长度的索引键字节数组。
     */
    static std::vector<char> make_index_key(const IndexMeta& index, const char* rec_data) {
        std::vector<char> key(index.col_tot_len);
        int offset = 0;
        for (int i = 0; i < index.col_num; ++i) {
            std::memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        return key;
    }

public:
    /**
     * @brief 创建更新执行器。
     * @param sm_manager 系统管理器。
     * @param tab_name 目标表名。
     * @param set_clauses SET 子句列表。
     * @param conds 更新前需要满足的条件。
     * @param rids 待检查的记录 RID 列表。
     * @param context 当前执行上下文。
     * @param subquery_runner SET 表达式内部子查询执行回调。
     */
    UpdateExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context* context,
                   QueryExprEvaluator::SubqueryRunner subquery_runner = {}) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
        subquery_runner_ = std::move(subquery_runner);
    }
    /**
     * @brief 执行更新操作。
     * @return DML 不产生上层数据行，始终返回 nullptr。
     * @throws TransactionAbortException 锁、MVCC 或 SSI 冲突时抛出。
     *
     * 流程依次为：读取可见版本并过滤、加写锁及刷新 RC 版本、计算新记录、
     * 原子检查 SSI、预检查新索引键、记录 WAL/undo、替换索引并最终写回记录。
     */
    std::unique_ptr<RmRecord> Next() override {
        // 非自引用 SET 需要额外检查 RC 中语句读时间之后是否出现新提交版本。
        bool has_non_self_ref_set = std::any_of(set_clauses_.begin(), set_clauses_.end(),
                                                [](const SetClause& clause) { return !clause.is_self_ref; });
        for (Rid& rid : rids_) {
            std::unique_ptr<RmRecord> rec = GetVisibleRecord(fh_, rid, context_);
            if (rec == nullptr) {
                if (context_ != nullptr && context_->txn_ != nullptr) {
                    throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
                }
                continue;
            }
            bool match = true;
            for (auto cond : conds_) // 判断是否匹配
            {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                // 第一阶段：获取写锁；READ COMMITTED 下刷新最新版本并重新验证条件。
                if (context_ != nullptr && context_->txn_ != nullptr) {
                    auto txn = context_->txn_;
                    timestamp_t statement_read_ts = txn->get_read_ts();
                    if (context_->lock_mgr_ != nullptr &&
                        !context_->lock_mgr_->lock_exclusive_on_record(txn, rid, fh_->GetFd())) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                    }
                    if (txn->get_isolation_level() == IsolationLevel::READ_COMMITTED && context_->txn_mgr_ != nullptr) {
                        auto current_record = GetCurrentRecordForRcWrite(fh_, rid, txn, context_);
                        if (!current_record.has_value()) {
                            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                        }
                        rec = std::move(current_record->record);
                        bool latest_match = true;
                        for (const auto& cond : conds_) {
                            if (!compare(cond, *rec)) {
                                latest_match = false;
                                break;
                            }
                        }
                        if (!latest_match) {
                            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                        }
                        const TupleMeta& latest_meta = current_record->meta;
                        if (has_non_self_ref_set && latest_meta.is_committed_ &&
                            latest_meta.writer_txn_id_ != txn->get_transaction_id() &&
                            latest_meta.commit_ts_ > statement_read_ts) {
                            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                        }
                    }
                    TupleMeta meta = fh_->get_tuple_meta(rid);
                    if (!meta.is_committed_ && meta.writer_txn_id_ != txn->get_transaction_id()) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                    }
                    IsolationLevel level = txn->get_isolation_level();
                    bool snapshot_conflict_check = level == IsolationLevel::SNAPSHOT_ISOLATION ||
                                                   level == IsolationLevel::REPEATABLE_READ ||
                                                   level == IsolationLevel::SERIALIZABLE;
                    if (snapshot_conflict_check && meta.is_committed_ && meta.commit_ts_ > txn->get_start_ts() &&
                        meta.writer_txn_id_ != txn->get_transaction_id()) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                    }
                }

                // 第二阶段：在旧记录副本上计算新记录，后续索引和 SSI 检查都使用这两个版本。
                auto new_rec = std::make_unique<RmRecord>(*rec); // 对原记录进行拷贝
                update_record(new_rec.get(), *rec);              // 对记录更新

                // 第三阶段：在一个原子检查中同时登记旧记录和新记录的 SSI 写影响。
                if (context_ != nullptr && context_->txn_ != nullptr &&
                    context_->txn_->get_isolation_level() == IsolationLevel::SERIALIZABLE &&
                    context_->txn_mgr_ != nullptr) {
                    auto* txn_mgr = context_->txn_mgr_;
                    txn_id_t writer_id = context_->txn_->get_transaction_id();
                    bool danger =
                        txn_mgr->CheckWriteAgainstReaders(writer_id, rid, tab_name_, std::optional<RmRecord>(*rec),
                                                          std::optional<RmRecord>(*new_rec), tab_.cols);
                    if (danger) {
                        throw TransactionAbortException(writer_id, AbortReason::SSI_DANGER);
                    }
                }

                // 第四阶段：只为发生变化的索引准备旧键/新键，并预检查唯一性。
                struct IndexUpdate {
                    const IndexMeta* index;
                    std::vector<char> old_key;
                    std::vector<char> new_key;
                };
                std::vector<IndexUpdate> index_updates;
                auto txn = context_ == nullptr ? nullptr : context_->txn_;

                for (const auto& index : tab_.indexes) {
                    auto old_key = make_index_key(index, rec->data);
                    auto new_key = make_index_key(index, new_rec->data);
                    if (old_key == new_key) {
                        continue;
                    }
                    sm_manager_->remember_historical_index_key(
                        tab_name_, sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols), old_key, rid);
                    auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                                  .get();
                    std::vector<Rid> result;
                    if (ih->get_value(new_key.data(), &result, txn) &&
                        std::any_of(result.begin(), result.end(), [&](const Rid& found) { return found != rid; })) {
                        throw IndexEntryExistsError();
                    }
                    const std::string index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
                    auto candidate_rids = sm_manager_->get_historical_index_key_rids(tab_name_, index_name, new_key);
                    for (const auto& candidate_rid : candidate_rids) {
                        if (candidate_rid != rid &&
                            HistoricalIndexKeyConflictsWithTxn(fh_, candidate_rid, index, new_key, context_)) {
                            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                        }
                    }
                    index_updates.push_back(IndexUpdate{&index, std::move(old_key), std::move(new_key)});
                }
                // 第五阶段：WAL 先记录旧值和新值，再保存事务 undo 版本与新元数据。
                if (context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr) {
                    UpdateLogRecord log_record(context_->txn_->get_transaction_id(), *rec, *new_rec, rid, tab_name_);
                    log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                    lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                    context_->txn_->set_prev_lsn(lsn);
                }
                if (context_ != nullptr && context_->txn_ != nullptr) {
                    context_->txn_->append_write_record(
                        std::make_unique<WriteRecord>(WType::UPDATE_TUPLE, tab_name_, rid, *rec));

                    // Save old version as undo log for MVCC version chain
                    UndoLog undo;
                    undo.is_deleted_ = false;
                    undo.old_meta_ = fh_->get_tuple_meta(rid);
                    undo.old_tuple_data_.assign(rec->data, rec->data + rec->size);
                    undo.prev_version_ = undo.old_meta_.version_chain_head_;
                    UndoLink undo_link = context_->txn_->AppendUndoLog(undo);

                    // Track modified slot for MVCC commit
                    context_->txn_->append_modified_slot(tab_name_, rid);
                    // Update TupleMeta: mark as uncommitted, owned by this txn, chain to old version
                    TupleMeta meta;
                    meta.writer_txn_id_ = context_->txn_->get_transaction_id();
                    meta.is_committed_ = false;
                    meta.is_deleted_ = false;
                    meta.version_chain_head_ = undo_link;
                    fh_->set_tuple_meta(rid, meta);
                }

                // 第六阶段：按索引逐个替换键；任一步失败都按逆序恢复新旧键。
                std::vector<size_t> deleted_indexes;
                std::vector<size_t> inserted_indexes;
                try {
                    for (size_t i = 0; i < index_updates.size(); ++i) {
                        const auto& update = index_updates[i];
                        auto ih = sm_manager_->ihs_
                                      .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, update.index->cols))
                                      .get();
                        ih->delete_entry(update.old_key.data(), rid, txn); // 删除旧索引
                        deleted_indexes.push_back(i);
                        ih->insert_entry(update.new_key.data(), rid, txn); // 插入新索引
                        inserted_indexes.push_back(i);
                    }
                } catch (...) // 失败时回滚已经修改过的索引
                {
                    for (auto it = inserted_indexes.rbegin(); it != inserted_indexes.rend(); ++it) {
                        const auto& update = index_updates[*it];
                        auto ih = sm_manager_->ihs_
                                      .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, update.index->cols))
                                      .get();
                        ih->delete_entry(update.new_key.data(), rid, txn); // 删除新索引
                    }
                    for (auto it = deleted_indexes.rbegin(); it != deleted_indexes.rend(); ++it) {
                        const auto& update = index_updates[*it];
                        auto ih = sm_manager_->ihs_
                                      .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, update.index->cols))
                                      .get();
                        ih->insert_entry(update.old_key.data(), rid, txn, true); // 恢复旧索引
                    }
                    throw; // 仍然抛出错误
                }
                fh_->update_record(rid, new_rec->data, context_);
            }
        }
        return nullptr;
    }

    /**
     * @brief 返回目标表列元数据。
     * @return 目标表列元数据引用。
     */
    const std::vector<ColMeta>& cols() const override {
        return tab_.cols;
    }

    /**
     * @brief 返回更新节点的抽象 RID。
     * @return 抽象记录号引用。
     */
    Rid& rid() override {
        return _abstract_rid;
    }
    /**
     * @brief 返回执行器类型名称。
     * @return "UpdateExecutor"。
     */
    std::string getType() override {
        return "UpdateExecutor"; // 返回执行器的名称
    }
    /**
     * @brief 按 SET 子句把旧记录更新为新记录。
     * @param rec 待原地修改的新记录缓冲区。
     * @param old_rec 更新前的旧记录，用于自引用和表达式求值。
     * @throws RMDBError SET 表达式结果为 NULL 时抛出。
     * @throws IncompatibleTypeError 目标列与赋值类型不可转换时抛出。
     *
     * SET 支持表达式赋值、列自引用赋值、数值自增/自减/乘除以及字面量赋值；
     * 所有分支都在写入前完成类型检查，字符串字段按目标列宽清零并截断复制。
     */
    void update_record(RmRecord* rec, const RmRecord& old_rec) {
        for (const auto& set_clause : set_clauses_) {
            auto col_meta = get_col_offset(set_clause.lhs);
            char* data = rec->data + col_meta.offset;
            // 优先处理完整 QueryExpr；求值使用旧记录，避免多个 SET 子句相互污染。
            if (set_clause.rhs_expr != nullptr) {
                static const std::vector<bool> no_nulls;
                QueryExprEvaluator evaluator(tab_.cols, no_nulls, &subquery_runner_);
                EvaluatedValue value = evaluator.evaluate(*set_clause.rhs_expr, old_rec);
                if (value.is_null) {
                    throw RMDBError("UPDATE cannot store NULL values");
                }
                if (!can_cast(col_meta.type, value.cell.type)) {
                    throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(value.cell.type));
                }
                switch (col_meta.type) {
                case TYPE_INT:
                    *reinterpret_cast<int*>(data) = value.cell.type == TYPE_FLOAT
                                                        ? static_cast<int>(value.cell.float_val)
                                                        : value.cell.int_val;
                    break;
                case TYPE_FLOAT:
                    *reinterpret_cast<double*>(data) = value.cell.type == TYPE_FLOAT
                                                           ? value.cell.float_val
                                                           : static_cast<double>(value.cell.int_val);
                    break;
                case TYPE_STRING:
                case TYPE_DATETIME:
                    std::memset(data, 0, col_meta.len);
                    std::memcpy(data, value.cell.str_val.data(),
                                std::min<int>(col_meta.len, value.cell.str_val.size()));
                    break;
                }
                continue;
            }
            // 列自引用分为直接赋值和数值运算两条路径。
            if (set_clause.is_self_ref) {
                auto rhs_col_meta = get_col_offset(set_clause.rhs_col);
                if (set_clause.op == UpdateOp::ASSIGNMENT) {
                    if (col_meta.type == TYPE_INT && rhs_col_meta.type == TYPE_FLOAT) {
                        *reinterpret_cast<int*>(data) =
                            static_cast<int>(*reinterpret_cast<double*>(old_rec.data + rhs_col_meta.offset));
                    } else if (col_meta.type == TYPE_FLOAT && rhs_col_meta.type == TYPE_INT) {
                        *reinterpret_cast<double*>(data) =
                            static_cast<double>(*reinterpret_cast<int*>(old_rec.data + rhs_col_meta.offset));
                    } else if (col_meta.type == TYPE_STRING || col_meta.type == TYPE_DATETIME) {
                        if (rhs_col_meta.type != TYPE_STRING && rhs_col_meta.type != TYPE_DATETIME) {
                            throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(rhs_col_meta.type));
                        }
                        std::memset(data, 0, col_meta.len);
                        std::memcpy(data, old_rec.data + rhs_col_meta.offset, std::min(col_meta.len, rhs_col_meta.len));
                    } else if (col_meta.type == TYPE_INT) {
                        if (rhs_col_meta.type != TYPE_INT) {
                            throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(rhs_col_meta.type));
                        }
                        *reinterpret_cast<int*>(data) = *reinterpret_cast<int*>(old_rec.data + rhs_col_meta.offset);
                    } else if (col_meta.type == TYPE_FLOAT) {
                        if (rhs_col_meta.type != TYPE_FLOAT) {
                            throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(rhs_col_meta.type));
                        }
                        *reinterpret_cast<double*>(data) =
                            *reinterpret_cast<double*>(old_rec.data + rhs_col_meta.offset);
                    }
                    continue;
                }

                if ((rhs_col_meta.type != TYPE_INT && rhs_col_meta.type != TYPE_FLOAT) ||
                    (set_clause.rhs.type != TYPE_INT && set_clause.rhs.type != TYPE_FLOAT)) {
                    throw IncompatibleTypeError(coltype2str(rhs_col_meta.type), coltype2str(set_clause.rhs.type));
                }

                // 先统一提升为 double 完成运算，再按目标列类型落回 int/double。
                double base = rhs_col_meta.type == TYPE_INT
                                  ? static_cast<double>(*reinterpret_cast<int*>(old_rec.data + rhs_col_meta.offset))
                                  : *reinterpret_cast<double*>(old_rec.data + rhs_col_meta.offset);
                double delta = set_clause.rhs.type == TYPE_INT ? static_cast<double>(set_clause.rhs.int_val)
                                                               : set_clause.rhs.float_val;
                double result = base;
                switch (set_clause.op) {
                case UpdateOp::SELF_ADD:
                    result = base + delta;
                    break;
                case UpdateOp::SELF_SUB:
                    result = base - delta;
                    break;
                case UpdateOp::SELF_MUL:
                    result = base * delta;
                    break;
                case UpdateOp::SELF_DIV:
                    if (delta == 0.0F) {
                        throw InternalError("division by zero in UPDATE");
                    }
                    result = base / delta;
                    break;
                case UpdateOp::ASSIGNMENT:
                    result = base;
                    break;
                }

                switch (col_meta.type) {
                case TYPE_INT:
                    *reinterpret_cast<int*>(data) = static_cast<int>(result);
                    break;
                case TYPE_FLOAT:
                    *reinterpret_cast<double*>(data) = result;
                    break;
                case TYPE_STRING:
                case TYPE_DATETIME:
                    throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(rhs_col_meta.type));
                }
                continue;
            }
            // 最后一条路径处理普通字面量赋值。
            if (can_cast(col_meta.type, set_clause.rhs.type) == false) {
                throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(set_clause.rhs.type));
            }
            switch (col_meta.type) {
            case TYPE_INT: {
                if (set_clause.rhs.type == TYPE_INT) {
                    *(int*)data = set_clause.rhs.int_val;
                } else {
                    *(int*)data = (int)set_clause.rhs.float_val;
                }
                break;
            }
            case TYPE_FLOAT: {
                if (set_clause.rhs.type == TYPE_FLOAT) {
                    *(double*)data = set_clause.rhs.float_val;
                } else {
                    *(double*)data = static_cast<double>(set_clause.rhs.int_val);
                }
                break;
            }
            case TYPE_STRING:
            case TYPE_DATETIME: {
                int len = get_col_offset(set_clause.lhs).len;
                std::memset(data, 0, len);
                std::memcpy(data, set_clause.rhs.str_val.c_str(), std::min(len, (int)set_clause.rhs.str_val.size()));
                break;
            }
            }
        }
    }
    /**
     * @brief 查找目标表列的元数据及偏移。
     * @param target 目标列。
     * @return 匹配的列元数据。
     * @throws ColumnNotFoundError 找不到目标列时抛出。
     */
    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : tab_.cols) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
};
