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
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 删除指定 RID 集合中满足条件的记录。
 *
 * 删除会重新读取记录的可见版本并再次检查谓词；事务模式下还负责锁、MVCC
 * 版本链、WAL、历史索引键以及 SSI 写依赖，非事务模式则直接删除物理记录。
 */
class DeleteExecutor : public AbstractExecutor {
private:
    TabMeta tab_;                  // 表的元数据
    std::vector<Condition> conds_; // delete的条件
    RmFileHandle* fh_;             // 表的数据文件句柄
    std::vector<Rid> rids_;        // 需要删除的记录的位置
    std::string tab_name_;         // 表名称
    SmManager* sm_manager_;

public:
    /**
     * @brief 创建删除执行器。
     * @param sm_manager 系统管理器。
     * @param tab_name 目标表名。
     * @param conds 删除前需要满足的条件。
     * @param rids 待检查的记录 RID 列表。
     * @param context 当前执行上下文。
     */
    DeleteExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    /**
     * @brief 执行删除操作。
     * @return DML 不产生上层数据行，始终返回 nullptr。
     * @throws TransactionAbortException 锁、MVCC 或 SSI 冲突时抛出。
     *
     * 对每个候选 RID 先取可见记录并过滤，再执行并发检查。事务模式下保存
     * undo 版本并写入墓碑元数据；索引删除过程若失败会按逆序恢复已删除索引。
     */
    std::unique_ptr<RmRecord> Next() override {
        if (rids_.empty()) {
            return nullptr; // 没有更多记录可以删除
        }
        // 先基于调用方提供的 RID 重新读取可见版本，避免直接操作过期快照。
        for (Rid rid : rids_) {
            auto rec = GetVisibleRecord(fh_, rid, context_);
            if (rec == nullptr) {
                continue;
            }
            char* rec_data = rec->data;
            bool match = true;
            for (const auto& cond : conds_) {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (!match) {
                continue; // 如果记录不匹配条件，则跳过删除
            }
            // 第一阶段：获取记录锁，并在 RC 下刷新到最新版本后重新验证谓词。
            if (context_ != nullptr && context_->txn_ != nullptr) {
                auto txn = context_->txn_;
                if (context_->lock_mgr_ != nullptr &&
                    !context_->lock_mgr_->lock_exclusive_on_record(txn, rid, fh_->GetFd())) {
                    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                }
                if (txn->get_isolation_level() == IsolationLevel::READ_COMMITTED && context_->txn_mgr_ != nullptr) {
                    auto current_record = GetCurrentRecordForRcWrite(fh_, rid, txn, context_);
                    if (!current_record.has_value()) {
                        continue;
                    }
                    rec = std::move(current_record->record);
                    rec_data = rec->data;
                    bool latest_match = true;
                    for (const auto& cond : conds_) {
                        if (!compare(cond, *rec)) {
                            latest_match = false;
                            break;
                        }
                    }
                    if (!latest_match) {
                        continue;
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

                // Serializable 阶段：检查本次删除是否与其他事务的读集合形成危险结构。
                if (txn->get_isolation_level() == IsolationLevel::SERIALIZABLE && context_->txn_mgr_ != nullptr) {
                    auto* txn_mgr = context_->txn_mgr_;
                    if (txn_mgr->CheckWriteAgainstReaders(txn->get_transaction_id(), rid, tab_name_,
                                                          std::optional<RmRecord>(*rec), std::nullopt, tab_.cols)) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::SSI_DANGER);
                    }
                }
            }
            // 第二阶段：WAL 记录必须先于物理删除产生。
            if (context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr) {
                DeleteLogRecord log_record(context_->txn_->get_transaction_id(), *rec, rid, tab_name_);
                log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                context_->txn_->set_prev_lsn(lsn);
            }
            auto undo_record = context_ != nullptr && context_->txn_ != nullptr
                                   ? std::make_unique<WriteRecord>(WType::DELETE_TUPLE, tab_name_, rid, *rec)
                                   : nullptr;
            // 第三阶段：删除所有索引键，并记录已完成项以便异常时回滚。
            struct DeletedIndex {
                const IndexMeta* index;
                std::vector<char> key;
            };
            std::vector<DeletedIndex> deleted_indexes;
            try {
                for (auto& index : tab_.indexes) {
                    auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                                  .get();
                    std::vector<char> key(index.col_tot_len);
                    int offset = 0;
                    for (int i = 0; i < index.col_num; ++i) {
                        std::memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
                        offset += index.cols[i].len;
                    }
                    sm_manager_->remember_historical_index_key(
                        tab_name_, sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols), key, rid);
                    ih->delete_entry(key.data(), rid, context_ == nullptr ? nullptr : context_->txn_);
                    deleted_indexes.push_back(DeletedIndex{&index, std::move(key)});
                }
            } catch (...) {
                for (auto it = deleted_indexes.rbegin(); it != deleted_indexes.rend(); ++it) {
                    auto ih =
                        sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, it->index->cols))
                            .get();
                    ih->insert_entry(it->key.data(), rid, context_ == nullptr ? nullptr : context_->txn_, true);
                }
                // undo_record is automatically cleaned up by unique_ptr
                throw;
            }
            // 第四阶段：事务删除使用 undo + tombstone 保留旧版本；无事务时直接释放槽位。
            if (undo_record != nullptr) {
                UndoLog undo;
                undo.is_deleted_ = true;
                undo.old_meta_ = fh_->get_tuple_meta(rid);
                undo.old_tuple_data_.assign(rec->data, rec->data + rec->size);
                undo.prev_version_ = undo.old_meta_.version_chain_head_;
                UndoLink undo_link = context_->txn_->AppendUndoLog(undo);

                context_->txn_->append_write_record(std::move(undo_record));
                context_->txn_->append_modified_slot(tab_name_, rid);

                TupleMeta tombstone;
                tombstone.writer_txn_id_ = context_->txn_->get_transaction_id();
                tombstone.is_committed_ = false;
                tombstone.is_deleted_ = true;
                tombstone.version_chain_head_ = undo_link;
                fh_->set_tuple_meta(rid, tombstone);
                sm_manager_->remember_deleted_tuple_candidate(tab_name_, rid);
            } else {
                fh_->delete_record(rid, context_);
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
     * @brief 返回执行器类型名称。
     * @return "DeleteExecutor"。
     */
    std::string getType() override {
        return "DeleteExecutor"; // 返回执行器的名称
    }
    /**
     * @brief 返回删除节点的抽象 RID。
     * @return 抽象记录号引用。
     */
    Rid& rid() override {
        return _abstract_rid;
    }
    /**
     * @brief 查找目标表列的元数据及偏移。
     * @param target 目标列。
     * @return 匹配的列元数据。
     * @throws ColumnNotFoundError 找不到目标列时抛出。
     */
    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : tab_.cols) {
            if (col.tab_name == tab_name_ && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
};
