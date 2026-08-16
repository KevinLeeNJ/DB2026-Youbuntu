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

#include <cstring>
#include <memory>
#include <vector>
#include <optional>

#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"
#include "common/common.h"
#include "record/rm_file_handle.h"
#include "record/rm_scan.h"

/**
 * @brief 根据基准记录、元数据和 Undo 链重建某个历史版本的元组。
 * @param schema 表结构，用于解释记录布局。
 * @param base_tuple 当前保存的基准记录。
 * @param base_meta 基准记录对应的元数据。
 * @param undo_logs 从新到旧排列的 Undo 日志。
 * @return 能够重建时返回历史记录，否则返回 std::nullopt。
 */
auto ReconstructTuple(const TabMeta* schema, const RmRecord& base_tuple, const TupleMeta& base_meta,
                      const std::vector<UndoLog>& undo_logs) -> std::optional<RmRecord>;

/**
 * @brief 判断记录时间戳是否与事务产生写写冲突。
 * @param tuple_ts 记录当前版本的时间戳。
 * @param txn 待读取或写入该记录的事务。
 * @return 存在冲突时返回 true。
 */
auto IsWriteWriteConflict(timestamp_t tuple_ts, Transaction* txn) -> bool;

/**
 * @brief 按当前事务的隔离级别获取一个 RID 对应的可见记录版本。
 * @param fh 表记录文件句柄。
 * @param rid 待读取的记录标识。
 * @param context 当前执行上下文，可为空。
 * @return 对当前事务可见的记录副本；记录删除或没有可见版本时返回 nullptr。
 *
 * READ COMMITTED 使用语句读取时间戳，其余事务隔离级别使用事务开始时间戳。
 * 函数会沿 MVCC Undo 链向旧版本回溯，并在观察到 Undo 日志正在发布的短暂窗口
 * 时重试一次，以避免把并发回滚误判为损坏链。
 */
inline std::unique_ptr<RmRecord> GetVisibleRecord(RmFileHandle* fh, const Rid& rid, Context* context) {
    if (context == nullptr || context->txn_ == nullptr || context->txn_mgr_ == nullptr) {
        return fh->get_record(rid, context);
    }

    auto* txn = context->txn_;
    auto* txn_mgr = context->txn_mgr_;
    const timestamp_t read_ts =
        txn->get_isolation_level() == IsolationLevel::READ_COMMITTED ? txn->get_read_ts() : txn->get_start_ts();
    const txn_id_t self_id = txn->get_transaction_id();

    constexpr int MAX_DEPTH = 100;

    // 无锁读可能恰好观察到写事务回滚并释放 Undo 日志的发布窗口；此时先重读，
    // 不把暂时缺失的 Undo 链误判为记录损坏。
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto record_with_meta = fh->get_record_with_meta(rid, context);
        TupleMeta meta = record_with_meta.meta;
        auto base_record = std::move(record_with_meta.record);
        std::optional<UndoLog> current_undo;
        bool retry = false;

        // 沿当前版本和 Undo 链向后查找第一个 commit_ts 不晚于 read_ts 的版本。
        for (int depth = 0; depth < MAX_DEPTH; ++depth) {
            if (!meta.is_committed_ && meta.writer_txn_id_ == self_id) {
                if (meta.is_deleted_) {
                    return nullptr;
                }
                return base_record;
            }

            if (!meta.is_committed_) {
                if (!meta.version_chain_head_.IsValid()) {
                    return nullptr;
                }
                current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
                if (!current_undo.has_value()) {
                    retry = true;
                    break;
                }
                meta = current_undo->old_meta_;
                continue;
            }

            if (meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
                return nullptr;
            }

            if (!meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
                if (!current_undo.has_value()) {
                    return base_record;
                }
                const auto& log = *current_undo;
                auto rec = std::make_unique<RmRecord>(static_cast<int>(log.old_tuple_data_.size()));
                memcpy(rec->data, log.old_tuple_data_.data(), log.old_tuple_data_.size());
                return rec;
            }

            if (!meta.version_chain_head_.IsValid()) {
                return nullptr;
            }
            current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
            if (!current_undo.has_value()) {
                retry = true;
                break;
            }
            meta = current_undo->old_meta_;
        }

        if (!retry) {
            return nullptr;
        }
    }
    return nullptr;
}

/**
 * @brief 获取 READ COMMITTED 写操作需要的当前版本记录。
 * @param fh 表记录文件句柄。
 * @param rid 目标记录标识。
 * @param txn 当前事务，必须已经持有记录 X 锁。
 * @param context 当前执行上下文。
 * @return 记录仍可由当前事务写入时返回记录及元数据，否则返回 std::nullopt。
 * @note 调用者必须在调用前获得该记录的排他锁。
 */
inline std::optional<RmRecordWithMeta> GetCurrentRecordForRcWrite(RmFileHandle* fh, const Rid& rid, Transaction* txn,
                                                                  Context* context) {
    auto record_with_meta = fh->get_record_with_meta(rid, context);
    const TupleMeta& meta = record_with_meta.meta;
    if (meta.is_deleted_ || (!meta.is_committed_ && meta.writer_txn_id_ != txn->get_transaction_id())) {
        return std::nullopt;
    }
    return record_with_meta;
}

/**
 * @brief 比较记录中索引列拼接出的键是否等于给定的定长键。
 * @param index 索引元数据，决定参与比较的列及其偏移量。
 * @param rec_data 记录原始数据起始地址。
 * @param key 已按索引列顺序拼接的键。
 * @return 所有索引列字节都相等时返回 true。
 */
inline bool IndexKeyEquals(const IndexMeta& index, const char* rec_data, const std::vector<char>& key) {
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
        if (std::memcmp(rec_data + index.cols[i].offset, key.data() + offset, index.cols[i].len) != 0) {
            return false;
        }
        offset += index.cols[i].len;
    }
    return true;
}

/**
 * @brief 判断两条记录的长度和原始字节是否完全一致。
 * @param lhs 左侧记录。
 * @param rhs 右侧记录。
 * @return 两条记录完全相同时返回 true。
 */
inline bool RecordDataEquals(const RmRecord& lhs, const RmRecord& rhs) {
    return lhs.size == rhs.size && std::memcmp(lhs.data, rhs.data, lhs.size) == 0;
}

/**
 * @brief 检查并发删除候选中是否存在与待插入记录完全相同的冲突版本。
 * @param fh 目标表的记录文件句柄。
 * @param sm_manager 系统管理器，用于取得和清理删除候选 RID。
 * @param tab_name 目标表名。
 * @param inserted_rec 即将插入的记录。
 * @param context 当前事务上下文。
 * @return 找到当前事务不可见的并发删除同值记录时返回 true。
 *
 * 已失效的 RID 或不再是删除标记的候选会被从系统管理器中清理；属于当前事务
 * 自己删除的记录不作为冲突处理。
 */
inline bool DeletedTupleCandidatesConflictWithInsert(RmFileHandle* fh, SmManager* sm_manager,
                                                     const std::string& tab_name, const RmRecord& inserted_rec,
                                                     Context* context) {
    if (fh == nullptr || sm_manager == nullptr || context == nullptr || context->txn_ == nullptr) {
        return false;
    }

    auto* txn = context->txn_;
    auto candidate_rids = sm_manager->get_deleted_tuple_candidates(tab_name);
    for (const auto& rid : candidate_rids) {
        if (!fh->is_record(rid)) {
            sm_manager->remove_deleted_tuple_candidate(tab_name, rid);
            continue;
        }

        TupleMeta meta = fh->get_tuple_meta(rid);
        if (!meta.is_deleted_) {
            sm_manager->remove_deleted_tuple_candidate(tab_name, rid);
            continue;
        }
        if (meta.writer_txn_id_ == txn->get_transaction_id()) {
            continue;
        }

        bool concurrent_delete = !meta.is_committed_ || meta.commit_ts_ > txn->get_start_ts();
        if (!concurrent_delete) {
            continue;
        }

        auto deleted_rec = fh->get_record(rid, context);
        if (deleted_rec != nullptr && RecordDataEquals(*deleted_rec, inserted_rec)) {
            return true;
        }
    }
    return false;
}

/**
 * @brief 判断历史索引键对应的记录版本是否与当前事务发生唯一键冲突。
 * @param fh 表记录文件句柄。
 * @param rid 历史索引项指向的记录标识。
 * @param index 待检查的索引元数据。
 * @param key 要匹配的索引键。
 * @param context 当前事务上下文。
 * @return 当前版本或可见历史版本匹配该键且不属于当前事务时返回 true。
 *
 * 当前版本不匹配时，函数继续沿 Undo 链检查旧版本；每个版本都依据提交时间和
 * 删除标志判断它对当前事务是否有意义。
 */
inline bool HistoricalIndexKeyConflictsWithTxn(RmFileHandle* fh, const Rid& rid, const IndexMeta& index,
                                               const std::vector<char>& key, Context* context) {
    if (context == nullptr || context->txn_ == nullptr || context->txn_mgr_ == nullptr || !fh->is_record(rid)) {
        return false;
    }

    auto* txn = context->txn_;
    TupleMeta meta = fh->get_tuple_meta(rid);
    if (meta.writer_txn_id_ == txn->get_transaction_id()) {
        return false;
    }

    auto current_rec = fh->get_record(rid, context);
    bool current_key_matches = current_rec != nullptr && IndexKeyEquals(index, current_rec->data, key);

    if (!meta.is_committed_) {
        if (current_key_matches) {
            return true;
        }
    } else if (meta.commit_ts_ <= txn->get_start_ts()) {
        return !meta.is_deleted_ && current_key_matches;
    } else if (current_key_matches) {
        return true;
    }

    // 当前索引键不匹配并不代表历史快照中不存在冲突，因此继续检查旧版本的键。
    UndoLink link = meta.version_chain_head_;
    constexpr int MAX_DEPTH = 100;
    for (int depth = 0; depth < MAX_DEPTH && link.IsValid(); ++depth) {
        UndoLog undo = context->txn_mgr_->GetUndoLog(link);
        const TupleMeta& old_meta = undo.old_meta_;
        bool old_key_matches = !old_meta.is_deleted_ && !undo.old_tuple_data_.empty() &&
                               IndexKeyEquals(index, undo.old_tuple_data_.data(), key);

        if (!old_meta.is_committed_) {
            if (old_meta.writer_txn_id_ == txn->get_transaction_id()) {
                return old_key_matches;
            }
            link = undo.prev_version_;
            continue;
        }

        if (old_meta.commit_ts_ <= txn->get_start_ts()) {
            return old_key_matches;
        }

        link = undo.prev_version_;
    }
    return false;
}
