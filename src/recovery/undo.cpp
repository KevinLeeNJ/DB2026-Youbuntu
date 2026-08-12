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

#include "log_recovery.h"
#include "common/fault_injection.h"

#include <cstring>
#include <queue>
#include <vector>

#include "minilog.h"

namespace {
TupleMeta MakeCommittedMeta(txn_id_t writer) {
    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = writer;
    meta.is_committed_ = true;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};
    return meta;
}

TupleMeta MakeLoserMeta(txn_id_t writer) {
    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = writer;
    meta.is_committed_ = false;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};
    return meta;
}

} // namespace

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    if (!has_dml_records_) {
        LOG_INFO("recovery undo: 0 records, 0 loser txns, 0 chain records read, %llu no-undo txns pruned",
                 static_cast<unsigned long long>(pruned_no_undo_transaction_count_));
        reset_wal_if_needed();
        return;
    }

    // Undo every loser in one merged pass ordered by descending LSN, which is
    // the exact reverse of the order the records were written. Undoing whole
    // transactions one after another would let an older transaction's rollback
    // run before a newer transaction's write on the same slot is undone.
    //
    // Every record on a retained chain is fetched with its own ReadWalRecordAt
    // (header and body preads). analyze() has already removed transactions with
    // no undoable WAL, so high-frequency conflicts that wrote only BEGIN do not
    // expand this random-read phase. The remaining work is proportional to
    // DML-bearing losers, including an ABORT whose physical undo was interrupted.
    std::priority_queue<lsn_t> pending;
    for (const auto& [txn_id, last_lsn] : active_txn_last_lsn_) {
        (void)txn_id;
        if (last_lsn != INVALID_LSN) {
            pending.push(last_lsn);
        }
    }

    std::vector<char> scratch;
    WalRecordView record;
    WalDmlView dml;
    while (!pending.empty()) {
        const lsn_t current_lsn = pending.top();
        while (!pending.empty() && pending.top() == current_lsn) {
            pending.pop();
        }

        // Each of the three failures below used to `continue`, which broke that
        // loser's whole prev_lsn chain and left the rest of its writes rolled
        // forward, with no exception and nothing in the log. None of them is
        // reachable today: write_restart_offset() always writes 0, so
        // scan_begin_offset_ is always 0 and record_locations_ holds every LSN
        // in the file. They become reachable the day fuzzy checkpointing lands,
        // because a restart offset that is not <= the first LSN of every live
        // transaction turns a missing lookup into a partially rolled-back
        // transaction -- a direct violation of `final.md:342` clause 2, and one
        // that leaves no trace. Failing the recovery instead makes the next
        // process retry from the complete WAL.
        const int64_t offset = offset_of_lsn(current_lsn);
        if (offset < 0) {
            throw InternalError("recovery could not locate WAL LSN " + std::to_string(current_lsn) +
                                " on a loser's prev_lsn chain; the scan started at offset " +
                                std::to_string(scan_begin_offset_) + "; WAL retained");
        }
        if (!ReadWalRecordAt(disk_manager_, offset, scan_end_offset_, &scratch, &record)) {
            throw InternalError("recovery could not re-read the WAL record for LSN " + std::to_string(current_lsn) +
                                " at offset " + std::to_string(offset) + "; WAL retained");
        }
        ++undo_chain_record_read_count_;
        if (record.log_type == LogType::BEGIN) {
            continue;
        }
        if (record.log_type == LogType::INSERT || record.log_type == LogType::DELETE ||
            record.log_type == LogType::UPDATE) {
            if (!ParseWalDml(record, &dml)) {
                throw InternalError("recovery failed to re-parse the DML payload for LSN " +
                                    std::to_string(current_lsn) + " that analyze accepted; WAL retained");
            }
            const uint16_t table_id = intern_table(dml.table_name);
            RecoveryTable& table = tables_[table_id];
            if (table.file_handle != nullptr) {
                ++undo_applied_count_;
                switch (record.log_type) {
                case LogType::INSERT:
                    undo_insert(record, dml, table);
                    FaultInjector::Point("mid_recovery_undo");
                    break;
                case LogType::DELETE:
                    undo_delete(record, dml, table);
                    FaultInjector::Point("mid_recovery_undo");
                    break;
                default:
                    undo_update(record, dml, table);
                    FaultInjector::Point("mid_recovery_undo");
                    break;
                }
            }
        }
        if (record.prev_lsn != INVALID_LSN) {
            pending.push(record.prev_lsn);
        }
    }
    LOG_INFO("recovery undo: %llu records, %llu loser txns, %llu chain records read, %llu no-undo txns pruned",
             static_cast<unsigned long long>(undo_applied_count_),
             static_cast<unsigned long long>(active_txn_last_lsn_.size()),
             static_cast<unsigned long long>(undo_chain_record_read_count_),
             static_cast<unsigned long long>(pruned_no_undo_transaction_count_));

    // Headers first: reset_touched_tuple_meta() skips any page at or beyond
    // num_pages, so a touched page still outside the header's page count would
    // keep a loser's writer_txn_id_ and is_committed_=false on disk forever.
    // The window is not constructible today, but the order costs nothing.
    repair_touched_file_headers();
    reset_touched_tuple_meta();
    // Repair only keys named by the WAL. Rebuilding every index from every heap
    // row makes recovery proportional to the whole database even when the
    // crash affected a handful of records. The repair is idempotent and
    // preserves the existing B+tree topology. A failure rebuilds only the
    // affected index; if that also fails, recovery stops with WAL retained.
    if (!touched_sorted_.empty()) {
        FaultInjector::Point("mid_index_rebuild");
        repair_touched_indexes();
    }
    if (!sm_manager_->flush_recovery_pages(touched_tables_)) {
        // Recovery results are not durable. Keep the complete WAL and refuse
        // normal startup so the next process can retry recovery.
        throw InternalError("recovery page flush failed; WAL retained");
    }
    // 表页与索引页已落盘后，截断日志文件并推进 global_lsn。
    // 这样已 undo 完毕的 loser 日志不再残留，避免下一次重启跨轮重复 undo
    // 同 RID 上的数据（尤其是 RID 复用且内容相同时，仅靠 undo 内容守卫无法区分）。
    reset_wal_if_needed();
}

void RecoveryManager::repair_touched_file_headers() {
    std::vector<page_id_t> touched_pages;
    for (size_t begin = 0; begin < touched_sorted_.size();) {
        const uint16_t table_id = touched_sorted_[begin].table_id;
        size_t end = begin;
        touched_pages.clear();
        int32_t previous_page = -1;
        while (end < touched_sorted_.size() && touched_sorted_[end].table_id == table_id) {
            if (touched_sorted_[end].page_no != previous_page) {
                previous_page = touched_sorted_[end].page_no;
                touched_pages.push_back(previous_page);
            }
            ++end;
        }
        RmFileHandle* file_handle = tables_[table_id].file_handle;
        if (file_handle != nullptr) {
            file_handle->repair_file_header_for_pages(touched_pages);
        }
        begin = end;
    }
}

void RecoveryManager::reset_touched_tuple_meta() {
    // Every surviving tuple on a page the WAL touched is committed once redo
    // and undo are done, so the whole page's metadata is normalized inside the
    // page latch we already hold. Reaching back through set_tuple_meta once
    // per slot cost two extra buffer-pool round trips per slot.
    std::vector<int> deleted_slots;
    for (size_t begin = 0; begin < touched_sorted_.size();) {
        const uint16_t table_id = touched_sorted_[begin].table_id;
        RmFileHandle* file_handle = tables_[table_id].file_handle;
        size_t end = begin;
        while (end < touched_sorted_.size() && touched_sorted_[end].table_id == table_id) {
            ++end;
        }
        if (file_handle == nullptr) {
            begin = end;
            continue;
        }

        int32_t previous_page = -1;
        for (size_t i = begin; i < end; ++i) {
            const int32_t page_no = touched_sorted_[i].page_no;
            if (page_no == previous_page) {
                continue;
            }
            previous_page = page_no;
            if (page_no < 0 || page_no >= file_handle->get_file_hdr().num_pages) {
                continue;
            }

            deleted_slots.clear();
            RmPageHandle page_handle = file_handle->fetch_page_handle(page_no);
            bool dirtied = false;
            {
                std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
                for (int slot_no = 0; slot_no < page_handle.file_hdr->num_records_per_page; ++slot_no) {
                    if (!Bitmap::is_set(page_handle.bitmap, slot_no)) {
                        continue;
                    }
                    TupleMeta& meta = page_handle.get_meta(slot_no);
                    if (meta.is_deleted_) {
                        deleted_slots.push_back(slot_no);
                        continue;
                    }
                    meta = MakeCommittedMeta(INVALID_TXN_ID);
                    dirtied = true;
                }
            }
            buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), dirtied);
            // Removing the tombstones has to go through delete_record: it also
            // maintains the page's record count and the file's free list.
            for (const int slot_no : deleted_slots) {
                file_handle->delete_record(Rid{page_no, slot_no}, nullptr);
            }
        }
        begin = end;
    }
}
void RecoveryManager::reset_wal_if_needed() {
    if (log_manager_ == nullptr) {
        return;
    }

    // 截断 WAL 会连带丢掉 COMMIT 记录里的 commit_ts，因此在截断**之前**必须把本轮算出的
    // 计数器下界发布到 db.restart：恢复在这一刻接过了 checkpoint 的同一份责任。
    // 不发布的后果很具体：本轮恢复之后、下一次 checkpoint 之前再崩一次，第二轮恢复既
    // 没有 WAL 也没有清单，计数器又回到 0，那些没被 reset_touched_tuple_meta 归一化过
    // 的页上的已提交行会再次变成不可见。它同时给出重复恢复的幂等性：同一个崩溃状态恢复
    // 两次得到同一个计数器下界。
    // write_restart_manifest 自带 tmp + fdatasync + rename + 目录 fsync，所以它在
    // reset_log 之前就已经 durable。
    const bool truncating = max_lsn_ != INVALID_LSN;
    RestartManifest manifest;
    // 截断之后扫描只能从文件头开始；不截断时保持 analyze 本轮用过的起点。
    manifest.checkpoint_offset = truncating ? 0 : checkpoint_offset_;
    manifest.next_timestamp = get_recovered_next_timestamp();
    manifest.next_txn_id = get_recovered_next_txn_id();
    log_manager_->write_restart_manifest(manifest);
    if (!truncating) {
        return;
    }
    const lsn_t next_lsn = max_lsn_ + 1;
    FaultInjector::Point("before_recovery_wal_reset");
    log_manager_->reset_log(next_lsn);
}

bool RecoveryManager::record_exists(const RecoveryTable& table, const Rid& rid) const {
    if (table.file_handle == nullptr) {
        return false;
    }
    if (rid.page_no < 0 || rid.page_no >= table.file_handle->get_file_hdr().num_pages) {
        return false;
    }
    try {
        return table.file_handle->is_record(rid);
    } catch (const std::exception&) {
        return false;
    }
}

std::unique_ptr<RmRecord> RecoveryManager::get_record_if_exists(const RecoveryTable& table, const Rid& rid) const {
    if (!record_exists(table, rid)) {
        return nullptr;
    }
    try {
        return table.file_handle->get_record(rid, nullptr);
    } catch (const std::exception&) {
        return nullptr;
    }
}

bool RecoveryManager::record_equals(const RecoveryTable& table, const Rid& rid, const char* expected,
                                    int expected_size) const {
    auto current = get_record_if_exists(table, rid);
    if (current == nullptr) {
        return false;
    }
    return current->size == expected_size && memcmp(current->data, expected, static_cast<size_t>(expected_size)) == 0;
}

void RecoveryManager::undo_insert(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    // 幂等守卫：仅当该 rid 仍持有本 loser 事务插入的值时才删除。
    // 内容比较无法区分「loser 未刷盘的 insert」与「committed 事务在同一 RID 复用并写入
    // 相同内容」（RID 复用 + d_next_o_id 回退后复位会导致两者完全相同），故必须用
    // TupleMeta.writer_txn_id_ 判断所有权：仅当 slot 仍归属本 loser 事务时才删除。
    if (!record_exists(table, dml.rid)) {
        return;
    }
    const TupleMeta meta = table.file_handle->get_tuple_meta(dml.rid);
    if (meta.writer_txn_id_ != record.txn_id) {
        return;
    }
    table.file_handle->delete_record(dml.rid, nullptr, record.lsn);
}

void RecoveryManager::undo_delete(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    // Both paths below copy record_size bytes out of before_image; see the note
    // above redo_insert().
    validate_installable_image(table, record, dml.before_size);
    // 幂等守卫：仅当该 slot 当前为空，或仍是本 loser 写下的 MVCC tombstone 时才恢复。
    // 若 slot 已被后续 committed 事务重新写入为 live tuple，跳过，避免覆盖 committed 数据。
    const TupleMeta restored_meta = MakeLoserMeta(record.txn_id);
    if (record_exists(table, dml.rid)) {
        const TupleMeta meta = table.file_handle->get_tuple_meta(dml.rid);
        if (meta.is_deleted_ && meta.writer_txn_id_ == record.txn_id) {
            table.file_handle->apply_tuple_update(dml.rid, dml.before_image, restored_meta, record.lsn);
        }
        return;
    }
    table.file_handle->insert_record(dml.rid, const_cast<char*>(dml.before_image), record.lsn);
    table.file_handle->set_tuple_meta(dml.rid, restored_meta, record.lsn);
}

void RecoveryManager::undo_update(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    // apply_tuple_update() copies record_size bytes out of before_image; see the
    // note above redo_insert().
    validate_installable_image(table, record, dml.before_size);
    // 幂等守卫：仅当该 rid 仍持有本 loser 事务写入的 new_value 时才回滚到 old_value。
    // 若 rid 已被后续 committed 事务覆盖为其他值，跳过，避免覆盖 committed 数据。
    if (!record_exists(table, dml.rid)) {
        return;
    }
    const TupleMeta meta = table.file_handle->get_tuple_meta(dml.rid);
    if (meta.writer_txn_id_ != record.txn_id) {
        return;
    }
    if (!record_equals(table, dml.rid, dml.after_image, dml.after_size)) {
        return;
    }
    table.file_handle->apply_tuple_update(dml.rid, dml.before_image, MakeLoserMeta(record.txn_id), record.lsn);
}
