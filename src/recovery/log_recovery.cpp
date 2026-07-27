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

#include <algorithm>
#include <chrono>
#include <cstring>
#include <queue>
#include <unordered_set>
#include <vector>

#include "index/ix_index_handle.h"
#include "minilog.h"

namespace {

// Number of DML records whose pages are handed to the kernel as one readahead
// batch. Recovery stays serial; this only lifts the queue depth above one.
constexpr size_t kPrefetchBatchRecords = 1024;

// A stale index entry that keeps reappearing would otherwise loop forever.
// A key can legitimately hold only a handful of duplicates.
constexpr int kMaxDuplicateDrain = 16;

// Number of repaired keys re-probed afterwards. A repair that did not take
// effect means the tree is not in a state this repair can fix, and the index
// has to be rebuilt.
constexpr size_t kRepairSpotCheckLimit = 64;

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

void BuildIndexKey(const IndexMeta& index, const char* record_data, char* key_out) {
    int offset = 0;
    for (const auto& col : index.cols) {
        std::memcpy(key_out + offset, record_data + col.offset, col.len);
        offset += col.len;
    }
}

} // namespace

uint16_t RecoveryManager::intern_table(std::string_view table_name) {
    table_name_scratch_.assign(table_name.data(), table_name.size());
    auto it = table_ids_.find(table_name_scratch_);
    if (it != table_ids_.end()) {
        return it->second;
    }

    RecoveryTable table;
    table.name = table_name_scratch_;
    auto file_it = sm_manager_->fhs_.find(table.name);
    if (file_it != sm_manager_->fhs_.end()) {
        table.file_handle = file_it->second.get();
    }
    if (sm_manager_->db_.is_table(table.name)) {
        table.meta = &sm_manager_->db_.get_table(table.name);
    }
    const auto table_id = static_cast<uint16_t>(tables_.size());
    tables_.push_back(std::move(table));
    table_ids_.emplace(table_name_scratch_, table_id);
    return table_id;
}

int64_t RecoveryManager::offset_of_lsn(lsn_t lsn) const {
    if (lsn == INVALID_LSN) {
        return -1;
    }
    const auto it = std::lower_bound(record_locations_.begin(), record_locations_.end(), lsn,
                                     [](const WalRecordLocation& entry, lsn_t target) { return entry.lsn < target; });
    if (it == record_locations_.end() || it->lsn != lsn) {
        return -1;
    }
    return it->offset;
}

void RecoveryManager::build_touched_index() {
    touched_sorted_ = touched_;
    std::sort(touched_sorted_.begin(), touched_sorted_.end());
    touched_sorted_.erase(std::unique(touched_sorted_.begin(), touched_sorted_.end()), touched_sorted_.end());
}

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
    active_txn_last_lsn_.clear();
    committed_txns_.clear();
    tables_.clear();
    table_ids_.clear();
    touched_.clear();
    touched_sorted_.clear();
    record_locations_.clear();
    record_locations_sorted_ = true;
    touched_tables_.clear();
    has_dml_records_ = false;
    max_lsn_ = INVALID_LSN;
    checkpoint_offset_ = 0;
    scan_begin_offset_ = 0;
    scan_end_offset_ = 0;
    scanned_record_count_ = 0;
    redo_applied_count_ = 0;
    redo_skipped_count_ = 0;
    undo_applied_count_ = 0;
    index_probe_count_ = 0;
    index_mutation_count_ = 0;
    index_unchanged_key_count_ = 0;

    const int64_t file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
    if (file_size <= 0) {
        return;
    }
    scan_end_offset_ = file_size;

    // A published restart offset lets the scan start at the last checkpoint
    // instead of at the beginning of the file.
    {
        std::vector<char> scratch;
        WalRecordView checkpoint;
        const int64_t restart_offset = log_manager_ != nullptr ? log_manager_->read_restart_offset() : 0;
        if (restart_offset > 0 && ReadWalRecordAt(disk_manager_, restart_offset, file_size, &scratch, &checkpoint) &&
            checkpoint.log_type == LogType::CHECKPOINT) {
            auto record = DeserializeLogRecord(checkpoint.bytes, static_cast<int>(checkpoint.total_len));
            if (record != nullptr && record->log_type_ == LogType::CHECKPOINT) {
                scan_begin_offset_ = restart_offset;
                checkpoint_offset_ = restart_offset;
                const auto* checkpoint_log = static_cast<const CheckpointLogRecord*>(record.get());
                for (const auto& [txn_id, last_lsn] : checkpoint_log->active_txns_) {
                    active_txn_last_lsn_[txn_id] = last_lsn;
                }
            }
        }
    }

    WalReader reader(disk_manager_, scan_begin_offset_, file_size);
    WalRecordView record;
    WalDmlView dml;
    lsn_t previous_lsn = INVALID_LSN;
    bool torn_payload = false;
    while (!torn_payload && reader.next(&record)) {
        ++scanned_record_count_;
        switch (record.log_type) {
        case LogType::BEGIN:
            active_txn_last_lsn_[record.txn_id] = record.lsn;
            break;
        case LogType::INSERT:
        case LogType::DELETE:
        case LogType::UPDATE: {
            if (!ParseWalDml(record, &dml)) {
                // A payload that does not parse cannot be replayed; the WAL
                // ends here, exactly as a short header would end it.
                scan_end_offset_ = record.offset;
                torn_payload = true;
                --scanned_record_count_;
                break;
            }
            active_txn_last_lsn_[record.txn_id] = record.lsn;
            has_dml_records_ = true;
            const uint16_t table_id = intern_table(dml.table_name);
            touched_tables_.insert(tables_[table_id].name);
            TouchedTuple touched;
            touched.table_id = table_id;
            touched.page_no = dml.rid.page_no;
            touched.slot_no = static_cast<int16_t>(dml.rid.slot_no);
            touched_.push_back(touched);
            break;
        }
        case LogType::COMMIT:
            committed_txns_.insert(record.txn_id);
            active_txn_last_lsn_.erase(record.txn_id);
            break;
        case LogType::ABORT:
            // ABORT only records that rollback was requested. This system does not write CLRs,
            // so recovery must still idempotently undo the transaction's original DML records.
            active_txn_last_lsn_[record.txn_id] = record.lsn;
            break;
        case LogType::CHECKPOINT:
            break;
        }
        if (torn_payload) {
            break;
        }

        record_locations_.push_back(WalRecordLocation{record.lsn, record.offset});
        if (record.lsn != INVALID_LSN && previous_lsn != INVALID_LSN && record.lsn <= previous_lsn) {
            record_locations_sorted_ = false;
        }
        previous_lsn = record.lsn;
        if (record.lsn != INVALID_LSN && (max_lsn_ == INVALID_LSN || record.lsn > max_lsn_)) {
            max_lsn_ = record.lsn;
        }
    }
    if (!torn_payload) {
        scan_end_offset_ = std::min(scan_end_offset_, reader.next_offset());
    }

    if (!record_locations_sorted_) {
        // LSNs are handed out under the append latch, so the file is normally
        // already ordered. Sort defensively so the undo lookup stays valid.
        std::sort(record_locations_.begin(), record_locations_.end(),
                  [](const WalRecordLocation& left, const WalRecordLocation& right) { return left.lsn < right.lsn; });
    }
    build_touched_index();

    LOG_INFO("recovery analyze: %llu records, %llu dml, %zu distinct rids, %llu wal preads, loser txns %zu",
             static_cast<unsigned long long>(scanned_record_count_), static_cast<unsigned long long>(touched_.size()),
             touched_sorted_.size(), static_cast<unsigned long long>(reader.read_count()), active_txn_last_lsn_.size());

    if (has_dml_records_) {
        // A crash may persist newly allocated record pages before the short
        // file header update reaches disk. Reconcile only pages named by WAL
        // before redo/undo fetches their RIDs.
        repair_touched_file_headers();
    }
}

void RecoveryManager::prefetch_redo_batch(size_t from_index) {
    if (from_index >= touched_.size()) {
        return;
    }
    const size_t end_index = std::min(from_index + kPrefetchBatchRecords, touched_.size());
    prefetch_scratch_.assign(touched_.begin() + static_cast<std::ptrdiff_t>(from_index),
                             touched_.begin() + static_cast<std::ptrdiff_t>(end_index));
    std::sort(prefetch_scratch_.begin(), prefetch_scratch_.end());
    uint16_t previous_table = 0;
    int32_t previous_page = -1;
    for (const auto& touched : prefetch_scratch_) {
        if (touched.page_no == previous_page && touched.table_id == previous_table && previous_page >= 0) {
            continue;
        }
        previous_table = touched.table_id;
        previous_page = touched.page_no;
        RmFileHandle* file_handle = tables_[touched.table_id].file_handle;
        if (file_handle != nullptr) {
            disk_manager_->prefetch_page(file_handle->GetFd(), touched.page_no);
        }
    }
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
    if (!has_dml_records_ && scanned_record_count_ == 0) {
        return;
    }

    // The pages the whole pass will touch are already known, so the kernel can
    // be reading the next batch while this one is applied.
    prefetch_redo_batch(0);
    prefetch_redo_batch(kPrefetchBatchRecords);

    WalReader reader(disk_manager_, scan_begin_offset_, scan_end_offset_);
    WalRecordView record;
    WalDmlView dml;
    size_t dml_index = 0;
    while (reader.next(&record)) {
        switch (record.log_type) {
        case LogType::INSERT:
        case LogType::DELETE:
        case LogType::UPDATE:
            break;
        default:
            continue;
        }
        if (!ParseWalDml(record, &dml)) {
            break;
        }
        const size_t current_index = dml_index++;
        if (current_index % kPrefetchBatchRecords == 0 && current_index > 0) {
            prefetch_redo_batch(current_index + 2 * kPrefetchBatchRecords);
        }
        if (committed_txns_.count(record.txn_id) == 0) {
            ++redo_skipped_count_;
            continue;
        }
        RecoveryTable* table = current_index < touched_.size() ? table_at(touched_[current_index].table_id) : nullptr;
        if (table == nullptr || table->file_handle == nullptr) {
            continue;
        }
        ++redo_applied_count_;
        switch (record.log_type) {
        case LogType::INSERT:
            redo_insert(record, dml, *table);
            FaultInjector::Point("mid_recovery_redo");
            break;
        case LogType::DELETE:
            redo_delete(record, dml, *table);
            FaultInjector::Point("mid_recovery_redo");
            break;
        case LogType::UPDATE:
            redo_update(record, dml, *table);
            FaultInjector::Point("mid_recovery_redo");
            break;
        default:
            break;
        }
    }
    LOG_INFO("recovery redo: applied %llu, skipped %llu, wal preads %llu",
             static_cast<unsigned long long>(redo_applied_count_), static_cast<unsigned long long>(redo_skipped_count_),
             static_cast<unsigned long long>(reader.read_count()));
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    if (!has_dml_records_) {
        reset_wal_if_needed();
        return;
    }

    // Undo every loser in one merged pass ordered by descending LSN, which is
    // the exact reverse of the order the records were written. Undoing whole
    // transactions one after another would let an older transaction's rollback
    // run before a newer transaction's write on the same slot is undone.
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

        const int64_t offset = offset_of_lsn(current_lsn);
        if (offset < 0 || !ReadWalRecordAt(disk_manager_, offset, scan_end_offset_, &scratch, &record)) {
            continue;
        }
        if (record.log_type == LogType::BEGIN) {
            continue;
        }
        if (record.log_type == LogType::INSERT || record.log_type == LogType::DELETE ||
            record.log_type == LogType::UPDATE) {
            if (!ParseWalDml(record, &dml)) {
                continue;
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
    LOG_INFO("recovery undo: %llu records", static_cast<unsigned long long>(undo_applied_count_));

    reset_touched_tuple_meta();
    repair_touched_file_headers();
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

void RecoveryManager::collect_wal_index_keys(std::map<std::string, IndexRepairPlan>* plans) {
    // Every image the WAL mentions is a candidate stale entry: without index
    // page LSNs recovery cannot tell whether the matching index write reached
    // disk, so each one has to be reconciled against the tree.
    WalReader reader(disk_manager_, scan_begin_offset_, scan_end_offset_);
    WalRecordView record;
    WalDmlView dml;
    while (reader.next(&record)) {
        switch (record.log_type) {
        case LogType::INSERT:
        case LogType::DELETE:
        case LogType::UPDATE:
            break;
        default:
            continue;
        }
        if (!ParseWalDml(record, &dml)) {
            break;
        }
        RecoveryTable& table = tables_[intern_table(dml.table_name)];
        if (table.meta == nullptr) {
            continue;
        }
        for (const auto& index_meta : table.meta->indexes) {
            const auto index_name = sm_manager_->get_ix_manager()->get_index_name(table.name, index_meta.cols);
            auto plan_it = plans->find(index_name);
            if (plan_it == plans->end()) {
                continue;
            }
            IndexRepairPlan& plan = plan_it->second;
            for (const char* image : {dml.before_image, dml.after_image}) {
                if (image == nullptr) {
                    continue;
                }
                const auto key_slot = static_cast<uint32_t>(plan.key_arena.size() / plan.key_len);
                plan.key_arena.resize(plan.key_arena.size() + static_cast<size_t>(plan.key_len));
                BuildIndexKey(index_meta, image, plan.key_arena.data() + static_cast<size_t>(key_slot) * plan.key_len);
                plan.entries.push_back(IndexRepairEntry{key_slot, dml.rid, false});
            }
        }
    }
}

void RecoveryManager::collect_heap_index_keys(std::map<std::string, IndexRepairPlan>* plans) {
    // Read the final tuple of every touched RID one page at a time. The RIDs
    // are already ordered by page, so this is a sequential sweep and the keys
    // of every index on the table come out of the same page pin.
    for (size_t begin = 0; begin < touched_sorted_.size();) {
        const uint16_t table_id = touched_sorted_[begin].table_id;
        size_t end = begin;
        while (end < touched_sorted_.size() && touched_sorted_[end].table_id == table_id) {
            ++end;
        }
        RecoveryTable& table = tables_[table_id];
        if (table.file_handle == nullptr || table.meta == nullptr) {
            begin = end;
            continue;
        }

        // Resolve each index's plan once per table instead of once per row.
        std::vector<std::pair<const IndexMeta*, IndexRepairPlan*>> table_plans;
        for (const auto& index_meta : table.meta->indexes) {
            const auto index_name = sm_manager_->get_ix_manager()->get_index_name(table.name, index_meta.cols);
            auto plan_it = plans->find(index_name);
            if (plan_it != plans->end()) {
                table_plans.emplace_back(&index_meta, &plan_it->second);
            }
        }
        if (table_plans.empty()) {
            begin = end;
            continue;
        }

        const int num_pages = table.file_handle->get_file_hdr().num_pages;
        for (size_t i = begin; i < end;) {
            const int32_t page_no = touched_sorted_[i].page_no;
            size_t page_end = i;
            while (page_end < end && touched_sorted_[page_end].page_no == page_no) {
                ++page_end;
            }
            if (page_no < 0 || page_no >= num_pages) {
                i = page_end;
                continue;
            }

            RmPageHandle page_handle = table.file_handle->fetch_page_handle(page_no);
            {
                std::shared_lock<std::shared_mutex> page_lock(page_handle.page->latch());
                for (size_t j = i; j < page_end; ++j) {
                    const int slot_no = touched_sorted_[j].slot_no;
                    if (slot_no < 0 || slot_no >= page_handle.file_hdr->num_records_per_page) {
                        continue;
                    }
                    if (!Bitmap::is_set(page_handle.bitmap, slot_no) || page_handle.get_meta(slot_no).is_deleted_) {
                        continue;
                    }
                    const char* row = page_handle.get_slot(slot_no);
                    const Rid rid{page_no, slot_no};
                    for (const auto& [index_meta, plan] : table_plans) {
                        const auto key_slot = static_cast<uint32_t>(plan->key_arena.size() / plan->key_len);
                        plan->key_arena.resize(plan->key_arena.size() + static_cast<size_t>(plan->key_len));
                        BuildIndexKey(*index_meta, row,
                                      plan->key_arena.data() + static_cast<size_t>(key_slot) * plan->key_len);
                        plan->entries.push_back(IndexRepairEntry{key_slot, rid, true});
                    }
                }
            }
            buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            i = page_end;
        }
        begin = end;
    }
}

void RecoveryManager::plan_touched_indexes(std::map<std::string, IndexRepairPlan>* plans) {
    // One plan per index that a touched table owns and that is actually open.
    for (const auto& touched : touched_sorted_) {
        RecoveryTable& table = tables_[touched.table_id];
        if (table.meta == nullptr) {
            continue;
        }
        for (const auto& index_meta : table.meta->indexes) {
            const auto index_name = sm_manager_->get_ix_manager()->get_index_name(table.name, index_meta.cols);
            if (plans->count(index_name) != 0 || sm_manager_->ihs_.count(index_name) == 0) {
                continue;
            }
            IndexRepairPlan plan;
            plan.index_name = index_name;
            plan.key_len = index_meta.col_tot_len;
            plans->emplace(index_name, std::move(plan));
        }
    }
}

void RecoveryManager::collect_index_repair_keys(std::map<std::string, IndexRepairPlan>* plans) {
    if (plans->empty()) {
        return;
    }
    collect_wal_index_keys(plans);
    collect_heap_index_keys(plans);
}

bool RecoveryManager::apply_index_repair_plan(IxIndexHandle* index, const IndexMeta& index_meta,
                                              IndexRepairPlan* plan) {
    const int key_len = plan->key_len;
    const char* arena = plan->key_arena.data();
    const auto key_of = [arena, key_len](const IndexRepairEntry& entry) {
        return arena + static_cast<size_t>(entry.key_slot) * key_len;
    };

    // Group by key in B+tree order so the leaves are visited left to right and
    // the internal nodes stay hot, then let each group make one decision.
    std::sort(
        plan->entries.begin(), plan->entries.end(), [&](const IndexRepairEntry& left, const IndexRepairEntry& right) {
            const int cmp = ix_compare(key_of(left), key_of(right), index->get_col_types(), index->get_col_lens());
            if (cmp != 0) {
                return cmp < 0;
            }
            if (!(left.rid == right.rid)) {
                return left.rid.page_no != right.rid.page_no ? left.rid.page_no < right.rid.page_no
                                                             : left.rid.slot_no < right.rid.slot_no;
            }
            return static_cast<int>(left.from_heap) < static_cast<int>(right.from_heap);
        });

    std::vector<Rid> existing;
    std::vector<Rid> required;   // must be present when the group is done
    std::vector<Rid> candidates; // WAL images that may be stale entries
    std::vector<const char*> spot_check_keys;
    std::vector<Rid> spot_check_rids;

    for (size_t begin = 0; begin < plan->entries.size();) {
        size_t end = begin + 1;
        const char* key = key_of(plan->entries[begin]);
        while (end < plan->entries.size() &&
               ix_compare(key_of(plan->entries[end]), key, index->get_col_types(), index->get_col_lens()) == 0) {
            ++end;
        }

        required.clear();
        candidates.clear();
        for (size_t i = begin; i < end; ++i) {
            auto& target = plan->entries[i].from_heap ? required : candidates;
            if (target.empty() || !(target.back() == plan->entries[i].rid)) {
                target.push_back(plan->entries[i].rid);
            }
        }

        existing.clear();
        ++index_probe_count_;
        index->get_value(key, &existing, nullptr);

        // Deleting every WAL image and then reinstalling the live keys leaves
        // this key holding exactly `required`. When the tree already holds
        // exactly that, the sequence is a no-op and the traversals, page
        // dirtying and node merges it would cause are all pure waste.
        const auto contains = [](const std::vector<Rid>& haystack, const Rid& needle) {
            return std::find(haystack.begin(), haystack.end(), needle) != haystack.end();
        };
        bool already_correct = true;
        for (const Rid& rid : required) {
            if (!contains(existing, rid)) {
                already_correct = false;
                break;
            }
        }
        if (already_correct) {
            for (const Rid& rid : existing) {
                if (contains(candidates, rid) && !contains(required, rid)) {
                    already_correct = false;
                    break;
                }
            }
        }
        if (already_correct) {
            ++index_unchanged_key_count_;
            begin = end;
            continue;
        }

        for (const Rid& rid : candidates) {
            if (!contains(existing, rid)) {
                continue;
            }
            // Drain duplicates: an interrupted index write can leave the same
            // pair more than once, and the old repair removed one copy per WAL
            // record that mentioned it.
            for (int attempt = 0; attempt < kMaxDuplicateDrain; ++attempt) {
                if (!index->delete_entry(key, rid, nullptr)) {
                    break;
                }
                ++index_mutation_count_;
            }
        }
        for (const Rid& rid : required) {
            if (contains(existing, rid) && !contains(candidates, rid)) {
                continue; // still installed and never deleted above
            }
            index->insert_entry(key, rid, nullptr, true);
            ++index_mutation_count_;
            if (spot_check_keys.size() < kRepairSpotCheckLimit) {
                spot_check_keys.push_back(key);
                spot_check_rids.push_back(rid);
            }
        }
        begin = end;
    }

    // A repair that did not take is evidence the tree cannot be fixed in place.
    for (size_t i = 0; i < spot_check_keys.size(); ++i) {
        existing.clear();
        index->get_value(spot_check_keys[i], &existing, nullptr);
        if (std::find(existing.begin(), existing.end(), spot_check_rids[i]) == existing.end()) {
            LOG_WARN("recovery index %s did not accept a repaired key", plan->index_name.c_str());
            return false;
        }
    }
    (void)index_meta;
    return true;
}

void RecoveryManager::repair_touched_indexes() {
    std::map<std::string, IndexRepairPlan> plans;
    // Only names and key widths at this point. Collecting the keys costs a WAL
    // pass and a heap sweep, so it waits until the gate has decided which
    // indexes are actually going to be repaired in place.
    plan_touched_indexes(&plans);
    if (plans.empty()) {
        return;
    }

    std::unordered_set<std::string> indexes_to_rebuild;
    // Structure gate. Two things happen here, in this order:
    //
    // 1. last_leaf_ is repaired. It is an append hint that reaches disk only
    //    when a checkpoint publishes the header, so after a crash it routinely
    //    names a leaf that has since been split. That alone makes the leaf
    //    chain look broken and makes delete_entry stop scanning early.
    // 2. The tree is validated. A tree that fails its own invariants cannot be
    //    fixed key by key, so it goes to the rebuild set instead. Until now
    //    nothing ever called the checker, which is why a stale root pointer on
    //    a self-consistent tree raised no exception at all.
    const auto validate_begin = std::chrono::steady_clock::now();
    size_t validated_indexes = 0;
    size_t repaired_endpoints = 0;
    for (const auto& [index_name, plan] : plans) {
        (void)plan;
        auto index_it = sm_manager_->ihs_.find(index_name);
        if (index_it == sm_manager_->ihs_.end()) {
            continue;
        }
        ++validated_indexes;
        if (!index_it->second->refresh_leaf_chain_endpoint()) {
            LOG_WARN("recovery could not follow the leaf chain of index %s", index_name.c_str());
            indexes_to_rebuild.insert(index_name);
            continue;
        }
        ++repaired_endpoints;
        if (!index_it->second->validate_structure()) {
            LOG_WARN("recovery found structurally invalid index %s", index_name.c_str());
            indexes_to_rebuild.insert(index_name);
        }
    }
    LOG_INFO("recovery index structure gate: %zu indexes, %zu leaf endpoints refreshed, %zu to rebuild, %lld ms",
             validated_indexes, repaired_endpoints, indexes_to_rebuild.size(),
             static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - validate_begin)
                                        .count()));

    // Drop the plans for indexes that are going to be rebuilt anyway, then pay
    // for the key collection only for the rest.
    for (auto it = plans.begin(); it != plans.end();) {
        it = indexes_to_rebuild.count(it->first) != 0 ? plans.erase(it) : std::next(it);
    }
    collect_index_repair_keys(&plans);

    for (auto& [index_name, plan] : plans) {
        auto index_it = sm_manager_->ihs_.find(index_name);
        if (index_it == sm_manager_->ihs_.end()) {
            continue;
        }
        const IndexMeta* index_meta = nullptr;
        for (auto& table : tables_) {
            if (table.meta == nullptr) {
                continue;
            }
            for (const auto& candidate : table.meta->indexes) {
                if (sm_manager_->get_ix_manager()->get_index_name(table.name, candidate.cols) == index_name) {
                    index_meta = &candidate;
                    break;
                }
            }
            if (index_meta != nullptr) {
                break;
            }
        }
        if (index_meta == nullptr) {
            continue;
        }
        try {
            if (!apply_index_repair_plan(index_it->second.get(), *index_meta, &plan)) {
                indexes_to_rebuild.insert(index_name);
            }
        } catch (const std::exception& error) {
            LOG_WARN("recovery found structurally inconsistent index %s: %s", index_name.c_str(), error.what());
            indexes_to_rebuild.insert(index_name);
        }
    }

    LOG_INFO("recovery index repair: %llu probes, %llu mutations, %llu keys already correct",
             static_cast<unsigned long long>(index_probe_count_),
             static_cast<unsigned long long>(index_mutation_count_),
             static_cast<unsigned long long>(index_unchanged_key_count_));

    if (!indexes_to_rebuild.empty()) {
        sm_manager_->rebuild_indexes(indexes_to_rebuild);
    }
}

void RecoveryManager::reset_wal_if_needed() {
    if (log_manager_ == nullptr || max_lsn_ == INVALID_LSN) {
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

bool RecoveryManager::redo_existing_slot(RecoveryTable& table, const Rid& rid, const char* image, int image_size,
                                         const TupleMeta& meta, lsn_t lsn) {
    // Installing an image plus its metadata used to cost two to four separate
    // buffer-pool round trips per record: an existence probe, a body write and a
    // metadata write, each fetching and unpinning the same page. One pin under
    // one exclusive page latch does all of it. That matters because the table is
    // several times larger than the pool, so every extra fetch is an extra
    // random read.
    if (table.file_handle == nullptr || rid.page_no < 0 || rid.page_no >= table.file_handle->get_file_hdr().num_pages) {
        return false;
    }
    RmPageHandle page_handle;
    try {
        page_handle = table.file_handle->fetch_page_handle(rid.page_no);
    } catch (const std::exception&) {
        return false;
    }
    bool applied = false;
    {
        std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
        if (rid.slot_no >= 0 && rid.slot_no < page_handle.file_hdr->num_records_per_page &&
            Bitmap::is_set(page_handle.bitmap, rid.slot_no) && image_size == page_handle.file_hdr->record_size) {
            memcpy(page_handle.get_slot(rid.slot_no), image, static_cast<size_t>(image_size));
            page_handle.get_meta(rid.slot_no) = meta;
            if (lsn != INVALID_LSN && page_handle.page->get_page_lsn() < lsn) {
                page_handle.page->set_page_lsn(lsn);
            }
            applied = true;
        }
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), applied);
    return applied;
}

void RecoveryManager::redo_insert(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    // The committed metadata is installed even when the tuple body was already
    // present: a page LSN can be ahead because the same page also holds a loser
    // operation, so it must not be used to skip this redo.
    const TupleMeta meta = MakeCommittedMeta(record.txn_id);
    if (redo_existing_slot(table, dml.rid, dml.after_image, dml.after_size, meta, record.lsn)) {
        return;
    }
    table.file_handle->insert_record(dml.rid, const_cast<char*>(dml.after_image), record.lsn);
    if (table.file_handle->is_record(dml.rid)) {
        table.file_handle->set_tuple_meta(dml.rid, meta, record.lsn);
    }
}

void RecoveryManager::redo_delete(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    TupleMeta meta = MakeCommittedMeta(record.txn_id);
    meta.is_deleted_ = true;
    if (redo_existing_slot(table, dml.rid, dml.before_image, dml.before_size, meta, record.lsn)) {
        return;
    }
    table.file_handle->insert_record(dml.rid, const_cast<char*>(dml.before_image), record.lsn);
    if (table.file_handle->is_record(dml.rid)) {
        table.file_handle->set_tuple_meta(dml.rid, meta, record.lsn);
    }
}

void RecoveryManager::redo_update(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    const TupleMeta meta = MakeCommittedMeta(record.txn_id);
    if (redo_existing_slot(table, dml.rid, dml.after_image, dml.after_size, meta, record.lsn)) {
        return;
    }
    table.file_handle->insert_record(dml.rid, const_cast<char*>(dml.after_image), record.lsn);
    if (table.file_handle->is_record(dml.rid)) {
        table.file_handle->set_tuple_meta(dml.rid, meta, record.lsn);
    }
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
