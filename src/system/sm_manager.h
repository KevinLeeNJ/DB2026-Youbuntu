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

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "version_history.h"

#include "common/context.h"
#include "common/fault_injection.h"
#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "sm_defs.h"
#include "sm_meta.h"

class Context;

struct TableDirtyPageStats {
    size_t dirty_pages{0};
    size_t frame_capacity{0};
};

struct ColDef {
    std::string name; // Column name
    ColType type;     // Type of column
    int len;          // Length of column
};

/* 系统管理器，负责元数据管理和DDL语句的执行 */
class SmManager {
public:
    DbMeta db_; // 当前打开的数据库的元数据
    std::unordered_map<std::string, std::unique_ptr<RmFileHandle>>
        fhs_; // file name -> record file handle, 当前数据库中每张表的数据文件
    std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>>
        ihs_; // file name -> index file handle, 当前数据库中每个索引的文件
private:
    DiskManager* disk_manager_;
    BufferPoolManager* buffer_pool_manager_;
    RmManager* rm_manager_;
    IxManager* ix_manager_;
    std::unique_ptr<VersionHistory> version_history_;
    // Catalog lifetime is guarded independently from data/index/page latches.
    // Callers must acquire this outermost, before starting work that can take
    // transaction, row, index, page, or WAL locks.
    mutable std::shared_mutex catalog_latch_;
    std::atomic<std::uint64_t> catalog_generation_{0};
    // Runtime-only table incarnations make transient logical-row reservations
    // immune to a DROP/CREATE reusing a name or file descriptor. They are
    // protected by catalog_latch_ and intentionally never persisted.
    std::unordered_map<std::string, uint64_t> table_runtime_ids_;
    uint64_t next_table_runtime_id_{1};

    void bump_catalog_generation() noexcept {
        catalog_generation_.fetch_add(1, std::memory_order_release);
    }

public:
    using CatalogSharedGuard = std::shared_lock<std::shared_mutex>;
    using CatalogExclusiveGuard = std::unique_lock<std::shared_mutex>;

    SmManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, RmManager* rm_manager,
              IxManager* ix_manager)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), rm_manager_(rm_manager),
          ix_manager_(ix_manager), version_history_(std::make_unique<VersionHistory>(*this)) {}

    ~SmManager() {}

    // Database-global toggle controlling whether query results are appended to
    // output.txt. Shared across all client connections on this server process.
    // Reset to true on every open_db. Toggled by "set output_file on|off".
    bool output_file_enabled_{true};

    BufferPoolManager* get_bpm() {
        return buffer_pool_manager_;
    }

    RmManager* get_rm_manager() {
        return rm_manager_;
    }

    IxManager* get_ix_manager() {
        return ix_manager_;
    }

    CatalogSharedGuard acquire_catalog_shared() const {
        return CatalogSharedGuard(catalog_latch_);
    }

    CatalogExclusiveGuard acquire_catalog_exclusive() {
        return CatalogExclusiveGuard(catalog_latch_);
    }

    // The caller must hold either catalog guard. Keeping this accessor
    // non-locking avoids recursively locking the non-recursive shared_mutex.
    const std::string& get_database_identity_under_catalog_guard() const noexcept {
        return db_.name_;
    }

    std::uint64_t get_catalog_generation() const noexcept {
        return catalog_generation_.load(std::memory_order_acquire);
    }

    uint64_t get_table_runtime_id_under_catalog_guard(const std::string& tab_name) const {
        auto it = table_runtime_ids_.find(tab_name);
        if (it == table_runtime_ids_.end()) {
            throw TableNotFoundError(tab_name);
        }
        return it->second;
    }

    bool is_dir(const std::string& db_name);

    void create_db(const std::string& db_name);

    void drop_db(const std::string& db_name);

    void open_db(const std::string& db_name);

    void close_db();

    void flush_meta();

    void show_tables(ExecutionOutput* output);

    void show_index(const std::string& tab_name, ExecutionOutput* output);

    void desc_table(const std::string& tab_name, ExecutionOutput* output);

    void create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context);

    void drop_table(const std::string& tab_name, Context* context);

    void create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);

    void drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);

    void drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names, Context* context);

    void insert_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& rec);

    void delete_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec);

    void update_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec,
                                    const RmRecord& new_rec);

    VersionHistory& version_history() noexcept {
        return *version_history_;
    }
    const VersionHistory& version_history() const noexcept {
        return *version_history_;
    }

    bool flush_all_table_and_index_pages(FlushDependencyPolicy policy = FlushDependencyPolicy::Enforce());
    bool flush_recovery_pages(const std::unordered_set<std::string>& table_names);

    TableDirtyPageStats table_dirty_page_stats();
    size_t flush_dirty_table_pages(size_t max_pages);

    // Compatibility wrapper for the recovery scale harness. This remains
    // table-only and is not part of automatic checkpoint scheduling.
    size_t flush_dirty_pages(size_t max_pages);

    void rebuild_all_indexes();
    void rebuild_indexes(const std::unordered_set<std::string>& index_names);

    void refresh_index_residency();

    // Validate and freeze the publication set before a COMMIT record is made
    // durable. Publication must not discover a missing table or invalid RID
    // after durable COMMIT has already been written.
    void prepare_commit_publication(Transaction& txn) {
        auto& modified_slots = txn.get_modified_slots();
        std::sort(modified_slots.begin(), modified_slots.end(), [](const auto& lhs, const auto& rhs) {
            if (lhs.first != rhs.first) {
                return lhs.first < rhs.first;
            }
            if (lhs.second.page_no != rhs.second.page_no) {
                return lhs.second.page_no < rhs.second.page_no;
            }
            return lhs.second.slot_no < rhs.second.slot_no;
        });
        for (const auto& [tab_name, rid] : modified_slots) {
            auto table_it = fhs_.find(tab_name);
            if (table_it == fhs_.end()) {
                throw InternalError("commit publication table is missing: " + tab_name);
            }
            const auto file_hdr = table_it->second->get_file_hdr();
            if (rid.page_no < RM_FIRST_RECORD_PAGE || rid.page_no >= file_hdr.num_pages || rid.slot_no < 0 ||
                rid.slot_no >= file_hdr.num_records_per_page) {
                throw InternalError("commit publication RID is invalid");
            }
        }
    }

    // MVCC: mark all slots modified by txn as committed with the given commit_ts
    void mark_slots_committed(Transaction& txn, timestamp_t commit_ts) {
        auto& modified_slots = txn.get_modified_slots();

        size_t offset = 0;
        while (offset < modified_slots.size()) {
            const auto& [tab_name, first_rid] = modified_slots[offset];
            auto table_it = fhs_.find(tab_name);
            size_t next = offset + 1;
            while (next < modified_slots.size() && modified_slots[next].first == tab_name &&
                   modified_slots[next].second.page_no == first_rid.page_no) {
                ++next;
            }
            if (table_it == fhs_.end()) {
                // Defensive check: prepare_commit_publication() should have
                // caught this before the COMMIT WAL became durable.
                throw InternalError("commit publication table is missing: " + tab_name);
            }
            const auto file_hdr = table_it->second->get_file_hdr();
            if (first_rid.page_no < RM_FIRST_RECORD_PAGE || first_rid.page_no >= file_hdr.num_pages) {
                throw InternalError("commit publication page is invalid");
            }
            for (size_t i = offset; i < next; ++i) {
                const Rid& rid = modified_slots[i].second;
                if (rid.slot_no < 0 || rid.slot_no >= file_hdr.num_records_per_page) {
                    throw InternalError("commit publication slot is invalid");
                }
            }
            auto page_handle = table_it->second->fetch_page_handle(first_rid.page_no);
            {
                std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
                for (size_t i = offset; i < next; ++i) {
                    page_handle.get_meta(modified_slots[i].second.slot_no).is_committed_ = true;
                    page_handle.get_meta(modified_slots[i].second.slot_no).commit_ts_ = commit_ts;
                }
                if (txn.get_prev_lsn() != INVALID_LSN && page_handle.page->get_page_lsn() < txn.get_prev_lsn()) {
                    page_handle.page->set_page_lsn(txn.get_prev_lsn());
                }
                FaultInjector::Point("mid_tuple_publication");
            }
            buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
            offset = next;
        }
        txn.clear_modified_slots();
    }

    // Bulk-load a CSV file into an existing table. The path is relative to the
    // server's working directory. Reuses the insert path (WAL + index + MVCC
    // meta) in self-managed batched transactions, skipping conflict checks.
    void load_csv_data(const std::string& file_path, const std::string& tab_name, Context* context);
};
