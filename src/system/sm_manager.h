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

#include <algorithm>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "common/context.h"
#include "common/fault_injection.h"
#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "sm_defs.h"
#include "sm_meta.h"

class Context;

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
    static std::string make_historical_index_key(const std::string& tab_name, const std::string& index_name,
                                                 const std::vector<char>& key) {
        std::string combined;
        combined.reserve(tab_name.size() + index_name.size() + key.size() + 2);
        combined.append(tab_name);
        combined.push_back('\0');
        combined.append(index_name);
        combined.push_back('\0');
        combined.append(key.data(), key.size());
        return combined;
    }

    mutable std::shared_mutex historical_index_keys_latch_;
    std::unordered_map<std::string, std::vector<Rid>> historical_index_keys_;
    mutable std::mutex deleted_tuple_candidates_latch_;
    std::unordered_map<std::string, std::vector<Rid>> deleted_tuple_candidates_;

public:
    SmManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, RmManager* rm_manager,
              IxManager* ix_manager)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), rm_manager_(rm_manager),
          ix_manager_(ix_manager) {}

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

    bool is_dir(const std::string& db_name);

    void create_db(const std::string& db_name);

    void drop_db(const std::string& db_name);

    void open_db(const std::string& db_name);

    void close_db();

    void flush_meta();

    void show_tables(Context* context);

    void show_index(const std::string& tab_name, Context* context);

    void desc_table(const std::string& tab_name, Context* context);

    void create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context);

    void drop_table(const std::string& tab_name, Context* context);

    void create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);

    void drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);

    void drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names, Context* context);

    void insert_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& rec);

    void delete_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec);

    void update_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec,
                                    const RmRecord& new_rec);

    void remember_historical_index_key(const std::string& tab_name, const std::string& index_name,
                                       const std::vector<char>& key, const Rid& rid) {
        std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        auto& rids = historical_index_keys_[make_historical_index_key(tab_name, index_name, key)];
        if (std::find(rids.begin(), rids.end(), rid) == rids.end()) {
            rids.push_back(rid);
        }
    }

    std::vector<Rid> get_historical_index_key_rids(const std::string& tab_name, const std::string& index_name,
                                                   const std::vector<char>& key) const {
        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, key));
        if (it == historical_index_keys_.end()) {
            return {};
        }
        return it->second;
    }

    std::vector<Rid> get_historical_index_rids(const std::string& tab_name, const std::string& index_name) const {
        std::string prefix;
        prefix.reserve(tab_name.size() + index_name.size() + 2);
        prefix.append(tab_name);
        prefix.push_back('\0');
        prefix.append(index_name);
        prefix.push_back('\0');

        std::vector<Rid> result;
        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        for (const auto& [key, rids] : historical_index_keys_) {
            if (key.compare(0, prefix.size(), prefix) == 0) {
                result.insert(result.end(), rids.begin(), rids.end());
            }
        }
        return result;
    }

    bool has_historical_index_keys(const std::string& tab_name, const std::string& index_name) const {
        std::string prefix;
        prefix.reserve(tab_name.size() + index_name.size() + 2);
        prefix.append(tab_name);
        prefix.push_back('\0');
        prefix.append(index_name);
        prefix.push_back('\0');

        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        return std::any_of(historical_index_keys_.begin(), historical_index_keys_.end(),
                           [&](const auto& entry) { return entry.first.compare(0, prefix.size(), prefix) == 0; });
    }

    void remember_deleted_tuple_candidate(const std::string& tab_name, const Rid& rid) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto& rids = deleted_tuple_candidates_[tab_name];
        if (std::find(rids.begin(), rids.end(), rid) == rids.end()) {
            rids.push_back(rid);
        }
    }

    std::vector<Rid> get_deleted_tuple_candidates(const std::string& tab_name) const {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto it = deleted_tuple_candidates_.find(tab_name);
        if (it == deleted_tuple_candidates_.end()) {
            return {};
        }
        return it->second;
    }

    void remove_deleted_tuple_candidate(const std::string& tab_name, const Rid& rid) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto it = deleted_tuple_candidates_.find(tab_name);
        if (it == deleted_tuple_candidates_.end()) {
            return;
        }
        auto& rids = it->second;
        rids.erase(std::remove(rids.begin(), rids.end(), rid), rids.end());
        if (rids.empty()) {
            deleted_tuple_candidates_.erase(it);
        }
    }

    /** @brief 按水位线回收版本链相关历史结构。
     *  移除当前 tuple 版本已提交且 commit_ts 严格小于水位线的 RID：
     *  此时任何活跃事务（read_ts >= 水位线）都不会再回溯该 RID 的版本链，
     *  对应的历史索引键/删除候选不再被冲突检测访问，可安全删除。
     *  由 TransactionManager::GarbageCollection 在 txn_map 回收后调用。 */
    void prune_version_history(timestamp_t watermark);

    void flush_all_table_and_index_pages();

    void rebuild_all_indexes();

    void reset_all_tuple_meta_after_recovery();

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
        modified_slots.erase(std::unique(modified_slots.begin(), modified_slots.end()), modified_slots.end());

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
        modified_slots.clear();
    }

    // Bulk-load a CSV file into an existing table. The path is relative to the
    // server's working directory. Reuses the insert path (WAL + index + MVCC
    // meta) in self-managed batched transactions, skipping conflict checks.
    void load_csv_data(const std::string& file_path, const std::string& tab_name, Context* context);
};
