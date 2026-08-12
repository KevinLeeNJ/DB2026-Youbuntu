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

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cctype>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"
#include "transaction/transaction_manager.h"

namespace {

std::vector<char> MakeIndexKey(const IndexMeta& index, const char* rec_data) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
        std::memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
        offset += index.cols[i].len;
    }
    return key;
}

} // namespace

bool SmManager::flush_all_table_and_index_pages(FlushDependencyPolicy policy) {
    std::vector<int> fds;
    fds.reserve(fhs_.size() + ihs_.size());
    for (const auto& [_, fh] : fhs_) {
        fds.push_back(fh->GetFd());
    }
    for (const auto& [_, ih] : ihs_) {
        fds.push_back(ih->GetFd());
    }
    if (!buffer_pool_manager_->flush_all_pages(fds, policy)) {
        return false;
    }
    for (const auto& [_, fh] : fhs_) {
        rm_manager_->flush_file_header(fh.get());
    }
    for (const auto& [_, ih] : ihs_) {
        ix_manager_->flush_index_header(ih.get());
    }
    for (int fd : fds) {
        disk_manager_->sync_file(fd);
    }
    return true;
}

TableDirtyPageStats SmManager::table_dirty_page_stats() {
    std::shared_lock catalog_guard{catalog_latch_};
    std::vector<int> fds;
    fds.reserve(fhs_.size());
    for (const auto& [_, fh] : fhs_) {
        fds.push_back(fh->GetFd());
    }
    return TableDirtyPageStats{buffer_pool_manager_->count_dirty_pages(fds), buffer_pool_manager_->frame_capacity()};
}

size_t SmManager::flush_dirty_table_pages(size_t max_pages) {
    std::shared_lock catalog_guard{catalog_latch_};
    std::vector<int> fds;
    fds.reserve(fhs_.size());
    for (const auto& [_, fh] : fhs_) {
        fds.push_back(fh->GetFd());
    }
    return buffer_pool_manager_->flush_dirty_pages(fds, max_pages).pages_written;
}

size_t SmManager::flush_dirty_pages(size_t max_pages) {
    return flush_dirty_table_pages(max_pages);
}

bool SmManager::flush_recovery_pages(const std::unordered_set<std::string>& table_names) {
    std::vector<int> fds;
    fds.reserve(table_names.size());
    for (const auto& tab_name : table_names) {
        auto fh_it = fhs_.find(tab_name);
        if (fh_it == fhs_.end()) {
            continue;
        }
        fds.push_back(fh_it->second->GetFd());
        const auto& tab = db_.get_table(tab_name);
        for (const auto& index : tab.indexes) {
            const auto index_name = ix_manager_->get_index_name(tab_name, index.cols);
            auto ih_it = ihs_.find(index_name);
            if (ih_it != ihs_.end()) {
                fds.push_back(ih_it->second->GetFd());
            }
        }
    }
    std::sort(fds.begin(), fds.end());
    fds.erase(std::unique(fds.begin(), fds.end()), fds.end());
    if (!buffer_pool_manager_->flush_all_pages(fds)) {
        return false;
    }
    // Headers are written outside the buffer pool. Write them only after the
    // repaired pages, then sync every affected file before WAL can be reset.
    for (const auto& tab_name : table_names) {
        auto fh_it = fhs_.find(tab_name);
        if (fh_it == fhs_.end()) {
            continue;
        }
        rm_manager_->flush_file_header(fh_it->second.get());
        const auto& tab = db_.get_table(tab_name);
        for (const auto& index : tab.indexes) {
            const auto index_name = ix_manager_->get_index_name(tab_name, index.cols);
            auto ih_it = ihs_.find(index_name);
            if (ih_it != ihs_.end()) {
                ix_manager_->flush_index_header(ih_it->second.get());
            }
        }
    }
    for (const int fd : fds) {
        disk_manager_->sync_file(fd);
    }
    FaultInjector::Point("after_recovery_data_sync");
    return true;
}

void SmManager::rebuild_all_indexes() {
    rebuild_indexes({});
}

void SmManager::rebuild_indexes(const std::unordered_set<std::string>& index_names) {
    std::vector<std::pair<std::string, std::vector<IndexMeta>>> indexes_by_table;
    indexes_by_table.reserve(db_.tabs_.size());
    for (const auto& [tab_name, tab] : db_.tabs_) {
        if (!tab.indexes.empty()) {
            indexes_by_table.emplace_back(tab_name, tab.indexes);
        }
    }

    for (const auto& [tab_name, indexes] : indexes_by_table) {
        for (const auto& index : indexes) {
            std::vector<std::string> col_names;
            col_names.reserve(index.cols.size());
            for (const auto& col : index.cols) {
                col_names.emplace_back(col.name);
            }
            const std::string index_name = ix_manager_->get_index_name(tab_name, index.cols);
            if (!index_names.empty() && index_names.count(index_name) == 0) {
                continue;
            }
            const std::string backup_name = index_name + ".rebuild.bak";
            const std::string temp_base = index_name + ".rebuild.tmp";
            const std::string temp_name = ix_manager_->get_index_name(temp_base, index.cols);

            ix_manager_->create_index(temp_base, index.cols);
            auto temp_handle = ix_manager_->open_index(temp_base, index.cols);
            try {
                auto file_handle = fhs_.at(tab_name).get();
                for (RmScan scan(file_handle); !scan.is_end(); scan.next()) {
                    auto record = file_handle->get_record(scan.rid(), nullptr);
                    auto key = MakeIndexKey(index, record->data);
                    temp_handle->insert_entry(key.data(), scan.rid(), IndexWriteWalContext::UnloggedRecoveryRebuild(),
                                              true);
                }
                ix_manager_->close_index(temp_handle.get());
                temp_handle.reset();
                FaultInjector::Point("mid_index_rebuild");
                disk_manager_->sync_path(temp_name);
            } catch (...) {
                if (temp_handle != nullptr) {
                    ix_manager_->close_index(temp_handle.get());
                }
                if (disk_manager_->is_file(temp_name)) {
                    ix_manager_->destroy_index(temp_base, index.cols);
                }
                throw;
            }

            auto old_it = ihs_.find(index_name);
            if (old_it == ihs_.end()) {
                throw InternalError("missing index handle during recovery rebuild");
            }
            ix_manager_->close_index(old_it->second.get());
            old_it->second.reset();
            ihs_.erase(old_it);

            if (rename(index_name.c_str(), backup_name.c_str()) != 0) {
                ihs_.emplace(index_name, ix_manager_->open_index(tab_name, index.cols));
                throw UnixError();
            }
            if (rename(temp_name.c_str(), index_name.c_str()) != 0) {
                if (rename(backup_name.c_str(), index_name.c_str()) != 0) {
                    throw UnixError();
                }
                ihs_.emplace(index_name, ix_manager_->open_index(tab_name, index.cols));
                throw UnixError();
            }
            disk_manager_->sync_directory(".");

            try {
                ihs_.emplace(index_name, ix_manager_->open_index(tab_name, index.cols));
            } catch (...) {
                std::remove(index_name.c_str());
                if (rename(backup_name.c_str(), index_name.c_str()) != 0) {
                    throw UnixError();
                }
                ihs_.emplace(index_name, ix_manager_->open_index(tab_name, index.cols));
                disk_manager_->sync_directory(".");
                throw;
            }
            if (std::remove(backup_name.c_str()) != 0) {
                throw UnixError();
            }
            disk_manager_->sync_directory(".");
        }
    }
}

void SmManager::refresh_index_residency() {
    for (auto& [_, index_handle] : ihs_) {
        // Reopening a large benchmark must not walk and cache every internal
        // index page during startup. Individual index creation and explicit
        // full refreshes retain the include_internal=true default.
        index_handle->refresh_page_residency(false);
    }
}
