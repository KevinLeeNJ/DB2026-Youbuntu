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
#include <string_view>
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

void InsertIndexEntryIdempotent(SmManager* sm_manager, const std::string& tab_name, const IndexMeta& index,
                                const RmRecord& rec, const Rid& rid) {
    auto key = MakeIndexKey(index, rec.data);
    auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
    std::vector<Rid> existing;
    if (ih->get_value(key.data(), &existing, nullptr)) {
        for (const auto& existing_rid : existing) {
            if (existing_rid == rid) {
                return;
            }
        }
    }
    ih->insert_entry(key.data(), rid, nullptr, true);
}

void DeleteIndexEntryIfExists(SmManager* sm_manager, const std::string& tab_name, const IndexMeta& index,
                              const RmRecord& rec, const Rid& rid) {
    auto key = MakeIndexKey(index, rec.data);
    auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
    ih->delete_entry(key.data(), rid, nullptr);
}

} // namespace

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    // 为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) { // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) { // 进入名为db_name的目录
        throw UnixError();
    }
    // 创建系统目录
    DbMeta new_db = DbMeta();
    new_db.name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);
    if (!ofs.is_open()) {
        throw UnixError();
    }

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << new_db; // 注意：此处重载了操作符<<

    ofs.flush();
    if (!ofs) {
        throw UnixError();
    }
    ofs.close();
    disk_manager_->sync_path(DB_META_NAME);

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);
    // The new WAL segment's directory entry must be durable before the segment
    // can be used to cover a COMMIT.
    disk_manager_->sync_directory(".");

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
    // Make the database directory entry itself durable as well.
    disk_manager_->sync_directory(".");
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    if (!db_.name_.empty()) {
        throw DatabaseExistsError(db_name);
    }
    {
        std::ifstream meta_check(db_name + "/" + DB_META_NAME);
        if (!meta_check || meta_check.peek() == std::ifstream::traits_type::eof()) {
            throw UnixError();
        }
    }
    char cwd_buf[4096];
    if (getcwd(cwd_buf, sizeof(cwd_buf)) == nullptr) {
        throw UnixError();
    }
    std::string original_cwd = cwd_buf;
    {
        std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        historical_index_keys_.clear();
        historical_retire_states_.clear();
        historical_retire_queue_.clear();
        historical_deferred_retire_queue_.clear();
        historical_retire_prefer_deferred_ = false;
        next_historical_retire_token_ = 1;
    }
    {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        deleted_tuple_candidates_.clear();
        deleted_retire_queue_.clear();
        deleted_deferred_retire_queue_.clear();
        deleted_retire_prefer_deferred_ = false;
        next_deleted_retire_token_ = 1;
    }
    try {
        if (chdir(db_name.c_str()) < 0) { // 进入名为db_name的目录
            throw UnixError();
        }
        // 读取数据库元数据
        std::ifstream ifs(DB_META_NAME);
        DbMeta loaded_db;
        if (!(ifs >> loaded_db) || loaded_db.name_.empty()) {
            throw UnixError();
        }
        db_ = std::move(loaded_db);
        // ifs.close();
        //  打开所有表的记录文件
        for (auto& entry : db_.tabs_) {
            auto& tab_meta = entry.second;
            auto file_handle = rm_manager_->open_file(tab_meta.name);
            // NULL 位地址不进磁盘元数据，每次打开库时按数据文件的 record_size 推导。
            tab_meta.bind_null_positions(file_handle->get_file_hdr().record_size);
            fhs_.emplace(tab_meta.name, std::move(file_handle));
            for (auto& index : tab_meta.indexes) {
                // 打开索引文件
                const std::string index_name = ix_manager_->get_index_name(tab_meta.name, index.cols);
                const std::string backup_name = index_name + ".rebuild.bak";
                const std::string temp_base = index_name + ".rebuild.tmp";
                const std::string temp_name = ix_manager_->get_index_name(temp_base, index.cols);
                // A crash between the two rename operations must leave a
                // complete old or new index that can be opened on restart.
                if (!disk_manager_->is_file(index_name) && disk_manager_->is_file(backup_name)) {
                    if (rename(backup_name.c_str(), index_name.c_str()) != 0) {
                        throw UnixError();
                    }
                    disk_manager_->sync_directory(".");
                }
                if (disk_manager_->is_file(index_name) && disk_manager_->is_file(backup_name)) {
                    if (std::remove(backup_name.c_str()) != 0) {
                        throw UnixError();
                    }
                    disk_manager_->sync_directory(".");
                }
                if (disk_manager_->is_file(temp_name)) {
                    if (std::remove(temp_name.c_str()) != 0) {
                        throw UnixError();
                    }
                }
                ihs_.emplace(index_name, ix_manager_->open_index(index.tab_name, index.cols));
            }
        }
        // Reset the database-global output_file toggle: opening a (possibly
        // different) database should not inherit the previous database's toggle.
        output_file_enabled_ = true;
        bump_catalog_generation();
    } catch (...) {
        fhs_.clear();
        ihs_.clear();
        db_ = DbMeta();
        chdir(original_cwd.c_str());
        throw;
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    const std::string temp_meta = std::string(DB_META_NAME) + ".tmp";
    std::ofstream ofs(temp_meta, std::ios::trunc);
    if (!ofs.is_open()) {
        throw UnixError();
    }
    ofs << db_;
    ofs.flush();
    if (!ofs) {
        throw UnixError();
    }
    ofs.close();
    disk_manager_->sync_path(temp_meta);
    if (rename(temp_meta.c_str(), DB_META_NAME.c_str()) != 0) {
        throw UnixError();
    }
    disk_manager_->sync_directory(".");
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    if (db_.name_.empty()) {
        throw DatabaseNotFoundError("No database is currently open.");
    }
    bump_catalog_generation();
    flush_meta();
    // 关闭所有表的记录文件
    for (auto& entry : fhs_) {
        rm_manager_->close_file(entry.second.get());
    }
    // 关闭所有索引文件
    for (auto& entry : ihs_) {
        ix_manager_->close_index(entry.second.get());
    }
    fhs_.clear();
    ihs_.clear();
    db_.name_.clear();
    db_.tabs_.clear();
    {
        std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        historical_index_keys_.clear();
        historical_retire_states_.clear();
        historical_retire_queue_.clear();
        historical_deferred_retire_queue_.clear();
        historical_retire_prefer_deferred_ = false;
        next_historical_retire_token_ = 1;
    }
    {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        deleted_tuple_candidates_.clear();
        deleted_retire_queue_.clear();
        deleted_deferred_retire_queue_.clear();
        deleted_retire_prefer_deferred_ = false;
        next_deleted_retire_token_ = 1;
    }
    if (chdir("..") < 0) { // 回到根目录
        throw UnixError();
    }
}

void SmManager::prune_version_history(timestamp_t watermark) {
    constexpr size_t kHistoryPruneBatch = 512;

    std::vector<HistoricalRetireCandidate> hist_snapshot;
    {
        std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        hist_snapshot.reserve(kHistoryPruneBatch);
        for (size_t scanned = 0; scanned < kHistoryPruneBatch; ++scanned) {
            auto deferred = historical_deferred_retire_queue_.begin();
            const bool deferred_ready =
                deferred != historical_deferred_retire_queue_.end() && watermark > deferred->first;
            if (historical_retire_queue_.empty() && !deferred_ready) {
                break;
            }
            const bool take_deferred =
                deferred_ready && (historical_retire_queue_.empty() || historical_retire_prefer_deferred_);
            HistoricalRetireCandidate candidate;
            if (take_deferred) {
                candidate = std::move(deferred->second.front());
                deferred->second.pop_front();
                if (deferred->second.empty()) {
                    historical_deferred_retire_queue_.erase(deferred);
                }
                historical_retire_prefer_deferred_ = false;
            } else {
                candidate = std::move(historical_retire_queue_.front());
                historical_retire_queue_.pop_front();
                historical_retire_prefer_deferred_ = true;
            }
            auto registered = historical_retire_states_.find(candidate.state->token);
            if (registered == historical_retire_states_.end() || registered->second != candidate.state) {
                continue;
            }
            if (candidate.state->generation != candidate.generation) {
                candidate.generation = candidate.state->generation;
                candidate.last_observed_ts = std::max(watermark, candidate.state->retry_after_ts);
            }
            hist_snapshot.push_back(std::move(candidate));
        }
    }

    std::vector<DeletedRetireCandidate> del_snapshot;
    {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        del_snapshot.reserve(kHistoryPruneBatch);
        for (size_t scanned = 0; scanned < kHistoryPruneBatch; ++scanned) {
            auto deferred = deleted_deferred_retire_queue_.begin();
            const bool deferred_ready = deferred != deleted_deferred_retire_queue_.end() && watermark > deferred->first;
            if (deleted_retire_queue_.empty() && !deferred_ready) {
                break;
            }
            const bool take_deferred =
                deferred_ready && (deleted_retire_queue_.empty() || deleted_retire_prefer_deferred_);
            DeletedRetireCandidate candidate;
            if (take_deferred) {
                candidate = std::move(deferred->second.front());
                deferred->second.pop_front();
                if (deferred->second.empty()) {
                    deleted_deferred_retire_queue_.erase(deferred);
                }
                deleted_retire_prefer_deferred_ = false;
            } else {
                candidate = std::move(deleted_retire_queue_.front());
                deleted_retire_queue_.pop_front();
                deleted_retire_prefer_deferred_ = true;
            }
            auto table = deleted_tuple_candidates_.find(candidate.tab_name);
            if (table == deleted_tuple_candidates_.end()) {
                continue;
            }
            auto state = table->second.entries.find(candidate.rid);
            if (state == table->second.entries.end() || state->second.token != candidate.token) {
                continue;
            }
            if (state->second.generation != candidate.generation) {
                candidate.generation = state->second.generation;
                candidate.last_observed_ts = std::max(watermark, state->second.retry_after_ts);
            }
            del_snapshot.push_back(std::move(candidate));
        }
    }

    enum class RetireDecision { RETRY, RETIRE };
    struct RetireResult {
        RetireDecision decision{RetireDecision::RETRY};
        timestamp_t last_observed_ts{INVALID_TS};
    };
    struct PageCandidate {
        Rid rid;
        bool historical;
        size_t snapshot_index;
    };

    std::vector<RetireResult> hist_results(hist_snapshot.size());
    std::vector<RetireResult> del_results(del_snapshot.size());
    std::unordered_map<std::string_view, std::vector<PageCandidate>> page_candidates_by_table;
    page_candidates_by_table.reserve(fhs_.size());
    for (size_t i = 0; i < hist_snapshot.size(); ++i) {
        const auto& candidate = hist_snapshot[i];
        hist_results[i].last_observed_ts = candidate.last_observed_ts;
        if (candidate.last_observed_ts == INVALID_TS || watermark > candidate.last_observed_ts) {
            const size_t nul = candidate.bucket_key.find('\0');
            const std::string_view tab_name =
                nul == std::string::npos ? std::string_view{} : std::string_view(candidate.bucket_key.data(), nul);
            page_candidates_by_table[tab_name].push_back(PageCandidate{candidate.rid, true, i});
        }
    }
    for (size_t i = 0; i < del_snapshot.size(); ++i) {
        const auto& candidate = del_snapshot[i];
        del_results[i].last_observed_ts = candidate.last_observed_ts;
        if (candidate.last_observed_ts == INVALID_TS || watermark > candidate.last_observed_ts) {
            page_candidates_by_table[candidate.tab_name].push_back(PageCandidate{candidate.rid, false, i});
        }
    }

    const auto result_for = [&](const PageCandidate& candidate) -> RetireResult& {
        return candidate.historical ? hist_results[candidate.snapshot_index] : del_results[candidate.snapshot_index];
    };
    for (auto& [tab_name, page_candidates] : page_candidates_by_table) {
        std::sort(page_candidates.begin(), page_candidates.end(),
                  [](const PageCandidate& lhs, const PageCandidate& rhs) {
                      if (lhs.rid.page_no != rhs.rid.page_no) {
                          return lhs.rid.page_no < rhs.rid.page_no;
                      }
                      return lhs.rid.slot_no < rhs.rid.slot_no;
                  });

        auto table = fhs_.find(std::string(tab_name));
        if (table == fhs_.end()) {
            for (const auto& candidate : page_candidates) {
                result_for(candidate).decision = RetireDecision::RETIRE;
            }
            continue;
        }

        RmFileHandle* fh = table->second.get();
        const RmFileHdr file_hdr = fh->get_file_hdr();
        size_t offset = 0;
        while (offset < page_candidates.size()) {
            size_t end = offset + 1;
            while (end < page_candidates.size() &&
                   page_candidates[end].rid.page_no == page_candidates[offset].rid.page_no) {
                ++end;
            }

            const int page_no = page_candidates[offset].rid.page_no;
            if (page_no < RM_FIRST_RECORD_PAGE || page_no >= file_hdr.num_pages) {
                for (size_t i = offset; i < end; ++i) {
                    result_for(page_candidates[i]).decision = RetireDecision::RETIRE;
                }
                offset = end;
                continue;
            }

            RmPageHandle page_handle;
            try {
                page_handle = fh->fetch_page_handle(page_no);
            } catch (...) {
                for (size_t i = offset; i < end; ++i) {
                    result_for(page_candidates[i]).last_observed_ts = watermark;
                }
                offset = end;
                continue;
            }
            {
                std::shared_lock<std::shared_mutex> page_lock(page_handle.page->latch());
                for (size_t i = offset; i < end; ++i) {
                    const PageCandidate& candidate = page_candidates[i];
                    RetireResult& result = result_for(candidate);
                    if (candidate.rid.slot_no < 0 || candidate.rid.slot_no >= file_hdr.num_records_per_page ||
                        !Bitmap::is_set(page_handle.bitmap, candidate.rid.slot_no)) {
                        result.decision = RetireDecision::RETIRE;
                        continue;
                    }
                    const TupleMeta meta = page_handle.get_meta(candidate.rid.slot_no);
                    if (meta.is_committed_ && meta.commit_ts_ != INVALID_TS && meta.commit_ts_ < watermark) {
                        result.decision = RetireDecision::RETIRE;
                    } else {
                        result.last_observed_ts =
                            meta.is_committed_ && meta.commit_ts_ != INVALID_TS ? meta.commit_ts_ : watermark;
                    }
                }
            }
            buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            offset = end;
        }
    }

    {
        std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        for (size_t i = 0; i < hist_snapshot.size(); ++i) {
            auto& candidate = hist_snapshot[i];
            auto registered = historical_retire_states_.find(candidate.state->token);
            if (registered == historical_retire_states_.end() || registered->second != candidate.state) {
                continue;
            }
            if (candidate.state->generation != candidate.generation) {
                candidate.generation = candidate.state->generation;
                candidate.last_observed_ts = std::max(watermark, candidate.state->retry_after_ts);
                historical_deferred_retire_queue_[candidate.last_observed_ts].push_back(std::move(candidate));
                continue;
            }
            if (hist_results[i].decision == RetireDecision::RETRY) {
                candidate.last_observed_ts = hist_results[i].last_observed_ts;
                if (candidate.last_observed_ts == INVALID_TS) {
                    historical_retire_queue_.push_back(std::move(candidate));
                } else {
                    historical_deferred_retire_queue_[candidate.last_observed_ts].push_back(std::move(candidate));
                }
                continue;
            }
            auto& states = candidate.entry->second;
            auto state = std::find(states.begin(), states.end(), candidate.state);
            if (state == states.end()) {
                historical_retire_states_.erase(registered);
                continue;
            }
            states.erase(state);
            historical_retire_states_.erase(registered);
            if (states.empty()) {
                candidate.bucket->entries.erase(candidate.entry);
            }
            if (candidate.bucket->entries.empty()) {
                historical_index_keys_.erase(candidate.bucket_key);
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        for (size_t i = 0; i < del_snapshot.size(); ++i) {
            auto& candidate = del_snapshot[i];
            auto table = deleted_tuple_candidates_.find(candidate.tab_name);
            if (table == deleted_tuple_candidates_.end()) {
                continue;
            }
            auto state = table->second.entries.find(candidate.rid);
            if (state == table->second.entries.end() || state->second.token != candidate.token) {
                continue;
            }
            if (state->second.generation != candidate.generation) {
                candidate.generation = state->second.generation;
                candidate.last_observed_ts = std::max(watermark, state->second.retry_after_ts);
                deleted_deferred_retire_queue_[candidate.last_observed_ts].push_back(std::move(candidate));
                continue;
            }
            if (del_results[i].decision == RetireDecision::RETRY) {
                candidate.last_observed_ts = del_results[i].last_observed_ts;
                if (candidate.last_observed_ts == INVALID_TS) {
                    deleted_retire_queue_.push_back(std::move(candidate));
                } else {
                    deleted_deferred_retire_queue_[candidate.last_observed_ts].push_back(std::move(candidate));
                }
                continue;
            }
            table->second.remove_from_hash_index(candidate.rid, state->second.tuple_hash);
            table->second.entries.erase(state);
            if (table->second.entries.empty()) {
                deleted_tuple_candidates_.erase(table);
            }
        }
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context* context) {
    const bool output_file_enabled = context != nullptr && context->output_file_enabled_ != nullptr
                                         ? *context->output_file_enabled_
                                         : output_file_enabled_;
    std::fstream outfile;
    if (output_file_enabled) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "| Tables |\n";
    }
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        printer.print_record({tab.name}, context);
        if (output_file_enabled) {
            outfile << "| " << tab.name << " |\n";
        }
    }
    printer.print_separator(context);
    if (output_file_enabled) {
        outfile.close();
    }
}

void SmManager::show_index(const std::string& tab_name, Context* context) {
    TabMeta& tab = db_.get_table(tab_name);
    std::vector<std::string> captions = {"Tables", "Type", "Column"};
    RecordPrinter printer(captions.size());

    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);

    const bool output_file_enabled = context != nullptr && context->output_file_enabled_ != nullptr
                                         ? *context->output_file_enabled_
                                         : output_file_enabled_;
    std::fstream outfile;
    if (output_file_enabled) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
    }
    for (const auto& index : tab.indexes) {
        std::string cols = "(";
        if (output_file_enabled) {
            outfile << "| " << tab_name << " | unique | (";
        }
        for (int i = 0; i < index.col_num; ++i) {
            if (i != 0) {
                cols += ",";
                if (output_file_enabled) {
                    outfile << ",";
                }
            }
            cols += index.cols[i].name;
            if (output_file_enabled) {
                outfile << index.cols[i].name;
            }
        }
        cols += ")";
        if (output_file_enabled) {
            outfile << ") |\n";
        }

        printer.print_record({tab_name, "unique", cols}, context);
    }
    if (output_file_enabled) {
        outfile.close();
    }

    printer.print_separator(context);
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta& tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto& col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    (void)context;
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    tab.cols.reserve(col_defs.size());
    for (auto& col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.emplace_back(col);
    }
    // Create & open record file
    // record_size = 数据区 + 尾部 null bitmap（每列 1 bit），列偏移量不受影响。
    int record_size = tab.record_len();
    tab.bind_null_positions(record_size);
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
    bump_catalog_generation();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    if (!db_.is_table(tab_name))
        throw TableNotFoundError(tab_name);
    TabMeta& tab = db_.get_table(tab_name);
    auto indexes = tab.indexes;
    for (auto& index : indexes)
        drop_index(tab_name, index.cols, context);
    rm_manager_->close_file(fhs_[tab_name].get());
    rm_manager_->destroy_file(tab_name); // 删除表的磁盘文件
    db_.tabs_.erase(tab_name);           // 删除对应键值对
    fhs_.erase(tab_name);
    flush_meta();
    bump_catalog_generation();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    if (!db_.is_table(tab_name))
        throw TableNotFoundError(tab_name);
    TabMeta& tab = db_.get_table(tab_name);
    if (tab.is_index(col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }
    // 获取索引包含的字段元数据
    std::vector<ColMeta> cols;
    cols.reserve(col_names.size());
    int total_len = 0;
    for (const auto& col_name : col_names) {
        auto col_meta = tab.get_col(col_name);
        cols.emplace_back(*col_meta);
        total_len += col_meta->len;
    }

    IndexMeta index_meta; // 创建索引元数据
    index_meta.tab_name = tab_name;
    index_meta.col_tot_len = total_len;
    index_meta.col_num = cols.size();
    index_meta.cols = cols;

    ix_manager_->create_index(tab_name, cols);
    auto index_handle = ix_manager_->open_index(tab_name, col_names);
    auto index_name = ix_manager_->get_index_name(tab_name, col_names);
    auto file_handle = fhs_[tab_name].get();

    try {
        for (RmScan scan(file_handle); !scan.is_end(); scan.next()) {
            // 对每条记录插入索引
            auto record = file_handle->get_record(scan.rid(), context);
            std::vector<char> key(total_len); // 所有索引字段的值拼接在一起作为键
            int offset = 0;
            for (const auto& col : cols) {
                std::memcpy(key.data() + offset, record->data + col.offset, col.len);
                offset += col.len;
            }
            // 插入索引。允许重复键：`CREATE INDEX` 只是建立访问路径，不是唯一约束
            // （final.md:168 明确数据模式除 CREATE INDEX 外不声明任何约束）。它也不能比
            // 已经把这些行放进表里的路径更严格 —— LOAD（:54）和恢复期索引重建（:812）
            // 都传 allow_duplicate=true，若此处仍拒重复，一张 LOAD 出来的合法表就可能
            // 建不出索引。INSERT/UPDATE 路径保留唯一性检查不变：它是并发取号冲突
            // （重复 d_next_o_id 等，final.md:444）的响亮失败信号，而 TPC-C 的复合索引
            // 靠尾列天然唯一（final.md:171），正常负载下不会误拒。
            index_handle->insert_entry(key.data(), scan.rid(), context == nullptr ? nullptr : context->txn_,
                                       /*allow_duplicate=*/true);
        }
        // Index creation is complete, so it is now safe to build the optional
        // root cache and upper-level residency state.
        index_handle->refresh_page_residency();
    } catch (...) {
        ix_manager_->close_index(index_handle.get());
        ix_manager_->destroy_index(tab_name, cols);
        throw;
    }
    // 更新表的元数据
    tab.indexes.emplace_back(index_meta);
    ihs_.emplace(index_name, std::move(index_handle));
    flush_meta();
    bump_catalog_generation();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    (void)context;
    TabMeta& tab = db_.get_table(tab_name);
    if (!tab.is_index(col_names)) {
        throw IndexNotFoundError(tab_name, col_names);
    }
    std::string index_name = ix_manager_->get_index_name(tab_name, col_names);
    auto index_meta = tab.get_index_meta(col_names);
    tab.indexes.erase(index_meta);
    auto index_handle = ihs_[index_name].get();
    ix_manager_->close_index(index_handle);
    ix_manager_->destroy_index(tab_name, col_names);
    ihs_.erase(index_name);
    flush_meta();
    bump_catalog_generation();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    (void)context;
    TabMeta& tab = db_.get_table(tab_name);
    std::vector<std::string> col_names;
    col_names.reserve(cols.size());
    for (const auto& col : cols) {
        col_names.emplace_back(col.name);
    }
    std::string index_name = ix_manager_->get_index_name(tab_name, col_names);
    auto index_meta = tab.get_index_meta(col_names);
    tab.indexes.erase(index_meta);
    auto index_handle = ihs_[index_name].get();
    ix_manager_->close_index(index_handle);
    ix_manager_->destroy_index(tab_name, col_names);
    ihs_.erase(index_name);
    flush_meta();
    bump_catalog_generation();
}

void SmManager::insert_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& rec) {
    auto& tab = db_.get_table(tab_name);
    auto* fh = fhs_.at(tab_name).get();
    fh->insert_record(rid, rec.data);
    for (const auto& index : tab.indexes) {
        InsertIndexEntryIdempotent(this, tab_name, index, rec, rid);
    }
}

void SmManager::delete_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec) {
    auto& tab = db_.get_table(tab_name);
    auto* fh = fhs_.at(tab_name).get();
    for (const auto& index : tab.indexes) {
        DeleteIndexEntryIfExists(this, tab_name, index, old_rec, rid);
    }
    fh->delete_record(rid, nullptr);
}

void SmManager::update_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec,
                                           const RmRecord& new_rec) {
    auto& tab = db_.get_table(tab_name);
    auto* fh = fhs_.at(tab_name).get();
    for (const auto& index : tab.indexes) {
        auto old_key = MakeIndexKey(index, old_rec.data);
        auto new_key = MakeIndexKey(index, new_rec.data);
        if (old_key != new_key) {
            DeleteIndexEntryIfExists(this, tab_name, index, old_rec, rid);
        }
    }
    fh->update_record(rid, new_rec.data, nullptr);
    for (const auto& index : tab.indexes) {
        auto old_key = MakeIndexKey(index, old_rec.data);
        auto new_key = MakeIndexKey(index, new_rec.data);
        if (old_key != new_key) {
            InsertIndexEntryIdempotent(this, tab_name, index, new_rec, rid);
        }
    }
}

bool SmManager::flush_all_table_and_index_pages(bool wal_preflushed) {
    bool success = true;
    std::vector<int> fds;
    fds.reserve(fhs_.size() + ihs_.size());
    for (const auto& [_, fh] : fhs_) {
        rm_manager_->flush_file_header(fh.get());
        fds.push_back(fh->GetFd());
    }
    for (const auto& [_, ih] : ihs_) {
        ix_manager_->flush_index_header(ih.get());
        fds.push_back(ih->GetFd());
    }
    success = buffer_pool_manager_->flush_all_pages(fds, wal_preflushed) && success;
    disk_manager_->sync_files(fds);
    return success;
}

bool SmManager::flush_dirty_data_pages(bool wal_preflushed) {
    std::vector<int> fds;
    fds.reserve(fhs_.size() + ihs_.size());
    for (const auto& [_, fh] : fhs_) {
        fds.push_back(fh->GetFd());
    }
    for (const auto& [_, ih] : ihs_) {
        fds.push_back(ih->GetFd());
    }
    return buffer_pool_manager_->flush_all_pages(fds, wal_preflushed);
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

size_t SmManager::flush_dirty_pages(size_t max_pages) {
    std::unordered_set<int> index_fds;
    index_fds.reserve(ihs_.size());
    for (const auto& [_, ih] : ihs_) {
        index_fds.insert(ih->GetFd());
    }
    return buffer_pool_manager_->flush_dirty_pages(max_pages, index_fds);
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
                    temp_handle->insert_entry(key.data(), scan.rid(), nullptr, true);
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
        // Reopening a large benchmark must not walk every index leaf merely
        // to warm optional residency state. Individual index creation and
        // tests retain the explicit full-refresh default.
        index_handle->refresh_page_residency(false);
    }
}

// 这里曾有一个 reset_all_tuple_meta_after_recovery()：恢复后全表扫描，把每个存活槽的
// TupleMeta 归一化成 commit_ts_ = 0。它从来没有调用者，也不该有——那是“重启后已提交行
// 不可见”的错误解法：代价与整库大小成正比（评测规模 5 GB），必然击穿 final.md:53 的
// 90 秒就绪预算。正解是让时间戳计数器在重启后恢复到高于任何已持久化的 commit_ts_，
// 见 RecoveryManager::get_recovered_next_timestamp()，代价 O(1)。
void SmManager::load_csv_data(const std::string& file_path, const std::string& tab_name, Context* context) {
    (void)context;
    std::ifstream infile(file_path);
    if (!infile.is_open()) {
        throw RMDBError("cannot open load file: " + file_path);
    }

    auto& tab = db_.get_table(tab_name);
    auto* fh = fhs_.at(tab_name).get();
    const int record_size = fh->get_file_hdr().record_size;
    const auto& cols = tab.cols;

    // CSV files may use CRLF line endings; strip a trailing '\r' from each line.
    auto strip_cr = [](std::string& s) {
        if (!s.empty() && s.back() == '\r') {
            s.pop_back();
        }
    };

    // 就地按 RFC 4180 切分一行，结果是指向 line 内部的 NUL 结尾字段。
    // 解引号只会删字符，所以写指针永不超过读指针，可以原地改写；末尾多留的
    // 一个字节用于给最后一个字段写 NUL。不支持引号内换行（TPC-C 数据不含换行，
    // Go 的 encoding/csv 也只在字段含 , " \n 时才加引号）。
    auto split_csv_line = [](std::string& s, std::vector<const char*>& out) {
        out.clear();
        s.push_back('\0');
        char* write = s.data();
        const char* read = s.data();
        const char* end = s.data() + s.size() - 1;
        while (true) {
            char* field_begin = write;
            if (read < end && *read == '"') {
                ++read; // 起始引号
                while (read < end) {
                    if (*read != '"') {
                        *write++ = *read++;
                        continue;
                    }
                    if (read + 1 < end && read[1] == '"') {
                        *write++ = '"'; // "" 还原成一个 "
                        read += 2;
                        continue;
                    }
                    ++read; // 结束引号
                    break;
                }
                while (read < end && *read != ',') {
                    ++read; // 容忍结束引号与逗号之间的多余字符
                }
            } else {
                while (read < end && *read != ',') {
                    *write++ = *read++;
                }
            }
            *write++ = '\0';
            out.push_back(field_begin);
            if (read >= end) {
                break;
            }
            ++read; // 跳过字段分隔符
        }
        s.pop_back();
    };

    // 复用的字段指针数组：提到循环外，省掉每行一次 malloc/free。
    std::vector<const char*> fields;
    fields.reserve(cols.size() * 2);

    // 表头嗅探：只有"字段数 == DDL 列数 ∧ 每个字段（trim 后）都命中列名 ∧ 互不
    // 重复"才认定为表头，否则按 DDL 列序做位置映射并把这一行也当数据行。
    // final.md 全文对表头未作规定，因此必须同时支持有/无表头两种 CSV。
    // TPC-C 列名是标识符，数据行是数字/时间戳/随机串，不可能凑巧全部命中。
    std::string line;
    if (!std::getline(infile, line)) {
        throw RMDBError("load file is empty: " + file_path);
    }
    strip_cr(line);
    split_csv_line(line, fields);

    // 行尾逗号（"a,b,c,"）按 RFC 4180 会多切出一个空字段。数据行多出来的尾部字段
    // 无害（下面按列序取前 cols.size() 个），但表头行一多一个字段就会让下面的
    // 嗅探判定失败，把表头当数据行去 strtol，整个装载失败。这里只吃掉刚好多出来
    // 的那一个空字段。
    if (fields.size() == cols.size() + 1 && *fields.back() == '\0') {
        fields.pop_back();
    }

    std::unordered_map<std::string, size_t> col_index;
    bool has_header = fields.size() == cols.size();
    if (has_header) {
        auto trim = [](const char* text) {
            const char* begin = text;
            const char* stop = text + std::strlen(text);
            while (begin < stop && std::isspace(static_cast<unsigned char>(*begin))) {
                ++begin;
            }
            while (stop > begin && std::isspace(static_cast<unsigned char>(stop[-1]))) {
                --stop;
            }
            return std::string(begin, static_cast<size_t>(stop - begin));
        };
        for (size_t i = 0; i < fields.size() && has_header; ++i) {
            std::string name = trim(fields[i]);
            has_header = tab.is_col(name) && col_index.emplace(std::move(name), i).second;
        }
        if (!has_header) {
            col_index.clear();
        }
    }

    // Per table column: its CSV field index and storage metadata.
    struct ColSrc {
        size_t csv_idx;
        ColType type;
        int len;
        int offset;
        int null_byte;
        uint8_t null_mask;
    };
    std::vector<ColSrc> col_src;
    col_src.reserve(cols.size());
    for (size_t i = 0; i < cols.size(); ++i) {
        const auto& col = cols[i];
        const size_t csv_idx = has_header ? col_index.at(col.name) : i;
        col_src.push_back({csv_idx, col.type, col.len, col.offset, col.null_byte, col.null_mask});
    }

    struct LoadIndexTarget {
        const IndexMeta* meta;
        std::vector<char> key;
        std::unique_ptr<IxIndexHandle::PinnedInserter> inserter;
    };
    std::vector<LoadIndexTarget> idx_inserters;
    idx_inserters.reserve(tab.indexes.size());
    for (const auto& index : tab.indexes) {
        auto ih = ihs_.at(get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        idx_inserters.push_back(
            {&index, std::vector<char>(index.col_tot_len), std::make_unique<IxIndexHandle::PinnedInserter>(ih)});
    }

    RmFileHandle::PinnedInserter rm_inserter(fh);
    std::vector<char> record(record_size, 0);

    // 把当前 fields 组装成一条记录并写入表和索引。
    auto emit_row = [&](size_t line_no) {
        // 字段数不足是硬错误，不能当成"这些列都是 NULL"：一个被截断或错列的 CSV
        // 会静默装成一片 NULL，而 COUNT(*) 校验照样通过，报错点离病因非常远。
        // 字段数多于列数则只忽略尾部（见上面的行尾逗号说明）。
        if (fields.size() < cols.size()) {
            throw RMDBError("load file row " + std::to_string(line_no) + " has fewer fields than expected: got " +
                            std::to_string(fields.size()) + ", need " + std::to_string(cols.size()));
        }
        std::memset(record.data(), 0, record_size);
        for (const auto& cs : col_src) {
            const char* raw = fields[cs.csv_idx];
            // finalv3 uses an empty string (not SQL NULL) for undelivered CHAR
            // timestamps. Preserve the existing NULL interpretation for empty
            // numeric CSV fields, while letting empty text flow through as a
            // non-NULL, zero-length string.
            if (*raw == '\0' && cs.type != TYPE_STRING && cs.type != TYPE_DATETIME) {
                set_null_at(record.data(), cs.null_byte, cs.null_mask);
                continue;
            }
            if (cs.type == TYPE_INT) {
                errno = 0;
                char* end = nullptr;
                long v = std::strtol(raw, &end, 10);
                if (errno != 0 || end == raw || *end != '\0') {
                    throw RMDBError("load file row " + std::to_string(line_no) + " invalid int for column");
                }
                int iv = static_cast<int>(v);
                std::memcpy(record.data() + cs.offset, &iv, cs.len);
            } else if (cs.type == TYPE_FLOAT) {
                errno = 0;
                char* end = nullptr;
                float v = std::strtof(raw, &end);
                if (errno != 0 || end == raw || *end != '\0') {
                    throw RMDBError("load file row " + std::to_string(line_no) + " invalid float for column");
                }
                write_float(record.data() + cs.offset, v);
            } else if (cs.type == TYPE_STRING || cs.type == TYPE_DATETIME) {
                size_t raw_len = std::strlen(raw);
                if (static_cast<int>(raw_len) > cs.len) {
                    throw RMDBError("load file row " + std::to_string(line_no) + " string too long for column");
                }
                std::memcpy(record.data() + cs.offset, raw, raw_len);
            }
        }

        Rid rid = rm_inserter.insert(record.data());

        // Build index keys and insert via pinned-leaf batch path.
        for (auto& target : idx_inserters) {
            int off = 0;
            for (int i = 0; i < target.meta->col_num; ++i) {
                std::memcpy(target.key.data() + off, record.data() + target.meta->cols[i].offset,
                            target.meta->cols[i].len);
                off += target.meta->cols[i].len;
            }
            target.inserter->insert(target.key.data(), rid, nullptr, true);
        }
    };

    size_t line_no = 1;
    if (!has_header && !line.empty()) {
        emit_row(line_no); // 第一行不是表头，它已经切分好了
    }
    while (std::getline(infile, line)) {
        ++line_no;
        strip_cr(line);
        if (line.empty()) {
            continue; // skip trailing blank lines
        }
        split_csv_line(line, fields);
        emit_row(line_no);
    }

    idx_inserters.clear();
    { RmFileHandle::PinnedInserter tmp(std::move(rm_inserter)); }

    flush_all_table_and_index_pages();
    flush_meta();
}
