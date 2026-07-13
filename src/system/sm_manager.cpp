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

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
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

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << new_db; // 注意：此处重载了操作符<<

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
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
    }
    {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        deleted_tuple_candidates_.clear();
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
            fhs_.emplace(tab_meta.name, rm_manager_->open_file(tab_meta.name));
            for (auto& index : tab_meta.indexes) {
                // 打开索引文件
                ihs_.emplace(ix_manager_->get_index_name(tab_meta.name, index.cols),
                             ix_manager_->open_index(index.tab_name, index.cols));
            }
        }
        // Reset the database-global output_file toggle: opening a (possibly
        // different) database should not inherit the previous database's toggle.
        output_file_enabled_ = true;
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
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    if (db_.name_.empty()) {
        throw DatabaseNotFoundError("No database is currently open.");
    }
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
    }
    {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        deleted_tuple_candidates_.clear();
    }
    if (chdir("..") < 0) { // 回到根目录
        throw UnixError();
    }
}

void SmManager::prune_version_history(timestamp_t watermark) {
    // 回收 historical_index_keys_：键编码为 tab_name + '\0' + index_name + '\0' + key
    // 对每个 RID，若其当前 tuple 版本已提交且 commit_ts < watermark，则任何活跃事务
    // （read_ts >= watermark）都不会回溯其版本链，该历史键不再被冲突检测访问。
    // 先在锁内快照待检查的 (key, RID) 列表，再在锁外读 meta，最后回锁内删除，避免
    // 持锁访问缓冲池造成长时间持锁/死锁。
    std::vector<std::pair<std::string, Rid>> hist_snapshot;
    {
        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        hist_snapshot.reserve(historical_index_keys_.size() * 2);
        for (const auto& [combined_key, rids] : historical_index_keys_) {
            for (const Rid& rid : rids) {
                hist_snapshot.emplace_back(combined_key, rid);
            }
        }
    }
    std::vector<std::pair<std::string, Rid>> hist_to_remove;
    for (auto& [combined_key, rid] : hist_snapshot) {
        // 解析 tab_name（第一个 '\0' 之前的部分）
        size_t nul = combined_key.find('\0');
        std::string tab_name = (nul != std::string::npos) ? combined_key.substr(0, nul) : std::string{};
        auto fh_it = fhs_.find(tab_name);
        if (fh_it == fhs_.end()) {
            // 表已不存在，整组历史键失效，直接标记删除
            hist_to_remove.emplace_back(std::move(combined_key), rid);
            continue;
        }
        RmFileHandle* fh = fh_it->second.get();
        if (!fh->is_record(rid)) {
            hist_to_remove.emplace_back(std::move(combined_key), rid);
            continue;
        }
        TupleMeta meta = fh->get_tuple_meta(rid);
        if (meta.is_committed_ && meta.commit_ts_ != INVALID_TS && meta.commit_ts_ < watermark) {
            hist_to_remove.emplace_back(std::move(combined_key), rid);
        }
    }
    if (!hist_to_remove.empty()) {
        std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        for (auto& [combined_key, rid] : hist_to_remove) {
            auto it = historical_index_keys_.find(combined_key);
            if (it == historical_index_keys_.end()) {
                continue;
            }
            auto& rids = it->second;
            rids.erase(std::remove(rids.begin(), rids.end(), rid), rids.end());
            if (rids.empty()) {
                historical_index_keys_.erase(it);
            }
        }
    }

    // 回收 deleted_tuple_candidates_：同样的水位线条件
    std::vector<std::pair<std::string, Rid>> del_snapshot;
    {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        del_snapshot.reserve(deleted_tuple_candidates_.size() * 2);
        for (const auto& [tab_name, rids] : deleted_tuple_candidates_) {
            for (const Rid& rid : rids) {
                del_snapshot.emplace_back(tab_name, rid);
            }
        }
    }
    std::vector<std::pair<std::string, Rid>> del_to_remove;
    for (auto& [tab_name, rid] : del_snapshot) {
        auto fh_it = fhs_.find(tab_name);
        if (fh_it == fhs_.end()) {
            del_to_remove.emplace_back(std::move(tab_name), rid);
            continue;
        }
        RmFileHandle* fh = fh_it->second.get();
        if (!fh->is_record(rid)) {
            del_to_remove.emplace_back(std::move(tab_name), rid);
            continue;
        }
        TupleMeta meta = fh->get_tuple_meta(rid);
        // 候选仅在 tombstone 仍可能被并发 INSERT 检测时需要保留；若当前版本已提交
        // 且 commit_ts < watermark，则无活跃事务会再把它视为并发删除。
        if (meta.is_committed_ && meta.commit_ts_ != INVALID_TS && meta.commit_ts_ < watermark) {
            del_to_remove.emplace_back(std::move(tab_name), rid);
        }
    }
    for (auto& [tab_name, rid] : del_to_remove) {
        remove_deleted_tuple_candidate(tab_name, rid);
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    if (output_file_enabled_) {
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
        if (output_file_enabled_) {
            outfile << "| " << tab.name << " |\n";
        }
    }
    printer.print_separator(context);
    if (output_file_enabled_) {
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

    std::fstream outfile;
    if (output_file_enabled_) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
    }
    for (const auto& index : tab.indexes) {
        std::string cols = "(";
        if (output_file_enabled_) {
            outfile << "| " << tab_name << " | unique | (";
        }
        for (int i = 0; i < index.col_num; ++i) {
            if (i != 0) {
                cols += ",";
                if (output_file_enabled_) {
                    outfile << ",";
                }
            }
            cols += index.cols[i].name;
            if (output_file_enabled_) {
                outfile << index.cols[i].name;
            }
        }
        cols += ")";
        if (output_file_enabled_) {
            outfile << ") |\n";
        }

        printer.print_record({tab_name, "unique", cols}, context);
    }
    if (output_file_enabled_) {
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
    int record_size = curr_offset; // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
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
            // 插入索引
            index_handle->insert_entry(key.data(), scan.rid(), context == nullptr ? nullptr : context->txn_);
        }
    } catch (...) {
        ix_manager_->close_index(index_handle.get());
        ix_manager_->destroy_index(tab_name, cols);
        throw;
    }
    // 更新表的元数据
    tab.indexes.emplace_back(index_meta);
    ihs_.emplace(index_name, std::move(index_handle));
    flush_meta();
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

void SmManager::flush_all_table_and_index_pages() {
    for (const auto& [_, fh] : fhs_) {
        rm_manager_->flush_file_header(fh.get());
        buffer_pool_manager_->flush_all_pages(fh->GetFd());
    }
    for (const auto& [_, ih] : ihs_) {
        ix_manager_->flush_index_header(ih.get());
        buffer_pool_manager_->flush_all_pages(ih->GetFd());
    }
}

void SmManager::rebuild_all_indexes() {
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
            drop_index(tab_name, index.cols, nullptr);
            create_index(tab_name, col_names, nullptr);
        }
    }
}

void SmManager::reset_all_tuple_meta_after_recovery() {
    TupleMeta clean_meta;
    clean_meta.commit_ts_ = 0;
    clean_meta.writer_txn_id_ = INVALID_TXN_ID;
    clean_meta.is_committed_ = true;
    clean_meta.is_deleted_ = false;
    clean_meta.version_chain_head_ = UndoLink{};

    for (const auto& [_, fh] : fhs_) {
        std::vector<Rid> tombstones;
        std::vector<Rid> live_records;
        for (RmScan scan(fh.get()); !scan.is_end(); scan.next()) {
            Rid rid = scan.rid();
            TupleMeta meta = fh->get_tuple_meta(rid);
            if (meta.is_deleted_) {
                tombstones.push_back(rid);
            } else {
                live_records.push_back(rid);
            }
        }
        for (const auto& rid : tombstones) {
            fh->delete_record(rid, nullptr);
        }
        for (const auto& rid : live_records) {
            if (fh->is_record(rid)) {
                fh->set_tuple_meta(rid, clean_meta);
            }
        }
    }
}

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

    // Build column-name -> CSV-field-index map from the header row.
    std::string header_line;
    if (!std::getline(infile, header_line)) {
        throw RMDBError("load file is empty: " + file_path);
    }
    strip_cr(header_line);
    std::vector<std::string> header_fields;
    {
        std::stringstream ss(header_line);
        std::string field;
        while (std::getline(ss, field, ',')) {
            header_fields.push_back(field);
        }
    }
    std::unordered_map<std::string, size_t> col_index;
    for (size_t i = 0; i < header_fields.size(); ++i) {
        col_index[header_fields[i]] = i;
    }
    for (const auto& col : cols) {
        if (col_index.find(col.name) == col_index.end()) {
            throw RMDBError("load file missing column: " + col.name);
        }
    }

    // Per table column: its CSV field index and storage metadata.
    struct ColSrc {
        size_t csv_idx;
        ColType type;
        int len;
        int offset;
    };
    std::vector<ColSrc> col_src;
    col_src.reserve(cols.size());
    for (const auto& col : cols) {
        col_src.push_back({col_index[col.name], col.type, col.len, col.offset});
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

    std::string line;
    size_t line_no = 1; // header was line 1
    while (std::getline(infile, line)) {
        ++line_no;
        strip_cr(line);
        if (line.empty()) {
            continue; // skip trailing blank lines
        }

        std::vector<const char*> fields;
        fields.reserve(col_src.size() * 2);
        {
            const char* field_start = line.data();
            for (char* p = line.data(); *p != '\0'; ++p) {
                if (*p == ',') {
                    *p = '\0';
                    fields.push_back(field_start);
                    field_start = p + 1;
                }
            }
            fields.push_back(field_start);
        }

        std::memset(record.data(), 0, record_size);
        for (const auto& cs : col_src) {
            if (cs.csv_idx >= fields.size()) {
                throw RMDBError("load file row " + std::to_string(line_no) + " has fewer fields than expected");
            }
            const char* raw = fields[cs.csv_idx];
            if (std::strchr(raw, '"') != nullptr) {
                throw RMDBError("load file row " + std::to_string(line_no) + " contains an unexpected quote character");
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
                double v = std::strtod(raw, &end);
                if (errno != 0 || end == raw || *end != '\0') {
                    throw RMDBError("load file row " + std::to_string(line_no) + " invalid float for column");
                }
                std::memcpy(record.data() + cs.offset, &v, cs.len);
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
    }

    idx_inserters.clear();
    { RmFileHandle::PinnedInserter tmp(std::move(rm_inserter)); }

    flush_all_table_and_index_pages();
    flush_meta();
}
