/* Copyright (c) 2023 Renmin University of China
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

#include <cstring>
#include <fstream>
#include <vector>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

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
        if (existing.size() == 1 && existing[0] == rid) {
            return;
        }
    }
    ih->insert_entry(key.data(), rid, nullptr);
}

void DeleteIndexEntryIfExists(SmManager* sm_manager, const std::string& tab_name, const IndexMeta& index,
                              const RmRecord& rec) {
    auto key = MakeIndexKey(index, rec.data);
    auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
    ih->delete_entry(key.data(), nullptr);
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
    if (chdir(db_name.c_str()) < 0) { // 进入名为db_name的目录
        throw UnixError();
    }
    // 读取数据库元数据
    std::ifstream ifs(DB_META_NAME);
    ifs >> db_;
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
    if (chdir("..") < 0) { // 回到根目录
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

void SmManager::show_index(const std::string& tab_name, Context* context) {
    TabMeta& tab = db_.get_table(tab_name);
    std::vector<std::string> captions = {"Tables", "Type", "Column"};
    RecordPrinter printer(captions.size());

    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);

    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    for (const auto& index : tab.indexes) {
        std::string cols = "(";
        outfile << "| " << tab_name << " | unique | (";
        for (int i = 0; i < index.col_num; ++i) {
            if (i != 0) {
                cols += ",";
                outfile << ",";
            }
            cols += index.cols[i].name;
            outfile << index.cols[i].name;
        }
        cols += ")";
        outfile << ") |\n";

        printer.print_record({tab_name, "unique", cols}, context);
    }
    outfile.close();

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
        DeleteIndexEntryIfExists(this, tab_name, index, old_rec);
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
            DeleteIndexEntryIfExists(this, tab_name, index, old_rec);
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
