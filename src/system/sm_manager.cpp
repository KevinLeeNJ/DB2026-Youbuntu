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
    version_history_->Clear();
    try {
        table_runtime_ids_.clear();
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
            table_runtime_ids_.emplace(tab_meta.name, next_table_runtime_id_++);
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
        table_runtime_ids_.clear();
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
    table_runtime_ids_.clear();
    db_.name_.clear();
    db_.tabs_.clear();
    version_history_->Clear();
    if (chdir("..") < 0) { // 回到根目录
        throw UnixError();
    }
}
