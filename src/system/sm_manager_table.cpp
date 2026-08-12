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
    ih->insert_entry(key.data(), rid, IndexWriteWalContext::RecoveryDurable(), true);
}

void DeleteIndexEntryIfExists(SmManager* sm_manager, const std::string& tab_name, const IndexMeta& index,
                              const RmRecord& rec, const Rid& rid) {
    auto key = MakeIndexKey(index, rec.data);
    auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
    ih->delete_entry(key.data(), rid, IndexWriteWalContext::RecoveryDurable());
}

} // namespace

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(ExecutionOutput* output) {
    const bool output_file_enabled = output != nullptr && output->output_file_enabled != nullptr
                                         ? *output->output_file_enabled
                                         : output_file_enabled_;
    std::fstream outfile;
    if (output_file_enabled) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "| Tables |\n";
    }
    RecordPrinter printer(1);
    printer.print_separator(output);
    printer.print_record({"Tables"}, output);
    printer.print_separator(output);
    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        printer.print_record({tab.name}, output);
        if (output_file_enabled) {
            outfile << "| " << tab.name << " |\n";
        }
    }
    printer.print_separator(output);
    if (output_file_enabled) {
        outfile.close();
    }
}

void SmManager::show_index(const std::string& tab_name, ExecutionOutput* output) {
    TabMeta& tab = db_.get_table(tab_name);
    std::vector<std::string> captions = {"Tables", "Type", "Column"};
    RecordPrinter printer(captions.size());

    printer.print_separator(output);
    printer.print_record(captions, output);
    printer.print_separator(output);

    const bool output_file_enabled = output != nullptr && output->output_file_enabled != nullptr
                                         ? *output->output_file_enabled
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

        printer.print_record({tab_name, "unique", cols}, output);
    }
    if (output_file_enabled) {
        outfile.close();
    }

    printer.print_separator(output);
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string& tab_name, ExecutionOutput* output) {
    TabMeta& tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(output);
    printer.print_record(captions, output);
    printer.print_separator(output);
    // Print fields
    for (auto& col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, output);
    }
    // Print footer
    printer.print_separator(output);
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
    table_runtime_ids_.emplace(tab_name, next_table_runtime_id_++);

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
    table_runtime_ids_.erase(tab_name);
    flush_meta();
    bump_catalog_generation();
    version_history_->ClearTable(tab_name);
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
            index_handle->insert_entry(key.data(), scan.rid(), IndexWriteWalContext::UnloggedDdlBuild(),
                                       /*allow_duplicate=*/true);
        }
        // Index creation is complete, so it is now safe to build the root
        // cache and upper-level residency state.
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
