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

#include "common/csv.h"
#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"
#include "transaction/transaction_manager.h"

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
    rmdb_csv::StripCr(line);
    rmdb_csv::SplitLineInPlace(line, fields);

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
            {&index, std::vector<char>(index.col_tot_len),
             std::make_unique<IxIndexHandle::PinnedInserter>(ih, IndexWriteWalContext::UnloggedBulkLoad())});
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
            target.inserter->insert(target.key.data(), rid, true);
        }
    };

    size_t line_no = 1;
    if (!has_header && !line.empty()) {
        emit_row(line_no); // 第一行不是表头，它已经切分好了
    }
    while (std::getline(infile, line)) {
        ++line_no;
        rmdb_csv::StripCr(line);
        if (line.empty()) {
            continue; // skip trailing blank lines
        }
        rmdb_csv::SplitLineInPlace(line, fields);
        emit_row(line_no);
    }

    idx_inserters.clear();
    { RmFileHandle::PinnedInserter tmp(std::move(rm_inserter)); }

    if (!flush_all_table_and_index_pages()) {
        throw InternalError("failed to flush loaded table and index pages");
    }
    flush_meta();
}
