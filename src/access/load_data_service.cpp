/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/Mulan PSL v2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "access/load_data_service.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "common/exception.h"
#include "record/rm_file_handle.h"
#include "system/sm.h"

namespace rmdb::access {

void LoadDataService::load_csv(const std::string& file_path, const std::string& tab_name, Context* ctx) {
    (void)ctx;
    std::ifstream infile(file_path);
    if (!infile.is_open()) {
        throw RMDBError("cannot open load file: " + file_path);
    }

    auto& tab = schema_mgr_->catalog().get_table(tab_name);
    auto* fh = schema_mgr_->get_table_handle(tab_name);
    const int record_size = fh->get_file_hdr().record_size;
    const auto& cols = tab.cols;

    // CSV 可能使用 CRLF 行尾，逐行剥离末尾 '\r'。
    auto strip_cr = [](std::string& s) {
        if (!s.empty() && s.back() == '\r') {
            s.pop_back();
        }
    };

    // 从表头行构建 列名 -> CSV 字段索引 映射。
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

    // 每个表列的 CSV 字段索引与存储元数据。
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

    // 逐行解析为 record buffer，攒批后统一交给 TableWriteService::bulk_insert。
    std::vector<std::vector<char>> rows;
    std::string line;
    size_t line_no = 1; // header was line 1
    while (std::getline(infile, line)) {
        ++line_no;
        strip_cr(line);
        if (line.empty()) {
            continue; // 跳过末尾空行
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

        std::vector<char> record(record_size, 0);
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
                float v = std::strtof(raw, &end);
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
        rows.push_back(std::move(record));
    }

    write_svc_->bulk_insert(tab_name, rows, ctx);

    // 批量写入完成后 flush 表页与索引页，并持久化元数据。
    schema_mgr_->flush_all_table_and_index_pages();
    schema_mgr_->flush_meta();
}

} // namespace rmdb::access
