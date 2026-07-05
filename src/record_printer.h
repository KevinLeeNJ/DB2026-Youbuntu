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

#include <cassert>
#include <iostream>
#include <iomanip>
#include <string>
#include <sstream>
#include "common/config.h"
#include "server/output_sink.h"

namespace rmdb::common {
#define RECORD_COUNT_LENGTH 40

class RecordPrinter {
    static constexpr size_t COL_WIDTH = 16;
    size_t num_cols;

public:
    RecordPrinter(size_t num_cols_) : num_cols(num_cols_) {
        assert(num_cols_ > 0);
    }

    void print_separator(OutputSink* sink) const {
        for (size_t i = 0; i < num_cols; i++) {
            // std::cout << '+' << std::string(COL_WIDTH + 2, '-');
            std::string str = "+" + std::string(COL_WIDTH + 2, '-');
            if (sink->ellipsis == false && *sink->offset + RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
                memcpy(sink->data_send + *sink->offset, str.c_str(), str.length());
                *sink->offset = *sink->offset + str.length();
            } else {
                sink->ellipsis = true;
            }
        }
        std::string str = "+\n";
        if (sink->ellipsis == false && *sink->offset + RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
            memcpy(sink->data_send + *sink->offset, str.c_str(), str.length());
            *sink->offset = *sink->offset + str.length();
        } else {
            sink->ellipsis = true;
        }
    }

    void print_record(const std::vector<std::string>& rec_str, OutputSink* sink) const {
        assert(rec_str.size() == num_cols);
        for (auto col : rec_str) {
            if (col.size() > COL_WIDTH) {
                col = col.substr(0, COL_WIDTH - 3) + "...";
            }
            // std::cout << "| " << std::setw(COL_WIDTH) << col << ' ';
            std::stringstream ss;
            ss << "| " << std::setw(COL_WIDTH) << col << " ";
            if (sink->ellipsis == false && *sink->offset + RECORD_COUNT_LENGTH + ss.str().length() < BUFFER_LENGTH) {
                memcpy(sink->data_send + *sink->offset, ss.str().c_str(), ss.str().length());
                *sink->offset = *sink->offset + ss.str().length();
            } else {
                sink->ellipsis = true;
            }
        }
        // std::cout << "|\n";
        std::string str = "|\n";
        if (sink->ellipsis == false && *sink->offset + RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
            memcpy(sink->data_send + *sink->offset, str.c_str(), str.length());
            *sink->offset = *sink->offset + str.length();
        }
    }

    static void print_record_count(size_t num_rec, OutputSink* sink) {
        // std::cout << "Total record(s): " << num_rec << '\n';
        std::string str = "";
        if (sink->ellipsis == true) {
            str = "... ...\n";
        }
        str += "Total record(s): " + std::to_string(num_rec) + '\n';
        memcpy(sink->data_send + *sink->offset, str.c_str(), str.length());
        *sink->offset = *sink->offset + str.length();
    }
};

} // namespace rmdb::common

namespace rmdb {
using common::RecordPrinter;
} // namespace rmdb
