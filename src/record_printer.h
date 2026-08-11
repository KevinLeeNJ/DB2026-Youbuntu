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
#include <charconv>
#include <iostream>
#include <iomanip>
#include <iterator>
#include <string>
#include "common/context.h"
#include "common/config.h"

#define RECORD_COUNT_LENGTH 40

class RecordPrinter {
    static constexpr size_t COL_WIDTH = 16;
    size_t num_cols;

public:
    RecordPrinter(size_t num_cols_) : num_cols(num_cols_) {
        assert(num_cols_ > 0);
    }

    void print_separator(ExecutionOutput* output) const {
        for (size_t i = 0; i < num_cols; i++) {
            constexpr size_t separator_length = COL_WIDTH + 3;
            if (!output->ellipsis && *output->offset + RECORD_COUNT_LENGTH + separator_length < BUFFER_LENGTH) {
                char* dest = output->data_send + *output->offset;
                dest[0] = '+';
                memset(dest + 1, '-', COL_WIDTH + 2);
                *output->offset += separator_length;
            } else {
                output->ellipsis = true;
            }
        }
        constexpr size_t end_length = 2;
        if (!output->ellipsis && *output->offset + RECORD_COUNT_LENGTH + end_length < BUFFER_LENGTH) {
            char* dest = output->data_send + *output->offset;
            dest[0] = '+';
            dest[1] = '\n';
            *output->offset += end_length;
        } else {
            output->ellipsis = true;
        }
    }

    void print_record(const std::vector<std::string>& rec_str, ExecutionOutput* output) const {
        assert(rec_str.size() == num_cols);
        constexpr size_t cell_length = COL_WIDTH + 3;
        for (const auto& col : rec_str) {
            if (!output->ellipsis && *output->offset + RECORD_COUNT_LENGTH + cell_length < BUFFER_LENGTH) {
                char* dest = output->data_send + *output->offset;
                dest[0] = '|';
                dest[1] = ' ';
                if (col.size() > COL_WIDTH) {
                    memcpy(dest + 2, col.data(), COL_WIDTH - 3);
                    memcpy(dest + 2 + COL_WIDTH - 3, "...", 3);
                } else {
                    const size_t padding = COL_WIDTH - col.size();
                    memset(dest + 2, ' ', padding);
                    memcpy(dest + 2 + padding, col.data(), col.size());
                }
                dest[cell_length - 1] = ' ';
                *output->offset += cell_length;
            } else {
                output->ellipsis = true;
            }
        }
        constexpr size_t end_length = 2;
        if (!output->ellipsis && *output->offset + RECORD_COUNT_LENGTH + end_length < BUFFER_LENGTH) {
            char* dest = output->data_send + *output->offset;
            dest[0] = '|';
            dest[1] = '\n';
            *output->offset += end_length;
        }
    }

    static void print_record_count(size_t num_rec, ExecutionOutput* output) {
        if (output->ellipsis) {
            constexpr char ellipsis[] = "... ...\n";
            memcpy(output->data_send + *output->offset, ellipsis, sizeof(ellipsis) - 1);
            *output->offset += sizeof(ellipsis) - 1;
        }
        constexpr char prefix[] = "Total record(s): ";
        memcpy(output->data_send + *output->offset, prefix, sizeof(prefix) - 1);
        *output->offset += sizeof(prefix) - 1;
        char count_buffer[3 * sizeof(size_t) + 1];
        const auto [count_end, error] = std::to_chars(std::begin(count_buffer), std::end(count_buffer), num_rec);
        assert(error == std::errc{});
        memcpy(output->data_send + *output->offset, count_buffer, count_end - std::begin(count_buffer));
        *output->offset += count_end - std::begin(count_buffer);
        output->data_send[(*output->offset)++] = '\n';
    }
};
