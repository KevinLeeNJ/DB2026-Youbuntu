/* Copyright (c) 2023 Renmin University of China
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

    void print_separator(Context* context) const {
        for (size_t i = 0; i < num_cols; i++) {
            constexpr size_t separator_length = COL_WIDTH + 3;
            if (!context->ellipsis_ &&
                *context->offset_ + RECORD_COUNT_LENGTH + separator_length < BUFFER_LENGTH) {
                char* dest = context->data_send_ + *context->offset_;
                dest[0] = '+';
                memset(dest + 1, '-', COL_WIDTH + 2);
                *context->offset_ += separator_length;
            } else {
                context->ellipsis_ = true;
            }
        }
        constexpr size_t end_length = 2;
        if (!context->ellipsis_ && *context->offset_ + RECORD_COUNT_LENGTH + end_length < BUFFER_LENGTH) {
            char* dest = context->data_send_ + *context->offset_;
            dest[0] = '+';
            dest[1] = '\n';
            *context->offset_ += end_length;
        } else {
            context->ellipsis_ = true;
        }
    }

    void print_record(const std::vector<std::string>& rec_str, Context* context) const {
        assert(rec_str.size() == num_cols);
        constexpr size_t cell_length = COL_WIDTH + 3;
        for (const auto& col : rec_str) {
            if (!context->ellipsis_ && *context->offset_ + RECORD_COUNT_LENGTH + cell_length < BUFFER_LENGTH) {
                char* dest = context->data_send_ + *context->offset_;
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
                *context->offset_ += cell_length;
            } else {
                context->ellipsis_ = true;
            }
        }
        constexpr size_t end_length = 2;
        if (!context->ellipsis_ && *context->offset_ + RECORD_COUNT_LENGTH + end_length < BUFFER_LENGTH) {
            char* dest = context->data_send_ + *context->offset_;
            dest[0] = '|';
            dest[1] = '\n';
            *context->offset_ += end_length;
        }
    }

    static void print_record_count(size_t num_rec, Context* context) {
        if (context->ellipsis_ == true) {
            constexpr char ellipsis[] = "... ...\n";
            memcpy(context->data_send_ + *context->offset_, ellipsis, sizeof(ellipsis) - 1);
            *context->offset_ += sizeof(ellipsis) - 1;
        }
        constexpr char prefix[] = "Total record(s): ";
        memcpy(context->data_send_ + *context->offset_, prefix, sizeof(prefix) - 1);
        *context->offset_ += sizeof(prefix) - 1;
        char count_buffer[3 * sizeof(size_t) + 1];
        const auto [count_end, error] = std::to_chars(std::begin(count_buffer), std::end(count_buffer), num_rec);
        assert(error == std::errc{});
        memcpy(context->data_send_ + *context->offset_, count_buffer, count_end - std::begin(count_buffer));
        *context->offset_ += count_end - std::begin(count_buffer);
        context->data_send_[(*context->offset_)++] = '\n';
    }
};
