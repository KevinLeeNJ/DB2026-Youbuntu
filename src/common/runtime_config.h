/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <charconv>
#include <cstddef>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <system_error>

#include "common/config.h"

struct BufferPoolRuntimeConfig {
    size_t gibibytes;
    size_t pages;
};

static constexpr size_t DEFAULT_BUFFER_POOL_GIB = 3;
static constexpr size_t MAX_BUFFER_POOL_GIB = 6;
static constexpr size_t PAGES_PER_GIB = (size_t{1} << 30) / PAGE_SIZE;

static_assert((size_t{1} << 30) % PAGE_SIZE == 0, "PAGE_SIZE must divide one GiB exactly");
static_assert(DEFAULT_BUFFER_POOL_GIB >= 1 && DEFAULT_BUFFER_POOL_GIB <= MAX_BUFFER_POOL_GIB,
              "the runtime buffer-pool default must be within the authorized range");

inline BufferPoolRuntimeConfig parse_buffer_pool_config(std::string_view value) {
    size_t gibibytes = 0;
    const char* begin = value.data();
    const char* end = begin + value.size();
    const auto result = std::from_chars(begin, end, gibibytes);
    if (value.empty() || result.ec != std::errc{} || result.ptr != end || gibibytes == 0 ||
        gibibytes > MAX_BUFFER_POOL_GIB) {
        throw std::invalid_argument("RMDB_BUFFER_POOL_GIB must be an integer between 1 and 6");
    }
    if (gibibytes > std::numeric_limits<size_t>::max() / PAGES_PER_GIB) {
        throw std::overflow_error("RMDB_BUFFER_POOL_GIB page count overflows size_t");
    }
    return BufferPoolRuntimeConfig{gibibytes, gibibytes * PAGES_PER_GIB};
}

inline BufferPoolRuntimeConfig default_buffer_pool_config() {
    return BufferPoolRuntimeConfig{DEFAULT_BUFFER_POOL_GIB, DEFAULT_BUFFER_POOL_GIB * PAGES_PER_GIB};
}
