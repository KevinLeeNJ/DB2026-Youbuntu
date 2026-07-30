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

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "common/config.h"
#include "defs.h"

// INDEX_SMO records are deliberately bounded independently from the ordinary
// WAL staging buffer. A cascading split normally touches only a few pages, but
// the format and append path must remain correct when its full-page images do
// not fit in LOG_BUFFER_SIZE.
static constexpr uint32_t MAX_INDEX_SMO_RECORD_BYTES = 64U * 1024U * 1024U;
static constexpr uint32_t MAX_INDEX_SMO_PAGE_COUNT =
    (MAX_INDEX_SMO_RECORD_BYTES - PAGE_SIZE) / (PAGE_SIZE + sizeof(page_id_t));
static constexpr uint32_t MAX_INDEX_SMO_FILE_NAME_BYTES = 4096;

struct IndexSmoPageImage {
    page_id_t page_no{INVALID_PAGE_ID};
    std::array<char, PAGE_SIZE> bytes{};
};

struct IndexSmoWalData {
    std::string index_file_name;
    uint64_t index_generation{0};
    std::vector<IndexSmoPageImage> pages;
    std::array<char, PAGE_SIZE> header{};
};
