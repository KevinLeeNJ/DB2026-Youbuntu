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

#include "rm_defs.h"

class RmFileHandle;

class RmScan : public RecScan {
    const RmFileHandle* file_handle_;
    RmFileHdr file_hdr_snapshot_;
    Rid rid_;
    std::vector<int> page_slots_;
    std::vector<TupleMeta> page_metas_;
    std::vector<char> page_records_;
    size_t page_index_{0};

    bool load_page(page_id_t page_no);
    int record_size() const;

public:
    RmScan(const RmFileHandle* file_handle);

    ~RmScan() override = default;

    RmScan(const RmScan&) = delete;
    RmScan& operator=(const RmScan&) = delete;

    void next() override;

    bool is_end() const override;

    Rid rid() const override;

    const TupleMeta& current_meta() const {
        return page_metas_[page_index_];
    }

    const char* current_data() const {
        const size_t offset = page_index_ * static_cast<size_t>(record_size());
        return page_records_.data() + offset;
    }
};
