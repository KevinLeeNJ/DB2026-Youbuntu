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

namespace rmdb::record {
class RmFileHandle;

class RmScan : public RecScan {
    const RmFileHandle* file_handle_;
    Rid rid_;

    // Pinned current data page. Stays pinned while walking slots on the same
    // page; released on move to next page / destruction / reaching end.
    Page* pinned_page_ = nullptr;

    void release_page();

public:
    RmScan(const RmFileHandle* file_handle);

    ~RmScan() override;

    RmScan(const RmScan&) = delete;
    RmScan& operator=(const RmScan&) = delete;

    void next() override;

    bool is_end() const override;

    Rid rid() const override;
};

} // namespace rmdb::record

namespace rmdb {
using record::RmScan;
} // namespace rmdb
