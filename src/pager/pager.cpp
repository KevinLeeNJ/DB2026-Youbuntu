/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "pager/pager.h"

#include "recovery/log_manager.h"

namespace rmdb::pager {

void Pager::flush_wal_if_needed() {
    if (log_manager_ != nullptr) {
        log_manager_->flush_log_to_disk();
    }
}

bool Pager::flush_page(rmdb::storage::PageId page_id) {
    flush_wal_if_needed();
    return bpm_->flush_page(page_id);
}

void Pager::flush_all_pages(int fd) {
    flush_wal_if_needed();
    bpm_->flush_all_pages(fd);
}

void Pager::discard_pages(int fd) {
    bpm_->delete_all_pages(fd);
}

void Pager::flush_before_write() noexcept {
    // BPM eviction 路径调用：保证脏页写盘前 WAL 已持久化
    flush_wal_if_needed();
}

} // namespace rmdb::pager
