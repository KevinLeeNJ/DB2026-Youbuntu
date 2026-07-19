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

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle* file_handle)
    : file_handle_(file_handle), file_hdr_snapshot_(file_handle->get_scan_file_hdr()) {
    page_slots_.reserve(file_hdr_snapshot_.num_records_per_page);
    page_metas_.reserve(file_hdr_snapshot_.num_records_per_page);
    page_records_.reserve(static_cast<size_t>(file_hdr_snapshot_.num_records_per_page) *
                          file_hdr_snapshot_.record_size);
    rid_ = Rid{-1, -1};
    for (page_id_t page_no = RM_FIRST_RECORD_PAGE; page_no < file_hdr_snapshot_.num_pages; ++page_no) {
        if (load_page(page_no)) {
            return;
        }
    }
}

int RmScan::record_size() const {
    return file_hdr_snapshot_.record_size;
}

bool RmScan::load_page(page_id_t page_no) {
    Page* page = file_handle_->buffer_pool_manager_->fetch_page(PageId{file_handle_->fd_, page_no});
    if (page == nullptr) {
        throw PageNotExistError("record", page_no);
    }

    page_slots_.clear();
    page_metas_.clear();
    page_records_.clear();
    {
        RmPageReadGuard page_guard(file_handle_->buffer_pool_manager_, page->get_page_id(), page);
        RmPageHandle page_handle(&file_hdr_snapshot_, page);
        for (int slot = Bitmap::first_bit(true, page_handle.bitmap, file_hdr_snapshot_.num_records_per_page);
             slot < file_hdr_snapshot_.num_records_per_page;
             slot = Bitmap::next_bit(true, page_handle.bitmap, file_hdr_snapshot_.num_records_per_page, slot)) {
            page_slots_.push_back(slot);
            page_metas_.push_back(page_handle.get_meta(slot));
            const char* record = page_handle.get_slot(slot);
            page_records_.insert(page_records_.end(), record, record + file_hdr_snapshot_.record_size);
        }
    }

    if (page_slots_.empty()) {
        return false;
    }
    page_index_ = 0;
    rid_ = Rid{page_no, page_slots_[0]};
    return true;
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 *        每页在一个读锁临界区内复制有效 slot，随后无锁遍历页快照。
 */
void RmScan::next() {
    if (is_end()) {
        return;
    }
    if (++page_index_ < page_slots_.size()) {
        rid_.slot_no = page_slots_[page_index_];
        return;
    }
    const page_id_t page_upper_bound = file_handle_->get_num_pages();
    for (page_id_t page_no = rid_.page_no + 1; page_no < page_upper_bound; ++page_no) {
        if (load_page(page_no)) {
            return;
        }
    }
    rid_ = Rid{-1, -1};
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    return rid_.page_no == -1;
}
/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}
