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
RmScan::RmScan(const RmFileHandle* file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    rid_.page_no = 1; // 从1开始，因为0页是文件头
    rid_.slot_no = -1;
    next(); // 寻找第一个存放了记录的位置
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    bool flag = false;                                       // 用于标记是否找到下一个有效的记录
    while (rid_.page_no < file_handle_->file_hdr_.num_pages) // 没到最后一页
    {
        RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no);
        int next_slot = Bitmap::next_bit(true, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page,
                                         rid_.slot_no); // 找到下一个存放了记录的slot
        PageId page_id = page_handle.page->get_page_id();
        if (next_slot != file_handle_->file_hdr_.num_records_per_page) // 成功找到
        {
            flag = true;
            rid_.slot_no = next_slot;
            file_handle_->buffer_pool_manager_->unpin_page(page_id, false);
            break;
        }
        file_handle_->buffer_pool_manager_->unpin_page(page_id, false);
        // 移动到下一页
        rid_.page_no++;
        rid_.slot_no = -1;
    }
    if (!flag)             // 没有找到下一个有效的记录
        rid_.page_no = -1; // 没有更多记录
    return;
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