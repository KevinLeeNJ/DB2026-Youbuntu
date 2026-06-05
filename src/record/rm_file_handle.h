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

#include <assert.h>

#include <memory>

#include "bitmap.h"
#include "common/context.h"
#include "rm_defs.h"

class RmManager;

/* 对表数据文件中的页面进行封装 */
struct RmPageHandle {
    const RmFileHdr* file_hdr; // 当前页面所在文件的文件头指针
    Page* page;                // 页面的实际数据，包括页面存储的数据、元信息等
    RmPageHdr* page_hdr; // page->data的第一部分，存储页面元信息，指针指向首地址，长度为sizeof(RmPageHdr)
    TupleMeta* meta_array; // TupleMeta 数组，在 RmPageHdr 之后
    char* bitmap; // page->data中TupleMeta之后的部分，存储页面的bitmap，指针指向首地址，长度为file_hdr->bitmap_size
    char* slots; // page->data的第三部分，存储表的记录，指针指向首地址，每个slot的长度为file_hdr->record_size

    RmPageHandle(const RmFileHdr* fhdr_, Page* page_) : file_hdr(fhdr_), page(page_) {
        page_hdr = reinterpret_cast<RmPageHdr*>(page->get_data() + page->OFFSET_PAGE_HDR);
        meta_array = reinterpret_cast<TupleMeta*>(page->get_data() + sizeof(RmPageHdr) + page->OFFSET_PAGE_HDR);
        bitmap = reinterpret_cast<char*>(meta_array + file_hdr->num_records_per_page);
        slots = bitmap + file_hdr->bitmap_size;
    }

    // 返回指定slot_no的slot存储收地址
    char* get_slot(int slot_no) const {
        return slots + slot_no * file_hdr->record_size; // slots的首地址 + slot个数 * 每个slot的大小(每个record的大小)
    }

    // 返回指定slot_no的TupleMeta引用
    TupleMeta& get_meta(int slot_no) const {
        return meta_array[slot_no];
    }
};

/* 每个RmFileHandle对应一个表的数据文件，里面有多个page，每个page的数据封装在RmPageHandle中 */
class RmFileHandle {
    friend class RmScan;
    friend class RmManager;

private:
    DiskManager* disk_manager_;
    BufferPoolManager* buffer_pool_manager_;
    int fd_;             // 打开文件后产生的文件句柄
    RmFileHdr file_hdr_; // 文件头，维护当前表文件的元数据

public:
    RmFileHandle(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, int fd)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
        // 注意：这里从磁盘中读出文件描述符为fd的文件的file_hdr，读到内存中
        // 这里实际就是初始化file_hdr，只不过是从磁盘中读出进行初始化
        // init file_hdr_
        disk_manager_->read_page(fd, RM_FILE_HDR_PAGE, (char*)&file_hdr_, sizeof(file_hdr_));
        // disk_manager管理的fd对应的文件中，设置从file_hdr_.num_pages开始分配page_no
        disk_manager_->set_fd2pageno(fd, file_hdr_.num_pages);
    }

    RmFileHdr get_file_hdr() {
        return file_hdr_;
    }
    int GetFd() {
        return fd_;
    }

    /* 判断指定位置上是否已经存在一条记录，通过Bitmap来判断 */
    bool is_record(const Rid& rid) const {
        RmPageHandle page_handle = fetch_page_handle(rid.page_no);
        bool is_record = Bitmap::is_set(page_handle.bitmap, rid.slot_no); // page的slot_no位置上是否有record
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        return is_record;
    }

    std::unique_ptr<RmRecord> get_record(const Rid& rid, Context* context) const;

    Rid insert_record(char* buf, Context* context);

    void insert_record(const Rid& rid, char* buf);

    void delete_record(const Rid& rid, Context* context);

    void update_record(const Rid& rid, char* buf, Context* context);

    RmPageHandle create_new_page_handle();

    RmPageHandle fetch_page_handle(int page_no) const;

    // MVCC: update TupleMeta for a slot (pins and unpins the page)
    void set_tuple_meta(const Rid& rid, const TupleMeta& meta);

    // MVCC: get TupleMeta for a slot
    TupleMeta get_tuple_meta(const Rid& rid) const;

    // Access buffer pool manager (for TupleMeta modifications that need explicit pin control)
    BufferPoolManager* get_bpm() {
        return buffer_pool_manager_;
    }

private:
    RmPageHandle create_page_handle();

    void release_page_handle(RmPageHandle& page_handle);
};
