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

#include <assert.h>

#include <memory>
#include <mutex>

#include "bitmap.h"
#include "rm_defs.h"
#include "statement/statement_context.h"

namespace rmdb::record {
class RmManager;

/* 对表数据文件中的页面进行封装 */
struct RmPageHandle {
    const RmFileHdr* file_hdr = nullptr; // 当前页面所在文件的文件头指针
    Page* page = nullptr;                // 页面的实际数据，包括页面存储的数据、元信息等
    RmPageHdr* page_hdr = nullptr; // page->data的第一部分，存储页面元信息，指针指向首地址，长度为sizeof(RmPageHdr)
    TupleMeta* meta_array = nullptr; // TupleMeta 数组，在 RmPageHdr 之后
    char* bitmap =
        nullptr; // page->data中TupleMeta之后的部分，存储页面的bitmap，指针指向首地址，长度为file_hdr->bitmap_size
    char* slots = nullptr; // page->data的第三部分，存储表的记录，指针指向首地址，每个slot的长度为file_hdr->record_size

    RmPageHandle() = default;

    RmPageHandle(const RmFileHdr* fhdr_, Page* page_) : file_hdr(fhdr_), page(page_) {
        page_hdr = reinterpret_cast<RmPageHdr*>(page->get_data() + page->OFFSET_PAGE_HDR);
        meta_array = reinterpret_cast<TupleMeta*>(page->get_data() + RM_PAGE_META_OFFSET);
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

struct RmPinnedInsert {
    RmPageHandle page_handle;
    Rid rid;
};

/* A record paired with its TupleMeta, fetched in a single buffer-pool pin. */
struct RmRecordWithMeta {
    TupleMeta meta;
    std::unique_ptr<RmRecord> record;
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
    std::mutex physical_latch_;

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

    std::unique_ptr<RmRecord> get_record(const Rid& rid, StatementContext* context) const;

    /* Fetch both TupleMeta and record data in a single page pin, halving the
       buffer-pool latch acquisitions compared to get_tuple_meta + get_record. */
    RmRecordWithMeta get_record_with_meta(const Rid& rid, StatementContext* context) const;

    Rid insert_record(char* buf, StatementContext* context);

    void insert_record(const Rid& rid, char* buf);

    RmPinnedInsert prepare_insert_record();

    void finish_insert_record(RmPinnedInsert& insert, char* buf);

    void abort_prepared_insert(RmPinnedInsert& insert);

    void delete_record(const Rid& rid, StatementContext* context);

    void update_record(const Rid& rid, char* buf, StatementContext* context);

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

    std::mutex& get_physical_latch() {
        return physical_latch_;
    }

    // Bulk-load: pins data page across rows to skip per-record fetch/unpin.
    struct PinnedInserter {
        RmFileHandle* fh;
        RmPageHandle page;
        bool active = false;

        explicit PinnedInserter(RmFileHandle* f) : fh(f) {
            page = fh->create_page_handle();
            active = true;
        }
        ~PinnedInserter() {
            if (active) {
                fh->buffer_pool_manager_->unpin_page(page.page->get_page_id(), true);
            }
        }
        PinnedInserter(const PinnedInserter&) = delete;
        PinnedInserter& operator=(const PinnedInserter&) = delete;
        PinnedInserter(PinnedInserter&& o) noexcept : fh(o.fh), page(o.page), active(o.active) {
            o.active = false;
        }

        // Insert one record into the pinned page; returns its Rid. Caller must
        // ensure `buf` points to `record_size` bytes.
        Rid insert(const char* buf) {
            int slot_no = Bitmap::first_bit(false, page.bitmap, fh->file_hdr_.num_records_per_page);
            if (slot_no == fh->file_hdr_.num_records_per_page) {
                // Page full → advance to next free page
                fh->buffer_pool_manager_->unpin_page(page.page->get_page_id(), true);
                if (fh->file_hdr_.first_free_page_no == -1) {
                    page = fh->create_new_page_handle();
                } else {
                    page = fh->fetch_page_handle(fh->file_hdr_.first_free_page_no);
                }
                slot_no = Bitmap::first_bit(false, page.bitmap, fh->file_hdr_.num_records_per_page);
            }
            memcpy(page.get_slot(slot_no), buf, fh->file_hdr_.record_size);
            TupleMeta& meta = page.get_meta(slot_no);
            meta.commit_ts_ = 0;
            meta.writer_txn_id_ = INVALID_TXN_ID;
            meta.is_committed_ = true;
            meta.is_deleted_ = false;
            meta.version_chain_head_ = UndoLink{};
            Bitmap::set(page.bitmap, slot_no);
            page.page_hdr->num_records++;
            if (page.page_hdr->num_records >= fh->file_hdr_.num_records_per_page) {
                fh->file_hdr_.first_free_page_no = page.page_hdr->next_free_page_no;
            }
            return Rid{page.page->get_page_id().page_no, slot_no};
        }
    };

private:
    RmPageHandle create_page_handle();

    void release_page_handle(RmPageHandle& page_handle);
};

} // namespace rmdb::record

namespace rmdb {
using record::RmFileHandle;
using record::RmPageHandle;
using record::RmPinnedInsert;
using record::RmRecordWithMeta;
} // namespace rmdb
