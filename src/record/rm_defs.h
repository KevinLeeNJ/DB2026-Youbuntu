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

#include "defs.h"
#include "storage/buffer_pool_manager.h"

namespace rmdb::record {
constexpr int RM_NO_PAGE = -1;
constexpr int RM_FILE_HDR_PAGE = 0;
constexpr int RM_FIRST_RECORD_PAGE = 1;
constexpr int RM_MAX_RECORD_SIZE = 512;

// UndoLink — points to a physical undo storage location.
// For in-memory undo (transitional): undo_page_id_ = 0, undo_slot_offset_ = log_index
// For physical undo: undo_page_id_ = undo page id, undo_slot_offset_ = byte offset
struct UndoLink {
    page_id_t undo_page_id_{INVALID_PAGE_ID};
    int undo_slot_offset_{0};
    txn_id_t undo_txn_id_{INVALID_TXN_ID}; // owner txn for in-memory undo lookup

    bool IsValid() const {
        return undo_txn_id_ != INVALID_TXN_ID;
    }

    friend auto operator==(const UndoLink& a, const UndoLink& b) {
        return a.undo_page_id_ == b.undo_page_id_ && a.undo_slot_offset_ == b.undo_slot_offset_ &&
               a.undo_txn_id_ == b.undo_txn_id_;
    }

    friend auto operator!=(const UndoLink& a, const UndoLink& b) {
        return !(a == b);
    }
};

struct TupleMeta {
    timestamp_t commit_ts_{INVALID_TS};      // commit timestamp (valid when is_committed_)
    txn_id_t writer_txn_id_{INVALID_TXN_ID}; // transaction that wrote this version
    bool is_committed_{false};               // true if the writer has committed
    bool is_deleted_{false};                 // true if this tuple is logically deleted
    UndoLink version_chain_head_;            // pointer to undo storage chain

    friend auto operator==(const TupleMeta& a, const TupleMeta& b) {
        return a.commit_ts_ == b.commit_ts_ && a.writer_txn_id_ == b.writer_txn_id_ &&
               a.is_committed_ == b.is_committed_ && a.is_deleted_ == b.is_deleted_ &&
               a.version_chain_head_ == b.version_chain_head_;
    }

    friend auto operator!=(const TupleMeta& a, const TupleMeta& b) {
        return !(a == b);
    }
};

// Size of TupleMeta in bytes (used for page layout calculations)
constexpr int TUPLE_META_SIZE = sizeof(TupleMeta);

/* 文件头，记录表数据文件的元信息，写入磁盘中文件的第0号页面 */
struct RmFileHdr {
    int record_size; // 表中每条记录的大小，由于不包含变长字段，因此当前字段初始化后保持不变
    int num_pages;            // 文件中分配的页面个数（初始化为1）
    int num_records_per_page; // 每个页面最多能存储的元组个数
    int first_free_page_no;   // 文件中当前第一个包含空闲空间的页面号（初始化为-1）
    int bitmap_size;          // 每个页面bitmap大小
};

/* 表数据文件中每个页面的页头，记录每个页面的元信息 */
struct RmPageHdr {
    int next_free_page_no; // 当前页面满了之后，下一个包含空闲空间的页面号（初始化为-1）
    int num_records;       // 当前页面中当前已经存储的记录个数（初始化为0）
};

constexpr int AlignUp(int value, int alignment) {
    return ((value + alignment - 1) / alignment) * alignment;
}

constexpr int RM_PAGE_META_OFFSET =
    AlignUp(static_cast<int>(Page::OFFSET_PAGE_HDR) + static_cast<int>(sizeof(RmPageHdr)), alignof(TupleMeta));

/* 表中的记录 */
struct RmRecord {
    char* data;              // 记录的数据
    int size;                // 记录的大小
    bool allocated_ = false; // 是否已经为数据分配空间

    RmRecord() = default;

    RmRecord(const RmRecord& other) {
        size = other.size;
        data = new char[size];
        memcpy(data, other.data, size);
        allocated_ = true;
    };

    RmRecord& operator=(const RmRecord& other) {
        size = other.size;
        if (allocated_) {
            delete[] data;
        }
        data = new char[size];
        memcpy(data, other.data, size);
        allocated_ = true;
        return *this;
    };

    RmRecord(int size_) {
        size = size_;
        data = new char[size_];
        allocated_ = true;
    }

    RmRecord(int size_, char* data_) {
        size = size_;
        data = new char[size_];
        memcpy(data, data_, size_);
        allocated_ = true;
    }

    void SetData(char* data_) {
        memcpy(data, data_, size);
    }

    void Deserialize(const char* data_) {
        size = *reinterpret_cast<const int*>(data_);
        if (allocated_) {
            delete[] data;
        }
        data = new char[size];
        memcpy(data, data_ + sizeof(int), size);
    }

    ~RmRecord() {
        if (allocated_) {
            delete[] data;
        }
        allocated_ = false;
        data = nullptr;
    }
};

} // namespace rmdb::record

namespace rmdb {
using record::AlignUp;
using record::RM_FILE_HDR_PAGE;
using record::RM_FIRST_RECORD_PAGE;
using record::RM_MAX_RECORD_SIZE;
using record::RM_NO_PAGE;
using record::RM_PAGE_META_OFFSET;
using record::RmFileHdr;
using record::RmPageHdr;
using record::RmRecord;
using record::TUPLE_META_SIZE;
using record::TupleMeta;
using record::UndoLink;
} // namespace rmdb
