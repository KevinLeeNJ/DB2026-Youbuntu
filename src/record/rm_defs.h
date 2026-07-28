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

#include <type_traits>

#include "defs.h"
#include "storage/buffer_pool_manager.h"

constexpr int RM_NO_PAGE = -1;
constexpr int RM_FILE_HDR_PAGE = 0;
constexpr int RM_FIRST_RECORD_PAGE = 1;
// 单条记录（数据区 + 尾部 null bitmap）的上限。
//
// 这只是一个校验闸门，不影响页密度：num_records_per_page 由实际的 record_size
// 算出，把闸门抬高不会让小记录的每页条数变少。而闸门定得太低是致命的——标准
// TPC-C 的 customer.c_data 是 char(500)，整条记录 749 字节，512 的旧上限会让
// CREATE TABLE 直接抛 InvalidRecordSizeError，装载预算一秒都用不上。final.md
// 没有公开 DDL 的列宽，所以这里留足余量。
//
// 上界由"每页至少放得下一条记录"决定：
//   RM_PAGE_META_OFFSET + TUPLE_META_SIZE + 1(bitmap) + record_size <= PAGE_SIZE
// 即 record_size <= 4096 - 16 - 32 - 1 = 4047，2048 在安全区内（还剩一半）。
constexpr int RM_MAX_RECORD_SIZE = 2048;

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

// TupleMeta — 每个 slot 的版本元数据，按 num_records_per_page 个一组存放在页头之后。
//
// 布局很值钱：每页的元组数是 (可用空间) / (record_size + TUPLE_META_SIZE)，窄表
// 上 TupleMeta 就是页密度的主要开销（new_orders 的记录只有 13 字节）。因此把两个
// 标志位打包进 writer_txn_id_ 所在的那个 64 位存储单元，消掉原布局里
// `bool bool + 6 字节纯 padding` 的空洞：40 → 32 字节。
//
// 位域刻意沿用原来的成员名，调用点写法不变（`meta.is_committed_ = true`），
// 没有任何位移/掩码散落在外面。writer_txn_id_ 只让出高 2 位，剩下 62 位是有符号
// 事务号，覆盖 ±2^61，远超任何可能的事务计数，且 INVALID_TXN_ID(-1) 正常回环。
struct TupleMeta {
    static constexpr int WRITER_TXN_ID_BITS = 62; // 让出高 2 位给下面两个标志

    timestamp_t commit_ts_{INVALID_TS};           // commit timestamp (valid when is_committed_)
    txn_id_t writer_txn_id_ : WRITER_TXN_ID_BITS; // transaction that wrote this version
    uint64_t is_committed_ : 1;                   // true if the writer has committed
    uint64_t is_deleted_ : 1;                     // true if this tuple is logically deleted
    UndoLink version_chain_head_;                 // pointer to undo storage chain

    // 位域在 C++17 里不能带默认成员初始化器，因此默认值放在构造函数里；
    // 语义与旧的 NSDMI 完全一致。
    TupleMeta() : writer_txn_id_(INVALID_TXN_ID), is_committed_(0), is_deleted_(0) {}

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

// 位域打包依赖 Itanium ABI 的相邻位域合并规则（GCC/Clang on Linux）。真的没打包成
// 一个 64 位单元时页密度会静默退化，所以在这里响亮地失败而不是默默变慢。
static_assert(TUPLE_META_SIZE == 32, "TupleMeta must stay 32 bytes: page density depends on it");
static_assert(std::is_trivially_copyable_v<TupleMeta>, "TupleMeta lives in raw page memory");

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

/**
 * @brief 一个页面最多能放多少条记录。
 *
 * 页面布局：RM_PAGE_META_OFFSET | n * TupleMeta | ceil(n/8) bitmap | n * record_size
 * 约束：RM_PAGE_META_OFFSET + n * (record_size + TUPLE_META_SIZE) + ceil(n/8) <= PAGE_SIZE
 * 解出的 n 就是下面的闭式解（BITMAP_WIDTH = 8 位/字节）。
 */
constexpr int rm_num_records_per_page(int record_size) {
    constexpr int kBitmapWidth = 8;
    const int effective_record_size = record_size + TUPLE_META_SIZE;
    return (kBitmapWidth * (PAGE_SIZE - 1 - RM_PAGE_META_OFFSET) + 1) / (1 + effective_record_size * kBitmapWidth);
}

// 记录上限必须保证"每页至少一条"，否则插入会永远失败。
static_assert(rm_num_records_per_page(RM_MAX_RECORD_SIZE) >= 1,
              "RM_MAX_RECORD_SIZE is too large: a page could not hold a single record");

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
        size = read_unaligned<int>(data_);
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
