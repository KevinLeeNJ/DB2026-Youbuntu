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

#include <atomic>

#include <assert.h>

#include <array>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

#include "bitmap.h"
#include "common/context.h"
#include "rm_defs.h"

class RmManager;

struct RmFileHeaderSnapshot {
    int fd{-1};
    std::array<char, sizeof(RmFileHdr)> bytes{};
};

struct RecoveryPageFinalizeResult {
    page_id_t page_no{INVALID_PAGE_ID};
    bool is_full{false};
    uint32_t normalized_records{0};
    uint32_t tombstones_removed{0};
};

struct RecoveryPagePublishStats {
    uint64_t candidates{0};
    uint64_t links{0};
    uint64_t candidate_ns{0};
    uint64_t link_ns{0};
    uint64_t wall_ns{0};
    uint64_t unpin_failures{0};
};

struct RecoveryPinnedFinalizePage {
    page_id_t page_no{INVALID_PAGE_ID};
    Page* page{nullptr};
};

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
    std::unique_ptr<std::unique_lock<std::shared_mutex>> page_lock;
    bool reserved{false};
};

/* A record paired with its TupleMeta, fetched in a single buffer-pool pin. */
struct RmRecordWithMeta {
    TupleMeta meta;
    std::unique_ptr<RmRecord> record;
};

// One input to the page-grouped record reader. Callers sort requests by
// rid.page_no while original_position maps each copied tuple back to its
// logical scan order.
struct RmBatchReadRequest {
    Rid rid;
    size_t original_position{0};
};

struct RmCopiedRecordWithMeta {
    TupleMeta meta;
    bool present{false};
};

enum class RmTupleMetaProbeState : uint8_t { Present, Absent, Retry };

// The GC path needs to determine both allocation and MVCC state while holding
// a single page pin and shared latch. `Retry` means that the probe could not
// acquire a page; it is never evidence that a candidate is reclaimable.
struct RmTupleMetaProbe {
    RmTupleMetaProbeState state{RmTupleMetaProbeState::Absent};
    TupleMeta meta;
};

// A read-only view of a record slot. The guard keeps both the buffer-pool
// frame resident and the page payload stable while executors consume view.
struct RmRecordView {
    const char* data = nullptr;
    uint32_t size = 0;
};

class RmPageReadGuard {
public:
    RmPageReadGuard() = default;

    RmPageReadGuard(BufferPoolManager* bpm, PageId page_id, Page* page)
        : bpm_(bpm), page_id_(page_id), page_(page), page_lock_(page->latch()) {}

    ~RmPageReadGuard() {
        release();
    }

    RmPageReadGuard(const RmPageReadGuard&) = delete;
    RmPageReadGuard& operator=(const RmPageReadGuard&) = delete;

    RmPageReadGuard(RmPageReadGuard&& other) noexcept
        : bpm_(other.bpm_), page_id_(other.page_id_), page_(other.page_), page_lock_(std::move(other.page_lock_)) {
        other.bpm_ = nullptr;
        other.page_ = nullptr;
    }

    RmPageReadGuard& operator=(RmPageReadGuard&& other) noexcept {
        if (this != &other) {
            release();
            bpm_ = other.bpm_;
            page_id_ = other.page_id_;
            page_ = other.page_;
            page_lock_ = std::move(other.page_lock_);
            other.bpm_ = nullptr;
            other.page_ = nullptr;
        }
        return *this;
    }

    Page* page() const {
        return page_;
    }

private:
    void release() {
        if (page_ == nullptr) {
            return;
        }
        page_lock_.unlock();
        if (bpm_ != nullptr) {
            bpm_->unpin_page(page_id_, false);
        }
        bpm_ = nullptr;
        page_ = nullptr;
    }

    BufferPoolManager* bpm_ = nullptr;
    PageId page_id_{};
    Page* page_ = nullptr;
    std::shared_lock<std::shared_mutex> page_lock_;
};

struct RmRecordViewWithMeta {
    TupleMeta meta;
    RmRecordView view;
    std::unique_ptr<RmPageReadGuard> guard;
    std::unique_ptr<RmRecord> owned;
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
    std::mutex free_space_latch_;
    std::mutex free_space_init_latch_;
    enum class FreeSpaceInitState : uint8_t { Uninitialized, DurableHintLoaded, BitmapAuthoritative };
    std::atomic<FreeSpaceInitState> free_space_init_state_{FreeSpaceInitState::Uninitialized};
    std::mutex extension_latch_;
    mutable std::mutex file_header_latch_;
    std::vector<page_id_t> free_page_candidates_;
    std::unordered_set<page_id_t> free_page_candidate_set_;
    size_t free_page_cursor_{0};
    std::atomic<bool> fail_recovery_physical_pin_for_test_{false};

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

    RmFileHdr get_file_hdr() const {
        std::lock_guard<std::mutex> lock(file_header_latch_);
        return file_hdr_;
    }

    RmFileHeaderSnapshot capture_file_header_snapshot() const {
        std::lock_guard<std::mutex> lock(file_header_latch_);
        RmFileHeaderSnapshot snapshot;
        snapshot.fd = fd_;
        std::memcpy(snapshot.bytes.data(), &file_hdr_, sizeof(file_hdr_));
        return snapshot;
    }

    int GetFd() {
        return fd_;
    }

    /* 判断指定位置上是否已经存在一条记录，通过Bitmap来判断 */
    bool is_record(const Rid& rid) const {
        RmPageHandle page_handle = fetch_page_handle(rid.page_no);
        bool is_record;
        {
            std::shared_lock<std::shared_mutex> page_lock(page_handle.page->latch());
            is_record = Bitmap::is_set(page_handle.bitmap, rid.slot_no); // page的slot_no位置上是否有record
        }
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        return is_record;
    }

    std::unique_ptr<RmRecord> get_record(const Rid& rid, Context* context) const;

    /* Fetch both TupleMeta and record data in a single page pin, halving the
       buffer-pool latch acquisitions
     * compared to get_tuple_meta + get_record. */
    RmRecordWithMeta get_record_with_meta(const Rid& rid, Context* context) const;

    // Borrow the current committed slot without copying its payload. The
    // returned guard must remain alive while view.data is accessed.
    RmRecordViewWithMeta get_record_view_with_meta(const Rid& rid) const;

    // Copy a page-grouped RID batch into caller-owned storage. Requests with
    // the same page_no must be contiguous; each group uses one buffer-pool pin
    // and one shared page latch. copied and record_arena are indexed by
    // original_position, preserving the caller's logical RID order.
    void copy_records_with_meta_batch(const std::vector<RmBatchReadRequest>& page_grouped_requests,
                                      std::vector<RmCopiedRecordWithMeta>& copied,
                                      std::vector<char>& record_arena) const;

    Rid insert_record(char* buf, Context* context);

    void insert_record(const Rid& rid, char* buf, lsn_t page_lsn = INVALID_LSN, const TupleMeta* tuple_meta = nullptr);

    RmPinnedInsert prepare_insert_record();

    void finish_insert_record(RmPinnedInsert& insert, char* buf, const TupleMeta* tuple_meta = nullptr,
                              lsn_t page_lsn = INVALID_LSN);

    void abort_prepared_insert(RmPinnedInsert& insert);

    void delete_record(const Rid& rid, Context* context, lsn_t page_lsn = INVALID_LSN);

    void update_record(const Rid& rid, char* buf, Context* context, lsn_t page_lsn = INVALID_LSN);

    // Apply the tuple image and its MVCC metadata as one page mutation. The
    // page LSN is installed only after both payloads have been written while
    // the same exclusive page latch is held.
    void apply_tuple_update(const Rid& rid, const char* buf, const TupleMeta& meta, lsn_t page_lsn);

    RmPageHandle create_new_page_handle();

    RmPageHandle fetch_page_handle(int page_no) const;

    // MVCC: update TupleMeta for a slot (pins and unpins the page)
    void set_tuple_meta(const Rid& rid, const TupleMeta& meta, lsn_t page_lsn = INVALID_LSN);

    // Advance the page LSN after a WAL record has been installed for a tuple
    // update. The page LSN is monotonic because recovery uses it as its redo
    // idempotence guard.
    void set_page_lsn(const Rid& rid, lsn_t lsn);

    lsn_t get_page_lsn(const Rid& rid) const;

    // Recovery rebuilds allocation metadata from the on-disk page bitmaps;
    // the file header itself may have been stale when the process crashed.
    void rebuild_file_header_from_pages();

    // Recovery repair for a bounded set of pages. Unlike
    // rebuild_file_header_from_pages(), this only inspects the supplied pages
    // and advances the allocation boundary when a touched RID references a
    // page that was allocated before its file-header update reached disk.
    void repair_file_header_for_pages(const std::vector<page_id_t>& page_nos);

    // Ignores persisted free-list hints and rebuilds the complete process-local
    // candidate set from physical page bitmaps before recovery mutates pages.
    void prepare_recovery_free_space();

    // Final recovery pass for WAL-touched pages. It advances a stale file
    // boundary, removes rolled-back tombstones, normalizes every surviving
    // tuple's MVCC state, recounts the bitmap, and reconciles the free-page
    // candidate exactly once while the page is exclusively latched.
    void finalize_recovery_pages(const std::vector<page_id_t>& page_nos);

    // RecoveryManager workers normalize disjoint pinned pages, then publish a
    // complete per-table result set after the frame-operation token is released.
    void publish_recovery_page_finalization(const std::vector<RecoveryPageFinalizeResult>& page_results,
                                             RecoveryPagePublishStats* stats = nullptr);
    int recovery_physical_page_frontier() const;
    std::optional<RecoveryPinnedFinalizePage>
    pin_recovery_finalize_page(page_id_t page_no, bool physical,
                               const BufferPoolManager::FrameOperationToken& operation);
    RecoveryPageFinalizeResult finalize_recovery_pinned_page(const RecoveryPinnedFinalizePage& page);
    void set_fail_recovery_physical_pin_for_test(bool enabled) {
        fail_recovery_physical_pin_for_test_.store(enabled, std::memory_order_release);
    }

    // MVCC: get TupleMeta for a slot
    TupleMeta get_tuple_meta(const Rid& rid) const;

    // Read allocation and TupleMeta in one bounded, read-only page probe.
    // Invalid page/slot bounds are Absent before touching BPM; unavailable BPM
    // resources and probe exceptions return Retry.
    RmTupleMetaProbe probe_tuple_meta(const Rid& rid) const;

    // Probe several slots from one record page while holding one buffer-pool
    // pin and one shared page latch. Results correspond to slot_nos in order.
    // A fetch failure or probe exception returns Retry for every valid slot in
    // the group; invalid slots remain Absent.
    std::vector<RmTupleMetaProbe> probe_tuple_meta_batch(page_id_t page_no, const std::vector<int>& slot_nos) const;

    // Visit several slots from one record page while holding one buffer-pool
    // pin and one shared page latch. The visitor is called once for every
    // input slot with Absent, Present, or Retry and an optional TupleMeta.
    //
    // The visitor must only update caller-owned probe state. In particular, it
    // must not acquire catalog/history latches while the page latch is held.
    // This API is used by GC to avoid allocating a result vector for every
    // page group.
    template <typename Visitor>
    void visit_tuple_meta_batch(page_id_t page_no, const std::vector<int>& slot_nos, Visitor&& visitor) const {
        const RmFileHdr file_hdr = get_file_hdr();
        const auto valid_slot = [&](int slot_no) { return slot_no >= 0 && slot_no < file_hdr.num_records_per_page; };
        const auto visit_absent = [&]() {
            for (size_t i = 0; i < slot_nos.size(); ++i) {
                visitor(i, RmTupleMetaProbeState::Absent, nullptr);
            }
        };
        const auto visit_retry = [&]() {
            for (size_t i = 0; i < slot_nos.size(); ++i) {
                visitor(i, valid_slot(slot_nos[i]) ? RmTupleMetaProbeState::Retry : RmTupleMetaProbeState::Absent,
                        nullptr);
            }
        };

        if (page_no < RM_FIRST_RECORD_PAGE || page_no >= file_hdr.num_pages) {
            visit_absent();
            return;
        }

        bool has_valid_slot = false;
        for (const int slot_no : slot_nos) {
            has_valid_slot = has_valid_slot || valid_slot(slot_no);
        }
        if (!has_valid_slot) {
            visit_absent();
            return;
        }

        const PageId page_id{fd_, page_no};
        Page* page = nullptr;
        try {
            page = buffer_pool_manager_->fetch_page(page_id);
        } catch (...) {
            visit_retry();
            return;
        }
        if (page == nullptr) {
            visit_retry();
            return;
        }

        std::optional<RmPageReadGuard> page_guard;
        try {
            page_guard.emplace(buffer_pool_manager_, page_id, page);
        } catch (...) {
            buffer_pool_manager_->unpin_page(page_id, false);
            visit_retry();
            return;
        }
        RmPageHandle page_handle(&file_hdr, page_guard->page());
        // Do not catch Visitor exceptions here. The guard above releases the
        // pin/latch during stack unwinding; catching and retrying would invoke
        // already-visited slots a second time.
        for (size_t i = 0; i < slot_nos.size(); ++i) {
            const int slot_no = slot_nos[i];
            if (!valid_slot(slot_no) || !Bitmap::is_set(page_handle.bitmap, slot_no)) {
                visitor(i, RmTupleMetaProbeState::Absent, nullptr);
                continue;
            }
            visitor(i, RmTupleMetaProbeState::Present, &page_handle.get_meta(slot_no));
        }
    }

    // Access buffer pool manager (for TupleMeta modifications that need explicit pin control)
    BufferPoolManager* get_bpm() {
        return buffer_pool_manager_;
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
                // Page full → hand it back and take a fresh one. Go through
                // create_page_handle(): it is the only routine that guarantees
                // a page with a free slot. Following first_free_page_no
                // directly (as this code used to) can land on an already-full
                // page, and the recomputed slot_no was then not re-checked —
                // writing at get_slot(num_records_per_page), i.e. past the end
                // of the page.
                fh->buffer_pool_manager_->unpin_page(page.page->get_page_id(), true);
                page = fh->create_page_handle();
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
                // Same stale-link hazard as insert_record(): publishing this
                // page's own next pointer as the new list head is only correct
                // when the page happens to be the head. Route through the
                // candidate bookkeeping, which never publishes a stale link and
                // also drops the page from free_page_candidates_.
                fh->remove_free_page_candidate(page.page->get_page_id().page_no, page.page_hdr->next_free_page_no);
            }
            return Rid{page.page->get_page_id().page_no, slot_no};
        }
    };

private:
    RmPageHandle create_page_handle();
    RmPageHandle create_new_page_handle_unlocked();

    void release_page_handle(RmPageHandle& page_handle);
    void ensure_free_space_candidates();
    void add_free_page_candidate(page_id_t page_no);
    void remove_free_page_candidate(page_id_t page_no, page_id_t next_free_page_no);
    std::optional<page_id_t> select_free_page_candidate();
};
