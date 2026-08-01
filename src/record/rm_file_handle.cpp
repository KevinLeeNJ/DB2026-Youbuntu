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

#include "rm_file_handle.h"

#include <algorithm>

namespace {

class PagePinGuard {
public:
    PagePinGuard(BufferPoolManager* buffer_pool_manager, PageId page_id)
        : buffer_pool_manager_(buffer_pool_manager), page_id_(page_id) {}

    ~PagePinGuard() {
        buffer_pool_manager_->unpin_page(page_id_, false);
    }

    PagePinGuard(const PagePinGuard&) = delete;
    PagePinGuard& operator=(const PagePinGuard&) = delete;

private:
    BufferPoolManager* buffer_pool_manager_;
    PageId page_id_;
};

} // namespace

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    (void)context;
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    RmPageHandle tmp_page_handle = fetch_page_handle(rid.page_no);
    std::unique_ptr<RmRecord> record_ptr;
    {
        std::shared_lock<std::shared_mutex> page_lock(tmp_page_handle.page->latch());
        int size_ = tmp_page_handle.file_hdr->record_size;
        char* data_ = tmp_page_handle.get_slot(rid.slot_no);
        record_ptr = std::make_unique<RmRecord>(size_, data_);
    }
    buffer_pool_manager_->unpin_page(tmp_page_handle.page->get_page_id(), false);
    return record_ptr;
}

RmRecordWithMeta RmFileHandle::get_record_with_meta(const Rid& rid, Context* context) const {
    (void)context;
    RmPageHandle tmp_page_handle = fetch_page_handle(rid.page_no);
    TupleMeta meta;
    std::unique_ptr<RmRecord> record_ptr;
    bool present = false;
    {
        std::shared_lock<std::shared_mutex> page_lock(tmp_page_handle.page->latch());
        present = rid.slot_no >= 0 && rid.slot_no < tmp_page_handle.file_hdr->num_records_per_page &&
                  Bitmap::is_set(tmp_page_handle.bitmap, rid.slot_no);
        if (present) {
            meta = tmp_page_handle.get_meta(rid.slot_no);
            int size_ = tmp_page_handle.file_hdr->record_size;
            char* data_ = tmp_page_handle.get_slot(rid.slot_no);
            record_ptr = std::make_unique<RmRecord>(size_, data_);
        }
    }
    buffer_pool_manager_->unpin_page(tmp_page_handle.page->get_page_id(), false);
    if (!present) {
        return RmRecordWithMeta{TupleMeta{}, nullptr};
    }
    return RmRecordWithMeta{meta, std::move(record_ptr)};
}

RmRecordViewWithMeta RmFileHandle::get_record_view_with_meta(const Rid& rid) const {
    const PageId page_id{fd_, rid.page_no};
    Page* page = buffer_pool_manager_->fetch_page(page_id);
    if (page == nullptr) {
        throw PageNotExistError("record", rid.page_no);
    }

    auto guard = std::make_unique<RmPageReadGuard>(buffer_pool_manager_, page_id, page);
    RmPageHandle page_handle(&file_hdr_, page);
    TupleMeta meta = page_handle.get_meta(rid.slot_no);
    return RmRecordViewWithMeta{
        meta, RmRecordView{page_handle.get_slot(rid.slot_no), static_cast<uint32_t>(file_hdr_.record_size)},
        std::move(guard), nullptr};
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    (void)context;
    auto prepared = prepare_insert_record();
    Rid rid = prepared.rid;
    finish_insert_record(prepared, buf);
    return rid;
}

RmPinnedInsert RmFileHandle::prepare_insert_record() {
    for (;;) {
        RmPageHandle page_handle;
        auto candidate = select_free_page_candidate();
        if (candidate.has_value()) {
            page_handle = fetch_page_handle(*candidate);
        } else {
            // Several inserters may observe an empty candidate list at the
            // same time. Serialize only the extension decision, then
            // re-check the list after acquiring the extension latch so that
            // waiters reuse the page created by the first inserter.
            std::lock_guard<std::mutex> extension_lock(extension_latch_);
            candidate = select_free_page_candidate();
            page_handle = candidate.has_value() ? fetch_page_handle(*candidate) : create_new_page_handle_unlocked();
        }

        auto page_lock =
            std::make_unique<std::unique_lock<std::shared_mutex>>(page_handle.page->latch(), std::try_to_lock);
        if (!page_lock->owns_lock()) {
            const bool can_add_active_page = [&] {
                std::lock_guard<std::mutex> lock(free_space_latch_);
                return free_page_candidates_.size() < 8;
            }();
            buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            if (can_add_active_page) {
                std::lock_guard<std::mutex> extension_lock(extension_latch_);
                page_handle = create_new_page_handle_unlocked();
            } else {
                continue;
            }
            page_lock = std::make_unique<std::unique_lock<std::shared_mutex>>(page_handle.page->latch());
        }
        int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
        if (slot_no == file_hdr_.num_records_per_page) {
            const page_id_t next_free_page_no = page_handle.page_hdr->next_free_page_no;
            page_lock->unlock();
            buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            remove_free_page_candidate(page_handle.page->get_page_id().page_no, next_free_page_no);
            continue;
        }

        Bitmap::set(page_handle.bitmap, slot_no);
        ++page_handle.page_hdr->num_records;
        const bool page_is_full = page_handle.page_hdr->num_records >= file_hdr_.num_records_per_page;
        const page_id_t next_free_page_no = page_handle.page_hdr->next_free_page_no;
        if (page_is_full) {
            remove_free_page_candidate(page_handle.page->get_page_id().page_no, next_free_page_no);
        } else {
            add_free_page_candidate(page_handle.page->get_page_id().page_no);
        }
        return RmPinnedInsert{page_handle, Rid{page_handle.page->get_page_id().page_no, slot_no}, std::move(page_lock),
                              true};
    }
}

void RmFileHandle::finish_insert_record(RmPinnedInsert& insert, char* buf, const TupleMeta* tuple_meta,
                                        lsn_t page_lsn) {
    auto& page_handle = insert.page_handle;
    if (insert.page_lock == nullptr || !insert.page_lock->owns_lock()) {
        throw InternalError("prepared insert page latch is not held");
    }
    const int slot_no = insert.rid.slot_no;
    memcpy(page_handle.get_slot(slot_no), buf, file_hdr_.record_size);
    TupleMeta& meta = page_handle.get_meta(slot_no);
    if (tuple_meta != nullptr) {
        meta = *tuple_meta;
    } else {
        meta.commit_ts_ = 0;
        meta.writer_txn_id_ = INVALID_TXN_ID;
        meta.is_committed_ = true;
        meta.is_deleted_ = false;
        meta.version_chain_head_ = UndoLink{};
    }
    if (page_lsn != INVALID_LSN && page_handle.page->get_page_lsn() < page_lsn) {
        page_handle.page->set_page_lsn(page_lsn);
    }
    insert.page_lock->unlock();
    insert.page_lock.reset();
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    insert.reserved = false;
}

void RmFileHandle::abort_prepared_insert(RmPinnedInsert& insert) {
    if (insert.page_lock != nullptr && insert.page_lock->owns_lock() && insert.reserved) {
        auto& page_handle = insert.page_handle;
        Bitmap::reset(page_handle.bitmap, insert.rid.slot_no);
        --page_handle.page_hdr->num_records;
        add_free_page_candidate(page_handle.page->get_page_id().page_no);
        insert.page_lock->unlock();
        insert.page_lock.reset();
    }
    buffer_pool_manager_->unpin_page(insert.page_handle.page->get_page_id(), false);
    insert.reserved = false;
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf, lsn_t page_lsn, const TupleMeta* tuple_meta) {
    // This overload takes the slot from the caller, and its callers include redo
    // and undo, where the Rid comes straight out of a WAL record — i.e. from
    // unvalidated bytes on disk.  Recovery validates on its side, but fail loudly
    // here as well so *every* caller gets the check: an out-of-range slot_no used
    // to reach Bitmap::set() and memcpy(get_slot(...)) and write past the end of
    // the page, and an absurd page_no turned the extension loop below into an
    // attempt to allocate billions of pages.
    if (rid.slot_no < 0 || rid.slot_no >= file_hdr_.num_records_per_page) {
        throw InternalError("insert_record: slot_no " + std::to_string(rid.slot_no) + " is out of range for table fd " +
                            std::to_string(fd_));
    }
    if (rid.page_no < RM_FIRST_RECORD_PAGE) {
        throw InternalError("insert_record: page_no " + std::to_string(rid.page_no) + " is not a record page");
    }
    // Extending the file one page at a time is fine for the small gaps redo can
    // legitimately produce, but bound it so a corrupt page_no cannot spin here.
    constexpr int kMaxExtensionPages = 1 << 20;
    if (rid.page_no - file_hdr_.num_pages > kMaxExtensionPages) {
        throw InternalError("insert_record: page_no " + std::to_string(rid.page_no) + " is implausibly far past " +
                            std::to_string(file_hdr_.num_pages) + " pages");
    }
    while (rid.page_no >= file_hdr_.num_pages) {
        auto new_page = create_new_page_handle();
        buffer_pool_manager_->unpin_page(new_page.page->get_page_id(), true);
    }
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    bool occupied_a_free_slot = false;
    bool page_became_full = false;
    {
        std::unique_lock<std::shared_mutex> page_lock(pageHandle.page->latch());
        occupied_a_free_slot = !Bitmap::is_set(pageHandle.bitmap, rid.slot_no);
        if (occupied_a_free_slot) {
            Bitmap::set(pageHandle.bitmap, rid.slot_no);
            pageHandle.page_hdr->num_records++;
            page_became_full = pageHandle.page_hdr->num_records >= file_hdr_.num_records_per_page;
        }

        char* slot = pageHandle.get_slot(rid.slot_no);
        memcpy(slot, buf, file_hdr_.record_size);
        TupleMeta& meta = pageHandle.get_meta(rid.slot_no);
        if (tuple_meta != nullptr) {
            meta = *tuple_meta;
        } else {
            // Initialize TupleMeta to safe defaults.
            meta.commit_ts_ = 0;
            meta.writer_txn_id_ = INVALID_TXN_ID;
            meta.is_committed_ = true;
            meta.is_deleted_ = false;
            meta.version_chain_head_ = UndoLink{};
        }
        if (page_lsn != INVALID_LSN && pageHandle.page->get_page_lsn() < page_lsn) {
            pageHandle.page->set_page_lsn(page_lsn);
        }
    }

    // Keep the free-space bookkeeping consistent.  This path is reached by redo
    // *and* by transaction rollback (transaction_manager.cpp re-inserts the old
    // image when undoing a delete), so it runs concurrently under normal load.
    // Publishing this page's own `next_free_page_no` into the file header — as
    // this code used to do — is wrong whenever the page is not the current list
    // head: it drops genuinely free pages and can splice a full page into the
    // chain, which `rmdb_verify` reports as "full page appears in record free
    // list".  remove_free_page_candidate() treats the candidate vector as the
    // serialized source of truth and never publishes a stale link, so route
    // through it instead.  Called after releasing the page latch to match the
    // ordering used by create_page_handle().
    if (page_became_full) {
        remove_free_page_candidate(rid.page_no, pageHandle.page_hdr->next_free_page_no);
    } else if (occupied_a_free_slot) {
        add_free_page_candidate(rid.page_no);
    }

    buffer_pool_manager_->unpin_page(pageHandle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context, lsn_t page_lsn) {
    (void)context;
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    RmPageHandle deletepage_handle = fetch_page_handle(rid.page_no);
    bool deleted = false;
    {
        std::unique_lock<std::shared_mutex> page_lock(deletepage_handle.page->latch());
        if (Bitmap::is_set(deletepage_handle.bitmap, rid.slot_no)) {
            Bitmap::reset(deletepage_handle.bitmap, rid.slot_no);
            deletepage_handle.page_hdr->num_records--;
            if (deletepage_handle.page_hdr->num_records == (file_hdr_.num_records_per_page - 1)) {
                release_page_handle(deletepage_handle);
            }
            if (page_lsn != INVALID_LSN && deletepage_handle.page->get_page_lsn() < page_lsn) {
                deletepage_handle.page->set_page_lsn(page_lsn);
            }
            deleted = true;
        }
    }
    buffer_pool_manager_->unpin_page(deletepage_handle.page->get_page_id(), deleted);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context, lsn_t page_lsn) {
    (void)context;
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录
    RmPageHandle updatepage_handle = fetch_page_handle(rid.page_no);
    {
        std::unique_lock<std::shared_mutex> page_lock(updatepage_handle.page->latch());
        memcpy(updatepage_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
        if (page_lsn != INVALID_LSN && updatepage_handle.page->get_page_lsn() < page_lsn) {
            updatepage_handle.page->set_page_lsn(page_lsn);
        }
    }
    buffer_pool_manager_->unpin_page(updatepage_handle.page->get_page_id(), true);
}

void RmFileHandle::apply_tuple_update(const Rid& rid, const char* buf, const TupleMeta& meta, lsn_t page_lsn) {
    // Also reachable from redo/undo with a WAL-supplied Rid — see insert_record().
    if (rid.slot_no < 0 || rid.slot_no >= file_hdr_.num_records_per_page) {
        throw InternalError("apply_tuple_update: slot_no " + std::to_string(rid.slot_no) +
                            " is out of range for table fd " + std::to_string(fd_));
    }
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    {
        std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
        memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
        page_handle.get_meta(rid.slot_no) = meta;
        if (page_lsn != INVALID_LSN && page_handle.page->get_page_lsn() < page_lsn) {
            page_handle.page->set_page_lsn(page_lsn);
        }
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
 */
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    PageId page_id;
    page_id.fd = fd_;
    page_id.page_no = page_no;
    Page* fetch_page = buffer_pool_manager_->fetch_page(page_id);
    if (fetch_page == nullptr) {
        const std::string temp("temp_table");
        throw PageNotExistError(temp, page_no);
        return RmPageHandle(&file_hdr_, nullptr);
    }
    return RmPageHandle(&file_hdr_, fetch_page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    std::lock_guard<std::mutex> extension_lock(extension_latch_);
    return create_new_page_handle_unlocked();
}

RmPageHandle RmFileHandle::create_new_page_handle_unlocked() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_
    PageId page_id;
    page_id.fd = fd_;
    Page* newpage = buffer_pool_manager_->new_page(&page_id);
    RmPageHandle newpage_handle;
    {
        std::lock_guard<std::mutex> header_lock(file_header_latch_);
        newpage_handle = RmPageHandle(&file_hdr_, newpage);
        newpage_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
        newpage_handle.page_hdr->num_records = 0;
        file_hdr_.first_free_page_no = newpage->get_page_id().page_no;
        file_hdr_.num_pages++;
    }
    add_free_page_candidate(newpage->get_page_id().page_no);
    return newpage_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
/**
 * @description: 取一个**确定有空闲槽位**的数据页（返回时已 pin）。
 *
 * 唯一调用方是批量装载的 PinnedInserter；逐条插入走 prepare_insert_record()。
 *
 * 不能直接信任 file_hdr_.first_free_page_no —— 那个链接可能是陈旧的并指向一个
 * 已满的页，调用方随后会在 get_slot(num_records_per_page) 处越界写出页外。所以
 * 复用 prepare_insert_record() 的候选机制，并在候选已满时把它剔除后重试。
 */
RmPageHandle RmFileHandle::create_page_handle() {
    for (;;) {
        RmPageHandle page_handle;
        auto candidate = select_free_page_candidate();
        if (candidate.has_value()) {
            page_handle = fetch_page_handle(*candidate);
        } else {
            // Serialize only the extension decision, then re-check so that
            // waiters reuse the page created by the first inserter.
            std::lock_guard<std::mutex> extension_lock(extension_latch_);
            candidate = select_free_page_candidate();
            page_handle = candidate.has_value() ? fetch_page_handle(*candidate) : create_new_page_handle_unlocked();
        }
        if (Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page) !=
            file_hdr_.num_records_per_page) {
            return page_handle;
        }
        const page_id_t page_no = page_handle.page->get_page_id().page_no;
        const page_id_t next_free_page_no = page_handle.page_hdr->next_free_page_no;
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        remove_free_page_candidate(page_no, next_free_page_no);
    }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::set_tuple_meta(const Rid& rid, const TupleMeta& meta, lsn_t page_lsn) {
    auto page_handle = fetch_page_handle(rid.page_no);
    {
        std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
        page_handle.get_meta(rid.slot_no) = meta;
        if (page_lsn != INVALID_LSN && page_handle.page->get_page_lsn() < page_lsn) {
            page_handle.page->set_page_lsn(page_lsn);
        }
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

void RmFileHandle::set_page_lsn(const Rid& rid, lsn_t lsn) {
    if (lsn == INVALID_LSN) {
        return;
    }
    auto page_handle = fetch_page_handle(rid.page_no);
    {
        std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
        if (page_handle.page->get_page_lsn() < lsn) {
            page_handle.page->set_page_lsn(lsn);
        }
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

lsn_t RmFileHandle::get_page_lsn(const Rid& rid) const {
    auto page_handle = fetch_page_handle(rid.page_no);
    lsn_t lsn;
    {
        std::shared_lock<std::shared_mutex> page_lock(page_handle.page->latch());
        lsn = page_handle.page->get_page_lsn();
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    return lsn;
}

void RmFileHandle::rebuild_file_header_from_pages() {
    const std::string& path = disk_manager_->get_file_name(fd_);
    int64_t file_size = disk_manager_->get_file_size(path);
    if (file_size < static_cast<int64_t>(sizeof(RmFileHdr))) {
        throw InternalError("record file has an incomplete page during recovery");
    }
    if (file_size > PAGE_SIZE && file_size % PAGE_SIZE != 0) {
        throw InternalError("record file has an incomplete page during recovery");
    }

    // The file header is intentionally a short write at page 0. Pages created
    // by redo may still exist only in the buffer pool, so retain the current
    // in-memory allocation count and use the disk size as a lower bound.
    int disk_page_count = file_size <= PAGE_SIZE ? 1 : static_cast<int>(file_size / PAGE_SIZE);
    int page_upper_bound = std::max(file_hdr_.num_pages, disk_page_count);
    file_hdr_.num_pages = page_upper_bound;
    file_hdr_.first_free_page_no = RM_NO_PAGE;

    std::vector<int> free_pages;
    free_pages.reserve(static_cast<size_t>(page_upper_bound));
    int actual_page_count = RM_FIRST_RECORD_PAGE;
    for (int page_no = RM_FIRST_RECORD_PAGE; page_no < page_upper_bound; ++page_no) {
        PageId page_id{fd_, page_no};
        if (page_no >= disk_page_count && !buffer_pool_manager_->is_page_resident(page_id)) {
            // A stale header can mention a page that never reached disk. A
            // redo-created page is still discoverable here through the BPM.
            break;
        }
        auto page_handle = fetch_page_handle(page_no);
        actual_page_count = page_no + 1;
        int num_records = 0;
        {
            std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
            for (int slot_no = 0; slot_no < file_hdr_.num_records_per_page; ++slot_no) {
                if (Bitmap::is_set(page_handle.bitmap, slot_no)) {
                    ++num_records;
                }
            }
            if (page_handle.page_hdr->num_records != num_records) {
                page_handle.page_hdr->num_records = num_records;
            }
            if (num_records == file_hdr_.num_records_per_page) {
                page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
            }
        }
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
        if (num_records < file_hdr_.num_records_per_page) {
            free_pages.push_back(page_no);
        }
    }

    for (size_t i = 0; i < free_pages.size(); ++i) {
        int page_no = free_pages[i];
        int next_page_no = i + 1 < free_pages.size() ? free_pages[i + 1] : RM_NO_PAGE;
        auto page_handle = fetch_page_handle(page_no);
        {
            std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
            page_handle.page_hdr->next_free_page_no = next_page_no;
        }
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    }
    if (!free_pages.empty()) {
        file_hdr_.first_free_page_no = free_pages.front();
    }
    file_hdr_.num_pages = actual_page_count;
    disk_manager_->set_fd2pageno(fd_, file_hdr_.num_pages);

    {
        std::scoped_lock lock(free_space_latch_, file_header_latch_);
        free_page_candidates_.clear();
        free_page_candidate_set_.clear();
        free_page_cursor_ = 0;
        for (page_id_t page_no : free_pages) {
            free_page_candidates_.push_back(page_no);
            free_page_candidate_set_.insert(page_no);
        }
    }
}

void RmFileHandle::repair_file_header_for_pages(const std::vector<page_id_t>& page_nos) {
    if (page_nos.empty()) {
        return;
    }

    std::vector<page_id_t> pages = page_nos;
    std::sort(pages.begin(), pages.end());
    pages.erase(std::unique(pages.begin(), pages.end()), pages.end());
    if (pages.front() < RM_FIRST_RECORD_PAGE) {
        throw InternalError("recovery referenced an invalid record page");
    }

    const std::string& path = disk_manager_->get_file_name(fd_);
    const int64_t file_size = disk_manager_->get_file_size(path);
    if (file_size < static_cast<int64_t>(sizeof(RmFileHdr)) || (file_size > PAGE_SIZE && file_size % PAGE_SIZE != 0)) {
        throw InternalError("record file has an incomplete page during recovery");
    }
    const int disk_page_count = file_size <= PAGE_SIZE ? 1 : static_cast<int>(file_size / PAGE_SIZE);

    // A newly allocated page may only exist in the buffer pool when the
    // process crashed. Pages that are in neither place are intentionally left
    // for redo/undo's existing exact-RID allocation path; this is the normal
    // case for a committed insert whose page was never created before the
    // crash.
    std::vector<page_id_t> existing_pages;
    existing_pages.reserve(pages.size());
    for (const page_id_t page_no : pages) {
        const PageId page_id{fd_, page_no};
        if (page_no < disk_page_count || buffer_pool_manager_->is_page_resident(page_id)) {
            existing_pages.push_back(page_no);
        }
    }

    if (!existing_pages.empty()) {
        std::lock_guard<std::mutex> header_lock(file_header_latch_);
        file_hdr_.num_pages = std::max(file_hdr_.num_pages, existing_pages.back() + 1);
        disk_manager_->set_fd2pageno(fd_, file_hdr_.num_pages);
    }

    // The bitmap is authoritative for a touched page. Repair its count and
    // update only the in-memory/persisted free-page chain entry for that page.
    // Untouched pages retain the existing chain, so the cost is proportional to
    // the pages named by the WAL rather than to the table size.
    for (const page_id_t page_no : existing_pages) {
        auto page_handle = fetch_page_handle(page_no);
        {
            std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
            int num_records = 0;
            for (int slot_no = 0; slot_no < file_hdr_.num_records_per_page; ++slot_no) {
                if (Bitmap::is_set(page_handle.bitmap, slot_no)) {
                    ++num_records;
                }
            }
            page_handle.page_hdr->num_records = num_records;

            std::scoped_lock metadata_lock(free_space_latch_, file_header_latch_);
            const bool page_is_full = num_records >= file_hdr_.num_records_per_page;
            if (page_is_full) {
                const bool was_candidate = free_page_candidate_set_.erase(page_no) != 0;
                if (was_candidate) {
                    free_page_candidates_.erase(
                        std::remove(free_page_candidates_.begin(), free_page_candidates_.end(), page_no),
                        free_page_candidates_.end());
                    if (free_page_cursor_ >= free_page_candidates_.size()) {
                        free_page_cursor_ = 0;
                    }
                }
                if (file_hdr_.first_free_page_no == page_no) {
                    file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
                }
                page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
                if (file_hdr_.first_free_page_no == page_no) {
                    file_hdr_.first_free_page_no = RM_NO_PAGE;
                }
            } else if (free_page_candidate_set_.insert(page_no).second) {
                free_page_candidates_.push_back(page_no);
                page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
                file_hdr_.first_free_page_no = page_no;
            } else if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
                // The candidate cache can be populated before the header is
                // repaired (for example after reopening a database).
                page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
                file_hdr_.first_free_page_no = page_no;
            }
        }
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    }
}

TupleMeta RmFileHandle::get_tuple_meta(const Rid& rid) const {
    auto page_handle = fetch_page_handle(rid.page_no);
    TupleMeta meta;
    {
        std::shared_lock<std::shared_mutex> page_lock(page_handle.page->latch());
        meta = page_handle.get_meta(rid.slot_no);
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    return meta;
}

RmTupleMetaProbe RmFileHandle::probe_tuple_meta(const Rid& rid) const {
    const RmFileHdr file_hdr = get_file_hdr();
    if (rid.page_no < RM_FIRST_RECORD_PAGE || rid.page_no >= file_hdr.num_pages || rid.slot_no < 0 ||
        rid.slot_no >= file_hdr.num_records_per_page) {
        return {};
    }

    const PageId page_id{fd_, rid.page_no};
    try {
        Page* page = buffer_pool_manager_->fetch_page(page_id);
        if (page == nullptr) {
            return RmTupleMetaProbe{RmTupleMetaProbeState::Retry, {}};
        }
        PagePinGuard pin_guard(buffer_pool_manager_, page_id);
        RmPageHandle page_handle(&file_hdr, page);
        std::shared_lock<std::shared_mutex> page_lock(page->latch());
        if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
            return {};
        }
        return RmTupleMetaProbe{RmTupleMetaProbeState::Present, page_handle.get_meta(rid.slot_no)};
    } catch (...) {
        return RmTupleMetaProbe{RmTupleMetaProbeState::Retry, {}};
    }
}

std::vector<RmTupleMetaProbe> RmFileHandle::probe_tuple_meta_batch(page_id_t page_no,
                                                                   const std::vector<int>& slot_nos) const {
    std::vector<RmTupleMetaProbe> probes(slot_nos.size());
    visit_tuple_meta_batch(page_no, slot_nos, [&](size_t index, RmTupleMetaProbeState state, const TupleMeta* meta) {
        probes[index].state = state;
        if (meta != nullptr) {
            probes[index].meta = *meta;
        }
    });
    return probes;
}

void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    std::scoped_lock lock(free_space_latch_, file_header_latch_);
    const page_id_t page_no = page_handle.page->get_page_id().page_no;
    if (free_page_candidate_set_.insert(page_no).second) {
        free_page_candidates_.push_back(page_no);
        page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
        file_hdr_.first_free_page_no = page_no;
    }
}

void RmFileHandle::add_free_page_candidate(page_id_t page_no) {
    if (page_no == RM_NO_PAGE) {
        return;
    }
    std::scoped_lock lock(free_space_latch_, file_header_latch_);
    if (free_page_candidate_set_.insert(page_no).second) {
        free_page_candidates_.push_back(page_no);
        if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
            file_hdr_.first_free_page_no = page_no;
        }
    }
}

void RmFileHandle::remove_free_page_candidate(page_id_t page_no, page_id_t next_free_page_no) {
    (void)next_free_page_no;
    std::scoped_lock lock(free_space_latch_, file_header_latch_);
    free_page_candidate_set_.erase(page_no);
    if (free_page_cursor_ >= free_page_candidates_.size()) {
        free_page_cursor_ = 0;
    }
    free_page_candidates_.erase(std::remove(free_page_candidates_.begin(), free_page_candidates_.end(), page_no),
                                free_page_candidates_.end());
    // The caller's next pointer can be stale: another inserter/rollback may
    // have changed the free-page chain while this page was pinned. The
    // candidate vector is the serialized source of truth, so never publish a
    // stale full-page link into the file header.
    if (file_hdr_.first_free_page_no == page_no || free_page_candidates_.empty()) {
        file_hdr_.first_free_page_no = free_page_candidates_.empty() ? RM_NO_PAGE : free_page_candidates_.front();
    }
}

std::optional<page_id_t> RmFileHandle::select_free_page_candidate() {
    std::scoped_lock lock(free_space_latch_, file_header_latch_);
    if (free_page_candidates_.empty() && file_hdr_.first_free_page_no != RM_NO_PAGE) {
        free_page_candidates_.push_back(file_hdr_.first_free_page_no);
        free_page_candidate_set_.insert(file_hdr_.first_free_page_no);
    }
    if (free_page_candidates_.empty()) {
        return std::nullopt;
    }
    if (free_page_cursor_ >= free_page_candidates_.size()) {
        free_page_cursor_ = 0;
    }
    return free_page_candidates_[free_page_cursor_++];
}
