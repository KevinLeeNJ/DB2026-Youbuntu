/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"
#include "transaction/transaction_manager.h"

#include <mutex>
#include <shared_mutex>

namespace {

std::mutex record_write_latch;

enum class VisibilityResult { VISIBLE, DELETED, NEED_OLDER };

VisibilityResult CheckVisibility(const TupleMeta& meta, Transaction* txn) {
    if (txn != nullptr && meta.owner_txn_ == txn->get_transaction_id()) {
        return meta.is_deleted_ ? VisibilityResult::DELETED : VisibilityResult::VISIBLE;
    }
    if (meta.owner_txn_ != INVALID_TXN_ID) {
        return VisibilityResult::NEED_OLDER;
    }
    if (txn == nullptr || meta.ts_ <= txn->get_start_ts()) {
        return meta.is_deleted_ ? VisibilityResult::DELETED : VisibilityResult::VISIBLE;
    }
    return VisibilityResult::NEED_OLDER;
}

std::optional<UndoLog> GetUndoLogFromLink(const UndoLink& link) {
    if (!link.IsValid()) {
        return std::nullopt;
    }
    std::shared_lock<std::shared_mutex> lock(TransactionManager::txn_map_mutex_);
    auto iter = TransactionManager::txn_map.find(link.prev_txn_);
    if (iter == TransactionManager::txn_map.end() || iter->second == nullptr) {
        return std::nullopt;
    }
    return iter->second->GetUndoLog(link.prev_log_idx_);
}

} // namespace

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    RmPageHandle tmp_page_handle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(tmp_page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(tmp_page_handle.page->get_page_id(), false);
        return nullptr;
    }

    Transaction* txn = context == nullptr ? nullptr : context->txn_;
    TupleMeta meta = *tmp_page_handle.get_tuple_meta(rid.slot_no);
    UndoLink undo_link = *tmp_page_handle.get_undo_link(rid.slot_no);
    std::unique_ptr<RmRecord> record_ptr;
    auto visibility = CheckVisibility(meta, txn);
    if (visibility == VisibilityResult::VISIBLE) {
        record_ptr = std::make_unique<RmRecord>(tmp_page_handle.file_hdr->record_size,
                                                tmp_page_handle.get_record_data(rid.slot_no));
    } else if (visibility == VisibilityResult::DELETED) {
        buffer_pool_manager_->unpin_page(tmp_page_handle.page->get_page_id(), false);
        return nullptr;
    }
    buffer_pool_manager_->unpin_page(tmp_page_handle.page->get_page_id(), false);

    while (record_ptr == nullptr && undo_link.IsValid()) {
        auto undo_log = GetUndoLogFromLink(undo_link);
        if (!undo_log.has_value()) {
            break;
        }
        auto undo_visibility = CheckVisibility(undo_log->meta_, txn);
        if (undo_visibility == VisibilityResult::VISIBLE) {
            record_ptr = std::make_unique<RmRecord>(undo_log->tuple_);
            break;
        }
        if (undo_visibility == VisibilityResult::DELETED) {
            break;
        }
        undo_link = undo_log->prev_version_;
    }
    return record_ptr;
}

std::unique_ptr<RmRecord> RmFileHandle::get_latest_record(const Rid& rid) const {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        return nullptr;
    }
    auto record =
        std::make_unique<RmRecord>(page_handle.file_hdr->record_size, page_handle.get_record_data(rid.slot_no));
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    return record;
}

TupleMeta RmFileHandle::get_tuple_meta(const Rid& rid) const {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    TupleMeta meta = *page_handle.get_tuple_meta(rid.slot_no);
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    return meta;
}

UndoLink RmFileHandle::get_undo_link(const Rid& rid) const {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    UndoLink link = *page_handle.get_undo_link(rid.slot_no);
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    return link;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    std::scoped_lock<std::mutex> lock(record_write_latch);
    RmPageHandle insertpage_handle = create_page_handle();
    int slot_no = Bitmap::first_bit(false, insertpage_handle.bitmap, file_hdr_.num_records_per_page);
    if (slot_no != file_hdr_.num_records_per_page) {
        Transaction* txn = context == nullptr ? nullptr : context->txn_;
        TupleMeta meta{txn == nullptr ? 0 : INVALID_TS, false,
                       txn == nullptr ? INVALID_TXN_ID : txn->get_transaction_id()};
        *insertpage_handle.get_tuple_meta(slot_no) = meta;
        *insertpage_handle.get_undo_link(slot_no) = UndoLink{};
        memcpy(insertpage_handle.get_record_data(slot_no), buf, file_hdr_.record_size);
        Bitmap::set(insertpage_handle.bitmap, slot_no);
        insertpage_handle.page_hdr->num_records++;
        if (insertpage_handle.page_hdr->num_records >= file_hdr_.num_records_per_page) {
            file_hdr_.first_free_page_no = insertpage_handle.page_hdr->next_free_page_no;
        }
    }
    buffer_pool_manager_->unpin_page(insertpage_handle.page->get_page_id(), true);
    return Rid{insertpage_handle.page->get_page_id().page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    std::scoped_lock<std::mutex> lock(record_write_latch);
    while (rid.page_no >= file_hdr_.num_pages) {
        auto new_page = create_new_page_handle();
        buffer_pool_manager_->unpin_page(new_page.page->get_page_id(), true);
    }
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    bool was_free = !Bitmap::is_set(pageHandle.bitmap, rid.slot_no);
    if (was_free) {
        Bitmap::set(pageHandle.bitmap, rid.slot_no);
        pageHandle.page_hdr->num_records++;
        if (pageHandle.page_hdr->num_records == file_hdr_.num_records_per_page) {
            file_hdr_.first_free_page_no = pageHandle.page_hdr->next_free_page_no;
        }
    }

    *pageHandle.get_tuple_meta(rid.slot_no) = TupleMeta{0, false, INVALID_TXN_ID};
    *pageHandle.get_undo_link(rid.slot_no) = UndoLink{};
    memcpy(pageHandle.get_record_data(rid.slot_no), buf, file_hdr_.record_size);

    buffer_pool_manager_->unpin_page(pageHandle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    std::scoped_lock<std::mutex> lock(record_write_latch);
    RmPageHandle deletepage_handle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(deletepage_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(deletepage_handle.page->get_page_id(), false);
        return;
    }
    Transaction* txn = context == nullptr ? nullptr : context->txn_;
    if (txn == nullptr) {
        Bitmap::reset(deletepage_handle.bitmap, rid.slot_no);
        deletepage_handle.page_hdr->num_records--;
        if (deletepage_handle.page_hdr->num_records == (file_hdr_.num_records_per_page - 1)) {
            release_page_handle(deletepage_handle);
        }
    } else {
        auto meta = deletepage_handle.get_tuple_meta(rid.slot_no);
        if (meta->owner_txn_ != INVALID_TXN_ID && meta->owner_txn_ != txn->get_transaction_id()) {
            buffer_pool_manager_->unpin_page(deletepage_handle.page->get_page_id(), false);
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WRITE_CONFLICT);
        }
        if (meta->owner_txn_ == INVALID_TXN_ID && meta->ts_ > txn->get_start_ts()) {
            buffer_pool_manager_->unpin_page(deletepage_handle.page->get_page_id(), false);
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WRITE_CONFLICT);
        }
        UndoLog undo_log;
        undo_log.meta_ = *meta;
        undo_log.tuple_ = RmRecord(file_hdr_.record_size, deletepage_handle.get_record_data(rid.slot_no));
        undo_log.prev_version_ = *deletepage_handle.get_undo_link(rid.slot_no);
        *deletepage_handle.get_undo_link(rid.slot_no) = txn->AppendUndoLog(undo_log);
        meta->is_deleted_ = true;
        meta->owner_txn_ = txn->get_transaction_id();
        meta->ts_ = INVALID_TS;
    }
    buffer_pool_manager_->unpin_page(deletepage_handle.page->get_page_id(), true);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    std::scoped_lock<std::mutex> lock(record_write_latch);
    RmPageHandle updatepage_handle = fetch_page_handle(rid.page_no);
    Transaction* txn = context == nullptr ? nullptr : context->txn_;
    if (txn != nullptr) {
        auto meta = updatepage_handle.get_tuple_meta(rid.slot_no);
        if (meta->owner_txn_ != INVALID_TXN_ID && meta->owner_txn_ != txn->get_transaction_id()) {
            buffer_pool_manager_->unpin_page(updatepage_handle.page->get_page_id(), false);
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WRITE_CONFLICT);
        }
        if (meta->owner_txn_ == INVALID_TXN_ID && meta->ts_ > txn->get_start_ts()) {
            buffer_pool_manager_->unpin_page(updatepage_handle.page->get_page_id(), false);
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WRITE_CONFLICT);
        }
        UndoLog undo_log;
        undo_log.meta_ = *meta;
        undo_log.tuple_ = RmRecord(file_hdr_.record_size, updatepage_handle.get_record_data(rid.slot_no));
        undo_log.prev_version_ = *updatepage_handle.get_undo_link(rid.slot_no);
        *updatepage_handle.get_undo_link(rid.slot_no) = txn->AppendUndoLog(undo_log);
        meta->owner_txn_ = txn->get_transaction_id();
        meta->ts_ = INVALID_TS;
        meta->is_deleted_ = false;
    }
    memcpy(updatepage_handle.get_record_data(rid.slot_no), buf, file_hdr_.record_size);
    buffer_pool_manager_->unpin_page(updatepage_handle.page->get_page_id(), true);
}

void RmFileHandle::finalize_record(const Rid& rid, txn_id_t txn_id, timestamp_t commit_ts) {
    std::scoped_lock<std::mutex> lock(record_write_latch);
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        auto meta = page_handle.get_tuple_meta(rid.slot_no);
        if (meta->owner_txn_ == txn_id) {
            meta->owner_txn_ = INVALID_TXN_ID;
            meta->ts_ = commit_ts;
        }
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

void RmFileHandle::finalize_records_owned_by(txn_id_t txn_id, timestamp_t commit_ts) {
    std::scoped_lock<std::mutex> lock(record_write_latch);
    for (int page_no = RM_FIRST_RECORD_PAGE; page_no < file_hdr_.num_pages; ++page_no) {
        RmPageHandle page_handle = fetch_page_handle(page_no);
        bool dirty = false;
        for (int slot_no = 0; slot_no < file_hdr_.num_records_per_page; ++slot_no) {
            if (!Bitmap::is_set(page_handle.bitmap, slot_no)) {
                continue;
            }
            auto meta = page_handle.get_tuple_meta(slot_no);
            if (meta->owner_txn_ == txn_id) {
                meta->owner_txn_ = INVALID_TXN_ID;
                meta->ts_ = commit_ts;
                dirty = true;
            }
        }
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), dirty);
    }
}

void RmFileHandle::rollback_insert(const Rid& rid) {
    std::scoped_lock<std::mutex> lock(record_write_latch);
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        Bitmap::reset(page_handle.bitmap, rid.slot_no);
        page_handle.page_hdr->num_records--;
        if (page_handle.page_hdr->num_records == (file_hdr_.num_records_per_page - 1)) {
            release_page_handle(page_handle);
        }
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

void RmFileHandle::restore_record(const Rid& rid, const UndoLog& undo_log) {
    std::scoped_lock<std::mutex> lock(record_write_latch);
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        Bitmap::set(page_handle.bitmap, rid.slot_no);
        page_handle.page_hdr->num_records++;
    }
    *page_handle.get_tuple_meta(rid.slot_no) = undo_log.meta_;
    *page_handle.get_undo_link(rid.slot_no) = undo_log.prev_version_;
    memcpy(page_handle.get_record_data(rid.slot_no), undo_log.tuple_.data, file_hdr_.record_size);
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
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_
    PageId page_id;
    page_id.fd = fd_;
    Page* newpage = buffer_pool_manager_->new_page(&page_id);
    RmPageHandle newpage_handle = RmPageHandle(&file_hdr_, newpage);
    newpage_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    newpage_handle.page_hdr->num_records = 0;
    file_hdr_.first_free_page_no = newpage->get_page_id().page_no;
    file_hdr_.num_pages++;
    return newpage_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层
    if (file_hdr_.first_free_page_no == -1) {
        return create_new_page_handle();
    }
    return fetch_page_handle(file_hdr_.first_free_page_no);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}
