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
    int size_ = tmp_page_handle.file_hdr->record_size;
    char* data_ = tmp_page_handle.get_slot(rid.slot_no);
    std::unique_ptr<RmRecord> record_ptr(new RmRecord(size_, data_));
    buffer_pool_manager_->unpin_page(tmp_page_handle.page->get_page_id(), false);
    return record_ptr;
}

RmRecordWithMeta RmFileHandle::get_record_with_meta(const Rid& rid, Context* context) const {
    (void)context;
    RmPageHandle tmp_page_handle = fetch_page_handle(rid.page_no);
    TupleMeta meta = tmp_page_handle.get_meta(rid.slot_no);
    int size_ = tmp_page_handle.file_hdr->record_size;
    char* data_ = tmp_page_handle.get_slot(rid.slot_no);
    std::unique_ptr<RmRecord> record_ptr(new RmRecord(size_, data_));
    buffer_pool_manager_->unpin_page(tmp_page_handle.page->get_page_id(), false);
    return RmRecordWithMeta{meta, std::move(record_ptr)};
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
    RmPageHandle insertpage_handle = create_page_handle();
    int slot_no = Bitmap::first_bit(false, insertpage_handle.bitmap, file_hdr_.num_records_per_page);
    if (slot_no != file_hdr_.num_records_per_page) {
        memcpy(insertpage_handle.get_slot(slot_no), buf, file_hdr_.record_size);
        // Initialize TupleMeta to safe defaults (committed, no specific writer)
        TupleMeta& meta = insertpage_handle.get_meta(slot_no);
        meta.commit_ts_ = 0;
        meta.writer_txn_id_ = INVALID_TXN_ID;
        meta.is_committed_ = true;
        meta.is_deleted_ = false;
        meta.version_chain_head_ = UndoLink{};
        Bitmap::set(insertpage_handle.bitmap, slot_no);
        insertpage_handle.page_hdr->num_records++;
        if (insertpage_handle.page_hdr->num_records >= file_hdr_.num_records_per_page) {
            file_hdr_.first_free_page_no = insertpage_handle.page_hdr->next_free_page_no;
        }
        buffer_pool_manager_->unpin_page(insertpage_handle.page->get_page_id(), true);
        return Rid{insertpage_handle.page->get_page_id().page_no, slot_no};
    }
    // Defensive: page should have free space, but if not, create new page and retry
    buffer_pool_manager_->unpin_page(insertpage_handle.page->get_page_id(), true);
    auto new_page = create_new_page_handle();
    slot_no = Bitmap::first_bit(false, new_page.bitmap, file_hdr_.num_records_per_page);
    assert(slot_no != file_hdr_.num_records_per_page);
    memcpy(new_page.get_slot(slot_no), buf, file_hdr_.record_size);
    TupleMeta& meta = new_page.get_meta(slot_no);
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = INVALID_TXN_ID;
    meta.is_committed_ = true;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};
    Bitmap::set(new_page.bitmap, slot_no);
    new_page.page_hdr->num_records++;
    buffer_pool_manager_->unpin_page(new_page.page->get_page_id(), true);
    return Rid{new_page.page->get_page_id().page_no, slot_no};
}

RmPinnedInsert RmFileHandle::prepare_insert_record() {
    RmPageHandle page_handle = create_page_handle();
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
    if (slot_no == file_hdr_.num_records_per_page) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        page_handle = create_new_page_handle();
        slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
        assert(slot_no != file_hdr_.num_records_per_page);
    }
    return RmPinnedInsert{page_handle, Rid{page_handle.page->get_page_id().page_no, slot_no}};
}

void RmFileHandle::finish_insert_record(RmPinnedInsert& insert, char* buf) {
    auto& page_handle = insert.page_handle;
    const int slot_no = insert.rid.slot_no;
    memcpy(page_handle.get_slot(slot_no), buf, file_hdr_.record_size);
    TupleMeta& meta = page_handle.get_meta(slot_no);
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = INVALID_TXN_ID;
    meta.is_committed_ = true;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};
    Bitmap::set(page_handle.bitmap, slot_no);
    page_handle.page_hdr->num_records++;
    if (page_handle.page_hdr->num_records >= file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

void RmFileHandle::abort_prepared_insert(RmPinnedInsert& insert) {
    buffer_pool_manager_->unpin_page(insert.page_handle.page->get_page_id(), false);
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
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

    char* slot = pageHandle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    // Initialize TupleMeta to safe defaults
    TupleMeta& meta = pageHandle.get_meta(rid.slot_no);
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = INVALID_TXN_ID;
    meta.is_committed_ = true;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};

    buffer_pool_manager_->unpin_page(pageHandle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    RmPageHandle deletepage_handle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(deletepage_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(deletepage_handle.page->get_page_id(), false);
        return;
    }
    Bitmap::reset(deletepage_handle.bitmap, rid.slot_no);
    deletepage_handle.page_hdr->num_records--;
    if (deletepage_handle.page_hdr->num_records == (file_hdr_.num_records_per_page - 1)) {
        release_page_handle(deletepage_handle);
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
    (void)context;
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录
    RmPageHandle updatepage_handle = fetch_page_handle(rid.page_no);
    memcpy(updatepage_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
    buffer_pool_manager_->unpin_page(updatepage_handle.page->get_page_id(), true);
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
void RmFileHandle::set_tuple_meta(const Rid& rid, const TupleMeta& meta) {
    auto page_handle = fetch_page_handle(rid.page_no);
    page_handle.get_meta(rid.slot_no) = meta;
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

TupleMeta RmFileHandle::get_tuple_meta(const Rid& rid) const {
    auto page_handle = fetch_page_handle(rid.page_no);
    TupleMeta meta = page_handle.get_meta(rid.slot_no);
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    return meta;
}

void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}
