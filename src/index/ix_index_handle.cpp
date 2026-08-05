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

#include "ix_index_handle.h"

#include "minilog.h"

#include <algorithm>
#include <array>
#include <functional>

#include "ix_scan.h"
#include "ix_smo_image.h"

std::atomic<IxIndexHandle::InsertSplitFault> IxIndexHandle::insert_split_fault_{InsertSplitFault::None};
std::mutex IxIndexHandle::insert_split_test_hook_latch_;
IxIndexHandle::InsertSplitTestHook IxIndexHandle::insert_split_test_hook_;

void IxIndexHandle::maybe_inject_insert_split_fault(InsertSplitFault fault) {
    InsertSplitFault expected = fault;
    if (insert_split_fault_.compare_exchange_strong(expected, InsertSplitFault::None, std::memory_order_relaxed)) {
        throw InternalError("injected index split preflight failure");
    }
}

void IxIndexHandle::set_insert_split_test_hook(InsertSplitTestHook hook) {
    std::scoped_lock lock{insert_split_test_hook_latch_};
    insert_split_test_hook_ = std::move(hook);
}

void IxIndexHandle::run_insert_split_test_hook(InsertSplitStage stage, PageId page_id, Page* page) {
    InsertSplitTestHook hook;
    {
        std::scoped_lock lock{insert_split_test_hook_latch_};
        hook = insert_split_test_hook_;
    }
    if (hook) {
        hook(stage, page_id, page);
    }
}

// Capture the complete after-image under the index structure latch. Runtime
// mutations append a redo-only INDEX_SMO record and attach its LSN to every
// dirty page and the index header before releasing the buffer pool's per-file
// write barrier. Eviction, checkpoint and close then enforce WAL-before-data.
// Origins without runtime WAL retain immediate pages-before-header publication.
void IxIndexHandle::publish_smo_pages_impl(bool* wal_barrier_released) const {
    if (!smo_structural_ || smo_pages_.empty()) {
        return;
    }
    std::sort(smo_pages_.begin(), smo_pages_.end());
    smo_pages_.erase(std::unique(smo_pages_.begin(), smo_pages_.end()), smo_pages_.end());

    smo_flush_batch_.clear();
    smo_flush_batch_.reserve(smo_pages_.size());
    for (const page_id_t page_no : smo_pages_) {
        if (page_no != IX_NO_PAGE) {
            smo_flush_batch_.push_back(PageId{fd_, page_no});
        }
    }
    if (wal_barrier_released != nullptr) {
        IndexSmoWalData data;
        data.index_file_name = disk_manager_->get_file_name(fd_);
        data.pages.reserve(smo_flush_batch_.size());
        for (const PageId& page_id : smo_flush_batch_) {
            Page* page = buffer_pool_manager_->fetch_page(page_id);
            if (page == nullptr) {
                throw InternalError("INDEX_SMO could not capture a resident page image");
            }
            IndexSmoPageImage image;
            image.page_no = page_id.page_no;
            {
                std::shared_lock page_lock{page->latch()};
                std::memcpy(image.bytes.data(), page->get_data(), PAGE_SIZE);
            }
            // Erased/moved slots retain arbitrary old bytes in the live page.
            // They are outside the B+tree's logical contents, so canonicalize
            // only the WAL copy to give the existing zero-run codec long,
            // deterministic runs. Unknown layouts deliberately stay raw.
            TryCanonicalizeIxPageImageForWal(*file_hdr_, page_id.page_no, &image.bytes);
            data.pages.push_back(std::move(image));
            unpin_if_not_cached(page_id);
        }
        data.header.fill(0);
        file_hdr_->serialize(data.header.data());
        const lsn_t smo_lsn = buffer_pool_manager_->append_index_smo(data);
        const PageWriteDependency dependency = PageWriteDependency::Wal(smo_lsn);
        for (const PageId& page_id : smo_flush_batch_) {
            Page* page = buffer_pool_manager_->fetch_page(page_id);
            if (page == nullptr) {
                throw InternalError("INDEX_SMO could not install page WAL dependency");
            }
            BufferPoolManager::mark_dirty(page, dependency);
            unpin_if_not_cached(page_id);
        }
        header_dependency_.merge(dependency);
        buffer_pool_manager_->end_index_smo(fd_);
        *wal_barrier_released = true;
        return;
    }

    const auto flushed = buffer_pool_manager_->flush_pages(smo_flush_batch_, FlushDependencyPolicy::Enforce());
    if (!flushed.success) {
        throw InternalError("index SMO page publication failed");
    }
    write_index_header_page();
}

void IxIndexHandle::write_index_header_page() const {
    buffer_pool_manager_->begin_index_file_write(fd_);
    try {
        const uint64_t dependency_epoch = header_dirty_epoch_;
        const PageWriteDependency dependency = header_dependency_;
        if (header_image_.size() != static_cast<size_t>(file_hdr_->tot_len_)) {
            header_image_.assign(static_cast<size_t>(file_hdr_->tot_len_), 0);
        }
        file_hdr_->serialize(header_image_.data());
        buffer_pool_manager_->ensure_write_dependency(dependency);
        disk_manager_->write_page(fd_, IX_FILE_HDR_PAGE, header_image_.data(), file_hdr_->tot_len_);
        if (header_dirty_epoch_ == dependency_epoch) {
            header_dependency_ = PageWriteDependency::None();
        }
    } catch (...) {
        buffer_pool_manager_->end_index_file_write(fd_);
        throw;
    }
    buffer_pool_manager_->end_index_file_write(fd_);
}

IxIndexHeaderSnapshot IxIndexHandle::capture_index_header_snapshot() const {
    auto index_guard = lock_shared();
    if (file_hdr_->tot_len_ <= 0 || file_hdr_->tot_len_ > PAGE_SIZE) {
        throw InternalError("invalid index header length during fixed checkpoint");
    }
    IxIndexHeaderSnapshot snapshot;
    snapshot.fd = fd_;
    snapshot.bytes.assign(static_cast<size_t>(file_hdr_->tot_len_), 0);
    file_hdr_->serialize(snapshot.bytes.data());
    snapshot.dependency = header_dependency_;
    return snapshot;
}

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char* target) const {
    int left = 0;
    int right = page_hdr->num_key;
    while (left < right) {
        int mid = left + (right - left) / 2;
        if (ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) < 0) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char* target) const {
    int left = 0;
    int right = page_hdr->num_key;
    while (left < right) {
        int mid = left + (right - left) / 2;
        if (ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) <= 0) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::leaf_lookup(const char* key, Rid** value) {
    int pos = lower_bound(key);
    if (pos < get_size() && ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        *value = get_rid(pos);
        return true;
    }
    return false;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::internal_lookup(const char* key) {
    int child_idx = upper_bound(key) - 1;
    if (child_idx < 0) {
        child_idx = 0;
    }
    return value_at(child_idx);
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, const char* key, const Rid* rid, int n) {
    assert(pos >= 0 && pos <= page_hdr->num_key);
    assert(n >= 0 && page_hdr->num_key + n <= get_max_size());
    if (n == 0) {
        return;
    }
    int move_count = page_hdr->num_key - pos;
    memmove(get_key(pos + n), get_key(pos), move_count * file_hdr->col_tot_len_);
    memmove(get_rid(pos + n), get_rid(pos), move_count * sizeof(Rid));
    memcpy(get_key(pos), key, n * file_hdr->col_tot_len_);
    memcpy(get_rid(pos), rid, n * sizeof(Rid));
    page_hdr->num_key += n;
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxNodeHandle::insert(const char* key, const Rid& value) {
    int pos = lower_bound(key);
    if (pos < get_size() && ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        return get_size();
    }
    insert_pair(pos, key, value);
    return get_size();
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    assert(pos >= 0 && pos < page_hdr->num_key);
    int move_count = page_hdr->num_key - pos - 1;
    memmove(get_key(pos), get_key(pos + 1), move_count * file_hdr->col_tot_len_);
    memmove(get_rid(pos), get_rid(pos + 1), move_count * sizeof(Rid));
    page_hdr->num_key--;
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 */
int IxNodeHandle::remove(const char* key) {
    int pos = lower_bound(key);
    if (pos < get_size() && ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        erase_pair(pos);
    }
    return get_size();
}

IxIndexHandle::IxIndexHandle(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    std::vector<char> buf(PAGE_SIZE, 0);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf.data(), PAGE_SIZE);
    file_hdr_ = std::make_unique<IxFileHdr>();
    file_hdr_->deserialize(buf.data());

    // fd2pageno_ is process-local state and is reset when the database is
    // reopened. Reconstruct the allocation cursor from the persisted index
    // header instead of incrementing an unrelated value left on this fd.
    //
    // num_pages_ lives in the index header and create_node() bumps it without
    // logging. A stale or torn header can therefore name fewer pages than the
    // file actually holds, and trusting it would make create_node() hand out
    // page numbers that live nodes already occupy - recovery would overwrite a
    // live subtree with a fresh node. The file length is a safe upper bound: a
    // page number that has ever been written is inside the file, so starting
    // after the last written page can never reuse one.
    const int64_t file_size = disk_manager_->get_file_size(disk_manager_->get_file_name(fd));
    if (file_size > 0) {
        const auto pages_on_disk = static_cast<page_id_t>((file_size + PAGE_SIZE - 1) / PAGE_SIZE);
        // Keep num_pages_ equal to the allocation cursor: the page-number range
        // checks in validate_structure() and elsewhere read it as "pages below
        // this may exist", and pages above the old value are simply unreachable.
        file_hdr_->num_pages_ = std::max(file_hdr_->num_pages_, pages_on_disk);
    }
    disk_manager_->set_fd2pageno(fd, file_hdr_->num_pages_);
    // SmManager opens catalog handles before WAL recovery. Do not fetch any
    // index page here: after a crash, the persisted header/tree may be
    // temporarily inconsistent until recovery rebuilds the index.
}
IxIndexHandle::~IxIndexHandle() {
    release_root_page_cache();
}

void IxIndexHandle::refresh_page_residency(bool include_internal) {
    auto structure_guard = lock_exclusive();
    refresh_root_page_cache();
    if (include_internal) {
        register_internal_pages();
    }
}

void IxIndexHandle::prepare_for_smo_redo() {
    release_root_page_cache();
    unregister_internal_pages();
    buffer_pool_manager_->delete_all_pages(fd_);
}

void IxIndexHandle::install_recovered_smo_header(const char* header_image) {
    file_hdr_->col_types_.clear();
    file_hdr_->col_lens_.clear();
    file_hdr_->deserialize(const_cast<char*>(header_image));
    disk_manager_->set_fd2pageno(fd_, file_hdr_->num_pages_);
}
void IxIndexHandle::refresh_root_page_cache(const BufferPoolManager::FrameOperationToken* frame_operation) noexcept {
    if (cached_root_page_ == nullptr || cached_root_page_no_ != file_hdr_->root_page_) {
        const PageId new_root_id{fd_, file_hdr_->root_page_};
        Page* old_root = cached_root_page_;
        PageId old_root_id{fd_, cached_root_page_no_};
        cached_root_page_ = nullptr;
        cached_root_page_no_ = IX_NO_PAGE;

        if (old_root != nullptr && old_root_id.page_no != IX_NO_PAGE) {
            if (resident_internal_pages_.erase(old_root_id.page_no) != 0) {
                buffer_pool_manager_->unmark_resident(old_root_id);
            }
            drop_cached_internal_page(old_root_id.page_no);
            buffer_pool_manager_->unpin_page(old_root_id, false);
        }

        Page* new_root = nullptr;
        bool marked_resident = false;
        try {
            run_insert_split_test_hook(InsertSplitStage::RootCacheRefresh);
            new_root = frame_operation == nullptr ? buffer_pool_manager_->fetch_page(new_root_id)
                                                  : frame_operation->fetch_page(new_root_id);
            if (new_root == nullptr) {
                if (resident_internal_pages_.erase(new_root_id.page_no) != 0) {
                    buffer_pool_manager_->unmark_resident(new_root_id);
                }
                drop_cached_internal_page(new_root_id.page_no);
                return;
            }

            // A height reduction promotes an internal child that may already
            // own a cache pin. The fetch above supplies the new root pin;
            // release the old internal-cache pin before installing it as root
            // so ownership remains exactly one pin.
            drop_cached_internal_page(new_root_id.page_no);
            marked_resident = buffer_pool_manager_->try_mark_resident(new_root_id, ResidencyClass::IndexInternal);
            if (!marked_resident) {
                resident_internal_pages_.erase(new_root_id.page_no);
                buffer_pool_manager_->unpin_page(new_root_id, false);
                return;
            }
            resident_internal_pages_.insert(new_root_id.page_no);
            cached_root_page_ = new_root;
            cached_root_page_no_ = new_root_id.page_no;
        } catch (...) {
            const bool was_resident = resident_internal_pages_.erase(new_root_id.page_no) != 0;
            drop_cached_internal_page(new_root_id.page_no);
            if (marked_resident || was_resident) {
                buffer_pool_manager_->unmark_resident(new_root_id);
            }
            if (new_root != nullptr) {
                buffer_pool_manager_->unpin_page(new_root_id, false);
            }
        }
    }
}

bool IxIndexHandle::mark_internal_page_resident(page_id_t page_no, Page* page,
                                                const BufferPoolManager::FrameOperationToken* frame_operation) {
    if (resident_internal_pages_.find(page_no) != resident_internal_pages_.end()) {
        return cache_internal_page(page_no, page, frame_operation);
    }
    const PageId page_id{fd_, page_no};
    if (!buffer_pool_manager_->try_mark_resident(page_id, ResidencyClass::IndexInternal)) {
        return false;
    }
    resident_internal_pages_.insert(page_no);
    if (!cache_internal_page(page_no, page, frame_operation)) {
        resident_internal_pages_.erase(page_no);
        buffer_pool_manager_->unmark_resident(page_id);
        return false;
    }
    return true;
}

/**
 * @brief Remember a raw Page* for an internal page and take the buffer-pool pin
 * that keeps it valid.
 *
 * Keeping the frame out of the replacer is a placement policy, not ownership: it
 * says "prefer not to evict this" and BufferPoolManager::clear_residency() may
 * revoke it without telling us. A real pin is the ownership primitive - the same
 * split InnoDB draws between its LRU policy and a block's fix count - so every
 * entry in cached_internal_pages_ holds exactly one pin, released by
 * drop_cached_internal_page().
 */
bool IxIndexHandle::cache_internal_page(page_id_t page_no, Page* page,
                                        const BufferPoolManager::FrameOperationToken* frame_operation) {
    if (page_no == file_hdr_->root_page_ || page_no == IX_NO_PAGE) {
        return true;
    }
    auto [it, inserted] = cached_internal_pages_.try_emplace(page_no, page);
    if (!inserted) {
        // The pin for this page is already held; only the frame may have moved.
        it->second = page;
        return true;
    }
    Page* pinned = frame_operation == nullptr ? buffer_pool_manager_->fetch_page(PageId{fd_, page_no})
                                              : frame_operation->fetch_page(PageId{fd_, page_no});
    if (pinned == nullptr) {
        cached_internal_pages_.erase(it);
        return false;
    }
    it->second = pinned;
    return true;
}

void IxIndexHandle::drop_cached_internal_page(page_id_t page_no) const {
    if (cached_internal_pages_.erase(page_no) == 0) {
        return;
    }
    buffer_pool_manager_->unpin_page(PageId{fd_, page_no}, false);
}

void IxIndexHandle::release_root_page_cache() const {
    auto structure_guard = lock_exclusive();
    unregister_internal_pages();
}

void IxIndexHandle::register_internal_pages() {
    // Recovery can rewrite a page in place, replace the root, or rebuild the
    // whole index. Drop non-root raw pointers before walking the repaired tree,
    // but preserve a valid root pin together with its BPM quota lease.
    unregister_internal_pages(/*preserve_root_cache=*/true);

    std::unordered_set<page_id_t> reachable;
    std::vector<page_id_t> pending;
    std::unordered_set<page_id_t> visited;
    reachable.reserve(static_cast<size_t>(std::max(file_hdr_->num_pages_, 1)));
    visited.reserve(reachable.size());
    pending.reserve(reachable.size());
    if (file_hdr_->root_page_ != IX_NO_PAGE) {
        pending.push_back(file_hdr_->root_page_);
    }
    if (cached_root_page_ != nullptr && cached_root_page_no_ == file_hdr_->root_page_ &&
        resident_internal_pages_.find(cached_root_page_no_) != resident_internal_pages_.end()) {
        // Root residency is independent of whether the current root is a leaf.
        reachable.insert(cached_root_page_no_);
    }

    try {
        while (!pending.empty()) {
            const page_id_t page_no = pending.back();
            pending.pop_back();
            if (!visited.insert(page_no).second) {
                continue;
            }

            const bool cached_root = cached_root_page_ != nullptr && page_no == cached_root_page_no_;
            Page* page = cached_root ? cached_root_page_ : buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
            if (page == nullptr) {
                throw InternalError("failed to fetch index page while registering residency");
            }
            IxNodeHandle node(file_hdr_.get(), page);
            if (!node.is_leaf_page()) {
                if (mark_internal_page_resident(page_no, page)) {
                    reachable.insert(page_no);
                }
                for (int child_idx = 0; child_idx < node.get_size(); ++child_idx) {
                    pending.push_back(node.value_at(child_idx));
                }
            }
            if (!cached_root) {
                buffer_pool_manager_->unpin_page(PageId{fd_, page_no}, false);
            }
        }
    } catch (...) {
        unregister_internal_pages(/*preserve_root_cache=*/true);
        throw;
    }

    resident_internal_pages_ = std::move(reachable);
}

void IxIndexHandle::unregister_internal_pages(bool preserve_root_cache) const {
    bool keep_root =
        preserve_root_cache && cached_root_page_ != nullptr && cached_root_page_no_ != IX_NO_PAGE &&
        frame_holds(cached_root_page_, cached_root_page_no_) &&
        resident_internal_pages_.find(cached_root_page_no_) != resident_internal_pages_.end() &&
        buffer_pool_manager_->get_residency_class(PageId{fd_, cached_root_page_no_}) == ResidencyClass::IndexInternal;
    for (const page_id_t page_no : resident_internal_pages_) {
        if (keep_root && page_no == cached_root_page_no_) {
            continue;
        }
        buffer_pool_manager_->unmark_resident(PageId{fd_, page_no});
    }
    resident_internal_pages_.clear();
    if (keep_root) {
        resident_internal_pages_.insert(cached_root_page_no_);
    } else if (cached_root_page_ != nullptr && cached_root_page_no_ != IX_NO_PAGE) {
        // A long-lived raw root pointer is valid only while the same page owns
        // both its cache pin and its BPM quota lease.
        if (frame_holds(cached_root_page_, cached_root_page_no_)) {
            buffer_pool_manager_->unpin_page(PageId{fd_, cached_root_page_no_}, false);
        }
        cached_root_page_ = nullptr;
        cached_root_page_no_ = IX_NO_PAGE;
    }
    // Release the pin every cache entry owns before dropping the pointers.
    for (const auto& [page_no, page] : cached_internal_pages_) {
        (void)page;
        buffer_pool_manager_->unpin_page(PageId{fd_, page_no}, false);
    }
    cached_internal_pages_.clear();
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 */
std::pair<IxNodeHandle*, bool> IxIndexHandle::find_leaf_page(const char* key, Operation operation,
                                                             Transaction* transaction, bool find_first) {
    (void)operation;
    (void)transaction;
    IxNodeHandle node;
    fetch_node_into(file_hdr_->root_page_, node);
    while (!node.is_leaf_page()) {
        page_id_t child_page_no = find_first ? node.value_at(0) : node.internal_lookup(key);
        unpin_if_not_cached(node.get_page_id());
        fetch_node_into(child_page_no, node);
    }
    // Caller owns the returned leaf (must unpin + delete).
    IxNodeHandle* leaf = new IxNodeHandle(node.file_hdr, node.page);
    return std::make_pair(leaf, false);
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::get_value(const char* key, std::vector<Rid>* result, Transaction* transaction) {
    (void)transaction;
    lookup_equal(key, *result);
    return !result->empty();
}

/**
 * @brief  将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note need to unpin the new node outside
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
bool IxIndexHandle::try_append_hint(const char* key, IxNodeHandle& leaf) const {
    const uint64_t hint = append_hint_.load(std::memory_order_relaxed);
    if (hint != pack_append_hint(static_cast<page_id_t>(static_cast<int32_t>(hint & 0xFFFFFFFFU)),
                                 topology_epoch_.load(std::memory_order_relaxed))) {
        return false;
    }
    const auto hint_page_no = static_cast<page_id_t>(static_cast<int32_t>(hint & 0xFFFFFFFFU));
    if (hint_page_no == IX_NO_PAGE || hint_page_no == IX_LEAF_HEADER_PAGE || hint_page_no == file_hdr_->root_page_) {
        return false;
    }

    fetch_node_into(hint_page_no, leaf);
    const bool valid =
        leaf.is_leaf_page() && leaf.get_size() > 0 && leaf.get_next_leaf() == IX_LEAF_HEADER_PAGE &&
        ix_compare(key, leaf.get_key(leaf.get_size() - 1), file_hdr_->col_types_, file_hdr_->col_lens_) > 0;
    if (!valid) {
        unpin_if_not_cached(leaf.get_page_id());
        return false;
    }
    return true;
}

void IxIndexHandle::remember_append_hint(page_id_t page_no) const {
    append_hint_.store(pack_append_hint(page_no, topology_epoch_.load(std::memory_order_relaxed)),
                       std::memory_order_relaxed);
}

IxNodeHandle* IxIndexHandle::split(IxNodeHandle* node, bool right_edge_append) {
    topology_epoch_.fetch_add(1, std::memory_order_relaxed);
    note_structure_change();
    IxNodeHandle* new_node = create_node();
    memcpy(new_node->page_hdr, node->page_hdr, sizeof(IxPageHdr));
    new_node->set_parent_page_no(node->get_parent_page_no());

    int old_size = node->get_size();
    int left_size = old_size / 2;
    if (right_edge_append && node->is_leaf_page()) {
        // Keep most historical entries on the old right edge and leave a
        // small, append-friendly leaf for the next inserts. Non-append and
        // internal-node splits retain the balanced 50/50 policy.
        left_size = std::max(1, (old_size * 4) / 5);
        if (left_size >= old_size) {
            left_size = old_size / 2;
        }
    }
    int right_size = old_size - left_size;
    new_node->set_size(0);
    new_node->insert_pairs(0, node->get_key(left_size), node->get_rid(left_size), right_size);
    node->set_size(left_size);

    if (node->is_leaf_page()) {
        new_node->set_prev_leaf(node->get_page_no());
        new_node->set_next_leaf(node->get_next_leaf());
        if (node->get_next_leaf() != IX_LEAF_HEADER_PAGE) {
            IxNodeHandle next_leaf;
            fetch_node_into(node->get_next_leaf(), next_leaf);
            next_leaf.set_prev_leaf(new_node->get_page_no());
            unpin_dirty_if_not_cached(next_leaf.get_page_id(), active_smo_wal_context());
        }
        node->set_next_leaf(new_node->get_page_no());
    } else {
        mark_internal_page_resident(new_node->get_page_no(), new_node->page);
        for (int i = 0; i < new_node->get_size(); ++i) {
            maintain_child(new_node, i);
        }
    }

    return new_node;
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 */
void IxIndexHandle::insert_into_parent(IxNodeHandle* old_node, const char* key, IxNodeHandle* new_node,
                                       const IndexWriteWalContext& wal_context) {
    note_structure_change();
    if (old_node->is_root_page()) {
        IxNodeHandle* new_root = create_node();
        new_root->page_hdr->is_leaf = false;
        new_root->set_size(0);
        new_root->set_parent_page_no(IX_NO_PAGE);
        new_root->insert_pair(0, old_node->get_key(0), Rid{old_node->get_page_no(), -1});
        new_root->insert_pair(1, new_node->get_key(0), Rid{new_node->get_page_no(), -1});

        old_node->set_parent_page_no(new_root->get_page_no());
        new_node->set_parent_page_no(new_root->get_page_no());
        update_root_page_no(new_root->get_page_no());
        refresh_root_page_cache();
        unpin_created_page(new_root->get_page_id(), active_smo_wal_context());
        delete new_root;
        return;
    }

    IxNodeHandle parent;
    fetch_node_into(old_node->get_parent_page_no(), parent);
    int insert_pos = parent.find_child(old_node) + 1;
    parent.insert_pair(insert_pos, key, Rid{new_node->get_page_no(), -1});
    new_node->set_parent_page_no(parent.get_page_no());

    if (parent.get_size() >= parent.get_max_size()) {
        IxNodeHandle* new_parent = split(&parent);
        insert_into_parent(&parent, new_parent->get_key(0), new_parent, wal_context);
        unpin_created_page(new_parent->get_page_id(), active_smo_wal_context());
        delete new_parent;
    }
    unpin_dirty_if_not_cached(parent.get_page_id(), active_smo_wal_context());
}

page_id_t IxIndexHandle::insert_entry_with_split(page_id_t leaf_page_no, const char* key, const Rid& value,
                                                 int insert_pos, bool right_edge_append,
                                                 const BufferPoolManager::FrameOperationToken& frame_operation,
                                                 const IndexWriteWalContext& wal_context) {
    struct PlannedPage {
        PageId id;
        bool existed;
        std::array<char, PAGE_SIZE> before{};
        std::unique_ptr<Page> after{std::make_unique<Page>()};
    };

    std::vector<PlannedPage> pages;
    std::unordered_map<page_id_t, size_t> page_index;
    std::vector<page_id_t> acquired_new_pages;
    bool allocation_started = false;

    struct AllocationGuard {
        std::vector<page_id_t>* acquired;
        std::vector<page_id_t>* spare;
        bool committed{false};

        ~AllocationGuard() {
            if (!committed && !acquired->empty()) {
                try {
                    spare->insert(spare->end(), acquired->begin(), acquired->end());
                } catch (...) {
                    // A bookkeeping allocation failure cannot be repaired here;
                    // preserve the original split exception.
                }
            }
        }
    } allocation_guard{&acquired_new_pages, &split_spare_pages_};

    auto add_page = [&](PageId id, bool existed, const char* data) -> size_t {
        const size_t index = pages.size();
        pages.push_back(PlannedPage{.id = id, .existed = existed});
        if (data != nullptr) {
            std::memcpy(pages.back().before.data(), data, PAGE_SIZE);
            std::memcpy(pages.back().after->get_data(), data, PAGE_SIZE);
        }
        page_index.emplace(id.page_no, index);
        return index;
    };

    auto load_existing = [&](page_id_t page_no, InsertSplitFault fault = InsertSplitFault::None) -> size_t {
        if (fault != InsertSplitFault::None) {
            maybe_inject_insert_split_fault(fault);
        }
        auto found = page_index.find(page_no);
        if (found != page_index.end()) {
            return found->second;
        }
        if (allocation_started) {
            throw InternalError("index split preflight attempted a new read after allocation");
        }
        Page* source = cached_page(page_no);
        const bool fetched = source == nullptr;
        if (fetched) {
            source = frame_operation.fetch_page(PageId{fd_, page_no});
        }
        if (source == nullptr) {
            throw InternalError("index split preflight could not fetch page " + std::to_string(page_no));
        }
        size_t index;
        {
            std::shared_lock page_lock{source->latch()};
            index = add_page(PageId{fd_, page_no}, true, source->get_data());
        }
        if (fetched) {
            buffer_pool_manager_->unpin_page(PageId{fd_, page_no}, false);
        }
        return index;
    };

    auto allocate_page = [&](InsertSplitFault fault) -> page_id_t {
        maybe_inject_insert_split_fault(fault);
        allocation_started = true;
        PageId page_id{fd_, INVALID_PAGE_ID};
        Page* page = nullptr;
        if (!split_spare_pages_.empty()) {
            page_id.page_no = split_spare_pages_.back();
            split_spare_pages_.pop_back();
            page = frame_operation.fetch_page(page_id);
            if (page != nullptr) {
                std::unique_lock page_lock{page->latch()};
                std::memset(page->get_data(), 0, PAGE_SIZE);
            }
        } else {
            page = frame_operation.new_page(&page_id);
        }
        if (page == nullptr) {
            if (page_id.page_no != INVALID_PAGE_ID) {
                file_hdr_->num_pages_ = std::max(file_hdr_->num_pages_, page_id.page_no + 1);
                acquired_new_pages.push_back(page_id.page_no);
            }
            throw InternalError("index split preflight could not allocate page");
        }
        file_hdr_->num_pages_ = std::max(file_hdr_->num_pages_, page_id.page_no + 1);
        acquired_new_pages.push_back(page_id.page_no);
        {
            std::shared_lock page_lock{page->latch()};
            add_page(page_id, false, page->get_data());
        }
        // A preallocated page may be evicted before commit. Marking its zero
        // image dirty makes that eviction materialize a readable page rather
        // than leaving a short-file hole that cannot be fetched again.
        buffer_pool_manager_->unpin_page(page_id, wal_context.dependency());
        return page_id.page_no;
    };

    auto node_at = [&](page_id_t page_no) {
        return IxNodeHandle(file_hdr_.get(), pages[page_index.at(page_no)].after.get());
    };

    const page_id_t old_root_page_no = file_hdr_->root_page_;
    const page_id_t old_last_leaf_page_no = file_hdr_->last_leaf_;
    page_id_t planned_root_page_no = old_root_page_no;
    page_id_t planned_last_leaf_page_no = old_last_leaf_page_no;
    load_existing(leaf_page_no);

    // Complete every fallible read before the first allocation. Besides the
    // leaf-chain neighbor, pre-read the complete ancestor path and the children
    // that an internal split will move. Once allocation_started is true,
    // load_existing() is deliberately forbidden from reaching the buffer pool.
    {
        IxNodeHandle leaf_before = node_at(leaf_page_no);
        const page_id_t next_leaf_page_no = leaf_before.get_next_leaf();
        if (next_leaf_page_no != IX_LEAF_HEADER_PAGE) {
            load_existing(next_leaf_page_no, InsertSplitFault::NextLeafFetch);
        }

        page_id_t child_page_no = leaf_page_no;
        page_id_t parent_page_no = leaf_before.get_parent_page_no();
        bool split_cascades = true;
        while (parent_page_no != IX_NO_PAGE) {
            load_existing(parent_page_no);
            IxNodeHandle parent = node_at(parent_page_no);
            int child_idx = 0;
            while (child_idx < parent.get_size() && parent.value_at(child_idx) != child_page_no) {
                ++child_idx;
            }
            if (child_idx == parent.get_size()) {
                throw InternalError("index split preflight found a parent without its child");
            }

            if (split_cascades && parent.get_size() + 1 >= parent.get_max_size()) {
                const int future_size = parent.get_size() + 1;
                const int left_size = future_size / 2;
                const int inserted_sibling_pos = child_idx + 1;
                for (int output_pos = left_size; output_pos < future_size; ++output_pos) {
                    if (output_pos == inserted_sibling_pos) {
                        continue;
                    }
                    const int original_pos = output_pos < inserted_sibling_pos ? output_pos : output_pos - 1;
                    load_existing(parent.value_at(original_pos), InsertSplitFault::MovedChildFetch);
                }
            } else {
                split_cascades = false;
            }

            child_page_no = parent_page_no;
            parent_page_no = parent.get_parent_page_no();
        }
    }

    {
        IxNodeHandle leaf = node_at(leaf_page_no);
        leaf.insert_pair(insert_pos, key, value);
    }

    page_id_t current_page_no = leaf_page_no;
    bool append_split = right_edge_append;
    while (true) {
        IxNodeHandle current_before_allocation = node_at(current_page_no);
        if (current_before_allocation.get_size() < current_before_allocation.get_max_size()) {
            break;
        }

        const bool current_is_leaf = current_before_allocation.is_leaf_page();
        const page_id_t parent_page_no = current_before_allocation.get_parent_page_no();
        const page_id_t sibling_page_no = allocate_page(InsertSplitFault::SiblingAllocation);

        IxNodeHandle current = node_at(current_page_no);
        IxNodeHandle sibling = node_at(sibling_page_no);
        std::memcpy(sibling.page_hdr, current.page_hdr, sizeof(IxPageHdr));
        sibling.set_parent_page_no(parent_page_no);

        const int old_size = current.get_size();
        int left_size = old_size / 2;
        if (append_split && current_is_leaf) {
            left_size = std::max(1, (old_size * 4) / 5);
            if (left_size >= old_size) {
                left_size = old_size / 2;
            }
        }
        const int right_size = old_size - left_size;
        sibling.set_size(0);
        sibling.insert_pairs(0, current.get_key(left_size), current.get_rid(left_size), right_size);
        current.set_size(left_size);

        if (current_is_leaf) {
            const page_id_t next_leaf_page_no = current.get_next_leaf();
            sibling.set_prev_leaf(current_page_no);
            sibling.set_next_leaf(next_leaf_page_no);
            current.set_next_leaf(sibling_page_no);
            if (next_leaf_page_no != IX_LEAF_HEADER_PAGE) {
                load_existing(next_leaf_page_no, InsertSplitFault::NextLeafFetch);
                IxNodeHandle next_leaf = node_at(next_leaf_page_no);
                next_leaf.set_prev_leaf(sibling_page_no);
            }
            if (planned_last_leaf_page_no == current_page_no) {
                planned_last_leaf_page_no = sibling_page_no;
            }
        } else {
            std::vector<page_id_t> moved_children;
            moved_children.reserve(static_cast<size_t>(sibling.get_size()));
            for (int child_idx = 0; child_idx < sibling.get_size(); ++child_idx) {
                moved_children.push_back(sibling.value_at(child_idx));
            }
            for (const page_id_t child_page_no : moved_children) {
                load_existing(child_page_no, InsertSplitFault::MovedChildFetch);
                IxNodeHandle child = node_at(child_page_no);
                child.set_parent_page_no(sibling_page_no);
            }
        }

        std::vector<char> promoted_key(static_cast<size_t>(file_hdr_->col_tot_len_));
        std::memcpy(promoted_key.data(), sibling.get_key(0), promoted_key.size());
        if (parent_page_no == IX_NO_PAGE) {
            const page_id_t new_root_page_no = allocate_page(InsertSplitFault::NewRootAllocation);
            IxNodeHandle left = node_at(current_page_no);
            IxNodeHandle right = node_at(sibling_page_no);
            IxNodeHandle new_root = node_at(new_root_page_no);
            new_root.page_hdr->is_leaf = false;
            new_root.set_size(0);
            new_root.set_parent_page_no(IX_NO_PAGE);
            new_root.insert_pair(0, left.get_key(0), Rid{current_page_no, -1});
            new_root.insert_pair(1, right.get_key(0), Rid{sibling_page_no, -1});
            left.set_parent_page_no(new_root_page_no);
            right.set_parent_page_no(new_root_page_no);
            planned_root_page_no = new_root_page_no;
            break;
        }

        load_existing(parent_page_no);
        IxNodeHandle parent = node_at(parent_page_no);
        int insert_position = 0;
        while (insert_position < parent.get_size() && parent.value_at(insert_position) != current_page_no) {
            ++insert_position;
        }
        if (insert_position == parent.get_size()) {
            throw InternalError("index split preflight found a parent without its child");
        }
        parent.insert_pair(insert_position + 1, promoted_key.data(), Rid{sibling_page_no, -1});
        IxNodeHandle right = node_at(sibling_page_no);
        right.set_parent_page_no(parent_page_no);
        current_page_no = parent_page_no;
        append_split = false;
    }

    if (insert_pos == 0) {
        page_id_t child_page_no = leaf_page_no;
        while (true) {
            IxNodeHandle child = node_at(child_page_no);
            const page_id_t parent_page_no = child.get_parent_page_no();
            if (parent_page_no == IX_NO_PAGE) {
                break;
            }
            load_existing(parent_page_no);
            IxNodeHandle parent = node_at(parent_page_no);
            int child_idx = 0;
            while (child_idx < parent.get_size() && parent.value_at(child_idx) != child_page_no) {
                ++child_idx;
            }
            if (child_idx == parent.get_size()) {
                throw InternalError("index split preflight lost a separator child");
            }
            parent.set_key(child_idx, child.get_key(0));
            if (child_idx != 0) {
                break;
            }
            child_page_no = parent_page_no;
        }
    }

    std::vector<size_t> commit_order;
    commit_order.reserve(pages.size());
    for (size_t index = 0; index < pages.size(); ++index) {
        if (!pages[index].existed) {
            commit_order.push_back(index);
        }
    }
    for (size_t index = 0; index < pages.size(); ++index) {
        if (pages[index].existed) {
            commit_order.push_back(index);
        }
    }

    auto acquire_page = [&](const PlannedPage& planned, bool* fetched) -> Page* {
        try {
            run_insert_split_test_hook(InsertSplitStage::CommitReacquire);
            Page* page = cached_page(planned.id.page_no);
            *fetched = page == nullptr;
            if (*fetched) {
                page = frame_operation.fetch_page(planned.id);
            }
            return page;
        } catch (...) {
            *fetched = false;
            return nullptr;
        }
    };
    auto release_page = [&](const PlannedPage& planned, bool fetched) {
        if (fetched) {
            buffer_pool_manager_->unpin_page(planned.id, false);
        }
    };
    auto note_image_locked = [&](InsertSplitStage stage, PageId page_id, Page* page) noexcept {
        try {
            // Test observers may synchronize another thread, but must not call
            // BPM or reacquire this page latch while the image latch is held.
            run_insert_split_test_hook(stage, page_id, page);
        } catch (...) {
            // Image-stage hooks are observers, never commit fault injectors.
        }
    };

    std::vector<size_t> applied;
    auto rollback_applied = [&]() -> bool {
        for (auto it = applied.rbegin(); it != applied.rend(); ++it) {
            PlannedPage& planned = pages[*it];
            if (!planned.existed) {
                continue;
            }
            bool fetched = false;
            Page* page = nullptr;
            try {
                run_insert_split_test_hook(InsertSplitStage::RollbackReacquire);
                page = cached_page(planned.id.page_no);
                fetched = page == nullptr;
                if (fetched) {
                    page = frame_operation.fetch_page(planned.id);
                }
            } catch (...) {
                page = nullptr;
            }
            if (page == nullptr) {
                return false;
            }
            {
                std::unique_lock page_lock{page->latch()};
                BufferPoolManager::mark_dirty(page, wal_context.dependency());
                note_image_locked(InsertSplitStage::RollbackBeforeImageRestore, planned.id, page);
                std::memcpy(page->get_data(), planned.before.data(), PAGE_SIZE);
                note_image_locked(InsertSplitStage::RollbackImageRestored, planned.id, page);
            }
            release_page(planned, fetched);
        }
        return true;
    };

    Page* current_page = nullptr;
    bool current_fetched = false;
    size_t current_index = 0;
    run_insert_split_test_hook(InsertSplitStage::CommitBegin);
    for (const size_t next_index : commit_order) {
        PlannedPage& next = pages[next_index];
        bool next_fetched = false;
        Page* next_page = acquire_page(next, &next_fetched);
        if (next_page == nullptr) {
            if (current_page != nullptr) {
                PlannedPage& current = pages[current_index];
                if (current.existed) {
                    std::unique_lock page_lock{current_page->latch()};
                    BufferPoolManager::mark_dirty(current_page, wal_context.dependency());
                    note_image_locked(InsertSplitStage::RollbackBeforeImageRestore, current.id, current_page);
                    std::memcpy(current_page->get_data(), current.before.data(), PAGE_SIZE);
                    note_image_locked(InsertSplitStage::RollbackImageRestored, current.id, current_page);
                }
                release_page(current, current_fetched);
            }
            if (!rollback_applied()) {
                throw InternalError("index split commit failed and rollback could not restore the old tree");
            }
            throw InternalError("index split commit could not fetch page " + std::to_string(next.id.page_no));
        }
        if (current_page != nullptr) {
            PlannedPage& current = pages[current_index];
            release_page(current, current_fetched);
            applied.push_back(current_index);
        }
        {
            std::unique_lock page_lock{next_page->latch()};
            BufferPoolManager::mark_dirty(next_page, wal_context.dependency());
            note_image_locked(InsertSplitStage::CommitBeforeImageApply, next.id, next_page);
            std::memcpy(next_page->get_data(), next.after->get_data(), PAGE_SIZE);
            note_image_locked(InsertSplitStage::CommitImageApplied, next.id, next_page);
        }
        current_page = next_page;
        current_fetched = next_fetched;
        current_index = next_index;
    }
    if (current_page != nullptr) {
        PlannedPage& current = pages[current_index];
        release_page(current, current_fetched);
        applied.push_back(current_index);
    }

    file_hdr_->root_page_ = planned_root_page_no;
    file_hdr_->last_leaf_ = planned_last_leaf_page_no;
    topology_epoch_.fetch_add(1, std::memory_order_relaxed);
    note_structure_change();
    for (const PlannedPage& planned : pages) {
        note_smo_page(planned.id.page_no);
    }

    if (planned_root_page_no != old_root_page_no) {
        refresh_root_page_cache(&frame_operation);
    }
    if (right_edge_append) {
        remember_append_hint(planned_last_leaf_page_no);
    }
    allocation_guard.committed = true;
    return leaf_page_no;
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
 */
page_id_t IxIndexHandle::insert_entry(const char* key, const Rid& value, const IndexWriteWalContext& wal_context,
                                      bool allow_duplicate) {
    // The common case only changes one existing leaf entry. Structure-shared
    // protects the root-to-leaf route while the leaf latch serializes writers
    // that target the same page. Splits, separator changes, and inserts at
    // position zero fall back to the stable tree-exclusive implementation.
    {
        auto structure_guard = lock_shared();
        IxNodeHandle leaf;
        if (!try_append_hint(key, leaf)) {
            fetch_node_into(file_hdr_->root_page_, leaf);
            while (!leaf.is_leaf_page()) {
                page_id_t child_page_no = leaf.internal_lookup(key);
                unpin_if_not_cached(leaf.get_page_id());
                fetch_node_into(child_page_no, leaf);
            }
        }

        std::unique_lock<std::shared_mutex> leaf_guard(leaf.page->latch());
        int pos = leaf.lower_bound(key);
        bool duplicate_key = pos < leaf.get_size() &&
                             ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0;
        if (duplicate_key && !allow_duplicate) {
            leaf_guard.unlock();
            unpin_if_not_cached(leaf.get_page_id());
            throw IndexEntryExistsError();
        }
        if (allow_duplicate) {
            while (pos < leaf.get_size() &&
                   ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0) {
                ++pos;
            }
        }

        const bool needs_structure_change = pos == 0 || leaf.get_size() + 1 >= leaf.get_max_size();
        if (!needs_structure_change) {
            const page_id_t inserted_page_no = leaf.get_page_no();
            BufferPoolManager::mark_dirty(leaf.page, wal_context.dependency());
            leaf.insert_pair(pos, key, value);
            if (pos == leaf.get_size() - 1 && leaf.get_next_leaf() == IX_LEAF_HEADER_PAGE) {
                remember_append_hint(inserted_page_no);
            }
            leaf_guard.unlock();
            unpin_if_not_cached(leaf.get_page_id());
            return inserted_page_no;
        }

        leaf_guard.unlock();
        unpin_if_not_cached(leaf.get_page_id());
    }

    auto structure_guard = lock_exclusive();
    return insert_entry_unlocked(key, value, wal_context, allow_duplicate);
}

page_id_t IxIndexHandle::insert_entry_unlocked(const char* key, const Rid& value,
                                               const IndexWriteWalContext& wal_context, bool allow_duplicate) {
    SmoScope smo(this, wal_context);
    IxNodeHandle leaf;
    if (!try_append_hint(key, leaf)) {
        fetch_node_into(file_hdr_->root_page_, leaf);
        while (!leaf.is_leaf_page()) {
            page_id_t child_page_no = leaf.internal_lookup(key);
            unpin_if_not_cached(leaf.get_page_id());
            fetch_node_into(child_page_no, leaf);
        }
    }

    int pos = leaf.lower_bound(key);
    bool duplicate_key =
        pos < leaf.get_size() && ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0;
    if (duplicate_key && !allow_duplicate) {
        unpin_if_not_cached(leaf.get_page_id());
        throw IndexEntryExistsError();
    }
    if (allow_duplicate) {
        while (pos < leaf.get_size() &&
               ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0) {
            ++pos;
        }
    }

    const bool right_edge_append = pos == leaf.get_size() && leaf.get_next_leaf() == IX_LEAF_HEADER_PAGE;
    page_id_t inserted_page_no = leaf.get_page_no();
    if (leaf.get_size() + 1 >= leaf.get_max_size()) {
        unpin_if_not_cached(leaf.get_page_id());
        smo.begin_frame_operation();
        inserted_page_no = insert_entry_with_split(inserted_page_no, key, value, pos, right_edge_append,
                                                   smo.frame_operation(), wal_context);
        smo.commit();
        return inserted_page_no;
    }

    {
        std::unique_lock leaf_guard{leaf.page->latch()};
        BufferPoolManager::mark_dirty(leaf.page, wal_context.dependency());
        leaf.insert_pair(pos, key, value);
    }
    if (right_edge_append) {
        remember_append_hint(leaf.get_page_no());
    }

    if (pos == 0) {
        maintain_parent(&leaf);
    }
    unpin_if_not_cached(leaf.get_page_id());
    smo.commit();
    return inserted_page_no;
}

IxIndexHandle::PinnedInserter::PinnedInserter(IxIndexHandle* h, const IndexWriteWalContext& write_wal_context)
    : ih(h), latch(h->lock_exclusive()), wal_context(write_wal_context) {
    ih->fetch_node_into(ih->file_hdr_->root_page_, leaf);
    while (!leaf.is_leaf_page()) {
        page_id_t child_page_no = leaf.value_at(0);
        ih->unpin_if_not_cached(leaf.get_page_id());
        ih->fetch_node_into(child_page_no, leaf);
    }
    active = true;
}

IxIndexHandle::PinnedInserter::~PinnedInserter() {
    if (active) {
        ih->unpin_if_not_cached(leaf.get_page_id());
    }
}

void IxIndexHandle::PinnedInserter::insert(const char* key, const Rid& value, bool allow_duplicate) {
    // Skip root→leaf walk if key belongs in the pinned leaf (ascending bulk load).
    if (leaf.get_size() > 0) {
        int cmp_first = ix_compare(key, leaf.get_key(0), ih->file_hdr_->col_types_, ih->file_hdr_->col_lens_);
        int cmp_last =
            ix_compare(key, leaf.get_key(leaf.get_size() - 1), ih->file_hdr_->col_types_, ih->file_hdr_->col_lens_);
        bool before_leaf = cmp_first < 0;
        bool after_leaf = cmp_last > 0 && leaf.get_page_no() != ih->file_hdr_->last_leaf_;
        if (before_leaf || after_leaf) {
            // Key out of range — rewalk. The leaf being released already holds
            // the entries earlier insert() calls put there, and nothing else
            // marks it dirty, so releasing it clean would let the buffer pool
            // drop every one of them. Non-ascending bulk loads - TPC-C's
            // customer(..., c_last, c_id) and orders(..., o_c_id, o_id) - take
            // this branch constantly.
            // While the walk is in flight `leaf` names a page this object no
            // longer owns, so a throwing fetch must not reach the destructor.
            active = false;
            ih->unpin_if_not_cached(leaf.get_page_id());
            ih->fetch_node_into(ih->file_hdr_->root_page_, leaf);
            while (!leaf.is_leaf_page()) {
                page_id_t child_page_no = leaf.internal_lookup(key);
                ih->unpin_if_not_cached(leaf.get_page_id());
                ih->fetch_node_into(child_page_no, leaf);
            }
            active = true;
        }
    }

    int pos = leaf.lower_bound(key);
    bool duplicate_key = pos < leaf.get_size() &&
                         ix_compare(leaf.get_key(pos), key, ih->file_hdr_->col_types_, ih->file_hdr_->col_lens_) == 0;
    if (duplicate_key && !allow_duplicate) {
        throw IndexEntryExistsError();
    }
    if (allow_duplicate) {
        while (pos < leaf.get_size() &&
               ix_compare(leaf.get_key(pos), key, ih->file_hdr_->col_types_, ih->file_hdr_->col_lens_) == 0) {
            ++pos;
        }
    }

    const bool right_edge_append = pos == leaf.get_size() && leaf.get_next_leaf() == IX_LEAF_HEADER_PAGE;
    if (leaf.get_size() + 1 >= leaf.get_max_size()) {
        // Release the bulk-load cursor before entering the shared split planner:
        // prior rows made this page dirty, but this row has not modified it yet.
        active = false;
        ih->unpin_if_not_cached(leaf.get_page_id());
        ih->insert_entry_unlocked(key, value, wal_context, allow_duplicate);

        ih->fetch_node_into(ih->file_hdr_->root_page_, leaf);
        while (!leaf.is_leaf_page()) {
            const page_id_t child_page_no = leaf.internal_lookup(key);
            ih->unpin_if_not_cached(leaf.get_page_id());
            ih->fetch_node_into(child_page_no, leaf);
        }
        active = true;
        return;
    }

    SmoScope smo(ih, wal_context);
    // This object holds the leaf pinned across rows, so note the page explicitly.
    ih->note_smo_page(leaf.get_page_no());
    {
        std::unique_lock leaf_guard{leaf.page->latch()};
        BufferPoolManager::mark_dirty(leaf.page, wal_context.dependency());
        leaf.insert_pair(pos, key, value);
    }
    if (right_edge_append) {
        ih->remember_append_hint(leaf.get_page_no());
    }
    if (pos == 0) {
        ih->maintain_parent(&leaf);
    }
    smo.commit();
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 */
bool IxIndexHandle::delete_entry(const char* key, const IndexWriteWalContext& wal_context) {
    {
        auto structure_guard = lock_shared();
        IxNodeHandle leaf;
        fetch_node_into(file_hdr_->root_page_, leaf);
        while (!leaf.is_leaf_page()) {
            page_id_t child_page_no = leaf.internal_lookup(key);
            unpin_if_not_cached(leaf.get_page_id());
            fetch_node_into(child_page_no, leaf);
        }

        std::unique_lock<std::shared_mutex> leaf_guard(leaf.page->latch());
        int pos = leaf.lower_bound(key);
        if (pos >= leaf.get_size() ||
            ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) != 0) {
            leaf_guard.unlock();
            unpin_if_not_cached(leaf.get_page_id());
            return false;
        }

        const bool needs_structure_change = pos == 0 || leaf.get_size() - 1 < leaf.get_min_size();
        if (!needs_structure_change) {
            BufferPoolManager::mark_dirty(leaf.page, wal_context.dependency());
            leaf.erase_pair(pos);
            leaf_guard.unlock();
            unpin_if_not_cached(leaf.get_page_id());
            return true;
        }

        leaf_guard.unlock();
        unpin_if_not_cached(leaf.get_page_id());
    }

    auto structure_guard = lock_exclusive();
    return delete_entry_unlocked(key, wal_context);
}

bool IxIndexHandle::delete_entry_unlocked(const char* key, const IndexWriteWalContext& wal_context) {
    SmoScope smo(this, wal_context);
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::DELETE, nullptr);
    int old_size = leaf->get_size();
    int pos = leaf->lower_bound(key);
    if (pos >= old_size || ix_compare(leaf->get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) != 0) {
        unpin_if_not_cached(leaf->get_page_id());
        delete leaf;
        return false;
    }

    {
        std::unique_lock leaf_guard{leaf->page->latch()};
        BufferPoolManager::mark_dirty(leaf->page, wal_context.dependency());
        leaf->erase_pair(pos);
    }
    if (leaf->get_size() > 0) {
        maintain_parent(leaf);
    }
    unpin_if_not_cached(leaf->get_page_id());
    delete leaf;
    smo.commit();
    return true;
}

bool IxIndexHandle::delete_entry(const char* key, const Rid& value, const IndexWriteWalContext& wal_context) {
    bool needs_structure_fallback = false;
    {
        auto structure_guard = lock_shared();
        IxNodeHandle leaf;
        fetch_node_into(file_hdr_->root_page_, leaf);
        while (!leaf.is_leaf_page()) {
            int child_idx = leaf.lower_bound(key);
            if (child_idx >= leaf.get_size()) {
                child_idx = leaf.get_size() - 1;
            } else if (child_idx > 0 &&
                       ix_compare(leaf.get_key(child_idx), key, file_hdr_->col_types_, file_hdr_->col_lens_) > 0) {
                --child_idx;
            }
            page_id_t child_page_no = leaf.value_at(child_idx);
            unpin_if_not_cached(leaf.get_page_id());
            fetch_node_into(child_page_no, leaf);
        }

        for (;;) {
            std::unique_lock<std::shared_mutex> leaf_guard(leaf.page->latch());
            int pos = leaf.lower_bound(key);
            bool stop_at_leaf = false;
            while (pos < leaf.get_size()) {
                int cmp = ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_);
                if (cmp > 0) {
                    stop_at_leaf = true;
                    break;
                }
                if (*leaf.get_rid(pos) == value) {
                    needs_structure_fallback = pos == 0 || leaf.get_size() - 1 < leaf.get_min_size();
                    if (!needs_structure_fallback) {
                        BufferPoolManager::mark_dirty(leaf.page, wal_context.dependency());
                        leaf.erase_pair(pos);
                        leaf_guard.unlock();
                        unpin_if_not_cached(leaf.get_page_id());
                        return true;
                    }
                    break;
                }
                ++pos;
            }

            const bool found_target =
                needs_structure_fallback ||
                (pos < leaf.get_size() && *leaf.get_rid(pos) == value &&
                 ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0);
            const bool at_last_leaf = leaf.get_page_no() == file_hdr_->last_leaf_;
            const page_id_t next_leaf = leaf.get_next_leaf();
            leaf_guard.unlock();
            unpin_if_not_cached(leaf.get_page_id());
            // last_leaf_ only reaches disk at a checkpoint, so after a crash it
            // can name a leaf that has since been split. The end of the chain is
            // the authoritative stop condition - without it a stale last_leaf_
            // makes this walk parse the leaf header page as a node. Same guard
            // as lookup_equal().
            if (found_target || stop_at_leaf || at_last_leaf || next_leaf == IX_LEAF_HEADER_PAGE) {
                break;
            }
            fetch_node_into(next_leaf, leaf);
        }
    }

    if (!needs_structure_fallback) {
        return false;
    }
    auto guard = lock_exclusive();
    return delete_entry_unlocked(key, value, wal_context);
}

bool IxIndexHandle::delete_entry_unlocked(const char* key, const Rid& value, const IndexWriteWalContext& wal_context) {
    SmoScope smo(this, wal_context);
    const Iid lower = lower_bound(key);
    IxNodeHandle leaf;
    fetch_node_into(lower.page_no, leaf);
    for (;;) {
        page_id_t next_leaf = IX_LEAF_HEADER_PAGE;
        bool found = false;
        bool stop = false;
        {
            std::unique_lock leaf_guard{leaf.page->latch()};
            int pos = leaf.lower_bound(key);
            while (pos < leaf.get_size()) {
                const int cmp = ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_);
                if (cmp > 0) {
                    stop = true;
                    break;
                }
                if (*leaf.get_rid(pos) == value) {
                    BufferPoolManager::mark_dirty(leaf.page, wal_context.dependency());
                    leaf.erase_pair(pos);
                    found = true;
                    break;
                }
                ++pos;
            }
            if (!found && !stop && leaf.get_next_leaf() != IX_LEAF_HEADER_PAGE) {
                next_leaf = leaf.get_next_leaf();
            }
        }
        if (found) {
            if (leaf.get_size() > 0) {
                maintain_parent(&leaf);
            }
            unpin_if_not_cached(leaf.get_page_id());
            smo.commit();
            return true;
        }
        unpin_if_not_cached(leaf.get_page_id());
        if (stop || next_leaf == IX_LEAF_HEADER_PAGE) {
            break;
        }
        fetch_node_into(next_leaf, leaf);
    }
    smo.commit();
    return false;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then redistribute.
 * Otherwise, merge(Coalesce).
 */
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle* node, const IndexWriteWalContext& wal_context,
                                             bool* root_is_latched) {
    if (node->is_root_page()) {
        return adjust_root(node);
    }
    if (node->get_size() >= node->get_min_size()) {
        return false;
    }

    IxNodeHandle parent_storage;
    fetch_node_into(node->get_parent_page_no(), parent_storage);
    IxNodeHandle* parent = &parent_storage;
    int index = parent->find_child(node);
    int neighbor_index = index == 0 ? 1 : index - 1;
    IxNodeHandle neighbor_storage;
    fetch_node_into(parent->value_at(neighbor_index), neighbor_storage);
    IxNodeHandle* neighbor = &neighbor_storage;

    bool deleted = false;
    if (node->get_size() + neighbor->get_size() >= node->get_min_size() * 2) {
        topology_epoch_.fetch_add(1, std::memory_order_relaxed);
        redistribute(neighbor, node, parent, index);
    } else {
        topology_epoch_.fetch_add(1, std::memory_order_relaxed);
        deleted = coalesce(&neighbor, &node, &parent, index, wal_context, root_is_latched);
    }

    unpin_dirty_if_not_cached(neighbor->get_page_id(), active_smo_wal_context());
    unpin_dirty_if_not_cached(parent->get_page_id(), active_smo_wal_context());
    return deleted;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesce_or_redistribute()
 */
bool IxIndexHandle::adjust_root(IxNodeHandle* old_root_node) {
    if (!old_root_node->is_leaf_page() && old_root_node->get_size() == 1) {
        topology_epoch_.fetch_add(1, std::memory_order_relaxed);
        note_structure_change();
        page_id_t child_page_no = old_root_node->value_at(0);
        const bool child_was_cached = cached_page(child_page_no) != nullptr;
        IxNodeHandle child;
        fetch_node_into(child_page_no, child);
        {
            std::unique_lock child_guard{child.page->latch()};
            BufferPoolManager::mark_dirty(child.page, active_smo_wal_context().dependency());
            child.set_parent_page_no(IX_NO_PAGE);
        }
        update_root_page_no(child_page_no);
        refresh_root_page_cache();
        if (child_was_cached) {
            // refresh_root_page_cache transferred the internal cache pin into
            // the root cache pin; fetch_node_into did not acquire another pin.
            unpin_if_not_cached(child.get_page_id());
        } else {
            // The child was fetched before it became the cached root. Release
            // that operation pin directly instead of mistaking it for the new
            // root cache's ownership pin.
            buffer_pool_manager_->unpin_page(child.get_page_id(), false);
        }
        return true;
    }
    if (old_root_node->is_leaf_page() && old_root_node->get_size() == 0) {
        return false;
    }
    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::redistribute(IxNodeHandle* neighbor_node, IxNodeHandle* node, IxNodeHandle* parent, int index) {
    (void)parent;
    note_structure_change();
    if (index == 0) {
        node->insert_pair(node->get_size(), neighbor_node->get_key(0), *neighbor_node->get_rid(0));
        neighbor_node->erase_pair(0);
        maintain_child(node, node->get_size() - 1);
        maintain_parent(neighbor_node);
    } else {
        int move_pos = neighbor_node->get_size() - 1;
        node->insert_pair(0, neighbor_node->get_key(move_pos), *neighbor_node->get_rid(move_pos));
        neighbor_node->erase_pair(move_pos);
        maintain_child(node, 0);
        maintain_parent(node);
    }
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::coalesce(IxNodeHandle** neighbor_node, IxNodeHandle** node, IxNodeHandle** parent, int index,
                             const IndexWriteWalContext& wal_context, bool* root_is_latched) {
    note_structure_change();
    if (index == 0) {
        std::swap(*neighbor_node, *node);
        index = 1;
    }

    IxNodeHandle* left = *neighbor_node;
    IxNodeHandle* right = *node;
    int left_size = left->get_size();
    left->insert_pairs(left_size, right->get_key(0), right->get_rid(0), right->get_size());
    for (int i = left_size; i < left->get_size(); ++i) {
        maintain_child(left, i);
    }

    if (right->is_leaf_page()) {
        if (file_hdr_->last_leaf_ == right->get_page_no()) {
            file_hdr_->last_leaf_ = left->get_page_no();
        }
        erase_leaf(right);
    }

    (*parent)->erase_pair(index);
    release_node_handle(*right);
    return coalesce_or_redistribute(*parent, wal_context, root_is_latched);
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid& iid) const {
    IxNodeHandle node;
    fetch_node_into(iid.page_no, node);
    if (iid.slot_no >= node.get_size()) {
        unpin_if_not_cached(node.get_page_id());
        throw IndexEntryNotFoundError();
    }
    Rid rid = *node.get_rid(iid.slot_no);
    unpin_if_not_cached(node.get_page_id());
    return rid;
}

/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char* key) const {
    IxNodeHandle leaf;
    fetch_root_node_into(leaf);
    while (!leaf.is_leaf_page()) {
        int child_idx = leaf.lower_bound(key);
        if (child_idx >= leaf.get_size()) {
            child_idx = leaf.get_size() - 1;
        } else if (child_idx > 0 &&
                   ix_compare(leaf.get_key(child_idx), key, file_hdr_->col_types_, file_hdr_->col_lens_) > 0) {
            --child_idx;
        }
        page_id_t child_page_no = leaf.value_at(child_idx);
        unpin_if_not_cached_root(leaf.get_page_id());
        fetch_node_into(child_page_no, leaf);
    }
    std::shared_lock<std::shared_mutex> leaf_guard(leaf.page->latch());
    int slot_no = leaf.lower_bound(key);
    Iid iid{leaf.get_page_no(), slot_no};
    if (slot_no == leaf.get_size() && leaf.get_page_no() != file_hdr_->last_leaf_) {
        iid = Iid{leaf.get_next_leaf(), 0};
    }
    leaf_guard.unlock();
    unpin_if_not_cached_root(leaf.get_page_id());
    return iid;
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char* key) {
    IxNodeHandle leaf;
    fetch_root_node_into(leaf);
    while (!leaf.is_leaf_page()) {
        page_id_t child_page_no = leaf.internal_lookup(key);
        unpin_if_not_cached_root(leaf.get_page_id());
        fetch_node_into(child_page_no, leaf);
    }

    std::shared_lock<std::shared_mutex> leaf_guard(leaf.page->latch());
    int slot_no = leaf.upper_bound(key);
    Iid iid{leaf.get_page_no(), slot_no};
    if (slot_no == leaf.get_size() && leaf.get_page_no() != file_hdr_->last_leaf_) {
        iid = Iid{leaf.get_next_leaf(), 0};
    }
    leaf_guard.unlock();
    unpin_if_not_cached_root(leaf.get_page_id());
    return iid;
}

/**
 * @brief Single root-to-leaf descent returning both lower and upper Iid for an
 * equality lookup. Avoids the double tree walk of lower_bound + upper_bound.
 */
std::pair<Iid, Iid> IxIndexHandle::equal_range(const char* key) {
    IxNodeHandle leaf;
    fetch_root_node_into(leaf);
    while (!leaf.is_leaf_page()) {
        int child_idx = leaf.lower_bound(key);
        if (child_idx >= leaf.get_size()) {
            child_idx = leaf.get_size() - 1;
        } else if (child_idx > 0 &&
                   ix_compare(leaf.get_key(child_idx), key, file_hdr_->col_types_, file_hdr_->col_lens_) > 0) {
            --child_idx;
        }
        page_id_t child_page_no = leaf.value_at(child_idx);
        unpin_if_not_cached_root(leaf.get_page_id());
        fetch_node_into(child_page_no, leaf);
    }
    std::shared_lock<std::shared_mutex> leaf_guard(leaf.page->latch());
    int lower_slot = leaf.lower_bound(key);
    int upper_slot = leaf.upper_bound(key);
    Iid lower{leaf.get_page_no(), lower_slot};
    if (lower_slot == leaf.get_size() && leaf.get_page_no() != file_hdr_->last_leaf_) {
        lower = Iid{leaf.get_next_leaf(), 0};
    }
    Iid upper{leaf.get_page_no(), upper_slot};
    if (upper_slot == leaf.get_size() && leaf.get_page_no() != file_hdr_->last_leaf_) {
        upper = Iid{leaf.get_next_leaf(), 0};
    }
    leaf_guard.unlock();
    unpin_if_not_cached_root(leaf.get_page_id());
    return {lower, upper};
}

void IxIndexHandle::lookup_equal(const char* key, std::vector<Rid>& result) const {
    result.clear();
    auto structure_guard = lock_shared();

    IxNodeHandle leaf;
    fetch_root_node_into(leaf);
    while (!leaf.is_leaf_page()) {
        int child_idx = leaf.lower_bound(key);
        if (child_idx >= leaf.get_size()) {
            child_idx = leaf.get_size() - 1;
        } else if (child_idx > 0 &&
                   ix_compare(leaf.get_key(child_idx), key, file_hdr_->col_types_, file_hdr_->col_lens_) > 0) {
            --child_idx;
        }
        page_id_t child_page_no = leaf.value_at(child_idx);
        unpin_if_not_cached_root(leaf.get_page_id());
        fetch_node_into(child_page_no, leaf);
    }

    while (true) {
        page_id_t next_leaf = IX_LEAF_HEADER_PAGE;
        bool stop = false;
        {
            std::shared_lock<std::shared_mutex> leaf_guard(leaf.page->latch());
            int slot = leaf.lower_bound(key);
            while (slot < leaf.get_size()) {
                const int cmp = ix_compare(leaf.get_key(slot), key, file_hdr_->col_types_, file_hdr_->col_lens_);
                if (cmp > 0) {
                    stop = true;
                    break;
                }
                if (cmp == 0) {
                    result.push_back(*leaf.get_rid(slot));
                }
                ++slot;
            }
            next_leaf = leaf.get_next_leaf();
            stop = stop || leaf.get_page_no() == file_hdr_->last_leaf_ || next_leaf == IX_LEAF_HEADER_PAGE;
        }

        unpin_if_not_cached_root(leaf.get_page_id());
        if (stop) {
            break;
        }
        fetch_node_into(next_leaf, leaf);
    }
}

UniqueLookupResult IxIndexHandle::lookup_unique(const char* key) const {
    auto structure_guard = lock_shared();

    IxNodeHandle leaf;
    fetch_root_node_into(leaf);
    while (!leaf.is_leaf_page()) {
        int child_idx = leaf.lower_bound(key);
        if (child_idx >= leaf.get_size()) {
            child_idx = leaf.get_size() - 1;
        } else if (child_idx > 0 &&
                   ix_compare(leaf.get_key(child_idx), key, file_hdr_->col_types_, file_hdr_->col_lens_) > 0) {
            --child_idx;
        }
        const page_id_t child_page_no = leaf.value_at(child_idx);
        unpin_if_not_cached_root(leaf.get_page_id());
        fetch_node_into(child_page_no, leaf);
    }

    UniqueLookupResult result{UniqueLookupStatus::NotFound, {}};
    bool duplicate = false;
    page_id_t next_leaf_page = IX_LEAF_HEADER_PAGE;
    {
        std::shared_lock<std::shared_mutex> leaf_guard(leaf.page->latch());
        const int slot = leaf.lower_bound(key);
        if (slot < leaf.get_size() &&
            ix_compare(leaf.get_key(slot), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0) {
            result.status = UniqueLookupStatus::Unique;
            result.rid = *leaf.get_rid(slot);
            if (slot + 1 < leaf.get_size()) {
                duplicate = ix_compare(leaf.get_key(slot + 1), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0;
            } else {
                next_leaf_page = leaf.get_next_leaf();
            }
        }
    }
    unpin_if_not_cached_root(leaf.get_page_id());
    if (result.status == UniqueLookupStatus::NotFound || duplicate || next_leaf_page == IX_LEAF_HEADER_PAGE) {
        if (duplicate) {
            result.status = UniqueLookupStatus::Duplicate;
        }
        return result;
    }

    fetch_node_into(next_leaf_page, leaf);
    {
        std::shared_lock<std::shared_mutex> next_leaf_guard(leaf.page->latch());
        if (leaf.get_size() > 0 && ix_compare(leaf.get_key(0), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0) {
            result.status = UniqueLookupStatus::Duplicate;
        }
    }
    unpin_if_not_cached_root(leaf.get_page_id());
    return result;
}

bool IxIndexHandle::refresh_leaf_chain_endpoint() {
    auto structure_guard = lock_exclusive();
    if (file_hdr_->root_page_ == IX_NO_PAGE) {
        return true;
    }
    if (file_hdr_->root_page_ < IX_INIT_ROOT_PAGE || file_hdr_->root_page_ >= file_hdr_->num_pages_) {
        return false;
    }

    // The endpoints are the leftmost and the rightmost leaf, so descend the two
    // edges of the tree instead of walking the leaf chain. That costs one page
    // read per level rather than one per leaf, and it still works when the chain
    // itself has a hole - which is what makes validate_structure's endpoint
    // check usable as a gate rather than a guaranteed failure after every crash.
    //
    // first_leaf_ is documented as immutable after index creation, but that only
    // holds while the leftmost leaf is never merged away; coalesce() can delete
    // it. Recomputing both endpoints costs one extra descent and removes the
    // difference between "the header is stale" and "the index must be rebuilt".
    page_id_t first_leaf = IX_NO_PAGE;
    page_id_t last_leaf = IX_NO_PAGE;
    if (!descend_leaf_chain_edge(false, &first_leaf) || !descend_leaf_chain_edge(true, &last_leaf)) {
        return false;
    }
    file_hdr_->first_leaf_ = first_leaf;
    file_hdr_->last_leaf_ = last_leaf;
    return true;
}

bool IxIndexHandle::descend_leaf_chain_edge(bool rightmost, page_id_t* leaf_page_no) const {
    page_id_t current = file_hdr_->root_page_;
    for (page_id_t level = 0; level <= file_hdr_->num_pages_; ++level) {
        IxNodeHandle node;
        try {
            fetch_node_into(current, node);
        } catch (...) {
            return false;
        }
        if (node.is_leaf_page()) {
            unpin_if_not_cached(node.get_page_id());
            *leaf_page_no = current;
            return true;
        }
        const int size = node.get_size();
        if (size <= 0 || size > node.get_max_size()) {
            unpin_if_not_cached(node.get_page_id());
            return false;
        }
        const page_id_t child = node.value_at(rightmost ? size - 1 : 0);
        unpin_if_not_cached(node.get_page_id());
        if (child < IX_INIT_ROOT_PAGE || child >= file_hdr_->num_pages_) {
            return false;
        }
        current = child;
    }
    return false;
}

bool IxIndexHandle::validate_structure() const {
    // Recovery decides from this result whether to repair the index key by key
    // or to rebuild it from the heap, so every rejection reports which
    // invariant broke. Without that an unexpected rebuild cannot be diagnosed
    // after the fact.
    const auto reject = [this](const char* reason, page_id_t page_no) {
        LOG_WARN("index %s failed structure validation: %s (page %d)", disk_manager_->get_file_name(fd_).c_str(),
                 reason, static_cast<int>(page_no));
        return false;
    };

    auto structure_guard = lock_shared();
    if (file_hdr_->num_pages_ < IX_INIT_NUM_PAGES) {
        return reject("page count below the initial layout", file_hdr_->num_pages_);
    }
    if (file_hdr_->root_page_ == IX_NO_PAGE) {
        return true;
    }
    if (file_hdr_->root_page_ < IX_INIT_ROOT_PAGE || file_hdr_->root_page_ >= file_hdr_->num_pages_) {
        return reject("root page out of range", file_hdr_->root_page_);
    }

    std::unordered_set<page_id_t> visited;
    std::unordered_set<page_id_t> leaves;
    std::function<bool(page_id_t, page_id_t)> visit = [&](page_id_t page_no, page_id_t expected_parent) {
        if (page_no < IX_INIT_ROOT_PAGE || page_no >= file_hdr_->num_pages_) {
            return reject("child page out of range", page_no);
        }
        if (!visited.insert(page_no).second) {
            return reject("page reachable more than once", page_no);
        }

        IxNodeHandle node;
        try {
            fetch_node_into(page_no, node);
        } catch (...) {
            return reject("page could not be read", page_no);
        }
        auto finish = [&](bool valid) {
            unpin_if_not_cached(node.get_page_id());
            return valid;
        };

        const int size = node.get_size();
        // An empty leaf is a legal steady state, not damage. delete_entry only
        // erases the key - coalesce_or_redistribute() is unreachable from the
        // delete path - so a leaf that has had all of its keys removed stays in
        // the tree and in the leaf chain. TPC-C reaches this on every index over
        // new_orders, whose leftmost leaf is emptied by Delivery taking
        // MIN(no_o_id), and rejecting it here used to send those indexes into a
        // full rebuild after every crash. Descent, lookup_equal() and the scans
        // all step past a zero-key leaf via its next_leaf link. An internal node
        // with no children is genuinely unusable and still rejected.
        const int min_size = node.is_leaf_page() ? 0 : 1;
        if (size < min_size || size > node.get_max_size()) {
            return finish(reject("node key count out of range", page_no));
        }
        if (node.get_parent_page_no() != expected_parent) {
            return finish(reject("parent back pointer does not match", page_no));
        }
        for (int i = 1; i < size; ++i) {
            if (ix_compare(node.get_key(i - 1), node.get_key(i), file_hdr_->col_types_, file_hdr_->col_lens_) > 0) {
                return finish(reject("keys out of order inside the page", page_no));
            }
        }

        if (node.is_leaf_page()) {
            leaves.insert(page_no);
            const page_id_t previous = node.get_prev_leaf();
            const page_id_t next = node.get_next_leaf();
            const bool previous_valid =
                previous == IX_LEAF_HEADER_PAGE || (previous >= IX_INIT_ROOT_PAGE && previous < file_hdr_->num_pages_);
            const bool next_valid =
                next == IX_LEAF_HEADER_PAGE || (next >= IX_INIT_ROOT_PAGE && next < file_hdr_->num_pages_);
            if (!previous_valid || !next_valid) {
                return finish(reject("leaf link out of range", page_no));
            }
            return finish(true);
        }

        std::vector<page_id_t> children;
        children.reserve(static_cast<std::size_t>(size));
        for (int i = 0; i < size; ++i) {
            children.push_back(node.value_at(i));
        }
        finish(true);
        for (const page_id_t child : children) {
            if (!visit(child, page_no)) {
                return false;
            }
        }
        return true;
    };

    try {
        if (!visit(file_hdr_->root_page_, IX_NO_PAGE)) {
            return false;
        }
        if (file_hdr_->first_leaf_ == IX_NO_PAGE || file_hdr_->last_leaf_ == IX_NO_PAGE) {
            return reject("leaf chain endpoints unset", file_hdr_->first_leaf_);
        }

        std::unordered_set<page_id_t> chain;
        page_id_t previous = IX_LEAF_HEADER_PAGE;
        page_id_t current = file_hdr_->first_leaf_;
        while (current != IX_LEAF_HEADER_PAGE) {
            if (current < IX_INIT_ROOT_PAGE || current >= file_hdr_->num_pages_) {
                return reject("leaf chain leaves the file", current);
            }
            if (!leaves.count(current)) {
                return reject("leaf chain names a page the tree walk did not reach", current);
            }
            if (!chain.insert(current).second) {
                return reject("leaf chain contains a cycle", current);
            }
            IxNodeHandle leaf;
            fetch_node_into(current, leaf);
            if (!leaf.is_leaf_page() || leaf.get_prev_leaf() != previous) {
                unpin_if_not_cached(leaf.get_page_id());
                return reject("leaf back link does not match the chain", current);
            }
            previous = current;
            current = leaf.get_next_leaf();
            unpin_if_not_cached(leaf.get_page_id());
        }
        if (chain != leaves) {
            return reject("tree walk and leaf chain disagree on the leaf set", file_hdr_->first_leaf_);
        }
        if (previous != file_hdr_->last_leaf_) {
            return reject("leaf chain does not end at last_leaf_", previous);
        }
        return true;
    } catch (...) {
        return reject("validation threw while reading a page", file_hdr_->root_page_);
    }
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle node;
    fetch_node_into(file_hdr_->last_leaf_, node);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node.get_size()};
    unpin_if_not_cached(node.get_page_id());
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle* IxIndexHandle::fetch_node(int page_no) const {
    note_smo_page(page_no);
    Page* page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    IxNodeHandle* node = new IxNodeHandle(file_hdr_.get(), page);

    return node;
}

void IxIndexHandle::fetch_node_into(int page_no, IxNodeHandle& out) const {
    note_smo_page(page_no);
    Page* page = cached_page(page_no);
    if (page == nullptr) {
        page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    }
    if (page == nullptr) {
        throw InternalError("failed to fetch index page");
    }
    out.file_hdr = file_hdr_.get();
    out.page = page;
    out.page_hdr = reinterpret_cast<IxPageHdr*>(page->get_data());
    out.keys = page->get_data() + sizeof(IxPageHdr);
    out.rids = reinterpret_cast<Rid*>(out.keys + file_hdr_->keys_size_);
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle* IxIndexHandle::create_node() {
    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page* page = buffer_pool_manager_->new_page(&new_page_id);
    if (page == nullptr) {
        throw InternalError("failed to allocate index page");
    }
    file_hdr_->num_pages_ = std::max(file_hdr_->num_pages_, new_page_id.page_no + 1);
    // A page that has only been allocated occupies no bytes in the file yet, so
    // it reads back as a hole full of zeros. Publishing it with the rest of the
    // SMO is the only thing that stops a parent from pointing at one.
    note_smo_page(new_page_id.page_no);
    return new IxNodeHandle(file_hdr_.get(), page);
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::maintain_parent(IxNodeHandle* node) {
    // Two alternating stack slots avoid heap alloc per parent fetch.
    IxNodeHandle slot_a;
    IxNodeHandle slot_b;
    slot_a.file_hdr = node->file_hdr;
    slot_a.page = node->page;
    slot_a.page_hdr = node->page_hdr;
    slot_a.keys = node->keys;
    slot_a.rids = node->rids;
    IxNodeHandle* curr = &slot_a;
    IxNodeHandle* next = &slot_b;
    while (curr->get_parent_page_no() != IX_NO_PAGE) {
        fetch_node_into(curr->get_parent_page_no(), *next);
        bool changed = false;
        {
            std::unique_lock parent_guard{next->page->latch()};
            int rank = next->find_child(curr);
            char* parent_key = next->get_key(rank);
            char* child_first_key = curr->get_key(0);
            if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) != 0) {
                // A separator key is as much a part of the tree's shape as a child
                // pointer. Publish the dependency and epoch under the same page
                // latch as the image change so a concurrent flush cannot observe
                // the new separator with the old WAL dependency.
                BufferPoolManager::mark_dirty(next->page, active_smo_wal_context().dependency());
                topology_epoch_.fetch_add(1, std::memory_order_relaxed);
                note_structure_change();
                memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);
                changed = true;
            }
        }
        if (!changed) {
            unpin_if_not_cached(next->get_page_id());
            break;
        }
        unpin_if_not_cached(next->get_page_id());
        std::swap(curr, next);
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle* leaf) {
    assert(leaf->is_leaf_page());
    note_structure_change();

    IxNodeHandle prev;
    fetch_node_into(leaf->get_prev_leaf(), prev);
    prev.set_next_leaf(leaf->get_next_leaf());
    unpin_dirty_if_not_cached(prev.get_page_id(), active_smo_wal_context());

    IxNodeHandle next;
    fetch_node_into(leaf->get_next_leaf(), next);
    next.set_prev_leaf(leaf->get_prev_leaf()); // 注意此处是SetPrevLeaf()
    unpin_dirty_if_not_cached(next.get_page_id(), active_smo_wal_context());
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle& node) {
    if (!node.is_leaf_page()) {
        drop_cached_internal_page(node.get_page_no());
        buffer_pool_manager_->unmark_resident(node.get_page_id());
        resident_internal_pages_.erase(node.get_page_no());
        // Cached pages normally have no BPM pin. This also releases the
        // creation pin if a just-created internal page is merged before its
        // caller's normal cleanup path runs.
        buffer_pool_manager_->unpin_page(node.get_page_id(), false);
    }
    file_hdr_->num_pages_--;
}

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle* node, int child_idx) {
    if (!node->is_leaf_page()) {
        // An internal split re-parents up to fanout/2 children this way, which is
        // why an SMO's dirty set is much larger than {leaf, sibling, parent} and
        // why it has to be collected rather than enumerated by hand.
        note_structure_change();
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle child;
        fetch_node_into(child_page_no, child);
        child.set_parent_page_no(node->get_page_no());
        unpin_dirty_if_not_cached(child.get_page_id(), active_smo_wal_context());
    }
}
