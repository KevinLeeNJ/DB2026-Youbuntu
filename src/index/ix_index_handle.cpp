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

#include <algorithm>
#include <functional>

#include "ix_scan.h"
#include "minilog.h"

namespace {

std::atomic<uint64_t> g_smo_publish_count{0};
std::atomic<uint64_t> g_smo_pages_written{0};

// One line roughly every few seconds of TPC-C, so the SMO rate and the pages
// per SMO can be read straight out of the server log after a benchmark run
// without adding anything to the per-row path.
constexpr uint64_t kSmoLogInterval = 4096;

// Opt-in, and emitted at WARN because the server runs at WARN outside recovery
// (rmdb.cpp sets the level) - an INFO line here is simply discarded.
bool SmoStatsLoggingEnabled() {
    static const bool enabled = [] {
        const char* value = std::getenv("RMDB_LOG_INDEX_SMO_STATS");
        return value != nullptr && std::string(value) == "1";
    }();
    return enabled;
}

} // namespace

std::atomic<bool> IxIndexHandle::smo_flush_enabled_{[] {
    const char* value = std::getenv("ENABLE_INDEX_SMO_FLUSH");
    return value == nullptr || std::string(value) != "0";
}()};

uint64_t IxIndexHandle::smo_publish_count() {
    return g_smo_publish_count.load(std::memory_order_relaxed);
}

uint64_t IxIndexHandle::smo_pages_written() {
    return g_smo_pages_written.load(std::memory_order_relaxed);
}

/**
 * @brief Publish one structure-modification operation's pages to the index file.
 *
 * Called by ~SmoScope, i.e. still under the structure-exclusive latch, so no
 * other index thread can read a half-published tree and nothing else can dirty
 * these pages while they are being written.
 *
 * The index header page goes out last and unconditionally. It is what carries
 * root_page_, num_pages_, first_leaf_ and last_leaf_, none of which live in a
 * tree page, and without it a root split would publish a new root that no
 * persisted header names. Writing it last is the better of two imperfect
 * orders: if the header is lost, the file still describes the previous tree
 * shape, whereas a header written first can name a page that was never written
 * at all - i.e. a hole that reads back as zeros.
 *
 * These are plain pwrites with no fsync, which is sound for the crash model the
 * finals specify (SIGKILL plus a restart of the same machine, final.md:349):
 * SIGKILL does not discard the kernel page cache. The durability audit only
 * scores objects in the WAL namespace (final.md:332-335), so index page writes
 * are neither counted nor penalised.
 *
 * What this does NOT give: atomicity *between* the pwrites. A SIGKILL landing
 * between two of them still leaves part of the set on disk. The exposure shrinks
 * from "however long the buffer pool takes to evict the rest", i.e. seconds to
 * minutes, to the handful of microseconds a few write syscalls take - six to
 * seven orders of magnitude - but that is a reduction, not a proof. Closing it
 * from construction requires making the SMO physically idempotent through the
 * log (an IX_SMO full-page-image record, which is what InnoDB does) at a cost
 * of roughly +36% WAL volume.
 */
void IxIndexHandle::publish_smo_pages() const noexcept {
    if (!smo_structural_ || smo_pages_.empty() || !smo_flush_enabled()) {
        return;
    }

    // ~SmoScope may run while an exception (IndexEntryExistsError, for one) is
    // already propagating, so nothing here may escape. Failing to publish is a
    // degradation, not damage: the pages stay dirty in the buffer pool and reach
    // disk the old way, on eviction.
    try {
        publish_smo_pages_impl();
    } catch (const std::exception& error) {
        LOG_WARN("index %s could not publish its structure change: %s", disk_manager_->get_file_name(fd_).c_str(),
                 error.what());
    } catch (...) {
        LOG_WARN("index %s could not publish its structure change", disk_manager_->get_file_name(fd_).c_str());
    }
}

void IxIndexHandle::publish_smo_pages_impl() const {
    std::sort(smo_pages_.begin(), smo_pages_.end());
    smo_pages_.erase(std::unique(smo_pages_.begin(), smo_pages_.end()), smo_pages_.end());

    smo_flush_batch_.clear();
    smo_flush_batch_.reserve(smo_pages_.size());
    for (const page_id_t page_no : smo_pages_) {
        if (page_no != IX_NO_PAGE) {
            smo_flush_batch_.push_back(PageId{fd_, page_no});
        }
    }

    // wal_preflushed: index pages have no page LSN, so there is no log record to
    // wait for - see BufferPoolManager::flush_pages().
    const auto flushed = buffer_pool_manager_->flush_pages(smo_flush_batch_, /*wal_preflushed=*/true);
    write_index_header_page();

    const uint64_t published = g_smo_publish_count.fetch_add(1, std::memory_order_relaxed) + 1;
    g_smo_pages_written.fetch_add(flushed.pages_written, std::memory_order_relaxed);
    if (published % kSmoLogInterval == 0 && SmoStatsLoggingEnabled()) {
        LOG_WARN("index SMO write-out stats: %lu operations, %lu page images", static_cast<unsigned long>(published),
                 static_cast<unsigned long>(smo_pages_written()));
        // The logger buffers 1 MiB and a benchmark run ends in SIGKILL, so a line
        // that is not flushed is a line that never existed. One fflush per
        // kSmoLogInterval operations - tens of seconds apart - is what makes this
        // counter readable at all.
        minilog::Logger::get().flush();
    }
}

void IxIndexHandle::write_index_header_page() const {
    if (header_image_.size() != static_cast<size_t>(file_hdr_->tot_len_)) {
        header_image_.assign(static_cast<size_t>(file_hdr_->tot_len_), 0);
    }
    file_hdr_->serialize(header_image_.data());
    disk_manager_->write_page(fd_, IX_FILE_HDR_PAGE, header_image_.data(), file_hdr_->tot_len_);
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
    // num_pages_ lives only in this in-memory header: create_node() bumps it
    // without logging, and the header reaches disk only through the checkpoint
    // path. After a crash it can therefore name fewer pages than the file
    // actually holds, and trusting it would make create_node() hand out page
    // numbers that live nodes already occupy - recovery would overwrite a live
    // subtree with a fresh node. The file length is a safe upper bound: a page
    // number that has ever been written is inside the file, so starting after
    // the last written page can never reuse one.
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
    if (include_internal && internal_page_cache_enabled()) {
        register_internal_pages();
    }
}
void IxIndexHandle::refresh_root_page_cache() {
    if (root_cache_enabled() && (cached_root_page_ == nullptr || cached_root_page_no_ != file_hdr_->root_page_)) {
        Page* new_root = buffer_pool_manager_->fetch_page(PageId{fd_, file_hdr_->root_page_});
        if (new_root == nullptr) {
            throw InternalError("failed to fetch index root page");
        }
        Page* old_root = cached_root_page_;
        PageId old_root_id{fd_, cached_root_page_no_};
        cached_root_page_ = new_root;
        cached_root_page_no_ = file_hdr_->root_page_;
        if (old_root != nullptr && old_root_id.page_no != IX_NO_PAGE) {
            if (resident_internal_pages_.erase(old_root_id.page_no) != 0) {
                buffer_pool_manager_->unmark_resident(old_root_id);
            }
            drop_cached_internal_page(old_root_id.page_no);
            buffer_pool_manager_->unpin_page(old_root_id, false);
        }
    }
    if (cached_root_page_ != nullptr) {
        IxNodeHandle root(file_hdr_.get(), cached_root_page_);
        if (!root.is_leaf_page()) {
            // Keep the root classified as an internal page without walking
            // the entire tree. Full internal residency is still established
            // by an explicit refresh_page_residency(true).
            mark_internal_page_resident(file_hdr_->root_page_, cached_root_page_);
        }
    }
}

void IxIndexHandle::mark_internal_page_resident(page_id_t page_no, Page* page) {
    if (resident_internal_pages_.insert(page_no).second) {
        buffer_pool_manager_->mark_resident(PageId{fd_, page_no}, ResidencyClass::IndexInternal);
    }
    cache_internal_page(page_no, page);
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
void IxIndexHandle::cache_internal_page(page_id_t page_no, Page* page) {
    if (!internal_page_cache_enabled() || page_no == file_hdr_->root_page_ || page_no == IX_NO_PAGE) {
        return;
    }
    auto [it, inserted] = cached_internal_pages_.try_emplace(page_no, page);
    if (!inserted) {
        // The pin for this page is already held; only the frame may have moved.
        it->second = page;
        return;
    }
    Page* pinned = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    if (pinned == nullptr) {
        cached_internal_pages_.erase(it);
        return;
    }
    it->second = pinned;
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
    if (cached_root_page_ == nullptr || cached_root_page_no_ == IX_NO_PAGE) {
        return;
    }
    buffer_pool_manager_->unpin_page(PageId{fd_, cached_root_page_no_}, false);
    cached_root_page_ = nullptr;
    cached_root_page_no_ = IX_NO_PAGE;
}

void IxIndexHandle::register_internal_pages() {
    // Recovery can rewrite a page in place, replace the root, or rebuild the
    // whole index. Drop all old raw pointers before walking the repaired tree.
    unregister_internal_pages();

    std::unordered_set<page_id_t> reachable;
    std::vector<page_id_t> pending;
    std::unordered_set<page_id_t> visited;
    reachable.reserve(static_cast<size_t>(std::max(file_hdr_->num_pages_, 1)));
    visited.reserve(reachable.size());
    pending.reserve(reachable.size());
    if (file_hdr_->root_page_ != IX_NO_PAGE) {
        pending.push_back(file_hdr_->root_page_);
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
                reachable.insert(page_no);
                resident_internal_pages_.insert(page_no);
                buffer_pool_manager_->mark_resident(PageId{fd_, page_no}, ResidencyClass::IndexInternal);
                cache_internal_page(page_no, page);
                for (int child_idx = 0; child_idx < node.get_size(); ++child_idx) {
                    pending.push_back(node.value_at(child_idx));
                }
            }
            if (!cached_root) {
                buffer_pool_manager_->unpin_page(PageId{fd_, page_no}, false);
            }
        }
    } catch (...) {
        unregister_internal_pages();
        throw;
    }

    resident_internal_pages_ = std::move(reachable);
}

void IxIndexHandle::unregister_internal_pages() const {
    for (const page_id_t page_no : resident_internal_pages_) {
        buffer_pool_manager_->unmark_resident(PageId{fd_, page_no});
    }
    resident_internal_pages_.clear();
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
            unpin_if_not_cached(next_leaf.get_page_id(), true);
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
                                       Transaction* transaction) {
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
        unpin_created_page(new_root->get_page_id());
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
        insert_into_parent(&parent, new_parent->get_key(0), new_parent, transaction);
        unpin_created_page(new_parent->get_page_id());
        delete new_parent;
    }
    unpin_if_not_cached(parent.get_page_id(), true);
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
 */
page_id_t IxIndexHandle::insert_entry(const char* key, const Rid& value, Transaction* transaction,
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
            leaf.insert_pair(pos, key, value);
            if (pos == leaf.get_size() - 1 && leaf.get_next_leaf() == IX_LEAF_HEADER_PAGE) {
                remember_append_hint(inserted_page_no);
            }
            leaf_guard.unlock();
            unpin_if_not_cached(leaf.get_page_id(), true);
            return inserted_page_no;
        }

        leaf_guard.unlock();
        unpin_if_not_cached(leaf.get_page_id());
    }

    auto structure_guard = lock_exclusive();
    return insert_entry_unlocked(key, value, transaction, allow_duplicate);
}

page_id_t IxIndexHandle::insert_entry_unlocked(const char* key, const Rid& value, Transaction* transaction,
                                               bool allow_duplicate) {
    SmoScope smo(this);
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
    leaf.insert_pair(pos, key, value);
    page_id_t inserted_page_no = leaf.get_page_no();
    bool did_split = false;
    if (leaf.get_size() >= leaf.get_max_size()) {
        did_split = true;
        IxNodeHandle* new_leaf = split(&leaf, right_edge_append);
        if (file_hdr_->last_leaf_ == leaf.get_page_no()) {
            file_hdr_->last_leaf_ = new_leaf->get_page_no();
        }
        insert_into_parent(&leaf, new_leaf->get_key(0), new_leaf, transaction);
        if (right_edge_append) {
            remember_append_hint(new_leaf->get_page_no());
        }
        unpin_created_page(new_leaf->get_page_id());
        delete new_leaf;
    }

    if (right_edge_append && !did_split) {
        remember_append_hint(leaf.get_page_no());
    }

    if (pos == 0) {
        maintain_parent(&leaf);
    }
    unpin_if_not_cached(leaf.get_page_id(), true);
    return inserted_page_no;
}

IxIndexHandle::PinnedInserter::PinnedInserter(IxIndexHandle* h) : ih(h), latch(h->lock_exclusive()) {
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
        ih->unpin_if_not_cached(leaf.get_page_id(), true);
    }
}

void IxIndexHandle::PinnedInserter::insert(const char* key, const Rid& value, Transaction* txn, bool allow_duplicate) {
    SmoScope smo(ih);
    // This object holds the leaf pinned across rows, so unlike the other two
    // entry points it does not necessarily re-fetch it inside the scope. Record
    // it explicitly - it is the one page note_smo_page()'s acquisition funnel
    // cannot see.
    ih->note_smo_page(leaf.get_page_no());

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
            ih->unpin_if_not_cached(leaf.get_page_id(), true);
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
    leaf.insert_pair(pos, key, value);
    if (leaf.get_size() >= leaf.get_max_size()) {
        IxNodeHandle* new_leaf = ih->split(&leaf, right_edge_append);
        if (ih->file_hdr_->last_leaf_ == leaf.get_page_no()) {
            ih->file_hdr_->last_leaf_ = new_leaf->get_page_no();
        }
        ih->insert_into_parent(&leaf, new_leaf->get_key(0), new_leaf, txn);
        page_id_t new_leaf_page_no = new_leaf->get_page_no();
        if (right_edge_append) {
            ih->remember_append_hint(new_leaf_page_no);
        }
        ih->unpin_created_page(new_leaf->get_page_id());
        delete new_leaf;

        // Reposition to the right leaf after split.
        active = false;
        ih->unpin_if_not_cached(leaf.get_page_id(), true);
        ih->fetch_node_into(new_leaf_page_no, leaf);
        active = true;
    } else if (right_edge_append) {
        ih->remember_append_hint(leaf.get_page_no());
    }

    if (pos == 0) {
        ih->maintain_parent(&leaf);
    }
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 */
bool IxIndexHandle::delete_entry(const char* key, Transaction* transaction) {
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
            leaf.erase_pair(pos);
            leaf_guard.unlock();
            unpin_if_not_cached(leaf.get_page_id(), true);
            return true;
        }

        leaf_guard.unlock();
        unpin_if_not_cached(leaf.get_page_id());
    }

    auto structure_guard = lock_exclusive();
    return delete_entry_unlocked(key, transaction);
}

bool IxIndexHandle::delete_entry_unlocked(const char* key, Transaction* transaction) {
    SmoScope smo(this);
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::DELETE, transaction);
    int old_size = leaf->get_size();
    int pos = leaf->lower_bound(key);
    if (pos >= old_size || ix_compare(leaf->get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) != 0) {
        unpin_if_not_cached(leaf->get_page_id());
        delete leaf;
        return false;
    }

    leaf->erase_pair(pos);
    if (leaf->get_size() > 0) {
        maintain_parent(leaf);
    }
    unpin_if_not_cached(leaf->get_page_id(), true);
    delete leaf;
    return true;
}

bool IxIndexHandle::delete_entry(const char* key, const Rid& value, Transaction* transaction) {
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
                        leaf.erase_pair(pos);
                        leaf_guard.unlock();
                        unpin_if_not_cached(leaf.get_page_id(), true);
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
    return delete_entry_unlocked(key, value, transaction);
}

bool IxIndexHandle::delete_entry_unlocked(const char* key, const Rid& value, Transaction* transaction) {
    (void)transaction;
    SmoScope smo(this);
    Iid lower = lower_bound(key);
    Iid upper = upper_bound(key);
    IxScan scan(this, lower, upper, buffer_pool_manager_, false);
    while (!scan.is_end()) {
        Iid iid = scan.iid();
        if (scan.rid() == value) {
            IxNodeHandle leaf;
            fetch_node_into(iid.page_no, leaf);
            leaf.erase_pair(iid.slot_no);
            if (leaf.get_size() > 0) {
                maintain_parent(&leaf);
            }
            unpin_if_not_cached(leaf.get_page_id(), true);
            return true;
        }
        scan.next();
    }
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
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle* node, Transaction* transaction, bool* root_is_latched) {
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
        deleted = coalesce(&neighbor, &node, &parent, index, transaction, root_is_latched);
    }

    unpin_if_not_cached(neighbor->get_page_id(), true);
    unpin_if_not_cached(parent->get_page_id(), true);
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
        IxNodeHandle child;
        fetch_node_into(child_page_no, child);
        child.set_parent_page_no(IX_NO_PAGE);
        update_root_page_no(child_page_no);
        refresh_root_page_cache();
        unpin_if_not_cached(child.get_page_id(), true);
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
                             Transaction* transaction, bool* root_is_latched) {
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
    return coalesce_or_redistribute(*parent, transaction, root_is_latched);
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
    IxNodeHandle* node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page* page = buffer_pool_manager_->new_page(&new_page_id);
    // A page that has only been allocated occupies no bytes in the file yet, so
    // it reads back as a hole full of zeros. Publishing it with the rest of the
    // SMO is the only thing that stops a parent from pointing at one.
    note_smo_page(new_page_id.page_no);
    node = new IxNodeHandle(file_hdr_.get(), page);
    return node;
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
        int rank = next->find_child(curr);
        char* parent_key = next->get_key(rank);
        char* child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
            unpin_if_not_cached(next->get_page_id(), true);
            break;
        }
        // A separator key is as much a part of the tree's shape as a child
        // pointer: if the new separator reaches disk without the leaf that
        // justifies it (or the other way round), a range of keys stops being
        // reachable by descent even though every page still validates.
        topology_epoch_.fetch_add(1, std::memory_order_relaxed);
        note_structure_change();
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);
        unpin_if_not_cached(next->get_page_id(), true);
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
    unpin_if_not_cached(prev.get_page_id(), true);

    IxNodeHandle next;
    fetch_node_into(leaf->get_next_leaf(), next);
    next.set_prev_leaf(leaf->get_prev_leaf()); // 注意此处是SetPrevLeaf()
    unpin_if_not_cached(next.get_page_id(), true);
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
        unpin_if_not_cached(child.get_page_id(), true);
    }
}
