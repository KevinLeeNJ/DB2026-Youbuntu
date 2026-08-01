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

#include "ix_defs.h"
#include "transaction/transaction.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

enum class Operation { FIND = 0, INSERT, DELETE }; // 三种操作：查找、插入、删除

enum class UniqueLookupStatus {
    NotFound,
    Unique,
    Duplicate,
};

struct UniqueLookupResult {
    UniqueLookupStatus status;
    // Valid only when status == Unique. Duplicate callers must use
    // lookup_equal() to obtain all matching RIDs; NotFound is definitive.
    Rid rid;
};

inline int ix_compare(const char* a, const char* b, ColType type, int col_len) {
    switch (type) {
    case TYPE_INT: {
        int ia;
        int ib;
        memcpy(&ia, a, sizeof(ia));
        memcpy(&ib, b, sizeof(ib));
        return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
    }
    case TYPE_FLOAT: {
        float fa;
        float fb;
        memcpy(&fa, a, sizeof(fa));
        memcpy(&fb, b, sizeof(fb));
        return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
    }
    case TYPE_STRING:
    case TYPE_DATETIME:
        return memcmp(a, b, col_len);
    default:
        throw InternalError("Unexpected data type");
    }
}

inline int ix_compare(const char* a, const char* b, const std::vector<ColType>& col_types,
                      const std::vector<int>& col_lens) {
    int offset = 0;
    for (size_t i = 0; i < col_types.size(); ++i) {
        int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        if (res != 0)
            return res;
        offset += col_lens[i];
    }
    return 0;
}

/* 管理B+树中的每个节点 */
class IxNodeHandle {
    friend class IxIndexHandle;
    friend class IxScan;

private:
    const IxFileHdr* file_hdr; // 节点所在文件的头部信息
    Page* page;                // 存储节点的页面
    IxPageHdr* page_hdr;       // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
    char* keys; // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
    Rid* rids;  // page->data的第三部分，指针指向首地址

public:
    IxNodeHandle() = default;

    IxNodeHandle(const IxFileHdr* file_hdr_, Page* page_) : file_hdr(file_hdr_), page(page_) {
        page_hdr = reinterpret_cast<IxPageHdr*>(page->get_data());
        keys = page->get_data() + sizeof(IxPageHdr);
        rids = reinterpret_cast<Rid*>(keys + file_hdr->keys_size_);
    }

    int get_size() {
        return page_hdr->num_key;
    }

    void set_size(int size) {
        page_hdr->num_key = size;
    }

    int get_max_size() {
        return file_hdr->btree_order_ + 1;
    }

    int get_min_size() {
        return get_max_size() / 2;
    }

    int key_at(int i) {
        return read_unaligned<int>(get_key(i));
    }

    /* 得到第i个孩子结点的page_no */
    page_id_t value_at(int i) {
        return get_rid(i)->page_no;
    }

    page_id_t get_page_no() {
        return page->get_page_id().page_no;
    }

    PageId get_page_id() {
        return page->get_page_id();
    }

    page_id_t get_next_leaf() {
        return page_hdr->next_leaf;
    }

    page_id_t get_prev_leaf() {
        return page_hdr->prev_leaf;
    }

    page_id_t get_parent_page_no() {
        return page_hdr->parent;
    }

    bool is_leaf_page() {
        return page_hdr->is_leaf;
    }

    bool is_root_page() {
        return get_parent_page_no() == INVALID_PAGE_ID;
    }

    void set_next_leaf(page_id_t page_no) {
        page_hdr->next_leaf = page_no;
    }

    void set_prev_leaf(page_id_t page_no) {
        page_hdr->prev_leaf = page_no;
    }

    void set_parent_page_no(page_id_t parent) {
        page_hdr->parent = parent;
    }

    char* get_key(int key_idx) const {
        return keys + key_idx * file_hdr->col_tot_len_;
    }

    Rid* get_rid(int rid_idx) const {
        return &rids[rid_idx];
    }

    void set_key(int key_idx, const char* key) {
        memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_);
    }

    void set_rid(int rid_idx, const Rid& rid) {
        rids[rid_idx] = rid;
    }

    int lower_bound(const char* target) const;

    int upper_bound(const char* target) const;

    void insert_pairs(int pos, const char* key, const Rid* rid, int n);

    page_id_t internal_lookup(const char* key);

    bool leaf_lookup(const char* key, Rid** value);

    int insert(const char* key, const Rid& value);

    // 用于在结点中的指定位置插入单个键值对
    void insert_pair(int pos, const char* key, const Rid& rid) {
        insert_pairs(pos, key, &rid, 1);
    }

    void erase_pair(int pos);

    int remove(const char* key);

    /**
     * @brief used in internal node to remove the last key in root node, and return the last child
     *
     * @return the last child
     */
    page_id_t remove_and_return_only_child() {
        assert(get_size() == 1);
        page_id_t child_page_no = value_at(0);
        erase_pair(0);
        assert(get_size() == 0);
        return child_page_no;
    }

    /**
     * @brief 由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
     * @param child
     * @return int
     */
    int find_child(IxNodeHandle* child) {
        int rid_idx;
        for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++) {
            if (get_rid(rid_idx)->page_no == child->get_page_no()) {
                break;
            }
        }
        if (rid_idx >= page_hdr->num_key) {
            throw InternalError("index parent does not reference child");
        }
        return rid_idx;
    }
};

/* B+树 */
class IxIndexHandle {
    friend class IxScan;
    friend class IxManager;

private:
    DiskManager* disk_manager_;
    BufferPoolManager* buffer_pool_manager_;
    int fd_; // 存储B+树的文件
    std::unique_ptr<IxFileHdr>
        file_hdr_; // 存了root_page，但其初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）
    mutable std::shared_mutex index_latch_;
    std::atomic<uint64_t> topology_epoch_{0};
    mutable Page* cached_root_page_{nullptr};
    mutable page_id_t cached_root_page_no_{IX_NO_PAGE};

    // Right-edge append hint. try_append_hint() can only ever accept the single
    // rightmost leaf, so one slot carries the whole benefit; packing the page
    // number together with the topology epoch into one word keeps the hottest
    // insert path free of both a mutex and a per-row key allocation. A stale
    // slot is harmless: try_append_hint() re-validates the leaf it names.
    static constexpr uint64_t kNoAppendHint = ~uint64_t{0};
    mutable std::atomic<uint64_t> append_hint_{kNoAppendHint};

    static uint64_t pack_append_hint(page_id_t page_no, uint64_t topology_epoch) {
        return (static_cast<uint64_t>(static_cast<uint32_t>(topology_epoch)) << 32) |
               static_cast<uint64_t>(static_cast<uint32_t>(page_no));
    }

public:
    using SharedIndexLatch = std::shared_lock<std::shared_mutex>;
    using UniqueIndexLatch = std::unique_lock<std::shared_mutex>;

    IxIndexHandle(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, int fd);
    ~IxIndexHandle();

    int GetFd() const {
        return fd_;
    }

    SharedIndexLatch lock_shared() const {
        return SharedIndexLatch(index_latch_);
    }

    UniqueIndexLatch lock_exclusive() const {
        return UniqueIndexLatch(index_latch_);
    }

    // for search
    bool get_value(const char* key, std::vector<Rid>* result, Transaction* transaction);

    std::pair<IxNodeHandle*, bool> find_leaf_page(const char* key, Operation operation, Transaction* transaction,
                                                  bool find_first = false);

    // for insert
    page_id_t insert_entry(const char* key, const Rid& value, const IndexWriteWalContext& wal_context,
                           bool allow_duplicate = false);

    // Variants for callers that already hold the index exclusive latch.  Keeping
    // the mutation and its visibility bookkeeping under one latch closes the
    // historical-key reader/writer window.
    page_id_t insert_entry_unlocked(const char* key, const Rid& value, const IndexWriteWalContext& wal_context,
                                    bool allow_duplicate = false);

    // Bulk-load batch insert: pins leaf across rows to skip root→leaf walk.
    struct PinnedInserter {
        IxIndexHandle* ih;
        UniqueIndexLatch latch;
        IxNodeHandle leaf;
        IndexWriteWalContext wal_context;
        bool active = false;

        PinnedInserter(IxIndexHandle* h, const IndexWriteWalContext& wal_context);
        ~PinnedInserter();
        PinnedInserter(const PinnedInserter&) = delete;
        PinnedInserter& operator=(const PinnedInserter&) = delete;

        void insert(const char* key, const Rid& value, bool allow_duplicate = false);
    };

    // A structure-modification operation (SMO) - a split, a new root, or a
    // separator-key update - dirties several pages at once, but the buffer pool
    // evicts each of them independently and at a moment of its own choosing. A
    // crash between two such evictions leaves a tree on disk in which, say, a
    // parent already names a child page that was never written; recovery then
    // has to rebuild the whole index, which blows the 90 s readiness budget on
    // a multi-GB index. Index pages carry no page LSN (IxPageHdr occupies
    // Page::OFFSET_LSN), so WAL redo cannot repair them either.
    //
    // SmoScope closes that gap: while it is alive it records every page this
    // operation touched. Logged runtime changes append a complete redo image and
    // attach its LSN as the write dependency of every page and the header.
    // Unlogged origins publish all pages plus the header immediately.
    class SmoScope {
    public:
        SmoScope(const IxIndexHandle* index_handle, const IndexWriteWalContext& wal_context)
            : index_handle_(index_handle) {
            // SMOs never nest: the three entry points that open a scope
            // (insert_entry_unlocked, PinnedInserter::insert,
            // delete_entry_unlocked) never call each other.
            assert(!index_handle_->smo_active_);
            index_handle_->smo_pages_.clear();
            index_handle_->smo_structural_ = false;
            index_handle_->smo_wal_context_ = wal_context;
            index_handle_->smo_active_ = true;
            const auto origin = wal_context.origin();
            wal_barrier_active_ = origin == IndexWriteWalContext::Origin::LoggedRuntime ||
                                  origin == IndexWriteWalContext::Origin::LiveRollback;
            if (wal_barrier_active_) {
                index_handle_->buffer_pool_manager_->begin_index_smo(index_handle_->fd_);
            }
        }

        ~SmoScope() noexcept {
            index_handle_->smo_active_ = false;
            if (wal_barrier_active_ && index_handle_->smo_structural_) {
                std::fprintf(stderr,
                             "FATAL: logged index structure modification unwound before durable INDEX_SMO publish\n");
                std::fflush(stderr);
                std::_Exit(134);
            }
            if (wal_barrier_active_ && !fail_closed_) {
                index_handle_->buffer_pool_manager_->end_index_smo(index_handle_->fd_);
            }
            index_handle_->smo_pages_.clear();
            index_handle_->smo_structural_ = false;
            index_handle_->smo_wal_context_.reset();
        }

        void commit() {
            frame_operation_.reset();
            try {
                bool barrier_released = false;
                index_handle_->publish_smo_pages_impl(wal_barrier_active_ ? &barrier_released : nullptr);
                if (barrier_released) {
                    wal_barrier_active_ = false;
                }
                if (wal_barrier_active_) {
                    index_handle_->buffer_pool_manager_->end_index_smo(index_handle_->fd_);
                    wal_barrier_active_ = false;
                }
            } catch (...) {
                // A dirty SMO page without durable redo must never become
                // writeable. Keep the per-file barrier installed and force
                // the caller to fail the operation/process.
                fail_closed_ = wal_barrier_active_;
                throw;
            }
        }

        void begin_frame_operation() {
            assert(!frame_operation_.has_value());
            frame_operation_.emplace(index_handle_->buffer_pool_manager_->acquire_frame_operation(2));
        }

        const BufferPoolManager::FrameOperationToken& frame_operation() const {
            assert(frame_operation_.has_value());
            return *frame_operation_;
        }

        SmoScope(const SmoScope&) = delete;
        SmoScope& operator=(const SmoScope&) = delete;

    private:
        const IxIndexHandle* index_handle_;
        bool wal_barrier_active_{false};
        bool fail_closed_{false};
        std::optional<BufferPoolManager::FrameOperationToken> frame_operation_;
    };

    IxNodeHandle* split(IxNodeHandle* node, bool right_edge_append = false);

    void insert_into_parent(IxNodeHandle* old_node, const char* key, IxNodeHandle* new_node,
                            const IndexWriteWalContext& wal_context);

    // for delete
    bool delete_entry(const char* key, const IndexWriteWalContext& wal_context);
    bool delete_entry(const char* key, const Rid& value, const IndexWriteWalContext& wal_context);
    bool delete_entry_unlocked(const char* key, const IndexWriteWalContext& wal_context);
    bool delete_entry_unlocked(const char* key, const Rid& value, const IndexWriteWalContext& wal_context);

    bool coalesce_or_redistribute(IxNodeHandle* node, const IndexWriteWalContext& wal_context,
                                  bool* root_is_latched = nullptr);
    bool adjust_root(IxNodeHandle* old_root_node);

    void redistribute(IxNodeHandle* neighbor_node, IxNodeHandle* node, IxNodeHandle* parent, int index);

    bool coalesce(IxNodeHandle** neighbor_node, IxNodeHandle** node, IxNodeHandle** parent, int index,
                  const IndexWriteWalContext& wal_context, bool* root_is_latched);

    Iid lower_bound(const char* key) const;

    Iid upper_bound(const char* key);

    std::pair<Iid, Iid> equal_range(const char* key);

    // Copy all RIDs for one complete key while holding the appropriate index
    // and leaf latches. This avoids constructing a general-purpose IxScan for
    // exact-key executor probes.
    void lookup_equal(const char* key, std::vector<Rid>& result) const;

    // Exact-key probe that distinguishes a missing key from a key with more
    // than one RID. The rid is valid only for the Unique result.
    UniqueLookupResult lookup_unique(const char* key) const;

    // A stale or torn header can name leaves that have since been split or
    // merged away, which makes the leaf chain look broken and makes
    // delete_entry stop scanning early. Recompute both by descending the left
    // and right edges of the tree: one page read per level. Returns false when
    // even that spine is unusable, which means the index has to be rebuilt.
    bool refresh_leaf_chain_endpoint();

    // Validate persisted parent/child relationships, key ordering, and the
    // complete leaf chain before crash recovery attempts incremental repair.
    bool validate_structure() const;

    // Key ordering of this index. Recovery sorts a whole batch of keys into
    // tree order before touching the index, which turns a random walk over the
    // leaves into a left-to-right sweep.
    const std::vector<ColType>& get_col_types() const {
        return file_hdr_->col_types_;
    }
    const std::vector<int>& get_col_lens() const {
        return file_hdr_->col_lens_;
    }
    int get_col_tot_len() const {
        return file_hdr_->col_tot_len_;
    }

    // Rebuild the root cache and upper-level residency after recovery has
    // repaired or rebuilt the on-disk index structure.
    // Refresh the root cache and, by default, the internal-page residency
    // cache. Database startup can request root-only refresh to avoid walking
    // every leaf of a large index.
    void refresh_page_residency(bool include_internal = true);
    void prepare_for_smo_redo();
    void install_recovered_smo_header(const char* header_image);

    Iid leaf_end() const;

    Iid leaf_begin() const;

private:
    enum class InsertSplitFault : uint8_t {
        None,
        SiblingAllocation,
        NewRootAllocation,
        NextLeafFetch,
        MovedChildFetch,
    };

    enum class InsertSplitStage : uint8_t {
        CommitBegin,
        CommitReacquire,
        CommitBeforeImageApply,
        CommitImageApplied,
        RollbackReacquire,
        RollbackBeforeImageRestore,
        RollbackImageRestored,
        RootCacheRefresh,
    };
    using InsertSplitTestHook = std::function<void(InsertSplitStage, PageId, Page*)>;

    static std::atomic<InsertSplitFault> insert_split_fault_;
    static std::mutex insert_split_test_hook_latch_;
    static InsertSplitTestHook insert_split_test_hook_;

    static void set_insert_split_fault(InsertSplitFault fault) {
        insert_split_fault_.store(fault, std::memory_order_relaxed);
    }

    static void maybe_inject_insert_split_fault(InsertSplitFault fault);
    static void set_insert_split_test_hook(InsertSplitTestHook hook);
    static void run_insert_split_test_hook(InsertSplitStage stage, PageId page_id = {}, Page* page = nullptr);

    page_id_t insert_entry_with_split(page_id_t leaf_page_no, const char* key, const Rid& value, int insert_pos,
                                      bool right_edge_append,
                                      const BufferPoolManager::FrameOperationToken& frame_operation,
                                      const IndexWriteWalContext& wal_context);

    bool try_append_hint(const char* key, IxNodeHandle& leaf) const;
    void remember_append_hint(page_id_t page_no) const;

    // Follows the leftmost (rightmost=false) or rightmost child pointer down to a
    // leaf, rejecting page numbers that leave the file.
    bool descend_leaf_chain_edge(bool rightmost, page_id_t* leaf_page_no) const;

    // Root pages are shared by every lookup and are cheap to retain. Startup
    // still uses refresh_page_residency(false) when it only needs root pages.

    // The single funnel through which a page becomes part of the current SMO's
    // page set. Called from fetch_node_into(), fetch_node() and create_node(),
    // which together are the *only* ways index code can obtain a page - so a
    // page cannot be modified without being collected. Collecting on acquisition
    // rather than on modification is deliberate: over-collection costs one hash
    // lookup in flush_pages(), which skips pages that turned out to be clean,
    // whereas under-collection silently reintroduces the torn-SMO bug.
    void note_smo_page(page_id_t page_no) const {
        if (smo_active_) {
            smo_pages_.push_back(page_no);
        }
    }

    // Records that this operation changed tree linkage or a separator key, i.e.
    // that it is an SMO and not just an in-place update of one leaf's payload.
    // Without this every bulk-load row would publish its leaf.
    void note_structure_change() const {
        smo_structural_ = true;
        assert(smo_wal_context_.has_value());
        header_dependency_.merge(smo_wal_context_->dependency());
        ++header_dirty_epoch_;
    }

    const IndexWriteWalContext& active_smo_wal_context() const {
        assert(smo_active_ && smo_wal_context_.has_value());
        return *smo_wal_context_;
    }

#ifndef NDEBUG
    bool smo_collected(page_id_t page_no) const {
        return std::find(smo_pages_.begin(), smo_pages_.end(), page_no) != smo_pages_.end();
    }
#endif

    void publish_smo_pages_impl(bool* wal_barrier_released = nullptr) const;
    void write_index_header_page() const;

    void refresh_root_page_cache(const BufferPoolManager::FrameOperationToken* frame_operation = nullptr) noexcept;
    void release_root_page_cache() const;
    void register_internal_pages();
    bool mark_internal_page_resident(page_id_t page_no, Page* page,
                                     const BufferPoolManager::FrameOperationToken* frame_operation = nullptr);
    bool cache_internal_page(page_id_t page_no, Page* page,
                             const BufferPoolManager::FrameOperationToken* frame_operation = nullptr);
    void drop_cached_internal_page(page_id_t page_no) const;
    void unregister_internal_pages(bool preserve_root_cache = false) const;

    // A cached raw Page* is only usable while its frame still holds the page we
    // asked for. The frame is meant to stay put - the cache pins it and the
    // buffer pool keeps it out of the replacer - but a single hole anywhere in
    // that bookkeeping (BufferPoolManager::clear_residency() downgrades a frame
    // without telling its holder) would otherwise make the index parse an
    // unrelated page as a B+ tree node. So re-check the identity on every hit,
    // the way InnoDB re-checks a block's page id: two int compares, no lock and
    // no hash lookup on the hottest path in the index.
    bool frame_holds(const Page* page, page_id_t page_no) const {
        return page != nullptr && page->get_page_id() == PageId{fd_, page_no};
    }

    Page* cached_page(page_id_t page_no) const {
        if (page_no == IX_NO_PAGE) {
            return nullptr;
        }
        if (cached_root_page_no_ == page_no && frame_holds(cached_root_page_, page_no)) {
            return cached_root_page_;
        }
        auto it = cached_internal_pages_.find(page_no);
        if (it == cached_internal_pages_.end() || !frame_holds(it->second, page_no)) {
            return nullptr;
        }
        return it->second;
    }

    void unpin_if_not_cached(PageId page_id) const {
        if (page_id.fd == fd_ && cached_page(page_id.page_no) != nullptr) {
            return;
        }
        buffer_pool_manager_->unpin_page(page_id, false);
    }

    void unpin_dirty_if_not_cached(PageId page_id, const IndexWriteWalContext& wal_context) const {
        // Every page an in-flight SMO dirties has to be in the set that
        // publish_smo_pages() writes out. note_smo_page() sits on the acquisition
        // paths, so this can only trip if a new code path obtains an index page
        // some other way - which is exactly the mistake that would silently
        // restore the old behaviour.
        assert(!smo_active_ || page_id.fd != fd_ || smo_collected(page_id.page_no));
        if (page_id.fd == fd_) {
            if (Page* page = cached_page(page_id.page_no); page != nullptr) {
                BufferPoolManager::mark_dirty(page, wal_context.dependency());
                return;
            }
        }
        buffer_pool_manager_->unpin_page(page_id, wal_context.dependency());
    }

    // Releases the allocation pin create_node() left on a fresh page. This
    // cannot go through unpin_if_not_cached(): a new internal page is registered
    // in the internal-page cache while it still holds that pin, and skipping the
    // unpin because the page is "cached" would leak it.
    void unpin_created_page(PageId page_id, const IndexWriteWalContext& wal_context) const {
        buffer_pool_manager_->unpin_page(page_id, wal_context.dependency());
    }

    void fetch_root_node_into(IxNodeHandle& out) const {
        // Same predicate as unpin_if_not_cached(), so a page taken from the cache
        // is never unpinned and a page taken from the buffer pool always is.
        if (Page* page = cached_page(file_hdr_->root_page_); page != nullptr) {
            out = IxNodeHandle(file_hdr_.get(), page);
            return;
        }
        fetch_node_into(file_hdr_->root_page_, out);
    }

    void unpin_if_not_cached_root(PageId page_id) const {
        unpin_if_not_cached(page_id);
    }

    // 辅助函数
    void update_root_page_no(page_id_t root) {
        file_hdr_->root_page_ = root;
    }

    bool is_empty() const {
        return file_hdr_->root_page_ == IX_NO_PAGE;
    }

    // for get/create node
    IxNodeHandle* fetch_node(int page_no) const;

    void fetch_node_into(int page_no, IxNodeHandle& out) const;

    IxNodeHandle* create_node();

    // for maintain data structure
    void maintain_parent(IxNodeHandle* node);

    void erase_leaf(IxNodeHandle* leaf);

    void release_node_handle(IxNodeHandle& node);

    void maintain_child(IxNodeHandle* node, int child_idx);

    // for index test
    Rid get_rid(const Iid& iid) const;

    mutable std::unordered_set<page_id_t> resident_internal_pages_;
    mutable std::unordered_map<page_id_t, Page*> cached_internal_pages_;
    std::vector<page_id_t> split_spare_pages_;

    // SMO bookkeeping, deliberately non-atomic. index_latch_ already orders it:
    // smo_active_ is only ever assigned by the holder of the structure-exclusive
    // latch, and every read of it (note_smo_page(), reached from fetch_node_into()
    // and create_node()) happens under at least the shared latch - every index
    // page acquisition in the codebase is latched, including the executors'
    // lower_bound()/upper_bound() calls. Storage is reused across operations so a
    // split costs no allocation once the vectors have grown.
    mutable bool smo_active_{false};
    mutable bool smo_structural_{false};
    mutable std::optional<IndexWriteWalContext> smo_wal_context_;
    mutable std::vector<page_id_t> smo_pages_;
    mutable std::vector<PageId> smo_flush_batch_;
    mutable std::vector<char> header_image_;
    mutable PageWriteDependency header_dependency_{PageWriteDependency::None()};
    mutable uint64_t header_dirty_epoch_{0};
};
