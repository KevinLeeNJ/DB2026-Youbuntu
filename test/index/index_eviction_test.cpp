/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Regressions for index behaviour when the working set does not fit in the
// buffer pool. Neither failure below shows up with the default 1 GB pool that
// the other index tests use, because nothing is ever evicted there.

#undef NDEBUG

#define private public
#include "index/ix.h"
#undef private

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <future>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "gtest/gtest.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_meta.h"

namespace {

// Small enough that a few thousand keys force continuous eviction, large enough
// that a root-to-leaf descent still finds every page it needs.
constexpr size_t kSmallPoolFrames = 48;
// 64-byte keys keep btree_order at 55, so a few thousand keys build a
// three-level tree that spans far more pages than the pool holds.
constexpr int kKeyLen = 64;

class IndexEvictionTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager;
    std::unique_ptr<IxManager> ix_manager;
    std::string table_name;
    std::vector<ColMeta> cols;

    void SetUp() override {
        table_name = "index_eviction_test_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        open_storage();
        cols = {ColMeta{
            .tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = kKeyLen, .offset = 0, .index = true}};
        cleanup();
        ix_manager->create_index(table_name, cols);
    }

    void TearDown() override {
        IxIndexHandle::set_insert_split_fault(IxIndexHandle::InsertSplitFault::None);
        IxIndexHandle::set_insert_split_test_hook({});
        BufferPoolManager::set_flush_page_test_hook({});
        // A fatal assertion can bypass close_index(). Release DiskManager last
        // so its RAII cleanup closes every remaining fd before unlinking files;
        // otherwise TearDown masks the primary failure with "File is opened".
        ix_manager.reset();
        buffer_pool_manager.reset();
        disk_manager.reset();
        open_storage();
        cleanup();
    }

    void open_storage(size_t pool_frames = kSmallPoolFrames) {
        disk_manager = std::make_unique<DiskManager>();
        buffer_pool_manager = std::make_unique<BufferPoolManager>(pool_frames, disk_manager.get());
        ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    }

    // Drops every process-local cache, which is what a restart does.
    void restart_storage(size_t pool_frames = kSmallPoolFrames) {
        ix_manager.reset();
        buffer_pool_manager.reset();
        disk_manager.reset();
        open_storage(pool_frames);
    }

    void cleanup() {
        auto index_name = ix_manager->get_index_name(table_name, cols);
        if (disk_manager->is_file(index_name)) {
            disk_manager->destroy_file(index_name);
        }
    }

    std::unique_ptr<IxIndexHandle> open_index() {
        return ix_manager->open_index(table_name, cols);
    }

    void close_index(std::unique_ptr<IxIndexHandle>& ih) {
        if (ih) {
            ix_manager->close_index(ih.get());
            ih.reset();
        }
    }

    static std::vector<char> key(int value) {
        std::vector<char> buf(kKeyLen, 0);
        // Zero-padded so memcmp order matches numeric order.
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(buf.data(), encoded.data(), encoded.size());
        return buf;
    }

    int count_missing(IxIndexHandle* ih, int key_count) {
        int missing = 0;
        for (int value = 0; value < key_count; ++value) {
            auto k = key(value);
            std::vector<Rid> result;
            if (!ih->get_value(k.data(), &result, nullptr) || result.size() != 1 ||
                !(result.front() == Rid{1, value})) {
                ++missing;
            }
        }
        return missing;
    }

    int total_pin_count() const {
        int total = 0;
        for (size_t frame = 0; frame < buffer_pool_manager->pool_size_; ++frame) {
            total += buffer_pool_manager->pages_[frame].pin_count_;
        }
        return total;
    }

    // Reads other pages of the same index until the buffer pool hands
    // target_frame to one of them, then keeps that page pinned so the frame
    // cannot drift back. Returns the page the frame ended up holding.
    page_id_t steal_frame(IxIndexHandle* ih, Page* target_frame, page_id_t original_page_no) {
        const page_id_t page_limit = ih->file_hdr_->num_pages_;
        for (int pass = 0; pass < 4; ++pass) {
            for (page_id_t page_no = IX_INIT_ROOT_PAGE; page_no < page_limit; ++page_no) {
                if (page_no == original_page_no) {
                    continue;
                }
                const PageId page_id{ih->fd_, page_no};
                Page* page = buffer_pool_manager->fetch_page(page_id);
                if (page == nullptr) {
                    continue;
                }
                if (page == target_frame) {
                    // Leave it pinned: the frame must keep holding this page.
                    return page_no;
                }
                buffer_pool_manager->unpin_page(page_id, false);
            }
        }
        return IX_NO_PAGE;
    }
};

// Root cause B: PinnedInserter::insert() releases the pinned leaf with
// is_dirty=false when the next key falls outside it, even though earlier calls
// already inserted into that leaf. Under eviction the frame is reclaimed and
// every one of those inserts is dropped.
TEST_F(IndexEvictionTest, NonAscendingBulkLoadKeepsEveryKeyAfterRestart) {
    constexpr int kKeyCount = 4000;
    std::vector<int> insertion_order(kKeyCount);
    std::iota(insertion_order.begin(), insertion_order.end(), 0);
    // TPC-C loads customer(c_w_id, c_d_id, c_last, c_id) and
    // orders(o_w_id, o_d_id, o_c_id, o_id) with a randomly ordered suffix, so
    // the bulk loader constantly leaves the pinned leaf and comes back to it.
    std::mt19937 rng(20260727);
    for (int block_begin = 0; block_begin < kKeyCount; block_begin += 200) {
        const int block_end = std::min(block_begin + 200, kKeyCount);
        std::shuffle(insertion_order.begin() + block_begin, insertion_order.begin() + block_end, rng);
    }

    {
        auto ih = open_index();
        {
            IxIndexHandle::PinnedInserter inserter(ih.get(), IndexWriteWalContext::TestNoWal());
            for (const int value : insertion_order) {
                auto k = key(value);
                inserter.insert(k.data(), Rid{1, value}, false);
            }
        }
        close_index(ih);
    }

    // Reopen through a fresh buffer pool: only what actually reached disk counts.
    restart_storage();
    auto ih = open_index();
    EXPECT_EQ(count_missing(ih.get(), kKeyCount), 0) << "bulk load did not survive eviction";
    EXPECT_TRUE(ih->validate_structure());
    close_index(ih);
}

// The same load through the ordinary insert path, for contrast: it must also
// survive, and it exercises split()/insert_into_parent() under eviction.
TEST_F(IndexEvictionTest, RandomInsertLoadKeepsEveryKeyAfterRestart) {
    constexpr int kKeyCount = 4000;
    std::vector<int> insertion_order(kKeyCount);
    std::iota(insertion_order.begin(), insertion_order.end(), 0);
    std::mt19937 rng(20260728);
    std::shuffle(insertion_order.begin(), insertion_order.end(), rng);

    {
        auto ih = open_index();
        for (const int value : insertion_order) {
            auto k = key(value);
            ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
        }
        close_index(ih);
    }

    restart_storage();
    auto ih = open_index();
    EXPECT_EQ(count_missing(ih.get(), kKeyCount), 0) << "random insert load did not survive eviction";
    EXPECT_TRUE(ih->validate_structure());
    close_index(ih);
}

// Root cause A: IxIndexHandle keeps raw Page* pointers for the root and for the
// internal pages. Their only protection is the buffer pool's IndexInternal
// residency class, and BufferPoolManager::clear_residency() drops that class
// without telling the index. This test punches exactly that hole - residency
// and pin gone, raw pointer retained - and requires the index to notice that
// the frame no longer holds the page it asked for, instead of parsing an
// unrelated page as a B+ tree node.
TEST_F(IndexEvictionTest, CachedInternalPageIsNotUsedAfterItsFrameIsReused) {
    constexpr int kKeyCount = 3000;
    auto ih = open_index();
    for (int value = 0; value < kKeyCount; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }
    ih->refresh_page_residency(true);
    ASSERT_FALSE(ih->cached_internal_pages_.empty());

    const auto cached = *ih->cached_internal_pages_.begin();
    const page_id_t cached_page_no = cached.first;
    Page* cached_frame = cached.second;
    ASSERT_NE(cached_frame, nullptr);

    buffer_pool_manager->unmark_resident(PageId{ih->fd_, cached_page_no});
    buffer_pool_manager->unpin_page(PageId{ih->fd_, cached_page_no}, false);
    const page_id_t squatter = steal_frame(ih.get(), cached_frame, cached_page_no);
    ASSERT_NE(squatter, IX_NO_PAGE) << "test no longer reproduces frame reuse";

    {
        auto structure_guard = ih->lock_shared();
        IxNodeHandle node;
        ih->fetch_node_into(cached_page_no, node);
        EXPECT_EQ(node.get_page_id().page_no, cached_page_no) << "fetch_node_into returned page " << squatter;
        EXPECT_EQ(node.get_page_id().fd, ih->fd_);
        ih->unpin_if_not_cached(node.get_page_id());
    }
    EXPECT_EQ(count_missing(ih.get(), kKeyCount), 0) << "a stale cached internal page hid keys";

    buffer_pool_manager->unpin_page(PageId{ih->fd_, squatter}, false);
    close_index(ih);
}

// The same hazard for the root cache, which every single lookup starts from.
TEST_F(IndexEvictionTest, CachedRootPageIsNotUsedAfterItsFrameIsReused) {
    constexpr int kKeyCount = 3000;
    auto ih = open_index();
    for (int value = 0; value < kKeyCount; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }
    ih->refresh_page_residency(false);
    ASSERT_NE(ih->cached_root_page_, nullptr);

    Page* root_frame = ih->cached_root_page_;
    const page_id_t root_page_no = ih->cached_root_page_no_;
    buffer_pool_manager->unmark_resident(PageId{ih->fd_, root_page_no});
    buffer_pool_manager->unpin_page(PageId{ih->fd_, root_page_no}, false);
    const page_id_t squatter = steal_frame(ih.get(), root_frame, root_page_no);
    ASSERT_NE(squatter, IX_NO_PAGE) << "test no longer reproduces frame reuse";

    {
        auto structure_guard = ih->lock_shared();
        IxNodeHandle node;
        ih->fetch_node_into(root_page_no, node);
        EXPECT_EQ(node.get_page_id().page_no, root_page_no) << "fetch_node_into returned page " << squatter;
        ih->unpin_if_not_cached(node.get_page_id());
    }
    EXPECT_EQ(count_missing(ih.get(), kKeyCount), 0) << "a stale cached root page hid keys";

    buffer_pool_manager->unpin_page(PageId{ih->fd_, squatter}, false);
    close_index(ih);
}

TEST_F(IndexEvictionTest, GlobalResidencyQuotaLeavesFramesForOrdinaryPressure) {
    cleanup();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(8, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    cols = {ColMeta{.tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = 512, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    auto ih = open_index();
    auto count_missing_wide = [&](IxIndexHandle* handle) {
        int missing = 0;
        for (int value = 0; value < 800; ++value) {
            std::vector<char> k(512, 0);
            const std::string encoded = std::to_string(1000000 + value);
            std::memcpy(k.data(), encoded.data(), encoded.size());
            std::vector<Rid> result;
            if (!handle->get_value(k.data(), &result, nullptr) || result.size() != 1 ||
                !(result.front() == Rid{1, value})) {
                ++missing;
            }
        }
        return missing;
    };

    ASSERT_EQ(buffer_pool_manager->index_internal_residency_budget(), 0U);
    ih->refresh_page_residency(true);
    ASSERT_EQ(ih->cached_root_page_, nullptr);
    ASSERT_TRUE(ih->resident_internal_pages_.empty());

    for (int value = 0; value < 800; ++value) {
        std::vector<char> k(512, 0);
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(k.data(), encoded.data(), encoded.size());
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }
    ih->refresh_page_residency(true);
    EXPECT_LE(buffer_pool_manager->index_internal_resident_count(),
              buffer_pool_manager->index_internal_residency_budget());
    EXPECT_EQ(buffer_pool_manager->index_internal_resident_count(), ih->resident_internal_pages_.size());

    const std::string pressure_file = table_name + "_pressure";
    disk_manager->create_file(pressure_file);
    const int pressure_fd = disk_manager->open_file(pressure_file);
    for (int i = 0; i < 32; ++i) {
        PageId page_id{pressure_fd, INVALID_PAGE_ID};
        ASSERT_NE(buffer_pool_manager->new_page(&page_id), nullptr) << "pressure page " << i;
        ASSERT_TRUE(buffer_pool_manager->unpin_page(page_id, false));
    }
    EXPECT_EQ(count_missing_wide(ih.get()), 0);
    EXPECT_TRUE(ih->validate_structure());

    close_index(ih);
    buffer_pool_manager->delete_all_pages(pressure_fd);
    disk_manager->close_file(pressure_fd);
    disk_manager->destroy_file(pressure_file);

    restart_storage(8);
    ih = open_index();
    EXPECT_EQ(count_missing_wide(ih.get()), 0);
    EXPECT_TRUE(ih->validate_structure());
    close_index(ih);
}

TEST_F(IndexEvictionTest, SplitAllocationFaultsLeaveRootLeafUnchangedAndRetryable) {
    cleanup();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(8, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    cols = {ColMeta{.tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = 512, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    auto ih = open_index();
    auto wide_key = [](int value) {
        std::vector<char> result(512, 0);
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(result.data(), encoded.data(), encoded.size());
        return result;
    };
    auto expect_prefix = [&](int count) {
        for (int value = 0; value < count; ++value) {
            auto k = wide_key(value);
            std::vector<Rid> result;
            ASSERT_TRUE(ih->get_value(k.data(), &result, nullptr)) << value;
            ASSERT_EQ(result, std::vector<Rid>({Rid{1, value}})) << value;
        }
    };

    const int split_key = ih->file_hdr_->btree_order_;
    for (int value = 0; value < split_key; ++value) {
        auto k = wide_key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }
    auto trigger = wide_key(split_key);

    IxIndexHandle::set_insert_split_fault(IxIndexHandle::InsertSplitFault::SiblingAllocation);
    EXPECT_THROW(ih->insert_entry(trigger.data(), Rid{1, split_key}, IndexWriteWalContext::TestNoWal()), InternalError);
    EXPECT_TRUE(ih->validate_structure());
    expect_prefix(split_key);
    EXPECT_EQ(total_pin_count(), 0);

    page_id_t stable_num_pages = IX_NO_PAGE;
    int64_t stable_file_size = -1;
    const std::string index_file = ix_manager->get_index_name(table_name, cols);
    for (int attempt = 0; attempt < 6; ++attempt) {
        IxIndexHandle::set_insert_split_fault(IxIndexHandle::InsertSplitFault::NewRootAllocation);
        EXPECT_THROW(ih->insert_entry(trigger.data(), Rid{1, split_key}, IndexWriteWalContext::TestNoWal()),
                     InternalError);
        EXPECT_TRUE(ih->validate_structure());
        expect_prefix(split_key);
        EXPECT_EQ(total_pin_count(), 0);
        if (attempt == 0) {
            stable_num_pages = ih->file_hdr_->num_pages_;
            stable_file_size = disk_manager->get_file_size(index_file);
        } else {
            EXPECT_EQ(ih->file_hdr_->num_pages_, stable_num_pages);
            EXPECT_EQ(disk_manager->get_file_size(index_file), stable_file_size);
        }
    }

    close_index(ih);
    restart_storage(8);
    ih = open_index();
    EXPECT_TRUE(ih->validate_structure());
    expect_prefix(split_key);
    ih->insert_entry(trigger.data(), Rid{1, split_key}, IndexWriteWalContext::TestNoWal());
    EXPECT_TRUE(ih->validate_structure());
    expect_prefix(split_key + 1);
    close_index(ih);
}

TEST_F(IndexEvictionTest, CommitReacquireFaultRollsBackAppliedPagesAndRetrySucceeds) {
    cleanup();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(8, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    cols = {ColMeta{.tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = 512, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    auto ih = open_index();
    auto wide_key = [](int value) {
        std::vector<char> result(512, 0);
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(result.data(), encoded.data(), encoded.size());
        return result;
    };

    int commit_reacquires = 0;
    int rollback_reacquires = 0;
    std::mutex barrier_latch;
    std::condition_variable barrier_cv;
    bool rollback_flush_started = false;
    bool restored_image_captured = false;
    bool flush_at_page_latch = false;
    bool flush_bypassed_unique_latch = false;
    PageId rollback_page_id;
    std::array<char, PAGE_SIZE> restored_image{};
    std::future<bool> flush_future;
    BufferPoolManager::set_flush_page_test_hook([&](PageId page_id, Page* page) {
        std::scoped_lock lock{barrier_latch};
        if (rollback_flush_started && page_id == rollback_page_id) {
            flush_bypassed_unique_latch = page->latch().try_lock_shared();
            if (flush_bypassed_unique_latch) {
                page->latch().unlock_shared();
            }
            flush_at_page_latch = true;
            barrier_cv.notify_all();
        }
    });
    IxIndexHandle::set_insert_split_test_hook([&](IxIndexHandle::InsertSplitStage stage, PageId page_id, Page* page) {
        if (stage == IxIndexHandle::InsertSplitStage::CommitBegin) {
            commit_reacquires = 0;
        } else if (stage == IxIndexHandle::InsertSplitStage::CommitReacquire && ++commit_reacquires == 7) {
            throw InternalError("injected commit reacquire failure");
        } else if (stage == IxIndexHandle::InsertSplitStage::RollbackReacquire) {
            ++rollback_reacquires;
        } else if (stage == IxIndexHandle::InsertSplitStage::RollbackBeforeImageRestore && !rollback_flush_started) {
            std::unique_lock lock{barrier_latch};
            rollback_page_id = page_id;
            rollback_flush_started = true;
            flush_future =
                std::async(std::launch::async, [&] { return buffer_pool_manager->flush_page(rollback_page_id); });
            barrier_cv.wait(lock, [&] { return flush_at_page_latch; });
            EXPECT_FALSE(flush_bypassed_unique_latch);
            EXPECT_EQ(flush_future.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
        } else if (stage == IxIndexHandle::InsertSplitStage::RollbackImageRestored && page_id == rollback_page_id) {
            std::memcpy(restored_image.data(), page->get_data(), PAGE_SIZE);
            restored_image_captured = true;
        }
    });

    int failed_value = -1;
    for (int value = 0; value < 2000; ++value) {
        auto k = wide_key(value);
        try {
            ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
        } catch (const InternalError&) {
            failed_value = value;
            break;
        }
    }
    ASSERT_GE(failed_value, 0);
    ASSERT_GT(rollback_reacquires, 0);
    IxIndexHandle::set_insert_split_test_hook({});
    BufferPoolManager::set_flush_page_test_hook({});
    ASSERT_TRUE(rollback_flush_started);
    ASSERT_TRUE(restored_image_captured);
    ASSERT_TRUE(flush_future.valid());
    EXPECT_TRUE(flush_future.get());
    std::array<char, PAGE_SIZE> disk_image{};
    disk_manager->read_page(rollback_page_id.fd, rollback_page_id.page_no, disk_image.data(), PAGE_SIZE);
    EXPECT_EQ(std::memcmp(disk_image.data(), restored_image.data(), PAGE_SIZE), 0);
    EXPECT_TRUE(ih->validate_structure());
    for (int value = 0; value < failed_value; ++value) {
        auto k = wide_key(value);
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(k.data(), &result, nullptr)) << value;
        ASSERT_EQ(result, std::vector<Rid>({Rid{1, value}})) << value;
    }
    auto failed_key = wide_key(failed_value);
    std::vector<Rid> absent;
    EXPECT_FALSE(ih->get_value(failed_key.data(), &absent, nullptr));
    ih->insert_entry(failed_key.data(), Rid{1, failed_value}, IndexWriteWalContext::TestNoWal());
    EXPECT_TRUE(ih->validate_structure());
    close_index(ih);
}

TEST_F(IndexEvictionTest, CommitImageExcludesConcurrentFlushUntilImageIsComplete) {
    cleanup();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(8, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    cols = {ColMeta{.tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = 512, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    auto ih = open_index();
    auto wide_key = [](int value) {
        std::vector<char> result(512, 0);
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(result.data(), encoded.data(), encoded.size());
        return result;
    };

    const int split_key = ih->file_hdr_->btree_order_;
    for (int value = 0; value < split_key; ++value) {
        auto k = wide_key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }

    std::mutex barrier_latch;
    std::condition_variable barrier_cv;
    bool commit_flush_started = false;
    bool committed_image_captured = false;
    bool flush_at_page_latch = false;
    bool flush_bypassed_unique_latch = false;
    bool manual_flush_verified = false;
    PageId committed_page_id;
    std::array<char, PAGE_SIZE> committed_image{};
    std::future<bool> flush_future;
    BufferPoolManager::set_flush_page_test_hook([&](PageId page_id, Page* page) {
        std::scoped_lock lock{barrier_latch};
        if (commit_flush_started && page_id == committed_page_id) {
            flush_bypassed_unique_latch = page->latch().try_lock_shared();
            if (flush_bypassed_unique_latch) {
                page->latch().unlock_shared();
            }
            flush_at_page_latch = true;
            barrier_cv.notify_all();
        }
    });
    IxIndexHandle::set_insert_split_test_hook([&](IxIndexHandle::InsertSplitStage stage, PageId page_id, Page* page) {
        if (stage == IxIndexHandle::InsertSplitStage::CommitBeforeImageApply && !commit_flush_started) {
            std::unique_lock lock{barrier_latch};
            committed_page_id = page_id;
            commit_flush_started = true;
            flush_future =
                std::async(std::launch::async, [&] { return buffer_pool_manager->flush_page(committed_page_id); });
            barrier_cv.wait(lock, [&] { return flush_at_page_latch; });
            EXPECT_FALSE(flush_bypassed_unique_latch);
            EXPECT_EQ(flush_future.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
        } else if (stage == IxIndexHandle::InsertSplitStage::CommitImageApplied && page_id == committed_page_id) {
            std::memcpy(committed_image.data(), page->get_data(), PAGE_SIZE);
            committed_image_captured = true;
        } else if (stage == IxIndexHandle::InsertSplitStage::RootCacheRefresh) {
            EXPECT_TRUE(committed_image_captured);
            EXPECT_TRUE(flush_future.valid());
            if (committed_image_captured && flush_future.valid()) {
                const bool flushed = flush_future.get();
                EXPECT_TRUE(flushed);
                std::array<char, PAGE_SIZE> disk_image{};
                disk_manager->read_page(committed_page_id.fd, committed_page_id.page_no, disk_image.data(), PAGE_SIZE);
                EXPECT_EQ(std::memcmp(disk_image.data(), committed_image.data(), PAGE_SIZE), 0);
                manual_flush_verified =
                    flushed && std::memcmp(disk_image.data(), committed_image.data(), PAGE_SIZE) == 0;
            }
        }
    });

    auto trigger = wide_key(split_key);
    EXPECT_NO_THROW(ih->insert_entry(trigger.data(), Rid{1, split_key}, IndexWriteWalContext::TestNoWal()));
    IxIndexHandle::set_insert_split_test_hook({});
    BufferPoolManager::set_flush_page_test_hook({});
    ASSERT_TRUE(commit_flush_started);
    ASSERT_TRUE(committed_image_captured);
    EXPECT_TRUE(manual_flush_verified);
    EXPECT_TRUE(ih->validate_structure());
    close_index(ih);
}

TEST_F(IndexEvictionTest, RootCacheRefreshFailureDoesNotFailCommittedSplit) {
    cleanup();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(16, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    cols = {ColMeta{.tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = 512, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    auto ih = open_index();
    auto wide_key = [](int value) {
        std::vector<char> result(512, 0);
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(result.data(), encoded.data(), encoded.size());
        return result;
    };

    const int split_key = ih->file_hdr_->btree_order_;
    for (int value = 0; value < split_key; ++value) {
        auto k = wide_key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }
    bool refresh_fault_reached = false;
    IxIndexHandle::set_insert_split_test_hook([&](IxIndexHandle::InsertSplitStage stage, PageId, Page*) {
        if (stage == IxIndexHandle::InsertSplitStage::RootCacheRefresh) {
            refresh_fault_reached = true;
            throw InternalError("injected root cache refresh failure");
        }
    });
    auto trigger = wide_key(split_key);
    EXPECT_NO_THROW(ih->insert_entry(trigger.data(), Rid{1, split_key}, IndexWriteWalContext::TestNoWal()));
    EXPECT_TRUE(refresh_fault_reached);
    EXPECT_EQ(ih->cached_root_page_, nullptr);
    EXPECT_EQ(ih->cached_root_page_no_, IX_NO_PAGE);
    EXPECT_TRUE(ih->validate_structure());

    IxIndexHandle::set_insert_split_test_hook({});
    ih->refresh_page_residency(false);
    EXPECT_NE(ih->cached_root_page_, nullptr);
    EXPECT_EQ(ih->cached_root_page_no_, ih->file_hdr_->root_page_);
    close_index(ih);
}

TEST_F(IndexEvictionTest, OtherFileFetchCannotConsumeFramesDuringSplitCommit) {
    cleanup();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(8, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    cols = {ColMeta{.tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = 512, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    auto ih = open_index();
    auto wide_key = [](int value) {
        std::vector<char> result(512, 0);
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(result.data(), encoded.data(), encoded.size());
        return result;
    };

    const std::string pressure_file = table_name + "_pressure";
    disk_manager->create_file(pressure_file);
    const int pressure_fd = disk_manager->open_file(pressure_file);
    PageId pressure_id{pressure_fd, INVALID_PAGE_ID};
    Page* pressure_page = buffer_pool_manager->new_page(&pressure_id);
    ASSERT_NE(pressure_page, nullptr);
    buffer_pool_manager->unpin_page(pressure_id, true);

    const int split_key = ih->file_hdr_->btree_order_;
    for (int value = 0; value < split_key; ++value) {
        auto k = wide_key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }

    std::atomic<bool> pressure_started{false};
    std::atomic<bool> pressure_finished{false};
    std::thread pressure_thread;
    IxIndexHandle::set_insert_split_test_hook([&](IxIndexHandle::InsertSplitStage stage, PageId, Page*) {
        if (stage != IxIndexHandle::InsertSplitStage::CommitReacquire || pressure_started.exchange(true)) {
            return;
        }
        pressure_thread = std::thread([&] {
            Page* page = buffer_pool_manager->fetch_page(pressure_id);
            pressure_finished.store(true, std::memory_order_release);
            if (page != nullptr) {
                buffer_pool_manager->unpin_page(pressure_id, false);
            }
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        EXPECT_FALSE(pressure_finished.load(std::memory_order_acquire));
    });

    auto trigger = wide_key(split_key);
    EXPECT_NO_THROW(ih->insert_entry(trigger.data(), Rid{1, split_key}, IndexWriteWalContext::TestNoWal()));
    IxIndexHandle::set_insert_split_test_hook({});
    ASSERT_TRUE(pressure_thread.joinable());
    pressure_thread.join();
    EXPECT_TRUE(pressure_finished.load(std::memory_order_acquire));
    EXPECT_TRUE(ih->validate_structure());

    close_index(ih);
    disk_manager->close_file(pressure_fd);
    disk_manager->destroy_file(pressure_file);
}

TEST_F(IndexEvictionTest, NextLeafPreflightFaultLeavesRandomTreeUnchangedAndRetryable) {
    cleanup();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(8, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    cols = {ColMeta{.tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = 512, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    auto ih = open_index();
    auto wide_key = [](int value) {
        std::vector<char> result(512, 0);
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(result.data(), encoded.data(), encoded.size());
        return result;
    };

    std::vector<int> order(300);
    std::iota(order.begin(), order.end(), 0);
    std::mt19937 rng(20260729);
    std::shuffle(order.begin(), order.end(), rng);
    std::vector<int> inserted;
    int failed_value = -1;
    IxIndexHandle::set_insert_split_fault(IxIndexHandle::InsertSplitFault::NextLeafFetch);
    for (const int value : order) {
        auto k = wide_key(value);
        try {
            ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
            inserted.push_back(value);
        } catch (const InternalError&) {
            failed_value = value;
            break;
        }
    }
    ASSERT_GE(failed_value, 0);
    EXPECT_TRUE(ih->validate_structure());
    for (const int value : inserted) {
        auto k = wide_key(value);
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(k.data(), &result, nullptr)) << value;
        ASSERT_EQ(result, std::vector<Rid>({Rid{1, value}})) << value;
    }
    EXPECT_EQ(total_pin_count(), 0);

    close_index(ih);
    restart_storage(8);
    ih = open_index();
    EXPECT_TRUE(ih->validate_structure());
    auto failed_key = wide_key(failed_value);
    ih->insert_entry(failed_key.data(), Rid{1, failed_value}, IndexWriteWalContext::TestNoWal());
    EXPECT_TRUE(ih->validate_structure());
    close_index(ih);
}

TEST_F(IndexEvictionTest, MovedChildPreflightFaultLeavesCascadeUnchangedAndRetryable) {
    cleanup();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(8, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    cols = {ColMeta{.tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = 512, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    auto ih = open_index();
    auto wide_key = [](int value) {
        std::vector<char> result(512, 0);
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(result.data(), encoded.data(), encoded.size());
        return result;
    };

    int failed_value = -1;
    IxIndexHandle::set_insert_split_fault(IxIndexHandle::InsertSplitFault::MovedChildFetch);
    for (int value = 0; value < 800; ++value) {
        auto k = wide_key(value);
        try {
            ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
        } catch (const InternalError&) {
            failed_value = value;
            break;
        }
    }
    ASSERT_GE(failed_value, 0);
    EXPECT_TRUE(ih->validate_structure());
    for (int value = 0; value < failed_value; ++value) {
        auto k = wide_key(value);
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(k.data(), &result, nullptr)) << value;
        ASSERT_EQ(result, std::vector<Rid>({Rid{1, value}})) << value;
    }
    EXPECT_EQ(total_pin_count(), 0);

    close_index(ih);
    restart_storage(8);
    ih = open_index();
    EXPECT_TRUE(ih->validate_structure());
    for (int value = failed_value; value < 800; ++value) {
        auto k = wide_key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }
    EXPECT_TRUE(ih->validate_structure());
    for (int value = 0; value < 800; ++value) {
        auto k = wide_key(value);
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(k.data(), &result, nullptr)) << value;
        ASSERT_EQ(result, std::vector<Rid>({Rid{1, value}})) << value;
    }
    close_index(ih);
}

TEST_F(IndexEvictionTest, PinnedInserterUsesBoundedSplitPlanWithEightFrames) {
    cleanup();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(8, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    cols = {ColMeta{.tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = 512, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    auto ih = open_index();
    auto wide_key = [](int value) {
        std::vector<char> result(512, 0);
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(result.data(), encoded.data(), encoded.size());
        return result;
    };

    {
        IxIndexHandle::PinnedInserter inserter(ih.get(), IndexWriteWalContext::TestNoWal());
        for (int value = 0; value < 800; ++value) {
            auto k = wide_key(value);
            inserter.insert(k.data(), Rid{1, value});
        }
    }
    EXPECT_TRUE(ih->validate_structure());
    close_index(ih);

    restart_storage(8);
    ih = open_index();
    EXPECT_TRUE(ih->validate_structure());
    for (int value = 0; value < 800; ++value) {
        auto k = wide_key(value);
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(k.data(), &result, nullptr)) << value;
        ASSERT_EQ(result, std::vector<Rid>({Rid{1, value}})) << value;
    }
    close_index(ih);
}

TEST_F(IndexEvictionTest, FullRefreshCannotKeepRootPinWhenGlobalQuotaIsFull) {
    cleanup();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(10, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());

    const std::string second_table = table_name + "_second";
    const std::string third_table = table_name + "_third";
    const std::vector<ColMeta> second_cols = {ColMeta{
        .tab_name = second_table, .name = "k", .type = TYPE_STRING, .len = kKeyLen, .offset = 0, .index = true}};
    const std::vector<ColMeta> third_cols = {
        ColMeta{.tab_name = third_table, .name = "k", .type = TYPE_STRING, .len = kKeyLen, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    ix_manager->create_index(second_table, second_cols);
    ix_manager->create_index(third_table, third_cols);
    auto first = ix_manager->open_index(table_name, cols);
    auto second = ix_manager->open_index(second_table, second_cols);
    auto third = ix_manager->open_index(third_table, third_cols);

    first->refresh_page_residency(false);
    ASSERT_NE(first->cached_root_page_, nullptr);
    first->unregister_internal_pages();
    ASSERT_EQ(first->cached_root_page_, nullptr);

    second->refresh_page_residency(false);
    ASSERT_NE(second->cached_root_page_, nullptr);
    third->refresh_page_residency(false);
    ASSERT_NE(third->cached_root_page_, nullptr);
    ASSERT_EQ(buffer_pool_manager->index_internal_resident_count(), 2U);
    ASSERT_EQ(buffer_pool_manager->index_internal_residency_budget(), 2U);

    first->refresh_page_residency(true);
    EXPECT_EQ(first->cached_root_page_, nullptr);
    EXPECT_EQ(first->cached_root_page_no_, IX_NO_PAGE);
    EXPECT_TRUE(first->resident_internal_pages_.empty());
    EXPECT_EQ(buffer_pool_manager->index_internal_resident_count(), 2U);

    ix_manager->close_index(first.get());
    first.reset();
    ix_manager->close_index(second.get());
    second.reset();
    ix_manager->close_index(third.get());
    third.reset();
    ix_manager->destroy_index(second_table, second_cols);
    ix_manager->destroy_index(third_table, third_cols);
}

// Measurement harness for the structure gate that crash recovery runs before it
// trusts an index. Disabled by default because it deliberately builds a large
// index; run it with
//   index_test --gtest_also_run_disabled_tests
//              --gtest_filter='*ValidateStructureCost*'
//
// Runs the same structure gate over the .idx files of a real database, which
// rmdb_verify cannot reach once a record file reports a problem first (it stops
// at the first failure). Point it at a database directory with
//   RMDB_VERIFY_INDEX_DIR=tpcc_w1_db index_test --gtest_also_run_disabled_tests
//              --gtest_filter='*ValidatesEveryIndexInDirectory*'
TEST(IndexValidateStructureCost, DISABLED_ValidatesEveryIndexInDirectory) {
    const char* dir = std::getenv("RMDB_VERIFY_INDEX_DIR");
    ASSERT_NE(dir, nullptr) << "set RMDB_VERIFY_INDEX_DIR to a database directory";
    ASSERT_EQ(chdir(dir), 0) << "cannot enter " << dir;

    std::vector<std::string> index_files;
    for (const auto& entry : std::filesystem::directory_iterator(".")) {
        if (entry.is_regular_file() && entry.path().extension() == ".idx") {
            index_files.push_back(entry.path().filename().string());
        }
    }
    std::sort(index_files.begin(), index_files.end());
    ASSERT_FALSE(index_files.empty()) << "no .idx files in " << dir;

    auto disk_manager = std::make_unique<DiskManager>();
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(65536, disk_manager.get());
    int failures = 0;
    for (const std::string& file : index_files) {
        const int fd = disk_manager->open_file(file);
        IxIndexHandle ih(disk_manager.get(), buffer_pool_manager.get(), fd);
        const bool valid = ih.validate_structure();
        fprintf(stderr, "%-56s pages=%-8d valid=%d\n", file.c_str(), ih.file_hdr_->num_pages_, static_cast<int>(valid));
        failures += valid ? 0 : 1;
        buffer_pool_manager->flush_all_pages(fd);
        buffer_pool_manager->delete_all_pages(fd);
        disk_manager->close_file(fd);
    }
    EXPECT_EQ(failures, 0) << failures << " of " << index_files.size() << " indexes failed structure validation";
}

TEST(IndexValidateStructureCost, DISABLED_ValidateStructureCostPerPage) {
    constexpr size_t kBigPoolFrames = 65536; // 256 MB, enough to hold the index
    constexpr int kWideKeyLen = 512;         // btree_order 6, so pages pile up fast
    constexpr int kKeyCount = 120000;

    const std::string table = "validate_structure_cost";
    auto disk_manager = std::make_unique<DiskManager>();
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(kBigPoolFrames, disk_manager.get());
    auto ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    std::vector<ColMeta> cols = {
        ColMeta{.tab_name = table, .name = "k", .type = TYPE_STRING, .len = kWideKeyLen, .offset = 0, .index = true}};
    const std::string index_name = ix_manager->get_index_name(table, cols);
    if (disk_manager->is_file(index_name)) {
        disk_manager->destroy_file(index_name);
    }
    ix_manager->create_index(table, cols);

    auto wide_key = [](int value) {
        std::vector<char> buf(kWideKeyLen, 0);
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(buf.data(), encoded.data(), encoded.size());
        return buf;
    };

    auto ih = ix_manager->open_index(table, cols);
    for (int value = 0; value < kKeyCount; ++value) {
        auto k = wide_key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }
    const int pages = ih->file_hdr_->num_pages_;

    const auto time_validation = [&ih](const char* label, int page_count) {
        const auto start = std::chrono::steady_clock::now();
        const bool valid = ih->validate_structure();
        const auto elapsed = std::chrono::steady_clock::now() - start;
        const double micros = std::chrono::duration<double, std::micro>(elapsed).count();
        fprintf(stderr, "validate_structure %-16s valid=%d pages=%d total=%.1f ms per_page=%.2f us\n", label,
                static_cast<int>(valid), page_count, micros / 1000.0, micros / std::max(page_count, 1));
    };

    time_validation("buffer-pool-hot", pages);
    // Same page cache, empty buffer pool: this is the shape recovery sees.
    buffer_pool_manager->flush_all_pages(ih->GetFd());
    ih->release_root_page_cache();
    buffer_pool_manager->delete_all_pages(ih->GetFd());
    ih->refresh_page_residency(false);
    time_validation("buffer-pool-cold", pages);

    ix_manager->close_index(ih.get());
    ih.reset();

    // The case that actually matters at benchmark scale: the index is far larger
    // than the buffer pool, so validation reads most pages twice - once in the
    // DFS and once along the leaf chain - and each read costs an eviction.
    auto small_pool = std::make_unique<BufferPoolManager>(4096, disk_manager.get());
    auto small_ix_manager = std::make_unique<IxManager>(disk_manager.get(), small_pool.get());
    auto small_ih = small_ix_manager->open_index(table, cols);
    {
        const auto start = std::chrono::steady_clock::now();
        const bool valid = small_ih->validate_structure();
        const double micros =
            std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start).count();
        fprintf(stderr, "validate_structure %-16s valid=%d pages=%d total=%.1f ms per_page=%.2f us\n",
                "pool-4096-frames", static_cast<int>(valid), pages, micros / 1000.0, micros / std::max(pages, 1));
    }
    small_ix_manager->close_index(small_ih.get());
    small_ih.reset();
    disk_manager->destroy_file(index_name);
}

} // namespace
