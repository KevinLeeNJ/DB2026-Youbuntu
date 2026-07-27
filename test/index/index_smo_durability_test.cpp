/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// A structure-modification operation (split, new root, separator update) dirties
// several pages at once, and index pages take part in no WAL at all. If the
// buffer pool is allowed to publish those pages independently, a crash between
// two evictions leaves a tree on disk that does not describe itself, and
// recovery has no option but to rebuild the whole index.
//
// Every test here ends in a *crash*, not a close: dropping the buffer pool
// without flushing is exactly what SIGKILL does to the pages it still holds.
// close_index() would hide the bug by flushing everything on the way out.

#undef NDEBUG

#define private public
#include "index/ix.h"
#undef private

#include <algorithm>
#include <cstring>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_meta.h"

namespace {

// Small enough that a few thousand keys force continuous eviction, which is what
// makes the on-disk image of an in-flight SMO partial in the first place.
constexpr size_t kSmallPoolFrames = 48;
// 64-byte keys keep btree_order at 55, so a few thousand keys build a
// three-level tree spanning far more pages than the pool holds.
constexpr int kKeyLen = 64;

// Turns the SMO write-out off for the negative controls, which reproduce the
// original bug and must fail the same check the positive tests pass.
class SmoFlushOff {
public:
    SmoFlushOff() {
        IxIndexHandle::set_smo_flush_enabled(false);
    }
    ~SmoFlushOff() {
        IxIndexHandle::set_smo_flush_enabled(true);
    }
};

class IndexSmoDurabilityTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager;
    std::unique_ptr<IxManager> ix_manager;
    std::string table_name;
    std::vector<ColMeta> cols;

    void SetUp() override {
        table_name = "index_smo_durability_test_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        open_storage();
        cols = {ColMeta{
            .tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = kKeyLen, .offset = 0, .index = true}};
        cleanup();
        ix_manager->create_index(table_name, cols);
    }

    void TearDown() override {
        cleanup();
    }

    void open_storage() {
        disk_manager = std::make_unique<DiskManager>();
        buffer_pool_manager = std::make_unique<BufferPoolManager>(kSmallPoolFrames, disk_manager.get());
        ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    }

    // Drops the buffer pool and the in-memory index header without flushing
    // either: the on-disk file is left holding exactly what was written before
    // this point, which is what a restart after SIGKILL sees.
    void crash_and_reopen_storage(std::unique_ptr<IxIndexHandle>& ih) {
        ih.reset();
        ix_manager.reset();
        buffer_pool_manager.reset();
        disk_manager.reset();
        open_storage();
    }

    void cleanup() {
        auto index_name = ix_manager->get_index_name(table_name, cols);
        // Every test leaves its handle open on purpose - a crash does not close
        // files - so drop the descriptor here rather than through close_index(),
        // which would flush the very pages the test is checking never reached
        // disk. Reaching into path2fd_ keeps this working when a test aborts
        // early, so a teardown error can never mask the real failure.
        if (auto it = disk_manager->path2fd_.find(index_name); it != disk_manager->path2fd_.end()) {
            disk_manager->close_file(it->second);
        }
        if (disk_manager->is_file(index_name)) {
            disk_manager->destroy_file(index_name);
        }
    }

    std::unique_ptr<IxIndexHandle> open_index() {
        return ix_manager->open_index(table_name, cols);
    }

    static std::vector<char> key(int value) {
        std::vector<char> buf(kKeyLen, 0);
        // Zero-padded so memcmp order matches numeric order.
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(buf.data(), encoded.data(), encoded.size());
        return buf;
    }
};

// The minimal torn SMO, built deliberately instead of waiting for the buffer
// pool to produce one. The first leaf split also replaces the root, so it
// dirties three pages plus the index header; publishing only the new root - one
// single frame eviction - is enough to leave a parent naming a child whose
// persisted image still claims to be a parentless root.
TEST_F(IndexSmoDurabilityTest, SplitPublishesEveryPageItDirtied) {
    auto ih = open_index();
    const int order = ih->file_hdr_->btree_order_;
    const uint64_t published_before = IxIndexHandle::smo_publish_count();

    // One key more than a single leaf can hold.
    for (int value = 0; value <= order; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }
    ASSERT_NE(ih->file_hdr_->root_page_, IX_INIT_ROOT_PAGE) << "expected the root leaf to split";
    ASSERT_GT(IxIndexHandle::smo_publish_count(), published_before) << "no SMO was published";

    // The single frame the buffer pool happened to evict, simulated exactly.
    buffer_pool_manager->flush_page(PageId{ih->fd_, ih->file_hdr_->root_page_});
    ix_manager->flush_index_header(ih.get());

    crash_and_reopen_storage(ih);
    auto reopened = open_index();
    EXPECT_TRUE(reopened->validate_structure());
}

// Negative control for the test above. Same construction, write-out disabled:
// the reopened index must be *rejected*, otherwise the passing test proves
// nothing about the mechanism.
TEST_F(IndexSmoDurabilityTest, WithoutTheWriteOutASplitIsTornOnDisk) {
    SmoFlushOff smo_flush_off;
    auto ih = open_index();
    const int order = ih->file_hdr_->btree_order_;
    for (int value = 0; value <= order; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }
    ASSERT_NE(ih->file_hdr_->root_page_, IX_INIT_ROOT_PAGE) << "expected the root leaf to split";

    buffer_pool_manager->flush_page(PageId{ih->fd_, ih->file_hdr_->root_page_});
    ix_manager->flush_index_header(ih.get());

    crash_and_reopen_storage(ih);
    auto reopened = open_index();
    EXPECT_FALSE(reopened->validate_structure()) << "the torn split was not detectable, so this control is useless";
}

// The realistic version: enough random inserts through a pool far smaller than
// the tree that the buffer pool is evicting continuously, so at any instant some
// page of some recent split is on disk and some is not.
TEST_F(IndexSmoDurabilityTest, RandomInsertsSurviveACrashWithAValidStructure) {
    constexpr int kKeyCount = 6000;
    std::vector<int> insertion_order(kKeyCount);
    std::iota(insertion_order.begin(), insertion_order.end(), 0);
    std::mt19937 rng(20260727);
    std::shuffle(insertion_order.begin(), insertion_order.end(), rng);

    auto ih = open_index();
    const uint64_t published_before = IxIndexHandle::smo_publish_count();
    const uint64_t pages_before = IxIndexHandle::smo_pages_written();
    for (const int value : insertion_order) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }
    const uint64_t published = IxIndexHandle::smo_publish_count() - published_before;
    const uint64_t pages = IxIndexHandle::smo_pages_written() - pages_before;
    ASSERT_GT(published, 100U) << "the tree did not split often enough for this test to mean anything";
    // A split touches the leaf, its new sibling, the following leaf and one
    // parent per level. Anything close to 1 would mean the collector is missing
    // pages, which is the failure mode this whole mechanism has to avoid.
    const double pages_per_smo = static_cast<double>(pages) / static_cast<double>(published);
    GTEST_LOG_(INFO) << published << " SMOs wrote " << pages << " page images (" << pages_per_smo << " per SMO)";
    EXPECT_GT(pages_per_smo, 2.0) << published << " SMOs wrote only " << pages << " page images";

    crash_and_reopen_storage(ih);
    auto reopened = open_index();
    EXPECT_TRUE(reopened->validate_structure());
}

// Negative control for the test above: continuous eviction without the write-out
// leaves the tree structurally broken. This is the shape of the failure seen on
// a real 50-warehouse TPC-C run, where 4 of 9 indexes failed the recovery
// structure gate after one SIGKILL and had to be rebuilt from the heap.
TEST_F(IndexSmoDurabilityTest, WithoutTheWriteOutRandomInsertsLeaveABrokenTree) {
    SmoFlushOff smo_flush_off;
    constexpr int kKeyCount = 6000;
    std::vector<int> insertion_order(kKeyCount);
    std::iota(insertion_order.begin(), insertion_order.end(), 0);
    std::mt19937 rng(20260727);
    std::shuffle(insertion_order.begin(), insertion_order.end(), rng);

    auto ih = open_index();
    for (const int value : insertion_order) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    crash_and_reopen_storage(ih);
    auto reopened = open_index();
    EXPECT_FALSE(reopened->validate_structure()) << "eviction did not tear a split, so this control is useless";
}

// Deletes route through the tree-exclusive path whenever they hit position zero,
// where they rewrite the separator key in every ancestor. The leaf and those
// ancestors have to reach disk together for the same reason a split's pages do.
TEST_F(IndexSmoDurabilityTest, SeparatorUpdatesSurviveACrashWithAValidStructure) {
    constexpr int kKeyCount = 4000;
    auto ih = open_index();
    for (int value = 0; value < kKeyCount; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }
    // TPC-C's Delivery takes MIN(no_o_id) and deletes it, so new_orders indexes
    // see nothing but position-zero deletes.
    for (int value = 0; value < kKeyCount / 2; ++value) {
        auto k = key(value);
        ASSERT_TRUE(ih->delete_entry(k.data(), Rid{1, value}, nullptr));
    }

    crash_and_reopen_storage(ih);
    auto reopened = open_index();
    EXPECT_TRUE(reopened->validate_structure());
}

// Emptying a leaf completely is a legal steady state, not damage: the delete
// path never merges nodes, so the leaf stays in the tree and in the leaf chain.
// The structure gate has to accept it, otherwise every index over new_orders
// gets rebuilt after every crash.
TEST_F(IndexSmoDurabilityTest, AnEmptyLeafIsNotStructuralDamage) {
    constexpr int kKeyCount = 4000;
    auto ih = open_index();
    for (int value = 0; value < kKeyCount; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }
    // Every key, so the leftmost leaves end up with no keys at all and the root
    // itself is eventually an empty leaf.
    for (int value = 0; value < kKeyCount; ++value) {
        auto k = key(value);
        ASSERT_TRUE(ih->delete_entry(k.data(), Rid{1, value}, nullptr));
    }
    // Without this the test could pass vacuously. The old gate rejected any node
    // with `size <= 0`, so the existence of a reachable zero-key leaf is exactly
    // what used to force a rebuild.
    int empty_leaves = 0;
    {
        auto structure_guard = ih->lock_shared();
        for (page_id_t page_no = IX_INIT_ROOT_PAGE; page_no < ih->file_hdr_->num_pages_; ++page_no) {
            IxNodeHandle node;
            ih->fetch_node_into(page_no, node);
            if (node.is_leaf_page() && node.get_size() == 0) {
                ++empty_leaves;
            }
            ih->unpin_if_not_cached(node.get_page_id());
        }
    }
    ASSERT_GT(empty_leaves, 0) << "no leaf was emptied, so this test checks nothing";
    EXPECT_TRUE(ih->validate_structure()) << "an emptied leaf was reported as damage";

    crash_and_reopen_storage(ih);
    auto reopened = open_index();
    EXPECT_TRUE(reopened->validate_structure());
}

} // namespace
