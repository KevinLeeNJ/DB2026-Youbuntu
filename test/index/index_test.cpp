/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#undef NDEBUG

#define private public
#include "index/ix.h"
#include "optimizer/planner.h"
#undef private

#include <array>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "common/context.h"
#include "errors.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_index_skip_scan.h"
#include "execution/prepared_select_descriptor.h"
#include "execution/executor_delete.h"
#include "execution/executor_insert.h"
#include "execution/executor_update.h"
#include "gtest/gtest.h"
#include "record/rm.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include "system/sm_meta.h"

namespace {

class IndexHandleTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager;
    std::unique_ptr<IxManager> ix_manager;
    std::string table_name;
    std::vector<ColMeta> cols;

    void SetUp() override {
        table_name = "index_handle_test_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        disk_manager = std::make_unique<DiskManager>();
        buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
        ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
        cols = {ColMeta{.tab_name = table_name,
                        .name = "id",
                        .type = TYPE_INT,
                        .len = static_cast<int>(sizeof(int)),
                        .offset = 0,
                        .index = true}};
        cleanup();
        ix_manager->create_index(table_name, cols);
    }

    void TearDown() override {
        cleanup();
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
        std::vector<char> buf(sizeof(int));
        std::memcpy(buf.data(), &value, sizeof(int));
        return buf;
    }

    struct LeafSnapshot {
        page_id_t page_no;
        std::vector<int> keys;
    };

    std::vector<LeafSnapshot> leaf_snapshots(IxIndexHandle* ih) {
        std::vector<LeafSnapshot> leaves;
        page_id_t page_no = ih->file_hdr_->first_leaf_;
        while (page_no != IX_LEAF_HEADER_PAGE) {
            IxNodeHandle* leaf = ih->fetch_node(page_no);
            LeafSnapshot snapshot{page_no, {}};
            for (int i = 0; i < leaf->get_size(); ++i) {
                int value = 0;
                std::memcpy(&value, leaf->get_key(i), sizeof(int));
                snapshot.keys.push_back(value);
            }
            page_id_t next_leaf = leaf->get_next_leaf();
            buffer_pool_manager->unpin_page(leaf->get_page_id(), false);
            delete leaf;
            leaves.push_back(std::move(snapshot));
            page_no = next_leaf;
        }
        return leaves;
    }
};

TEST(IxNodeHandleTest, FindsChildBySeparatorWithSafeFallbacks) {
    constexpr int order = 8;
    IxFileHdr file_hdr(IX_NO_PAGE, 4, 10, 1, sizeof(int), order, IxKeysSize(order + 1, sizeof(int)), 101, 103);
    file_hdr.col_types_ = {TYPE_INT};
    file_hdr.col_lens_ = {sizeof(int)};

    Page parent_page;
    Page child_page;
    std::memset(parent_page.get_data(), 0, PAGE_SIZE);
    std::memset(child_page.get_data(), 0, PAGE_SIZE);
    parent_page.id_ = PageId{1, 10};
    child_page.id_ = PageId{1, 102};

    IxNodeHandle parent(&file_hdr, &parent_page);
    IxNodeHandle child(&file_hdr, &child_page);
    parent.set_parent_page_no(IX_NO_PAGE);
    child.set_parent_page_no(parent.get_page_no());

    for (const auto [separator, page_no] : {std::pair{10, 101}, std::pair{20, 102}, std::pair{30, 103}}) {
        parent.insert_pair(parent.get_size(), reinterpret_cast<const char*>(&separator), Rid{page_no, -1});
    }
    int child_first_key = 20;
    child.insert_pair(0, reinterpret_cast<const char*>(&child_first_key), Rid{7, 0});

    EXPECT_EQ(parent.find_child(&child), 1);

    child_first_key = 25;
    child.set_key(0, reinterpret_cast<const char*>(&child_first_key));
    EXPECT_EQ(parent.find_child(&child), 1);

    int duplicate_separator = 20;
    parent.set_key(2, reinterpret_cast<const char*>(&duplicate_separator));
    child_page.id_ = PageId{1, 103};
    child.set_key(0, reinterpret_cast<const char*>(&duplicate_separator));
    EXPECT_EQ(parent.find_child(&child), 2);
}

TEST(IxNodeHandleTest, SignedIntegerAndCompositeKeysKeepNumericOrder) {
    constexpr int order = 8;
    IxFileHdr file_hdr(IX_NO_PAGE, 4, 2, 1, sizeof(int), order, IxKeysSize(order + 1, sizeof(int)), 2, 2);
    file_hdr.col_types_ = {TYPE_INT};
    file_hdr.col_lens_ = {sizeof(int)};
    Page page;
    page.id_ = PageId{1, 2};
    std::memset(page.get_data(), 0, PAGE_SIZE);
    IxNodeHandle node(&file_hdr, &page);

    for (int value : {0, std::numeric_limits<int>::max(), -1, std::numeric_limits<int>::min()}) {
        node.insert(reinterpret_cast<const char*>(&value), Rid{1, value});
    }
    ASSERT_EQ(node.get_size(), 4);
    for (int index = 1; index < node.get_size(); ++index) {
        EXPECT_LT(node.key_at(index - 1), node.key_at(index));
    }
    for (int index = 0; index < node.get_size(); ++index) {
        EXPECT_EQ(node.lower_bound(node.get_key(index)), index);
        EXPECT_EQ(node.upper_bound(node.get_key(index)), index + 1);
    }
    EXPECT_EQ(node.key_at(0), std::numeric_limits<int>::min());
    EXPECT_EQ(node.key_at(node.get_size() - 1), std::numeric_limits<int>::max());

    IxFileHdr composite_hdr(IX_NO_PAGE, 4, 2, 2, 2 * static_cast<int>(sizeof(int)), order,
                            IxKeysSize(order + 1, 2 * sizeof(int)), 2, 2);
    composite_hdr.col_types_ = {TYPE_INT, TYPE_INT};
    composite_hdr.col_lens_ = {sizeof(int), sizeof(int)};
    Page composite_page;
    composite_page.id_ = PageId{1, 3};
    std::memset(composite_page.get_data(), 0, PAGE_SIZE);
    IxNodeHandle composite(&composite_hdr, &composite_page);
    const std::array<std::array<int, 2>, 5> keys{{
        {0, std::numeric_limits<int>::max()},
        {std::numeric_limits<int>::min(), -1},
        {std::numeric_limits<int>::max(), std::numeric_limits<int>::min()},
        {0, std::numeric_limits<int>::min()},
        {std::numeric_limits<int>::min(), std::numeric_limits<int>::max()},
    }};
    for (const auto& key : keys) {
        composite.insert(reinterpret_cast<const char*>(key.data()), Rid{1, key[1]});
    }
    ASSERT_EQ(composite.get_size(), static_cast<int>(keys.size()));
    for (int index = 1; index < composite.get_size(); ++index) {
        EXPECT_LT(ix_compare(composite.get_key(index - 1), composite.get_key(index), composite_hdr.col_types_,
                             composite_hdr.col_lens_),
                  0);
    }
    for (int index = 0; index < composite.get_size(); ++index) {
        EXPECT_EQ(composite.lower_bound(composite.get_key(index)), index);
        EXPECT_EQ(composite.upper_bound(composite.get_key(index)), index + 1);
    }
}

TEST_F(IndexHandleTest, InsertsUniqueKeysAndFindsValues) {
    auto ih = open_index();
    auto k10 = key(10);
    auto k20 = key(20);

    ih->insert_entry(k10.data(), Rid{1, 10}, nullptr);
    ih->insert_entry(k20.data(), Rid{1, 20}, nullptr);

    std::vector<Rid> result;
    EXPECT_TRUE(ih->get_value(k10.data(), &result, nullptr));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].page_no, 1);
    EXPECT_EQ(result[0].slot_no, 10);

    result.clear();
    EXPECT_TRUE(ih->get_value(k20.data(), &result, nullptr));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].slot_no, 20);

    close_index(ih);
}

TEST_F(IndexHandleTest, UniqueLookupFindsSingleKey) {
    auto ih = open_index();
    for (int value = 0; value < 2000; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{7, value}, nullptr);
    }

    auto found = ih->lookup_unique(key(137).data());
    ASSERT_EQ(found.status, UniqueLookupStatus::Unique);
    EXPECT_EQ(found.rid.page_no, 7);
    EXPECT_EQ(found.rid.slot_no, 137);

    const auto missing = ih->lookup_unique(key(9999).data());
    EXPECT_EQ(missing.status, UniqueLookupStatus::NotFound);

    std::vector<page_id_t> resident_pages(ih->resident_internal_pages_.begin(), ih->resident_internal_pages_.end());
    ASSERT_FALSE(resident_pages.empty());
    for (page_id_t page_no : resident_pages) {
        EXPECT_TRUE(buffer_pool_manager->is_page_resident(PageId{ih->fd_, page_no}));
        ASSERT_EQ(buffer_pool_manager->get_residency_class(PageId{ih->fd_, page_no}), ResidencyClass::IndexInternal);
    }

    const int index_fd = ih->fd_;
    close_index(ih);
    for (page_id_t page_no : resident_pages) {
        EXPECT_FALSE(buffer_pool_manager->is_page_resident(PageId{index_fd, page_no}));
    }
}

TEST_F(IndexHandleTest, RejectsDuplicateKeys) {
    auto ih = open_index();
    auto k10 = key(10);

    ih->insert_entry(k10.data(), Rid{1, 10}, nullptr);

    EXPECT_THROW(ih->insert_entry(k10.data(), Rid{2, 10}, nullptr), IndexEntryExistsError);

    close_index(ih);
}

TEST_F(IndexHandleTest, ScansRangeInKeyOrderAcrossSplits) {
    auto ih = open_index();
    for (int value = 1000; value >= 0; --value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{value + 1, value}, nullptr);
    }

    auto lower_key = key(123);
    auto upper_key = key(130);
    IxScan scan(ih.get(), ih->lower_bound(lower_key.data()), ih->upper_bound(upper_key.data()),
                buffer_pool_manager.get());

    std::vector<int> slots;
    while (!scan.is_end()) {
        slots.push_back(scan.rid().slot_no);
        scan.next();
    }

    EXPECT_EQ(slots, std::vector<int>({123, 124, 125, 126, 127, 128, 129, 130}));

    close_index(ih);
}

TEST_F(IndexHandleTest, ReverseScanWalksLeafLinksWithoutReSeek) {
    auto ih = open_index();
    for (int value = 0; value < 200; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    std::vector<int> values;
    {
        IxScan scan(ih.get(), ih->leaf_begin(), ih->leaf_end(), buffer_pool_manager.get(), true, false,
                    ScanDirection::Backward);
        while (!scan.is_end()) {
            values.push_back(scan.rid().slot_no);
            scan.next();
        }
    }

    ASSERT_EQ(values.size(), 200u);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(values[i], 199 - static_cast<int>(i));
    }
    close_index(ih);
}

TEST_F(IndexHandleTest, AppendSplitsPreserveOrder) {
    auto ih = open_index();
    for (int value = 0; value < 2000; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    std::vector<int> values;
    IxScan scan(ih.get(), ih->leaf_begin(), ih->leaf_end(), buffer_pool_manager.get());
    while (!scan.is_end()) {
        values.push_back(scan.rid().slot_no);
        scan.next();
    }
    ASSERT_EQ(values.size(), 2000u);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(values[i], static_cast<int>(i));
    }
    close_index(ih);
}

TEST_F(IndexHandleTest, HybridUsesPinnedCursorForSingleLeafRange) {
    auto ih = open_index();
    for (int value : {10, 20, 30}) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    auto lower_key = key(10);
    auto upper_key = key(20);
    ih->refresh_page_residency();
    Iid lower = ih->lower_bound(lower_key.data());
    Iid upper = ih->upper_bound(upper_key.data());
    ASSERT_EQ(lower.page_no, upper.page_no);

    std::vector<int> values;
    {
        IxScan scan(ih.get(), lower, upper, buffer_pool_manager.get(), true, true);
        while (!scan.is_end()) {
            int value = 0;
            std::memcpy(&value, scan.key(), sizeof(value));
            values.push_back(value);
            scan.next();
        }
    }
    EXPECT_EQ(values, std::vector<int>({10, 20}));

    Page* cached_root = ih->cached_page(ih->file_hdr_->root_page_);
    if (cached_root == nullptr) {
        ADD_FAILURE() << "refresh_page_residency did not cache the leaf root";
        close_index(ih);
        return;
    }
    const int baseline_pin_count = cached_root->pin_count_.load(std::memory_order_relaxed);
    {
        IxScan scan(ih.get(), lower, upper, buffer_pool_manager.get(), true, true);
        EXPECT_EQ(scan.pinned_leaf_page_, cached_root);
        EXPECT_FALSE(scan.pinned_leaf_has_bpm_pin_);
    }
    EXPECT_EQ(cached_root->pin_count_.load(std::memory_order_relaxed), baseline_pin_count);
    close_index(ih);
}

TEST_F(IndexHandleTest, SingleLeafHybridScanDoesNotRetainTreeLatch) {
    auto ih = open_index();
    for (int value = 0; value < 2000; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    auto lower_key = key(10);
    auto upper_key = key(20);
    auto scan = std::make_unique<IxScan>(ih.get(), ih->lower_bound(lower_key.data()), ih->upper_bound(upper_key.data()),
                                         buffer_pool_manager.get());
    std::atomic<bool> writer_done{false};
    std::thread writer([&] {
        for (int value = 5000; value < 5400; ++value) {
            auto k = key(value);
            ih->insert_entry(k.data(), Rid{1, value}, nullptr);
        }
        writer_done.store(true, std::memory_order_release);
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (!writer_done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    const bool writer_progressed_while_scan_alive = writer_done.load(std::memory_order_acquire);
    scan.reset();
    writer.join();

    EXPECT_TRUE(writer_progressed_while_scan_alive);
    close_index(ih);
}

TEST_F(IndexHandleTest, CoupledResumeReseeksAfterEarlierLeafLocalDelete) {
    auto ih = open_index();
    for (int value = 0; value < 2500; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    std::vector<int> values;
    {
        IxScan scan(ih.get(), ih->leaf_begin(), ih->leaf_end(), buffer_pool_manager.get());
        ASSERT_TRUE(scan.coupled_mode_);
        const size_t first_batch_size = scan.batch_.size();
        ASSERT_GT(first_batch_size, 2u);

        values.reserve(2500);
        for (size_t index = 0; index < first_batch_size; ++index) {
            values.push_back(scan.rid().slot_no);
            if (index + 1 == first_batch_size) {
                const int earlier_value = values.back() - 1;
                const auto epoch_before = ih->topology_epoch_.load(std::memory_order_relaxed);
                auto earlier_key = key(earlier_value);
                ASSERT_TRUE(ih->delete_entry(earlier_key.data(), nullptr));
                EXPECT_EQ(ih->topology_epoch_.load(std::memory_order_relaxed), epoch_before);
            }
            scan.next();
        }
        while (!scan.is_end()) {
            values.push_back(scan.rid().slot_no);
            scan.next();
        }
    }

    ASSERT_EQ(values.size(), 2500u);
    for (int value = 0; value < 2500; ++value) {
        EXPECT_EQ(values[static_cast<size_t>(value)], value);
    }
    close_index(ih);
}

TEST_F(IndexHandleTest, EqualRangeMatchesLowerBoundPlusUpperBound) {
    auto ih = open_index();
    // Insert enough keys to cause several leaf splits (btree_order is small).
    for (int value = 0; value < 2000; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    // For every key (including non-existent ones at boundaries), verify that
    // equal_range produces the same IxScan result as lower_bound + upper_bound.
    for (int value = 0; value < 2000; ++value) {
        auto k = key(value);

        Iid lo1 = ih->lower_bound(k.data());
        Iid hi1 = ih->upper_bound(k.data());

        auto [lo2, hi2] = ih->equal_range(k.data());

        // Collect rids via the two-visit approach.
        std::vector<int> rids1;
        IxScan scan1(ih.get(), lo1, hi1, buffer_pool_manager.get());
        while (!scan1.is_end()) {
            rids1.push_back(scan1.rid().slot_no);
            scan1.next();
        }

        // Collect rids via equal_range.
        std::vector<int> rids2;
        IxScan scan2(ih.get(), lo2, hi2, buffer_pool_manager.get());
        while (!scan2.is_end()) {
            rids2.push_back(scan2.rid().slot_no);
            scan2.next();
        }

        EXPECT_EQ(rids1, rids2) << "Mismatch at key=" << value;
        ASSERT_EQ(rids1.size(), 1u) << "Expected exactly one entry for key=" << value;
        EXPECT_EQ(rids1[0], value);
    }

    // Also test non-existent keys (gaps after deletion would be ideal, but
    // with all keys present, test boundary values).
    for (int value : {-1, 2000, 2001}) {
        auto k = key(value);
        Iid lo1 = ih->lower_bound(k.data());
        Iid hi1 = ih->upper_bound(k.data());
        auto [lo2, hi2] = ih->equal_range(k.data());

        std::vector<int> rids1;
        IxScan scan1(ih.get(), lo1, hi1, buffer_pool_manager.get());
        while (!scan1.is_end()) {
            rids1.push_back(scan1.rid().slot_no);
            scan1.next();
        }

        std::vector<int> rids2;
        IxScan scan2(ih.get(), lo2, hi2, buffer_pool_manager.get());
        while (!scan2.is_end()) {
            rids2.push_back(scan2.rid().slot_no);
            scan2.next();
        }

        EXPECT_EQ(rids1, rids2) << "Mismatch at non-existent key=" << value;
        EXPECT_TRUE(rids1.empty()) << "Expected empty result for non-existent key=" << value;
    }

    close_index(ih);
}

TEST_F(IndexHandleTest, ReopenRestoresPageAllocationCursor) {
    auto ih = open_index();
    for (int value = 0; value < 1000; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }
    close_index(ih);

    // Simulate a new server process: fd2pageno_ is process-local and must be
    // reconstructed from the persisted index header when the index is opened.
    ix_manager.reset();
    buffer_pool_manager.reset();
    disk_manager.reset();
    disk_manager = std::make_unique<DiskManager>();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    ih = open_index();

    for (int value = 1000; value < 2000; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    auto k = key(1500);
    std::vector<Rid> result;
    ASSERT_TRUE(ih->get_value(k.data(), &result, nullptr));
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result.front(), (Rid{1, 1500}));
    close_index(ih);
}

TEST_F(IndexHandleTest, CachesMultiLevelInternalPagesAcrossReopen) {
    cleanup();
    cols = {ColMeta{.tab_name = table_name, .name = "id", .type = TYPE_STRING, .len = 128, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    auto ih = open_index();

    const auto wide_key = [](int value) {
        std::vector<char> buf(128, 0);
        const std::string encoded = std::to_string(value);
        std::memcpy(buf.data(), encoded.data(), encoded.size());
        return buf;
    };
    for (int value = 0; value < 4000; ++value) {
        auto k = wide_key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    ih->refresh_page_residency();
    ASSERT_GT(ih->cached_internal_pages_.size(), 1u);

    const auto cached = *ih->cached_internal_pages_.begin();
    ASSERT_NE(cached.second, nullptr);
    const int pin_count_before = cached.second->pin_count_;
    {
        auto structure_guard = ih->lock_shared();
        IxNodeHandle node;
        ih->fetch_node_into(cached.first, node);
        EXPECT_EQ(node.page, cached.second);
        EXPECT_EQ(cached.second->pin_count_, pin_count_before);
        ih->unpin_if_not_cached(node.get_page_id());
        EXPECT_EQ(cached.second->pin_count_, pin_count_before);
    }

    const int index_fd = ih->fd_;
    close_index(ih);
    for (page_id_t page_no : {cached.first}) {
        EXPECT_FALSE(buffer_pool_manager->is_page_resident(PageId{index_fd, page_no}));
    }

    ix_manager.reset();
    buffer_pool_manager.reset();
    disk_manager.reset();
    disk_manager = std::make_unique<DiskManager>();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    ih = open_index();
    ih->refresh_page_residency();
    ASSERT_GT(ih->cached_internal_pages_.size(), 1u);

    auto lookup_key = wide_key(1379);
    std::vector<Rid> result;
    ASSERT_TRUE(ih->get_value(lookup_key.data(), &result, nullptr));
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result.front(), (Rid{1, 1379}));
    close_index(ih);
}

TEST_F(IndexHandleTest, DuplicateKeyRangeSpansLeavesAndDeleteByRidRemovesOne) {
    auto ih = open_index();
    auto duplicate_key = key(777);

    std::vector<Rid> inserted;
    for (int i = 0; i < 1000; ++i) {
        Rid rid{i + 1, i};
        inserted.push_back(rid);
        ih->insert_entry(duplicate_key.data(), rid, nullptr, true);
    }

    std::vector<Rid> result;
    ASSERT_TRUE(ih->get_value(duplicate_key.data(), &result, nullptr));
    ASSERT_EQ(result.size(), inserted.size());

    std::vector<Rid> fast_result;
    ih->lookup_equal(duplicate_key.data(), fast_result);
    EXPECT_EQ(fast_result, inserted);

    const auto duplicate_lookup = ih->lookup_unique(duplicate_key.data());
    EXPECT_EQ(duplicate_lookup.status, UniqueLookupStatus::Duplicate);

    Rid target = inserted[inserted.size() / 2];
    EXPECT_TRUE(ih->delete_entry(duplicate_key.data(), target, nullptr));

    result.clear();
    ASSERT_TRUE(ih->get_value(duplicate_key.data(), &result, nullptr));
    EXPECT_EQ(result.size(), inserted.size() - 1);
    EXPECT_TRUE(std::none_of(result.begin(), result.end(), [&](const Rid& rid) { return rid == target; }));

    close_index(ih);
}

TEST_F(IndexHandleTest, LeafLocalMutationDoesNotInvalidateTopologyEpoch) {
    auto ih = open_index();
    for (int value = 0; value < 200; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    const auto leaves = leaf_snapshots(ih.get());
    const int max_size = ih->file_hdr_->btree_order_ + 1;
    bool tested = false;
    for (const auto& snapshot : leaves) {
        if (snapshot.keys.size() < 2 || static_cast<int>(snapshot.keys.size()) + 1 >= max_size) {
            continue;
        }

        auto duplicate_key = key(snapshot.keys[1]);
        const Rid duplicate_rid{9001, snapshot.keys[1]};
        const auto epoch_before = ih->topology_epoch_.load(std::memory_order_relaxed);
        ih->insert_entry(duplicate_key.data(), duplicate_rid, nullptr, true);
        EXPECT_EQ(ih->topology_epoch_.load(std::memory_order_relaxed), epoch_before);
        ASSERT_TRUE(ih->delete_entry(duplicate_key.data(), duplicate_rid, nullptr));
        EXPECT_EQ(ih->topology_epoch_.load(std::memory_order_relaxed), epoch_before);
        tested = true;
        break;
    }
    EXPECT_TRUE(tested);

    close_index(ih);
}

TEST_F(IndexHandleTest, ConcurrentInsertDeleteByRidDoesNotCorruptTree) {
    auto ih = open_index();
    auto duplicate_key = key(888);

    constexpr int thread_count = 8;
    constexpr int iterations = 3000;
    std::atomic<bool> start{false};
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
        threads.emplace_back([&, thread_id] {
            while (!start.load(std::memory_order_acquire)) {
            }
            for (int i = 0; i < iterations; ++i) {
                Rid rid{thread_id + 1, i};
                try {
                    ih->insert_entry(duplicate_key.data(), rid, nullptr, true);
                    if (!ih->delete_entry(duplicate_key.data(), rid, nullptr)) {
                        failures.fetch_add(1, std::memory_order_relaxed);
                    }
                } catch (...) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0);

    std::vector<Rid> result;
    bool found = ih->get_value(duplicate_key.data(), &result, nullptr);
    EXPECT_FALSE(found);
    EXPECT_TRUE(result.empty());

    close_index(ih);
}

TEST_F(IndexHandleTest, LongScanAllowsStructuralChangesWithoutMissingOrDuplicatingEntries) {
    auto ih = open_index();
    for (int value = 0; value < 2000; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{value + 1, value}, nullptr);
    }

    auto inserted_key = key(5000);
    auto deleted_key = key(1500); // non-first entry in its leaf: optimistic delete path
    std::atomic<bool> scan_ready{false};
    std::atomic<bool> release_scan{false};
    std::atomic<bool> insert_done{false};
    std::atomic<bool> delete_done{false};
    std::atomic<bool> delete_result{false};
    std::vector<int> scanned_slots;

    std::thread reader([&] {
        IxScan scan(ih.get(), ih->leaf_begin(), ih->leaf_end(), buffer_pool_manager.get());
        scan_ready.store(true, std::memory_order_release);
        while (!release_scan.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        while (!scan.is_end()) {
            scanned_slots.push_back(scan.rid().slot_no);
            scan.next();
        }
    });

    std::thread inserter([&] {
        while (!scan_ready.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        ih->insert_entry(inserted_key.data(), Rid{5001, 5000}, nullptr);
        insert_done.store(true, std::memory_order_release);
    });

    std::thread deleter([&] {
        while (!scan_ready.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        delete_result.store(ih->delete_entry(deleted_key.data(), nullptr), std::memory_order_release);
        delete_done.store(true, std::memory_order_release);
    });

    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
    while (!scan_ready.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    ASSERT_TRUE(scan_ready.load(std::memory_order_acquire));

    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while ((!insert_done.load(std::memory_order_acquire) || !delete_done.load(std::memory_order_acquire)) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    // A scan object may outlive a leaf batch, but it must not retain the
    // structure latch while its consumer is processing that batch.
    EXPECT_TRUE(insert_done.load(std::memory_order_acquire));
    EXPECT_TRUE(delete_done.load(std::memory_order_acquire));

    release_scan.store(true, std::memory_order_release);
    reader.join();
    inserter.join();
    deleter.join();

    EXPECT_TRUE(insert_done.load(std::memory_order_acquire));
    EXPECT_TRUE(delete_done.load(std::memory_order_acquire));
    EXPECT_TRUE(delete_result.load(std::memory_order_acquire));
    ASSERT_EQ(scanned_slots.size(), 2000u);
    EXPECT_TRUE(std::is_sorted(scanned_slots.begin(), scanned_slots.end()));
    EXPECT_EQ(std::count(scanned_slots.begin(), scanned_slots.end(), 1500), 0);
    EXPECT_EQ(std::count(scanned_slots.begin(), scanned_slots.end(), 5000), 1);

    std::vector<Rid> result;
    EXPECT_TRUE(ih->get_value(inserted_key.data(), &result, nullptr));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], (Rid{5001, 5000}));

    result.clear();
    EXPECT_FALSE(ih->get_value(deleted_key.data(), &result, nullptr));
    EXPECT_TRUE(result.empty());

    IxScan final_scan(ih.get(), ih->leaf_begin(), ih->leaf_end(), buffer_pool_manager.get());
    std::vector<int> slots;
    while (!final_scan.is_end()) {
        slots.push_back(final_scan.rid().slot_no);
        final_scan.next();
    }

    ASSERT_EQ(slots.size(), 2000u);
    EXPECT_TRUE(std::is_sorted(slots.begin(), slots.end()));
    EXPECT_EQ(std::count(slots.begin(), slots.end(), 1500), 0);
    EXPECT_EQ(std::count(slots.begin(), slots.end(), 5000), 1);

    close_index(ih);
}

TEST_F(IndexHandleTest, DeletesKeysAndKeepsRemainingEntriesSearchable) {
    auto ih = open_index();
    for (int value = 0; value < 300; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{value + 1, value}, nullptr);
    }

    for (int value = 25; value < 275; value += 2) {
        auto k = key(value);
        EXPECT_TRUE(ih->delete_entry(k.data(), nullptr));
    }

    for (int value = 0; value < 300; ++value) {
        auto k = key(value);
        std::vector<Rid> result;
        bool found = ih->get_value(k.data(), &result, nullptr);
        if (value >= 25 && value < 275 && value % 2 == 1) {
            EXPECT_FALSE(found) << value;
        } else {
            EXPECT_TRUE(found) << value;
            ASSERT_EQ(result.size(), 1);
            EXPECT_EQ(result[0].slot_no, value);
        }
    }

    close_index(ih);
}

TEST_F(IndexHandleTest, DeleteMergesUnderfullLeafAndKeepsRangeScanCorrect) {
    auto ih = open_index();
    for (int value = 0; value < 2500; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{value + 1, value}, nullptr);
    }

    auto leaves = leaf_snapshots(ih.get());
    ASSERT_GE(leaves.size(), 3);
    ASSERT_FALSE(leaves[0].keys.empty());
    ASSERT_FALSE(leaves[1].keys.empty());
    ASSERT_FALSE(leaves[2].keys.empty());

    int before_empty = leaves[0].keys.back();
    int after_empty = leaves[2].keys.front();
    page_id_t emptied_page = leaves[1].page_no;
    for (int value : leaves[1].keys) {
        auto k = key(value);
        EXPECT_TRUE(ih->delete_entry(k.data(), nullptr)) << value;
    }

    leaves = leaf_snapshots(ih.get());
    auto removed_leaf = std::find_if(leaves.begin(), leaves.end(),
                                     [&](const LeafSnapshot& leaf) { return leaf.page_no == emptied_page; });
    EXPECT_EQ(removed_leaf, leaves.end());

    auto lower_key = key(before_empty);
    auto upper_key = key(after_empty);
    std::vector<int> slots;
    {
        IxScan scan(ih.get(), ih->lower_bound(lower_key.data()), ih->upper_bound(upper_key.data()),
                    buffer_pool_manager.get());
        while (!scan.is_end()) {
            slots.push_back(scan.rid().slot_no);
            scan.next();
        }
    }

    EXPECT_EQ(slots, std::vector<int>({before_empty, after_empty}));

    close_index(ih);
    ih = open_index();
    auto post_merge_key = key(10000);
    ih->insert_entry(post_merge_key.data(), Rid{10, 10000}, nullptr);
    std::vector<Rid> post_merge_result;
    ASSERT_TRUE(ih->get_value(post_merge_key.data(), &post_merge_result, nullptr));
    ASSERT_EQ(post_merge_result.size(), 1u);
    EXPECT_EQ(post_merge_result.front(), (Rid{10, 10000}));
    close_index(ih);
}

TEST_F(IndexHandleTest, LeftEdgeMergeReleasesTheFetchedRightNeighborPin) {
    auto ih = open_index();
    for (int value = 0; value < 2500; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    const auto leaves = leaf_snapshots(ih.get());
    ASSERT_GE(leaves.size(), 3u);
    const page_id_t left_page_no = leaves[0].page_no;
    const page_id_t fetched_neighbor_page_no = leaves[1].page_no;
    Page* fetched_neighbor = buffer_pool_manager->fetch_page(PageId{ih->fd_, fetched_neighbor_page_no});
    ASSERT_NE(fetched_neighbor, nullptr);
    ASSERT_TRUE(buffer_pool_manager->unpin_page(fetched_neighbor->get_page_id(), false));
    const int baseline_pin_count = fetched_neighbor->pin_count_;

    for (int value : leaves[0].keys) {
        auto k = key(value);
        ASSERT_TRUE(ih->delete_entry(k.data(), nullptr)) << value;
    }

    const auto after_merge = leaf_snapshots(ih.get());
    EXPECT_NE(std::find_if(after_merge.begin(), after_merge.end(),
                           [&](const LeafSnapshot& leaf) { return leaf.page_no == left_page_no; }),
              after_merge.end());
    EXPECT_EQ(std::find_if(after_merge.begin(), after_merge.end(),
                           [&](const LeafSnapshot& leaf) { return leaf.page_no == fetched_neighbor_page_no; }),
              after_merge.end());
    EXPECT_EQ(fetched_neighbor->pin_count_, baseline_pin_count);

    for (int value : {leaves[1].keys.front(), leaves[1].keys.back(), 2499}) {
        auto k = key(value);
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(k.data(), &result, nullptr));
        ASSERT_EQ(result.size(), 1u);
        EXPECT_EQ(result.front(), (Rid{1, value}));
    }
    close_index(ih);
}

TEST_F(IndexHandleTest, ConcurrentAppendWritersPreserveHintAndCacheSafety) {
    auto ih = open_index();
    for (int value = 0; value < 2000; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    constexpr int writer_count = 6;
    constexpr int values_per_writer = 500;
    constexpr int first_value = 10000;
    std::atomic<bool> start{false};
    std::atomic<int> failures{0};
    std::vector<std::thread> writers;
    for (int writer = 0; writer < writer_count; ++writer) {
        writers.emplace_back([&, writer] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int offset = 0; offset < values_per_writer; ++offset) {
                const int value = first_value + offset * writer_count + writer;
                try {
                    auto k = key(value);
                    ih->insert_entry(k.data(), Rid{writer + 2, value}, nullptr);
                } catch (...) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    std::thread cache_reader([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (int iteration = 0; iteration < 1000; ++iteration) {
            const int value = iteration % 2000;
            auto k = key(value);
            std::vector<Rid> result;
            if (!ih->get_value(k.data(), &result, nullptr) || result.size() != 1) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    start.store(true, std::memory_order_release);
    for (auto& writer : writers) {
        writer.join();
    }
    cache_reader.join();

    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0);
    for (int offset = 0; offset < values_per_writer; ++offset) {
        for (int writer = 0; writer < writer_count; ++writer) {
            const int value = first_value + offset * writer_count + writer;
            auto k = key(value);
            std::vector<Rid> result;
            ASSERT_TRUE(ih->get_value(k.data(), &result, nullptr));
            ASSERT_EQ(result.size(), 1u);
            EXPECT_EQ(result.front(), (Rid{writer + 2, value}));
        }
    }
    close_index(ih);
}

TEST_F(IndexHandleTest, ConcurrentReadsSurviveRootInternalSplitsAndMerges) {
    cleanup();
    cols = {ColMeta{.tab_name = table_name, .name = "id", .type = TYPE_STRING, .len = 128, .offset = 0, .index = true}};
    ix_manager->create_index(table_name, cols);
    auto ih = open_index();

    const auto wide_key = [](int value) {
        std::vector<char> buf(128, 0);
        std::snprintf(buf.data(), buf.size(), "%08d", value);
        return buf;
    };
    constexpr int stable_count = 4000;
    constexpr int transient_count = 1600;
    for (int value = 0; value < stable_count; ++value) {
        auto k = wide_key(value);
        ih->insert_entry(k.data(), Rid{1, value}, nullptr);
    }

    auto tree_height = [&] {
        auto structure_guard = ih->lock_shared();
        IxNodeHandle node;
        ih->fetch_node_into(ih->file_hdr_->root_page_, node);
        int height = 1;
        while (!node.is_leaf_page()) {
            const page_id_t child = node.value_at(0);
            ih->unpin_if_not_cached(node.get_page_id());
            ih->fetch_node_into(child, node);
            ++height;
        }
        ih->unpin_if_not_cached(node.get_page_id());
        return height;
    };
    ASSERT_GE(tree_height(), 3);

    std::atomic<bool> start{false};
    std::atomic<bool> inserted{false};
    std::atomic<bool> stop{false};
    std::atomic<int> failures{0};

    auto point_reader = [&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (int iteration = 0; iteration < 3000 && !stop.load(std::memory_order_acquire); ++iteration) {
            const int value = (iteration * 7919) % stable_count;
            auto k = wide_key(value);
            std::vector<Rid> result;
            if (!ih->get_value(k.data(), &result, nullptr) || result.size() != 1 || result.front() != Rid{1, value}) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::thread reader_a(point_reader);
    std::thread reader_b(point_reader);
    std::thread range_reader([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (int iteration = 0; iteration < 12 && !stop.load(std::memory_order_acquire); ++iteration) {
            auto lower_key = wide_key(500);
            auto upper_key = wide_key(3499);
            IxScan scan(ih.get(), ih->lower_bound(lower_key.data()), ih->upper_bound(upper_key.data()),
                        buffer_pool_manager.get());
            int expected = 500;
            while (!scan.is_end()) {
                if (scan.rid() != Rid{1, expected}) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                ++expected;
                scan.next();
            }
            if (expected != 3500) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });
    std::thread writer([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (int value = stable_count; value < stable_count + transient_count; ++value) {
            auto k = wide_key(value);
            ih->insert_entry(k.data(), Rid{1, value}, nullptr);
        }
        inserted.store(true, std::memory_order_release);
        for (int value = stable_count; value < stable_count + transient_count; ++value) {
            auto k = wide_key(value);
            if (!ih->delete_entry(k.data(), nullptr)) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        }
        stop.store(true, std::memory_order_release);
    });

    start.store(true, std::memory_order_release);
    reader_a.join();
    reader_b.join();
    range_reader.join();
    writer.join();

    EXPECT_TRUE(inserted.load(std::memory_order_acquire));
    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0);
    EXPECT_GE(tree_height(), 3);

    for (int value : {0, 137, 2048, stable_count - 1}) {
        auto k = wide_key(value);
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(k.data(), &result, nullptr));
        ASSERT_EQ(result.size(), 1u);
        EXPECT_EQ(result.front(), (Rid{1, value}));
    }
    close_index(ih);
}

} // namespace

namespace {

class ObservableIndexScanExecutor : public IndexScanExecutor {
public:
    using IndexScanExecutor::IndexScanExecutor;

    bool uses_single_rid_cursor() const {
        return single_rid_cursor_.has_value();
    }

    bool uses_empty_single_rid_cursor() const {
        return uses_single_rid_cursor() && single_rid_cursor_->is_end();
    }

    bool uses_rid_vector_cursor() const {
        return rid_vector_cursor_.has_value();
    }

    uint64_t constraint_rebuild_count() const {
        return constraint_rebuild_count_;
    }

#ifdef RMDB_ENABLE_JIT
    uint64_t jit_rebuild_count() const {
        return jit_rebuild_count_;
    }
#endif
};

class IndexScanFeatureTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager;
    std::unique_ptr<RmManager> rm_manager;
    std::unique_ptr<IxManager> ix_manager;
    std::unique_ptr<SmManager> sm_manager;
    std::string db_name = "index_scan_feature_test_db";
    bool opened = false;

    void SetUp() override {
        disk_manager = std::make_unique<DiskManager>();
        buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
        rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
        ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
        sm_manager = std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(),
                                                 ix_manager.get());
        if (sm_manager->is_dir(db_name)) {
            sm_manager->drop_db(db_name);
        }
        sm_manager->create_db(db_name);
        sm_manager->open_db(db_name);
        opened = true;
    }

    void TearDown() override {
        if (opened) {
            sm_manager->close_db();
            opened = false;
        }
        if (sm_manager->is_dir(db_name)) {
            sm_manager->drop_db(db_name);
        }
    }

    void create_warehouse() {
        std::vector<ColDef> cols = {{"w_id", TYPE_INT, 4}, {"name", TYPE_STRING, 8}};
        sm_manager->create_table("warehouse", cols, nullptr);
        insert_row(10, "qweruiop");
        insert_row(534, "asdfhjkl");
        insert_row(100, "qwerghjk");
        insert_row(500, "bgtyhnmj");
    }

    void create_two_int_table(const std::string& tab_name) {
        std::vector<ColDef> cols = {{"a", TYPE_INT, 4}, {"b", TYPE_INT, 4}};
        sm_manager->create_table(tab_name, cols, nullptr);
    }

    void create_three_int_table(const std::string& tab_name) {
        std::vector<ColDef> cols = {{"a", TYPE_INT, 4}, {"b", TYPE_INT, 4}, {"c", TYPE_INT, 4}};
        sm_manager->create_table(tab_name, cols, nullptr);
    }

    void create_scores() {
        std::vector<ColDef> cols = {{"sid", TYPE_INT, 4}, {"cid", TYPE_INT, 4}, {"score", TYPE_FLOAT, 8}};
        sm_manager->create_table("scores", cols, nullptr);
    }

    void insert_row(int w_id, const std::string& name) {
        Value id;
        id.set_int(w_id);
        Value name_val;
        name_val.set_str(name);
        char data_send[BUFFER_LENGTH] = {};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send, &offset);
        InsertExecutor executor(sm_manager.get(), "warehouse", {id, name_val}, &context);
        executor.Next();
    }

    void insert_two_ints(const std::string& tab_name, int a, int b) {
        Value av;
        av.set_int(a);
        Value bv;
        bv.set_int(b);
        char data_send[BUFFER_LENGTH] = {};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send, &offset);
        InsertExecutor executor(sm_manager.get(), tab_name, {av, bv}, &context);
        executor.Next();
    }

    void insert_three_ints(const std::string& tab_name, int a, int b, int c) {
        Value av;
        av.set_int(a);
        Value bv;
        bv.set_int(b);
        Value cv;
        cv.set_int(c);
        char data_send[BUFFER_LENGTH] = {};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send, &offset);
        InsertExecutor executor(sm_manager.get(), tab_name, {av, bv, cv}, &context);
        executor.Next();
    }

    void insert_score(int sid, int cid, double score) {
        Value sid_val;
        sid_val.set_int(sid);
        Value cid_val;
        cid_val.set_int(cid);
        Value score_val;
        score_val.set_float(score);
        char data_send[BUFFER_LENGTH] = {};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send, &offset);
        InsertExecutor executor(sm_manager.get(), "scores", {sid_val, cid_val, score_val}, &context);
        executor.Next();
    }

    static Condition int_cond(CompOp op, int value) {
        Condition cond;
        cond.lhs_col = {"warehouse", "w_id"};
        cond.op = op;
        cond.is_rhs_val = true;
        cond.rhs_val.set_int(value);
        return cond;
    }

    static Condition string_cond(CompOp op, const std::string& value) {
        Condition cond;
        cond.lhs_col = {"warehouse", "name"};
        cond.op = op;
        cond.is_rhs_val = true;
        cond.rhs_val.set_str(value);
        return cond;
    }

    static Condition table_int_cond(const std::string& tab_name, const std::string& col_name, CompOp op, int value) {
        Condition cond;
        cond.lhs_col = {tab_name, col_name};
        cond.op = op;
        cond.is_rhs_val = true;
        cond.rhs_val.set_int(value);
        return cond;
    }

    static SetClause set_int_clause(const std::string& tab_name, const std::string& col_name, int value) {
        SetClause clause;
        clause.lhs = {tab_name, col_name};
        clause.rhs.set_int(value);
        return clause;
    }

    static SetClause set_string_clause(const std::string& tab_name, const std::string& col_name,
                                       const std::string& value) {
        SetClause clause;
        clause.lhs = {tab_name, col_name};
        clause.rhs.set_str(value);
        return clause;
    }

    std::vector<int> scan_ids(const std::vector<Condition>& conds, const std::vector<std::string>& index_cols) {
        char data_send[BUFFER_LENGTH] = {};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send, &offset);
        IndexScanExecutor executor(sm_manager.get(), "warehouse", conds, index_cols, &context);
        std::vector<int> ids;
        for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
            auto rec = executor.Next();
            ids.push_back(*reinterpret_cast<int*>(rec->data));
        }
        return ids;
    }

    std::vector<int> scan_descriptor_ids(const IndexScanDescriptor& descriptor) {
        char data_send[BUFFER_LENGTH] = {};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send, &offset);
        IndexScanExecutor executor(sm_manager.get(), descriptor, &context);
        std::vector<int> ids;
        for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
            auto rec = executor.Next();
            ids.push_back(*reinterpret_cast<int*>(rec->data));
        }
        return ids;
    }

    std::vector<int> scan_table_ints(const std::string& tab_name, const std::vector<Condition>& conds,
                                     const std::vector<std::string>& index_cols) {
        char data_send[BUFFER_LENGTH] = {};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send, &offset);
        IndexScanExecutor executor(sm_manager.get(), tab_name, conds, index_cols, &context);
        std::vector<int> values;
        for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
            auto rec = executor.Next();
            values.push_back(*reinterpret_cast<int*>(rec->data));
        }
        return values;
    }

    std::vector<std::pair<int, int>> scan_score_keys(const std::vector<Condition>& conds,
                                                     const std::vector<std::string>& index_cols) {
        char data_send[BUFFER_LENGTH] = {};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send, &offset);
        IndexScanExecutor executor(sm_manager.get(), "scores", conds, index_cols, &context);
        std::vector<std::pair<int, int>> values;
        for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
            auto rec = executor.Next();
            values.emplace_back(*reinterpret_cast<int*>(rec->data), *reinterpret_cast<int*>(rec->data + sizeof(int)));
        }
        return values;
    }

    std::vector<int> skip_scan_three_int_a_values(const std::vector<Condition>& conds,
                                                  const std::vector<std::string>& index_cols) {
        char data_send[BUFFER_LENGTH] = {};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send, &offset);
        IndexSkipScanExecutor executor(sm_manager.get(), "triples", conds, index_cols, &context);
        std::vector<int> values;
        for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
            auto rec = executor.Next();
            values.push_back(*reinterpret_cast<int*>(rec->data));
        }
        return values;
    }
};

TEST_F(IndexScanFeatureTest, UsesSingleColumnIndexForPointAndRangeScans) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id"}, nullptr);

    EXPECT_EQ(scan_ids({int_cond(OP_EQ, 10)}, {"w_id"}), std::vector<int>({10}));
    EXPECT_EQ(scan_ids({int_cond(OP_LT, 534), int_cond(OP_GT, 100)}, {"w_id"}), std::vector<int>({500}));
}

TEST_F(IndexScanFeatureTest, DescriptorPathMatchesLegacyConstruction) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id"}, nullptr);

    const std::vector<Condition> range_conditions = {int_cond(OP_GE, 100), int_cond(OP_LT, 534)};
    auto descriptor = IndexScanDescriptor::Build(sm_manager.get(), "warehouse", range_conditions, {"w_id"});

    EXPECT_EQ(descriptor.catalog_generation(), sm_manager->get_catalog_generation());
    ASSERT_EQ(descriptor.condition_layouts().size(), range_conditions.size());
    EXPECT_EQ(descriptor.condition_layouts()[0].lhs.offset, 0);
    EXPECT_EQ(descriptor.condition_layouts()[0].lhs.type, TYPE_INT);
    ASSERT_EQ(descriptor.compiled_index_conditions().size(), range_conditions.size());
    EXPECT_EQ(scan_descriptor_ids(descriptor), scan_ids(range_conditions, {"w_id"}));

    const std::vector<Condition> point_conditions = {int_cond(OP_EQ, 500)};
    auto point_descriptor = IndexScanDescriptor::Build(sm_manager.get(), "warehouse", point_conditions, {"w_id"});
    EXPECT_EQ(scan_descriptor_ids(point_descriptor), scan_ids(point_conditions, {"w_id"}));
}

TEST_F(IndexScanFeatureTest, DescriptorRejectsStaleCatalogGenerationBeforeResolvingHandles) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id"}, nullptr);
    auto descriptor = IndexScanDescriptor::Build(sm_manager.get(), "warehouse", {int_cond(OP_EQ, 100)}, {"w_id"});

    sm_manager->drop_index("warehouse", {"w_id"}, nullptr);
    sm_manager->create_index("warehouse", {"w_id"}, nullptr);

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    EXPECT_THROW((IndexScanExecutor(sm_manager.get(), descriptor, &context)), InternalError);

    auto refreshed = IndexScanDescriptor::Build(sm_manager.get(), "warehouse", {int_cond(OP_EQ, 100)}, {"w_id"});
    EXPECT_EQ(scan_descriptor_ids(refreshed), std::vector<int>({100}));
}

TEST_F(IndexScanFeatureTest, PreparedSelectDescriptorBindsEachRequestWithoutCloningPlan) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id"}, nullptr);

    auto scan_condition = int_cond(OP_GE, 10);
    scan_condition.rhs_val.lexical_slot = 0;
    auto filter_condition = string_cond(OP_EQ, "qweruiop");
    filter_condition.rhs_val.lexical_slot = 1;
    auto scan = std::make_unique<ScanPlan>(T_IndexScan, sm_manager.get(), "warehouse",
                                           std::vector<Condition>{scan_condition}, std::vector<std::string>{"w_id"});
    auto filter = std::make_unique<FilterPlan>(T_Filter, std::move(scan), std::vector<Condition>{filter_condition});
    SelectItem item;
    item.expr.type = QueryExprType::COLUMN;
    item.expr.col = {"warehouse", "w_id"};
    item.output_name = "selected_id";
    auto projection = std::make_unique<ProjectionPlan>(T_Projection, std::move(filter), std::vector<SelectItem>{item},
                                                       std::vector<std::string>{"selected_id"});
    DMLPlan select(T_select, std::move(projection), "warehouse", {}, {}, {});
    auto descriptor = PreparedSelectDescriptor::Build(select, sm_manager.get());
    ASSERT_NE(descriptor, nullptr);
    ASSERT_EQ(descriptor->nodes().size(), 3U);
    EXPECT_TRUE(std::holds_alternative<PreparedIndexScanNode>(descriptor->nodes()[0]));
    EXPECT_TRUE(std::holds_alternative<PreparedFilterNode>(descriptor->nodes()[1]));
    EXPECT_TRUE(std::holds_alternative<PreparedProjectionNode>(descriptor->nodes()[2]));

    auto execute = [&](const std::string& sql) -> std::vector<int> {
        auto lexical = parser::normalize_sql(sql, false);
        EXPECT_TRUE(lexical);
        char data_send[BUFFER_LENGTH] = {};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send, &offset);
        auto executor = descriptor->Instantiate(lexical, sm_manager.get(), &context);
        EXPECT_NE(executor, nullptr);
        std::vector<int> values;
        if (executor == nullptr) {
            return values;
        }
        for (executor->beginTuple(); !executor->is_end(); executor->nextTuple()) {
            auto tuple = executor->current();
            if (!tuple) {
                ADD_FAILURE() << "prepared executor returned an empty current tuple";
                break;
            }
            values.push_back(read_unaligned<int>(tuple.data));
        }
        return values;
    };

    EXPECT_EQ(execute("select w_id from warehouse where w_id >= 10 and name = 'qweruiop';"), std::vector<int>({10}));
    EXPECT_EQ(execute("select w_id from warehouse where w_id >= 100 and name = 'qwerghjk';"), std::vector<int>({100}));
    const auto pool_stats = descriptor->pool_stats();
    EXPECT_EQ(pool_stats.constructed, 1U);
    EXPECT_EQ(pool_stats.reused, 1U);
    EXPECT_EQ(pool_stats.available, 1U);

    auto lexical = parser::normalize_sql("select w_id from warehouse where w_id >= 500 and name = 'asdfghjk';", false);
    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    auto executor = descriptor->Instantiate(lexical, sm_manager.get(), &context);
    ASSERT_NE(executor, nullptr);
    const auto& ssi_conditions = executor->scan_conditions_ref();
    ASSERT_EQ(ssi_conditions.size(), 2U);
    EXPECT_EQ(ssi_conditions[0].rhs_val.int_val, 500);
    EXPECT_EQ(ssi_conditions[1].rhs_val.str_val, "asdfghjk");
}

TEST_F(IndexScanFeatureTest, PreparedSelectDescriptorComposesSharedAggregateWithRequestLocalState) {
    const std::string table_name = "prepared_agg";
    create_two_int_table(table_name);
    insert_two_ints(table_name, 1, 10);
    insert_two_ints(table_name, 1, 20);
    insert_two_ints(table_name, 2, 7);
    sm_manager->create_index(table_name, {"a", "b"}, nullptr);

    auto scan_condition = table_int_cond(table_name, "a", OP_GE, 1);
    scan_condition.rhs_val.lexical_slot = 0;
    auto filter_condition = table_int_cond(table_name, "b", OP_GE, 10);
    filter_condition.rhs_val.lexical_slot = 1;
    auto scan = std::make_unique<ScanPlan>(T_IndexScan, sm_manager.get(), table_name,
                                           std::vector<Condition>{scan_condition}, std::vector<std::string>{"a", "b"});
    auto filter = std::make_unique<FilterPlan>(T_Filter, std::move(scan), std::vector<Condition>{filter_condition});

    AggExpr sum;
    sum.type = AggType::SUM;
    sum.col = {table_name, "b"};
    sum.display_name = "SUM(b)";
    AggExpr count;
    count.type = AggType::COUNT;
    count.is_star = true;
    count.display_name = "COUNT(*)";
    HavingCondition having;
    having.lhs.type = QueryExprType::AGGREGATE;
    having.lhs.agg = sum;
    having.lhs.display_name = sum.display_name;
    having.op = OP_GT;
    having.is_rhs_val = false;
    having.rhs_expr.type = QueryExprType::AGGREGATE;
    having.rhs_expr.agg = count;
    having.rhs_expr.display_name = count.display_name;

    auto aggregate =
        std::make_unique<AggregatePlan>(T_Aggregate, std::move(filter), std::vector<TabCol>{{table_name, "a"}},
                                        std::vector<AggExpr>{sum, count}, std::vector<HavingCondition>{having});
    SelectItem group_item;
    group_item.expr.type = QueryExprType::COLUMN;
    group_item.expr.col = {table_name, "a"};
    SelectItem sum_item;
    sum_item.expr.type = QueryExprType::AGGREGATE;
    sum_item.expr.agg = sum;
    sum_item.expr.display_name = sum.display_name;
    sum_item.output_name = sum.display_name;
    auto projection = std::make_unique<ProjectionPlan>(T_Projection, std::move(aggregate),
                                                       std::vector<SelectItem>{group_item, sum_item},
                                                       std::vector<std::string>{"a", sum.display_name});
    DMLPlan select(T_select, std::move(projection), table_name, {}, {}, {});

    auto descriptor = PreparedSelectDescriptor::Build(select, sm_manager.get());
    ASSERT_NE(descriptor, nullptr);
    ASSERT_EQ(descriptor->nodes().size(), 4U);
    EXPECT_TRUE(std::holds_alternative<PreparedIndexScanNode>(descriptor->nodes()[0]));
    EXPECT_TRUE(std::holds_alternative<PreparedFilterNode>(descriptor->nodes()[1]));
    ASSERT_TRUE(std::holds_alternative<PreparedAggregateNode>(descriptor->nodes()[2]));
    EXPECT_TRUE(std::holds_alternative<PreparedProjectionNode>(descriptor->nodes()[3]));
    const auto& shared_aggregate = std::get<PreparedAggregateNode>(descriptor->nodes()[2]).descriptor;
    ASSERT_NE(shared_aggregate, nullptr);
    EXPECT_EQ(shared_aggregate->group_cols().size(), 1U);
    EXPECT_EQ(shared_aggregate->aggregates().size(), 2U);
    EXPECT_EQ(shared_aggregate->having().size(), 1U);

    auto first_lexical = parser::normalize_sql(
        "select a, sum(b) from prepared_agg where a >= 1 and b >= 10 group by a having sum(b) > count(*);", false);
    auto second_lexical = parser::normalize_sql(
        "select a, sum(b) from prepared_agg where a >= 1 and b >= 20 group by a having sum(b) > count(*);", false);
    ASSERT_TRUE(first_lexical);
    ASSERT_TRUE(second_lexical);
    char first_buffer[BUFFER_LENGTH] = {};
    char second_buffer[BUFFER_LENGTH] = {};
    int first_offset = 0;
    int second_offset = 0;
    Context first_context(nullptr, nullptr, nullptr, first_buffer, &first_offset);
    Context second_context(nullptr, nullptr, nullptr, second_buffer, &second_offset);
    auto first = descriptor->Instantiate(first_lexical, sm_manager.get(), &first_context);
    auto second = descriptor->Instantiate(second_lexical, sm_manager.get(), &second_context);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);

    first->beginTuple();
    second->beginTuple();
    ASSERT_FALSE(first->is_end());
    ASSERT_FALSE(second->is_end());
    auto first_tuple = first->current();
    auto second_tuple = second->current();
    ASSERT_TRUE(first_tuple);
    ASSERT_TRUE(second_tuple);
    EXPECT_EQ(read_unaligned<int>(first_tuple.data), 1);
    EXPECT_EQ(read_unaligned<int>(first_tuple.data + sizeof(int)), 30);
    EXPECT_EQ(read_unaligned<int>(second_tuple.data), 1);
    EXPECT_EQ(read_unaligned<int>(second_tuple.data + sizeof(int)), 20);

    first.reset();
    second.reset();
    auto reused = descriptor->Instantiate(first_lexical, sm_manager.get(), &first_context);
    ASSERT_NE(reused, nullptr);
    reused->beginTuple();
    ASSERT_FALSE(reused->is_end());
    auto reused_tuple = reused->current();
    ASSERT_TRUE(reused_tuple);
    EXPECT_EQ(read_unaligned<int>(reused_tuple.data), 1);
    EXPECT_EQ(read_unaligned<int>(reused_tuple.data + sizeof(int)), 30);
    EXPECT_GE(descriptor->pool_stats().reused, 1U);
}

TEST_F(IndexScanFeatureTest, PreparedSelectDescriptorRejectsParameterizedHavingCapability) {
    const std::string table_name = "prepared_having";
    create_two_int_table(table_name);
    insert_two_ints(table_name, 1, 10);
    sm_manager->create_index(table_name, {"a", "b"}, nullptr);

    auto scan_condition = table_int_cond(table_name, "a", OP_GE, 1);
    scan_condition.rhs_val.lexical_slot = 0;
    auto scan = std::make_unique<ScanPlan>(T_IndexScan, sm_manager.get(), table_name,
                                           std::vector<Condition>{scan_condition}, std::vector<std::string>{"a", "b"});
    AggExpr sum;
    sum.type = AggType::SUM;
    sum.col = {table_name, "b"};
    sum.display_name = "SUM(b)";
    HavingCondition having;
    having.lhs.type = QueryExprType::AGGREGATE;
    having.lhs.agg = sum;
    having.lhs.display_name = sum.display_name;
    having.op = OP_GT;
    having.is_rhs_val = true;
    having.rhs_val.set_int(5);
    having.rhs_val.lexical_slot = 1;
    auto aggregate = std::make_unique<AggregatePlan>(T_Aggregate, std::move(scan), std::vector<TabCol>{},
                                                     std::vector<AggExpr>{sum}, std::vector<HavingCondition>{having});
    SelectItem item;
    item.expr.type = QueryExprType::AGGREGATE;
    item.expr.agg = sum;
    item.expr.display_name = sum.display_name;
    item.output_name = sum.display_name;
    auto projection = std::make_unique<ProjectionPlan>(
        T_Projection, std::move(aggregate), std::vector<SelectItem>{item}, std::vector<std::string>{sum.display_name});
    DMLPlan select(T_select, std::move(projection), table_name, {}, {}, {});

    EXPECT_EQ(PreparedSelectDescriptor::Build(select, sm_manager.get()), nullptr);
}

TEST_F(IndexScanFeatureTest, PreparedSelectDescriptorSupportsConcurrentRequestLocalFrames) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id"}, nullptr);

    auto condition = int_cond(OP_EQ, 10);
    condition.rhs_val.lexical_slot = 0;
    auto scan = std::make_unique<ScanPlan>(T_IndexScan, sm_manager.get(), "warehouse",
                                           std::vector<Condition>{condition}, std::vector<std::string>{"w_id"});
    SelectItem item;
    item.expr.type = QueryExprType::COLUMN;
    item.expr.col = {"warehouse", "w_id"};
    auto projection = std::make_unique<ProjectionPlan>(T_Projection, std::move(scan), std::vector<SelectItem>{item},
                                                       std::vector<std::string>{"w_id"}, true);
    DMLPlan select(T_select, std::move(projection), "warehouse", {}, {}, {});
    auto descriptor = PreparedSelectDescriptor::Build(select, sm_manager.get());
    ASSERT_NE(descriptor, nullptr);

    std::atomic<bool> failed{false};
    std::vector<std::thread> workers;
    for (int i = 0; i < 8; ++i) {
        workers.emplace_back([&, i] {
            const int expected = i % 2 == 0 ? 100 : 500;
            auto lexical = parser::normalize_sql(
                "select w_id from warehouse where w_id = " + std::to_string(expected) + ";", false);
            char data_send[BUFFER_LENGTH] = {};
            int offset = 0;
            Context context(nullptr, nullptr, nullptr, data_send, &offset);
            auto executor = descriptor->Instantiate(lexical, sm_manager.get(), &context);
            if (executor == nullptr) {
                failed = true;
                return;
            }
            executor->beginTuple();
            auto tuple = executor->current();
            if (!tuple || read_unaligned<int>(tuple.data) != expected) {
                failed = true;
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_FALSE(failed.load());
    const auto pool_stats = descriptor->pool_stats();
    EXPECT_GE(pool_stats.constructed, 1U);
    EXPECT_EQ(pool_stats.available, pool_stats.constructed);
}

TEST_F(IndexScanFeatureTest, PreparedSelectDescriptorFallsBackAfterCatalogGenerationChanges) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id"}, nullptr);
    auto condition = int_cond(OP_EQ, 100);
    condition.rhs_val.lexical_slot = 0;
    auto scan = std::make_unique<ScanPlan>(T_IndexScan, sm_manager.get(), "warehouse",
                                           std::vector<Condition>{condition}, std::vector<std::string>{"w_id"});
    SelectItem item;
    item.expr.type = QueryExprType::COLUMN;
    item.expr.col = {"warehouse", "w_id"};
    auto projection = std::make_unique<ProjectionPlan>(T_Projection, std::move(scan), std::vector<SelectItem>{item},
                                                       std::vector<std::string>{"w_id"}, true);
    DMLPlan select(T_select, std::move(projection), "warehouse", {}, {}, {});
    auto descriptor = PreparedSelectDescriptor::Build(select, sm_manager.get());
    ASSERT_NE(descriptor, nullptr);

    sm_manager->drop_index("warehouse", {"w_id"}, nullptr);
    sm_manager->create_index("warehouse", {"w_id"}, nullptr);
    auto lexical = parser::normalize_sql("select w_id from warehouse where w_id = 100;", false);
    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    EXPECT_EQ(descriptor->Instantiate(lexical, sm_manager.get(), &context), nullptr);
}

TEST_F(IndexScanFeatureTest, DegenerateClosedRangeUsesPointLookup) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id"}, nullptr);

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    ObservableIndexScanExecutor executor(sm_manager.get(), "warehouse", {int_cond(OP_GE, 100), int_cond(OP_LE, 100)},
                                         {"w_id"}, &context);

    executor.beginTuple();
    EXPECT_TRUE(executor.uses_single_rid_cursor());
    EXPECT_FALSE(executor.uses_rid_vector_cursor());
    std::vector<int> result;
    for (; !executor.is_end(); executor.nextTuple()) {
        auto rec = executor.Next();
        ASSERT_NE(rec, nullptr);
        result.push_back(*reinterpret_cast<int*>(rec->data));
    }
    EXPECT_EQ(result, std::vector<int>({100}));
}

TEST_F(IndexScanFeatureTest, CompoundEqAndDegenerateClosedRangeUsesPointLookup) {
    create_two_int_table("compound_rows");
    insert_two_ints("compound_rows", 7, 41);
    insert_two_ints("compound_rows", 7, 42);
    insert_two_ints("compound_rows", 7, 43);
    insert_two_ints("compound_rows", 8, 42);
    sm_manager->create_index("compound_rows", {"a", "b"}, nullptr);

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    ObservableIndexScanExecutor executor(sm_manager.get(), "compound_rows",
                                         {table_int_cond("compound_rows", "a", OP_EQ, 7),
                                          table_int_cond("compound_rows", "b", OP_GE, 42),
                                          table_int_cond("compound_rows", "b", OP_LE, 42)},
                                         {"a", "b"}, &context);

    executor.beginTuple();
    EXPECT_TRUE(executor.uses_single_rid_cursor());
    EXPECT_FALSE(executor.uses_rid_vector_cursor());
    std::vector<std::pair<int, int>> result;
    for (; !executor.is_end(); executor.nextTuple()) {
        auto rec = executor.Next();
        ASSERT_NE(rec, nullptr);
        result.emplace_back(*reinterpret_cast<int*>(rec->data), *reinterpret_cast<int*>(rec->data + sizeof(int)));
    }
    const std::vector<std::pair<int, int>> expected = {{7, 42}};
    EXPECT_EQ(result, expected);
}

TEST_F(IndexScanFeatureTest, ReusesCompiledConstraintsAcrossInjectedLookups) {
    create_two_int_table("lookup_rows");
    insert_two_ints("lookup_rows", 10, 1);
    insert_two_ints("lookup_rows", 20, 2);
    insert_two_ints("lookup_rows", 30, 3);
    sm_manager->create_index("lookup_rows", {"b"}, nullptr);

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    IndexScanExecutor executor(sm_manager.get(), "lookup_rows", {}, {"b"}, &context);

    auto scan_for_b = [&](int value) {
        executor.set_key_conditions({table_int_cond("lookup_rows", "b", OP_EQ, value)});
        std::vector<int> result;
        for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
            auto rec = executor.Next();
            result.push_back(*reinterpret_cast<int*>(rec->data));
        }
        return result;
    };

    EXPECT_EQ(scan_for_b(2), std::vector<int>({20}));
    EXPECT_EQ(scan_for_b(1), std::vector<int>({10}));
    EXPECT_EQ(scan_for_b(3), std::vector<int>({30}));
}

TEST_F(IndexScanFeatureTest, DirectLookupKeyReturnsMatchingRows) {
    create_two_int_table("lookup_direct");
    insert_two_ints("lookup_direct", 10, 7);
    insert_two_ints("lookup_direct", 20, 8);
    sm_manager->create_index("lookup_direct", {"b"}, nullptr);

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    ObservableIndexScanExecutor executor(sm_manager.get(), "lookup_direct", {}, {"b"}, &context);
    int lookup_value = 7;
    executor.bind_lookup_key(TabCol{"lookup_direct", "b"}, LookupKeyView{reinterpret_cast<const char*>(&lookup_value),
                                                                         sizeof(lookup_value), TYPE_INT});

    executor.beginTuple();
    EXPECT_TRUE(executor.uses_single_rid_cursor());
    EXPECT_FALSE(executor.uses_empty_single_rid_cursor());
    EXPECT_FALSE(executor.uses_rid_vector_cursor());

    std::vector<int> result;
    for (; !executor.is_end(); executor.nextTuple()) {
        auto rec = executor.Next();
        ASSERT_NE(rec, nullptr);
        result.push_back(*reinterpret_cast<int*>(rec->data));
    }
    EXPECT_EQ(result, std::vector<int>({10}));
}

TEST_F(IndexScanFeatureTest, ExactLookupMissUsesEmptySingleRidCursor) {
    create_two_int_table("lookup_miss");
    insert_two_ints("lookup_miss", 10, 7);
    sm_manager->create_index("lookup_miss", {"b"}, nullptr);

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    ObservableIndexScanExecutor executor(sm_manager.get(), "lookup_miss", {}, {"b"}, &context);
    int lookup_value = 8;
    executor.bind_lookup_key(TabCol{"lookup_miss", "b"}, LookupKeyView{reinterpret_cast<const char*>(&lookup_value),
                                                                       sizeof(lookup_value), TYPE_INT});

    executor.beginTuple();

    EXPECT_TRUE(executor.is_end());
    EXPECT_TRUE(executor.uses_empty_single_rid_cursor());
    EXPECT_FALSE(executor.uses_rid_vector_cursor());
}

TEST_F(IndexScanFeatureTest, ExactLookupFallsBackForDuplicateIndexKeys) {
    create_two_int_table("lookup_duplicates");
    sm_manager->create_index("lookup_duplicates", {"b"}, nullptr);

    const std::string csv_path = db_name + "_duplicate_lookup.csv";
    {
        std::ofstream out(csv_path);
        ASSERT_TRUE(out.is_open());
        out << "a,b\n10,7\n20,7\n";
    }
    sm_manager->load_csv_data(csv_path, "lookup_duplicates", nullptr);
    std::remove(csv_path.c_str());

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    ObservableIndexScanExecutor executor(sm_manager.get(), "lookup_duplicates", {}, {"b"}, &context);
    int lookup_value = 7;
    executor.bind_lookup_key(
        TabCol{"lookup_duplicates", "b"},
        LookupKeyView{reinterpret_cast<const char*>(&lookup_value), sizeof(lookup_value), TYPE_INT});

    std::vector<int> result;
    for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
        auto rec = executor.Next();
        ASSERT_NE(rec, nullptr);
        result.push_back(*reinterpret_cast<int*>(rec->data));
    }
    EXPECT_EQ(result, std::vector<int>({10, 20}));
    EXPECT_FALSE(executor.uses_single_rid_cursor());
    EXPECT_TRUE(executor.uses_rid_vector_cursor());
}

TEST_F(IndexScanFeatureTest, TypedLookupConvertsValuesWithoutRebuildingScanStructures) {
    create_two_int_table("typed_lookup_int");
    insert_two_ints("typed_lookup_int", 10, 7);
    insert_two_ints("typed_lookup_int", 20, 8);
    sm_manager->create_index("typed_lookup_int", {"b"}, nullptr);

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    ObservableIndexScanExecutor executor(sm_manager.get(), "typed_lookup_int", {}, {"b"}, &context);
    const auto constraint_builds = executor.constraint_rebuild_count();
#ifdef RMDB_ENABLE_JIT
    const auto jit_builds = executor.jit_rebuild_count();
#endif

    double integral = 7.0;
    executor.bind_lookup_key(TabCol{"typed_lookup_int", "b"},
                             LookupKeyView{reinterpret_cast<const char*>(&integral), sizeof(integral), TYPE_FLOAT});
    executor.beginTuple();
    ASSERT_FALSE(executor.is_end());
    EXPECT_EQ(read_unaligned<int>(executor.Next()->data), 10);
    EXPECT_EQ(executor.constraint_rebuild_count(), constraint_builds);
#ifdef RMDB_ENABLE_JIT
    EXPECT_EQ(executor.jit_rebuild_count(), jit_builds);
#endif

    double fractional = 7.5;
    executor.bind_lookup_key(TabCol{"typed_lookup_int", "b"},
                             LookupKeyView{reinterpret_cast<const char*>(&fractional), sizeof(fractional), TYPE_FLOAT});
    executor.beginTuple();
    EXPECT_TRUE(executor.is_end());
    EXPECT_TRUE(executor.uses_empty_single_rid_cursor());
    EXPECT_EQ(executor.constraint_rebuild_count(), constraint_builds);

    double nan = std::numeric_limits<double>::quiet_NaN();
    executor.bind_lookup_key(TabCol{"typed_lookup_int", "b"},
                             LookupKeyView{reinterpret_cast<const char*>(&nan), sizeof(nan), TYPE_FLOAT});
    executor.beginTuple();
    EXPECT_TRUE(executor.is_end());
    EXPECT_TRUE(executor.uses_empty_single_rid_cursor());

    double out_of_range = static_cast<double>(std::numeric_limits<int>::max()) * 2.0;
    executor.bind_lookup_key(
        TabCol{"typed_lookup_int", "b"},
        LookupKeyView{reinterpret_cast<const char*>(&out_of_range), sizeof(out_of_range), TYPE_FLOAT});
    executor.beginTuple();
    EXPECT_TRUE(executor.is_end());
    EXPECT_TRUE(executor.uses_empty_single_rid_cursor());
    EXPECT_EQ(executor.constraint_rebuild_count(), constraint_builds);
}

TEST_F(IndexScanFeatureTest, TypedLookupRejectsStringOverflowWithoutTruncatingProbe) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"name"}, nullptr);

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    ObservableIndexScanExecutor executor(sm_manager.get(), "warehouse", {}, {"name"}, &context);
    const auto constraint_builds = executor.constraint_rebuild_count();
    const char oversized[] = "qweruiopX";
    executor.bind_lookup_key(TabCol{"warehouse", "name"}, LookupKeyView{oversized, sizeof(oversized) - 1, TYPE_STRING});
    executor.beginTuple();
    EXPECT_TRUE(executor.is_end());
    EXPECT_TRUE(executor.uses_empty_single_rid_cursor());
    EXPECT_EQ(executor.constraint_rebuild_count(), constraint_builds);

    char wider_source[16] = "qweruiop";
    executor.bind_lookup_key(TabCol{"warehouse", "name"},
                             LookupKeyView{wider_source, sizeof(wider_source), TYPE_STRING});
    executor.beginTuple();
    ASSERT_FALSE(executor.is_end());
    EXPECT_EQ(read_unaligned<int>(executor.Next()->data), 10);
    EXPECT_EQ(executor.constraint_rebuild_count(), constraint_builds);
}

TEST_F(IndexScanFeatureTest, UsesCompositeIndexWithReorderedEqualityPrefixAndRange) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id", "name"}, nullptr);

    EXPECT_EQ(scan_ids({string_cond(OP_EQ, "qwerghjk"), int_cond(OP_EQ, 100)}, {"w_id", "name"}),
              std::vector<int>({100}));
    EXPECT_EQ(scan_ids({int_cond(OP_LT, 600), string_cond(OP_GT, "bztyhnmj")}, {"w_id", "name"}),
              std::vector<int>({10, 100}));
}

TEST_F(IndexScanFeatureTest, SkipScanUsesSuffixEqualityAcrossDistinctPrefixes) {
    create_three_int_table("triples");
    insert_three_ints("triples", 1, 2, 3);
    insert_three_ints("triples", 1, 2, 4);
    insert_three_ints("triples", 2, 2, 3);
    insert_three_ints("triples", 2, 1, 3);
    insert_three_ints("triples", 3, 2, 3);
    sm_manager->create_index("triples", {"a", "b", "c"}, nullptr);

    auto result = skip_scan_three_int_a_values(
        {table_int_cond("triples", "b", OP_EQ, 2), table_int_cond("triples", "c", OP_EQ, 3)}, {"a", "b", "c"});

    EXPECT_EQ(result, std::vector<int>({1, 2, 3}));
}

TEST_F(IndexScanFeatureTest, DroppedIndexPagesDoNotPolluteRecreatedIndexes) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id"}, nullptr);
    EXPECT_EQ(scan_ids({int_cond(OP_GE, 100)}, {"w_id"}), std::vector<int>({100, 500, 534}));

    sm_manager->drop_index("warehouse", {"w_id"}, nullptr);
    sm_manager->create_index("warehouse", {"w_id", "name"}, nullptr);

    EXPECT_EQ(scan_ids({int_cond(OP_LT, 500)}, {"w_id", "name"}), std::vector<int>({10, 100}));
}

TEST_F(IndexScanFeatureTest, DroppedTableRecordPagesDoNotPolluteLaterIndexBuilds) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id", "name"}, nullptr);
    sm_manager->drop_table("warehouse", nullptr);

    create_scores();
    for (int sid = 10; sid <= 15; ++sid) {
        insert_score(sid, 101, static_cast<double>(sid));
        insert_score(sid, 102, static_cast<double>(sid) + 0.5);
        insert_score(sid, 103, static_cast<double>(sid) + 1.0);
    }
    sm_manager->create_index("scores", {"sid", "cid"}, nullptr);

    auto result = scan_score_keys(
        {table_int_cond("scores", "sid", OP_GE, 10), table_int_cond("scores", "sid", OP_LE, 15)}, {"sid", "cid"});
    std::vector<std::pair<int, int>> expected;
    for (int sid = 10; sid <= 15; ++sid) {
        expected.emplace_back(sid, 101);
        expected.emplace_back(sid, 102);
        expected.emplace_back(sid, 103);
    }
    EXPECT_EQ(result, expected);
}

TEST_F(IndexScanFeatureTest, PlannerSelectsBestLeftmostPrefixIndexAndReordersConditions) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id", "name"}, nullptr);
    Planner planner(sm_manager.get());
    std::vector<Condition> conds = {string_cond(OP_EQ, "qwerghjk"), int_cond(OP_EQ, 100)};
    std::vector<std::string> index_cols;

    EXPECT_TRUE(planner.get_index_cols("warehouse", conds, index_cols));
    EXPECT_EQ(index_cols, std::vector<std::string>({"w_id", "name"}));
    ASSERT_EQ(conds.size(), 2);
    EXPECT_EQ(conds[0].lhs_col.col_name, "w_id");
    EXPECT_EQ(conds[1].lhs_col.col_name, "name");
}

TEST_F(IndexScanFeatureTest, InsertDeleteAndUpdateMaintainSingleColumnIndex) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"w_id"}, nullptr);

    insert_row(700, "newdance");
    EXPECT_EQ(scan_ids({int_cond(OP_EQ, 700)}, {"w_id"}), std::vector<int>({700}));

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    std::vector<Rid> delete_rids;
    IndexScanExecutor delete_scan(sm_manager.get(), "warehouse", {int_cond(OP_EQ, 700)}, {"w_id"}, &context);
    for (delete_scan.beginTuple(); !delete_scan.is_end(); delete_scan.nextTuple()) {
        delete_rids.push_back(delete_scan.rid());
    }
    DeleteExecutor delete_exec(sm_manager.get(), "warehouse", {int_cond(OP_EQ, 700)}, delete_rids, &context);
    delete_exec.Next();
    EXPECT_TRUE(scan_ids({int_cond(OP_EQ, 700)}, {"w_id"}).empty());

    std::vector<Rid> update_rids;
    IndexScanExecutor update_scan(sm_manager.get(), "warehouse", {int_cond(OP_EQ, 534)}, {"w_id"}, &context);
    for (update_scan.beginTuple(); !update_scan.is_end(); update_scan.nextTuple()) {
        update_rids.push_back(update_scan.rid());
    }
    UpdateExecutor update_exec(sm_manager.get(), "warehouse", {set_int_clause("warehouse", "w_id", 507)},
                               {int_cond(OP_EQ, 534)}, update_rids, &context);
    update_exec.Next();
    EXPECT_EQ(scan_ids({int_cond(OP_GT, 100), int_cond(OP_LT, 534)}, {"w_id"}), std::vector<int>({500, 507}));
}

TEST_F(IndexScanFeatureTest, InsertConflictRollsBackAllPreviouslyInsertedIndexEntries) {
    create_two_int_table("pairs");
    insert_two_ints("pairs", 1, 1);
    insert_two_ints("pairs", 2, 2);
    sm_manager->create_index("pairs", {"a"}, nullptr);
    sm_manager->create_index("pairs", {"b"}, nullptr);

    EXPECT_THROW(insert_two_ints("pairs", 3, 2), IndexEntryExistsError);
    EXPECT_NO_THROW(insert_two_ints("pairs", 3, 3));
    EXPECT_EQ(scan_table_ints("pairs", {table_int_cond("pairs", "a", OP_EQ, 3)}, {"a"}), std::vector<int>({3}));
}

TEST_F(IndexScanFeatureTest, UpdateConflictOnNonLeadingColumnIndexDoesNotCorruptIndexesOrRecord) {
    create_warehouse();
    sm_manager->create_index("warehouse", {"name"}, nullptr);

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    std::vector<Rid> rids;
    IndexScanExecutor scan(sm_manager.get(), "warehouse", {string_cond(OP_EQ, "asdfhjkl")}, {"name"}, &context);
    for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
        rids.push_back(scan.rid());
    }

    UpdateExecutor update_exec(sm_manager.get(), "warehouse", {set_string_clause("warehouse", "name", "qweruiop")},
                               {string_cond(OP_EQ, "asdfhjkl")}, rids, &context);
    EXPECT_THROW(update_exec.Next(), IndexEntryExistsError);

    EXPECT_EQ(scan_ids({string_cond(OP_EQ, "asdfhjkl")}, {"name"}), std::vector<int>({534}));
    EXPECT_EQ(scan_ids({string_cond(OP_EQ, "qweruiop")}, {"name"}), std::vector<int>({10}));
}

TEST_F(IndexScanFeatureTest, CreateIndexRejectsDuplicateExistingKeysAndDoesNotLeaveCatalogEntry) {
    create_two_int_table("dups");
    insert_two_ints("dups", 1, 10);
    insert_two_ints("dups", 1, 20);

    EXPECT_THROW(sm_manager->create_index("dups", {"a"}, nullptr), IndexEntryExistsError);
    EXPECT_FALSE(sm_manager->db_.get_table("dups").is_index({"a"}));
    EXPECT_TRUE(sm_manager->ihs_.find(sm_manager->get_ix_manager()->get_index_name(
                    "dups", std::vector<std::string>{"a"})) == sm_manager->ihs_.end());
}

} // namespace

namespace {

class ShowIndexTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager;
    std::unique_ptr<RmManager> rm_manager;
    std::unique_ptr<IxManager> ix_manager;
    std::unique_ptr<SmManager> sm_manager;
    std::string db_name = "show_index_test_db";
    bool opened = false;

    void SetUp() override {
        disk_manager = std::make_unique<DiskManager>();
        buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
        rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
        ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
        sm_manager = std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(),
                                                 ix_manager.get());
        if (sm_manager->is_dir(db_name)) {
            sm_manager->drop_db(db_name);
        }
        sm_manager->create_db(db_name);
        sm_manager->open_db(db_name);
        opened = true;
    }

    void TearDown() override {
        if (opened) {
            sm_manager->close_db();
            opened = false;
        }
        if (sm_manager->is_dir(db_name)) {
            sm_manager->drop_db(db_name);
        }
    }
};

TEST_F(ShowIndexTest, PrintsFormattedIndexMetadataTable) {
    std::vector<ColDef> cols = {{"id", TYPE_INT, 4}, {"name", TYPE_STRING, 8}};
    sm_manager->create_table("warehouse", cols, nullptr);
    sm_manager->create_index("warehouse", {"id"}, nullptr);
    sm_manager->create_index("warehouse", {"id", "name"}, nullptr);

    char data_send[BUFFER_LENGTH] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);

    sm_manager->show_index("warehouse", &context);

    std::string output(data_send, offset);
    EXPECT_NE(output.find("+------------------+------------------+------------------+"), std::string::npos);
    EXPECT_NE(output.find("|           Tables |             Type |           Column |"), std::string::npos);
    EXPECT_NE(output.find("|        warehouse |           unique |             (id) |"), std::string::npos);
    EXPECT_NE(output.find("|        warehouse |           unique |        (id,name) |"), std::string::npos);
}

} // namespace
