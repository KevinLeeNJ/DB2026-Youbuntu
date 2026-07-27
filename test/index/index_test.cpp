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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/context.h"
#include "errors.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_index_skip_scan.h"
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

    static std::vector<char> float_key(float value) {
        std::vector<char> buf(sizeof(float));
        std::memcpy(buf.data(), &value, sizeof(float));
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

TEST_F(IndexHandleTest, FloatKeysUseBinary32Ordering) {
    const auto int_index_name = ix_manager->get_index_name(table_name, cols);
    disk_manager->destroy_file(int_index_name);
    ASSERT_FALSE(disk_manager->is_file(int_index_name));
    cols = {ColMeta{.tab_name = table_name,
                    .name = "value",
                    .type = TYPE_FLOAT,
                    .len = static_cast<int>(sizeof(float)),
                    .offset = 0,
                    .index = true}};
    ix_manager->create_index(table_name, cols);

    auto ih = open_index();
    for (const auto [value, slot] :
         std::vector<std::pair<float, int>>{{1.5F, 115}, {300000.01F, 3000100}, {-2.0F, 80}}) {
        auto k = float_key(value);
        ih->insert_entry(k.data(), Rid{1, slot}, nullptr);
    }

    auto target = float_key(300000.01F);
    std::vector<Rid> result;
    ASSERT_TRUE(ih->get_value(target.data(), &result, nullptr));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result.front().slot_no, 3000100);

    auto lower = float_key(-3.0F);
    auto upper = float_key(2.0F);
    IxScan scan(ih.get(), ih->lower_bound(lower.data()), ih->upper_bound(upper.data()), buffer_pool_manager.get());
    std::vector<int> slots;
    while (!scan.is_end()) {
        slots.push_back(scan.rid().slot_no);
        scan.next();
    }
    EXPECT_EQ(slots, std::vector<int>({80, 115}));
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
    Iid lower = ih->lower_bound(lower_key.data());
    Iid upper = ih->upper_bound(upper_key.data());
    ASSERT_EQ(lower.page_no, upper.page_no);

    IxScan scan(ih.get(), lower, upper, buffer_pool_manager.get(), true, true);
    std::vector<int> values;
    while (!scan.is_end()) {
        int value = 0;
        std::memcpy(&value, scan.key(), sizeof(value));
        values.push_back(value);
        scan.next();
    }

    EXPECT_EQ(values, std::vector<int>({10, 20}));
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

TEST_F(IndexHandleTest, ScanSkipsEmptyLeafLeftByDeletes) {
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
    auto emptied_leaf = std::find_if(leaves.begin(), leaves.end(),
                                     [&](const LeafSnapshot& leaf) { return leaf.page_no == emptied_page; });
    ASSERT_NE(emptied_leaf, leaves.end());
    ASSERT_TRUE(emptied_leaf->keys.empty());

    auto lower_key = key(before_empty);
    auto upper_key = key(after_empty);
    IxScan scan(ih.get(), ih->lower_bound(lower_key.data()), ih->upper_bound(upper_key.data()),
                buffer_pool_manager.get());

    std::vector<int> slots;
    while (!scan.is_end()) {
        slots.push_back(scan.rid().slot_no);
        scan.next();
    }

    EXPECT_EQ(slots, std::vector<int>({before_empty, after_empty}));

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
        std::vector<ColDef> cols = {{"sid", TYPE_INT, 4}, {"cid", TYPE_INT, 4}, {"score", TYPE_FLOAT, 4}};
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

    void insert_score(int sid, int cid, float score) {
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
    executor.set_lookup_key(TabCol{"lookup_direct", "b"}, reinterpret_cast<const char*>(&lookup_value),
                            sizeof(lookup_value));

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
    executor.set_lookup_key(TabCol{"lookup_miss", "b"}, reinterpret_cast<const char*>(&lookup_value),
                            sizeof(lookup_value));

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
    executor.set_lookup_key(TabCol{"lookup_duplicates", "b"}, reinterpret_cast<const char*>(&lookup_value),
                            sizeof(lookup_value));

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
        insert_score(sid, 101, static_cast<float>(sid));
        insert_score(sid, 102, static_cast<float>(sid) + 0.5f);
        insert_score(sid, 103, static_cast<float>(sid) + 1.0f);
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
