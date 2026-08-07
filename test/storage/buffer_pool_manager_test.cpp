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
#include "storage/buffer_pool_manager.h"
#undef private

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>

#include "gtest/gtest.h"
#include "recovery/log_manager.h"
#include "replacer/clock_replacer.h"
#include "storage/disk_manager.h"
#include "errors.h"

const std::string TEST_DB_NAME = "buffer_pool_manager_test_db";
const std::string TEST_FILE_NAME = "basic";
const std::string TEST_FILE_NAME_CCUR = "concurrency";

class ScopedCurrentPath {
public:
    explicit ScopedCurrentPath(const std::filesystem::path& path) : old_path_(std::filesystem::current_path()) {
        std::filesystem::current_path(path);
    }

    ~ScopedCurrentPath() {
        std::error_code ignored;
        std::filesystem::current_path(old_path_, ignored);
    }

private:
    std::filesystem::path old_path_;
};

std::unique_ptr<LogManager> InstallTestWal(BufferPoolManager* bpm, DiskManager* disk_manager, lsn_t max_lsn) {
    if (!disk_manager->is_file(LOG_FILE_NAME)) {
        disk_manager->create_file(LOG_FILE_NAME);
    }
    disk_manager->truncate_log();
    auto log_manager = std::make_unique<LogManager>(disk_manager);
    for (lsn_t expected_lsn = 0; expected_lsn <= max_lsn; ++expected_lsn) {
        BeginLogRecord record(expected_lsn);
        EXPECT_EQ(log_manager->add_log_to_buffer(&record), expected_lsn);
    }
    bpm->set_log_manager(log_manager.get());
    return log_manager;
}

class ScopedOpenTestFile {
public:
    ScopedOpenTestFile(DiskManager* disk_manager, std::string name)
        : disk_manager_(disk_manager), name_(std::move(name)) {
        if (disk_manager_->is_file(name_)) {
            disk_manager_->destroy_file(name_);
        }
        disk_manager_->create_file(name_);
        fd_ = disk_manager_->open_file(name_);
    }

    ~ScopedOpenTestFile() {
        if (fd_ >= 0) {
            disk_manager_->close_file(fd_);
        }
        if (disk_manager_->is_file(name_)) {
            disk_manager_->destroy_file(name_);
        }
    }

    int fd() const {
        return fd_;
    }

    void Close() {
        if (fd_ >= 0) {
            disk_manager_->close_file(fd_);
            fd_ = -1;
        }
    }

private:
    DiskManager* disk_manager_;
    std::string name_;
    int fd_{-1};
};

class BufferPoolManagerTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<ScopedCurrentPath> test_path_;
    int fd_ = -1; // 此文件描述符为disk_manager_->open_file的返回值
    bool fd_closed_{false};

public:
    // This function is called before every test.
    void SetUp() override {
        ::testing::Test::SetUp();
        // For each test, we create a new DiskManager
        disk_manager_ = std::make_unique<DiskManager>();
        // 如果测试目录不存在，则先创建测试目录
        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        // Restore the process cwd even if a test or fd cleanup throws. Throwing
        // again from TearDown would hide the primary assertion failure.
        test_path_ = std::make_unique<ScopedCurrentPath>(TEST_DB_NAME);
        // 如果测试文件存在，则先删除原文件（最后留下来的文件存的是最后一个测试点的数据）
        if (disk_manager_->is_file(TEST_FILE_NAME)) {
            disk_manager_->destroy_file(TEST_FILE_NAME);
        }
        // 创建测试文件
        disk_manager_->create_file(TEST_FILE_NAME);
        assert(disk_manager_->is_file(TEST_FILE_NAME));
        // 打开测试文件
        fd_ = disk_manager_->open_file(TEST_FILE_NAME);
        assert(fd_ != -1);
    }

    // This function is called after every test.
    void TearDown() override {
        BufferPoolManager::set_flush_page_test_hook({});
        BufferPoolManager::set_flush_page_after_write_test_hook({});
        BufferPoolManager::set_flush_batch_before_write_test_hook({});
        BufferPoolManager::set_ensure_dependency_test_hook({});
        BufferPoolManager::set_flush_claim_test_hook({});
        if (!fd_closed_) {
            disk_manager_->close_file(fd_);
        }
        // disk_manager_->destroy_file(TEST_FILE_NAME);  // you can choose to delete the file

        test_path_.reset();
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    }
};

TEST_F(BufferPoolManagerTest, ResidentDirectoryShardMixesFileDescriptorWithPageNumber) {
    BufferPoolManager bpm(1, disk_manager_.get());
    std::array<bool, 64> covered{};
    constexpr page_id_t kFixedPageNo = 41;
    for (int fd = 0; fd < 256; ++fd) {
        covered[bpm.resident_directory_shard_index(PageId{fd, kFixedPageNo})] = true;
    }
    const size_t distinct_shards = std::count(covered.begin(), covered.end(), true);
    EXPECT_GE(distinct_shards, 48U);
}

TEST_F(BufferPoolManagerTest, FailedDirtyEvictionRetainsOriginalPage) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId old_page_id{fd_, INVALID_PAGE_ID};
    Page* old_page = bpm->new_page(&old_page_id);
    ASSERT_NE(old_page, nullptr);
    std::memcpy(old_page->get_data(), "dirty-page", 11);
    ASSERT_TRUE(bpm->unpin_page(old_page_id, true));

    // Closing the file makes the victim write fail deterministically while the
    // frame still contains the only copy of the dirty page.
    disk_manager_->close_file(fd_);
    fd_closed_ = true;

    PageId missing_page{fd_, 99};
    EXPECT_EQ(bpm->fetch_page(missing_page), nullptr);
    EXPECT_TRUE(bpm->is_page_resident(old_page_id));
    Page* retained_page = bpm->fetch_page(old_page_id);
    ASSERT_NE(retained_page, nullptr);
    EXPECT_EQ(std::memcmp(retained_page->get_data(), "dirty-page", 11), 0);
    EXPECT_TRUE(bpm->unpin_page(old_page_id, true));
}

TEST_F(BufferPoolManagerTest, WalDependentFlushFailsClosedWithoutLogManager) {
    BufferPoolManager bpm(2, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm.new_page(&page_id);
    ASSERT_NE(page, nullptr);
    std::memcpy(page->get_data(), "requires-wal", 12);
    ASSERT_TRUE(bpm.unpin_page(page_id, PageWriteDependency::Wal(7)));

    EXPECT_FALSE(bpm.flush_page(page_id));
    Page* retained = bpm.fetch_page(page_id);
    ASSERT_NE(retained, nullptr);
    EXPECT_TRUE(retained->is_dirty_.load(std::memory_order_acquire));
    EXPECT_EQ(std::memcmp(retained->get_data(), "requires-wal", 12), 0);
    EXPECT_TRUE(bpm.unpin_page(page_id, false));
}

TEST_F(BufferPoolManagerTest, IndexSmoBarrierDrainsAnAlreadyClaimedFlush) {
    BufferPoolManager bpm(2, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm.new_page(&page_id);
    ASSERT_NE(page, nullptr);
    ASSERT_TRUE(bpm.unpin_page(page_id, PageWriteDependency::None()));

    std::mutex mutex;
    std::condition_variable cv;
    bool flush_claimed = false;
    bool release_flush = false;
    bool begin_started = false;
    bool barrier_acquired = false;
    bool hook_timed_out = false;
    BufferPoolManager::set_flush_batch_before_write_test_hook([&](PageId, Page*) {
        std::unique_lock lock{mutex};
        flush_claimed = true;
        cv.notify_all();
        if (!cv.wait_for(lock, std::chrono::seconds(2), [&] { return release_flush; })) hook_timed_out = true;
    });

    std::vector<PageId> pages{page_id};
    std::promise<void> flush_done_promise;
    auto flush_done = flush_done_promise.get_future();
    std::thread flusher([&] {
        EXPECT_TRUE(bpm.flush_pages(pages, FlushDependencyPolicy::Enforce()).success);
        flush_done_promise.set_value();
    });
    {
        std::unique_lock lock{mutex};
        if (!cv.wait_for(lock, std::chrono::seconds(1), [&] { return flush_claimed; })) {
            release_flush = true;
            lock.unlock();
            cv.notify_all();
            const auto rescued = flush_done.wait_for(std::chrono::seconds(2));
            EXPECT_EQ(rescued, std::future_status::ready);
            flusher.join();
            FAIL() << "batch flush never reached its claimed-frame hook";
        }
    }
    std::promise<void> smo_done_promise;
    auto smo_done = smo_done_promise.get_future();
    std::thread smo([&] {
        {
            std::lock_guard lock{mutex};
            begin_started = true;
            cv.notify_all();
        }
        bpm.begin_index_smo(fd_);
        {
            std::lock_guard lock{mutex};
            barrier_acquired = true;
            cv.notify_all();
        }
        bpm.end_index_smo(fd_);
        smo_done_promise.set_value();
    });
    {
        std::unique_lock lock{mutex};
        const bool observed_begin = cv.wait_for(lock, std::chrono::seconds(1), [&] { return begin_started; });
        const bool acquired_while_claimed = barrier_acquired;
        release_flush = true;
        lock.unlock();
        cv.notify_all();
        const auto flush_status = flush_done.wait_for(std::chrono::seconds(2));
        const auto smo_status = smo_done.wait_for(std::chrono::seconds(2));
        EXPECT_EQ(flush_status, std::future_status::ready);
        EXPECT_EQ(smo_status, std::future_status::ready);
        flusher.join();
        smo.join();
        EXPECT_TRUE(observed_begin);
        EXPECT_FALSE(acquired_while_claimed);
    }
    EXPECT_TRUE(barrier_acquired);
    EXPECT_FALSE(hook_timed_out);
}

TEST_F(BufferPoolManagerTest, IndexSmoBarrierDrainsAnAlreadyClaimedDirectHeaderWrite) {
    BufferPoolManager bpm(2, disk_manager_.get());
    bpm.begin_index_file_write(fd_);
    std::atomic<bool> started{false};
    std::atomic<bool> acquired{false};
    std::thread smo([&] {
        started.store(true, std::memory_order_release);
        bpm.begin_index_smo(fd_);
        acquired.store(true, std::memory_order_release);
        bpm.end_index_smo(fd_);
    });
    while (!started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(acquired.load(std::memory_order_acquire));
    bpm.end_index_file_write(fd_);
    smo.join();
    EXPECT_TRUE(acquired.load(std::memory_order_acquire));
}

TEST_F(BufferPoolManagerTest, FlushCleanupWakesFrameReservation) {
    BufferPoolManager bpm(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    ASSERT_NE(bpm.new_page(&page_id), nullptr);
    ASSERT_TRUE(bpm.unpin_page(page_id, true));

    std::mutex mutex;
    std::condition_variable cv;
    bool claimed = false;
    bool release = false;
    bool hook_timed_out = false;
    BufferPoolManager::set_flush_batch_before_write_test_hook([&](PageId, Page*) {
        std::unique_lock lock{mutex};
        claimed = true;
        cv.notify_all();
        if (!cv.wait_for(lock, std::chrono::seconds(2), [&] { return release; })) hook_timed_out = true;
    });

    std::vector<PageId> pages{page_id};
    std::thread flusher([&] { EXPECT_TRUE(bpm.flush_pages(pages, FlushDependencyPolicy::Enforce()).success); });
    {
        std::unique_lock lock{mutex};
        if (!cv.wait_for(lock, std::chrono::seconds(1), [&] { return claimed; })) {
            release = true;
            lock.unlock();
            cv.notify_all();
            flusher.join();
            FAIL() << "flush never claimed its frame";
        }
    }
    auto reservation = std::async(std::launch::async, [&] { return bpm.acquire_frame_operation(1); });
    EXPECT_EQ(reservation.wait_for(std::chrono::milliseconds(30)), std::future_status::timeout);
    {
        std::lock_guard lock{mutex};
        release = true;
    }
    cv.notify_all();
    if (reservation.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        // Failure cleanup must not leave an async future blocked during
        // assertion unwinding; explicitly wake it before reporting.
        bpm.notify_frame_operation_waiters_for_test();
        flusher.join();
        const auto rescued = reservation.wait_for(std::chrono::seconds(1));
        EXPECT_EQ(rescued, std::future_status::ready);
        FAIL() << "frame reservation was not notified by batch cleanup";
    }
    auto token = reservation.get();
    flusher.join();
    EXPECT_FALSE(hook_timed_out);
}

TEST_F(BufferPoolManagerTest, FiniteFlushDrainsFirstFdBeforeBlockedSecondFd) {
    ScopedOpenTestFile second_file(disk_manager_.get(), "second_flush_fd");
    BufferPoolManager bpm(2, disk_manager_.get());
    PageId first{fd_, INVALID_PAGE_ID};
    PageId second{second_file.fd(), INVALID_PAGE_ID};
    ASSERT_NE(bpm.new_page(&first), nullptr);
    ASSERT_TRUE(bpm.unpin_page(first, true));
    ASSERT_NE(bpm.new_page(&second), nullptr);
    ASSERT_TRUE(bpm.unpin_page(second, true));

    bpm.begin_index_smo(second.fd);
    std::vector<PageId> pages{first, second};
    auto flushed = std::async(std::launch::async, [&] {
        return bpm.flush_pages_until(pages, FlushDependencyPolicy::Enforce(),
                                     std::chrono::steady_clock::now() + std::chrono::milliseconds(100));
    });
    if (flushed.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        bpm.end_index_smo(second.fd);
        const auto rescued = flushed.wait_for(std::chrono::seconds(1));
        EXPECT_EQ(rescued, std::future_status::ready);
        FAIL() << "finite flush did not honor its SMO deadline";
    }
    const auto result = flushed.get();
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.pages_written, 1u);

    // The first fd was cleaned before waiting on the SMO barrier, so an
    // independent frame reservation cannot form a FLUSHING/SMO lock ring.
    auto reservation = std::async(std::launch::async, [&] { return bpm.acquire_frame_operation(1); });
    if (reservation.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        bpm.end_index_smo(second.fd);
        bpm.notify_frame_operation_waiters_for_test();
        const auto rescued = reservation.wait_for(std::chrono::seconds(1));
        EXPECT_EQ(rescued, std::future_status::ready);
        FAIL() << "frame reservation did not complete after first-fd cleanup";
    }
    auto token = reservation.get();
    bpm.end_index_smo(second.fd);
}

TEST_F(BufferPoolManagerTest, FiniteFlushRetainsBlockedCursorAndContinuesAfterBarrierRelease) {
    ScopedOpenTestFile second_file(disk_manager_.get(), "second_flush_release_fd");
    BufferPoolManager bpm(2, disk_manager_.get());
    PageId first{fd_, INVALID_PAGE_ID};
    PageId second{second_file.fd(), INVALID_PAGE_ID};
    ASSERT_NE(bpm.new_page(&first), nullptr);
    ASSERT_TRUE(bpm.unpin_page(first, true));
    ASSERT_NE(bpm.new_page(&second), nullptr);
    ASSERT_TRUE(bpm.unpin_page(second, true));

    bpm.begin_index_smo(second.fd);
    std::thread release_barrier([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        bpm.end_index_smo(second.fd);
    });
    std::vector<PageId> pages{first, second};
    const auto result = bpm.flush_pages_until(pages, FlushDependencyPolicy::Enforce(),
                                              std::chrono::steady_clock::now() + std::chrono::seconds(1));
    release_barrier.join();
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.pages_written, 2u);
    EXPECT_EQ(bpm.count_dirty_pages({fd_, second.fd}), 0u);
}

TEST_F(BufferPoolManagerTest, FlushClaimFaultsRestoreEveryOwnedStateAndRemainRetryable) {
    constexpr std::array<std::string_view, 4> fault_points{
        "after_inflight_claim", "after_resident_extract", "after_flushing", "cleanup_first"};
    for (const std::string_view fault_point : fault_points) {
        SCOPED_TRACE(fault_point);
        BufferPoolManager bpm(2, disk_manager_.get());
        auto log_manager = InstallTestWal(&bpm, disk_manager_.get(), 1);
        PageId first{fd_, INVALID_PAGE_ID};
        PageId second{fd_, INVALID_PAGE_ID};
        ASSERT_NE(bpm.new_page(&first), nullptr);
        ASSERT_TRUE(bpm.unpin_page(first, PageWriteDependency::Wal(0)));
        ASSERT_NE(bpm.new_page(&second), nullptr);
        ASSERT_TRUE(bpm.unpin_page(second, PageWriteDependency::Wal(1)));

        bool injected = false;
        BufferPoolManager::set_flush_claim_test_hook([&](std::string_view point, PageId) {
            if (!injected && point == fault_point) {
                injected = true;
                throw InternalError("injected flush claim fault");
            }
        });
        const auto failed = bpm.flush_dirty_pages({fd_}, 2);
        EXPECT_FALSE(failed.success);
        EXPECT_TRUE(injected);
        EXPECT_TRUE(bpm.index_writes_inflight_.empty());
        EXPECT_EQ(bpm.replacer_->Size(), 2u);
        EXPECT_TRUE(bpm.resident_directory_is_consistent_for_test());
        for (const auto [page_id, lsn] : {std::pair{first, lsn_t{0}}, std::pair{second, lsn_t{1}}}) {
            const auto hit = bpm.page_table_.find(page_id);
            ASSERT_NE(hit, bpm.page_table_.end());
            Page* page = &bpm.pages_[hit->second];
            EXPECT_EQ(page->state_.load(std::memory_order_acquire), FrameState::VALID);
            EXPECT_TRUE(page->is_dirty_.load(std::memory_order_acquire));
            EXPECT_EQ(page->write_dependency_.kind(), PageWriteDependency::Kind::WalLsn);
            EXPECT_EQ(page->write_dependency_.wal_lsn(), lsn);
        }

        BufferPoolManager::set_flush_claim_test_hook({});
        const auto retried = bpm.flush_dirty_pages({fd_}, 2);
        EXPECT_TRUE(retried.success);
        EXPECT_EQ(retried.pages_written, 2u);
        EXPECT_TRUE(bpm.index_writes_inflight_.empty());
        EXPECT_EQ(bpm.replacer_->Size(), 2u);
        EXPECT_TRUE(bpm.resident_directory_is_consistent_for_test());
    }
}

TEST_F(BufferPoolManagerTest, FlushClaimScopeGuardRestoresClaimsDuringExceptionUnwind) {
    BufferPoolManager bpm(2, disk_manager_.get());
    std::vector<PageId> pages;
    for (int i = 0; i < 2; ++i) {
        PageId page_id{fd_, INVALID_PAGE_ID};
        ASSERT_NE(bpm.new_page(&page_id), nullptr);
        ASSERT_TRUE(bpm.unpin_page(page_id, true));
        pages.push_back(page_id);
    }
    BufferPoolManager::set_flush_claim_test_hook([](std::string_view point, PageId) {
        if (point == "scope_unwind") throw std::bad_alloc();
    });
    EXPECT_THROW((void)bpm.flush_pages(pages, FlushDependencyPolicy::Enforce()), std::bad_alloc);
    BufferPoolManager::set_flush_claim_test_hook({});

    EXPECT_TRUE(bpm.index_writes_inflight_.empty());
    EXPECT_EQ(bpm.replacer_->Size(), 2u);
    EXPECT_TRUE(bpm.resident_directory_is_consistent_for_test());
    for (const PageId page_id : pages) {
        const auto hit = bpm.page_table_.find(page_id);
        ASSERT_NE(hit, bpm.page_table_.end());
        EXPECT_EQ(bpm.pages_[hit->second].state_.load(std::memory_order_acquire), FrameState::VALID);
        EXPECT_TRUE(bpm.pages_[hit->second].is_dirty_.load(std::memory_order_acquire));
    }
    const auto retry = bpm.flush_pages(pages, FlushDependencyPolicy::Enforce());
    EXPECT_TRUE(retry.success);
    EXPECT_EQ(retry.pages_written, 2u);
}

TEST_F(BufferPoolManagerTest, BlockedVictimScratchIsReusedAcrossFullPoolMisses) {
    constexpr size_t pool_size = 8;
    BufferPoolManager bpm(pool_size, disk_manager_.get());
    ASSERT_EQ(bpm.blocked_victims_scratch_.capacity(), pool_size);
    const frame_id_t* const scratch_data = bpm.blocked_victims_scratch_.data();

    for (size_t i = 0; i < pool_size; ++i) {
        PageId page_id{fd_, INVALID_PAGE_ID};
        Page* page = bpm.new_page(&page_id);
        ASSERT_NE(page, nullptr);
        ASSERT_TRUE(bpm.unpin_page(page_id, true));
    }

    bpm.begin_index_smo(fd_);
    for (int attempt = 0; attempt < 3; ++attempt) {
        PageId missing{fd_, INVALID_PAGE_ID};
        EXPECT_EQ(bpm.new_page(&missing), nullptr);
        EXPECT_TRUE(bpm.blocked_victims_scratch_.empty());
        EXPECT_EQ(bpm.blocked_victims_scratch_.capacity(), pool_size);
        EXPECT_EQ(bpm.blocked_victims_scratch_.data(), scratch_data);
        EXPECT_EQ(bpm.replacer_->Size(), pool_size);
    }
    bpm.end_index_smo(fd_);

    PageId replacement{fd_, INVALID_PAGE_ID};
    Page* page = bpm.new_page(&replacement);
    ASSERT_NE(page, nullptr);
    EXPECT_TRUE(bpm.unpin_page(replacement, false));
}

TEST_F(BufferPoolManagerTest, SampleTest) {
    // create BufferPoolManager
    const size_t buffer_pool_size = 10;
    auto disk_manager = BufferPoolManagerTest::disk_manager_.get();
    auto bpm = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager);
    // create tmp PageId
    int fd = BufferPoolManagerTest::fd_;
    PageId page_id_temp = {.fd = fd, .page_no = INVALID_PAGE_ID};
    auto* page0 = bpm->new_page(&page_id_temp);

    // Scenario: The buffer pool is empty. We should be able to create a new page.
    ASSERT_NE(nullptr, page0);
    EXPECT_EQ(0, page_id_temp.page_no);

    // Scenario: Once we have a page, we should be able to read and write content.
    snprintf(page0->get_data(), sizeof(page0->get_data()), "Hello");
    EXPECT_EQ(0, strcmp(page0->get_data(), "Hello"));

    // Scenario: We should be able to create new pages until we fill up the buffer pool.
    for (size_t i = 1; i < buffer_pool_size; ++i) {
        EXPECT_NE(nullptr, bpm->new_page(&page_id_temp));
    }

    // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
    for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
        EXPECT_EQ(nullptr, bpm->new_page(&page_id_temp));
    }

    // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
    // there would still be one cache frame left for reading page 0.
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(true, bpm->unpin_page(PageId{fd, i}, true));
    }
    for (int i = 0; i < 4; ++i) {
        EXPECT_NE(nullptr, bpm->new_page(&page_id_temp));
    }

    // Scenario: We should be able to fetch the data we wrote a while ago.
    page0 = bpm->fetch_page(PageId{fd, 0});
    EXPECT_EQ(0, strcmp(page0->get_data(), "Hello"));
    EXPECT_EQ(true, bpm->unpin_page(PageId{fd, 0}, true));
    // new_page again, and now all buffers are pinned. Page 0 would be failed to fetch.
    EXPECT_NE(nullptr, bpm->new_page(&page_id_temp));
    EXPECT_EQ(nullptr, bpm->fetch_page(PageId{fd, 0}));

    bpm->flush_all_pages(fd);
}

// 验证 BufferPoolManager 构造时 replacer 被正确创建
TEST_F(BufferPoolManagerTest, ReplacerCreatedCorrectly) {
    const size_t buffer_pool_size = 10;
    auto disk_manager = BufferPoolManagerTest::disk_manager_.get();
    auto bpm = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager);
    ASSERT_NE(nullptr, bpm->replacer_);
    EXPECT_NE(nullptr, dynamic_cast<ClockReplacer*>(bpm->replacer_.get()));
    // 验证 replacer 可正常使用（非空且功能正确）
    EXPECT_EQ(0, bpm->replacer_->Size());
}

TEST_F(BufferPoolManagerTest, NewPageFromFreshFrameHasOnlyValidPageTableEntry) {
    auto disk_manager = BufferPoolManagerTest::disk_manager_.get();
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager);

    PageId page_id{BufferPoolManagerTest::fd_, INVALID_PAGE_ID};
    auto* page = bpm->new_page(&page_id);

    ASSERT_NE(nullptr, page);
    EXPECT_EQ((PageId{BufferPoolManagerTest::fd_, 0}), page->get_page_id());
    ASSERT_EQ(1, bpm->page_table_.size());
    EXPECT_EQ(1, bpm->page_table_.count(page_id));
    EXPECT_EQ(0, bpm->page_table_.count(PageId{0, INVALID_PAGE_ID}));
}

TEST_F(BufferPoolManagerTest, FreeFrameCounterAndRecycleVectorPreserveAllocationPaths) {
    auto bpm = std::make_unique<BufferPoolManager>(3, disk_manager_.get());
    EXPECT_EQ(bpm->next_unused_frame_, 0);
    EXPECT_TRUE(bpm->recycled_frames_.empty());

    PageId first_page{fd_, INVALID_PAGE_ID};
    PageId second_page{fd_, INVALID_PAGE_ID};
    ASSERT_NE(bpm->new_page(&first_page), nullptr);
    ASSERT_NE(bpm->new_page(&second_page), nullptr);
    ASSERT_TRUE(bpm->unpin_page(first_page, false));
    ASSERT_TRUE(bpm->unpin_page(second_page, false));
    EXPECT_EQ(bpm->next_unused_frame_, 2);

    ASSERT_TRUE(bpm->delete_page(first_page));
    ASSERT_EQ(bpm->recycled_frames_.size(), 1u);
    EXPECT_EQ(bpm->recycled_frames_.back(), 0);

    PageId recycled_page{fd_, INVALID_PAGE_ID};
    Page* recycled_frame = bpm->new_page(&recycled_page);
    ASSERT_NE(recycled_frame, nullptr);
    EXPECT_EQ(recycled_frame, &bpm->pages_[0]);
    EXPECT_TRUE(bpm->recycled_frames_.empty());

    PageId fresh_page{fd_, INVALID_PAGE_ID};
    Page* fresh_frame = bpm->new_page(&fresh_page);
    ASSERT_NE(fresh_frame, nullptr);
    EXPECT_EQ(fresh_frame, &bpm->pages_[2]);
    EXPECT_EQ(bpm->next_unused_frame_, 3);
}

TEST_F(BufferPoolManagerTest, ReplacerFallbackAndExhaustionRemainUnchanged) {
    auto bpm = std::make_unique<BufferPoolManager>(2, disk_manager_.get());
    PageId first_page{fd_, INVALID_PAGE_ID};
    PageId second_page{fd_, INVALID_PAGE_ID};
    ASSERT_NE(bpm->new_page(&first_page), nullptr);
    ASSERT_NE(bpm->new_page(&second_page), nullptr);
    ASSERT_TRUE(bpm->unpin_page(first_page, false));
    ASSERT_TRUE(bpm->recycled_frames_.empty());

    PageId replacement_page{fd_, INVALID_PAGE_ID};
    ASSERT_NE(bpm->new_page(&replacement_page), nullptr);
    EXPECT_FALSE(bpm->is_page_resident(first_page));

    PageId exhausted_page{fd_, INVALID_PAGE_ID};
    EXPECT_EQ(bpm->new_page(&exhausted_page), nullptr);
    EXPECT_EQ(bpm->fetch_page(first_page), nullptr);
}

TEST_F(BufferPoolManagerTest, PageTableReserveAndLoadFactorAvoidPoolSizedRehash) {
    constexpr size_t buffer_pool_size = 10;
    auto bpm = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager_.get());
    const size_t initial_bucket_count = bpm->page_table_.bucket_count();
    const size_t expected_reserved_pages = buffer_pool_size + (buffer_pool_size + 4) / 5;

    EXPECT_FLOAT_EQ(bpm->page_table_.max_load_factor(), 0.7F);
    EXPECT_GE(initial_bucket_count, expected_reserved_pages);

    for (size_t i = 0; i < buffer_pool_size; ++i) {
        PageId page_id{fd_, INVALID_PAGE_ID};
        ASSERT_NE(bpm->new_page(&page_id), nullptr);
        ASSERT_TRUE(bpm->unpin_page(page_id, false));
    }
    EXPECT_EQ(bpm->page_table_.size(), buffer_pool_size);
    EXPECT_EQ(bpm->page_table_.bucket_count(), initial_bucket_count);
}

// Two owners can hold the same page - the index root cache pins the root and
// hands the very same raw page to a writer - so the owner that releases second
// finds pin_count_ already at zero. Losing its dirty mark there would silently
// drop a committed page modification.
TEST_F(BufferPoolManagerTest, UnpinMarksDirtyEvenWhenThePinWasAlreadyReleased) {
    auto bpm = std::make_unique<BufferPoolManager>(4, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);

    // First owner releases the only pin without reporting a modification.
    ASSERT_TRUE(bpm->unpin_page(page_id, false));
    ASSERT_FALSE(page->is_dirty());

    // Second owner reports its modification after the pin is already gone.
    EXPECT_FALSE(bpm->unpin_page(page_id, true));
    EXPECT_TRUE(page->is_dirty());
}

TEST_F(BufferPoolManagerTest, ResidentClassificationSurvivesVictimPressureAndUnmark) {
    auto bpm = std::make_unique<BufferPoolManager>(9, disk_manager_.get());
    PageId resident_id{fd_, INVALID_PAGE_ID};
    PageId ordinary_id{fd_, INVALID_PAGE_ID};
    PageId replacement_id{fd_, INVALID_PAGE_ID};

    ASSERT_NE(nullptr, bpm->new_page(&resident_id));
    ASSERT_TRUE(bpm->unpin_page(resident_id, false));
    ASSERT_NE(nullptr, bpm->new_page(&ordinary_id));
    ASSERT_TRUE(bpm->unpin_page(ordinary_id, false));

    ASSERT_TRUE(bpm->try_mark_resident(resident_id, ResidencyClass::IndexInternal));
    ASSERT_EQ(bpm->get_residency_class(resident_id), ResidencyClass::IndexInternal);

    // Keep every remaining frame pinned, leaving only the ordinary page as the
    // first victim and, after unmark, the former resident as the final victim.
    std::vector<PageId> pinned_fillers(7, PageId{fd_, INVALID_PAGE_ID});
    for (auto& filler_id : pinned_fillers) {
        ASSERT_NE(nullptr, bpm->new_page(&filler_id));
    }

    // The ordinary page is the first victim; keep the replacement page pinned
    // so the resident page is the only possible victim after it is explicitly
    // made evictable again.
    ASSERT_NE(nullptr, bpm->new_page(&replacement_id));
    EXPECT_TRUE(bpm->is_page_resident(resident_id));
    ASSERT_EQ(bpm->get_residency_class(resident_id), ResidencyClass::IndexInternal);

    bpm->unmark_resident(resident_id);
    ASSERT_EQ(bpm->get_residency_class(resident_id), ResidencyClass::Normal);

    PageId final_id{fd_, INVALID_PAGE_ID};
    ASSERT_NE(nullptr, bpm->new_page(&final_id));
    EXPECT_FALSE(bpm->is_page_resident(resident_id));
    EXPECT_FALSE(bpm->get_residency_class(resident_id).has_value());
    for (const auto& filler_id : pinned_fillers) {
        ASSERT_TRUE(bpm->unpin_page(filler_id, false));
    }
    ASSERT_TRUE(bpm->unpin_page(replacement_id, false));
    ASSERT_TRUE(bpm->unpin_page(final_id, false));
}

TEST_F(BufferPoolManagerTest, IndexResidencyQuotaIsGlobalIdempotentAndReclaimable) {
    auto bpm = std::make_unique<BufferPoolManager>(10, disk_manager_.get());
    std::vector<PageId> page_ids(10, PageId{fd_, INVALID_PAGE_ID});
    for (auto& page_id : page_ids) {
        ASSERT_NE(nullptr, bpm->new_page(&page_id));
        ASSERT_TRUE(bpm->unpin_page(page_id, false));
    }

    EXPECT_EQ(bpm->index_internal_residency_budget(), 2U);
    ASSERT_TRUE(bpm->try_mark_resident(page_ids[0], ResidencyClass::IndexInternal));
    ASSERT_TRUE(bpm->try_mark_resident(page_ids[0], ResidencyClass::IndexInternal));
    ASSERT_TRUE(bpm->try_mark_resident(page_ids[1], ResidencyClass::IndexInternal));
    EXPECT_FALSE(bpm->try_mark_resident(page_ids[2], ResidencyClass::IndexInternal));
    EXPECT_EQ(bpm->index_internal_resident_count(), 2U);
    EXPECT_EQ(bpm->get_residency_class(page_ids[2]), ResidencyClass::Normal);

    // Verify quota reclamation while the rejected page is still present.
    // Waiting until after replacement would accidentally test an evicted id.
    bpm->unmark_resident(page_ids[0]);
    EXPECT_EQ(bpm->index_internal_resident_count(), 1U);
    EXPECT_TRUE(bpm->try_mark_resident(page_ids[2], ResidencyClass::IndexInternal));
    EXPECT_EQ(bpm->index_internal_resident_count(), 2U);

    size_t ordinary_before = 0;
    for (const auto& page_id : page_ids) {
        ordinary_before += bpm->get_residency_class(page_id) == ResidencyClass::Normal;
    }
    PageId replacement_id{fd_, INVALID_PAGE_ID};
    ASSERT_NE(nullptr, bpm->new_page(&replacement_id));
    EXPECT_TRUE(bpm->is_page_resident(page_ids[1]));
    EXPECT_TRUE(bpm->is_page_resident(page_ids[2]));
    size_t ordinary_after = 0;
    for (const auto& page_id : page_ids) {
        ordinary_after += bpm->get_residency_class(page_id) == ResidencyClass::Normal;
    }
    EXPECT_EQ(ordinary_after + 1, ordinary_before);
    ASSERT_TRUE(bpm->unpin_page(replacement_id, false));
}

TEST_F(BufferPoolManagerTest, FlushPageFlushesWalBeforePageWrite) {
    auto disk_manager = BufferPoolManagerTest::disk_manager_.get();
    if (disk_manager->is_file(LOG_FILE_NAME)) {
        disk_manager->destroy_file(LOG_FILE_NAME);
    }
    disk_manager->create_file(LOG_FILE_NAME);

    LogManager log_manager(disk_manager);
    auto bpm = std::make_unique<BufferPoolManager>(10, disk_manager);
    bpm->set_log_manager(&log_manager);

    PageId page_id{BufferPoolManagerTest::fd_, INVALID_PAGE_ID};
    auto* page = bpm->new_page(&page_id);
    ASSERT_NE(nullptr, page);
    strcpy(page->get_data(), "wal-before-page");
    ASSERT_TRUE(bpm->unpin_page(page_id, true));

    BeginLogRecord begin_log(1);
    log_manager.add_log_to_buffer(&begin_log);
    ASSERT_EQ(0, disk_manager->get_file_size(LOG_FILE_NAME));

    ASSERT_TRUE(bpm->flush_page(page_id));
    EXPECT_GT(disk_manager->get_file_size(LOG_FILE_NAME), 0);
}

TEST_F(BufferPoolManagerTest, FlushAllPagesSkipsCleanPages) {
    auto disk_manager = BufferPoolManagerTest::disk_manager_.get();
    auto bpm = std::make_unique<BufferPoolManager>(2, disk_manager);

    PageId page_id{BufferPoolManagerTest::fd_, INVALID_PAGE_ID};
    auto* page = bpm->new_page(&page_id);
    ASSERT_NE(nullptr, page);
    std::strcpy(page->get_data(), "persisted");
    ASSERT_TRUE(bpm->unpin_page(page_id, true));
    bpm->flush_all_pages(BufferPoolManagerTest::fd_);

    page = bpm->fetch_page(page_id);
    ASSERT_NE(nullptr, page);
    std::strcpy(page->get_data(), "clean-only-memory");
    ASSERT_TRUE(bpm->unpin_page(page_id, false));
    bpm->flush_all_pages(BufferPoolManagerTest::fd_);

    BufferPoolManager reopened_bpm(2, disk_manager);
    auto* reopened_page = reopened_bpm.fetch_page(page_id);
    ASSERT_NE(nullptr, reopened_page);
    EXPECT_STREQ("persisted", reopened_page->get_data());
    ASSERT_TRUE(reopened_bpm.unpin_page(page_id, false));
}

TEST_F(BufferPoolManagerTest, FlushDirtyPagesHonorsPageBudget) {
    auto bpm = std::make_unique<BufferPoolManager>(3, disk_manager_.get());
    std::vector<PageId> page_ids;
    for (int i = 0; i < 3; ++i) {
        PageId page_id{fd_, INVALID_PAGE_ID};
        Page* page = bpm->new_page(&page_id);
        ASSERT_NE(page, nullptr);
        std::strcpy(page->get_data(), "preflush");
        ASSERT_TRUE(bpm->unpin_page(page_id, true));
        page_ids.push_back(page_id);
    }

    const auto first_flush = bpm->flush_dirty_pages({fd_}, 1);
    ASSERT_TRUE(first_flush.success);
    EXPECT_EQ(first_flush.pages_written, 1u);
    size_t dirty_pages = 0;
    for (const PageId& page_id : page_ids) {
        Page* page = bpm->fetch_page(page_id);
        ASSERT_NE(page, nullptr);
        dirty_pages += page->is_dirty() ? 1u : 0u;
        ASSERT_TRUE(bpm->unpin_page(page_id, false));
    }
    EXPECT_EQ(dirty_pages, 2u);

    const auto second_flush = bpm->flush_dirty_pages({fd_}, 3);
    ASSERT_TRUE(second_flush.success);
    EXPECT_EQ(second_flush.pages_written, 2u);
    for (const PageId& page_id : page_ids) {
        Page* page = bpm->fetch_page(page_id);
        ASSERT_NE(page, nullptr);
        EXPECT_STREQ(page->get_data(), "preflush");
        ASSERT_TRUE(bpm->unpin_page(page_id, false));
    }
}

TEST_F(BufferPoolManagerTest, FlushClaimEnsuresMergedDependencyOnceAcrossRuns) {
    ScopedOpenTestFile other_file(disk_manager_.get(), "dependency-other");
    auto bpm = std::make_unique<BufferPoolManager>(3, disk_manager_.get());
    auto log_manager = InstallTestWal(bpm.get(), disk_manager_.get(), 17);
    std::vector<PageId> pages;
    for (int i = 0; i < 3; ++i) {
        PageId id{i == 1 ? other_file.fd() : fd_, INVALID_PAGE_ID};
        Page* page = bpm->new_page(&id);
        ASSERT_NE(page, nullptr);
        std::strcpy(page->get_data(), "claim");
        BufferPoolManager::mark_dirty(page, PageWriteDependency::Wal(i == 1 ? 17 : 5));
        ASSERT_TRUE(bpm->unpin_page(id, false));
        pages.push_back(id);
    }

    size_t ensure_calls = 0;
    lsn_t ensured_lsn = INVALID_LSN;
    BufferPoolManager::set_ensure_dependency_test_hook([&](const PageWriteDependency& dependency) {
        ++ensure_calls;
        ensured_lsn = dependency.wal_lsn();
    });
    const auto result = bpm->flush_pages(pages, FlushDependencyPolicy::Enforce());
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.pages_written, 3u);
    EXPECT_EQ(ensure_calls, 1u);
    EXPECT_EQ(ensured_lsn, 17);
}

TEST_F(BufferPoolManagerTest, FlushClaimDependencyFailureWritesNothingAndRestoresFrames) {
    ScopedOpenTestFile other_file(disk_manager_.get(), "dependency-failure-other");
    auto bpm = std::make_unique<BufferPoolManager>(2, disk_manager_.get());
    auto log_manager = InstallTestWal(bpm.get(), disk_manager_.get(), 9);
    std::vector<PageId> pages;
    for (int i = 0; i < 2; ++i) {
        PageId id{i == 0 ? fd_ : other_file.fd(), INVALID_PAGE_ID};
        Page* page = bpm->new_page(&id);
        ASSERT_NE(page, nullptr);
        std::strcpy(page->get_data(), "not-written");
        BufferPoolManager::mark_dirty(page, PageWriteDependency::Wal(9));
        ASSERT_TRUE(bpm->unpin_page(id, false));
        pages.push_back(id);
    }
    size_t data_write_completions = 0;
    BufferPoolManager::set_flush_page_after_write_test_hook([&](PageId, Page*) { ++data_write_completions; });
    BufferPoolManager::set_ensure_dependency_test_hook(
        [](const PageWriteDependency&) { throw InternalError("injected dependency failure"); });
    const auto result = bpm->flush_pages(pages, FlushDependencyPolicy::Enforce());
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.pages_written, 0u);
    EXPECT_EQ(data_write_completions, 0u);
    for (const PageId id : pages) {
        Page* page = bpm->fetch_page(id);
        ASSERT_NE(page, nullptr);
        EXPECT_TRUE(page->is_dirty());
        EXPECT_EQ(page->state_.load(std::memory_order_acquire), FrameState::VALID);
        EXPECT_TRUE(bpm->unpin_page(id, false));
    }
}

TEST_F(BufferPoolManagerTest, FlushClaimKeepsFailedAndUnwrittenRunsDirty) {
    ScopedOpenTestFile failed_file(disk_manager_.get(), "failed-run");
    auto bpm = std::make_unique<BufferPoolManager>(2, disk_manager_.get());
    std::vector<PageId> pages;
    for (const int fd : {fd_, failed_file.fd()}) {
        PageId id{fd, INVALID_PAGE_ID};
        Page* page = bpm->new_page(&id);
        ASSERT_NE(page, nullptr);
        std::strcpy(page->get_data(), fd == fd_ ? "written" : "retained");
        ASSERT_TRUE(bpm->unpin_page(id, true));
        pages.push_back(id);
    }
    failed_file.Close();

    const auto result = bpm->flush_pages(pages, FlushDependencyPolicy::Enforce());
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.pages_written, 1u);
    for (size_t i = 0; i < pages.size(); ++i) {
        Page* page = bpm->fetch_page(pages[i]);
        ASSERT_NE(page, nullptr);
        EXPECT_EQ(page->is_dirty(), i != 0);
        EXPECT_EQ(page->state_.load(std::memory_order_acquire), FrameState::VALID);
        EXPECT_TRUE(bpm->unpin_page(pages[i], false));
    }
}

TEST_F(BufferPoolManagerTest, BackgroundFlushChecksSoftDeadlineAtClaimBoundary) {
    auto bpm = std::make_unique<BufferPoolManager>(96, disk_manager_.get());
    for (size_t i = 0; i < 96; ++i) {
        PageId id{fd_, INVALID_PAGE_ID};
        Page* page = bpm->new_page(&id);
        ASSERT_NE(page, nullptr);
        ASSERT_TRUE(bpm->unpin_page(id, true));
    }
    BufferPoolManager::set_flush_batch_before_write_test_hook(
        [](PageId, Page*) { std::this_thread::sleep_for(std::chrono::milliseconds(6)); });
    const auto result = bpm->flush_dirty_pages({fd_}, 96);
    EXPECT_TRUE(result.success);
    EXPECT_GT(result.pages_written, 0u);
    EXPECT_LE(result.pages_written, 64u);
    EXPECT_GT(bpm->count_dirty_pages({fd_}), 0u);
}

TEST_F(BufferPoolManagerTest, FlushBatchKeepsDependencyWhenPageIsRedirtiedAfterWrite) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    auto log_manager = InstallTestWal(bpm.get(), disk_manager_.get(), 73);
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);
    {
        std::unique_lock page_guard{page->latch()};
        std::strcpy(page->get_data(), "first");
        BufferPoolManager::mark_dirty(page, PageWriteDependency::Wal(41));
    }
    ASSERT_TRUE(bpm->unpin_page(page_id, false));

    BufferPoolManager::set_flush_page_after_write_test_hook([&](PageId flushed_id, Page* flushed_page) {
        if (!(flushed_id == page_id)) {
            return;
        }
        std::unique_lock page_guard{flushed_page->latch()};
        std::strcpy(flushed_page->get_data(), "second");
        BufferPoolManager::mark_dirty(flushed_page, PageWriteDependency::Wal(73));
    });
    std::vector<PageId> pages{page_id};
    ASSERT_TRUE(bpm->flush_pages(pages, FlushDependencyPolicy::Enforce()).success);
    BufferPoolManager::set_flush_page_after_write_test_hook({});

    page = bpm->fetch_page(page_id);
    ASSERT_NE(page, nullptr);
    EXPECT_TRUE(page->is_dirty());
    {
        std::scoped_lock dirty_guard{page->dirty_latch_};
        EXPECT_EQ(page->write_dependency_.kind(), PageWriteDependency::Kind::WalLsn);
        EXPECT_EQ(page->write_dependency_.wal_lsn(), 73);
    }
    ASSERT_TRUE(bpm->unpin_page(page_id, false));

    std::array<char, PAGE_SIZE> disk_image{};
    disk_manager_->read_page(fd_, page_id.page_no, disk_image.data(), PAGE_SIZE);
    EXPECT_STREQ(disk_image.data(), "first");

    ASSERT_TRUE(bpm->flush_pages(pages, FlushDependencyPolicy::Enforce()).success);
    page = bpm->fetch_page(page_id);
    ASSERT_NE(page, nullptr);
    EXPECT_FALSE(page->is_dirty());
    ASSERT_TRUE(bpm->unpin_page(page_id, false));
    disk_manager_->read_page(fd_, page_id.page_no, disk_image.data(), PAGE_SIZE);
    EXPECT_STREQ(disk_image.data(), "second");
}

TEST_F(BufferPoolManagerTest, FlushUsesPayloadAndWalDependencyPublishedUnderWriterLatch) {
    constexpr lsn_t kOldLsn = 41;
    constexpr lsn_t kLatestLsn = 73;
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    auto log_manager = InstallTestWal(bpm.get(), disk_manager_.get(), kLatestLsn);
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);
    {
        std::unique_lock page_lock{page->latch()};
        std::strcpy(page->get_data(), "old-wal-image");
        page->set_page_lsn(kOldLsn);
        BufferPoolManager::mark_dirty_locked(page, PageWriteDependency::Wal(kOldLsn));
    }
    ASSERT_TRUE(bpm->unpin_page(page_id, false));

    page = bpm->fetch_page(page_id);
    ASSERT_NE(page, nullptr);
    std::unique_lock writer_lock{page->latch()};

    std::mutex hook_mutex;
    std::condition_variable hook_cv;
    bool flush_waiting_for_writer = false;
    BufferPoolManager::set_flush_page_test_hook([&](PageId flushed_id, Page*) {
        if (!(flushed_id == page_id)) {
            return;
        }
        std::lock_guard lock{hook_mutex};
        flush_waiting_for_writer = true;
        hook_cv.notify_all();
    });

    std::atomic<bool> flush_ok{false};
    std::thread flusher([&] { flush_ok.store(bpm->flush_page(page_id), std::memory_order_release); });
    {
        std::unique_lock hook_lock{hook_mutex};
        EXPECT_TRUE(hook_cv.wait_for(hook_lock, std::chrono::seconds(2), [&] { return flush_waiting_for_writer; }));
    }

    // The flush thread has claimed the frame but cannot copy it until this
    // writer releases the payload latch. Publish the bytes and its matching
    // WAL dependency as one writer epoch.
    std::strcpy(page->get_data(), "latest-wal-image");
    page->set_page_lsn(kLatestLsn);
    BufferPoolManager::mark_dirty_locked(page, PageWriteDependency::Wal(kLatestLsn));
    writer_lock.unlock();
    EXPECT_TRUE(bpm->unpin_page(page_id, false));
    flusher.join();
    BufferPoolManager::set_flush_page_test_hook({});

    EXPECT_TRUE(flush_ok.load(std::memory_order_acquire));
    EXPECT_GE(log_manager->get_durable_lsn(), kLatestLsn);

    std::array<char, PAGE_SIZE> disk_image{};
    disk_manager_->read_page(fd_, page_id.page_no, disk_image.data(), PAGE_SIZE);
    EXPECT_STREQ(disk_image.data(), "latest-wal-image");

    BufferPoolManager reopened_bpm(1, disk_manager_.get());
    Page* reopened_page = reopened_bpm.fetch_page(page_id);
    ASSERT_NE(reopened_page, nullptr);
    EXPECT_STREQ(reopened_page->get_data(), "latest-wal-image");
    EXPECT_TRUE(reopened_bpm.unpin_page(page_id, false));
}

TEST_F(BufferPoolManagerTest, VictimReuseClearsExternalWriteDependency) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    auto log_manager = InstallTestWal(bpm.get(), disk_manager_.get(), 97);
    PageId first_id{fd_, INVALID_PAGE_ID};
    Page* first = bpm->new_page(&first_id);
    ASSERT_NE(first, nullptr);
    {
        std::unique_lock page_guard{first->latch()};
        std::strcpy(first->get_data(), "victim");
        BufferPoolManager::mark_dirty(first, PageWriteDependency::Wal(97));
    }
    ASSERT_TRUE(bpm->unpin_page(first_id, false));

    PageId second_id{fd_, INVALID_PAGE_ID};
    Page* second = bpm->new_page(&second_id);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(second, first);
    EXPECT_FALSE(second->is_dirty());
    {
        std::scoped_lock dirty_guard{second->dirty_latch_};
        EXPECT_EQ(second->dirty_epoch_.load(), 0u);
        EXPECT_EQ(second->write_dependency_.kind(), PageWriteDependency::Kind::None);
    }
    ASSERT_TRUE(bpm->unpin_page(second_id, false));

    Page* restored = bpm->fetch_page(first_id);
    ASSERT_NE(restored, nullptr);
    EXPECT_STREQ(restored->get_data(), "victim");
    ASSERT_TRUE(bpm->unpin_page(first_id, false));
}

TEST_F(BufferPoolManagerTest, BackgroundFlushUsesFdWhitelistAndRoundRobinCursor) {
    ScopedOpenTestFile other_file(disk_manager_.get(), "index-like");
    const int other_fd = other_file.fd();

    auto bpm = std::make_unique<BufferPoolManager>(5, disk_manager_.get());
    std::vector<PageId> table_pages;
    std::vector<PageId> other_pages;
    for (int frame = 0; frame < 5; ++frame) {
        PageId page_id{frame % 2 == 0 ? fd_ : other_fd, INVALID_PAGE_ID};
        Page* page = bpm->new_page(&page_id);
        ASSERT_NE(page, nullptr);
        std::strcpy(page->get_data(), "dirty");
        ASSERT_TRUE(bpm->unpin_page(page_id, true));
        (frame % 2 == 0 ? table_pages : other_pages).push_back(page_id);
    }

    ASSERT_EQ(bpm->count_dirty_pages({fd_}), 3u);
    ASSERT_EQ(bpm->count_dirty_pages({other_fd}), 2u);
    EXPECT_EQ(bpm->flush_dirty_pages({}, 5).pages_written, 0u);

    auto page_dirty = [&](PageId page_id) {
        Page* page = bpm->fetch_page(page_id);
        EXPECT_NE(page, nullptr);
        if (page == nullptr) {
            return false;
        }
        const bool dirty = page->is_dirty();
        EXPECT_TRUE(bpm->unpin_page(page_id, false));
        return dirty;
    };
    auto redirty = [&](PageId page_id) {
        Page* page = bpm->fetch_page(page_id);
        ASSERT_NE(page, nullptr);
        ASSERT_TRUE(bpm->unpin_page(page_id, true));
    };

    // Re-dirty each page after it is selected. A scanner that restarts from
    // the same frame would keep choosing it; the cursor must advance to the
    // next whitelisted dirty frame instead.
    for (size_t selected = 0; selected < table_pages.size(); ++selected) {
        const auto flushed = bpm->flush_dirty_pages({fd_}, 1);
        ASSERT_TRUE(flushed.success);
        EXPECT_EQ(flushed.pages_written, 1u);
        EXPECT_FALSE(page_dirty(table_pages[selected]));
        for (size_t later = selected + 1; later < table_pages.size(); ++later) {
            EXPECT_TRUE(page_dirty(table_pages[later]));
        }
        EXPECT_EQ(bpm->count_dirty_pages({other_fd}), 2u);
        redirty(table_pages[selected]);
    }

    EXPECT_EQ(bpm->count_dirty_pages({fd_}), 3u);
    const auto bounded_flush = bpm->flush_dirty_pages({fd_}, 2);
    ASSERT_TRUE(bounded_flush.success);
    EXPECT_EQ(bounded_flush.pages_written, 2u);
    EXPECT_EQ(bpm->count_dirty_pages({fd_}), 1u);
    const auto final_flush = bpm->flush_dirty_pages({fd_}, 2);
    ASSERT_TRUE(final_flush.success);
    EXPECT_EQ(final_flush.pages_written, 1u);

    for (const PageId& page_id : table_pages) {
        Page* page = bpm->fetch_page(page_id);
        ASSERT_NE(page, nullptr);
        EXPECT_FALSE(page->is_dirty());
        ASSERT_TRUE(bpm->unpin_page(page_id, false));
    }
    for (const PageId& page_id : other_pages) {
        Page* page = bpm->fetch_page(page_id);
        ASSERT_NE(page, nullptr);
        EXPECT_TRUE(page->is_dirty());
        ASSERT_TRUE(bpm->unpin_page(page_id, false));
    }
}

TEST_F(BufferPoolManagerTest, DerivedResidentDirectoryTracksAuthoritativeMappingTransitions) {
    auto bpm = std::make_unique<BufferPoolManager>(2, disk_manager_.get());
    EXPECT_TRUE(bpm->resident_directory_is_consistent_for_test());

    PageId first{fd_, INVALID_PAGE_ID};
    Page* first_page = bpm->new_page(&first);
    ASSERT_NE(first_page, nullptr);
    std::strcpy(first_page->get_data(), "first-resident");
    ASSERT_TRUE(bpm->unpin_page(first, true));
    EXPECT_TRUE(bpm->resident_directory_is_consistent_for_test());

    bool observed_batch_flushing = false;
    BufferPoolManager::set_flush_batch_before_write_test_hook([&](PageId page_id, Page*) {
        if (page_id == first) {
            observed_batch_flushing = true;
            EXPECT_TRUE(bpm->resident_directory_is_consistent_for_test());
        }
    });
    std::vector<PageId> batch{first};
    const auto flushed = bpm->flush_pages(batch, FlushDependencyPolicy::Enforce());
    BufferPoolManager::set_flush_batch_before_write_test_hook({});
    ASSERT_TRUE(flushed.success);
    EXPECT_EQ(flushed.pages_written, 1u);
    EXPECT_TRUE(observed_batch_flushing);
    EXPECT_TRUE(bpm->resident_directory_is_consistent_for_test());

    PageId second{fd_, INVALID_PAGE_ID};
    ASSERT_NE(bpm->new_page(&second), nullptr);
    ASSERT_TRUE(bpm->unpin_page(second, false));
    PageId third{fd_, INVALID_PAGE_ID};
    ASSERT_NE(bpm->new_page(&third), nullptr);
    ASSERT_TRUE(bpm->unpin_page(third, false));
    EXPECT_TRUE(bpm->resident_directory_is_consistent_for_test());

    EXPECT_TRUE(bpm->delete_page(third));
    EXPECT_TRUE(bpm->resident_directory_is_consistent_for_test());
    bpm->delete_all_pages(fd_);
    EXPECT_TRUE(bpm->page_table_.empty());
    EXPECT_TRUE(bpm->resident_directory_is_consistent_for_test());

    auto resident_bpm = std::make_unique<BufferPoolManager>(16, disk_manager_.get());
    PageId internal{fd_, INVALID_PAGE_ID};
    ASSERT_NE(resident_bpm->new_page(&internal), nullptr);
    ASSERT_TRUE(resident_bpm->try_mark_resident(internal, ResidencyClass::IndexInternal));
    EXPECT_TRUE(resident_bpm->resident_directory_is_consistent_for_test());
    resident_bpm->unmark_resident(internal);
    EXPECT_TRUE(resident_bpm->resident_directory_is_consistent_for_test());
    EXPECT_TRUE(resident_bpm->unpin_page(internal, false));
}

TEST_F(BufferPoolManagerTest, FastFetchPinPreventsConcurrentVictimReuse) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* original = bpm->new_page(&page_id);
    ASSERT_NE(original, nullptr);
    ASSERT_TRUE(bpm->unpin_page(page_id, false));

    std::unique_lock pin_block{original->pin_latch_};
    std::atomic<Page*> fetched{nullptr};
    std::atomic<bool> release_fetch{false};
    std::thread fetcher([&] {
        Page* page = bpm->fetch_page(page_id);
        fetched.store(page, std::memory_order_release);
        while (!release_fetch.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        if (page != nullptr) {
            EXPECT_TRUE(bpm->unpin_page(page_id, false));
        }
    });

    auto& shard = bpm->resident_directory_[bpm->resident_directory_shard_index(page_id)];
    bool fast_fetch_holds_shard = false;
    for (size_t attempt = 0; attempt < 1000000 && !fast_fetch_holds_shard; ++attempt) {
        if (shard.latch.try_lock()) {
            shard.latch.unlock();
            std::this_thread::yield();
        } else {
            fast_fetch_holds_shard = true;
        }
    }
    EXPECT_TRUE(fast_fetch_holds_shard);
    if (!fast_fetch_holds_shard) {
        pin_block.unlock();
        release_fetch.store(true, std::memory_order_release);
        fetcher.join();
        FAIL() << "fast fetch did not enter the resident shard";
    }

    PageId replacement_id{fd_, INVALID_PAGE_ID};
    std::atomic<Page*> replacement{original};
    std::atomic<bool> replacement_done{false};
    std::thread victim([&] {
        replacement.store(bpm->new_page(&replacement_id), std::memory_order_release);
        replacement_done.store(true, std::memory_order_release);
    });
    bool victim_removed_clock_candidate = false;
    for (size_t attempt = 0; attempt < 1000000 && !victim_removed_clock_candidate; ++attempt) {
        victim_removed_clock_candidate = bpm->replacer_->Size() == 0;
        std::this_thread::yield();
    }
    EXPECT_TRUE(victim_removed_clock_candidate);

    pin_block.unlock();
    victim.join();
    EXPECT_TRUE(replacement_done.load(std::memory_order_acquire));
    EXPECT_EQ(replacement.load(std::memory_order_acquire), nullptr);
    EXPECT_EQ(fetched.load(std::memory_order_acquire), original);
    EXPECT_EQ(original->get_page_id(), page_id);

    release_fetch.store(true, std::memory_order_release);
    fetcher.join();
    Page* retried = bpm->new_page(&replacement_id);
    ASSERT_NE(retried, nullptr);
    EXPECT_TRUE(bpm->unpin_page(replacement_id, false));
}

TEST_F(BufferPoolManagerTest, FrameOperationDrainsStartedFastHitBeforeReservingFrames) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);
    ASSERT_TRUE(bpm->unpin_page(page_id, false));

    std::unique_lock pin_block{page->pin_latch_};
    std::atomic<bool> fetched{false};
    std::atomic<bool> allow_unpin{false};
    std::thread fetcher([&] {
        Page* fetched_page = bpm->fetch_page(page_id);
        fetched.store(fetched_page != nullptr, std::memory_order_release);
        while (!allow_unpin.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        if (fetched_page != nullptr) {
            EXPECT_TRUE(bpm->unpin_page(page_id, false));
        }
    });

    auto& shard = bpm->resident_directory_[bpm->resident_directory_shard_index(page_id)];
    bool fast_fetch_holds_shard = false;
    for (size_t attempt = 0; attempt < 1000000 && !fast_fetch_holds_shard; ++attempt) {
        if (shard.latch.try_lock()) {
            shard.latch.unlock();
            std::this_thread::yield();
        } else {
            fast_fetch_holds_shard = true;
        }
    }
    EXPECT_TRUE(fast_fetch_holds_shard);
    if (!fast_fetch_holds_shard) {
        pin_block.unlock();
        allow_unpin.store(true, std::memory_order_release);
        fetcher.join();
        FAIL() << "fast fetch did not enter the resident shard";
    }

    std::atomic<bool> reservation_acquired{false};
    std::atomic<bool> release_reservation{false};
    std::thread reserver([&] {
        auto reservation = bpm->acquire_frame_operation(1);
        reservation_acquired.store(true, std::memory_order_release);
        while (!release_reservation.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    });
    while (!bpm->frame_operation_active_.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(reservation_acquired.load(std::memory_order_acquire));

    pin_block.unlock();
    while (!fetched.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(reservation_acquired.load(std::memory_order_acquire));

    allow_unpin.store(true, std::memory_order_release);
    fetcher.join();
    while (!reservation_acquired.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    release_reservation.store(true, std::memory_order_release);
    reserver.join();
    EXPECT_FALSE(bpm->frame_operation_active_.load(std::memory_order_acquire));
}

TEST_F(BufferPoolManagerTest, FastFetchRejectsStaleDerivedEntryAndDeleteAllSkipsPinnedFrame) {
    auto bpm = std::make_unique<BufferPoolManager>(2, disk_manager_.get());
    PageId first_id{fd_, INVALID_PAGE_ID};
    Page* first = bpm->new_page(&first_id);
    ASSERT_NE(first, nullptr);
    ASSERT_TRUE(bpm->unpin_page(first_id, false));
    PageId second_id{fd_, INVALID_PAGE_ID};
    Page* second = bpm->new_page(&second_id);
    ASSERT_NE(second, nullptr);
    ASSERT_TRUE(bpm->unpin_page(second_id, false));

    const frame_id_t first_fid = static_cast<frame_id_t>(first - bpm->pages_.get());
    const frame_id_t second_fid = static_cast<frame_id_t>(second - bpm->pages_.get());
    auto& shard = bpm->resident_directory_[bpm->resident_directory_shard_index(first_id)];
    {
        std::unique_lock global_lock{bpm->latch_};
        std::unique_lock shard_lock{shard.latch};
        shard.entries.insert_or_assign(first_id, second_fid);
    }
    Page* fetched = bpm->fetch_page(first_id);
    ASSERT_EQ(fetched, first);
    EXPECT_NE(fetched, second);
    ASSERT_TRUE(bpm->unpin_page(first_id, false));
    {
        std::unique_lock global_lock{bpm->latch_};
        std::unique_lock shard_lock{shard.latch};
        shard.entries.insert_or_assign(first_id, first_fid);
    }
    EXPECT_TRUE(bpm->resident_directory_is_consistent_for_test());

    std::atomic<Page*> pinned{nullptr};
    std::atomic<bool> release_pin{false};
    std::thread holder([&] {
        Page* held = bpm->fetch_page(first_id);
        pinned.store(held, std::memory_order_release);
        while (!release_pin.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        if (held != nullptr) {
            EXPECT_TRUE(bpm->unpin_page(first_id, false));
        }
    });
    while (pinned.load(std::memory_order_acquire) == nullptr) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(bpm->delete_page(first_id));
    bpm->delete_all_pages(fd_);
    EXPECT_NE(bpm->page_table_.find(first_id), bpm->page_table_.end());
    EXPECT_EQ(first->get_page_id(), first_id);

    release_pin.store(true, std::memory_order_release);
    holder.join();
    bpm->delete_all_pages(fd_);
    EXPECT_TRUE(bpm->page_table_.empty());
    EXPECT_TRUE(bpm->resident_directory_is_consistent_for_test());
}

TEST_F(BufferPoolManagerTest, CleanFastUnpinHandlesPinTransitionsAndResidentFrames) {
    auto bpm = std::make_unique<BufferPoolManager>(16, disk_manager_.get());

    auto unpin_while_global_locked = [&](PageId page_id, Page* page, int expected_pin_count,
                                         size_t expected_replacer_size) {
        std::atomic<bool> finished{false};
        std::atomic<bool> result{false};
        std::unique_lock global_block{bpm->latch_};
        std::thread unpinner([&] {
            result.store(bpm->unpin_page(page_id, false), std::memory_order_release);
            finished.store(true, std::memory_order_release);
        });

        bool completed_without_global = false;
        for (size_t attempt = 0; attempt < 1000000 && !completed_without_global; ++attempt) {
            completed_without_global = finished.load(std::memory_order_acquire);
            std::this_thread::yield();
        }
        global_block.unlock();
        unpinner.join();

        EXPECT_TRUE(completed_without_global);
        EXPECT_TRUE(result.load(std::memory_order_acquire));
        {
            std::scoped_lock pin_lock{page->pin_latch_};
            EXPECT_EQ(page->pin_count_, expected_pin_count);
        }
        EXPECT_EQ(bpm->replacer_->Size(), expected_replacer_size);
    };

    PageId single_pin_id{fd_, INVALID_PAGE_ID};
    Page* single_pin = bpm->new_page(&single_pin_id);
    ASSERT_NE(single_pin, nullptr);
    unpin_while_global_locked(single_pin_id, single_pin, 0, 1);
    EXPECT_FALSE(bpm->unpin_page(single_pin_id, false));

    PageId multiple_pin_id{fd_, INVALID_PAGE_ID};
    Page* multiple_pin = bpm->new_page(&multiple_pin_id);
    ASSERT_NE(multiple_pin, nullptr);
    ASSERT_EQ(bpm->fetch_page(multiple_pin_id), multiple_pin);
    unpin_while_global_locked(multiple_pin_id, multiple_pin, 1, 1);
    ASSERT_TRUE(bpm->unpin_page(multiple_pin_id, false));
    EXPECT_EQ(bpm->replacer_->Size(), 2u);

    PageId internal_id{fd_, INVALID_PAGE_ID};
    Page* internal = bpm->new_page(&internal_id);
    ASSERT_NE(internal, nullptr);
    ASSERT_TRUE(bpm->try_mark_resident(internal_id, ResidencyClass::IndexInternal));
    unpin_while_global_locked(internal_id, internal, 0, 2);
    bpm->unmark_resident(internal_id);
    EXPECT_EQ(bpm->replacer_->Size(), 3u);
}

TEST_F(BufferPoolManagerTest, FlushTransitionRepinsAfterConcurrentFastLastUnpin) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);
    std::strcpy(page->get_data(), "flush-race");
    BufferPoolManager::mark_dirty(page, PageWriteDependency::None());

    std::atomic<bool> flush_entered{false};
    std::atomic<bool> release_flush{false};
    BufferPoolManager::set_flush_page_test_hook([&](PageId flushed_id, Page*) {
        if (!(flushed_id == page_id)) {
            return;
        }
        flush_entered.store(true, std::memory_order_release);
        while (!release_flush.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    });

    std::unique_lock pin_block{page->pin_latch_};
    std::atomic<bool> unpin_result{false};
    std::atomic<bool> unpin_finished{false};
    std::thread unpinner([&] {
        unpin_result.store(bpm->unpin_page(page_id, false), std::memory_order_release);
        unpin_finished.store(true, std::memory_order_release);
    });

    auto& shard = bpm->resident_directory_[bpm->resident_directory_shard_index(page_id)];
    bool fast_unpin_holds_shard = false;
    for (size_t attempt = 0; attempt < 1000000 && !fast_unpin_holds_shard; ++attempt) {
        if (shard.latch.try_lock()) {
            shard.latch.unlock();
            std::this_thread::yield();
        } else {
            fast_unpin_holds_shard = true;
        }
    }
    if (!fast_unpin_holds_shard) {
        pin_block.unlock();
        unpinner.join();
        BufferPoolManager::set_flush_page_test_hook({});
        FAIL() << "clean unpin did not enter the resident shard";
    }

    std::atomic<bool> flush_result{false};
    std::thread flusher([&] { flush_result.store(bpm->flush_page(page_id), std::memory_order_release); });
    bool flusher_holds_global = false;
    for (size_t attempt = 0; attempt < 1000000 && !flusher_holds_global; ++attempt) {
        if (bpm->latch_.try_lock()) {
            bpm->latch_.unlock();
            std::this_thread::yield();
        } else {
            flusher_holds_global = true;
        }
    }
    if (!flusher_holds_global) {
        pin_block.unlock();
        release_flush.store(true, std::memory_order_release);
        unpinner.join();
        flusher.join();
        BufferPoolManager::set_flush_page_test_hook({});
        FAIL() << "flush did not reach the shard transition";
    }

    pin_block.unlock();
    while (!unpin_finished.load(std::memory_order_acquire) || !flush_entered.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(unpin_result.load(std::memory_order_acquire));
    EXPECT_EQ(page->state_.load(std::memory_order_acquire), FrameState::FLUSHING);
    EXPECT_EQ(bpm->replacer_->Size(), 0u);

    release_flush.store(true, std::memory_order_release);
    unpinner.join();
    flusher.join();
    BufferPoolManager::set_flush_page_test_hook({});
    EXPECT_TRUE(flush_result.load(std::memory_order_acquire));
    EXPECT_EQ(page->state_.load(std::memory_order_acquire), FrameState::VALID);
    EXPECT_EQ(bpm->replacer_->Size(), 1u);
    EXPECT_TRUE(bpm->resident_directory_is_consistent_for_test());
}

TEST_F(BufferPoolManagerTest, FastLastUnpinWakesFrameOperationReservation) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    ASSERT_NE(bpm->new_page(&page_id), nullptr);

    std::atomic<bool> reservation_acquired{false};
    std::atomic<bool> release_reservation{false};
    std::thread reserver([&] {
        auto reservation = bpm->acquire_frame_operation(1);
        reservation_acquired.store(true, std::memory_order_release);
        while (!release_reservation.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    });
    while (!bpm->frame_operation_active_.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    bool reservation_is_waiting = false;
    for (size_t attempt = 0; attempt < 1000000 && !reservation_is_waiting; ++attempt) {
        if (bpm->latch_.try_lock_shared()) {
            bpm->latch_.unlock_shared();
            reservation_is_waiting = true;
        } else {
            std::this_thread::yield();
        }
    }
    if (!reservation_is_waiting) {
        EXPECT_TRUE(bpm->unpin_page(page_id, false));
        bpm->frame_operation_cv_.notify_all();
        while (!reservation_acquired.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        release_reservation.store(true, std::memory_order_release);
        reserver.join();
        FAIL() << "frame operation did not reach its availability wait";
    }
    ASSERT_TRUE(bpm->unpin_page(page_id, false));

    bool woke_without_rescue = false;
    for (size_t attempt = 0; attempt < 1000000 && !woke_without_rescue; ++attempt) {
        woke_without_rescue = reservation_acquired.load(std::memory_order_acquire);
        std::this_thread::yield();
    }
    if (!woke_without_rescue) {
        bpm->frame_operation_cv_.notify_all();
        while (!reservation_acquired.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }
    release_reservation.store(true, std::memory_order_release);
    reserver.join();
    EXPECT_TRUE(woke_without_rescue);
    EXPECT_FALSE(bpm->frame_operation_active_.load(std::memory_order_acquire));
}

TEST_F(BufferPoolManagerTest, CleanUnpinFallsBackWhilePageIsFlushing) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);
    std::strcpy(page->get_data(), "flushing-fallback");
    BufferPoolManager::mark_dirty(page, PageWriteDependency::None());

    std::mutex mutex;
    std::condition_variable cv;
    bool flush_entered = false;
    bool release_flush = false;
    BufferPoolManager::set_flush_page_test_hook([&](PageId flushed_id, Page*) {
        if (!(flushed_id == page_id)) {
            return;
        }
        std::unique_lock lock{mutex};
        flush_entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_flush; });
    });

    std::thread flusher([&] { EXPECT_TRUE(bpm->flush_page(page_id)); });
    {
        std::unique_lock lock{mutex};
        cv.wait(lock, [&] { return flush_entered; });
    }
    EXPECT_EQ(page->state_.load(std::memory_order_acquire), FrameState::FLUSHING);
    EXPECT_EQ(bpm->replacer_->Size(), 0u);
    EXPECT_TRUE(bpm->unpin_page(page_id, false));
    {
        std::scoped_lock pin_lock{page->pin_latch_};
        EXPECT_EQ(page->pin_count_, 0);
    }
    EXPECT_EQ(bpm->replacer_->Size(), 0u);

    {
        std::lock_guard lock{mutex};
        release_flush = true;
    }
    cv.notify_all();
    flusher.join();
    BufferPoolManager::set_flush_page_test_hook({});
    EXPECT_EQ(page->state_.load(std::memory_order_acquire), FrameState::VALID);
    EXPECT_EQ(bpm->replacer_->Size(), 1u);
    EXPECT_TRUE(bpm->resident_directory_is_consistent_for_test());
}

TEST_F(BufferPoolManagerTest, CheckpointCohortHasFixedWhitelistedMembership) {
    ScopedOpenTestFile other_file(disk_manager_.get(), "checkpoint-other");
    auto bpm = std::make_unique<BufferPoolManager>(4, disk_manager_.get());

    auto make_page = [&](int fd, const char* contents, bool dirty) {
        PageId page_id{fd, INVALID_PAGE_ID};
        Page* page = bpm->new_page(&page_id);
        EXPECT_NE(page, nullptr);
        if (page != nullptr) {
            std::strcpy(page->get_data(), contents);
            EXPECT_TRUE(bpm->unpin_page(page_id, dirty));
        }
        return page_id;
    };

    const PageId first = make_page(fd_, "first", true);
    const PageId second = make_page(fd_, "second", true);
    const PageId late = make_page(fd_, "late", false);
    const PageId excluded = make_page(other_file.fd(), "excluded", true);

    const auto cohort = bpm->begin_checkpoint_cohort({fd_});
    ASSERT_NE(cohort.epoch, 0u);
    ASSERT_EQ(cohort.pages_marked, 2u);

    Page* late_page = bpm->fetch_page(late);
    ASSERT_NE(late_page, nullptr);
    std::strcpy(late_page->get_data(), "late-dirty");
    ASSERT_TRUE(bpm->unpin_page(late, true));

    const auto first_batch = bpm->flush_checkpoint_cohort(cohort.epoch, 1);
    ASSERT_TRUE(first_batch.success);
    EXPECT_EQ(first_batch.pages_written, 1u);
    EXPECT_EQ(first_batch.pages_remaining, 1u);
    const auto final_batch = bpm->flush_checkpoint_cohort(cohort.epoch, 8);
    ASSERT_TRUE(final_batch.success);
    EXPECT_EQ(final_batch.pages_written, 1u);
    EXPECT_EQ(final_batch.pages_remaining, 0u);

    EXPECT_EQ(bpm->count_dirty_pages({fd_}), 1u);
    EXPECT_EQ(bpm->count_dirty_pages({other_file.fd()}), 1u);
    for (const PageId& page_id : {first, second}) {
        Page* page = bpm->fetch_page(page_id);
        ASSERT_NE(page, nullptr);
        EXPECT_FALSE(page->is_dirty());
        EXPECT_TRUE(bpm->unpin_page(page_id, false));
    }
    late_page = bpm->fetch_page(late);
    ASSERT_NE(late_page, nullptr);
    EXPECT_TRUE(late_page->is_dirty());
    {
        std::scoped_lock dirty_lock{late_page->dirty_latch_};
        EXPECT_EQ(late_page->checkpoint_cohort_epoch_, 0u);
    }
    ASSERT_TRUE(bpm->unpin_page(late, false));

    const auto next_cohort = bpm->begin_checkpoint_cohort({fd_});
    EXPECT_GT(next_cohort.epoch, cohort.epoch);
    EXPECT_EQ(next_cohort.pages_marked, 1u);
    EXPECT_EQ(bpm->flush_checkpoint_cohort(next_cohort.epoch, 1).pages_remaining, 0u);

    Page* excluded_page = bpm->fetch_page(excluded);
    ASSERT_NE(excluded_page, nullptr);
    EXPECT_TRUE(excluded_page->is_dirty());
    ASSERT_TRUE(bpm->unpin_page(excluded, false));
}

TEST_F(BufferPoolManagerTest, CheckpointCohortPacingVisitsOnlyFixedPendingFrames) {
    constexpr size_t kCleanResidentPages = 2048;
    auto bpm = std::make_unique<BufferPoolManager>(kCleanResidentPages + 2, disk_manager_.get());
    for (size_t i = 0; i < kCleanResidentPages; ++i) {
        PageId clean_id{fd_, INVALID_PAGE_ID};
        ASSERT_NE(bpm->new_page(&clean_id), nullptr);
        ASSERT_TRUE(bpm->unpin_page(clean_id, false));
    }
    for (int i = 0; i < 2; ++i) {
        PageId dirty_id{fd_, INVALID_PAGE_ID};
        Page* page = bpm->new_page(&dirty_id);
        ASSERT_NE(page, nullptr);
        std::strcpy(page->get_data(), "fixed-pending");
        ASSERT_TRUE(bpm->unpin_page(dirty_id, true));
    }

    const auto cohort = bpm->begin_checkpoint_cohort({fd_});
    ASSERT_TRUE(cohort.success);
    ASSERT_EQ(cohort.pages_marked, 2u);
    ASSERT_EQ(bpm->checkpoint_cohort_pending_frames_.size(), 2u);
    ASSERT_GT(bpm->page_table_.size(), 2000u);

    const auto first = bpm->flush_checkpoint_cohort(cohort.epoch, 1);
    ASSERT_TRUE(first.success);
    EXPECT_EQ(first.pages_written, 1u);
    EXPECT_EQ(first.pages_remaining, 1u);
    EXPECT_LE(bpm->checkpoint_cohort_frames_visited_for_test_, 2u);
    EXPECT_LT(bpm->checkpoint_cohort_frames_visited_for_test_, bpm->page_table_.size());

    const auto second = bpm->flush_checkpoint_cohort(cohort.epoch, 1);
    ASSERT_TRUE(second.success);
    EXPECT_EQ(second.pages_written, 1u);
    EXPECT_EQ(second.pages_remaining, 0u);
    EXPECT_LE(bpm->checkpoint_cohort_frames_visited_for_test_, 2u);
    EXPECT_TRUE(bpm->checkpoint_cohort_pending_frames_.empty());
    EXPECT_EQ(bpm->active_checkpoint_cohort_epoch_, 0u);
}

TEST_F(BufferPoolManagerTest, CheckpointCohortSeparatesVisitAndIoBudgets) {
    auto bpm = std::make_unique<BufferPoolManager>(2, disk_manager_.get());
    PageId first{fd_, INVALID_PAGE_ID};
    PageId second{fd_, INVALID_PAGE_ID};
    ASSERT_NE(bpm->new_page(&first), nullptr);
    ASSERT_TRUE(bpm->unpin_page(first, true));
    ASSERT_NE(bpm->new_page(&second), nullptr);
    ASSERT_TRUE(bpm->unpin_page(second, true));
    const auto cohort = bpm->begin_checkpoint_cohort({fd_});
    ASSERT_TRUE(cohort.success);
    const auto bounded = bpm->flush_checkpoint_cohort(cohort.epoch, 8, 1);
    EXPECT_TRUE(bounded.success);
    EXPECT_EQ(bounded.pages_written, 1u);
    EXPECT_EQ(bounded.pages_remaining, 1u);
    EXPECT_EQ(bpm->checkpoint_cohort_frames_visited_for_test_, 1u);
    const auto final = bpm->flush_checkpoint_cohort(cohort.epoch, 1, 1);
    EXPECT_TRUE(final.success);
    EXPECT_EQ(final.pages_remaining, 0u);
}

TEST_F(BufferPoolManagerTest, CheckpointCohortCompletesOldImageButKeepsRedirty) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);
    std::strcpy(page->get_data(), "checkpoint-image");
    ASSERT_TRUE(bpm->unpin_page(page_id, true));

    const auto cohort = bpm->begin_checkpoint_cohort({fd_});
    ASSERT_EQ(cohort.pages_marked, 1u);
    BufferPoolManager::set_flush_page_after_write_test_hook([&](PageId flushed_id, Page* flushed_page) {
        if (!(flushed_id == page_id)) {
            return;
        }
        std::unique_lock page_lock{flushed_page->latch()};
        std::strcpy(flushed_page->get_data(), "newer-image");
        BufferPoolManager::mark_dirty(flushed_page, PageWriteDependency::None());
    });

    const auto flushed = bpm->flush_checkpoint_cohort(cohort.epoch, 1);
    BufferPoolManager::set_flush_page_after_write_test_hook({});
    ASSERT_TRUE(flushed.success);
    EXPECT_EQ(flushed.pages_written, 1u);
    EXPECT_EQ(flushed.pages_remaining, 0u);

    page = bpm->fetch_page(page_id);
    ASSERT_NE(page, nullptr);
    EXPECT_TRUE(page->is_dirty());
    EXPECT_STREQ(page->get_data(), "newer-image");
    {
        std::scoped_lock dirty_lock{page->dirty_latch_};
        EXPECT_EQ(page->checkpoint_cohort_epoch_, 0u);
    }
    ASSERT_TRUE(bpm->unpin_page(page_id, false));

    std::array<char, PAGE_SIZE> disk_image{};
    disk_manager_->read_page(fd_, page_id.page_no, disk_image.data(), PAGE_SIZE);
    EXPECT_STREQ(disk_image.data(), "checkpoint-image");
}

TEST_F(BufferPoolManagerTest, CheckpointPendingFrameRotatesWhileForegroundFlushIsInflight) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);
    std::strcpy(page->get_data(), "foreground-image");
    ASSERT_TRUE(bpm->unpin_page(page_id, true));
    const auto cohort = bpm->begin_checkpoint_cohort({fd_});
    ASSERT_TRUE(cohort.success);
    ASSERT_EQ(cohort.pages_marked, 1u);

    std::mutex mutex;
    std::condition_variable cv;
    bool write_complete = false;
    bool release_flush = false;
    BufferPoolManager::set_flush_page_after_write_test_hook([&](PageId flushed_id, Page*) {
        if (!(flushed_id == page_id)) {
            return;
        }
        std::unique_lock lock{mutex};
        write_complete = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_flush; });
    });
    std::thread foreground_flush([&] { EXPECT_TRUE(bpm->flush_page(page_id)); });
    {
        std::unique_lock lock{mutex};
        cv.wait(lock, [&] { return write_complete; });
    }

    const auto inflight = bpm->flush_checkpoint_cohort(cohort.epoch, 1);
    EXPECT_TRUE(inflight.success);
    EXPECT_EQ(inflight.pages_written, 0u);
    EXPECT_EQ(inflight.pages_remaining, 1u);
    EXPECT_EQ(bpm->checkpoint_cohort_pending_frames_.size(), 1u);
    EXPECT_EQ(bpm->checkpoint_cohort_frames_visited_for_test_, 1u);

    {
        std::lock_guard lock{mutex};
        release_flush = true;
    }
    cv.notify_all();
    foreground_flush.join();
    BufferPoolManager::set_flush_page_after_write_test_hook({});

    const auto settled = bpm->flush_checkpoint_cohort(cohort.epoch, 1);
    EXPECT_TRUE(settled.success);
    EXPECT_EQ(settled.pages_written, 0u);
    EXPECT_EQ(settled.pages_remaining, 0u);
    EXPECT_TRUE(bpm->checkpoint_cohort_pending_frames_.empty());
    EXPECT_EQ(bpm->active_checkpoint_cohort_epoch_, 0u);
}

TEST_F(BufferPoolManagerTest, CheckpointCutDoesNotSkipInflightRedirty) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);
    std::strcpy(page->get_data(), "old-image");
    ASSERT_TRUE(bpm->unpin_page(page_id, true));

    std::mutex mutex;
    std::condition_variable cv;
    bool redirtied_after_write = false;
    bool release_flush = false;
    BufferPoolManager::set_flush_page_after_write_test_hook([&](PageId flushed_id, Page* flushed_page) {
        if (!(flushed_id == page_id)) {
            return;
        }
        {
            std::unique_lock page_lock{flushed_page->latch()};
            std::strcpy(flushed_page->get_data(), "cut-visible-image");
            BufferPoolManager::mark_dirty(flushed_page, PageWriteDependency::None());
        }
        std::unique_lock lock{mutex};
        redirtied_after_write = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_flush; });
    });

    std::thread flusher([&] {
        const auto result = bpm->flush_dirty_pages({fd_}, 1);
        EXPECT_TRUE(result.success);
        EXPECT_EQ(result.pages_written, 1u);
    });
    {
        std::unique_lock lock{mutex};
        cv.wait(lock, [&] { return redirtied_after_write; });
    }

    // Before the fix this returned a successful empty cohort because the page
    // was FLUSHING. A bounded cut must instead decline without consuming an
    // epoch while the old write is still claimed.
    const auto timed_out = bpm->begin_checkpoint_cohort({fd_}, std::chrono::milliseconds(0));
    EXPECT_FALSE(timed_out.success);
    EXPECT_EQ(timed_out.epoch, 0u);
    EXPECT_EQ(timed_out.pages_marked, 0u);

    {
        std::lock_guard lock{mutex};
        release_flush = true;
    }
    cv.notify_all();
    flusher.join();
    BufferPoolManager::set_flush_page_after_write_test_hook({});

    const auto cohort = bpm->begin_checkpoint_cohort({fd_});
    ASSERT_TRUE(cohort.success);
    ASSERT_EQ(cohort.pages_marked, 1u);
    page = bpm->fetch_page(page_id);
    ASSERT_NE(page, nullptr);
    EXPECT_TRUE(page->is_dirty());
    EXPECT_STREQ(page->get_data(), "cut-visible-image");
    {
        std::scoped_lock dirty_lock{page->dirty_latch_};
        EXPECT_EQ(page->checkpoint_cohort_epoch_, cohort.epoch);
    }
    ASSERT_TRUE(bpm->unpin_page(page_id, false));
    EXPECT_EQ(bpm->cancel_checkpoint_cohort(cohort.epoch), 1u);
}

TEST_F(BufferPoolManagerTest, CheckpointCohortEvictionDischargesOldPageMarker) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId old_page_id{fd_, INVALID_PAGE_ID};
    Page* old_page = bpm->new_page(&old_page_id);
    ASSERT_NE(old_page, nullptr);
    std::strcpy(old_page->get_data(), "cohort-victim");
    ASSERT_TRUE(bpm->unpin_page(old_page_id, true));
    const auto cohort = bpm->begin_checkpoint_cohort({fd_});
    ASSERT_EQ(cohort.pages_marked, 1u);

    PageId new_page_id{fd_, INVALID_PAGE_ID};
    Page* new_page = bpm->new_page(&new_page_id);
    ASSERT_NE(new_page, nullptr);
    ASSERT_EQ(new_page, old_page);
    {
        std::scoped_lock dirty_lock{new_page->dirty_latch_};
        EXPECT_EQ(new_page->checkpoint_cohort_epoch_, 0u);
    }
    EXPECT_TRUE(bpm->unpin_page(new_page_id, false));

    const auto progress = bpm->flush_checkpoint_cohort(cohort.epoch, 1);
    EXPECT_TRUE(progress.success);
    EXPECT_EQ(progress.pages_written, 0u);
    EXPECT_EQ(progress.pages_remaining, 0u);

    std::array<char, PAGE_SIZE> disk_image{};
    disk_manager_->read_page(fd_, old_page_id.page_no, disk_image.data(), PAGE_SIZE);
    EXPECT_STREQ(disk_image.data(), "cohort-victim");
}

TEST_F(BufferPoolManagerTest, FailedCheckpointCohortWriteRetainsMarkerForRetry) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);
    std::strcpy(page->get_data(), "retry-image");
    ASSERT_TRUE(bpm->unpin_page(page_id, true));
    const auto cohort = bpm->begin_checkpoint_cohort({fd_});
    ASSERT_EQ(cohort.pages_marked, 1u);

    BufferPoolManager::set_flush_batch_before_write_test_hook(
        [](PageId, Page*) { throw InternalError("injected checkpoint write failure"); });
    const auto failed = bpm->flush_checkpoint_cohort(cohort.epoch, 1);
    EXPECT_FALSE(failed.success);
    EXPECT_EQ(failed.pages_written, 0u);
    EXPECT_EQ(failed.pages_remaining, 1u);

    page = bpm->fetch_page(page_id);
    ASSERT_NE(page, nullptr);
    EXPECT_TRUE(page->is_dirty());
    {
        std::scoped_lock dirty_lock{page->dirty_latch_};
        EXPECT_EQ(page->checkpoint_cohort_epoch_, cohort.epoch);
    }
    ASSERT_TRUE(bpm->unpin_page(page_id, false));

    BufferPoolManager::set_flush_batch_before_write_test_hook({});
    const auto retried = bpm->flush_checkpoint_cohort(cohort.epoch, 1);
    EXPECT_TRUE(retried.success);
    EXPECT_EQ(retried.pages_written, 1u);
    EXPECT_EQ(retried.pages_remaining, 0u);
}

TEST_F(BufferPoolManagerTest, CancelCheckpointCohortIsExactAndPreservesDirtyDependency) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);
    std::strcpy(page->get_data(), "cancelled-image");
    ASSERT_TRUE(bpm->unpin_page(page_id, PageWriteDependency::Wal(55)));

    const auto cohort = bpm->begin_checkpoint_cohort({fd_});
    ASSERT_EQ(cohort.pages_marked, 1u);
    EXPECT_EQ(bpm->cancel_checkpoint_cohort(cohort.epoch + 1), 0u);
    EXPECT_EQ(bpm->flush_checkpoint_cohort(cohort.epoch, 0).pages_remaining, 1u);

    EXPECT_EQ(bpm->cancel_checkpoint_cohort(cohort.epoch), 1u);
    EXPECT_EQ(bpm->cancel_checkpoint_cohort(cohort.epoch), 0u);
    EXPECT_EQ(bpm->flush_checkpoint_cohort(cohort.epoch, 0).pages_remaining, 0u);

    page = bpm->fetch_page(page_id);
    ASSERT_NE(page, nullptr);
    EXPECT_TRUE(page->is_dirty());
    {
        std::scoped_lock dirty_lock{page->dirty_latch_};
        EXPECT_EQ(page->checkpoint_cohort_epoch_, 0u);
        EXPECT_EQ(page->write_dependency_.kind(), PageWriteDependency::Kind::WalLsn);
        EXPECT_EQ(page->write_dependency_.wal_lsn(), 55);
    }
    ASSERT_TRUE(bpm->unpin_page(page_id, false));

    const auto next_cohort = bpm->begin_checkpoint_cohort({fd_});
    EXPECT_GT(next_cohort.epoch, cohort.epoch);
    EXPECT_EQ(next_cohort.pages_marked, 1u);
    EXPECT_EQ(bpm->cancel_checkpoint_cohort(next_cohort.epoch), 1u);
}

TEST_F(BufferPoolManagerTest, ConcurrentFetchAndLastUnpinKeepPinnedFrameOutOfReplacer) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    ASSERT_NE(bpm->new_page(&page_id), nullptr);

    for (int iteration = 0; iteration < 1000; ++iteration) {
        std::atomic<int> ready{0};
        std::atomic<bool> start{false};
        Page* fetched = nullptr;
        std::thread fetcher([&] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            fetched = bpm->fetch_page(page_id);
        });
        std::thread unpinner([&] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            EXPECT_TRUE(bpm->unpin_page(page_id, false));
        });
        while (ready.load(std::memory_order_acquire) != 2) {
            std::this_thread::yield();
        }
        start.store(true, std::memory_order_release);
        fetcher.join();
        unpinner.join();

        ASSERT_NE(fetched, nullptr);
        EXPECT_EQ(bpm->pages_[0].pin_count_, 1);
        EXPECT_EQ(bpm->replacer_->Size(), 0);
    }

    EXPECT_TRUE(bpm->unpin_page(page_id, false));
}

TEST_F(BufferPoolManagerTest, BackgroundFlushDoesNotClearDirtyFromConcurrentWriter) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = bpm->new_page(&page_id);
    ASSERT_NE(page, nullptr);
    std::strcpy(page->get_data(), "before-flush");
    ASSERT_TRUE(bpm->unpin_page(page_id, true));

    Page* writer_page = bpm->fetch_page(page_id);
    ASSERT_NE(writer_page, nullptr);
    std::unique_lock<std::shared_mutex> block_flush(writer_page->latch());
    std::atomic<bool> writer_started{false};
    std::thread flusher([&] {
        const auto flushed = bpm->flush_dirty_pages({fd_}, 1);
        EXPECT_TRUE(flushed.success);
        EXPECT_EQ(flushed.pages_written, 1u);
    });
    while (writer_page->state_.load(std::memory_order_acquire) != FrameState::FLUSHING) {
        std::this_thread::yield();
    }
    std::thread writer([&] {
        writer_started.store(true, std::memory_order_release);
        std::unique_lock<std::shared_mutex> page_lock(writer_page->latch());
        std::strcpy(writer_page->get_data(), "after-flush-started");
        page_lock.unlock();
        EXPECT_TRUE(bpm->unpin_page(page_id, true));
    });
    while (!writer_started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    block_flush.unlock();
    flusher.join();
    writer.join();

    EXPECT_TRUE(writer_page->is_dirty());
    ASSERT_TRUE(bpm->flush_page(page_id));

    BufferPoolManager reopened_bpm(1, disk_manager_.get());
    Page* reopened_page = reopened_bpm.fetch_page(page_id);
    ASSERT_NE(reopened_page, nullptr);
    EXPECT_STREQ("after-flush-started", reopened_page->get_data());
    EXPECT_TRUE(reopened_bpm.unpin_page(page_id, false));
}

class BufferPoolManagerConcurrencyTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;
    int fd_ = -1; // 此文件描述符为disk_manager_->open_file的返回值

public:
    // This function is called before every test.
    void SetUp() override {
        ::testing::Test::SetUp();
        // For each test, we create a new DiskManager
        disk_manager_ = std::make_unique<DiskManager>();
        // 如果测试目录不存在，则先创建测试目录
        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        // 进入测试目录
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
        // 如果测试文件存在，则先删除原文件（最后留下来的文件存的是最后一个测试点的数据）
        if (disk_manager_->is_file(TEST_FILE_NAME_CCUR)) {
            disk_manager_->destroy_file(TEST_FILE_NAME_CCUR);
        }
        // 创建测试文件
        disk_manager_->create_file(TEST_FILE_NAME_CCUR);
        assert(disk_manager_->is_file(TEST_FILE_NAME_CCUR));
        // 打开测试文件
        fd_ = disk_manager_->open_file(TEST_FILE_NAME_CCUR);
        assert(fd_ != -1);
    }

    // This function is called after every test.
    void TearDown() override {
        disk_manager_->close_file(fd_);
        // disk_manager_->destroy_file(TEST_FILE_NAME_CCUR);  // you can choose to delete the file

        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    }
};

TEST_F(BufferPoolManagerConcurrencyTest, ConcurrencyTest) {
    const int num_threads = 5;
    const int num_runs = 50;

    // get fd
    int fd = BufferPoolManagerConcurrencyTest::fd_;

    for (int run = 0; run < num_runs; run++) {
        // create BufferPoolManager
        auto disk_manager = BufferPoolManagerConcurrencyTest::disk_manager_.get();
        auto bpm = std::make_shared<BufferPoolManager>(50, disk_manager);

        std::vector<std::thread> threads;
        for (int tid = 0; tid < num_threads; tid++) {
            threads.push_back(std::thread([&bpm, fd]() { // NOLINT
                PageId temp_page_id = {.fd = fd, .page_no = INVALID_PAGE_ID};
                std::vector<PageId> page_ids;
                for (int i = 0; i < 10; i++) {
                    auto new_page = bpm->new_page(&temp_page_id);
                    EXPECT_NE(nullptr, new_page);
                    ASSERT_NE(nullptr, new_page);
                    strcpy(new_page->get_data(), std::to_string(temp_page_id.page_no).c_str()); // NOLINT
                    page_ids.push_back(temp_page_id);
                }
                for (int i = 0; i < 10; i++) {
                    EXPECT_EQ(1, bpm->unpin_page(page_ids[i], true));
                }
                for (int j = 0; j < 10; j++) {
                    auto page = bpm->fetch_page(page_ids[j]);
                    EXPECT_NE(nullptr, page);
                    ASSERT_NE(nullptr, page);
                    EXPECT_EQ(0, std::strcmp(std::to_string(page_ids[j].page_no).c_str(), (page->get_data())));
                    EXPECT_EQ(1, bpm->unpin_page(page_ids[j], true));
                }
                for (int j = 0; j < 10; j++) {
                    EXPECT_EQ(1, bpm->delete_page(page_ids[j]));
                }
                bpm->flush_all_pages(fd); // add this test by jiawen
            }));
        } // end loop tid=[0,num_threads)

        for (int i = 0; i < num_threads; i++) {
            threads[i].join();
        }
    } // end loop run=[0,num_runs)
}
