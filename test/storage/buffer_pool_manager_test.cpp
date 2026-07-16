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

#include <cstring>
#include <cstdlib>
#include <memory>
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

class BufferPoolManagerTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;
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
        // 进入测试目录
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
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
        if (!fd_closed_) {
            disk_manager_->close_file(fd_);
        }
        // disk_manager_->destroy_file(TEST_FILE_NAME);  // you can choose to delete the file

        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    }
};

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

    EXPECT_EQ(bpm->flush_dirty_pages(1), 1u);
    size_t dirty_pages = 0;
    for (const PageId& page_id : page_ids) {
        Page* page = bpm->fetch_page(page_id);
        ASSERT_NE(page, nullptr);
        dirty_pages += page->is_dirty() ? 1u : 0u;
        ASSERT_TRUE(bpm->unpin_page(page_id, false));
    }
    EXPECT_EQ(dirty_pages, 2u);

    EXPECT_EQ(bpm->flush_dirty_pages(3), 2u);
    for (const PageId& page_id : page_ids) {
        Page* page = bpm->fetch_page(page_id);
        ASSERT_NE(page, nullptr);
        EXPECT_STREQ(page->get_data(), "preflush");
        ASSERT_TRUE(bpm->unpin_page(page_id, false));
    }
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

TEST_F(BufferPoolManagerTest, BpmMetricsReportPinTransitionsWhenEnabled) {
    if (std::getenv("RMDB_BPM_METRICS") == nullptr) {
        GTEST_SKIP() << "RMDB_BPM_METRICS is disabled";
    }

    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    PageId page_id{fd_, INVALID_PAGE_ID};
    ASSERT_NE(bpm->new_page(&page_id), nullptr);
    bpm->reset_stats();

    ASSERT_NE(bpm->fetch_page(page_id), nullptr);
    ASSERT_TRUE(bpm->unpin_page(page_id, false));
    ASSERT_TRUE(bpm->unpin_page(page_id, false));
    ASSERT_NE(bpm->fetch_page(page_id), nullptr);
    ASSERT_TRUE(bpm->unpin_page(page_id, false));

    BufferPoolManager::Stats stats = bpm->get_stats();
    EXPECT_EQ(stats.fetch_hits, 2u);
    EXPECT_EQ(stats.fetch_misses, 0u);
    EXPECT_EQ(stats.pin_0_to_1, 1u);
    EXPECT_EQ(stats.unpin_1_to_0, 2u);
}

TEST_F(BufferPoolManagerTest, FlushDoesNotClearDirtyFromConcurrentWriter) {
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
    std::thread flusher([&] { EXPECT_TRUE(bpm->flush_page(page_id)); });
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
