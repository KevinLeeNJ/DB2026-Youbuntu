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
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <unistd.h>

#include "gtest/gtest.h"
#include "storage/disk_manager.h"
#include "replacer/lru_replacer.h"
#include "errors.h"
#include "test_util.h"

const std::string TEST_DB_NAME = "disk_manager_test_db";
const std::string TEST_FILE_NAME_BIG = "bigdata";
constexpr int MAX_FILES = 32;
constexpr int MAX_PAGES = 128;
constexpr size_t TEST_BUFFER_POOL_SIZE = MAX_FILES * MAX_PAGES;

namespace {

std::unique_ptr<DiskManager> disk_manager;
std::unique_ptr<BufferPoolManager> buffer_pool_manager;
std::unordered_map<int, char*> mock; // fd -> buffer

char* mock_get_page(int fd, int page_no) {
    return &mock[fd][page_no * PAGE_SIZE];
}

class ScopedRegistryTempDir {
public:
    explicit ScopedRegistryTempDir(const std::string& tag)
        : path_(std::filesystem::temp_directory_path() /
                ("rmdb_" + tag + "_" + std::to_string(getpid()) + "_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))) {
        std::filesystem::create_directory(path_);
    }
    ~ScopedRegistryTempDir() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }
    const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

class ScopedCurrentDirectory {
public:
    explicit ScopedCurrentDirectory(const std::filesystem::path& path) : previous_(std::filesystem::current_path()) {
        std::filesystem::current_path(path);
    }
    ~ScopedCurrentDirectory() { std::filesystem::current_path(previous_); }

private:
    std::filesystem::path previous_;
};

size_t ActiveRegistryOperationsForTest(DiskManager* disk) {
    std::lock_guard<std::mutex> lock(disk->registry_->mutex);
    return disk->registry_->active_operations;
}

struct SegmentedReadBarrier {
    std::mutex mutex;
    std::condition_variable cv;
    bool entered{false};
    bool release{false};

    static void Pause(void* context) {
        auto* barrier = static_cast<SegmentedReadBarrier*>(context);
        std::unique_lock<std::mutex> lock(barrier->mutex);
        barrier->entered = true;
        barrier->cv.notify_all();
        barrier->cv.wait(lock, [&] { return barrier->release; });
    }
};

void RunNestedOpenAcrossShutdown(const std::function<void(DiskManager*)>& operation) {
    ScopedRegistryTempDir dir("registry_nested_admission");
    ScopedCurrentDirectory current(dir.path());
    auto disk = std::make_unique<DiskManager>();
    disk->create_file(LOG_FILE_NAME);
    std::mutex mutex;
    std::condition_variable cv;
    bool entered_open = false;
    bool release_open = false;
    disk->set_file_lifecycle_test_hooks([&] {
        std::unique_lock<std::mutex> lock(mutex);
        entered_open = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_open; });
    }, {});
    auto active = std::async(std::launch::async, [&] { operation(disk.get()); });
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return entered_open; }));
    }
    EXPECT_EQ(ActiveRegistryOperationsForTest(disk.get()), 1U);
    auto shutdown = std::async(std::launch::async, [&] { disk.reset(); });
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    {
        std::lock_guard<std::mutex> lock(mutex);
        release_open = true;
    }
    cv.notify_all();
    ASSERT_EQ(active.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    EXPECT_NO_THROW(active.get());
    ASSERT_EQ(shutdown.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    shutdown.get();
}

void check_disk(int fd, int page_no) {
    char buf[PAGE_SIZE];
    disk_manager->read_page(fd, page_no, buf, PAGE_SIZE);
    char* mock_buf = mock_get_page(fd, page_no);
    assert(memcmp(buf, mock_buf, PAGE_SIZE) == 0);
}

void check_disk_all() {
    for (auto& file : mock) {
        int fd = file.first;
        for (int page_no = 0; page_no < MAX_PAGES; page_no++) {
            check_disk(fd, page_no);
        }
    }
}

void check_cache(int fd, int page_no) {
    Page* page = buffer_pool_manager->fetch_page(PageId{fd, page_no});
    char* mock_buf = mock_get_page(fd, page_no);
    assert(memcmp(page->get_data(), mock_buf, PAGE_SIZE) == 0);
    buffer_pool_manager->unpin_page(PageId{fd, page_no}, false);
}

void check_cache_all() {
    for (auto& file : mock) {
        int fd = file.first;
        for (int page_no = 0; page_no < MAX_PAGES; page_no++) {
            check_cache(fd, page_no);
        }
    }
}

int rand_fd() {
    assert(mock.size() == MAX_FILES);
    int fd_idx = rand() % MAX_FILES;
    auto it = mock.begin();
    for (int i = 0; i < fd_idx; i++) {
        it++;
    }
    return it->first;
}

} // namespace

class BigStorageTest : public ::testing::Test {
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
        if (disk_manager_->is_file(TEST_FILE_NAME_BIG)) {
            disk_manager_->destroy_file(TEST_FILE_NAME_BIG);
        }
        // 创建测试文件
        disk_manager_->create_file(TEST_FILE_NAME_BIG);
        assert(disk_manager_->is_file(TEST_FILE_NAME_BIG));
        // 打开测试文件
        fd_ = disk_manager_->open_file(TEST_FILE_NAME_BIG);
        assert(fd_ != -1);
    }

    // This function is called after every test.
    void TearDown() override {
        disk_manager_->close_file(fd_);
        // disk_manager_->destroy_file(TEST_FILE_NAME_BIG);  // you can choose to delete the file

        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    }
};

TEST(StorageRegistryTest, ClosingPathRemainsReservedWhileOtherPathProgresses) {
    const std::filesystem::path dir = std::filesystem::temp_directory_path() /
                                      ("rmdb_registry_" + std::to_string(getpid()) + "_" +
                                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directory(dir);
    const std::string first = (dir / "first").string();
    const std::string other = (dir / "other").string();
    DiskManager disk;
    disk.create_file(first);
    disk.create_file(other);
    const int fd = disk.open_file(first);
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false;
    bool release = false;
    bool first_close = true;
    disk.set_file_lifecycle_test_hooks({}, [&] {
        std::unique_lock<std::mutex> lock(mutex);
        if (!first_close) return;
        first_close = false;
        entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
    });
    auto closing = std::async(std::launch::async, [&] { disk.close_file(fd); });
    { std::unique_lock<std::mutex> lock(mutex); ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return entered; })); }
    EXPECT_THROW(disk.open_file(first), FileNotOpenError);
    EXPECT_THROW(disk.destroy_file(first), FileNotClosedError);
    const int other_fd = disk.open_file(other);
    EXPECT_GE(other_fd, 0);
    disk.close_file(other_fd);
    { std::lock_guard<std::mutex> lock(mutex); release = true; }
    cv.notify_all();
    ASSERT_EQ(closing.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    closing.get();
    disk.destroy_file(first);
    disk.destroy_file(other);
    std::filesystem::remove_all(dir);
}

TEST(StorageRegistryTest, ClaimMoveAssignmentReleasesPreviousCounter) {
    const std::string path = (std::filesystem::temp_directory_path() /
                              ("rmdb_registry_move_" + std::to_string(getpid()))).string();
    std::filesystem::remove(path);
    DiskManager disk;
    disk.create_file(path);
    const int fd = disk.open_file(path);
    auto first = disk.acquire_file_write_claim(fd);
    auto second = disk.acquire_file_write_claim(fd);
    first = std::move(second);
    auto close = std::async(std::launch::async, [&] { disk.close_file(fd); });
    EXPECT_EQ(close.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    first = DiskManager::FileWriteClaim{};
    ASSERT_EQ(close.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    close.get();
    disk.destroy_file(path);
}

TEST(StorageRegistryTest, SlowOpenDoesNotBlockAnotherPath) {
    const std::filesystem::path dir = std::filesystem::temp_directory_path() /
                                      ("rmdb_registry_open_" + std::to_string(getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directory(dir);
    DiskManager disk;
    const std::string first = (dir / "first").string();
    const std::string other = (dir / "other").string();
    disk.create_file(first);
    disk.create_file(other);
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false;
    bool release = false;
    bool first_open = true;
    disk.set_file_lifecycle_test_hooks([&] {
        std::unique_lock<std::mutex> lock(mutex);
        if (!first_open) return;
        first_open = false;
        entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
    }, {});
    auto opening = std::async(std::launch::async, [&] { return disk.open_file(first); });
    { std::unique_lock<std::mutex> lock(mutex); ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return entered; })); }
    const int other_fd = disk.open_file(other);
    EXPECT_GE(other_fd, 0);
    disk.close_file(other_fd);
    { std::lock_guard<std::mutex> lock(mutex); release = true; }
    cv.notify_all();
    ASSERT_EQ(opening.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    disk.close_file(opening.get());
    disk.destroy_file(first);
    disk.destroy_file(other);
    std::filesystem::remove_all(dir);
}

TEST(StorageRegistryTest, CursorResetsAfterCloseAndManagerWaitsForLateClaim) {
    const std::string path = (std::filesystem::temp_directory_path() /
                              ("rmdb_registry_late_" + std::to_string(getpid()))).string();
    std::filesystem::remove(path);
    auto disk = std::make_unique<DiskManager>();
    disk->create_file(path);
    const int fd = disk->open_file(path);
    disk->set_fd2pageno(fd, 7);
    EXPECT_EQ(disk->allocate_page(fd), 7);
    auto claim = disk->acquire_file_write_claim(fd);
    auto destroy = std::async(std::launch::async, [&] { disk.reset(); });
    EXPECT_EQ(destroy.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    claim = DiskManager::FileWriteClaim{};
    ASSERT_EQ(destroy.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    destroy.get();
    DiskManager reopen;
    const int reopened = reopen.open_file(path);
    EXPECT_EQ(reopen.allocate_page(reopened), 0);
    reopen.close_file(reopened);
    reopen.destroy_file(path);
}

TEST(StorageRegistryTest, OperationFailureReleasesLeaseBeforeClose) {
    const std::string path = (std::filesystem::temp_directory_path() /
                              ("rmdb_registry_fault_" + std::to_string(getpid()))).string();
    std::filesystem::remove(path);
    DiskManager disk;
    disk.create_file(path);
    const int fd = disk.open_file(path);
    std::atomic<int> remaining{3};
    disk.set_file_operation_test_hook([&](DiskManager::FileOperationForTest) {
        if (remaining.fetch_sub(1) > 0) throw std::runtime_error("injected operation failure");
    });
    char page[PAGE_SIZE]{};
    EXPECT_THROW(disk.write_page(fd, 0, page, PAGE_SIZE), std::runtime_error);
    EXPECT_THROW(disk.read_page(fd, 0, page, PAGE_SIZE), std::runtime_error);
    EXPECT_THROW(disk.sync_file(fd), std::runtime_error);
    disk.set_file_operation_test_hook({});
    auto close = std::async(std::launch::async, [&] { disk.close_file(fd); });
    ASSERT_EQ(close.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    close.get();
    const int reopened = disk.open_file(path);
    disk.close_file(reopened);
    disk.destroy_file(path);
}

TEST(StorageRegistryTest, ReusedDescriptorRejectsOldGeneration) {
    const std::filesystem::path dir = std::filesystem::temp_directory_path() /
                                      ("rmdb_registry_aba_" + std::to_string(getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directory(dir);
    DiskManager disk;
    const std::string old_path = (dir / "old").string();
    const std::string new_path = (dir / "new").string();
    disk.create_file(old_path);
    disk.create_file(new_path);
    const int old_fd = disk.open_file(old_path);
    const auto old_identity = disk.capture_file_identity_for_test(old_fd);
    disk.close_file(old_fd);
    int new_fd = -1;
    for (int attempt = 0; attempt != 32 && new_fd != old_fd; ++attempt) {
        new_fd = disk.open_file(new_path);
        if (new_fd != old_fd) disk.close_file(new_fd);
    }
    ASSERT_EQ(new_fd, old_fd);
    EXPECT_TRUE(disk.stale_file_identity_rejected_for_test(old_identity));
    char page[PAGE_SIZE]{};
    page[0] = 'x';
    EXPECT_NO_THROW(disk.write_page(new_fd, 0, page, PAGE_SIZE));
    disk.close_file(new_fd);
    disk.destroy_file(old_path);
    disk.destroy_file(new_path);
    std::filesystem::remove_all(dir);
}

TEST(StorageRegistryTest, ClosingDescriptorRejectsRawSyncAndWaitsBeforeReuse) {
    ScopedRegistryTempDir dir("registry_close_barrier");
    const std::string first = (dir.path() / "first").string();
    const std::string second = (dir.path() / "second").string();
    DiskManager disk;
    disk.create_file(first);
    disk.create_file(second);
    const int fd = disk.open_file(first);
    std::mutex mutex;
    std::condition_variable cv;
    bool after_close = false;
    bool release = false;
    disk.set_file_operation_test_hook([&](DiskManager::FileOperationForTest operation) {
        if (operation != DiskManager::FileOperationForTest::CloseAfterSyscall) return;
        std::unique_lock<std::mutex> lock(mutex);
        after_close = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
    });
    auto closing = std::async(std::launch::async, [&] { disk.close_file(fd); });
    const bool close_reached_barrier = [&] {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(2), [&] { return after_close; });
    }();
    EXPECT_TRUE(close_reached_barrier);
    std::future<int> opening;
    if (close_reached_barrier) {
        EXPECT_THROW(disk.sync_file(fd), FileNotOpenError);
        opening = std::async(std::launch::async, [&] { return disk.open_file(second); });
        EXPECT_EQ(opening.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    }
    { std::lock_guard<std::mutex> lock(mutex); release = true; }
    cv.notify_all();
    ASSERT_EQ(closing.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    closing.get();
    if (close_reached_barrier) {
        ASSERT_EQ(opening.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        EXPECT_EQ(opening.get(), fd);
    } else {
        const int reopened = disk.open_file(second);
        EXPECT_EQ(reopened, fd);
    }
    disk.set_file_operation_test_hook({});
    disk.close_file(fd);
    disk.destroy_file(first);
    disk.destroy_file(second);
}

TEST(StorageRegistryTest, ExistingClaimCanBecomeLeaseWhileShutdownDrains) {
    const std::string path = (std::filesystem::temp_directory_path() /
                              ("rmdb_registry_shutdown_lease_" + std::to_string(getpid()) + "_" +
                               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())))
                                 .string();
    auto disk = std::make_unique<DiskManager>();
    disk->create_file(path);
    const int fd = disk->open_file(path);
    auto claim = disk->acquire_file_write_claim(fd);
    auto destroy = std::async(std::launch::async, [&] { disk.reset(); });
    ASSERT_EQ(destroy.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    auto lease = claim.acquire_lease();
    ASSERT_EQ(destroy.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    lease = DiskManager::FileLease{};
    ASSERT_EQ(destroy.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    destroy.get();
    DiskManager reopen;
    const int reopened = reopen.open_file(path);
    reopen.close_file(reopened);
    reopen.destroy_file(path);
}

TEST(StorageRegistryTest, AdmittedClaimCompletesAfterShutdownStarts) {
    ScopedRegistryTempDir dir("registry_admitted_claim_shutdown");
    const std::string path = (dir.path() / "file").string();
    auto disk = std::make_unique<DiskManager>();
    disk->create_file(path);
    const int fd = disk->open_file(path);
    const auto registry = disk->registry_;

    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false;
    bool release = false;
    disk->set_file_operation_entry_test_hook([&](DiskManager::FileOperationForTest operation) {
        if (operation != DiskManager::FileOperationForTest::Claim) return;
        std::unique_lock<std::mutex> lock(mutex);
        entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
    });
    auto acquiring = std::async(std::launch::async, [&] { return disk->acquire_file_write_claim(fd); });
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (!cv.wait_for(lock, std::chrono::seconds(2), [&] { return entered; })) {
            release = true;
            lock.unlock();
            cv.notify_all();
            FAIL() << "claim did not reach the post-admission barrier";
        }
    }
    EXPECT_EQ(ActiveRegistryOperationsForTest(disk.get()), 1U);

    auto shutdown = std::async(std::launch::async, [&] { disk.reset(); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    bool shutdown_started = false;
    while (std::chrono::steady_clock::now() < deadline) {
        {
            std::lock_guard<std::mutex> lock(registry->mutex);
            shutdown_started = registry->shutting_down;
        }
        if (shutdown_started) break;
        std::this_thread::yield();
    }
    if (!shutdown_started) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            release = true;
        }
        cv.notify_all();
        try {
            auto cleanup_claim = acquiring.get();
            (void)cleanup_claim;
        } catch (...) {
        }
        shutdown.wait();
        FAIL() << "destructor did not enter registry shutdown";
    }
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    {
        std::lock_guard<std::mutex> lock(mutex);
        release = true;
    }
    cv.notify_all();

    ASSERT_EQ(acquiring.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    auto claim = acquiring.get();
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    auto lease = claim.acquire_lease();
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    lease = DiskManager::FileLease{};
    ASSERT_EQ(shutdown.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    shutdown.get();
}

TEST(StorageRegistryTest, ClosingWaitsForClaimToLeaseHandoff) {
    ScopedRegistryTempDir dir("registry_closing_claim_handoff");
    const std::string path = (dir.path() / "file").string();
    DiskManager disk;
    disk.create_file(path);
    const int fd = disk.open_file(path);
    const auto identity = disk.capture_file_identity_for_test(fd);
    auto claim = disk.acquire_file_write_claim(fd);
    auto closing = std::async(std::launch::async, [&] { disk.close_file(fd); });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    bool closing_observed = false;
    while (std::chrono::steady_clock::now() < deadline) {
        try {
            auto new_claim = disk.acquire_file_write_claim(fd);
            new_claim = DiskManager::FileWriteClaim{};
        } catch (const FileNotOpenError&) {
            closing_observed = true;
            break;
        }
        std::this_thread::yield();
    }
    EXPECT_TRUE(closing_observed);
    if (!closing_observed) {
        claim = DiskManager::FileWriteClaim{};
        ASSERT_EQ(closing.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        closing.get();
        disk.destroy_file(path);
        return;
    }
    EXPECT_TRUE(disk.stale_file_identity_rejected_for_test(identity));
    EXPECT_TRUE(disk.stale_file_identity_rejected_for_test({identity.fd, identity.generation + 1}));

    auto lease = claim.acquire_lease();
    EXPECT_EQ(closing.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    lease = DiskManager::FileLease{};
    ASSERT_EQ(closing.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    closing.get();
    disk.destroy_file(path);
}

TEST(StorageRegistryTest, AdmittedReadWriteAndSyncDrainBeforeDestructor) {
    const std::array<DiskManager::FileOperationForTest, 3> operations{
        DiskManager::FileOperationForTest::Read,
        DiskManager::FileOperationForTest::Write,
        DiskManager::FileOperationForTest::Sync,
    };
    for (const auto expected : operations) {
        ScopedRegistryTempDir dir("registry_admission");
        const std::string path = (dir.path() / "file").string();
        auto disk = std::make_unique<DiskManager>();
        disk->create_file(path);
        const int fd = disk->open_file(path);
        char page[PAGE_SIZE]{};
        disk->write_page(fd, 0, page, PAGE_SIZE);
        std::mutex mutex;
        std::condition_variable cv;
        bool entered = false;
        bool release = false;
        disk->set_file_operation_entry_test_hook([&](DiskManager::FileOperationForTest operation) {
            if (operation != expected) return;
            std::unique_lock<std::mutex> lock(mutex);
            entered = true;
            cv.notify_all();
            cv.wait(lock, [&] { return release; });
        });
        auto active = std::async(std::launch::async, [&] {
            if (expected == DiskManager::FileOperationForTest::Read) {
                disk->read_page(fd, 0, page, PAGE_SIZE);
            } else if (expected == DiskManager::FileOperationForTest::Write) {
                disk->write_page(fd, 0, page, PAGE_SIZE);
            } else {
                disk->sync_file(fd);
            }
        });
        {
            std::unique_lock<std::mutex> lock(mutex);
            ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return entered; }));
        }
        EXPECT_EQ(ActiveRegistryOperationsForTest(disk.get()), 1U);
        auto destroy = std::async(std::launch::async, [&] { disk.reset(); });
        EXPECT_EQ(destroy.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
        {
            std::lock_guard<std::mutex> lock(mutex);
            release = true;
        }
        cv.notify_all();
        ASSERT_EQ(active.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        active.get();
        ASSERT_EQ(destroy.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        destroy.get();
    }
}

TEST(StorageRegistryTest, AdmittedRawSyncDrainsAndEntryFailureRollsBack) {
    ScopedRegistryTempDir dir("registry_raw_sync_admission");
    const std::string path = (dir.path() / "file").string();
    auto disk = std::make_unique<DiskManager>();
    disk->create_file(path);
    const int raw_fd = open(path.c_str(), O_RDWR);
    ASSERT_GE(raw_fd, 0);
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false;
    bool release = false;
    disk->set_file_operation_entry_test_hook([&](DiskManager::FileOperationForTest operation) {
        if (operation != DiskManager::FileOperationForTest::Sync) return;
        std::unique_lock<std::mutex> lock(mutex);
        entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
    });
    auto sync = std::async(std::launch::async, [&] { disk->sync_file(raw_fd); });
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return entered; }));
    }
    auto destroy = std::async(std::launch::async, [&] { disk.reset(); });
    EXPECT_EQ(destroy.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    {
        std::lock_guard<std::mutex> lock(mutex);
        release = true;
    }
    cv.notify_all();
    ASSERT_EQ(sync.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    sync.get();
    ASSERT_EQ(destroy.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    destroy.get();
    ASSERT_EQ(close(raw_fd), 0);

    auto failing = std::make_unique<DiskManager>();
    failing->set_file_operation_entry_test_hook([](DiskManager::FileOperationForTest operation) {
        if (operation == DiskManager::FileOperationForTest::Sync) throw std::runtime_error("entry failure");
    });
    const int second_raw_fd = open(path.c_str(), O_RDWR);
    ASSERT_GE(second_raw_fd, 0);
    EXPECT_THROW(failing->sync_file(second_raw_fd), std::runtime_error);
    EXPECT_EQ(ActiveRegistryOperationsForTest(failing.get()), 0U);
    auto failing_destroy = std::async(std::launch::async, [&] { failing.reset(); });
    ASSERT_EQ(failing_destroy.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    failing_destroy.get();
    ASSERT_EQ(close(second_raw_fd), 0);
}

TEST(StorageRegistryTest, MissingPathGetFileFdCompletesItsAdmittedNestedOpenAcrossShutdown) {
    ScopedRegistryTempDir dir("registry_get_file_fd_admission");
    const std::string path = (dir.path() / "missing_open").string();
    auto disk = std::make_unique<DiskManager>();
    disk->create_file(path);
    std::mutex mutex;
    std::condition_variable cv;
    bool entered_open = false;
    bool release_open = false;
    disk->set_file_lifecycle_test_hooks([&] {
        std::unique_lock<std::mutex> lock(mutex);
        entered_open = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_open; });
    }, {});
    auto active = std::async(std::launch::async, [&] { return disk->get_file_fd(path); });
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return entered_open; }));
    }
    auto shutdown = std::async(std::launch::async, [&] { disk.reset(); });
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    {
        std::lock_guard<std::mutex> lock(mutex);
        release_open = true;
    }
    cv.notify_all();
    ASSERT_EQ(active.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    EXPECT_GE(active.get(), 0);
    ASSERT_EQ(shutdown.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    shutdown.get();
}

TEST(StorageRegistryTest, LegacyWalNestedOpenCompletesAcrossShutdown) {
    const std::array<std::function<void(DiskManager*)>, 6> operations{
        [](DiskManager* disk) { EXPECT_EQ(disk->get_log_file_size(), 0); },
        [](DiskManager* disk) {
            char byte = 0;
            EXPECT_EQ(disk->read_log(&byte, 0, 0), 0);
        },
        [](DiskManager* disk) {
            char byte = 'w';
            disk->write_log(&byte, 1);
        },
        [](DiskManager* disk) { disk->fsync_log(); },
        [](DiskManager* disk) { disk->truncate_log(); },
        [](DiskManager* disk) { disk->truncate_log_to(0); },
    };
    for (const auto& operation : operations) {
        RunNestedOpenAcrossShutdown(operation);
    }
}

TEST(StorageRegistryTest, SegmentedReadChunkKeepsOuterAdmissionAcrossShutdown) {
    ScopedRegistryTempDir dir("registry_segmented_read_admission");
    ScopedCurrentDirectory current(dir.path());
    auto disk = std::make_unique<DiskManager>();
    disk->configure_segmented_wal(77, 0, 64);
    disk->ensure_segmented_wal_root();
    std::array<char, 80> written{};
    for (size_t i = 0; i < written.size(); ++i) written[i] = static_cast<char>(i);
    disk->write_log(written.data(), static_cast<int>(written.size()));

    SegmentedReadBarrier barrier;
    disk->set_segmented_read_test_hook(&SegmentedReadBarrier::Pause, &barrier);
    std::array<char, 80> read{};
    auto active = std::async(std::launch::async, [&] { return disk->read_log(read.data(), read.size(), 0); });
    {
        std::unique_lock<std::mutex> lock(barrier.mutex);
        ASSERT_TRUE(barrier.cv.wait_for(lock, std::chrono::seconds(2), [&] { return barrier.entered; }));
    }
    EXPECT_EQ(ActiveRegistryOperationsForTest(disk.get()), 1U);
    auto shutdown = std::async(std::launch::async, [&] { disk.reset(); });
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    {
        std::lock_guard<std::mutex> lock(barrier.mutex);
        barrier.release = true;
    }
    barrier.cv.notify_all();
    ASSERT_EQ(active.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    EXPECT_EQ(active.get(), static_cast<int>(read.size()));
    EXPECT_EQ(read, written);
    ASSERT_EQ(shutdown.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    shutdown.get();
}

TEST(StorageRegistryTest, ShutdownRejectsNewPublicRegistryEntry) {
    ScopedRegistryTempDir dir("registry_shutdown_rejects_claim");
    const std::string path = (dir.path() / "file").string();
    DiskManager disk;
    disk.create_file(path);
    const int fd = disk.open_file(path);
    {
        std::lock_guard<std::mutex> lock(disk.registry_->mutex);
        disk.registry_->shutting_down = true;
    }
    EXPECT_THROW(disk.acquire_file_write_claim(fd), FileNotOpenError);
    EXPECT_THROW(disk.get_file_fd("not_admitted"), FileNotOpenError);
    EXPECT_THROW(disk.get_log_file_size(), FileNotOpenError);
    EXPECT_EQ(ActiveRegistryOperationsForTest(&disk), 0U);
}

// TODO: fix detected memory leaks found by Google Test
TEST(StorageTest, SimpleTest) {
    srand((unsigned)time(nullptr));

    disk_manager = std::make_unique<DiskManager>();
    buffer_pool_manager = std::make_unique<BufferPoolManager>(TEST_BUFFER_POOL_SIZE, disk_manager.get());

    /** Test disk_manager */
    std::vector<std::string> filenames(MAX_FILES); // MAX_FILES=32
    std::unordered_map<int, std::string> fd2name;
    for (size_t i = 0; i < filenames.size(); i++) {
        auto& filename = filenames[i];
        filename = std::to_string(i) + ".txt";
        if (disk_manager->is_file(filename)) {
            disk_manager->destroy_file(filename);
        }
        // open without create
        try {
            disk_manager->open_file(filename);
            assert(false);
        } catch (const FileNotFoundError& e) {
        }

        disk_manager->create_file(filename);
        assert(disk_manager->is_file(filename));
        try {
            disk_manager->create_file(filename);
            assert(false);
        } catch (const FileExistsError& e) {
        }

        // open file
        int fd = disk_manager->open_file(filename);
        char* tmp = new char[PAGE_SIZE * MAX_PAGES]; // TODO: fix error in detected memory leaks

        mock[fd] = tmp;
        fd2name[fd] = filename;

        disk_manager->set_fd2pageno(fd, 0); // diskmanager在fd对应的文件中从0开始分配page_no
    }

    /** Test buffer_pool_manager*/
    int num_pages = 0;
    char init_buf[PAGE_SIZE];
    for (auto& fh : mock) {
        int fd = fh.first;
        for (page_id_t i = 0; i < MAX_PAGES; i++) {
            test::rand_buf(PAGE_SIZE, init_buf); // 将init_buf填充PAGE_SIZE个字节的随机数据

            PageId tmp_page_id = {.fd = fd, .page_no = INVALID_PAGE_ID};
            Page* page = buffer_pool_manager->new_page(&tmp_page_id);
            int page_no = tmp_page_id.page_no;
            assert(page_no != INVALID_PAGE_ID);
            assert(page_no == i);

            memcpy(page->get_data(), init_buf, PAGE_SIZE);
            buffer_pool_manager->unpin_page(PageId{fd, page_no}, true);

            char* mock_buf = mock_get_page(fd, page_no); // &mock[fd][page_no * PAGE_SIZE]
            memcpy(mock_buf, init_buf, PAGE_SIZE);

            num_pages++;

            check_cache(fd, page_no); // 调用了fetch_page, unpin_page
        }
    }
    check_cache_all();

    assert(num_pages == TEST_BUFFER_POOL_SIZE);

    /** Test flush_all_pages() */
    // Flush and test disk
    for (auto& entry : fd2name) {
        int fd = entry.first;
        buffer_pool_manager->flush_all_pages(fd);
        for (int page_no = 0; page_no < MAX_PAGES; page_no++) {
            check_disk(fd, page_no);
        }
    }
    check_disk_all();

    for (int r = 0; r < 10000; r++) {
        int fd = rand_fd();
        int page_no = rand() % MAX_PAGES;
        // fetch page
        Page* page = buffer_pool_manager->fetch_page(PageId{fd, page_no});
        char* mock_buf = mock_get_page(fd, page_no);
        assert(memcmp(page->get_data(), mock_buf, PAGE_SIZE) == 0);

        // modify
        test::rand_buf(PAGE_SIZE, init_buf);
        memcpy(page->get_data(), init_buf, PAGE_SIZE);
        memcpy(mock_buf, init_buf, PAGE_SIZE);

        buffer_pool_manager->unpin_page(page->get_page_id(), true);
        // BufferPool::mark_dirty(page);

        // flush
        if (rand() % 10 == 0) {
            buffer_pool_manager->flush_page(page->get_page_id());
            check_disk(fd, page_no);
        }
        // flush entire file
        if (rand() % 100 == 0) {
            buffer_pool_manager->flush_all_pages(fd);
        }
        // re-open file
        if (rand() % 100 == 0) {
            disk_manager->close_file(fd);
            auto filename = fd2name[fd];
            char* buf = mock[fd];
            fd2name.erase(fd);
            mock.erase(fd);
            int new_fd = disk_manager->open_file(filename);
            mock[new_fd] = buf;
            fd2name[new_fd] = filename;
        }
        // assert equal in cache
        check_cache(fd, page_no);
    }
    check_cache_all();

    for (auto& entry : fd2name) {
        int fd = entry.first;
        buffer_pool_manager->flush_all_pages(fd);
        for (int page_no = 0; page_no < MAX_PAGES; page_no++) {
            check_disk(fd, page_no);
        }
    }
    check_disk_all();

    // close and destroy files
    for (auto& entry : fd2name) {
        int fd = entry.first;
        auto& filename = entry.second;
        disk_manager->close_file(fd);
        disk_manager->destroy_file(filename);
        try {
            disk_manager->destroy_file(filename);
            assert(false);
        } catch (const FileNotFoundError& e) {
        }
        delete[] mock[fd]; // Free allocated mock buffer
    }
    mock.clear();
}
