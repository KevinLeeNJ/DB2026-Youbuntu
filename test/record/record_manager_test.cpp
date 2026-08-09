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
#include "record/rm.h"
#include "storage/buffer_pool_manager.h"
#undef private

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>
#include <string>
#include <unistd.h>

#include "gtest/gtest.h"
#include "errors.h"
#include "test_util.h"

const std::string TEST_DB_NAME = "record_manager_test_db";

namespace {

struct rid_hash_t {
    size_t operator()(const Rid& rid) const {
        return (rid.page_no << 16) | rid.slot_no;
    }
};

struct rid_equal_t {
    bool operator()(const Rid& x, const Rid& y) const {
        return x.page_no == y.page_no && x.slot_no == y.slot_no;
    }
};

void check_equal(const RmFileHandle* file_handle,
                 const std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t>& mock) {
    // Test all records
    for (auto& entry : mock) {
        Rid rid = entry.first;
        auto mock_buf = (char*)entry.second.c_str();
        auto rec = file_handle->get_record(rid, nullptr);
        assert(memcmp(mock_buf, rec->data, file_handle->file_hdr_.record_size) == 0);
    }
    // Randomly get record
    for (int i = 0; i < 10; i++) {
        Rid rid = {.page_no = 1 + rand() % (file_handle->file_hdr_.num_pages - 1),
                   .slot_no = rand() % file_handle->file_hdr_.num_records_per_page};
        bool mock_exist = mock.count(rid) > 0;
        bool rm_exist = file_handle->is_record(rid);
        assert(rm_exist == mock_exist);
    }
    // Test RM scan
    size_t num_records = 0;
    for (RmScan scan(file_handle); !scan.is_end(); scan.next()) {
        assert(mock.count(scan.rid()) > 0);
        auto rec = file_handle->get_record(scan.rid(), nullptr);
        assert(memcmp(rec->data, mock.at(scan.rid()).c_str(), file_handle->file_hdr_.record_size) == 0);
        num_records++;
    }
    assert(num_records == mock.size());
}

std::string UniqueTestFileName(const std::string& prefix) {
    static std::atomic<uint64_t> next_id{0};
    return prefix + "_" + std::to_string(getpid()) + "_" + std::to_string(next_id.fetch_add(1));
}

void PersistRecordFileAndClose(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager,
                               std::unique_ptr<RmFileHandle>* file_handle) {
    ASSERT_NE(file_handle, nullptr);
    ASSERT_NE(file_handle->get(), nullptr);
    RmFileHandle* handle = file_handle->get();
    const RmFileHdr header = handle->get_file_hdr();
    const int fd = handle->GetFd();
    for (page_id_t page_no = RM_FIRST_RECORD_PAGE; page_no < header.num_pages; ++page_no) {
        RmPageHandle page = handle->fetch_page_handle(page_no);
        std::array<char, PAGE_SIZE> image{};
        {
            std::shared_lock<std::shared_mutex> page_lock(page.page->latch());
            std::memcpy(image.data(), page.page->get_data(), PAGE_SIZE);
        }
        ASSERT_TRUE(buffer_pool_manager->unpin_page(page.page->get_page_id(), false));
        disk_manager->write_page(fd, page_no, image.data(), PAGE_SIZE);
    }
    disk_manager->write_page(fd, RM_FILE_HDR_PAGE, reinterpret_cast<const char*>(&header), sizeof(header));
    buffer_pool_manager->delete_all_pages(fd);
    disk_manager->close_file(fd);
    file_handle->reset();
}

class ScopedRecordFile {
public:
    ScopedRecordFile(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, int record_size)
        : disk_manager_(disk_manager), rm_manager_(disk_manager, buffer_pool_manager), filename_(UniqueTestFileName("recovery_fd")) {
        rm_manager_.create_file(filename_, record_size);
        file_handle_ = rm_manager_.open_file(filename_);
    }

    ~ScopedRecordFile() {
        if (file_handle_ != nullptr) {
            try {
                rm_manager_.close_file(file_handle_.get());
            } catch (...) {
            }
        }
        if (disk_manager_->is_file(filename_)) {
            try {
                disk_manager_->destroy_file(filename_);
            } catch (...) {
            }
        }
    }

    RmFileHandle* handle() const {
        return file_handle_.get();
    }

    const std::string& filename() const {
        return filename_;
    }

private:
    DiskManager* disk_manager_;
    RmManager rm_manager_;
    std::string filename_;
    std::unique_ptr<RmFileHandle> file_handle_;
};

class ScopedDiskFile {
public:
    explicit ScopedDiskFile(DiskManager* disk_manager)
        : disk_manager_(disk_manager), filename_(UniqueTestFileName("closed_fd_size")) {
        disk_manager_->create_file(filename_);
        fd_ = disk_manager_->open_file(filename_);
    }

    ~ScopedDiskFile() {
        if (fd_ >= 0) {
            try {
                disk_manager_->close_file(fd_);
            } catch (...) {
            }
        }
        if (disk_manager_->is_file(filename_)) {
            try {
                disk_manager_->destroy_file(filename_);
            } catch (...) {
            }
        }
    }

    int fd() const {
        return fd_;
    }

    void close() {
        disk_manager_->close_file(fd_);
        fd_ = -1;
    }

private:
    DiskManager* disk_manager_;
    std::string filename_;
    int fd_{-1};
};

} // namespace

class RecordManagerTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;

public:
    // This function is called before every test.
    void SetUp() override {
        ::testing::Test::SetUp();
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
    }

    // This function is called after every test.
    void TearDown() override {
        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    }
};

TEST_F(RecordManagerTest, RecoverySizeChecksUseOpenFdAndKeepTailFailuresDistinct) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
    ScopedRecordFile file(disk_manager_.get(), buffer_pool_manager.get(), 64);
    RmFileHandle* const file_handle = file.handle();
    const int fd = file_handle->GetFd();
    EXPECT_EQ(disk_manager_->get_file_size(fd), disk_manager_->get_file_size(file.filename()));
    EXPECT_EQ(disk_manager_->get_file_size(fd), static_cast<int64_t>(sizeof(RmFileHdr)));
    EXPECT_NO_THROW(file_handle->repair_file_header_for_pages({RM_FIRST_RECORD_PAGE}));

    ASSERT_EQ(ftruncate(fd, static_cast<off_t>(PAGE_SIZE)), 0);
    EXPECT_NO_THROW(file_handle->repair_file_header_for_pages({RM_FIRST_RECORD_PAGE}));
    ASSERT_EQ(ftruncate(fd, static_cast<off_t>(PAGE_SIZE * 2)), 0);
    EXPECT_NO_THROW(file_handle->repair_file_header_for_pages({RM_FIRST_RECORD_PAGE}));
}

TEST_F(RecordManagerTest, RecoveryFinalizeRemovesAnInteriorFullPageFromDurableFreeChain) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
    ScopedRecordFile file(disk_manager_.get(), buffer_pool_manager.get(), 64);
    RmFileHandle* const file_handle = file.handle();

    RmPageHandle first = file_handle->create_new_page_handle();
    const page_id_t first_page = first.page->get_page_id().page_no;
    buffer_pool_manager->unpin_page(first.page->get_page_id(), true);
    RmPageHandle second = file_handle->create_new_page_handle();
    const page_id_t second_page = second.page->get_page_id().page_no;
    buffer_pool_manager->unpin_page(second.page->get_page_id(), true);

    // Establish a durable two-page chain, discard the process-local cache as
    // a reopen would, then make the tail full without repairing its untouched
    // predecessor. This is the exact B -> A(full) stale-link shape recovery
    // finalization must eliminate while retaining B.
    file_handle->finalize_recovery_pages({first_page, second_page});
    file_handle->free_page_candidates_.clear();
    file_handle->free_page_candidate_set_.clear();
    {
        RmPageHandle page = file_handle->fetch_page_handle(second_page);
        {
            std::unique_lock<std::shared_mutex> lock(page.page->latch());
            for (int slot = 0; slot < page.file_hdr->num_records_per_page; ++slot) {
                Bitmap::set(page.bitmap, slot);
                page.get_meta(slot).is_deleted_ = false;
            }
            page.page_hdr->num_records = page.file_hdr->num_records_per_page;
            BufferPoolManager::mark_dirty_locked(page.page);
        }
        buffer_pool_manager->unpin_page(page.page->get_page_id(), false);
    }

    file_handle->finalize_recovery_pages({first_page, second_page});
    EXPECT_EQ(file_handle->file_hdr_.first_free_page_no, first_page);
    RmPageHandle remaining = file_handle->fetch_page_handle(first_page);
    EXPECT_EQ(remaining.page_hdr->next_free_page_no, RM_NO_PAGE);
    buffer_pool_manager->unpin_page(remaining.page->get_page_id(), false);
}

TEST_F(RecordManagerTest, CleanReopenLoadsTheWholeDurableFreeChainBeforeFillingItsHead) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(64, disk_manager_.get());
    RmManager rm_manager(disk_manager_.get(), buffer_pool_manager.get());
    const std::string filename = UniqueTestFileName("clean_reopen_free_chain");
    rm_manager.create_file(filename, 1000);
    auto file_handle = rm_manager.open_file(filename);
    for (int page = 0; page < 3; ++page) {
        RmPageHandle created = file_handle->create_new_page_handle();
        ASSERT_TRUE(buffer_pool_manager->unpin_page(created.page->get_page_id(), true));
    }
    file_handle->finalize_recovery_pages({1, 2, 3});
    PersistRecordFileAndClose(disk_manager_.get(), buffer_pool_manager.get(), &file_handle);

    file_handle = rm_manager.open_file(filename);
    const RmFileHdr before = file_handle->get_file_hdr();
    ASSERT_EQ(before.num_pages, 4);
    const page_id_t old_head = before.first_free_page_no;
    ASSERT_NE(old_head, RM_NO_PAGE);
    RmPageHandle head_page = file_handle->fetch_page_handle(old_head);
    const page_id_t old_tail = head_page.page_hdr->next_free_page_no;
    ASSERT_TRUE(buffer_pool_manager->unpin_page(head_page.page->get_page_id(), false));
    ASSERT_NE(old_tail, RM_NO_PAGE);

    std::vector<char> record(1000, 0);
    for (int slot = 0; slot < before.num_records_per_page; ++slot) {
        file_handle->insert_record(Rid{old_head, slot}, record.data());
    }
    const Rid reused = file_handle->insert_record(record.data(), nullptr);
    EXPECT_NE(reused.page_no, old_head);
    EXPECT_LT(reused.page_no, before.num_pages);
    EXPECT_EQ(file_handle->get_file_hdr().num_pages, before.num_pages);

    PersistRecordFileAndClose(disk_manager_.get(), buffer_pool_manager.get(), &file_handle);
    rm_manager.destroy_file(filename);
}

TEST_F(RecordManagerTest, RecoveryBitmapScanRepairsPartiallyPersistedFreeLinksAndSubsetHead) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(64, disk_manager_.get());
    RmManager rm_manager(disk_manager_.get(), buffer_pool_manager.get());
    const std::string filename = UniqueTestFileName("partial_free_chain");
    rm_manager.create_file(filename, 1000);
    auto file_handle = rm_manager.open_file(filename);
    for (int page = 0; page < 3; ++page) {
        RmPageHandle created = file_handle->create_new_page_handle();
        ASSERT_TRUE(buffer_pool_manager->unpin_page(created.page->get_page_id(), true));
    }
    file_handle->finalize_recovery_pages({1, 2, 3});
    PersistRecordFileAndClose(disk_manager_.get(), buffer_pool_manager.get(), &file_handle);

    const auto persist_corruption = [&](page_id_t head, page_id_t next) {
        auto handle = rm_manager.open_file(filename);
        RmPageHandle page = handle->fetch_page_handle(head);
        {
            std::unique_lock<std::shared_mutex> page_lock(page.page->latch());
            page.page_hdr->next_free_page_no = next;
            BufferPoolManager::mark_dirty_locked(page.page);
        }
        ASSERT_TRUE(buffer_pool_manager->unpin_page(page.page->get_page_id(), false));
        handle->file_hdr_.first_free_page_no = head;
        PersistRecordFileAndClose(disk_manager_.get(), buffer_pool_manager.get(), &handle);
    };
    const auto verify_complete_chain = [&] {
        auto handle = rm_manager.open_file(filename);
        const RmFileHdr header = handle->get_file_hdr();
        std::set<page_id_t> chain;
        page_id_t page_no = header.first_free_page_no;
        while (page_no != RM_NO_PAGE) {
            ASSERT_GE(page_no, RM_FIRST_RECORD_PAGE);
            ASSERT_LT(page_no, header.num_pages);
            ASSERT_TRUE(chain.insert(page_no).second) << "free-list contains a cycle";
            RmPageHandle page = handle->fetch_page_handle(page_no);
            page_no = page.page_hdr->next_free_page_no;
            ASSERT_TRUE(buffer_pool_manager->unpin_page(page.page->get_page_id(), false));
        }
        EXPECT_EQ(chain, (std::set<page_id_t>{1, 2, 3}));
        PersistRecordFileAndClose(disk_manager_.get(), buffer_pool_manager.get(), &handle);
    };

    // Old header plus one newly persisted, self-referential link.
    persist_corruption(1, 1);
    file_handle = rm_manager.open_file(filename);
    file_handle->prepare_recovery_free_space();
    file_handle->finalize_recovery_pages({1});
    PersistRecordFileAndClose(disk_manager_.get(), buffer_pool_manager.get(), &file_handle);
    verify_complete_chain();

    // A structurally valid but incomplete new head/link first loads as a
    // durable hint; recovery must still upgrade it to a complete bitmap view.
    persist_corruption(2, RM_NO_PAGE);
    file_handle = rm_manager.open_file(filename);
    file_handle->ensure_free_space_candidates();
    ASSERT_EQ(file_handle->free_space_init_state_, RmFileHandle::FreeSpaceInitState::DurableHintLoaded);
    file_handle->prepare_recovery_free_space();
    ASSERT_EQ(file_handle->free_space_init_state_, RmFileHandle::FreeSpaceInitState::BitmapAuthoritative);
    file_handle->finalize_recovery_pages({2});
    PersistRecordFileAndClose(disk_manager_.get(), buffer_pool_manager.get(), &file_handle);
    verify_complete_chain();

    rm_manager.destroy_file(filename);
}

TEST_F(RecordManagerTest, EmptyRecoveryFinalizeSummaryCanonicalizesSubsetFreeChain) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(64, disk_manager_.get());
    RmManager rm_manager(disk_manager_.get(), buffer_pool_manager.get());
    const std::string filename = UniqueTestFileName("empty_finalize_subset_chain");
    rm_manager.create_file(filename, 1000);
    auto file_handle = rm_manager.open_file(filename);
    for (int page = 0; page < 3; ++page) {
        RmPageHandle created = file_handle->create_new_page_handle();
        ASSERT_TRUE(buffer_pool_manager->unpin_page(created.page->get_page_id(), true));
    }
    file_handle->finalize_recovery_pages({1, 2, 3});
    PersistRecordFileAndClose(disk_manager_.get(), buffer_pool_manager.get(), &file_handle);
    file_handle = rm_manager.open_file(filename);
    RmPageHandle subset_head = file_handle->fetch_page_handle(2);
    { std::unique_lock<std::shared_mutex> lock(subset_head.page->latch()); subset_head.page_hdr->next_free_page_no = RM_NO_PAGE; BufferPoolManager::mark_dirty_locked(subset_head.page); }
    ASSERT_TRUE(buffer_pool_manager->unpin_page(subset_head.page->get_page_id(), false));
    file_handle->file_hdr_.first_free_page_no = 2;
    PersistRecordFileAndClose(disk_manager_.get(), buffer_pool_manager.get(), &file_handle);
    for (int reopen = 0; reopen < 2; ++reopen) {
        file_handle = rm_manager.open_file(filename);
        file_handle->prepare_recovery_free_space();
        file_handle->publish_recovery_page_finalization({});
        PersistRecordFileAndClose(disk_manager_.get(), buffer_pool_manager.get(), &file_handle);
    }
    file_handle = rm_manager.open_file(filename);
    std::set<page_id_t> chain;
    for (page_id_t page_no = file_handle->get_file_hdr().first_free_page_no; page_no != RM_NO_PAGE;) {
        ASSERT_TRUE(chain.insert(page_no).second);
        RmPageHandle page = file_handle->fetch_page_handle(page_no);
        page_no = page.page_hdr->next_free_page_no;
        ASSERT_TRUE(buffer_pool_manager->unpin_page(page.page->get_page_id(), false));
    }
    EXPECT_EQ(chain, (std::set<page_id_t>{1, 2, 3}));
    std::vector<char> record(1000, 0);
    const Rid reused = file_handle->insert_record(record.data(), nullptr);
    EXPECT_EQ(reused.page_no, 1);
    PersistRecordFileAndClose(disk_manager_.get(), buffer_pool_manager.get(), &file_handle);
    rm_manager.destroy_file(filename);
}

TEST_F(RecordManagerTest, RecoverySizeChecksRejectShortAndSubPageTails) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
    ScopedRecordFile file(disk_manager_.get(), buffer_pool_manager.get(), 64);
    RmFileHandle* const file_handle = file.handle();
    const int fd = file_handle->GetFd();

    ASSERT_EQ(ftruncate(fd, static_cast<off_t>(sizeof(RmFileHdr) - 1)), 0);
    try {
        file_handle->repair_file_header_for_pages({RM_FIRST_RECORD_PAGE});
        FAIL() << "short record-file header was accepted";
    } catch (const RMDBError& error) {
        EXPECT_NE(std::string(error.what()).find("header is shorter"), std::string::npos);
    }

    for (const int64_t size : {static_cast<int64_t>(sizeof(RmFileHdr)) + 1, static_cast<int64_t>(PAGE_SIZE) - 1,
                               static_cast<int64_t>(PAGE_SIZE) + 1}) {
        ASSERT_EQ(ftruncate(fd, static_cast<off_t>(size)), 0);
        try {
            file_handle->repair_file_header_for_pages({RM_FIRST_RECORD_PAGE});
            FAIL() << "unaligned record-file size " << size << " was accepted";
        } catch (const RMDBError& error) {
            EXPECT_NE(std::string(error.what()).find("incomplete page"), std::string::npos);
            EXPECT_EQ(std::string(error.what()).find("header is shorter"), std::string::npos);
        }
    }
}

TEST_F(RecordManagerTest, OpenFdSizeSurvivesPathRemovalAndReportsClosedFd) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
    ScopedRecordFile file(disk_manager_.get(), buffer_pool_manager.get(), 64);
    RmFileHandle* const file_handle = file.handle();
    ASSERT_EQ(unlink(file.filename().c_str()), 0);
    EXPECT_NO_THROW(file_handle->repair_file_header_for_pages({RM_FIRST_RECORD_PAGE}));

    ScopedDiskFile closed_file(disk_manager_.get());
    const int closed_fd = closed_file.fd();
    closed_file.close();

    try {
        (void)disk_manager_->get_file_size(closed_fd);
        FAIL() << "closed descriptor returned a size";
    } catch (const RMDBError& error) {
        EXPECT_NE(std::string(error.what()).find("get_file_size(fstat) failed"), std::string::npos);
        EXPECT_EQ(std::string(error.what()).find("incomplete page"), std::string::npos);
    }
}

TEST_F(RecordManagerTest, SimpleTest) {
    srand((unsigned)time(nullptr));

    // 创建RmManager类的对象rm_manager
    auto disk_manager = std::make_unique<DiskManager>();
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
    auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());

    std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> mock;

    std::string filename = "abc.txt";

    int record_size = 4 + rand() % 256; // 元组大小随便设置，只要不超过RM_MAX_RECORD_SIZE
    // test files
    {
        // 删除残留的同名文件
        if (disk_manager->is_file(filename)) {
            disk_manager->destroy_file(filename);
        }
        // 将file header写入到磁盘中的filename文件
        rm_manager->create_file(filename, record_size);
        // 将磁盘中的filename文件读出到内存中的file handle的file header
        std::unique_ptr<RmFileHandle> file_handle = rm_manager->open_file(filename);
        // 检查filename文件在内存中的file header的参数
        assert(file_handle->file_hdr_.record_size == record_size);
        assert(file_handle->file_hdr_.first_free_page_no == RM_NO_PAGE);
        assert(file_handle->file_hdr_.num_pages == 1);

        int max_bytes = file_handle->file_hdr_.record_size * file_handle->file_hdr_.num_records_per_page +
                        file_handle->file_hdr_.bitmap_size +
                        file_handle->file_hdr_.num_records_per_page * TUPLE_META_SIZE + RM_PAGE_META_OFFSET;
        assert(max_bytes <= PAGE_SIZE);
        int rand_val = rand();
        file_handle->file_hdr_.num_pages = rand_val;
        rm_manager->close_file(file_handle.get());

        // reopen file
        file_handle = rm_manager->open_file(filename);
        assert(file_handle->file_hdr_.num_pages == rand_val);
        rm_manager->close_file(file_handle.get());
        rm_manager->destroy_file(filename);
    }
    // test pages
    rm_manager->create_file(filename, record_size);
    auto file_handle = rm_manager->open_file(filename);

    char write_buf[PAGE_SIZE];
    size_t add_cnt = 0;
    size_t upd_cnt = 0;
    size_t del_cnt = 0;
    for (int round = 0; round < 1000; round++) {
        double insert_prob = 1. - mock.size() / 250.;
        double dice = rand() * 1. / RAND_MAX;
        if (mock.empty() || dice < insert_prob) {
            test::rand_buf(file_handle->file_hdr_.record_size, write_buf);
            Rid rid = file_handle->insert_record(write_buf, nullptr);
            mock[rid] = std::string((char*)write_buf, file_handle->file_hdr_.record_size);
            add_cnt++;
        } else {
            // update or erase random rid
            int rid_idx = rand() % mock.size();
            auto it = mock.begin();
            for (int i = 0; i < rid_idx; i++) {
                it++;
            }
            auto rid = it->first;
            if (rand() % 2 == 0) {
                // update
                test::rand_buf(file_handle->file_hdr_.record_size, write_buf);
                file_handle->update_record(rid, write_buf, nullptr);
                mock[rid] = std::string((char*)write_buf, file_handle->file_hdr_.record_size);
                upd_cnt++;
            } else {
                // erase
                file_handle->delete_record(rid, nullptr);
                mock.erase(rid);
                del_cnt++;
            }
        }
        // Randomly re-open file
        if (round % 50 == 0) {
            rm_manager->close_file(file_handle.get());
            file_handle = rm_manager->open_file(filename);
        }
        check_equal(file_handle.get(), mock);
    }
    assert(mock.size() == add_cnt - del_cnt);
    std::cout << "insert " << add_cnt << '\n' << "delete " << del_cnt << '\n' << "update " << upd_cnt << '\n';
    // clean up
    rm_manager->close_file(file_handle.get());
    rm_manager->destroy_file(filename);
}

TEST_F(RecordManagerTest, CloseFailsClosedWhenWalDependencyCannotFlush) {
    auto bpm = std::make_unique<BufferPoolManager>(8, disk_manager_.get());
    RmManager rm_manager(disk_manager_.get(), bpm.get());
    const std::string filename = "close_fail_closed.txt";
    if (disk_manager_->is_file(filename)) {
        disk_manager_->destroy_file(filename);
    }
    rm_manager.create_file(filename, sizeof(int));
    auto file = rm_manager.open_file(filename);
    int value = 7;
    const Rid rid = file->insert_record(reinterpret_cast<char*>(&value), nullptr);
    Page* page = bpm->fetch_page(PageId{file->GetFd(), rid.page_no});
    ASSERT_NE(page, nullptr);
    BufferPoolManager::mark_dirty(page, PageWriteDependency::Wal(7));
    ASSERT_TRUE(bpm->unpin_page(PageId{file->GetFd(), rid.page_no}, false));

    EXPECT_THROW(rm_manager.close_file(file.get()), InternalError);
    EXPECT_TRUE(bpm->is_page_resident(PageId{file->GetFd(), rid.page_no}));

    // Cleanup without pretending the failed close succeeded.
    disk_manager_->close_file(file->GetFd());
    disk_manager_->destroy_file(filename);
}

TEST_F(RecordManagerTest, NewRecordPageFlushFailureThrowsWithoutDereferencingNull) {
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager_.get());
    RmManager rm_manager(disk_manager_.get(), bpm.get());
    const std::string filename = UniqueTestFileName("new_page_flush_failure");
    rm_manager.create_file(filename, sizeof(int));
    auto file = rm_manager.open_file(filename);
    ScopedDiskFile blocker(disk_manager_.get());

    PageId blocker_id{blocker.fd(), INVALID_PAGE_ID};
    Page* blocker_page = bpm->new_page(&blocker_id);
    ASSERT_NE(blocker_page, nullptr);
    {
        std::unique_lock page_lock{blocker_page->latch()};
        blocker_page->set_page_lsn(0);
        BufferPoolManager::mark_dirty_locked(blocker_page);
    }
    ASSERT_TRUE(bpm->unpin_page(blocker_id, false));

    int value = 7;
    EXPECT_THROW(file->insert_record(reinterpret_cast<char*>(&value), nullptr), InternalError);
    EXPECT_TRUE(bpm->is_page_resident(blocker_id));
    Page* retained = bpm->fetch_page(blocker_id);
    ASSERT_NE(retained, nullptr);
    EXPECT_TRUE(retained->is_dirty());
    EXPECT_EQ(retained->get_page_lsn(), 0);
    EXPECT_TRUE(bpm->unpin_page(blocker_id, false));

    rm_manager.close_file(file.get());
    rm_manager.destroy_file(filename);
    bpm->delete_all_pages(blocker.fd());
}

TEST_F(RecordManagerTest, ApplyTupleUpdateInstallsTupleAndPageLsnTogether) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(8, disk_manager_.get());
    auto rm_manager = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager.get());
    const std::string filename = "apply_tuple_update_test.txt";
    if (disk_manager_->is_file(filename)) {
        disk_manager_->destroy_file(filename);
    }

    constexpr int record_size = 64;
    rm_manager->create_file(filename, record_size);
    auto file_handle = rm_manager->open_file(filename);

    char initial[record_size] = {};
    Rid rid = file_handle->insert_record(initial, nullptr);
    char updated[record_size] = {};
    std::memcpy(updated, "updated", 8);
    TupleMeta meta;
    meta.writer_txn_id_ = 7;
    meta.is_committed_ = false;
    meta.is_deleted_ = false;

    file_handle->apply_tuple_update(rid, updated, meta, 42);

    auto record = file_handle->get_record(rid, nullptr);
    ASSERT_NE(record, nullptr);
    EXPECT_EQ(std::memcmp(record->data, updated, record_size), 0);
    EXPECT_EQ(file_handle->get_tuple_meta(rid).writer_txn_id_, 7);
    EXPECT_FALSE(file_handle->get_tuple_meta(rid).is_committed_);
    EXPECT_EQ(file_handle->get_page_lsn(rid), 42);

    // This unit test deliberately installs a synthetic WAL LSN without a
    // LogManager. Discard its in-memory page after the atomicity assertions;
    // close_file must not pretend that the synthetic dependency is durable.
    const int fd = file_handle->GetFd();
    buffer_pool_manager->delete_all_pages(fd);
    disk_manager_->close_file(fd);
    file_handle.reset();
    rm_manager->destroy_file(filename);
}

TEST_F(RecordManagerTest, PreparedInsertInstallsTupleMetaAndPageLsnTogether) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(8, disk_manager_.get());
    auto rm_manager = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager.get());
    const std::string filename = "prepared_insert_test.txt";
    if (disk_manager_->is_file(filename)) {
        disk_manager_->destroy_file(filename);
    }

    constexpr int record_size = 64;
    rm_manager->create_file(filename, record_size);
    auto file_handle = rm_manager->open_file(filename);
    auto prepared = file_handle->prepare_insert_record();
    char value[record_size] = {};
    std::memcpy(value, "inserted", 9);
    TupleMeta meta;
    meta.writer_txn_id_ = 11;
    meta.is_committed_ = false;
    meta.is_deleted_ = false;

    file_handle->finish_insert_record(prepared, value, &meta, 77);

    auto record = file_handle->get_record(prepared.rid, nullptr);
    ASSERT_NE(record, nullptr);
    EXPECT_EQ(std::memcmp(record->data, value, record_size), 0);
    EXPECT_EQ(file_handle->get_tuple_meta(prepared.rid).writer_txn_id_, 11);
    EXPECT_FALSE(file_handle->get_tuple_meta(prepared.rid).is_committed_);
    EXPECT_EQ(file_handle->get_page_lsn(prepared.rid), 77);

    const int fd = file_handle->GetFd();
    buffer_pool_manager->delete_all_pages(fd);
    disk_manager_->close_file(fd);
    file_handle.reset();
    rm_manager->destroy_file(filename);
}

TEST_F(RecordManagerTest, ConcurrentInsertsUseMultipleHeapPages) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(64, disk_manager_.get());
    auto rm_manager = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager.get());
    const std::string filename = "concurrent_insert_test.txt";
    if (disk_manager_->is_file(filename)) {
        disk_manager_->destroy_file(filename);
    }

    constexpr int record_size = 128;
    constexpr int worker_count = 8;
    constexpr int inserts_per_worker = 100;
    rm_manager->create_file(filename, record_size);
    auto file_handle = rm_manager->open_file(filename);

    std::vector<std::thread> workers;
    workers.reserve(worker_count);
    for (int worker = 0; worker < worker_count; ++worker) {
        workers.emplace_back([&, worker] {
            char record[record_size] = {};
            std::memcpy(record, &worker, sizeof(worker));
            for (int i = 0; i < inserts_per_worker; ++i) {
                file_handle->insert_record(record, nullptr);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    std::set<page_id_t> pages;
    int record_count = 0;
    for (RmScan scan(file_handle.get()); !scan.is_end(); scan.next()) {
        pages.insert(scan.rid().page_no);
        ++record_count;
    }
    EXPECT_EQ(record_count, worker_count * inserts_per_worker);
    EXPECT_GT(pages.size(), 1u);

    rm_manager->close_file(file_handle.get());
    rm_manager->destroy_file(filename);
}

// insert_record(rid, ...) 是恢复 redo 和**事务回滚**（撤销 delete 时重插旧镜像）
// 共用的路径。它曾无条件把「本页的 next_free_page_no」发布成空闲链表头，而这只在
// 本页恰好是链表头时才正确。
//
// 诚实说明这个用例的强度：它在**修复前也能通过** —— 因为 free_page_candidates_
// 才是运行期的真值来源，那个被填满的页仍留在候选向量里，prepare_insert_record()
// 下次取到它、发现已满、剔除后重试，于是自愈了。所以这里守的是「可观察行为」
// （空闲页仍被复用），而不是一个曾经暴露过的失败。真正会崩的那条路径由下面的
// BulkLoadSurvivesAStaleFreePageHead 覆盖。
TEST_F(RecordManagerTest, RefillingANonHeadPageKeepsFreeListConsistent) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(64, disk_manager_.get());
    auto rm_manager = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager.get());
    const std::string filename = "free_list_consistency_test.txt";
    if (disk_manager_->is_file(filename)) {
        disk_manager_->destroy_file(filename);
    }

    // 记录够大，使每页只放少量记录，便于精确造出「两个非满页」的局面。
    constexpr int record_size = 1000;
    rm_manager->create_file(filename, record_size);
    auto file_handle = rm_manager->open_file(filename);
    const int per_page = file_handle->file_hdr_.num_records_per_page;
    ASSERT_GE(per_page, 2) << "record_size 需要让每页至少放两条，否则造不出这个局面";

    std::vector<char> record(record_size, 0);
    std::vector<Rid> rids;
    for (int i = 0; i < per_page * 2; ++i) {
        std::memcpy(record.data(), &i, sizeof(i));
        rids.push_back(file_handle->insert_record(record.data(), nullptr));
    }
    // 至少两个页，且此时都已装满。
    std::set<page_id_t> pages;
    for (const auto& rid : rids) {
        pages.insert(rid.page_no);
    }
    ASSERT_GE(pages.size(), 2u);

    // 各删一条，让两个页都变成非满 —— 两者都成为空闲候选，链表头是其中一个。
    const Rid first_page_slot = rids.front();
    const Rid second_page_slot = rids.back();
    ASSERT_NE(first_page_slot.page_no, second_page_slot.page_no);
    file_handle->delete_record(first_page_slot, nullptr);
    file_handle->delete_record(second_page_slot, nullptr);

    // 回滚形状：往**非链表头**的那个页重插，把它重新填满。
    const page_id_t head_before = file_handle->file_hdr_.first_free_page_no;
    const Rid non_head = head_before == first_page_slot.page_no ? second_page_slot : first_page_slot;
    std::memcpy(record.data(), &non_head.slot_no, sizeof(non_head.slot_no));
    file_handle->insert_record(non_head, record.data());

    // 关键不变式：仍有空槽的那个页必须还能被分配到。旧代码把「本页的
    // next_free_page_no」当成新的链表头发布，会把它连带丢掉，于是这条插入
    // 会去扩一个新页而不是复用已有空槽。
    const Rid still_free = non_head.page_no == first_page_slot.page_no ? second_page_slot : first_page_slot;
    const Rid reused = file_handle->insert_record(record.data(), nullptr);
    EXPECT_EQ(reused.page_no, still_free.page_no) << "真正空闲的页被从空闲候选里丢掉了";

    rm_manager->close_file(file_handle.get());
    rm_manager->destroy_file(filename);
}

// PinnedInserter（批量装载）过去直接跟着 file_hdr_.first_free_page_no 走，而那个
// 链接可能指向一个已经满了的页；换页后它又不重新检查 slot_no，于是会在
// get_slot(num_records_per_page) 处越界写出页外。这里显式把链表头指向一个满页来
// 逼出那条路径。
TEST_F(RecordManagerTest, BulkLoadSurvivesAStaleFreePageHead) {
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(64, disk_manager_.get());
    auto rm_manager = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager.get());
    const std::string filename = "bulk_load_stale_head_test.txt";
    if (disk_manager_->is_file(filename)) {
        disk_manager_->destroy_file(filename);
    }

    constexpr int record_size = 1000;
    rm_manager->create_file(filename, record_size);
    auto file_handle = rm_manager->open_file(filename);
    const int per_page = file_handle->file_hdr_.num_records_per_page;
    ASSERT_GE(per_page, 2);

    std::vector<char> record(record_size, 0);
    // 装满第一个记录页。
    for (int i = 0; i < per_page; ++i) {
        std::memcpy(record.data(), &i, sizeof(i));
        file_handle->insert_record(record.data(), nullptr);
    }
    // 把链表头强行指回那个满页（模拟陈旧链接）。
    {
        std::scoped_lock lock(file_handle->free_space_latch_, file_handle->file_header_latch_);
        file_handle->free_page_candidates_.clear();
        file_handle->free_page_candidate_set_.clear();
        file_handle->free_page_cursor_ = 0;
        file_handle->file_hdr_.first_free_page_no = RM_FIRST_RECORD_PAGE;
    }

    // 批量装载必须自己发现该页已满并另取一页，而不是越界写。
    const int bulk_rows = per_page * 3;
    {
        RmFileHandle::PinnedInserter inserter(file_handle.get());
        for (int i = 0; i < bulk_rows; ++i) {
            const int value = 1000 + i;
            std::memcpy(record.data(), &value, sizeof(value));
            const Rid rid = inserter.insert(record.data());
            ASSERT_GE(rid.slot_no, 0);
            ASSERT_LT(rid.slot_no, per_page) << "slot_no 越界，写到了页外";
        }
    }

    int total = 0;
    for (RmScan scan(file_handle.get()); !scan.is_end(); scan.next()) {
        ++total;
    }
    EXPECT_EQ(total, per_page + bulk_rows);

    rm_manager->close_file(file_handle.get());
    rm_manager->destroy_file(filename);
}

// 验证 is_record 调用后正确 unpin buffer pool page
TEST(RecordManagerIsRecordTest, is_record_unpins_page) {
    auto disk_manager = std::make_unique<DiskManager>();
    const size_t bpm_size = 10;
    auto bpm = std::make_unique<BufferPoolManager>(bpm_size, disk_manager.get());
    auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), bpm.get());

    std::string filename = "is_record_test.txt";
    if (disk_manager->is_file(filename))
        disk_manager->destroy_file(filename);

    int record_size = 64;
    rm_manager->create_file(filename, record_size);
    auto fh = rm_manager->open_file(filename);

    // 插入一条记录
    char buf[64] = {};
    Rid rid = fh->insert_record(buf, nullptr);

    // insert_record 的 unpin 应该已经把 pin_count 归零，
    // 现在调用 is_record 后再检查 pin_count
    fh->is_record(rid);

    // 通过 BPM 内部检查 page 的 pin_count 已归零
    PageId page_id = {.fd = fh->GetFd(), .page_no = rid.page_no};
    auto it = bpm->page_table_.find(page_id);
    ASSERT_NE(it, bpm->page_table_.end());
    Page& page = bpm->pages_[it->second];
    EXPECT_EQ(0, page.pin_count_);

    rm_manager->close_file(fh.get());
    rm_manager->destroy_file(filename);
}

TEST(RecordManagerRecordWithMetaTest, absent_slot_returns_no_record) {
    auto disk_manager = std::make_unique<DiskManager>();
    auto bpm = std::make_unique<BufferPoolManager>(10, disk_manager.get());
    auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), bpm.get());
    const std::string filename = "record_with_meta_absent_test.txt";
    if (disk_manager->is_file(filename)) {
        disk_manager->destroy_file(filename);
    }
    rm_manager->create_file(filename, 64);
    auto fh = rm_manager->open_file(filename);

    char buf[64] = {};
    const Rid present = fh->insert_record(buf, nullptr);
    ASSERT_EQ(present.slot_no, 0);
    const auto absent = fh->get_record_with_meta(Rid{present.page_no, present.slot_no + 1}, nullptr);
    EXPECT_EQ(absent.record, nullptr);

    rm_manager->close_file(fh.get());
    rm_manager->destroy_file(filename);
}

TEST(RecordManagerTupleMetaProbeTest, reports_state_and_never_leaks_a_pin) {
    auto disk_manager = std::make_unique<DiskManager>();
    auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager.get());
    auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), bpm.get());
    const std::string filename = "tuple_meta_probe_test.txt";
    if (disk_manager->is_file(filename)) {
        disk_manager->destroy_file(filename);
    }
    constexpr int record_size = 64;
    rm_manager->create_file(filename, record_size);
    auto fh = rm_manager->open_file(filename);

    std::vector<char> value(record_size, 0);
    const Rid first = fh->insert_record(value.data(), nullptr);
    const int slots_per_page = fh->get_file_hdr().num_records_per_page;
    for (int i = 1; i < slots_per_page; ++i) {
        fh->insert_record(value.data(), nullptr);
    }
    const Rid second = fh->insert_record(value.data(), nullptr);
    ASSERT_NE(first.page_no, second.page_no);
    TupleMeta meta;
    meta.writer_txn_id_ = 77;
    fh->set_tuple_meta(first, meta);

    const auto present = fh->probe_tuple_meta(first);
    ASSERT_EQ(present.state, RmTupleMetaProbeState::Present);
    EXPECT_EQ(present.meta.writer_txn_id_, 77);
    const auto batch = fh->probe_tuple_meta_batch(first.page_no, {first.slot_no, first.slot_no + 1, -1});
    ASSERT_EQ(batch.size(), 3u);
    EXPECT_EQ(batch[0].state, RmTupleMetaProbeState::Present);
    EXPECT_EQ(batch[0].meta.writer_txn_id_, 77);
    EXPECT_EQ(batch[1].state, RmTupleMetaProbeState::Present);
    EXPECT_EQ(batch[2].state, RmTupleMetaProbeState::Absent);
    std::vector<RmTupleMetaProbeState> visited_states;
    visited_states.reserve(3);
    fh->visit_tuple_meta_batch(
        first.page_no, {first.slot_no, first.slot_no + 1, -1},
        [&](size_t, RmTupleMetaProbeState state, const TupleMeta*) { visited_states.push_back(state); });
    ASSERT_EQ(visited_states.size(), 3u);
    EXPECT_EQ(visited_states[0], RmTupleMetaProbeState::Present);
    EXPECT_EQ(visited_states[1], RmTupleMetaProbeState::Present);
    EXPECT_EQ(visited_states[2], RmTupleMetaProbeState::Absent);
    int callback_count = 0;
    EXPECT_THROW(fh->visit_tuple_meta_batch(first.page_no, {first.slot_no, first.slot_no + 1},
                                            [&](size_t, RmTupleMetaProbeState, const TupleMeta*) {
                                                ++callback_count;
                                                throw std::runtime_error("visitor failure");
                                            }),
                 std::runtime_error);
    EXPECT_EQ(callback_count, 1);
    EXPECT_EQ(fh->probe_tuple_meta(first).state, RmTupleMetaProbeState::Present);
    const auto bitmap_unset = fh->probe_tuple_meta_batch(second.page_no, {second.slot_no, second.slot_no + 1});
    ASSERT_EQ(bitmap_unset.size(), 2u);
    EXPECT_EQ(bitmap_unset[0].state, RmTupleMetaProbeState::Present);
    EXPECT_EQ(bitmap_unset[1].state, RmTupleMetaProbeState::Absent);
    EXPECT_EQ(fh->probe_tuple_meta(Rid{0, 0}).state, RmTupleMetaProbeState::Absent);
    EXPECT_EQ(fh->probe_tuple_meta(Rid{first.page_no, -1}).state, RmTupleMetaProbeState::Absent);
    EXPECT_EQ(fh->probe_tuple_meta(Rid{first.page_no, fh->get_file_hdr().num_records_per_page}).state,
              RmTupleMetaProbeState::Absent);

    // With one frame occupied by page two, a valid probe of page one fails its
    // fetch cleanly. The held page remains pinned exactly once, then returns to
    // zero after its caller releases it.
    Page* held = bpm->fetch_page(PageId{fh->GetFd(), second.page_no});
    ASSERT_NE(held, nullptr);
    EXPECT_EQ(fh->probe_tuple_meta(first).state, RmTupleMetaProbeState::Retry);
    const auto retry_batch = fh->probe_tuple_meta_batch(first.page_no, {first.slot_no, first.slot_no + 1});
    ASSERT_EQ(retry_batch.size(), 2u);
    EXPECT_EQ(retry_batch[0].state, RmTupleMetaProbeState::Retry);
    EXPECT_EQ(retry_batch[1].state, RmTupleMetaProbeState::Retry);
    EXPECT_TRUE(bpm->unpin_page(PageId{fh->GetFd(), second.page_no}, false));
    EXPECT_EQ(fh->probe_tuple_meta(first).state, RmTupleMetaProbeState::Present);
    EXPECT_EQ(fh->probe_tuple_meta(Rid{second.page_no, second.slot_no + 1}).state, RmTupleMetaProbeState::Absent);
    for (const Rid rid : {first, second}) {
        const PageId page_id{fh->GetFd(), rid.page_no};
        const auto page_it = bpm->page_table_.find(page_id);
        if (page_it != bpm->page_table_.end()) {
            EXPECT_EQ(bpm->pages_[page_it->second].pin_count_, 0);
        }
    }

    rm_manager->close_file(fh.get());
    rm_manager->destroy_file(filename);
}
