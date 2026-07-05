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
using namespace rmdb;
#undef private

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
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
