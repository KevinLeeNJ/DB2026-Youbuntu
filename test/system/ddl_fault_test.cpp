/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// DDL 故障注入测试：验证 DDL 操作失败后元数据与句柄状态保持一致。
// 覆盖重构指南 DDL 故障补偿规则表中"元数据不变"的场景。
// 真正的文件系统故障注入需要 mock，超出 Phase 2 范围。

#undef NDEBUG

#define private public
#include "system/sm_manager.h"
using namespace rmdb;
#undef private

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "record/rm_manager.h"
#include "index/ix_manager.h"
#include "pager/pager.h"
#include "common/config.h"

namespace {

const std::string TEST_DB_NAME = "ddl_fault_test_db";

class DdlFaultTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<rmdb::pager::Pager> pager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    bool db_opened_ = false;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        pager_ = std::make_unique<rmdb::pager::Pager>(buffer_pool_manager_.get(), nullptr);
        buffer_pool_manager_->set_wal_guard(pager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get(), pager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get(), pager_.get());
        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_manager_.get(),
                                                  ix_manager_.get(), pager_.get());
        if (sm_manager_->is_dir(TEST_DB_NAME)) {
            sm_manager_->drop_db(TEST_DB_NAME);
        }
    }

    void TearDown() override {
        if (db_opened_) {
            sm_manager_->close_db();
            db_opened_ = false;
        }
        if (sm_manager_->is_dir(TEST_DB_NAME)) {
            sm_manager_->drop_db(TEST_DB_NAME);
        }
    }

    void setup_db() {
        sm_manager_->create_db(TEST_DB_NAME);
        sm_manager_->open_db(TEST_DB_NAME);
        db_opened_ = true;
    }

    std::vector<ColDef> make_int_cols(const std::vector<std::string>& names) {
        std::vector<ColDef> cols;
        for (auto& name : names) {
            cols.push_back({name, TYPE_INT, 4});
        }
        return cols;
    }
};

// 重复建表后，元数据与句柄状态不变：原表仍可访问，无重复条目。
TEST_F(DdlFaultTest, create_table_duplicate_leaves_existing_table_intact) {
    setup_db();
    auto cols = make_int_cols({"id", "v"});
    sm_manager_->create_table("t", cols, nullptr);
    ASSERT_TRUE(sm_manager_->db_.is_table("t"));
    ASSERT_EQ(sm_manager_->fhs_.count("t"), 1u);

    EXPECT_THROW(sm_manager_->create_table("t", cols, nullptr), TableExistsError);

    // 失败后状态不变
    EXPECT_TRUE(sm_manager_->db_.is_table("t"));
    EXPECT_EQ(sm_manager_->fhs_.count("t"), 1u);
    EXPECT_EQ(sm_manager_->db_.get_table("t").cols.size(), 2u);
}

// drop 不存在的表抛异常后，元数据不变。
TEST_F(DdlFaultTest, drop_table_not_found_leaves_metadata_unchanged) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("keep", cols, nullptr);
    size_t table_count_before = sm_manager_->db_.tabs_.size();

    EXPECT_THROW(sm_manager_->drop_table("no_such_table", nullptr), TableNotFoundError);

    EXPECT_EQ(sm_manager_->db_.tabs_.size(), table_count_before);
    EXPECT_TRUE(sm_manager_->db_.is_table("keep"));
}

// 重复建索引后，索引列表不变。
TEST_F(DdlFaultTest, create_index_duplicate_leaves_index_list_intact) {
    setup_db();
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("t", cols, nullptr);
    sm_manager_->create_index("t", {"a"}, nullptr);
    size_t index_count_before = sm_manager_->db_.get_table("t").indexes.size();

    EXPECT_THROW(sm_manager_->create_index("t", {"a"}, nullptr), IndexExistsError);

    EXPECT_EQ(sm_manager_->db_.get_table("t").indexes.size(), index_count_before);
}

// drop 不存在的索引抛异常后，索引列表不变。
TEST_F(DdlFaultTest, drop_index_not_found_leaves_index_list_intact) {
    setup_db();
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("t", cols, nullptr);
    sm_manager_->create_index("t", {"a"}, nullptr);
    size_t index_count_before = sm_manager_->db_.get_table("t").indexes.size();

    EXPECT_THROW(sm_manager_->drop_index("t", std::vector<std::string>{"b"}, nullptr), IndexNotFoundError);

    EXPECT_EQ(sm_manager_->db_.get_table("t").indexes.size(), index_count_before);
}

// create_table 成功后，表文件句柄与元数据一致；drop_table 后两者均消失。
TEST_F(DdlFaultTest, create_then_drop_table_keeps_handles_and_meta_in_sync) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("sync_t", cols, nullptr);

    EXPECT_TRUE(sm_manager_->db_.is_table("sync_t"));
    EXPECT_EQ(sm_manager_->fhs_.count("sync_t"), 1u);

    sm_manager_->drop_table("sync_t", nullptr);

    EXPECT_FALSE(sm_manager_->db_.is_table("sync_t"));
    EXPECT_EQ(sm_manager_->fhs_.count("sync_t"), 0u);
}

// drop_table 级联删除索引：索引句柄与元数据同步消失。
TEST_F(DdlFaultTest, drop_table_cascades_index_handles_and_meta) {
    setup_db();
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("cascade_t", cols, nullptr);
    sm_manager_->create_index("cascade_t", {"a"}, nullptr);
    sm_manager_->create_index("cascade_t", {"b"}, nullptr);

    auto ix_name_a = sm_manager_->get_ix_manager()->get_index_name("cascade_t", std::vector<std::string>{"a"});
    auto ix_name_b = sm_manager_->get_ix_manager()->get_index_name("cascade_t", std::vector<std::string>{"b"});
    EXPECT_EQ(sm_manager_->ihs_.count(ix_name_a), 1u);
    EXPECT_EQ(sm_manager_->ihs_.count(ix_name_b), 1u);

    sm_manager_->drop_table("cascade_t", nullptr);

    EXPECT_FALSE(sm_manager_->db_.is_table("cascade_t"));
    EXPECT_EQ(sm_manager_->fhs_.count("cascade_t"), 0u);
    EXPECT_EQ(sm_manager_->ihs_.count(ix_name_a), 0u);
    EXPECT_EQ(sm_manager_->ihs_.count(ix_name_b), 0u);
}

} // namespace
