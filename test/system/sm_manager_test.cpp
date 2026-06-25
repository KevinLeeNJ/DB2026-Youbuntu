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
#include "system/sm_manager.h"
#undef private

#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

#include "common/config.h"
#include "errors.h"
#include "gtest/gtest.h"
#include "index/ix.h"
#include "record/rm.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"

const std::string TEST_DB_NAME = "sm_manager_test_db";

class SmManagerTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    bool db_opened_ = false;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_manager_.get(),
                                                  ix_manager_.get());
        // 清理可能残留的测试目录
        if (sm_manager_->is_dir(TEST_DB_NAME)) {
            sm_manager_->drop_db(TEST_DB_NAME);
        }
    }

    void TearDown() override {
        // 确保数据库已关闭，防止chdir状态混乱
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

    void teardown_db() {
        if (db_opened_) {
            sm_manager_->close_db();
            db_opened_ = false;
        }
        if (sm_manager_->is_dir(TEST_DB_NAME)) {
            sm_manager_->drop_db(TEST_DB_NAME);
        }
    }

    std::vector<ColDef> make_int_cols(const std::vector<std::string>& names) {
        std::vector<ColDef> cols;
        for (auto& name : names) {
            cols.push_back({name, TYPE_INT, 4});
        }
        return cols;
    }
};

// =============================================================================
// 数据库生命周期测试
// =============================================================================

TEST_F(SmManagerTest, create_db_success) {
    EXPECT_FALSE(sm_manager_->is_dir(TEST_DB_NAME));
    sm_manager_->create_db(TEST_DB_NAME);
    EXPECT_TRUE(sm_manager_->is_dir(TEST_DB_NAME));
}

TEST_F(SmManagerTest, create_db_duplicate_throws) {
    sm_manager_->create_db(TEST_DB_NAME);
    EXPECT_THROW(sm_manager_->create_db(TEST_DB_NAME), DatabaseExistsError);
}

TEST_F(SmManagerTest, drop_db_success) {
    sm_manager_->create_db(TEST_DB_NAME);
    EXPECT_TRUE(sm_manager_->is_dir(TEST_DB_NAME));
    sm_manager_->drop_db(TEST_DB_NAME);
    EXPECT_FALSE(sm_manager_->is_dir(TEST_DB_NAME));
}

TEST_F(SmManagerTest, drop_db_not_found_throws) {
    EXPECT_FALSE(sm_manager_->is_dir(TEST_DB_NAME));
    EXPECT_THROW(sm_manager_->drop_db(TEST_DB_NAME), DatabaseNotFoundError);
}

TEST_F(SmManagerTest, open_db_success) {
    sm_manager_->create_db(TEST_DB_NAME);
    sm_manager_->open_db(TEST_DB_NAME);
    db_opened_ = true;
    EXPECT_EQ(db_opened_, true);
    // 验证元数据正确加载
    EXPECT_EQ(sm_manager_->db_.name_, TEST_DB_NAME);
    EXPECT_TRUE(sm_manager_->db_.tabs_.empty());
    sm_manager_->close_db();
    db_opened_ = false;
}

TEST_F(SmManagerTest, open_db_not_found_throws) {
    EXPECT_THROW(sm_manager_->open_db(TEST_DB_NAME), DatabaseNotFoundError);
}

TEST_F(SmManagerTest, close_db_success) {
    setup_db();
    sm_manager_->close_db();
    db_opened_ = false;
    // 验证关闭后文件夹仍然存在
    EXPECT_TRUE(sm_manager_->is_dir(TEST_DB_NAME));
}

// =============================================================================
// 元数据持久化测试
// =============================================================================

TEST_F(SmManagerTest, flush_and_reload_meta) {
    setup_db();
    // 创建一张表
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("test_tab", cols, nullptr);
    EXPECT_TRUE(sm_manager_->db_.is_table("test_tab"));

    // 关闭再重新打开，验证元数据持久化
    sm_manager_->close_db();
    db_opened_ = false;
    sm_manager_->open_db(TEST_DB_NAME);
    db_opened_ = true;

    EXPECT_TRUE(sm_manager_->db_.is_table("test_tab"));
    auto& tab = sm_manager_->db_.get_table("test_tab");
    EXPECT_EQ(tab.cols.size(), 2);
    EXPECT_EQ(tab.cols[0].name, "a");
    EXPECT_EQ(tab.cols[1].name, "b");
}

TEST_F(SmManagerTest, flush_and_reload_meta_with_index) {
    setup_db();
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("test_tab", cols, nullptr);
    sm_manager_->create_index("test_tab", {"a"}, nullptr);

    sm_manager_->close_db();
    db_opened_ = false;
    sm_manager_->open_db(TEST_DB_NAME);
    db_opened_ = true;

    auto& tab = sm_manager_->db_.get_table("test_tab");
    EXPECT_EQ(tab.indexes.size(), 1);
    EXPECT_TRUE(tab.is_index({"a"}));
}

// =============================================================================
// 表操作测试
// =============================================================================

TEST_F(SmManagerTest, create_table_success) {
    setup_db();
    auto cols = make_int_cols({"id", "score"});
    sm_manager_->create_table("students", cols, nullptr);

    EXPECT_TRUE(sm_manager_->db_.is_table("students"));
    auto& tab = sm_manager_->db_.get_table("students");
    EXPECT_EQ(tab.cols.size(), 2);
    EXPECT_EQ(tab.cols[0].name, "id");
    EXPECT_EQ(tab.cols[0].type, TYPE_INT);
    EXPECT_EQ(tab.cols[0].len, 4);
    EXPECT_EQ(tab.cols[0].offset, 0);
    EXPECT_EQ(tab.cols[1].name, "score");
    EXPECT_EQ(tab.cols[1].offset, 4);
    // 验证文件句柄已打开
    EXPECT_TRUE(sm_manager_->fhs_.find("students") != sm_manager_->fhs_.end());
}

TEST_F(SmManagerTest, create_table_multiple_types) {
    setup_db();
    std::vector<ColDef> cols = {
        {"id", TYPE_INT, 4},
        {"val", TYPE_FLOAT, 8},
        {"name", TYPE_STRING, 32},
    };
    sm_manager_->create_table("multi_types", cols, nullptr);

    auto& tab = sm_manager_->db_.get_table("multi_types");
    EXPECT_EQ(tab.cols.size(), 3);
    EXPECT_EQ(tab.cols[0].type, TYPE_INT);
    EXPECT_EQ(tab.cols[1].type, TYPE_FLOAT);
    EXPECT_EQ(tab.cols[2].type, TYPE_STRING);
}

TEST_F(SmManagerTest, create_table_duplicate_throws) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("dup_table", cols, nullptr);
    EXPECT_THROW(sm_manager_->create_table("dup_table", cols, nullptr), TableExistsError);
}

TEST_F(SmManagerTest, drop_table_success) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("to_drop", cols, nullptr);
    EXPECT_TRUE(sm_manager_->db_.is_table("to_drop"));

    sm_manager_->drop_table("to_drop", nullptr);
    EXPECT_FALSE(sm_manager_->db_.is_table("to_drop"));
    EXPECT_TRUE(sm_manager_->fhs_.find("to_drop") == sm_manager_->fhs_.end());
}

TEST_F(SmManagerTest, drop_table_not_found_throws) {
    setup_db();
    EXPECT_THROW(sm_manager_->drop_table("no_such_table", nullptr), TableNotFoundError);
}

TEST_F(SmManagerTest, drop_table_cascades_indexes) {
    setup_db();
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("cascade_tab", cols, nullptr);
    sm_manager_->create_index("cascade_tab", {"a"}, nullptr);
    sm_manager_->create_index("cascade_tab", {"b"}, nullptr);

    auto& tab = sm_manager_->db_.get_table("cascade_tab");
    EXPECT_EQ(tab.indexes.size(), 2);

    // 删除表应级联删除所有索引句柄
    sm_manager_->drop_table("cascade_tab", nullptr);
    EXPECT_FALSE(sm_manager_->db_.is_table("cascade_tab"));
    EXPECT_EQ(sm_manager_->ihs_.size(), 0);
}

// =============================================================================
// show_tables / desc_table 测试
// =============================================================================

TEST_F(SmManagerTest, show_tables_empty) {
    setup_db();
    // 删除可能已存在的output.txt
    std::remove("output.txt");
    char buf[BUFFER_LENGTH] = {0};
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    EXPECT_NO_THROW(sm_manager_->show_tables(&ctx));
    EXPECT_GT(offset, 0);
}

TEST_F(SmManagerTest, show_tables_with_data) {
    setup_db();
    std::remove("output.txt");
    auto cols = make_int_cols({"a"});
    sm_manager_->create_table("tab1", cols, nullptr);
    sm_manager_->create_table("tab2", cols, nullptr);

    char buf[BUFFER_LENGTH] = {0};
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    sm_manager_->show_tables(&ctx);
    EXPECT_GT(offset, 0);

    // 验证output.txt包含表名
    std::ifstream outfile("output.txt");
    std::string content((std::istreambuf_iterator<char>(outfile)), std::istreambuf_iterator<char>());
    EXPECT_NE(content.find("tab1"), std::string::npos);
    EXPECT_NE(content.find("tab2"), std::string::npos);
}

TEST_F(SmManagerTest, desc_table_success) {
    setup_db();
    std::vector<ColDef> cols = {
        {"id", TYPE_INT, 4},
        {"name", TYPE_STRING, 16},
    };
    sm_manager_->create_table("desc_tab", cols, nullptr);

    char buf[BUFFER_LENGTH] = {0};
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    EXPECT_NO_THROW(sm_manager_->desc_table("desc_tab", &ctx));
    std::string output(buf);
    EXPECT_NE(output.find("id"), std::string::npos);
    EXPECT_NE(output.find("INT"), std::string::npos);
    EXPECT_NE(output.find("name"), std::string::npos);
    EXPECT_NE(output.find("STRING"), std::string::npos);
}

TEST_F(SmManagerTest, desc_table_not_found_throws) {
    setup_db();
    char buf[BUFFER_LENGTH] = {0};
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    EXPECT_THROW(sm_manager_->desc_table("no_such_table", &ctx), TableNotFoundError);
}

// =============================================================================
// 索引操作测试 (空表)
// =============================================================================

TEST_F(SmManagerTest, create_index_success) {
    setup_db();
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("idx_tab", cols, nullptr);
    sm_manager_->create_index("idx_tab", {"a"}, nullptr);

    auto& tab = sm_manager_->db_.get_table("idx_tab");
    EXPECT_EQ(tab.indexes.size(), 1);
    EXPECT_TRUE(tab.is_index({"a"}));
    EXPECT_FALSE(tab.is_index({"b"}));

    // 验证索引句柄已打开
    std::string ix_name = sm_manager_->ix_manager_->get_index_name("idx_tab", tab.indexes[0].cols);
    EXPECT_TRUE(sm_manager_->ihs_.find(ix_name) != sm_manager_->ihs_.end());
}

TEST_F(SmManagerTest, create_compound_index_success) {
    setup_db();
    auto cols = make_int_cols({"a", "b", "c"});
    sm_manager_->create_table("compound_tab", cols, nullptr);
    sm_manager_->create_index("compound_tab", {"a", "b"}, nullptr);

    auto& tab = sm_manager_->db_.get_table("compound_tab");
    EXPECT_EQ(tab.indexes.size(), 1);
    EXPECT_TRUE(tab.is_index({"a", "b"}));
    EXPECT_EQ(tab.indexes[0].col_num, 2);
    EXPECT_EQ(tab.indexes[0].col_tot_len, 8);
}

TEST_F(SmManagerTest, create_multiple_indexes) {
    setup_db();
    auto cols = make_int_cols({"a", "b", "c"});
    sm_manager_->create_table("multi_idx_tab", cols, nullptr);
    sm_manager_->create_index("multi_idx_tab", {"a"}, nullptr);
    sm_manager_->create_index("multi_idx_tab", {"b"}, nullptr);
    sm_manager_->create_index("multi_idx_tab", {"a", "c"}, nullptr);

    auto& tab = sm_manager_->db_.get_table("multi_idx_tab");
    EXPECT_EQ(tab.indexes.size(), 3);
    EXPECT_EQ(sm_manager_->ihs_.size(), 3);
}

TEST_F(SmManagerTest, create_index_table_not_found_throws) {
    setup_db();
    EXPECT_THROW(sm_manager_->create_index("no_such_table", {"a"}, nullptr), TableNotFoundError);
}

TEST_F(SmManagerTest, create_index_column_not_found_throws) {
    setup_db();
    auto cols = make_int_cols({"a"});
    sm_manager_->create_table("col_err_tab", cols, nullptr);
    EXPECT_THROW(sm_manager_->create_index("col_err_tab", {"no_such_col"}, nullptr), ColumnNotFoundError);
}

TEST_F(SmManagerTest, create_index_duplicate_throws) {
    setup_db();
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("dup_idx_tab", cols, nullptr);
    sm_manager_->create_index("dup_idx_tab", {"a"}, nullptr);
    EXPECT_THROW(sm_manager_->create_index("dup_idx_tab", {"a"}, nullptr), IndexExistsError);
}

// =============================================================================
// 删除索引测试
// =============================================================================

TEST_F(SmManagerTest, drop_index_success) {
    setup_db();
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("drop_idx_tab", cols, nullptr);
    sm_manager_->create_index("drop_idx_tab", {"a"}, nullptr);
    EXPECT_EQ(sm_manager_->ihs_.size(), 1);

    sm_manager_->drop_index("drop_idx_tab", {"a"}, nullptr);
    auto& tab = sm_manager_->db_.get_table("drop_idx_tab");
    EXPECT_EQ(tab.indexes.size(), 0);
    EXPECT_EQ(sm_manager_->ihs_.size(), 0);
}

TEST_F(SmManagerTest, drop_one_index_keeps_others) {
    setup_db();
    auto cols = make_int_cols({"a", "b", "c"});
    sm_manager_->create_table("partial_idx_tab", cols, nullptr);
    sm_manager_->create_index("partial_idx_tab", {"a"}, nullptr);
    sm_manager_->create_index("partial_idx_tab", {"b"}, nullptr);

    EXPECT_EQ(sm_manager_->ihs_.size(), 2);
    sm_manager_->drop_index("partial_idx_tab", {"a"}, nullptr);

    auto& tab = sm_manager_->db_.get_table("partial_idx_tab");
    EXPECT_EQ(tab.indexes.size(), 1);
    EXPECT_TRUE(tab.is_index({"b"}));
    EXPECT_FALSE(tab.is_index({"a"}));
    EXPECT_EQ(sm_manager_->ihs_.size(), 1);
}

TEST_F(SmManagerTest, drop_index_table_not_found_throws) {
    setup_db();
    EXPECT_THROW(sm_manager_->drop_index("no_such_table", {"a"}, nullptr), TableNotFoundError);
}

TEST_F(SmManagerTest, drop_index_not_found_throws) {
    setup_db();
    auto cols = make_int_cols({"a"});
    sm_manager_->create_table("no_idx_tab", cols, nullptr);
    EXPECT_THROW(sm_manager_->drop_index("no_idx_tab", {"a"}, nullptr), IndexNotFoundError);
}

TEST_F(SmManagerTest, drop_index_colmeta_overload) {
    setup_db();
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("colmeta_tab", cols, nullptr);
    sm_manager_->create_index("colmeta_tab", {"a"}, nullptr);
    EXPECT_EQ(sm_manager_->ihs_.size(), 1);

    // 通过ColMeta重载删除
    auto& tab = sm_manager_->db_.get_table("colmeta_tab");
    sm_manager_->drop_index("colmeta_tab", tab.indexes[0].cols, nullptr);
    EXPECT_EQ(tab.indexes.size(), 0);
    EXPECT_EQ(sm_manager_->ihs_.size(), 0);
}

// =============================================================================
// 综合集成测试
// =============================================================================

TEST_F(SmManagerTest, full_lifecycle) {
    // 创建 → 打开 → 建表 → 建索引 → 删索引 → 删表 → 关闭 → 删除库
    setup_db();

    // 建表
    auto cols = make_int_cols({"id", "name", "score"});
    sm_manager_->create_table("full_tab", cols, nullptr);
    EXPECT_TRUE(sm_manager_->db_.is_table("full_tab"));
    EXPECT_EQ(sm_manager_->fhs_.size(), 1);

    // 建多个索引
    sm_manager_->create_index("full_tab", {"id"}, nullptr);
    sm_manager_->create_index("full_tab", {"score"}, nullptr);
    sm_manager_->create_index("full_tab", {"id", "name"}, nullptr);
    EXPECT_EQ(sm_manager_->ihs_.size(), 3);

    // desc 验证
    char buf[BUFFER_LENGTH] = {0};
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    EXPECT_NO_THROW(sm_manager_->desc_table("full_tab", &ctx));

    // 删一个索引
    sm_manager_->drop_index("full_tab", {"score"}, nullptr);
    EXPECT_EQ(sm_manager_->ihs_.size(), 2);

    // 删表
    sm_manager_->drop_table("full_tab", nullptr);
    EXPECT_FALSE(sm_manager_->db_.is_table("full_tab"));
    EXPECT_EQ(sm_manager_->fhs_.size(), 0);
    EXPECT_EQ(sm_manager_->ihs_.size(), 0);

    sm_manager_->close_db();
    db_opened_ = false;
    sm_manager_->drop_db(TEST_DB_NAME);
    EXPECT_FALSE(sm_manager_->is_dir(TEST_DB_NAME));
}

TEST_F(SmManagerTest, reopen_preserves_tables) {
    setup_db();
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("persist_tab", cols, nullptr);

    sm_manager_->close_db();
    db_opened_ = false;
    sm_manager_->open_db(TEST_DB_NAME);
    db_opened_ = true;

    EXPECT_TRUE(sm_manager_->db_.is_table("persist_tab"));
    EXPECT_EQ(sm_manager_->fhs_.size(), 1);

    auto& tab = sm_manager_->db_.get_table("persist_tab");
    EXPECT_EQ(tab.cols.size(), 2);
}

TEST_F(SmManagerTest, multiple_tables_and_indexes) {
    setup_db();

    // 创建多张表
    for (int i = 0; i < 5; i++) {
        sm_manager_->create_table("tab" + std::to_string(i), make_int_cols({"a"}), nullptr);
    }
    EXPECT_EQ(sm_manager_->db_.tabs_.size(), 5);
    EXPECT_EQ(sm_manager_->fhs_.size(), 5);

    // 在每张表上创建索引
    for (int i = 0; i < 5; i++) {
        sm_manager_->create_index("tab" + std::to_string(i), {"a"}, nullptr);
    }
    EXPECT_EQ(sm_manager_->ihs_.size(), 5);

    // 逐张删除表
    for (int i = 0; i < 5; i++) {
        sm_manager_->drop_table("tab" + std::to_string(i), nullptr);
    }
    EXPECT_EQ(sm_manager_->db_.tabs_.size(), 0);
    EXPECT_EQ(sm_manager_->fhs_.size(), 0);
    EXPECT_EQ(sm_manager_->ihs_.size(), 0);
}

TEST_F(SmManagerTest, close_db_clears_all_handles) {
    setup_db();
    auto cols = make_int_cols({"a"});
    sm_manager_->create_table("cleanup_tab", cols, nullptr);
    sm_manager_->create_index("cleanup_tab", {"a"}, nullptr);

    EXPECT_EQ(sm_manager_->fhs_.size(), 1);
    EXPECT_EQ(sm_manager_->ihs_.size(), 1);

    sm_manager_->close_db();
    db_opened_ = false;

    EXPECT_EQ(sm_manager_->fhs_.size(), 0);
    EXPECT_EQ(sm_manager_->ihs_.size(), 0);
}

TEST_F(SmManagerTest, drop_compound_index) {
    setup_db();
    auto cols = make_int_cols({"a", "b", "c"});
    sm_manager_->create_table("compound_drop_tab", cols, nullptr);
    sm_manager_->create_index("compound_drop_tab", {"a", "b"}, nullptr);

    EXPECT_EQ(sm_manager_->ihs_.size(), 1);
    sm_manager_->drop_index("compound_drop_tab", std::vector<std::string>{"a", "b"}, nullptr);

    auto& tab = sm_manager_->db_.get_table("compound_drop_tab");
    EXPECT_EQ(tab.indexes.size(), 0);
    EXPECT_EQ(sm_manager_->ihs_.size(), 0);
}
