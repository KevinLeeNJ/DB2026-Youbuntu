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
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
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

TEST_F(SmManagerTest, open_db_empty_directory_throws_without_changing_cwd) {
    std::string empty_db_name = TEST_DB_NAME + "_empty";
    if (sm_manager_->is_dir(empty_db_name)) {
        sm_manager_->drop_db(empty_db_name);
    }
    ASSERT_EQ(mkdir(empty_db_name.c_str(), 0755), 0);

    char before_buf[1024];
    ASSERT_NE(getcwd(before_buf, sizeof(before_buf)), nullptr);
    std::string before = before_buf;

    EXPECT_THROW(sm_manager_->open_db(empty_db_name), RMDBError);

    char after_buf[1024];
    ASSERT_NE(getcwd(after_buf, sizeof(after_buf)), nullptr);
    std::string after = after_buf;
    if (after != before) {
        ASSERT_EQ(chdir(before.c_str()), 0);
    }
    EXPECT_EQ(after, before);
    EXPECT_TRUE(sm_manager_->db_.name_.empty());

    if (sm_manager_->is_dir(empty_db_name)) {
        sm_manager_->drop_db(empty_db_name);
    }
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

TEST_F(SmManagerTest, LoadFlushFailureDoesNotPublishMetadataOrReportSuccess) {
    setup_db();
    sm_manager_->create_table("load_fail", make_int_cols({"id"}), nullptr);
    {
        std::ofstream csv("load_fail.csv", std::ios::trunc);
        ASSERT_TRUE(csv.is_open());
        csv << "id\n1\n";
    }

    constexpr std::string_view kMetadataSentinel = "load_metadata_must_not_publish";
    sm_manager_->db_.get_table("load_fail").name = std::string(kMetadataSentinel);
    struct FlushHookReset {
        ~FlushHookReset() {
            BufferPoolManager::set_flush_batch_before_write_test_hook({});
        }
    } hook_reset;
    BufferPoolManager::set_flush_batch_before_write_test_hook(
        [](PageId, Page*) { throw std::runtime_error("injected load flush failure"); });

    EXPECT_THROW(sm_manager_->load_csv_data("./load_fail.csv", "load_fail", nullptr), InternalError);

    std::ifstream meta(DB_META_NAME);
    const std::string meta_contents((std::istreambuf_iterator<char>(meta)), std::istreambuf_iterator<char>());
    EXPECT_EQ(meta_contents.find(kMetadataSentinel), std::string::npos);

    BufferPoolManager::set_flush_batch_before_write_test_hook({});
    sm_manager_->db_.get_table("load_fail").name = "load_fail";
    std::remove("load_fail.csv");
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
        {"val", TYPE_FLOAT, 4},
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

TEST_F(SmManagerTest, drop_table_clears_only_its_runtime_version_history) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("reused_name", cols, nullptr);
    sm_manager_->create_index("reused_name", {"id"}, nullptr);
    sm_manager_->create_table("survivor", cols, nullptr);
    sm_manager_->create_index("survivor", {"id"}, nullptr);

    const auto& reused_index = sm_manager_->db_.get_table("reused_name").indexes[0];
    const auto& survivor_index = sm_manager_->db_.get_table("survivor").indexes[0];
    const std::string reused_index_name = sm_manager_->ix_manager_->get_index_name("reused_name", reused_index.cols);
    const std::string survivor_index_name = sm_manager_->ix_manager_->get_index_name("survivor", survivor_index.cols);
    int reused_value = 7;
    int survivor_value = 11;
    std::vector<char> reused_key(sizeof(reused_value));
    std::vector<char> survivor_key(sizeof(survivor_value));
    std::memcpy(reused_key.data(), &reused_value, sizeof(reused_value));
    std::memcpy(survivor_key.data(), &survivor_value, sizeof(survivor_value));
    const Rid reused_rid{1, 2};
    const Rid survivor_rid{3, 4};
    TupleMeta reused_tombstone;
    reused_tombstone.writer_txn_id_ = 201;
    reused_tombstone.is_deleted_ = true;
    reused_tombstone.version_chain_head_ = UndoLink{0, 1, 201};
    TupleMeta survivor_tombstone;
    survivor_tombstone.writer_txn_id_ = 202;
    survivor_tombstone.is_deleted_ = true;
    survivor_tombstone.version_chain_head_ = UndoLink{0, 2, 202};
    const std::string reused_row("reused", 6);
    const std::string survivor_row("survivor", 8);
    RmRecord reused_record(static_cast<int>(reused_row.size()));
    std::memcpy(reused_record.data, reused_row.data(), reused_row.size());
    RmRecord survivor_record(static_cast<int>(survivor_row.size()));
    std::memcpy(survivor_record.data, survivor_row.data(), survivor_row.size());

    sm_manager_->remember_historical_index_key("reused_name", reused_index_name, reused_key, reused_rid, reused_index);
    sm_manager_->remember_deleted_tuple_candidate_for_test("reused_name", reused_rid, reused_row, reused_tombstone);
    sm_manager_->remember_historical_index_key("survivor", survivor_index_name, survivor_key, survivor_rid,
                                               survivor_index);
    sm_manager_->remember_deleted_tuple_candidate_for_test("survivor", survivor_rid, survivor_row, survivor_tombstone);

    sm_manager_->drop_table("reused_name", nullptr);
    EXPECT_TRUE(sm_manager_->get_historical_index_key_rids("reused_name", reused_index_name, reused_key).empty());
    EXPECT_TRUE(sm_manager_->get_deleted_tuple_candidates("reused_name", reused_record).empty());
    EXPECT_EQ(sm_manager_->get_historical_index_key_rids("survivor", survivor_index_name, survivor_key),
              std::vector<Rid>{survivor_rid});
    auto survivor_candidates = sm_manager_->get_deleted_tuple_candidates("survivor", survivor_record);
    ASSERT_EQ(survivor_candidates.size(), 1u);
    EXPECT_EQ(survivor_candidates[0].rid, survivor_rid);
    ASSERT_EQ(sm_manager_->historical_retire_queue_.size(), 1u);
    EXPECT_TRUE(sm_manager_->historical_bucket_belongs_to_table(
        sm_manager_->historical_retire_queue_.front().bucket_key, "survivor"));

    sm_manager_->create_table("reused_name", cols, nullptr);
    sm_manager_->create_index("reused_name", {"id"}, nullptr);
    EXPECT_TRUE(sm_manager_->get_historical_index_key_rids("reused_name", reused_index_name, reused_key).empty());
}

TEST_F(SmManagerTest, deleted_tuple_candidate_gc_is_strict_and_rotates_past_512_unsafe_prefix) {
    setup_db();
    sm_manager_->create_table("candidate_gc", make_int_cols({"id"}), nullptr);
    auto* fh = sm_manager_->fhs_.at("candidate_gc").get();

    RmRecord record(sizeof(int));
    const int value = 9;
    std::memcpy(record.data, &value, sizeof(value));

    // The first entry remains unsafe.  The other 513 entries are safe at
    // watermark 11, which proves that a bounded FIFO pass does not repeatedly
    // starve entries after an unsafe prefix.
    Rid first_rid;
    for (int i = 0; i < 514; ++i) {
        Rid rid = fh->insert_record(record.data, nullptr);
        TupleMeta tombstone;
        tombstone.writer_txn_id_ = 1000 + i;
        tombstone.is_deleted_ = true;
        tombstone.is_committed_ = i != 0;
        tombstone.commit_ts_ = i == 0 ? INVALID_TS : 10;
        tombstone.version_chain_head_ = UndoLink{0, i + 1, 1000 + i};
        fh->set_tuple_meta(rid, tombstone);
        sm_manager_->remember_deleted_tuple_candidate("candidate_gc", rid, record, tombstone);
        if (i == 0) {
            first_rid = rid;
        }
    }

    sm_manager_->prune_version_history(11);
    EXPECT_EQ(sm_manager_->get_deleted_tuple_candidate_stats().active_candidates, 3u);
    sm_manager_->prune_version_history(11);
    EXPECT_EQ(sm_manager_->get_deleted_tuple_candidate_stats().active_candidates, 1u);

    // Equality is not safe: exactly watermark must remain. Advancing the
    // watermark by one makes the same tombstone reclaimable.
    TupleMeta first_meta = fh->get_tuple_meta(first_rid);
    first_meta.is_committed_ = true;
    first_meta.commit_ts_ = 11;
    fh->set_tuple_meta(first_rid, first_meta);
    sm_manager_->prune_version_history(11);
    EXPECT_EQ(sm_manager_->get_deleted_tuple_candidate_stats().active_candidates, 1u);
    sm_manager_->prune_version_history(12);
    EXPECT_EQ(sm_manager_->get_deleted_tuple_candidate_stats().active_candidates, 0u);
}

TEST_F(SmManagerTest, database_lifecycle_resets_all_deleted_tuple_candidate_diagnostics) {
    setup_db();
    sm_manager_->create_table("candidate_stats", make_int_cols({"id"}), nullptr);
    auto* fh = sm_manager_->fhs_.at("candidate_stats").get();

    RmRecord record(sizeof(int));
    const int value = 17;
    std::memcpy(record.data, &value, sizeof(value));
    const Rid rid = fh->insert_record(record.data, nullptr);

    TupleMeta tombstone;
    tombstone.writer_txn_id_ = 301;
    tombstone.is_deleted_ = true;
    tombstone.version_chain_head_ = UndoLink{0, 1, 301};
    fh->set_tuple_meta(rid, tombstone);

    sm_manager_->set_deleted_tuple_candidate_diagnostics_enabled(true);
    sm_manager_->remember_deleted_tuple_candidate("candidate_stats", rid, record, tombstone);
    EXPECT_EQ(sm_manager_->get_deleted_tuple_candidates("candidate_stats", record).size(), 1u);
    RmRecord different_record(sizeof(int));
    const int different_value = 18;
    std::memcpy(different_record.data, &different_value, sizeof(different_value));
    EXPECT_TRUE(sm_manager_->get_deleted_tuple_candidates("candidate_stats", different_record).empty());
    sm_manager_->note_deleted_tuple_candidate_validated(2);
    sm_manager_->note_deleted_tuple_candidate_page_probe();

    // Exercise both GC outcomes so the cumulative retire counters are nonzero.
    sm_manager_->prune_version_history(10);
    tombstone.is_committed_ = true;
    tombstone.commit_ts_ = 9;
    fh->set_tuple_meta(rid, tombstone);
    sm_manager_->prune_version_history(10);
    sm_manager_->remember_deleted_tuple_candidate("candidate_stats", rid, record, tombstone);

    auto before = sm_manager_->get_deleted_tuple_candidate_stats();
    EXPECT_GT(before.lookups, 0u);
    EXPECT_GT(before.exact_hits, 0u);
    EXPECT_GT(before.exact_misses, 0u);
    EXPECT_GT(before.validated_candidates, 0u);
    EXPECT_GT(before.page_probes, 0u);
    EXPECT_GT(before.active_candidates, 0u);
    EXPECT_GT(before.active_buckets, 0u);
    EXPECT_GT(before.retire_pops, 0u);
    EXPECT_GT(before.retire_requeues, 0u);
    EXPECT_GT(before.retire_prunes, 0u);
    EXPECT_GT(before.retire_max_queue, 0u);

    sm_manager_->close_db();
    db_opened_ = false;
    auto after_close = sm_manager_->get_deleted_tuple_candidate_stats();
    EXPECT_EQ(after_close.lookups, 0u);
    EXPECT_EQ(after_close.exact_hits, 0u);
    EXPECT_EQ(after_close.exact_misses, 0u);
    EXPECT_EQ(after_close.validated_candidates, 0u);
    EXPECT_EQ(after_close.page_probes, 0u);
    EXPECT_EQ(after_close.active_candidates, 0u);
    EXPECT_EQ(after_close.active_buckets, 0u);
    EXPECT_EQ(after_close.retire_pops, 0u);
    EXPECT_EQ(after_close.retire_requeues, 0u);
    EXPECT_EQ(after_close.retire_prunes, 0u);
    EXPECT_EQ(after_close.retire_max_queue, 0u);

    sm_manager_->open_db(TEST_DB_NAME);
    db_opened_ = true;
    auto after_reopen = sm_manager_->get_deleted_tuple_candidate_stats();
    EXPECT_EQ(after_reopen.lookups, 0u);
    EXPECT_EQ(after_reopen.exact_hits, 0u);
    EXPECT_EQ(after_reopen.exact_misses, 0u);
    EXPECT_EQ(after_reopen.validated_candidates, 0u);
    EXPECT_EQ(after_reopen.page_probes, 0u);
    EXPECT_EQ(after_reopen.active_candidates, 0u);
    EXPECT_EQ(after_reopen.active_buckets, 0u);
    EXPECT_EQ(after_reopen.retire_pops, 0u);
    EXPECT_EQ(after_reopen.retire_requeues, 0u);
    EXPECT_EQ(after_reopen.retire_prunes, 0u);
    EXPECT_EQ(after_reopen.retire_max_queue, 0u);
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

    // 验证索引句柄已打开，且 CREATE INDEX 在树构建完成后默认建立根缓存。
    std::string ix_name = sm_manager_->ix_manager_->get_index_name("idx_tab", tab.indexes[0].cols);
    auto handle = sm_manager_->ihs_.find(ix_name);
    ASSERT_NE(handle, sm_manager_->ihs_.end());
    ASSERT_NE(handle->second->cached_root_page_, nullptr);
    EXPECT_EQ(handle->second->cached_root_page_no_, handle->second->file_hdr_->root_page_);
    EXPECT_TRUE(
        buffer_pool_manager_->is_page_resident(PageId{handle->second->fd_, handle->second->cached_root_page_no_}));
}

TEST_F(SmManagerTest, PostRecoveryRefreshRestoresRootCacheAfterReopen) {
    setup_db();
    auto cols = make_int_cols({"a"});
    sm_manager_->create_table("reopen_idx_tab", cols, nullptr);
    sm_manager_->create_index("reopen_idx_tab", {"a"}, nullptr);

    sm_manager_->close_db();
    db_opened_ = false;
    sm_manager_->open_db(TEST_DB_NAME);
    db_opened_ = true;

    const std::string ix_name =
        sm_manager_->ix_manager_->get_index_name("reopen_idx_tab", std::vector<std::string>{"a"});
    auto handle = sm_manager_->ihs_.find(ix_name);
    ASSERT_NE(handle, sm_manager_->ihs_.end());
    EXPECT_EQ(handle->second->cached_root_page_, nullptr);

    // Production invokes this exact root-only hook after WAL recovery.
    sm_manager_->refresh_index_residency();
    ASSERT_NE(handle->second->cached_root_page_, nullptr);
    EXPECT_EQ(handle->second->cached_root_page_no_, handle->second->file_hdr_->root_page_);
    EXPECT_TRUE(
        buffer_pool_manager_->is_page_resident(PageId{handle->second->fd_, handle->second->cached_root_page_no_}));
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
    auto* handle = sm_manager_->ihs_.begin()->second.get();
    const PageId cached_root_id{handle->fd_, handle->cached_root_page_no_};
    ASSERT_NE(handle->cached_root_page_, nullptr);
    EXPECT_TRUE(buffer_pool_manager_->is_page_resident(cached_root_id));

    sm_manager_->drop_index("drop_idx_tab", {"a"}, nullptr);
    auto& tab = sm_manager_->db_.get_table("drop_idx_tab");
    EXPECT_EQ(tab.indexes.size(), 0);
    EXPECT_EQ(sm_manager_->ihs_.size(), 0);
    EXPECT_FALSE(buffer_pool_manager_->is_page_resident(cached_root_id));
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
