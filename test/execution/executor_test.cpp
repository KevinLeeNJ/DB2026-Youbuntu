#undef NDEBUG

#define private public
#include "execution/executor_insert.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_projection.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_delete.h"
#include "execution/executor_update.h"
#undef private

#include <memory>
#include "gtest/gtest.h"
#include "system/sm_manager.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "record/rm.h"
#include "index/ix.h"
#include "common/config.h"

const std::string TEST_DB_NAME = "executor_test_db";

class ExecutorTest : public ::testing::Test {
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

    void insert_test_rows(const std::string& tab_name, const std::vector<int>& ids) {
        for (int id : ids) {
            std::vector<Value> vals;
            Value v;
            v.set_int(id);
            vals.push_back(v);
            char buf[4096];
            int offset = 0;
            Context ctx(nullptr, nullptr, nullptr, buf, &offset);
            InsertExecutor exec(sm_manager_.get(), tab_name, vals, &ctx);
            exec.Next();
        }
    }
};

TEST_F(ExecutorTest, seq_scan_empty_table) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("empty_t", cols, nullptr);

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    SeqScanExecutor exec(sm_manager_.get(), "empty_t", {}, &ctx);

    exec.beginTuple();
    EXPECT_TRUE(exec.is_end());
}

TEST_F(ExecutorTest, seq_scan_all_records) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("t1", cols, nullptr);
    insert_test_rows("t1", {10, 20, 30});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    SeqScanExecutor exec(sm_manager_.get(), "t1", {}, &ctx);

    std::vector<std::unique_ptr<RmRecord>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        results.push_back(exec.Next());
    }
    ASSERT_EQ(results.size(), 3);
    // Check values: records should contain int values 10, 20, 30 at offset 0
    EXPECT_EQ(*reinterpret_cast<int*>(results[0]->data), 10);
    EXPECT_EQ(*reinterpret_cast<int*>(results[1]->data), 20);
    EXPECT_EQ(*reinterpret_cast<int*>(results[2]->data), 30);
}

TEST_F(ExecutorTest, seq_scan_with_equality_condition) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("t2", cols, nullptr);
    insert_test_rows("t2", {5, 10, 5, 15});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    Condition cond;
    cond.lhs_col = {"t2", "id"};
    cond.op = OP_EQ;
    cond.is_rhs_val = true;
    Value rhs;
    rhs.set_int(5);
    cond.rhs_val = rhs;

    SeqScanExecutor exec(sm_manager_.get(), "t2", {cond}, &ctx);

    std::vector<std::unique_ptr<RmRecord>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        results.push_back(exec.Next());
    }
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(*reinterpret_cast<int*>(results[0]->data), 5);
    EXPECT_EQ(*reinterpret_cast<int*>(results[1]->data), 5);
}

TEST_F(ExecutorTest, seq_scan_with_range_condition) {
    setup_db();
    auto cols = make_int_cols({"val"});
    sm_manager_->create_table("t3", cols, nullptr);
    insert_test_rows("t3", {1, 2, 3, 4, 5});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    Condition cond;
    cond.lhs_col = {"t3", "val"};
    cond.op = OP_GT;
    cond.is_rhs_val = true;
    Value rhs;
    rhs.set_int(3);
    cond.rhs_val = rhs;

    SeqScanExecutor exec(sm_manager_.get(), "t3", {cond}, &ctx);

    std::vector<std::unique_ptr<RmRecord>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        results.push_back(exec.Next());
    }
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(*reinterpret_cast<int*>(results[0]->data), 4);
    EXPECT_EQ(*reinterpret_cast<int*>(results[1]->data), 5);
}

TEST_F(ExecutorTest, index_scan_empty_table) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("idx_empty", cols, nullptr);
    sm_manager_->create_index("idx_empty", {"id"}, nullptr);

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    IndexScanExecutor exec(sm_manager_.get(), "idx_empty", {}, {"id"}, &ctx);

    exec.beginTuple();
    EXPECT_TRUE(exec.is_end());
}

TEST_F(ExecutorTest, index_scan_all_records) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("idx_t1", cols, nullptr);
    sm_manager_->create_index("idx_t1", {"id"}, nullptr);
    insert_test_rows("idx_t1", {30, 10, 20});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    IndexScanExecutor exec(sm_manager_.get(), "idx_t1", {}, {"id"}, &ctx);

    std::vector<std::unique_ptr<RmRecord>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        results.push_back(exec.Next());
    }
    ASSERT_EQ(results.size(), 3);
    // Index scan returns records in key order: 10, 20, 30
    EXPECT_EQ(*reinterpret_cast<int*>(results[0]->data), 10);
    EXPECT_EQ(*reinterpret_cast<int*>(results[1]->data), 20);
    EXPECT_EQ(*reinterpret_cast<int*>(results[2]->data), 30);
}

TEST_F(ExecutorTest, index_scan_equality_condition) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("idx_t2", cols, nullptr);
    sm_manager_->create_index("idx_t2", {"id"}, nullptr);
    insert_test_rows("idx_t2", {5, 2, 3, 1, 4});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    Condition cond;
    cond.lhs_col = {"idx_t2", "id"};
    cond.op = OP_EQ;
    cond.is_rhs_val = true;
    Value rhs;
    rhs.set_int(2);
    cond.rhs_val = rhs;

    IndexScanExecutor exec(sm_manager_.get(), "idx_t2", {cond}, {"id"}, &ctx);

    std::vector<std::unique_ptr<RmRecord>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        results.push_back(exec.Next());
    }
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(*reinterpret_cast<int*>(results[0]->data), 2);
}

TEST_F(ExecutorTest, index_scan_range_condition) {
    setup_db();
    auto cols = make_int_cols({"val"});
    sm_manager_->create_table("idx_t3", cols, nullptr);
    sm_manager_->create_index("idx_t3", {"val"}, nullptr);
    insert_test_rows("idx_t3", {30, 10, 40, 20, 50});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    Condition cond;
    cond.lhs_col = {"idx_t3", "val"};
    cond.op = OP_GE;
    cond.is_rhs_val = true;
    Value rhs;
    rhs.set_int(30);
    cond.rhs_val = rhs;

    IndexScanExecutor exec(sm_manager_.get(), "idx_t3", {cond}, {"val"}, &ctx);

    std::vector<std::unique_ptr<RmRecord>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        results.push_back(exec.Next());
    }
    ASSERT_EQ(results.size(), 3);
    // In key order: 30, 40, 50
    EXPECT_EQ(*reinterpret_cast<int*>(results[0]->data), 30);
    EXPECT_EQ(*reinterpret_cast<int*>(results[1]->data), 40);
    EXPECT_EQ(*reinterpret_cast<int*>(results[2]->data), 50);
}

TEST_F(ExecutorTest, index_scan_residual_filter) {
    setup_db();
    std::vector<ColDef> cols = {
        {"id", TYPE_INT, 4},
        {"val", TYPE_INT, 4},
    };
    sm_manager_->create_table("idx_t4", cols, nullptr);
    sm_manager_->create_index("idx_t4", {"id"}, nullptr);

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    // Insert rows: id=1,val=100; id=2,val=200; id=3,val=100
    for (int id = 1; id <= 3; id++) {
        std::vector<Value> vals;
        Value v1;
        v1.set_int(id);
        Value v2;
        v2.set_int(id == 2 ? 200 : 100);
        vals.push_back(v1);
        vals.push_back(v2);
        InsertExecutor exec(sm_manager_.get(), "idx_t4", vals, &ctx);
        exec.Next();
    }

    // Filter on non-indexed column: val == 100
    Condition cond;
    cond.lhs_col = {"idx_t4", "val"};
    cond.op = OP_EQ;
    cond.is_rhs_val = true;
    Value rhs;
    rhs.set_int(100);
    cond.rhs_val = rhs;

    IndexScanExecutor exec(sm_manager_.get(), "idx_t4", {cond}, {"id"}, &ctx);

    std::vector<std::unique_ptr<RmRecord>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        results.push_back(exec.Next());
    }
    ASSERT_EQ(results.size(), 2);
    // id=1 first (key order), then id=3
    EXPECT_EQ(*reinterpret_cast<int*>(results[0]->data), 1);
    EXPECT_EQ(*reinterpret_cast<int*>(results[1]->data), 3);
}

TEST_F(ExecutorTest, seq_scan_no_matches) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("t4", cols, nullptr);
    insert_test_rows("t4", {1, 2, 3});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    Condition cond;
    cond.lhs_col = {"t4", "id"};
    cond.op = OP_EQ;
    cond.is_rhs_val = true;
    Value rhs;
    rhs.set_int(99);
    cond.rhs_val = rhs;

    SeqScanExecutor exec(sm_manager_.get(), "t4", {cond}, &ctx);

    exec.beginTuple();
    EXPECT_TRUE(exec.is_end());
}

TEST_F(ExecutorTest, index_scan_gt_boundary) {
    setup_db();
    auto cols = make_int_cols({"val"});
    sm_manager_->create_table("idx_gt", cols, nullptr);
    sm_manager_->create_index("idx_gt", {"val"}, nullptr);
    insert_test_rows("idx_gt", {10, 30, 50});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    Condition cond;
    cond.lhs_col = {"idx_gt", "val"};
    cond.op = OP_GT;
    cond.is_rhs_val = true;
    Value rhs;
    rhs.set_int(30);
    cond.rhs_val = rhs;

    IndexScanExecutor exec(sm_manager_.get(), "idx_gt", {cond}, {"val"}, &ctx);

    std::vector<std::unique_ptr<RmRecord>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        results.push_back(exec.Next());
    }
    ASSERT_EQ(results.size(), 1); // Only 50 > 30, NOT 30 itself
    EXPECT_EQ(*reinterpret_cast<int*>(results[0]->data), 50);
}

TEST_F(ExecutorTest, index_scan_lt_boundary) {
    setup_db();
    auto cols = make_int_cols({"val"});
    sm_manager_->create_table("idx_lt", cols, nullptr);
    sm_manager_->create_index("idx_lt", {"val"}, nullptr);
    insert_test_rows("idx_lt", {10, 30, 50});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    Condition cond;
    cond.lhs_col = {"idx_lt", "val"};
    cond.op = OP_LT;
    cond.is_rhs_val = true;
    Value rhs;
    rhs.set_int(30);
    cond.rhs_val = rhs;

    IndexScanExecutor exec(sm_manager_.get(), "idx_lt", {cond}, {"val"}, &ctx);

    std::vector<std::unique_ptr<RmRecord>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        results.push_back(exec.Next());
    }
    ASSERT_EQ(results.size(), 1); // Only 10 < 30, NOT 30 itself
    EXPECT_EQ(*reinterpret_cast<int*>(results[0]->data), 10);
}

TEST_F(ExecutorTest, projection_subset_columns) {
    setup_db();
    std::vector<ColDef> cols = {
        {"id", TYPE_INT, 4},
        {"name", TYPE_STRING, 16},
        {"score", TYPE_INT, 4},
    };
    sm_manager_->create_table("proj_t1", cols, nullptr);

    // Insert a row
    {
        std::vector<Value> vals;
        Value v1;
        v1.set_int(42);
        Value v2;
        v2.set_str("hello");
        Value v3;
        v3.set_int(99);
        vals.push_back(v1);
        vals.push_back(v2);
        vals.push_back(v3);
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        InsertExecutor exec(sm_manager_.get(), "proj_t1", vals, &ctx);
        exec.Next();
    }

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    // Scan child: read all columns
    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "proj_t1", std::vector<Condition>{}, &ctx);

    // Project only "id" and "score" (skip "name")
    std::vector<TabCol> sel_cols = {{"proj_t1", "id"}, {"proj_t1", "score"}};
    ProjectionExecutor exec(std::move(scan), sel_cols);

    exec.beginTuple();
    ASSERT_FALSE(exec.is_end());
    auto rec = exec.Next();
    // Should have 2 columns: id(4 bytes) + score(4 bytes) = 8 bytes
    EXPECT_EQ(exec.tupleLen(), 8);
    EXPECT_EQ(*reinterpret_cast<int*>(rec->data), 42);     // id at offset 0
    EXPECT_EQ(*reinterpret_cast<int*>(rec->data + 4), 99); // score at offset 4
}

TEST_F(ExecutorTest, projection_all_columns) {
    setup_db();
    auto cols = make_int_cols({"a", "b"});
    sm_manager_->create_table("proj_t2", cols, nullptr);

    {
        std::vector<Value> vals;
        Value v1;
        v1.set_int(10);
        Value v2;
        v2.set_int(20);
        vals.push_back(v1);
        vals.push_back(v2);
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        InsertExecutor exec(sm_manager_.get(), "proj_t2", vals, &ctx);
        exec.Next();
    }

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "proj_t2", std::vector<Condition>{}, &ctx);
    std::vector<TabCol> sel_cols = {{"proj_t2", "a"}, {"proj_t2", "b"}};
    ProjectionExecutor exec(std::move(scan), sel_cols);

    exec.beginTuple();
    ASSERT_FALSE(exec.is_end());
    auto rec = exec.Next();
    EXPECT_EQ(exec.tupleLen(), 8);
    EXPECT_EQ(*reinterpret_cast<int*>(rec->data), 10);
    EXPECT_EQ(*reinterpret_cast<int*>(rec->data + 4), 20);
}

TEST_F(ExecutorTest, projection_multiple_rows) {
    setup_db();
    auto cols = make_int_cols({"x"});
    sm_manager_->create_table("proj_t3", cols, nullptr);
    insert_test_rows("proj_t3", {1, 2, 3});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "proj_t3", std::vector<Condition>{}, &ctx);
    std::vector<TabCol> sel_cols = {{"proj_t3", "x"}};
    ProjectionExecutor exec(std::move(scan), sel_cols);

    std::vector<int> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        auto rec = exec.Next();
        results.push_back(*reinterpret_cast<int*>(rec->data));
    }
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 1);
    EXPECT_EQ(results[1], 2);
    EXPECT_EQ(results[2], 3);
}

TEST_F(ExecutorTest, nljoin_empty_left) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("join_left", cols, nullptr);
    sm_manager_->create_table("join_right", cols, nullptr);
    // Left is empty, right has data
    insert_test_rows("join_right", {1, 2, 3});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "join_left", std::vector<Condition>{}, &ctx);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "join_right", std::vector<Condition>{}, &ctx);
    NestedLoopJoinExecutor exec(std::move(left), std::move(right), {});

    exec.beginTuple();
    EXPECT_TRUE(exec.is_end());
}

TEST_F(ExecutorTest, nljoin_empty_right) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("join_l2", cols, nullptr);
    sm_manager_->create_table("join_r2", cols, nullptr);
    insert_test_rows("join_l2", {1, 2});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "join_l2", std::vector<Condition>{}, &ctx);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "join_r2", std::vector<Condition>{}, &ctx);
    NestedLoopJoinExecutor exec(std::move(left), std::move(right), {});

    exec.beginTuple();
    EXPECT_TRUE(exec.is_end());
}

TEST_F(ExecutorTest, nljoin_cross_product) {
    setup_db();
    auto cols = make_int_cols({"x"});
    sm_manager_->create_table("jl", cols, nullptr);
    sm_manager_->create_table("jr", cols, nullptr);
    insert_test_rows("jl", {1, 2});
    insert_test_rows("jr", {10, 20, 30});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "jl", std::vector<Condition>{}, &ctx);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "jr", std::vector<Condition>{}, &ctx);
    NestedLoopJoinExecutor exec(std::move(left), std::move(right), {});

    std::vector<std::pair<int, int>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        auto rec = exec.Next();
        int l_val = *reinterpret_cast<int*>(rec->data);
        int r_val = *reinterpret_cast<int*>(rec->data + 4);
        results.push_back({l_val, r_val});
    }
    ASSERT_EQ(results.size(), 6); // 2 x 3
    // First left=1 paired with all rights, then left=2
    EXPECT_EQ(results[0].first, 1);
    EXPECT_EQ(results[0].second, 10);
    EXPECT_EQ(results[1].first, 1);
    EXPECT_EQ(results[1].second, 20);
    EXPECT_EQ(results[2].first, 1);
    EXPECT_EQ(results[2].second, 30);
    EXPECT_EQ(results[3].first, 2);
    EXPECT_EQ(results[3].second, 10);
    EXPECT_EQ(results[4].first, 2);
    EXPECT_EQ(results[4].second, 20);
    EXPECT_EQ(results[5].first, 2);
    EXPECT_EQ(results[5].second, 30);
}

TEST_F(ExecutorTest, nljoin_with_condition) {
    setup_db();
    auto cols = make_int_cols({"val"});
    sm_manager_->create_table("jc_l", cols, nullptr);
    sm_manager_->create_table("jc_r", cols, nullptr);
    insert_test_rows("jc_l", {1, 2, 3});
    insert_test_rows("jc_r", {1, 2, 4});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "jc_l", std::vector<Condition>{}, &ctx);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "jc_r", std::vector<Condition>{}, &ctx);

    // Join condition: left.val == right.val
    Condition cond;
    cond.lhs_col = {"jc_l", "val"};
    cond.op = OP_EQ;
    cond.is_rhs_val = false;
    cond.rhs_col = {"jc_r", "val"};

    NestedLoopJoinExecutor exec(std::move(left), std::move(right), {cond});

    std::vector<std::pair<int, int>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        auto rec = exec.Next();
        int l_val = *reinterpret_cast<int*>(rec->data);
        int r_val = *reinterpret_cast<int*>(rec->data + 4);
        results.push_back({l_val, r_val});
    }
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0].first, 1);
    EXPECT_EQ(results[0].second, 1);
    EXPECT_EQ(results[1].first, 2);
    EXPECT_EQ(results[1].second, 2);
}

TEST_F(ExecutorTest, delete_all_records) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("del_t1", cols, nullptr);
    insert_test_rows("del_t1", {1, 2, 3});

    // Verify 3 records exist
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "del_t1", {}, &ctx);
        int count = 0;
        for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
            count++;
        }
        EXPECT_EQ(count, 3);
    }

    // Collect Rids and delete
    std::vector<Rid> rids;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "del_t1", {}, &ctx);
        for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
            scan.Next();
            rids.push_back(scan.rid());
        }
    }

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    DeleteExecutor exec(sm_manager_.get(), "del_t1", {}, rids, &ctx);
    exec.Next();

    // Verify all records are gone
    {
        char buf2[4096];
        int offset2 = 0;
        Context ctx2(nullptr, nullptr, nullptr, buf2, &offset2);
        SeqScanExecutor scan(sm_manager_.get(), "del_t1", {}, &ctx2);
        scan.beginTuple();
        EXPECT_TRUE(scan.is_end());
    }
}

TEST_F(ExecutorTest, delete_maintains_index) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("del_t2", cols, nullptr);
    sm_manager_->create_index("del_t2", {"id"}, nullptr);
    insert_test_rows("del_t2", {10, 20, 30});

    // Collect Rids for records where id <= 20
    std::vector<Rid> rids;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "del_t2", {}, &ctx);
        for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
            auto rec = scan.Next();
            int val = *reinterpret_cast<int*>(rec->data);
            if (val <= 20) {
                rids.push_back(scan.rid());
            }
        }
    }
    ASSERT_EQ(rids.size(), 2);

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    DeleteExecutor exec(sm_manager_.get(), "del_t2", {}, rids, &ctx);
    exec.Next();

    // Verify index scan still finds id=30 and not deleted ones
    IndexScanExecutor idx_scan(sm_manager_.get(), "del_t2", {}, {"id"}, &ctx);
    std::vector<int> remaining;
    for (idx_scan.beginTuple(); !idx_scan.is_end(); idx_scan.nextTuple()) {
        auto rec = idx_scan.Next();
        remaining.push_back(*reinterpret_cast<int*>(rec->data));
    }
    ASSERT_EQ(remaining.size(), 1);
    EXPECT_EQ(remaining[0], 30);
}

TEST_F(ExecutorTest, update_single_field) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("upd_t1", cols, nullptr);
    insert_test_rows("upd_t1", {10, 20, 30});

    // Collect all Rids
    std::vector<Rid> rids;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "upd_t1", {}, &ctx);
        for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
            scan.Next();
            rids.push_back(scan.rid());
        }
    }

    // Update all rows: set id = 110
    SetClause sc;
    sc.lhs = {"upd_t1", "id"};
    Value new_val;
    new_val.set_int(110);
    sc.rhs = new_val;

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    UpdateExecutor exec(sm_manager_.get(), "upd_t1", {sc}, {}, rids, &ctx);
    exec.Next();

    // Verify all records now have id=110
    SeqScanExecutor scan(sm_manager_.get(), "upd_t1", {}, &ctx);
    std::vector<int> results;
    for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
        auto rec = scan.Next();
        results.push_back(*reinterpret_cast<int*>(rec->data));
    }
    ASSERT_EQ(results.size(), 3);
    for (int val : results) {
        EXPECT_EQ(val, 110);
    }
}

TEST_F(ExecutorTest, update_maintains_index) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("upd_t2", cols, nullptr);
    sm_manager_->create_index("upd_t2", {"id"}, nullptr);
    insert_test_rows("upd_t2", {1, 2, 3});

    // Collect all Rids
    std::vector<Rid> rids;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "upd_t2", {}, &ctx);
        for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
            scan.Next();
            rids.push_back(scan.rid());
        }
    }

    // Update only the first record: set id = 100
    SetClause sc;
    sc.lhs = {"upd_t2", "id"};
    Value new_val;
    new_val.set_int(100);
    sc.rhs = new_val;

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    UpdateExecutor exec(sm_manager_.get(), "upd_t2", {sc}, {}, {rids[0]}, &ctx);
    exec.Next();

    // Verify via index scan: records are 2, 3, 100 (in key order)
    IndexScanExecutor idx_scan(sm_manager_.get(), "upd_t2", {}, {"id"}, &ctx);
    std::vector<int> results;
    for (idx_scan.beginTuple(); !idx_scan.is_end(); idx_scan.nextTuple()) {
        auto rec = idx_scan.Next();
        results.push_back(*reinterpret_cast<int*>(rec->data));
    }
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 2);
    EXPECT_EQ(results[1], 3);
    EXPECT_EQ(results[2], 100);
}

TEST_F(ExecutorTest, update_multiple_fields) {
    setup_db();
    std::vector<ColDef> cols = {
        {"a", TYPE_INT, 4},
        {"b", TYPE_STRING, 16},
    };
    sm_manager_->create_table("upd_t3", cols, nullptr);

    // Insert a row
    {
        std::vector<Value> vals;
        Value v1;
        v1.set_int(5);
        Value v2;
        v2.set_str("old");
        vals.push_back(v1);
        vals.push_back(v2);
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        InsertExecutor exec(sm_manager_.get(), "upd_t3", vals, &ctx);
        exec.Next();
    }

    // Collect Rid
    std::vector<Rid> rids;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "upd_t3", {}, &ctx);
        scan.beginTuple();
        scan.Next();
        rids.push_back(scan.rid());
    }

    // Update b to "new"
    SetClause sc;
    sc.lhs = {"upd_t3", "b"};
    Value new_val;
    new_val.set_str("new");
    sc.rhs = new_val;

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    UpdateExecutor exec(sm_manager_.get(), "upd_t3", {sc}, {}, rids, &ctx);
    exec.Next();

    // Verify
    SeqScanExecutor scan(sm_manager_.get(), "upd_t3", {}, &ctx);
    scan.beginTuple();
    auto rec = scan.Next();
    EXPECT_EQ(*reinterpret_cast<int*>(rec->data), 5);
    EXPECT_EQ(strncmp(rec->data + 4, "new", 3), 0);
}
