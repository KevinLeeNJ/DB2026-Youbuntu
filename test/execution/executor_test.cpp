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
#include "execution/executor_insert.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_filter.h"
#include "execution/executor_projection.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_delete.h"
#include "execution/executor_update.h"
#include "execution/execution_manager.h"
#undef private

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include "gtest/gtest.h"
#include "system/sm_manager.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "record/rm.h"
#include "index/ix.h"
#include "common/config.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

const std::string TEST_DB_NAME = "executor_test_db";

namespace {

ColMeta make_test_col(const std::string& tab_name, const std::string& name, ColType type, int len, int offset) {
    return ColMeta{tab_name, name, type, len, offset, false};
}

RmRecord make_join_record(int value) {
    RmRecord rec(sizeof(int));
    std::memcpy(rec.data, &value, sizeof(int));
    return rec;
}

RmRecord make_filter_record(int lhs, int rhs) {
    RmRecord rec(sizeof(int) * 2);
    std::memcpy(rec.data, &lhs, sizeof(int));
    std::memcpy(rec.data + sizeof(int), &rhs, sizeof(int));
    return rec;
}

class CountingExecutor : public AbstractExecutor {
public:
    CountingExecutor(std::vector<ColMeta> cols, std::vector<RmRecord> records,
                     int* external_next_record_calls = nullptr, bool* tracking_enabled_on_begin = nullptr)
        : cols_(std::move(cols)), records_(std::move(records)), external_next_record_calls_(external_next_record_calls),
          tracking_enabled_on_begin_(tracking_enabled_on_begin) {
        len_ = 0;
        for (const auto& col : cols_) {
            len_ = std::max(len_, static_cast<size_t>(col.offset + col.len));
        }
    }

    void beginTuple() override {
        ++begin_calls_;
        if (tracking_enabled_on_begin_ != nullptr) {
            *tracking_enabled_on_begin_ = context_ != nullptr && context_->enable_ssi_read_tracking_;
        }
        if (throw_on_begin_) {
            throw std::runtime_error("test child begin failure");
        }
        cursor_ = 0;
    }

    void nextTuple() override {
        ++next_calls_;
        if (!is_end()) {
            ++cursor_;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        ++next_record_calls_;
        if (external_next_record_calls_ != nullptr) {
            ++*external_next_record_calls_;
        }
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(records_[cursor_]);
    }

    TupleView current() const override {
        if (is_end()) {
            return {};
        }
        const auto& record = records_[cursor_];
        return TupleView{record.data, static_cast<uint32_t>(record.size)};
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    bool is_end() const override {
        return cursor_ >= records_.size();
    }

    const std::vector<ColMeta>& cols() const override {
        ++cols_calls_;
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : cols_) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + "." + target.col_name);
    }

    int begin_calls_ = 0;
    int next_calls_ = 0;
    int next_record_calls_ = 0;
    mutable int cols_calls_ = 0;
    bool throw_on_begin_ = false;

    std::string scan_table_name() const override {
        return "t";
    }

private:
    std::vector<ColMeta> cols_;
    std::vector<RmRecord> records_;
    int* external_next_record_calls_ = nullptr;
    bool* tracking_enabled_on_begin_ = nullptr;
    size_t len_ = 0;
    size_t cursor_ = 0;
};

class CountingResultSink : public QueryResultSink {
public:
    void begin_query(const std::vector<ColMeta>& columns, const std::vector<std::string>& names) override {
        ++begin_calls;
        column_count = columns.size();
        captions = names;
    }

    void append_row(const std::vector<ColMeta>&, const char*, std::size_t) override {
        ++row_count;
    }

    int begin_calls = 0;
    int row_count = 0;
    std::size_t column_count = 0;
    std::vector<std::string> captions;
};

class BareExecutor : public AbstractExecutor {
public:
    std::unique_ptr<RmRecord> Next() override {
        return nullptr;
    }

    Rid& rid() override {
        return _abstract_rid;
    }
};

} // namespace

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

TEST(AbstractExecutorFocusedTest, DefaultColsIsSafe) {
    BareExecutor executor;
    EXPECT_TRUE(executor.cols().empty());
}

TEST(FilterExecutorFocusedTest, CachesConditionAddressesAndReusesChildSchema) {
    const std::vector<ColMeta> cols{make_test_col("t", "id", TYPE_INT, sizeof(int), 0),
                                    make_test_col("t", "threshold", TYPE_INT, sizeof(int), sizeof(int))};
    auto child = std::make_unique<CountingExecutor>(
        cols, std::vector<RmRecord>{make_filter_record(5, 4), make_filter_record(2, 4)});
    const auto* child_schema = &child->cols();
    auto* child_ptr = child.get();

    Condition condition;
    condition.lhs_col = {"t", "id"};
    condition.op = OP_GT;
    condition.is_rhs_val = false;
    condition.rhs_col = {"t", "threshold"};

    FilterExecutor executor(std::move(child), {condition});

    EXPECT_EQ(child_ptr->cols_calls_, 3);
    EXPECT_EQ(&executor.cols(), child_schema);
    EXPECT_EQ(child_ptr->cols_calls_, 4);

    executor.beginTuple();
    ASSERT_FALSE(executor.is_end());
    auto record = executor.Next();
    ASSERT_NE(record, nullptr);
    EXPECT_EQ(*reinterpret_cast<const int*>(record->data), 5);
    executor.nextTuple();
    EXPECT_TRUE(executor.is_end());
    EXPECT_EQ(child_ptr->cols_calls_, 4);
    EXPECT_EQ(child_ptr->next_record_calls_, 0);
}

TEST(FilterExecutorFocusedTest, RestoresContextTrackingWhenChildThrows) {
    LockManager lock_manager;
    TransactionManager txn_manager(&lock_manager, nullptr);
    Transaction txn(1, IsolationLevel::SERIALIZABLE);
    char data_send[64] = {};
    int offset = 0;
    Context context(&lock_manager, nullptr, &txn, data_send, &offset, &txn_manager);
    context.enable_ssi_read_tracking_ = true;

    auto child =
        std::make_unique<CountingExecutor>(std::vector<ColMeta>{make_test_col("t", "id", TYPE_INT, sizeof(int), 0)},
                                           std::vector<RmRecord>{make_join_record(1)});
    child->context_ = &context;
    child->throw_on_begin_ = true;
    FilterExecutor executor(std::move(child), {});

    EXPECT_THROW(executor.beginTuple(), std::runtime_error);
    EXPECT_TRUE(context.enable_ssi_read_tracking_);
}

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

TEST_F(ExecutorTest, tombstone_candidates_are_deduplicated_and_removable) {
    Rid first{1, 3};
    Rid second{2, 5};

    EXPECT_TRUE(sm_manager_->get_deleted_tuple_candidates("t").empty());

    sm_manager_->remember_deleted_tuple_candidate("t", first);
    sm_manager_->remember_deleted_tuple_candidate("t", first);
    sm_manager_->remember_deleted_tuple_candidate("t", second);

    auto candidates = sm_manager_->get_deleted_tuple_candidates("t");
    ASSERT_EQ(candidates.size(), 2);
    EXPECT_NE(std::find(candidates.begin(), candidates.end(), first), candidates.end());
    EXPECT_NE(std::find(candidates.begin(), candidates.end(), second), candidates.end());

    sm_manager_->remove_deleted_tuple_candidate("t", first);
    candidates = sm_manager_->get_deleted_tuple_candidates("t");
    ASSERT_EQ(candidates.size(), 1);
    EXPECT_EQ(candidates[0], second);
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
    // 2 columns: id(4) + score(4) + 1 byte trailing NULL bitmap
    EXPECT_EQ(exec.tupleLen(), 8 + 1);
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
    // a(4) + b(4) + 1 byte trailing NULL bitmap
    EXPECT_EQ(exec.tupleLen(), 8 + 1);
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

    // The joined tuple concatenates both child tuples, each with its own
    // trailing NULL bitmap, so column offsets must come from cols().
    const int l_off = exec.cols()[0].offset;
    const int r_off = exec.cols()[1].offset;
    std::vector<std::pair<int, int>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        auto rec = exec.Next();
        int l_val = read_unaligned<int>(rec->data + l_off);
        int r_val = read_unaligned<int>(rec->data + r_off);
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

    const int l_off = exec.cols()[0].offset;
    const int r_off = exec.cols()[1].offset;
    std::vector<std::pair<int, int>> results;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        auto rec = exec.Next();
        int l_val = read_unaligned<int>(rec->data + l_off);
        int r_val = read_unaligned<int>(rec->data + r_off);
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

TEST_F(ExecutorTest, seq_scan_rid_after_beginTuple) {
    setup_db();
    auto cols = make_int_cols({"id"});
    sm_manager_->create_table("rid_t1", cols, nullptr);
    insert_test_rows("rid_t1", {10, 20, 30});

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    SeqScanExecutor exec(sm_manager_.get(), "rid_t1", {}, &ctx);

    std::vector<Rid> rids;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        rids.push_back(exec.rid());
    }
    ASSERT_EQ(rids.size(), 3);
    // Each rid should reference a valid page (>= RM_FIRST_RECORD_PAGE = 1)
    for (auto& rid : rids) {
        EXPECT_GE(rid.page_no, 1);
        EXPECT_GE(rid.slot_no, 0);
    }
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

TEST_F(ExecutorTest, row_mutation_binding_offsets_types_and_execution) {
    setup_db();
    std::vector<ColDef> cols = {
        {"id", TYPE_INT, 4},         {"source", TYPE_INT, 4},  {"assigned", TYPE_INT, 4},
        {"arithmetic", TYPE_INT, 4}, {"score", TYPE_FLOAT, 4},
    };
    sm_manager_->create_table("mutation_bind", cols, nullptr);

    Value id;
    id.set_int(1);
    Value source;
    source.set_int(7);
    Value assigned;
    assigned.set_int(3);
    Value arithmetic;
    arithmetic.set_int(4);
    Value score;
    score.set_float(1.5);
    char buf[4096] = {};
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    InsertExecutor insert(sm_manager_.get(), "mutation_bind", {id, source, assigned, arithmetic, score}, &ctx);
    insert.Next();

    Condition literal_condition;
    literal_condition.lhs_col = {"mutation_bind", "score"};
    literal_condition.op = OP_GT;
    literal_condition.is_rhs_val = true;
    literal_condition.rhs_val.set_int(1);
    Condition column_condition;
    column_condition.lhs_col = {"mutation_bind", "source"};
    column_condition.op = OP_GT;
    column_condition.is_rhs_val = false;
    column_condition.rhs_col = {"mutation_bind", "assigned"};
    std::vector<Condition> conditions{literal_condition, column_condition};

    const auto& tab = sm_manager_->db_.get_table("mutation_bind");
    auto bound_conditions = BindMutationConditions(tab, conditions);
    ASSERT_EQ(bound_conditions.size(), 2U);
    EXPECT_EQ(bound_conditions[0].lhs.offset, 16U);
    EXPECT_EQ(bound_conditions[0].lhs.len, 4U);
    EXPECT_EQ(bound_conditions[0].lhs.type, TYPE_FLOAT);
    EXPECT_EQ(bound_conditions[0].rhs.offset, 0U);
    EXPECT_EQ(bound_conditions[0].rhs.len, 0U);
    EXPECT_EQ(bound_conditions[0].rhs.type, TYPE_INT);
    EXPECT_EQ(bound_conditions[1].lhs.offset, 4U);
    EXPECT_EQ(bound_conditions[1].lhs.len, 4U);
    EXPECT_EQ(bound_conditions[1].lhs.type, TYPE_INT);
    EXPECT_EQ(bound_conditions[1].rhs.offset, 8U);
    EXPECT_EQ(bound_conditions[1].rhs.len, 4U);
    EXPECT_EQ(bound_conditions[1].rhs.type, TYPE_INT);

    SetClause literal_set;
    literal_set.lhs = {"mutation_bind", "score"};
    literal_set.rhs.set_int(42);
    SetClause column_set;
    column_set.lhs = {"mutation_bind", "assigned"};
    column_set.is_self_ref = true;
    column_set.rhs_col = {"mutation_bind", "source"};
    column_set.op = UpdateOp::ASSIGNMENT;
    SetClause arithmetic_set;
    arithmetic_set.lhs = {"mutation_bind", "arithmetic"};
    arithmetic_set.rhs.set_int(5);
    arithmetic_set.is_self_ref = true;
    arithmetic_set.rhs_col = {"mutation_bind", "source"};
    arithmetic_set.op = UpdateOp::SELF_ADD;
    UpdateTerm subtract_term;
    subtract_term.op = UpdateOp::SELF_SUB;
    subtract_term.rhs.set_int(2);
    UpdateTerm add_term;
    add_term.op = UpdateOp::SELF_ADD;
    add_term.rhs.set_int(3);
    arithmetic_set.additional_terms = {subtract_term, add_term};
    std::vector<SetClause> set_clauses{literal_set, column_set, arithmetic_set};

    auto bound_set_clauses = BindMutationSetClauses(tab, set_clauses);
    ASSERT_EQ(bound_set_clauses.size(), 3U);
    EXPECT_EQ(bound_set_clauses[0].lhs.offset, 16U);
    EXPECT_EQ(bound_set_clauses[0].lhs.len, 4U);
    EXPECT_EQ(bound_set_clauses[0].lhs.type, TYPE_FLOAT);
    EXPECT_EQ(bound_set_clauses[0].rhs.offset, 0U);
    EXPECT_EQ(bound_set_clauses[0].rhs.len, 0U);
    EXPECT_EQ(bound_set_clauses[0].rhs.type, TYPE_INT);
    EXPECT_EQ(bound_set_clauses[1].lhs.offset, 8U);
    EXPECT_EQ(bound_set_clauses[1].lhs.len, 4U);
    EXPECT_EQ(bound_set_clauses[1].lhs.type, TYPE_INT);
    EXPECT_EQ(bound_set_clauses[1].rhs.offset, 4U);
    EXPECT_EQ(bound_set_clauses[1].rhs.len, 4U);
    EXPECT_EQ(bound_set_clauses[1].rhs.type, TYPE_INT);
    EXPECT_EQ(bound_set_clauses[2].lhs.offset, 12U);
    EXPECT_EQ(bound_set_clauses[2].lhs.len, 4U);
    EXPECT_EQ(bound_set_clauses[2].lhs.type, TYPE_INT);
    EXPECT_EQ(bound_set_clauses[2].rhs.offset, 4U);
    EXPECT_EQ(bound_set_clauses[2].rhs.len, 4U);
    EXPECT_EQ(bound_set_clauses[2].rhs.type, TYPE_INT);

    Rid rid;
    {
        SeqScanExecutor scan(sm_manager_.get(), "mutation_bind", {}, &ctx);
        scan.beginTuple();
        ASSERT_FALSE(scan.is_end());
        rid = scan.rid();
    }

    UpdateExecutor update(sm_manager_.get(), "mutation_bind", set_clauses, conditions, {rid}, &ctx);
    update.Next();
    auto record = sm_manager_->fhs_.at("mutation_bind")->get_record(rid, nullptr);
    ASSERT_NE(record, nullptr);
    EXPECT_EQ(*reinterpret_cast<int*>(record->data + 8), 7);
    EXPECT_EQ(*reinterpret_cast<int*>(record->data + 12), 13);
    EXPECT_FLOAT_EQ(read_float(record->data + 16), 42.0f);

    Condition delete_literal;
    delete_literal.lhs_col = {"mutation_bind", "id"};
    delete_literal.op = OP_EQ;
    delete_literal.is_rhs_val = true;
    delete_literal.rhs_val.set_int(1);
    Condition delete_column;
    delete_column.lhs_col = {"mutation_bind", "assigned"};
    delete_column.op = OP_EQ;
    delete_column.is_rhs_val = false;
    delete_column.rhs_col = {"mutation_bind", "source"};
    DeleteExecutor delete_exec(sm_manager_.get(), "mutation_bind", {delete_literal, delete_column}, {rid}, &ctx);
    delete_exec.Next();

    SeqScanExecutor after_delete(sm_manager_.get(), "mutation_bind", {}, &ctx);
    after_delete.beginTuple();
    EXPECT_TRUE(after_delete.is_end());
}

TEST_F(ExecutorTest, update_rejects_non_finite_float_result_without_writing_row) {
    setup_db();
    sm_manager_->create_table("finite_update", {{"amount", TYPE_FLOAT, 4}}, nullptr);

    Value maximum;
    maximum.set_float(std::numeric_limits<float>::max());
    char buf[4096] = {};
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    InsertExecutor insert(sm_manager_.get(), "finite_update", {maximum}, &ctx);
    insert.Next();

    Rid rid;
    {
        SeqScanExecutor scan(sm_manager_.get(), "finite_update", {}, &ctx);
        scan.beginTuple();
        ASSERT_FALSE(scan.is_end());
        rid = scan.rid();
    }

    SetClause add;
    add.lhs = {"finite_update", "amount"};
    add.rhs = maximum;
    add.is_self_ref = true;
    add.rhs_col = {"finite_update", "amount"};
    add.op = UpdateOp::SELF_ADD;
    UpdateExecutor update(sm_manager_.get(), "finite_update", {add}, {}, {rid}, &ctx);

    EXPECT_THROW((void)update.Next(), RMDBError);
    auto record = sm_manager_->fhs_.at("finite_update")->get_record(rid, nullptr);
    ASSERT_NE(record, nullptr);
    EXPECT_FLOAT_EQ(read_float(record->data), std::numeric_limits<float>::max());
}

TEST_F(ExecutorTest, read_committed_update_rechecks_latest_version) {
    setup_db();
    auto cols = make_int_cols({"id", "next_id"});
    sm_manager_->create_table("rc_lost_update", cols, nullptr);

    {
        std::vector<Value> vals;
        Value id;
        id.set_int(1);
        Value next_id;
        next_id.set_int(3020);
        vals.push_back(id);
        vals.push_back(next_id);

        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        InsertExecutor exec(sm_manager_.get(), "rc_lost_update", vals, &ctx);
        exec.Next();
    }

    Rid rid;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "rc_lost_update", {}, &ctx);
        scan.beginTuple();
        ASSERT_FALSE(scan.is_end());
        rid = scan.rid();
    }

    LockManager lock_manager;
    TransactionManager txn_manager(&lock_manager, sm_manager_.get());
    auto* txn1 = txn_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
    auto* txn2 = txn_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);

    char buf1[4096];
    int offset1 = 0;
    Context ctx1(&lock_manager, nullptr, txn1, buf1, &offset1, &txn_manager);
    char buf2[4096];
    int offset2 = 0;
    Context ctx2(&lock_manager, nullptr, txn2, buf2, &offset2, &txn_manager);

    auto* fh = sm_manager_->fhs_.at("rc_lost_update").get();
    auto txn2_old_rec = GetVisibleRecord(fh, rid, &ctx2);
    ASSERT_NE(txn2_old_rec, nullptr);
    ASSERT_EQ(*reinterpret_cast<int*>(txn2_old_rec->data + 4), 3020);

    SetClause set_next_to_3021;
    set_next_to_3021.lhs = {"rc_lost_update", "next_id"};
    Value next_3021;
    next_3021.set_int(3021);
    set_next_to_3021.rhs = next_3021;

    UpdateExecutor txn1_update(sm_manager_.get(), "rc_lost_update", {set_next_to_3021}, {}, {rid}, &ctx1);
    ASSERT_NO_THROW(txn1_update.Next());
    txn_manager.commit(txn1, nullptr);

    SetClause set_next_to_3022;
    set_next_to_3022.lhs = {"rc_lost_update", "next_id"};
    Value next_3022;
    next_3022.set_int(3022);
    set_next_to_3022.rhs = next_3022;
    ASSERT_NO_THROW({
        UpdateExecutor txn2_update(sm_manager_.get(), "rc_lost_update", {set_next_to_3022}, {}, {rid}, &ctx2);
        txn2_update.Next();
    });
    txn_manager.commit(txn2, nullptr);

    EXPECT_FALSE(txn2->get_state() == TransactionState::ABORTED)
        << "RC UPDATE should re-read the latest committed row after waiting for its writer";
    auto final_rec = fh->get_record(rid, nullptr);
    ASSERT_NE(final_rec, nullptr);
    EXPECT_EQ(*reinterpret_cast<int*>(final_rec->data + 4), 3022);
}

TEST_F(ExecutorTest, transaction_end_commands_clear_session_transaction_id) {
    setup_db();
    LockManager lock_manager;
    TransactionManager txn_manager(&lock_manager, sm_manager_.get());
    QlManager ql_manager(sm_manager_.get(), &txn_manager, nullptr);

    for (PlanTag tag : {T_Transaction_commit, T_Transaction_rollback, T_Transaction_abort}) {
        Transaction* txn = txn_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
        txn_id_t txn_id = txn->get_transaction_id();
        char buf[4096]{};
        int offset = 0;
        Context context(&lock_manager, nullptr, txn, buf, &offset, &txn_manager);
        OtherPlan plan(tag, "");

        ql_manager.run_cmd_utility(&plan, &txn_id, &context);

        EXPECT_EQ(txn_id, INVALID_TXN_ID);
        EXPECT_EQ(context.txn_, nullptr);
    }
}

TEST_F(ExecutorTest, select_from_prefers_current_view_over_next_record) {
    sm_manager_->output_file_enabled_ = false;
    int next_record_calls = 0;
    auto executor = std::make_unique<CountingExecutor>(
        std::vector<ColMeta>{make_test_col("t", "id", TYPE_INT, sizeof(int), 0)},
        std::vector<RmRecord>{make_join_record(10), make_join_record(20)}, &next_record_calls);

    char data_send[4096] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    QlManager ql_manager(sm_manager_.get(), nullptr, nullptr);

    ql_manager.select_from(std::move(executor), {"id"}, &context);

    EXPECT_EQ(next_record_calls, 0);
    EXPECT_EQ(std::string(data_send, offset), "+------------------+\n"
                                              "|               id |\n"
                                              "+------------------+\n"
                                              "|               10 |\n"
                                              "|               20 |\n"
                                              "+------------------+\n"
                                              "Total record(s): 2\n");
}

TEST_F(ExecutorTest, select_from_result_sink_enables_and_restores_ssi_read_tracking) {
    bool tracking_enabled_on_begin = false;
    auto executor = std::make_unique<CountingExecutor>(
        std::vector<ColMeta>{make_test_col("t", "id", TYPE_INT, sizeof(int), 0)},
        std::vector<RmRecord>{make_join_record(10)}, nullptr, &tracking_enabled_on_begin);

    char data_send[64] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    CountingResultSink sink;
    context.result_sink_ = &sink;
    executor->context_ = &context;
    QlManager ql_manager(sm_manager_.get(), nullptr, nullptr);

    ql_manager.select_from(std::move(executor), {"id"}, &context);

    EXPECT_TRUE(tracking_enabled_on_begin);
    EXPECT_FALSE(context.enable_ssi_read_tracking_);
    EXPECT_EQ(sink.begin_calls, 1);
    EXPECT_EQ(sink.row_count, 1);
    EXPECT_EQ(sink.column_count, 1U);
    EXPECT_EQ(sink.captions, std::vector<std::string>({"id"}));
}

TEST_F(ExecutorTest, select_from_result_sink_restores_ssi_tracking_when_executor_throws) {
    bool tracking_enabled_on_begin = false;
    auto executor =
        std::make_unique<CountingExecutor>(std::vector<ColMeta>{make_test_col("t", "id", TYPE_INT, sizeof(int), 0)},
                                           std::vector<RmRecord>{}, nullptr, &tracking_enabled_on_begin);
    executor->throw_on_begin_ = true;

    char data_send[64] = {};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, data_send, &offset);
    CountingResultSink sink;
    context.result_sink_ = &sink;
    executor->context_ = &context;
    QlManager ql_manager(sm_manager_.get(), nullptr, nullptr);

    EXPECT_THROW(ql_manager.select_from(std::move(executor), {"id"}, &context), std::runtime_error);
    EXPECT_TRUE(tracking_enabled_on_begin);
    EXPECT_FALSE(context.enable_ssi_read_tracking_);
}

TEST_F(ExecutorTest, select_from_result_sink_records_serializable_nonempty_and_empty_predicates) {
    setup_db();
    sm_manager_->create_table("sink_ssi", make_int_cols({"id"}), nullptr);
    insert_test_rows("sink_ssi", {1});

    LockManager lock_manager;
    TransactionManager txn_manager(&lock_manager, sm_manager_.get());
    QlManager ql_manager(sm_manager_.get(), &txn_manager, nullptr);

    for (int key : {1, 2}) {
        Transaction* txn = txn_manager.begin(nullptr, nullptr, IsolationLevel::SERIALIZABLE);
        char data_send[64] = {};
        int offset = 0;
        Context context(&lock_manager, nullptr, txn, data_send, &offset, &txn_manager);
        CountingResultSink sink;
        context.result_sink_ = &sink;

        Condition condition;
        condition.lhs_col = {"sink_ssi", "id"};
        condition.op = OP_EQ;
        condition.is_rhs_val = true;
        condition.rhs_val.set_int(key);
        auto executor = std::make_unique<SeqScanExecutor>(sm_manager_.get(), "sink_ssi",
                                                          std::vector<Condition>{condition}, &context);

        ql_manager.select_from(std::move(executor), {"id"}, &context);

        ASSERT_EQ(txn->predicate_reads_.size(), 1U);
        EXPECT_EQ(txn->predicate_reads_[0].tab_name_, "sink_ssi");
        EXPECT_EQ(sink.row_count, key == 1 ? 1 : 0);
        EXPECT_EQ(txn->read_rids_.empty(), key != 1);
        EXPECT_FALSE(context.enable_ssi_read_tracking_);
        txn_manager.abort(txn, nullptr);
    }
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

TEST_F(ExecutorTest, delete_with_condition_via_scan_rids) {
    setup_db();
    std::vector<ColDef> cols = {
        {"id", TYPE_INT, 4},
        {"val", TYPE_INT, 4},
    };
    sm_manager_->create_table("del_cond", cols, nullptr);
    for (int i = 1; i <= 3; i++) {
        std::vector<Value> vals;
        Value v1;
        v1.set_int(i);
        Value v2;
        v2.set_int(i * 10);
        vals.push_back(v1);
        vals.push_back(v2);
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        InsertExecutor exec(sm_manager_.get(), "del_cond", vals, &ctx);
        exec.Next();
    }

    Condition cond;
    cond.lhs_col = {"del_cond", "val"};
    cond.op = OP_GT;
    cond.is_rhs_val = true;
    Value rhs;
    rhs.set_int(15);
    cond.rhs_val = rhs;

    std::vector<Rid> rids;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "del_cond", {cond}, &ctx);
        for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
            rids.push_back(scan.rid());
        }
    }
    ASSERT_EQ(rids.size(), 2);

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    DeleteExecutor exec(sm_manager_.get(), "del_cond", {cond}, rids, &ctx);
    exec.Next();

    SeqScanExecutor scan(sm_manager_.get(), "del_cond", {}, &ctx);
    std::vector<int> remaining;
    for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
        auto rec = scan.Next();
        remaining.push_back(*reinterpret_cast<int*>(rec->data));
    }
    ASSERT_EQ(remaining.size(), 1);
    EXPECT_EQ(remaining[0], 1);
}

TEST_F(ExecutorTest, update_with_condition_via_scan_rids) {
    setup_db();
    std::vector<ColDef> cols = {
        {"id", TYPE_INT, 4},
        {"score", TYPE_INT, 4},
    };
    sm_manager_->create_table("upd_cond", cols, nullptr);
    for (int i = 1; i <= 3; i++) {
        std::vector<Value> vals;
        Value v1;
        v1.set_int(i);
        Value v2;
        v2.set_int(i * 10);
        vals.push_back(v1);
        vals.push_back(v2);
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        InsertExecutor exec(sm_manager_.get(), "upd_cond", vals, &ctx);
        exec.Next();
    }

    Condition cond;
    cond.lhs_col = {"upd_cond", "score"};
    cond.op = OP_GT;
    cond.is_rhs_val = true;
    Value rhs;
    rhs.set_int(15);
    cond.rhs_val = rhs;

    std::vector<Rid> rids;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "upd_cond", {cond}, &ctx);
        for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
            rids.push_back(scan.rid());
        }
    }
    ASSERT_EQ(rids.size(), 2);

    SetClause sc;
    sc.lhs = {"upd_cond", "score"};
    Value new_val;
    new_val.set_int(99);
    sc.rhs = new_val;

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    UpdateExecutor exec(sm_manager_.get(), "upd_cond", {sc}, {cond}, rids, &ctx);
    exec.Next();

    SeqScanExecutor scan(sm_manager_.get(), "upd_cond", {}, &ctx);
    std::vector<std::pair<int, int>> results;
    for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
        auto rec = scan.Next();
        int id = *reinterpret_cast<int*>(rec->data);
        int score = *reinterpret_cast<int*>(rec->data + 4);
        results.push_back({id, score});
    }
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0].second, 10);
    EXPECT_EQ(results[1].second, 99);
    EXPECT_EQ(results[2].second, 99);
}

TEST_F(ExecutorTest, update_int_to_float_promotion) {
    setup_db();
    std::vector<ColDef> cols = {
        {"id", TYPE_INT, 4},
        {"score", TYPE_FLOAT, 4},
    };
    sm_manager_->create_table("upd_promo", cols, nullptr);
    {
        std::vector<Value> vals;
        Value v1;
        v1.set_int(1);
        Value v2;
        v2.set_float(90.5f);
        vals.push_back(v1);
        vals.push_back(v2);
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        InsertExecutor exec(sm_manager_.get(), "upd_promo", vals, &ctx);
        exec.Next();
    }

    std::vector<Rid> rids;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "upd_promo", {}, &ctx);
        scan.beginTuple();
        scan.Next();
        rids.push_back(scan.rid());
    }

    SetClause sc;
    sc.lhs = {"upd_promo", "score"};
    Value new_val;
    new_val.set_int(90);
    sc.rhs = new_val;

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    UpdateExecutor exec(sm_manager_.get(), "upd_promo", {sc}, {}, rids, &ctx);
    exec.Next();

    SeqScanExecutor scan(sm_manager_.get(), "upd_promo", {}, &ctx);
    scan.beginTuple();
    auto rec = scan.Next();
    float score = read_float(rec->data + 4);
    EXPECT_FLOAT_EQ(score, 90.0f);
}

TEST_F(ExecutorTest, update_float_to_int_truncation) {
    setup_db();
    std::vector<ColDef> cols = {
        {"id", TYPE_INT, 4},
        {"score", TYPE_FLOAT, 4},
    };
    sm_manager_->create_table("upd_trunc", cols, nullptr);
    {
        std::vector<Value> vals;
        Value v1;
        v1.set_int(1);
        Value v2;
        v2.set_float(90.5f);
        vals.push_back(v1);
        vals.push_back(v2);
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        InsertExecutor exec(sm_manager_.get(), "upd_trunc", vals, &ctx);
        exec.Next();
    }

    std::vector<Rid> rids;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "upd_trunc", {}, &ctx);
        scan.beginTuple();
        scan.Next();
        rids.push_back(scan.rid());
    }

    SetClause sc;
    sc.lhs = {"upd_trunc", "id"};
    Value new_val;
    new_val.set_float(5.7f);
    sc.rhs = new_val;

    char buf[4096];
    int offset = 0;
    Context ctx(nullptr, nullptr, nullptr, buf, &offset);
    UpdateExecutor exec(sm_manager_.get(), "upd_trunc", {sc}, {}, rids, &ctx);
    exec.Next();

    SeqScanExecutor scan(sm_manager_.get(), "upd_trunc", {}, &ctx);
    scan.beginTuple();
    auto rec = scan.Next();
    int id = *reinterpret_cast<int*>(rec->data);
    EXPECT_EQ(id, 5);
}

TEST(ExecutorJoinFocusedTest, PrecomputesConditionMetadataWithAdjustedRightOffsets) {
    auto left = std::make_unique<CountingExecutor>(
        std::vector<ColMeta>{make_test_col("left_t", "id", TYPE_INT, sizeof(int), 0)},
        std::vector<RmRecord>{make_join_record(1)});
    auto right = std::make_unique<CountingExecutor>(
        std::vector<ColMeta>{make_test_col("right_t", "id", TYPE_INT, sizeof(int), 0)},
        std::vector<RmRecord>{make_join_record(1)});

    Condition cond;
    cond.lhs_col = {"left_t", "id"};
    cond.op = OP_EQ;
    cond.is_rhs_val = false;
    cond.rhs_col = {"right_t", "id"};

    NestedLoopJoinExecutor exec(std::move(left), std::move(right), {cond});

    ASSERT_EQ(exec.cols_.size(), 2U);
    ASSERT_EQ(exec.cols_map.size(), 2U);
    EXPECT_EQ(exec.cols_map.at("left_t.id")->offset, 0);
    EXPECT_EQ(exec.cols_map.at("right_t.id")->offset, sizeof(int));
    EXPECT_EQ(exec.fed_conds_.size(), 1U);
    EXPECT_EQ(exec.fed_conds_[0].lhs_col.tab_name, "left_t");
    EXPECT_EQ(exec.fed_conds_[0].rhs_col.tab_name, "right_t");
}

TEST(ExecutorJoinFocusedTest, ReusesCurrentLeftRecordAcrossMultipleRightMatches) {
    auto left = std::make_unique<CountingExecutor>(
        std::vector<ColMeta>{make_test_col("left_t", "id", TYPE_INT, sizeof(int), 0)},
        std::vector<RmRecord>{make_join_record(7)});
    auto right = std::make_unique<CountingExecutor>(
        std::vector<ColMeta>{make_test_col("right_t", "id", TYPE_INT, sizeof(int), 0)},
        std::vector<RmRecord>{make_join_record(7), make_join_record(7)});

    Condition cond;
    cond.lhs_col = {"left_t", "id"};
    cond.op = OP_EQ;
    cond.is_rhs_val = false;
    cond.rhs_col = {"right_t", "id"};

    NestedLoopJoinExecutor exec(std::move(left), std::move(right), {cond});

    exec.beginTuple();
    ASSERT_FALSE(exec.is_end());
    ASSERT_TRUE(exec.current_left_view_);
    EXPECT_EQ(exec.current_left_owned_, nullptr);
    ASSERT_NE(exec._buffered_record, nullptr);
    const char* first_left_ptr = exec.current_left_view_.data;

    auto first = exec.Next();
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(*reinterpret_cast<int*>(first->data), 7);
    EXPECT_EQ(*reinterpret_cast<int*>(first->data + sizeof(int)), 7);

    exec.nextTuple();
    ASSERT_FALSE(exec.is_end());
    ASSERT_TRUE(exec.current_left_view_);
    EXPECT_EQ(exec.current_left_owned_, nullptr);
    EXPECT_EQ(exec.current_left_view_.data, first_left_ptr);
    ASSERT_NE(exec._buffered_record, nullptr);

    auto second = exec.Next();
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(*reinterpret_cast<int*>(second->data), 7);
    EXPECT_EQ(*reinterpret_cast<int*>(second->data + sizeof(int)), 7);

    exec.nextTuple();
    EXPECT_TRUE(exec.is_end());
    EXPECT_FALSE(exec.current_left_view_);
}

TEST(ExecutorJoinFocusedTest, RightInputIsRewoundPerLeftRowNotPerOutputRow) {
    auto left = std::make_unique<CountingExecutor>(
        std::vector<ColMeta>{make_test_col("left_t", "id", TYPE_INT, sizeof(int), 0)},
        std::vector<RmRecord>{make_join_record(1), make_join_record(2)});
    auto right = std::make_unique<CountingExecutor>(
        std::vector<ColMeta>{make_test_col("right_t", "id", TYPE_INT, sizeof(int), 0)},
        std::vector<RmRecord>{make_join_record(1), make_join_record(1), make_join_record(2)});
    auto* left_ptr = left.get();
    auto* right_ptr = right.get();

    Condition cond;
    cond.lhs_col = {"left_t", "id"};
    cond.op = OP_EQ;
    cond.is_rhs_val = false;
    cond.rhs_col = {"right_t", "id"};

    NestedLoopJoinExecutor exec(std::move(left), std::move(right), {cond});

    int produced = 0;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        auto rec = exec.Next();
        ASSERT_NE(rec, nullptr);
        ++produced;
    }

    EXPECT_EQ(produced, 3);
    EXPECT_EQ(left_ptr->begin_calls_, 1);
    EXPECT_EQ(left_ptr->next_record_calls_, 0);
    EXPECT_EQ(left_ptr->next_calls_, 2);
    EXPECT_EQ(right_ptr->begin_calls_, 3);
    // The join consumes the right child's borrowed current view; only
    // executors without current() support should use the compatibility Next()
    // path.
    EXPECT_EQ(right_ptr->next_record_calls_, 0);
    EXPECT_EQ(right_ptr->next_calls_, 6);
}

// 回归测试：TPC-C 场景下每个写事务都会追加 undo log，曾因 txn_map 永不回收导致
// RSS 持续增长。这里反复执行「更新+提交」并断言 txn_map 不会无限膨胀，且 GC 后
// 旧事务被安全回收。同时验证版本链遍历在 GC 后仍正确（无 InternalError/崩溃）。
TEST_F(ExecutorTest, gc_reclaims_txn_map_after_committed_updates) {
    setup_db();
    auto cols = make_int_cols({"id", "val"});
    sm_manager_->create_table("gc_tab", cols, nullptr);

    // 插入一行初值
    {
        std::vector<Value> vals;
        Value id;
        id.set_int(1);
        Value val;
        val.set_int(0);
        vals.push_back(id);
        vals.push_back(val);
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        InsertExecutor exec(sm_manager_.get(), "gc_tab", vals, &ctx);
        exec.Next();
    }

    Rid rid;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "gc_tab", {}, &ctx);
        scan.beginTuple();
        ASSERT_FALSE(scan.is_end());
        rid = scan.rid();
    }

    LockManager lock_manager;
    TransactionManager txn_manager(&lock_manager, sm_manager_.get());

    SetClause set_val;
    set_val.lhs = {"gc_tab", "val"};
    Value v;
    v.set_int(0);
    set_val.rhs = v;

    // 反复提交大量更新事务，每个都会产生 undo log
    constexpr int N = 3000;
    for (int i = 0; i < N; ++i) {
        auto* txn = txn_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
        char buf[4096];
        int offset = 0;
        Context ctx(&lock_manager, nullptr, txn, buf, &offset, &txn_manager);
        UpdateExecutor upd(sm_manager_.get(), "gc_tab", {set_val}, {}, {rid}, &ctx);
        ASSERT_NO_THROW(upd.Next());
        txn_manager.commit(txn, nullptr);
    }

    // 提交后触发 GC，txn_map 必须被回收，不得随事务数线性增长
    txn_manager.GarbageCollection();
    size_t map_size = txn_manager.txn_map.size();
    EXPECT_LT(map_size, 100u) << "txn_map must be reclaimed by GC, got " << map_size << " entries after " << N
                              << " commits";

    // GC 后仍能正确读取（版本链遍历不应命中已回收事务导致 InternalError/崩溃）
    auto* fh = sm_manager_->fhs_.at("gc_tab").get();
    auto rec = fh->get_record(rid, nullptr);
    ASSERT_NE(rec, nullptr);
    EXPECT_EQ(*reinterpret_cast<int*>(rec->data + 4), 0);

    // 再做一次更新+读取，确认 GC 后系统状态正常
    {
        auto* txn = txn_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
        char buf[4096];
        int offset = 0;
        Context ctx(&lock_manager, nullptr, txn, buf, &offset, &txn_manager);
        UpdateExecutor upd(sm_manager_.get(), "gc_tab", {set_val}, {}, {rid}, &ctx);
        ASSERT_NO_THROW(upd.Next());
        txn_manager.commit(txn, nullptr);
    }
}

// 回归测试：并发场景下 GC 与版本链遍历的安全性。一个线程持续提交更新事务
// （驱动 GC 回收旧事务），另一线程并发扫描读，验证不会因 GC 回收了仍在被遍历
// 的事务而崩溃或抛 InternalError。
TEST_F(ExecutorTest, gc_concurrent_with_version_chain_reads_is_safe) {
    setup_db();
    auto cols = make_int_cols({"id", "val"});
    sm_manager_->create_table("gc_conc", cols, nullptr);

    {
        std::vector<Value> vals;
        Value id;
        id.set_int(1);
        Value val;
        val.set_int(0);
        vals.push_back(id);
        vals.push_back(val);
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        InsertExecutor exec(sm_manager_.get(), "gc_conc", vals, &ctx);
        exec.Next();
    }

    Rid rid;
    {
        char buf[4096];
        int offset = 0;
        Context ctx(nullptr, nullptr, nullptr, buf, &offset);
        SeqScanExecutor scan(sm_manager_.get(), "gc_conc", {}, &ctx);
        scan.beginTuple();
        ASSERT_FALSE(scan.is_end());
        rid = scan.rid();
    }

    LockManager lock_manager;
    TransactionManager txn_manager(&lock_manager, sm_manager_.get());

    SetClause set_val;
    set_val.lhs = {"gc_conc", "val"};
    Value v;
    v.set_int(0);
    set_val.rhs = v;

    std::atomic<bool> stop{false};
    std::atomic<int> errors{0};

    // 写线程：反复提交更新，驱动 GC
    auto writer = [&]() {
        for (int i = 0; i < 2000 && !stop.load(); ++i) {
            auto* txn = txn_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
            char buf[4096];
            int offset = 0;
            Context ctx(&lock_manager, nullptr, txn, buf, &offset, &txn_manager);
            UpdateExecutor upd(sm_manager_.get(), "gc_conc", {set_val}, {}, {rid}, &ctx);
            try {
                upd.Next();
                txn_manager.commit(txn, nullptr);
            } catch (...) {
                ++errors;
                txn_manager.abort(txn, nullptr);
            }
        }
    };

    // 读线程：并发扫描，触发版本链遍历
    auto reader = [&]() {
        for (int i = 0; i < 2000 && !stop.load(); ++i) {
            auto* txn = txn_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
            char buf[4096];
            int offset = 0;
            Context ctx(&lock_manager, nullptr, txn, buf, &offset, &txn_manager);
            try {
                SeqScanExecutor scan(sm_manager_.get(), "gc_conc", {}, &ctx);
                scan.beginTuple();
                while (!scan.is_end()) {
                    scan.Next();
                    scan.nextTuple();
                }
                txn_manager.commit(txn, nullptr);
            } catch (const TransactionAbortException&) {
                txn_manager.abort(txn, nullptr);
            } catch (...) {
                // InternalError 等非预期异常视为失败
                ++errors;
                txn_manager.abort(txn, nullptr);
            }
        }
    };

    std::thread w1(writer);
    std::thread r1(reader);
    std::thread r2(reader);
    w1.join();
    r1.join();
    r2.join();

    EXPECT_EQ(errors.load(), 0) << "concurrent GC + version-chain reads must not crash or throw InternalError";
    txn_manager.GarbageCollection();
    EXPECT_LT(txn_manager.txn_map.size(), 100u) << "txn_map should be reclaimed after concurrent run";
}

// RC aborts are common on TPC-C hot rows. A rolled-back insert frees a slot
// while other transactions are allocating slots from the same table, so this
// exercises the free-page chain and bitmap updates under that exact mix.
TEST_F(ExecutorTest, rc_concurrent_insert_rollback_preserves_committed_rows) {
    setup_db();
    sm_manager_->create_table("rc_insert_rollback", make_int_cols({"id"}), nullptr);

    LockManager lock_manager;
    TransactionManager txn_manager(&lock_manager, sm_manager_.get());
    std::atomic<int> committed{0};
    std::atomic<int> errors{0};

    constexpr int kWorkers = 8;
    constexpr int kTransactionsPerWorker = 400;
    std::vector<std::thread> workers;
    workers.reserve(kWorkers);
    for (int worker = 0; worker < kWorkers; ++worker) {
        workers.emplace_back([&, worker] {
            for (int i = 0; i < kTransactionsPerWorker; ++i) {
                auto* txn = txn_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
                char output[4096]{};
                int offset = 0;
                Context context(&lock_manager, nullptr, txn, output, &offset, &txn_manager);
                Value value;
                value.set_int(worker * kTransactionsPerWorker + i);
                try {
                    InsertExecutor insert(sm_manager_.get(), "rc_insert_rollback", {value}, &context);
                    insert.Next();
                    if ((i & 1) == 0) {
                        txn_manager.commit(txn, nullptr);
                        ++committed;
                    } else {
                        txn_manager.abort(txn, nullptr);
                    }
                } catch (...) {
                    ++errors;
                    txn_manager.abort(txn, nullptr);
                }
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    EXPECT_EQ(errors.load(), 0);

    char output[4096]{};
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, output, &offset);
    SeqScanExecutor scan(sm_manager_.get(), "rc_insert_rollback", {}, &context);
    std::unordered_set<int> ids;
    for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
        auto record = scan.Next();
        ASSERT_NE(record, nullptr);
        ids.insert(*reinterpret_cast<int*>(record->data));
    }
    EXPECT_EQ(ids.size(), static_cast<size_t>(committed.load()));
}
