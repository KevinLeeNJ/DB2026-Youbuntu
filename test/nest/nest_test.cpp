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

#include <chrono>
#include <memory>
#include <string>
#include <sstream>

#include "gtest/gtest.h"
#include "common/config.h"
#include "common/context.h"
#include "execution/execution_manager.h"
#include "index/ix.h"
#include "record/rm.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "optimizer/planner.h"
#include "analyze/analyze.h"
#include "transaction/transaction_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "parser/parser.h"

namespace {

const std::string TEST_DB_NAME = "nest_test_db"; // unique per test via SetUp/TearDown isolation

class NestTest : public ::testing::Test {
protected:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> txn_manager_;
    std::unique_ptr<Planner> planner_;
    std::unique_ptr<Analyze> analyze_;
    bool db_opened_ = false;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_manager_.get(),
                                                  ix_manager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get(), sm_manager_.get());
        planner_ = std::make_unique<Planner>(sm_manager_.get());
        analyze_ = std::make_unique<Analyze>(sm_manager_.get());

        if (sm_manager_->is_dir(TEST_DB_NAME)) {
            sm_manager_->drop_db(TEST_DB_NAME);
        }
        sm_manager_->create_db(TEST_DB_NAME);
        sm_manager_->open_db(TEST_DB_NAME);
        db_opened_ = true;
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

    std::unique_ptr<ast::TreeNode> parse_sql(const std::string& sql) {
        std::string sql_with_semi = sql.back() == ';' ? sql : sql + ";";
        return ast::parse_sql(sql_with_semi);
    }

    void execute(const std::string& sql) {
        auto parse = parse_sql(sql);
        auto query = analyze_->do_analyze(std::move(parse));
        auto plan = planner_->do_planner(std::move(query), nullptr);
        QlManager ql_mgr(sm_manager_.get(), txn_manager_.get());
        txn_id_t txn = 0;
        Context context(lock_manager_.get(), nullptr, nullptr, txn_manager_.get());
        ExecutionOutput output;
        ql_mgr.execute(std::move(plan), &txn, &context, &output);
    }

    std::vector<std::vector<std::string>> select(const std::string& sql) {
        auto parse = parse_sql(sql);
        auto query = analyze_->do_analyze(std::move(parse));
        auto plan = planner_->do_planner(std::move(query), nullptr);
        class ResultSink final : public QueryResultSink {
        public:
            void begin_query(const std::vector<ColMeta>& result_columns, const std::vector<std::string>&) override {
                columns = result_columns;
            }
            void append_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) override {
                rows.emplace_back(data, data + size);
                this->columns = columns;
            }
            std::vector<ColMeta> columns;
            std::vector<std::vector<char>> rows;
        } sink;
        Context context(lock_manager_.get(), nullptr, nullptr, txn_manager_.get());
        ExecutionOutput output{nullptr, nullptr, false, &sink};
        QlManager ql_mgr(sm_manager_.get(), txn_manager_.get());
        txn_id_t txn = 0;
        ql_mgr.execute(std::move(plan), &txn, &context, &output);

        std::vector<std::vector<std::string>> rows;
        for (const auto& record : sink.rows) {
            std::vector<std::string> row;
            for (const auto& col : sink.columns) {
                std::string val;
                const char* data = record.data() + col.offset;
                switch (col.type) {
                case TYPE_INT:
                    val = std::to_string(*reinterpret_cast<const int*>(data));
                    break;
                case TYPE_FLOAT: {
                    std::ostringstream tmp;
                    tmp << *reinterpret_cast<const float*>(data);
                    val = tmp.str();
                    break;
                }
                case TYPE_STRING:
                case TYPE_DATETIME:
                    val = std::string(data, strnlen(data, col.len));
                    break;
                }
                row.push_back(val);
            }
            rows.push_back(std::move(row));
        }
        return rows;
    }

    std::string explain_analyze(const std::string& sql) {
        auto parse = parse_sql(sql);
        auto query = analyze_->do_analyze(std::move(parse));
        auto plan = planner_->do_planner(std::move(query), nullptr);
        char data_send[BUFFER_LENGTH] = {};
        int offset = 0;
        Context context(lock_manager_.get(), nullptr, nullptr, txn_manager_.get());
        ExecutionOutput output{data_send, &offset};
        QlManager ql_mgr(sm_manager_.get(), txn_manager_.get());
        txn_id_t txn = 0;
        ql_mgr.execute(std::move(plan), &txn, &context, &output);
        return {data_send, static_cast<std::size_t>(offset)};
    }
};

// ============================================================
// LargeDataPartialMatch — NLJ vs INLJ performance benchmark
// (Nest.md §2 test4: 两表大数据部分匹配 + 性能比较)
// (other NLJ/INLJ correctness tests moved to test/e2e/slt/nest_nlj_inlj.slt)
// ============================================================
TEST_F(NestTest, LargeDataPartialMatch) {
    sm_manager_->create_table("large_left", {{"id", TYPE_INT, 4}, {"val", TYPE_INT, 4}}, nullptr);
    sm_manager_->create_table("large_right", {{"id", TYPE_INT, 4}, {"ref", TYPE_INT, 4}}, nullptr);

    // Partial match: left id=0..999, right ref=0,2,4,...,1998 (unique)
    // Only even left ids match → 500 out of 1000 rows
    constexpr int N = 1000;
    constexpr int expected_matches = N / 2;
    for (int i = 0; i < N; ++i) {
        execute("INSERT INTO large_left VALUES (" + std::to_string(i) + ", " + std::to_string(i * 10) + ")");
        execute("INSERT INTO large_right VALUES (" + std::to_string(i) + ", " + std::to_string(i * 2) + ")");
    }

    // NLJ phase — SELECT
    auto t1_start = std::chrono::high_resolution_clock::now();
    auto rows = select(
        "SELECT large_left.id, large_right.id FROM large_left JOIN large_right ON large_left.id = large_right.ref");
    auto t1_end = std::chrono::high_resolution_clock::now();
    double t_nl = std::chrono::duration<double>(t1_end - t1_start).count();
    EXPECT_EQ(rows.size(), expected_matches);

    // NLJ phase — EXPLAIN ANALYZE (right SeqScan rows = N*N)
    std::string nlj_explain = explain_analyze("EXPLAIN ANALYZE SELECT large_left.id, large_right.id FROM large_left "
                                              "JOIN large_right ON large_left.id = large_right.ref");
    EXPECT_NE(nlj_explain.find("SeqScan"), std::string::npos);
    EXPECT_NE(nlj_explain.find("rows=" + std::to_string(N * N)), std::string::npos)
        << "Right SeqScan rows should be " << N * N << ", got:\n"
        << nlj_explain;

    // INLJ phase — create index on inner table join column (unique)
    execute("CREATE INDEX large_right(ref)");

    // INLJ phase — SELECT
    auto t2_start = std::chrono::high_resolution_clock::now();
    rows = select(
        "SELECT large_left.id, large_right.id FROM large_left JOIN large_right ON large_left.id = large_right.ref");
    auto t2_end = std::chrono::high_resolution_clock::now();
    double t_inl = std::chrono::duration<double>(t2_end - t2_start).count();
    EXPECT_EQ(rows.size(), expected_matches);

    // Performance check: INLJ must be at least 2x faster than NLJ
    double ratio = t_inl / t_nl;
    EXPECT_LT(ratio, 0.5) << "INLJ/NLJ ratio = " << ratio << ", expected < 0.5";

    // INLJ phase — EXPLAIN ANALYZE
    std::string inlj_explain = explain_analyze("EXPLAIN ANALYZE SELECT large_left.id, large_right.id FROM large_left "
                                               "JOIN large_right ON large_left.id = large_right.ref");
    EXPECT_NE(inlj_explain.find("IndexScan"), std::string::npos);
    EXPECT_NE(inlj_explain.find("using_index=(ref)"), std::string::npos);
    EXPECT_NE(inlj_explain.find("rows=" + std::to_string(expected_matches)), std::string::npos)
        << "IndexScan rows should be " << expected_matches << ", got:\n"
        << inlj_explain;
}

} // namespace
