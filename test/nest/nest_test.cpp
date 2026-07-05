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
#include "portal.h"
using namespace rmdb;
#undef private

#include <chrono>
#include <memory>
#include <string>
#include <sstream>

#include "gtest/gtest.h"
#include "common/config.h"
#include "index/ix.h"
#include "pager/pager.h"
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
    std::unique_ptr<rmdb::pager::Pager> pager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SchemaManager> schema_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> txn_manager_;
    std::unique_ptr<Planner> planner_;
    std::unique_ptr<Analyze> analyze_;
    std::unique_ptr<rmdb::access::TableWriteService> write_service_;
    std::unique_ptr<Portal> portal_;
    bool db_opened_ = false;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        pager_ = std::make_unique<rmdb::pager::Pager>(buffer_pool_manager_.get(), nullptr);
        buffer_pool_manager_->set_wal_guard(pager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get(), pager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get(), pager_.get());
        schema_manager_ = std::make_unique<SchemaManager>(disk_manager_.get(), buffer_pool_manager_.get(),
                                                          rm_manager_.get(), ix_manager_.get(), pager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get(), schema_manager_.get());
        planner_ = std::make_unique<Planner>(schema_manager_.get());
        analyze_ = std::make_unique<Analyze>(schema_manager_.get());
        write_service_ = std::make_unique<rmdb::access::TableWriteService>(schema_manager_.get(), lock_manager_.get(),
                                                                           nullptr, txn_manager_.get());
        portal_ = std::make_unique<Portal>(schema_manager_.get(), write_service_.get());

        if (schema_manager_->is_dir(TEST_DB_NAME)) {
            schema_manager_->drop_db(TEST_DB_NAME);
        }
        schema_manager_->create_db(TEST_DB_NAME);
        schema_manager_->open_db(TEST_DB_NAME);
        db_opened_ = true;
    }

    void TearDown() override {
        if (db_opened_) {
            schema_manager_->close_db();
            db_opened_ = false;
        }
        if (schema_manager_->is_dir(TEST_DB_NAME)) {
            schema_manager_->drop_db(TEST_DB_NAME);
        }
    }

    std::unique_ptr<rmdb::parser::ast::TreeNode> parse_sql(const std::string& sql) {
        std::string sql_with_semi = sql.back() == ';' ? sql : sql + ";";
        return rmdb::parser::ast::parse_sql(sql_with_semi);
    }

    void execute(const std::string& sql) {
        auto parse = parse_sql(sql);
        auto query = analyze_->do_analyze(std::move(parse));
        auto plan = planner_->do_planner(std::move(query), nullptr);
        auto portal_stmt = portal_->start(std::move(plan), nullptr);
        QlManager ql_mgr(schema_manager_.get(), txn_manager_.get(), planner_.get(), nullptr);
        txn_id_t txn = 0;
        portal_->run(std::move(portal_stmt), &ql_mgr, &txn, nullptr);
    }

    std::vector<std::vector<std::string>> select(const std::string& sql) {
        auto parse = parse_sql(sql);
        auto query = analyze_->do_analyze(std::move(parse));
        auto plan = planner_->do_planner(std::move(query), nullptr);
        auto portal_stmt = portal_->start(std::move(plan), nullptr);

        std::vector<std::vector<std::string>> rows;
        if (portal_stmt->tag == PORTAL_ONE_SELECT) {
            for (portal_stmt->root->beginTuple(); !portal_stmt->root->is_end(); portal_stmt->root->nextTuple()) {
                auto rec = portal_stmt->root->Next();
                if (rec == nullptr)
                    break;
                std::vector<std::string> row;
                const auto& cols = portal_stmt->root->cols();
                for (const auto& col : cols) {
                    std::string val;
                    const char* data = rec->data + col.offset;
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
        }
        return rows;
    }

    std::string explain_analyze(const std::string& sql) {
        auto parse = parse_sql(sql);
        auto query = analyze_->do_analyze(std::move(parse));
        auto plan = planner_->do_planner(std::move(query), nullptr);
        auto portal_stmt = portal_->start(std::move(plan), nullptr);

        if (portal_stmt->tag == PORTAL_EXPLAIN_ANALYZE) {
            for (portal_stmt->root->beginTuple(); !portal_stmt->root->is_end(); portal_stmt->root->nextTuple()) {
                (void)portal_stmt->root->Next();
            }
            auto* dml = static_cast<DMLPlan*>(portal_stmt->plan.get());
            std::ostringstream out;
            Portal::render_explain_plan(dml->subplan_.get(), 0, out);
            return out.str();
        }
        return "";
    }
};

// ============================================================
// LargeDataPartialMatch — NLJ vs INLJ performance benchmark
// (Nest.md §2 test4: 两表大数据部分匹配 + 性能比较)
// (other NLJ/INLJ correctness tests moved to test/e2e/slt/nest_nlj_inlj.slt)
// ============================================================
TEST_F(NestTest, LargeDataPartialMatch) {
    schema_manager_->create_table("large_left", {{"id", TYPE_INT, 4}, {"val", TYPE_INT, 4}}, nullptr);
    schema_manager_->create_table("large_right", {{"id", TYPE_INT, 4}, {"ref", TYPE_INT, 4}}, nullptr);

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
