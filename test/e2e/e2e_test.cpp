/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include "analyze/analyze.h"
#include "common/config.h"
#include "common/context.h"
#include "errors.h"
#include "execution/execution_manager.h"
#include "gtest/gtest.h"
#include "index/ix_manager.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parser.h"
#include "portal.h"
#include "record/rm_manager.h"
#include "record/rm_scan.h"
#include "recovery/log_manager.h"
#include "recovery/log_recovery.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

// =============================================================================
// EmbeddedDB: in-process database engine that bypasses the network layer
// =============================================================================

class EmbeddedDB {
public:
    explicit EmbeddedDB(const std::string& db_name, bool reset_database = true, bool cleanup_on_destroy = true)
        : db_name_(db_name), cleanup_on_destroy_(cleanup_on_destroy) {
        // Save original CWD for teardown
        char cwd_buf[1024];
        getcwd(cwd_buf, sizeof(cwd_buf));
        original_cwd_ = cwd_buf;

        if (reset_database) {
            // Clean up any leftover from previous runs (use absolute path)
            std::string cmd = "rm -rf " + original_cwd_ + "/" + db_name_;
            system(cmd.c_str());
        }

        // 构建全局所需的管理器对象（同 src/rmdb.cpp）
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_manager_.get(),
                                                  ix_manager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get(), sm_manager_.get());
        planner_ = std::make_unique<Planner>(sm_manager_.get());
        optimizer_ = std::make_unique<Optimizer>(sm_manager_.get(), planner_.get());
        ql_manager_ =
            std::make_unique<QlManager>(sm_manager_.get(), txn_manager_.get(), static_cast<Planner*>(nullptr));
        log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
        recovery_ =
            std::make_unique<RecoveryManager>(disk_manager_.get(), buffer_pool_manager_.get(), sm_manager_.get());
        portal_ = std::make_unique<Portal>(sm_manager_.get());
        analyze_ = std::make_unique<Analyze>(sm_manager_.get());

        // Create and open the database
        if (!sm_manager_->is_dir(db_name_)) {
            sm_manager_->create_db(db_name_);
        }
        sm_manager_->open_db(db_name_);

        log_manager_->initialize_from_existing_log();
        buffer_pool_manager_->set_log_manager(log_manager_.get());

        // ARIES recovery
        recovery_->analyze();
        recovery_->redo();
        recovery_->undo();
        // 与 rmdb.cpp 的启动顺序保持一致：commit_ts_ 持久化在数据页里，计数器必须在任何
        // 事务开始之前抬到高于任何已持久化的 commit_ts_，否则上一世提交的行会被判成
        // “来自未来”而不可见。缺了这一步，本 harness 就不再是生产启动路径的镜像。
        txn_manager_->seed_counters_after_recovery(recovery_->get_recovered_next_timestamp(),
                                                   recovery_->get_recovered_next_txn_id());
    }

    ~EmbeddedDB() {
        // close_db() does chdir("..") internally — do NOT chdir before it.
        try {
            sm_manager_->close_db();
        } catch (...) {
            // If close_db() throws, we may be stuck inside the db dir.
            // Force-restore CWD so cleanup works.
            chdir(original_cwd_.c_str());
        }
        // Now CWD is back at original_cwd_ (one level above the db dir)
        if (cleanup_on_destroy_) {
            std::string cmd = "rm -rf " + original_cwd_ + "/" + db_name_;
            system(cmd.c_str());
        }
    }

    /// Execute a single SQL statement (must include trailing ';').
    /// Returns the captured text output. Throws RMDBError on failure.
    std::string exec_sql(const std::string& sql, bool* output_file_enabled = nullptr) {
        char data_send[BUFFER_LENGTH];
        memset(data_send, 0, BUFFER_LENGTH);
        int offset = 0;

        Context context(lock_manager_.get(), log_manager_.get(), nullptr, data_send, &offset, txn_manager_.get());
        context.output_file_enabled_ = output_file_enabled;

        // Parse
        std::unique_ptr<ast::TreeNode> parse_tree;
        try {
            parse_tree = ast::parse_sql(sql);
        } catch (...) {
            abort_failed_statement(&context);
            throw RMDBError("Parse error for: " + sql);
        }
        if (parse_tree == nullptr) {
            finish_statement(&context);
            return ""; // EXIT or EOF
        }
        bool is_checkpoint = parse_tree->type == ast::AstType::StaticCheckpoint;
        bool is_load = parse_tree->type == ast::AstType::LoadStmt;
        if (!is_checkpoint && !is_load) {
            set_transaction(&context);
        }

        // Analyze → Optimize → Portal → Execute
        try {
            std::unique_ptr<Query> query = analyze_->do_analyze(std::move(parse_tree));
            std::unique_ptr<Plan> plan = optimizer_->plan_query(std::move(query), &context);
            std::unique_ptr<PortalStmt> portal_stmt = portal_->start(std::move(plan), &context);
            portal_->run(std::move(portal_stmt), ql_manager_.get(), &txn_id_, &context);
            portal_->drop();
            finish_statement(&context);
        } catch (...) {
            abort_failed_statement(&context);
            throw;
        }

        // Capture output from data_send buffer
        std::string result(data_send, offset);
        return result;
    }

    /// Execute SQL that is expected to throw.
    /// Returns the exception message. Throws std::runtime_error if no exception.
    std::string exec_sql_expect_error(const std::string& sql) {
        try {
            exec_sql(sql);
        } catch (RMDBError& e) {
            return std::string(e.what());
        } catch (TransactionAbortException& e) {
            return std::string(e.GetInfo());
        }
        throw std::runtime_error("Expected exception but none thrown for: " + sql);
    }

    /// Remove output.txt if it exists (to prevent cross-test contamination)
    void clean_output_txt() {
        // output.txt is written in the db directory (cwd after open_db)
        std::remove("output.txt");
    }

    RmFileHdr file_header(const std::string& table_name) {
        return sm_manager_->fhs_.at(table_name)->get_file_hdr();
    }

    TupleMeta first_tuple_meta(const std::string& table_name) {
        auto* fh = sm_manager_->fhs_.at(table_name).get();
        RmScan scan(fh);
        if (scan.is_end()) {
            throw InternalError("table has no tuple: " + table_name);
        }
        return fh->get_tuple_meta(scan.rid());
    }

private:
    void set_transaction(Context* context) {
        context->txn_ = txn_id_ == INVALID_TXN_ID ? nullptr : txn_manager_->get_transaction(txn_id_);
        if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
            context->txn_->get_state() == TransactionState::ABORTED) {
            context->txn_ = txn_manager_->begin(nullptr, context->log_mgr_, context->isolation_level_);
            txn_id_ = context->txn_->get_transaction_id();
            context->txn_->set_txn_mode(false);
        }
        txn_manager_->BeginStatement(context->txn_);
    }

    void finish_statement(Context* context) {
        if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
            context->txn_->get_state() != TransactionState::COMMITTED &&
            context->txn_->get_state() != TransactionState::ABORTED) {
            txn_manager_->commit(context->txn_, context->log_mgr_);
        }
        context->txn_ = nullptr;
    }

    void abort_failed_statement(Context* context) {
        Transaction* txn = context->txn_;
        if (txn == nullptr && txn_id_ != INVALID_TXN_ID) {
            txn = txn_manager_->get_transaction(txn_id_);
        }
        if (txn != nullptr && txn->get_state() != TransactionState::COMMITTED &&
            txn->get_state() != TransactionState::ABORTED) {
            txn_manager_->abort(txn, context->log_mgr_);
        }
        txn_id_ = INVALID_TXN_ID;
        context->txn_ = nullptr;
    }

    std::string db_name_;
    std::string original_cwd_;
    bool cleanup_on_destroy_{true};
    txn_id_t txn_id_{INVALID_TXN_ID};
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> txn_manager_;
    std::unique_ptr<Planner> planner_;
    std::unique_ptr<Optimizer> optimizer_;
    std::unique_ptr<QlManager> ql_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<RecoveryManager> recovery_;
    std::unique_ptr<Portal> portal_;
    std::unique_ptr<Analyze> analyze_;
};

// =============================================================================
// .slt file parser & test runner
// =============================================================================

enum class SltDirective {
    STATEMENT_OK,
    STATEMENT_ERROR,
    QUERY,
};

struct SltTestCase {
    SltDirective directive;
    std::string sql;
    std::string expected; // for QUERY: expected output; for STATEMENT_OK/ERROR: unused
    int line_no;          // line number in .slt file (for error reporting)
};

/// Parse an .slt file and return the list of test cases.
static std::vector<SltTestCase> parse_slt_file(const std::string& path) {
    std::vector<SltTestCase> cases;
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open .slt file: " + path);
    }

    std::string line;
    int line_no = 0;
    SltDirective current_directive = SltDirective::STATEMENT_OK;
    std::string current_sql;
    std::string current_expected;
    bool in_result = false; // true when reading expected output after ----
    int start_line = 0;

    auto flush_case = [&]() {
        if (!current_sql.empty()) {
            // Trim trailing whitespace from SQL
            while (!current_sql.empty() && (current_sql.back() == '\n' || current_sql.back() == ' ')) {
                current_sql.pop_back();
            }
            cases.push_back({current_directive, current_sql, current_expected, start_line});
        }
        current_sql.clear();
        current_expected.clear();
        in_result = false;
    };

    while (std::getline(file, line)) {
        line_no++;

        // Strip trailing \r (Windows line endings)
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        // Blank lines terminate the current result block and flush the case
        if (line.empty()) {
            if (in_result) {
                // End of result block — flush the query case
                flush_case();
            } else {
                flush_case();
            }
            continue;
        }

        // Comments (outside result blocks only)
        if (line[0] == '#' && !in_result) {
            continue;
        }

        // Directives (only recognized outside result blocks)
        if (!in_result) {
            if (line == "statement ok") {
                flush_case();
                current_directive = SltDirective::STATEMENT_OK;
                start_line = line_no;
                continue;
            } else if (line == "statement error") {
                flush_case();
                current_directive = SltDirective::STATEMENT_ERROR;
                start_line = line_no;
                continue;
            } else if (line.rfind("query", 0) == 0) {
                flush_case();
                current_directive = SltDirective::QUERY;
                start_line = line_no;
                continue;
            } else if (line == "----") {
                in_result = true;
                continue;
            }
        }

        if (in_result) {
            if (!current_expected.empty()) {
                current_expected += "\n";
            }
            current_expected += line;
        } else {
            if (!current_sql.empty()) {
                current_sql += "\n";
            }
            current_sql += line;
        }
    }

    flush_case();
    return cases;
}

// =============================================================================
// E2E test fixture
// =============================================================================

class E2ETest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use unique DB name per test to allow parallel execution via ctest
        const auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        std::string db_name = std::string("e2e_") + test_info->name();
        db_ = std::make_unique<EmbeddedDB>(db_name);
    }

    void TearDown() override {
        db_.reset();
    }

    std::unique_ptr<EmbeddedDB> db_;
};

// =============================================================================
// Path resolution for .slt files
// =============================================================================

/// Try multiple base paths to find a .slt file (works from any CWD via RMDB_SRC_DIR).
static std::string resolve_slt_path(const std::string& relative_path) {
    // Use the compile-time source directory as the definitive base
#ifdef RMDB_SRC_DIR
    std::string from_src = std::string(RMDB_SRC_DIR) + "/" + relative_path;
    std::ifstream test(from_src);
    if (test.good()) {
        return from_src;
    }
#endif
    // Fallback: try relative paths from common CWD locations
    const char* candidates[] = {
        "../../../", // from build/test/<db>/
        "../../",    // from build/<db>/ or project_root/<db>/
        "../",       // from build/ or project_root/<db>/
        "",          // from project root
    };
    for (const auto* base : candidates) {
        std::string full = std::string(base) + relative_path;
        std::ifstream test2(full);
        if (test2.good()) {
            return full;
        }
    }
    // Last resort: return as-is
    return relative_path;
}

// =============================================================================
// Parameterized test: one test per .slt file
// =============================================================================

class SltFileTest : public E2ETest {
protected:
    void run_slt_file(const std::string& slt_filename) {
        std::string slt_path = resolve_slt_path("test/e2e/slt/" + slt_filename);
        auto cases = parse_slt_file(slt_path);

        for (size_t i = 0; i < cases.size(); i++) {
            const auto& tc = cases[i];
            SCOPED_TRACE("Case #" + std::to_string(i + 1) + " at line " + std::to_string(tc.line_no) + ": " +
                         tc.sql.substr(0, 80));

            switch (tc.directive) {
            case SltDirective::STATEMENT_OK: {
                EXPECT_NO_THROW({ db_->exec_sql(tc.sql); }) << "Expected success but got exception";
                db_->clean_output_txt();
                break;
            }
            case SltDirective::STATEMENT_ERROR: {
                // `statement error` only asserts that the statement fails. A retryable conflict is
                // reported as TransactionAbortException instead of RMDBError, and both are failures.
                bool failed = false;
                try {
                    db_->exec_sql(tc.sql);
                } catch (const RMDBError&) {
                    failed = true;
                } catch (const TransactionAbortException&) {
                    failed = true;
                }
                EXPECT_TRUE(failed) << "Expected the statement to fail but it succeeded";
                db_->clean_output_txt();
                break;
            }
            case SltDirective::QUERY: {
                std::string output;
                EXPECT_NO_THROW({ output = db_->exec_sql(tc.sql); }) << "Query threw unexpected exception";
                // Strip trailing newline from both sides for consistent comparison
                while (!output.empty() && output.back() == '\n')
                    output.pop_back();
                std::string expected = tc.expected;
                while (!expected.empty() && expected.back() == '\n')
                    expected.pop_back();
                EXPECT_EQ(output, expected) << "Query output mismatch";
                db_->clean_output_txt();
                break;
            }
            }
        }
    }
};

// ---- Discover and run every .slt file under test/e2e/slt/ ----

TEST_F(SltFileTest, BasicCreate) {
    run_slt_file("basic_create.slt");
}

TEST_F(SltFileTest, BasicInsertSelect) {
    run_slt_file("basic_insert_select.slt");
}

TEST_F(SltFileTest, BasicUpdateDelete) {
    run_slt_file("basic_update_delete.slt");
}

TEST_F(SltFileTest, BasicIndex) {
    run_slt_file("basic_index.slt");
}

TEST_F(SltFileTest, PointDml) {
    run_slt_file("point_dml.slt");
}

TEST_F(SltFileTest, Transaction) {
    run_slt_file("transaction.slt");
}

TEST_F(SltFileTest, Join) {
    run_slt_file("join.slt");
}

TEST_F(SltFileTest, IndexLarge) {
    run_slt_file("index_large.slt");
}

TEST_F(SltFileTest, Errors) {
    run_slt_file("errors.slt");
}

TEST_F(SltFileTest, Aggregate) {
    run_slt_file("aggregate.slt");
}

TEST_F(SltFileTest, AggregateJoin) {
    run_slt_file("aggregate_join.slt");
}

TEST_F(SltFileTest, StringMinMax) {
    run_slt_file("string_min_max.slt");
}

TEST_F(SltFileTest, NullBasic) {
    run_slt_file("null_basic.slt");
}

TEST_F(SltFileTest, NullAggregate) {
    run_slt_file("null_aggregate.slt");
}

TEST_F(SltFileTest, NullJoinSort) {
    run_slt_file("null_join_sort.slt");
}

TEST_F(SltFileTest, OutputFile) {
    run_slt_file("output_file.slt");
}

TEST_F(SltFileTest, Union) {
    run_slt_file("union.slt");
}

TEST_F(SltFileTest, QueryOptimize) {
    run_slt_file("query_optimize.slt");
}

TEST_F(SltFileTest, OrderByNonProjected) {
    run_slt_file("order_by_nonprojected.slt");
}

TEST_F(SltFileTest, NestNljInlj) {
    run_slt_file("nest_nlj_inlj.slt");
}

TEST_F(SltFileTest, Checkpoint) {
    run_slt_file("checkpoint.slt");
}

TEST_F(SltFileTest, ArithConstant) {
    run_slt_file("arith_constant.slt");
}

TEST_F(SltFileTest, PerformanceLoadQuery) {
    run_slt_file("performance_load_query.slt");
}

TEST_F(SltFileTest, UpdateIndexOptimization) {
    run_slt_file("update_index_optimization.slt");
}

TEST_F(SltFileTest, InsertUniqueIndexRegression) {
    run_slt_file("insert_unique_index_regression.slt");
}

TEST_F(SltFileTest, TransactionRepeatedUpdateMerge) {
    run_slt_file("transaction_repeated_update_merge.slt");
}

TEST_F(E2ETest, HeapTableAllowsDuplicateRows) {
    ASSERT_NO_THROW(db_->exec_sql("create table dup_heap (id int, val int);"));
    ASSERT_NO_THROW(db_->exec_sql("insert into dup_heap values(1, 10);"));
    ASSERT_NO_THROW(db_->exec_sql("insert into dup_heap values(1, 10);"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select * from dup_heap;"); });
    while (!output.empty() && output.back() == '\n') {
        output.pop_back();
    }

    const std::string expected = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "|                1 |               10 |\n"
                                 "|                1 |               10 |\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 2";
    EXPECT_EQ(output, expected);
}

TEST_F(E2ETest, IndexedTableRejectsDuplicateKey) {
    ASSERT_NO_THROW(db_->exec_sql("create table dup_idx (id int, val int);"));
    ASSERT_NO_THROW(db_->exec_sql("create index dup_idx (id);"));
    ASSERT_NO_THROW(db_->exec_sql("insert into dup_idx values(1, 10);"));

    std::string err;
    ASSERT_NO_THROW({ err = db_->exec_sql_expect_error("insert into dup_idx values(1, 20);"); });
    EXPECT_NE(err.find("Index"), std::string::npos);
}

TEST(CheckpointRecoveryTest, CheckpointRestartOffsetSurvivesRestartAndUndoRuns) {
    const std::string db_name = "e2e_checkpoint_recovery";
    const std::string select_expected = "+------------------+------------------+\n"
                                        "|               id |              num |\n"
                                        "+------------------+------------------+\n"
                                        "|                1 |                1 |\n"
                                        "+------------------+------------------+\n"
                                        "Total record(s): 1";

    {
        EmbeddedDB db(db_name, true, false);
        ASSERT_NO_THROW(db.exec_sql("create table t1 (id int, num int);"));
        ASSERT_NO_THROW(db.exec_sql("begin;"));
        ASSERT_NO_THROW(db.exec_sql("insert into t1 values(1, 1);"));
        ASSERT_NO_THROW(db.exec_sql("commit;"));
        ASSERT_NO_THROW(db.exec_sql("create static_checkpoint;"));
        ASSERT_NO_THROW(db.exec_sql("begin;"));
        ASSERT_NO_THROW(db.exec_sql("insert into t1 values(2, 2);"));
    }

    std::ifstream restart_file(db_name + "/" + LogManager::RESTART_FILE_NAME);
    ASSERT_TRUE(restart_file.is_open());
    int64_t restart_offset = 0;
    restart_file >> restart_offset;
    EXPECT_EQ(restart_offset, 0);

    TransactionManager::txn_map.clear();

    {
        EmbeddedDB recovered(db_name, false, true);
        std::string output;
        ASSERT_NO_THROW({ output = recovered.exec_sql("select * from t1;"); });
        while (!output.empty() && output.back() == '\n') {
            output.pop_back();
        }
        EXPECT_EQ(output, select_expected);
    }
}

TEST(CheckpointRecoveryTest, CommittedDeleteBeforeCheckpointDoesNotReappearAfterRestart) {
    const std::string db_name = "e2e_checkpoint_delete_recovery";
    const std::string select_expected = "+------------------+------------------+\n"
                                        "|               id |              num |\n"
                                        "+------------------+------------------+\n"
                                        "+------------------+------------------+\n"
                                        "Total record(s): 0";

    {
        EmbeddedDB db(db_name, true, false);
        ASSERT_NO_THROW(db.exec_sql("create table t1 (id int, num int);"));
        ASSERT_NO_THROW(db.exec_sql("begin;"));
        ASSERT_NO_THROW(db.exec_sql("insert into t1 values(1, 1);"));
        ASSERT_NO_THROW(db.exec_sql("commit;"));
        ASSERT_NO_THROW(db.exec_sql("begin;"));
        ASSERT_NO_THROW(db.exec_sql("delete from t1 where id = 1;"));
        ASSERT_NO_THROW(db.exec_sql("commit;"));
        ASSERT_NO_THROW(db.exec_sql("create static_checkpoint;"));
    }

    TransactionManager::txn_map.clear();

    {
        EmbeddedDB recovered(db_name, false, true);
        std::string output;
        ASSERT_NO_THROW({ output = recovered.exec_sql("select * from t1;"); });
        while (!output.empty() && output.back() == '\n') {
            output.pop_back();
        }
        EXPECT_EQ(output, select_expected);
    }
}

// =============================================================================
// LoadCsv: bulk-load a CSV via the "load <path> into <table>;" statement.
// The CSV is copied next to the DB (cwd after open_db) so the relative path
// is just the filename, regardless of the test process's original CWD.
// =============================================================================

TEST_F(E2ETest, LoadCsvMatchesFileContents) {
    // Locate the shipped sample CSV via the compile-time source dir.
    std::string src_csv;
#ifdef RMDB_SRC_DIR
    src_csv = std::string(RMDB_SRC_DIR) + "/src/test/performance_test/table_data/warehouse.csv";
#else
    src_csv = "src/test/performance_test/table_data/warehouse.csv";
#endif
    std::ifstream check(src_csv);
    ASSERT_TRUE(check.good()) << "missing sample CSV: " << src_csv;
    check.close();

    // Copy the CSV into the current (db) directory so the load path is just the filename.
    char cwd_buf[1024];
    getcwd(cwd_buf, sizeof(cwd_buf));
    std::string dest_csv = std::string(cwd_buf) + "/warehouse_load_test.csv";
    {
        std::ifstream in(src_csv, std::ios::binary);
        std::ofstream out(dest_csv, std::ios::binary);
        out << in.rdbuf();
    }

    // warehouse.csv columns: w_id,w_name,w_street_1,w_street_2,w_city,w_state,w_zip,w_tax,w_ytd
    ASSERT_NO_THROW(
        db_->exec_sql("create table warehouse (w_id int, w_name char(20), w_street_1 char(40), w_street_2 char(40), "
                      "w_city char(20), w_state char(2), w_zip char(9), w_tax float, w_ytd float);"));
    // Load via "./<name>" — the "./" prefix is recognized as VALUE_PATH by the lexer,
    // and since cwd is the db directory, the copied CSV is found by relative path.
    ASSERT_NO_THROW(db_->exec_sql("load ./warehouse_load_test.csv into warehouse;"));

    // warehouse.csv has 1 data row.
    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from warehouse;"); });
    EXPECT_NE(output.find("1"), std::string::npos) << output;

    // Verify content: w_id=1, w_state=JY, w_tax=0.125 (from the sample row).
    ASSERT_NO_THROW({ output = db_->exec_sql("select w_id, w_state, w_tax from warehouse;"); });
    EXPECT_NE(output.find("1"), std::string::npos);
    EXPECT_NE(output.find("JY"), std::string::npos);
    EXPECT_NE(output.find("0.125"), std::string::npos) << output;

    // Cleanup the copied CSV.
    std::remove(dest_csv.c_str());
}

TEST_F(E2ETest, LoadCsvStoresRowsAsCommittedBaseData) {
    char cwd_buf[1024];
    getcwd(cwd_buf, sizeof(cwd_buf));
    std::string dest_csv = std::string(cwd_buf) + "/load_base_meta_test.csv";
    {
        std::ofstream out(dest_csv);
        out << "id,val,name\n";
        out << "1,10,alpha\n";
        out << "2,20,beta\n";
    }

    ASSERT_NO_THROW(db_->exec_sql("create table load_base_meta (id int, val int, name char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("create index load_base_meta(id);"));
    ASSERT_NO_THROW(db_->exec_sql("load ./load_base_meta_test.csv into load_base_meta;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select val from load_base_meta where id = 2;"); });
    EXPECT_NE(output.find("20"), std::string::npos) << output;

    TupleMeta meta = db_->first_tuple_meta("load_base_meta");
    EXPECT_TRUE(meta.is_committed_);
    EXPECT_EQ(0, meta.commit_ts_);
    EXPECT_EQ(INVALID_TXN_ID, meta.writer_txn_id_);
    EXPECT_FALSE(meta.is_deleted_);
    EXPECT_FALSE(meta.version_chain_head_.IsValid());

    std::remove(dest_csv.c_str());
}

// ---- LOAD CSV 表头嗅探与容错 -------------------------------------------------
// final.md:273 只承诺 "load <csv路径> into <改写表名>;"，对表头/分隔符/引号/NULL
// 表示一字未提，所以有表头、无表头、列序打乱、空字段、引号字段、字段数不匹配
// 都必须能装载而不是抛错。

// 把 CSV 写到 db 目录（open_db 之后的 cwd），装载路径就只是文件名。
class LoadCsvFixture : public E2ETest {
protected:
    std::string write_csv(const std::string& name, const std::string& body) {
        char cwd_buf[1024];
        getcwd(cwd_buf, sizeof(cwd_buf));
        std::string path = std::string(cwd_buf) + "/" + name;
        std::ofstream out(path, std::ios::binary);
        out << body;
        out.close();
        temp_files_.push_back(path);
        return "./" + name;
    }

    void TearDown() override {
        for (const auto& path : temp_files_) {
            std::remove(path.c_str());
        }
        E2ETest::TearDown();
    }

private:
    std::vector<std::string> temp_files_;
};

TEST_F(LoadCsvFixture, HeaderlessCsvUsesDdlColumnOrder) {
    const std::string path = write_csv("load_no_header.csv", "1,10,alpha\n2,20,beta\n");
    ASSERT_NO_THROW(db_->exec_sql("create table lnh (id int, val int, name char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into lnh;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from lnh;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select val, name from lnh where id = 1;"); });
    EXPECT_NE(output.find("10"), std::string::npos) << output;
    EXPECT_NE(output.find("alpha"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select name from lnh where id = 2;"); });
    EXPECT_NE(output.find("beta"), std::string::npos) << output;
}

TEST_F(LoadCsvFixture, HeaderCsvMapsByName) {
    const std::string path = write_csv("load_header.csv", "id,val,name\n1,10,alpha\n2,20,beta\n");
    ASSERT_NO_THROW(db_->exec_sql("create table lh (id int, val int, name char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into lh;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from lh;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select name from lh where val = 20;"); });
    EXPECT_NE(output.find("beta"), std::string::npos) << output;
}

TEST_F(LoadCsvFixture, ShuffledHeaderCsvMapsByName) {
    const std::string path = write_csv("load_shuffled.csv", "name, val ,id\nalpha,10,1\nbeta,20,2\n");
    ASSERT_NO_THROW(db_->exec_sql("create table lsh (id int, val int, name char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into lsh;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select val, name from lsh where id = 2;"); });
    EXPECT_NE(output.find("20"), std::string::npos) << output;
    EXPECT_NE(output.find("beta"), std::string::npos) << output;
}

TEST_F(LoadCsvFixture, EmptyFieldsBecomeNullForEveryColumnType) {
    // 每种列类型各留一个空字段：INT / FLOAT / CHAR / DATETIME。
    const std::string path = write_csv("load_nulls.csv", "id,i,f,c,d\n"
                                                         "1,10,1.5,aa,2026-01-01 00:00:00\n"
                                                         "2,,,,\n");
    ASSERT_NO_THROW(db_->exec_sql("create table ln (id int, i int, f float, c char(8), d datetime);"));
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into ln;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from ln;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;

    // 装载完整性校验第 7 条就是这种 "空配送时间行数" 计数。
    for (const char* column : {"i", "f", "c", "d"}) {
        ASSERT_NO_THROW(
            { output = db_->exec_sql(std::string("select count(*) from ln where ") + column + " is null;"); });
        EXPECT_NE(output.find("1"), std::string::npos) << column << ": " << output;
        ASSERT_NO_THROW(
            { output = db_->exec_sql(std::string("select count(*) from ln where ") + column + " is not null;"); });
        EXPECT_NE(output.find("1"), std::string::npos) << column << ": " << output;
    }
    // 空字段必须存成 NULL 而不是空串 / 0。
    ASSERT_NO_THROW({ output = db_->exec_sql("select id from ln where c = '';"); });
    EXPECT_NE(output.find("Total record(s): 0"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select id from ln where i = 0;"); });
    EXPECT_NE(output.find("Total record(s): 0"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select id from ln where i is null;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;
}

TEST_F(LoadCsvFixture, QuotedFieldsAreUnescapedInsteadOfRejected) {
    // RFC 4180：外层引号剥掉，"" 还原成一个 "，引号内的逗号不是分隔符。
    const std::string path = write_csv("load_quoted.csv", "id,name\n"
                                                          "1,\"x,y\"\n"
                                                          "2,\"a\"\"b\"\n"
                                                          "3,\"\"\n");
    ASSERT_NO_THROW(db_->exec_sql("create table lq (id int, name char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into lq;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from lq;"); });
    EXPECT_NE(output.find("3"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select name from lq where id = 1;"); });
    EXPECT_NE(output.find("x,y"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select name from lq where id = 2;"); });
    EXPECT_NE(output.find("a\"b"), std::string::npos) << output;
    // 引号里的空字段同样按 NULL 处理（空字段一律是 NULL）。
    ASSERT_NO_THROW({ output = db_->exec_sql("select id from lq where name is null;"); });
    EXPECT_NE(output.find("3"), std::string::npos) << output;
}

TEST_F(LoadCsvFixture, ExtraTrailingFieldsAreIgnored) {
    // 字段数超出 DDL 列数 -> 多出来的尾部字段忽略（表头/无表头都按列序取前 n 个）。
    const std::string path = write_csv("load_extra.csv", "id,val,name\n"
                                                         "1,10,alpha\n"
                                                         "3,30,gamma,999,extra\n");
    ASSERT_NO_THROW(db_->exec_sql("create table lx (id int, val int, name char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into lx;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from lx;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select val, name from lx where id = 3;"); });
    EXPECT_NE(output.find("30"), std::string::npos) << output;
    EXPECT_NE(output.find("gamma"), std::string::npos) << output;
}

TEST_F(LoadCsvFixture, FewerFieldsThanColumnsIsRejected) {
    // 字段数不足是硬错误：截断/错列的 CSV 不能静默装成一片 NULL，否则 9 张表的
    // COUNT(*) 校验照样通过，病因要等到完整性校验才暴露。
    const std::string path = write_csv("load_short.csv", "id,val,name\n"
                                                         "1,10,alpha\n"
                                                         "2,20\n");
    ASSERT_NO_THROW(db_->exec_sql("create table lsf (id int, val int, name char(8));"));

    std::string err;
    ASSERT_NO_THROW({ err = db_->exec_sql_expect_error("load " + path + " into lsf;"); });
    EXPECT_NE(err.find("fewer fields"), std::string::npos) << err;
    EXPECT_NE(err.find("row 3"), std::string::npos) << err;

    // 空字段仍然是合法的 NULL，只有"字段个数不够"才报错。
    const std::string ok_path = write_csv("load_short_ok.csv", "id,val,name\n"
                                                               "1,10,alpha\n"
                                                               "2,20,\n");
    ASSERT_NO_THROW(db_->exec_sql("create table lsf_ok (id int, val int, name char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("load " + ok_path + " into lsf_ok;"));
    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from lsf_ok where name is null;"); });
    EXPECT_NE(output.find("1"), std::string::npos) << output;
}

TEST_F(LoadCsvFixture, TrailingCommaOnEveryLineStillLoads) {
    // 行尾逗号按 RFC 4180 多切出一个空字段。表头行一多一个字段就会让表头嗅探
    // 失败，进而把表头当数据行去 strtol，整个装载失败。
    const std::string path = write_csv("load_trailing_comma.csv", "id,val,name,\n"
                                                                  "1,10,alpha,\n"
                                                                  "2,20,beta,\n");
    ASSERT_NO_THROW(db_->exec_sql("create table ltc (id int, val int, name char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into ltc;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from ltc;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select val, name from ltc where id = 2;"); });
    EXPECT_NE(output.find("20"), std::string::npos) << output;
    EXPECT_NE(output.find("beta"), std::string::npos) << output;
    // 表头被正确识别，不能有第 3 行数据，也不能出现 name 为 NULL 的行。
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from ltc where name is null;"); });
    EXPECT_NE(output.find("0"), std::string::npos) << output;
}

TEST_F(LoadCsvFixture, TrailingCommaWithoutHeaderStillLoads) {
    const std::string path = write_csv("load_trailing_comma_nh.csv", "1,10,alpha,\n"
                                                                     "2,20,beta,\n");
    ASSERT_NO_THROW(db_->exec_sql("create table ltcn (id int, val int, name char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into ltcn;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from ltcn;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select name from ltcn where id = 1;"); });
    EXPECT_NE(output.find("alpha"), std::string::npos) << output;
}

TEST_F(LoadCsvFixture, FirstRowThatOnlyPartlyMatchesColumnNamesIsData) {
    // 只有"字段数相等 ∧ 全部命中列名 ∧ 不重复"才算表头。这里 "id,val,zzz" 的
    // 第三个字段不是列名，所以整行按数据处理（zzz 落到 char 列里）。
    const std::string path = write_csv("load_partial_header.csv", "1,10,zzz\n2,20,beta\n");
    ASSERT_NO_THROW(db_->exec_sql("create table lph (id int, val int, name char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into lph;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from lph;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select name from lph where id = 1;"); });
    EXPECT_NE(output.find("zzz"), std::string::npos) << output;
}

TEST_F(LoadCsvFixture, DuplicateHeaderNamesAreTreatedAsData) {
    // 列名重复不能当表头（映射有歧义），按位置映射并把该行当数据。
    ASSERT_NO_THROW(db_->exec_sql("create table ldh (a char(4), b char(4));"));
    const std::string path = write_csv("load_dup_header.csv", "a,a\nxx,yy\n");
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into ldh;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from ldh;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;
}

// 标准 TPC-C 的 customer.c_data 是 char(500)，整条记录 749 字节。RM_MAX_RECORD_SIZE
// 只有 512 时 CREATE TABLE 第一步就抛 InvalidRecordSizeError，900 秒装载预算一秒
// 都用不上。final.md 没有公开列宽，所以这条形状必须能建、能装、能查。
TEST_F(LoadCsvFixture, CustomerShapedTableWithChar500LoadsAndQueries) {
    const std::string c_data(500, 'z');
    // 与 benchmark/tpcc/schema/rmdb_schema.sql 的 customer 完全同形，只把 c_data
    // 从 char(100) 换成标准 TPC-C 的 char(500)：整条记录 749 字节。
    ASSERT_NO_THROW(db_->exec_sql("create table customer500 ("
                                  "c_id int, c_d_id int, c_w_id int, c_first char(20), c_middle char(2), "
                                  "c_last char(40), c_street_1 char(40), c_street_2 char(40), c_city char(20), "
                                  "c_state char(2), c_zip char(9), c_phone char(16), c_since char(19), "
                                  "c_credit char(2), c_credit_lim float, c_discount float, c_balance float, "
                                  "c_ytd_payment float, c_payment_cnt int, c_delivery_cnt int, c_data char(500));"));
    EXPECT_EQ(db_->file_header("customer500").record_size, 749);
    ASSERT_NO_THROW(db_->exec_sql("create index customer500(c_w_id, c_d_id, c_id);"));

    auto row = [&](int c_id, const std::string& last, const std::string& data) {
        return std::to_string(c_id) + ",1,1,First,OE," + last +
               ",street1,street2,city,CA,123456789,1234567890123456,2026-01-01 "
               "00:00:00,GC,50000.0,0.5,-10.0,10.0,1,0," +
               data + "\n";
    };
    const std::string path =
        write_csv("load_customer500.csv",
                  row(1, "BARBARBAR", c_data) + row(2, "OUGHTOUGHT", std::string(500, 'y')) + row(3, "ABLEABLE", ""));
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into customer500;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from customer500;"); });
    EXPECT_NE(output.find("3"), std::string::npos) << output;
    // 索引点查（复合键的尾列是 c_id，和 final.md 的 10 个索引同形）
    ASSERT_NO_THROW(
        { output = db_->exec_sql("select c_last from customer500 where c_w_id = 1 and c_d_id = 1 and c_id = 2;"); });
    EXPECT_NE(output.find("OUGHTOUGHT"), std::string::npos) << output;
    // 500 字节的定长列必须完整往返（比较按 strnlen 裁掉右侧零填充）
    ASSERT_NO_THROW({ output = db_->exec_sql("select c_id from customer500 where c_data = '" + c_data + "';"); });
    EXPECT_NE(output.find("Total record(s): 1"), std::string::npos) << output;
    // 空的 c_data 字段仍然是 NULL
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from customer500 where c_data is null;"); });
    EXPECT_NE(output.find("1"), std::string::npos) << output;
    // UPDATE 也要走通（Payment 事务会往 c_data 前面拼 500 字节的历史）
    ASSERT_NO_THROW(db_->exec_sql("update customer500 set c_data = '" + std::string(500, 'w') +
                                  "' where c_w_id = 1 and c_d_id = 1 and c_id = 3;"));
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from customer500 where c_data is null;"); });
    EXPECT_NE(output.find("0"), std::string::npos) << output;
}

// RM_MAX_RECORD_SIZE 的边界：null bitmap 也算在记录长度里，所以 char(512) 的单列
// 表是 513 字节。旧的 512 上限会把它拒掉。
// 页密度回归：TupleMeta 是每个 slot 的固定开销，窄表上它就是主要开销
// （new_orders 的记录只有 13 字节）。这里从真实的文件头读 record_size /
// num_records_per_page，把 9 张 TPC-C 表的每页元组数钉住，任何 TupleMeta 布局
// 回退（例如 padding 又长回来）都会在这里立刻失败。
TEST_F(E2ETest, TpccPageDensityMatchesCompactTupleMeta) {
    ASSERT_EQ(TUPLE_META_SIZE, 32) << "TupleMeta 变大会直接吃掉每页的元组数";

    struct Shape {
        const char* ddl;
        const char* table;
        int record_size;            // 数据区 + 尾部 null bitmap
        int num_records_per_page;   // 32 字节 TupleMeta 下的每页元组数
        int records_per_page_at_40; // 旧的 40 字节 TupleMeta 下的每页元组数
    };
    // DDL 与 benchmark/tpcc/schema/rmdb_schema.sql 一致。
    const std::vector<Shape> shapes = {
        {"create table warehouse (w_id int, w_name char(20), w_street_1 char(40), w_street_2 char(40), "
         "w_city char(20), w_state char(2), w_zip char(9), w_tax float, w_ytd float);",
         "warehouse", 145, 23, 22},
        {"create table district (d_id int, d_w_id int, d_name char(20), d_street_1 char(40), d_street_2 char(40), "
         "d_city char(20), d_state char(2), d_zip char(9), d_tax float, d_ytd float, d_next_o_id int);",
         "district", 153, 22, 21},
        {"create table customer (c_id int, c_d_id int, c_w_id int, c_first char(20), c_middle char(2), "
         "c_last char(40), c_street_1 char(40), c_street_2 char(40), c_city char(20), c_state char(2), "
         "c_zip char(9), c_phone char(16), c_since char(19), c_credit char(2), c_credit_lim float, "
         "c_discount float, c_balance float, c_ytd_payment float, c_payment_cnt int, c_delivery_cnt int, "
         "c_data char(100));",
         "customer", 349, 10, 10},
        {"create table history (h_c_id int, h_c_d_id int, h_c_w_id int, h_d_id int, h_w_id int, "
         "h_date char(19), h_amount float, h_data char(24));",
         "history", 68, 40, 37},
        {"create table new_orders (no_o_id int, no_d_id int, no_w_id int);", "new_orders", 13, 90, 76},
        {"create table orders (o_id int, o_d_id int, o_w_id int, o_c_id int, o_entry_d char(19), "
         "o_carrier_id int, o_ol_cnt int, o_all_local int);",
         "orders", 48, 50, 46},
        {"create table order_line (ol_o_id int, ol_d_id int, ol_w_id int, ol_number int, ol_i_id int, "
         "ol_supply_w_id int, ol_delivery_d char(19), ol_quantity int, ol_amount float, ol_dist_info char(24));",
         "order_line", 77, 37, 34},
        {"create table item (i_id int, i_im_id int, i_name char(40), i_price float, i_data char(50));", "item", 103, 30,
         28},
        {"create table stock (s_i_id int, s_w_id int, s_quantity int, s_dist_01 char(24), s_dist_02 char(24), "
         "s_dist_03 char(24), s_dist_04 char(24), s_dist_05 char(24), s_dist_06 char(24), s_dist_07 char(24), "
         "s_dist_08 char(24), s_dist_09 char(24), s_dist_10 char(24), s_ytd float, s_order_cnt int, "
         "s_remote_cnt int, s_data char(50));",
         "stock", 317, 11, 11},
    };

    for (const auto& shape : shapes) {
        ASSERT_NO_THROW(db_->exec_sql(shape.ddl)) << shape.table;
        const RmFileHdr hdr = db_->file_header(shape.table);
        EXPECT_EQ(hdr.record_size, shape.record_size) << shape.table;
        EXPECT_EQ(hdr.num_records_per_page, shape.num_records_per_page) << shape.table;
        EXPECT_EQ(rm_num_records_per_page(shape.record_size), shape.num_records_per_page) << shape.table;
        EXPECT_GE(shape.num_records_per_page, shape.records_per_page_at_40) << shape.table;
        // 布局必须真的放得进一个页面
        EXPECT_LE(RM_PAGE_META_OFFSET + hdr.num_records_per_page * (hdr.record_size + TUPLE_META_SIZE) +
                      hdr.bitmap_size,
                  PAGE_SIZE)
            << shape.table;
        std::cout << "[page-density] " << shape.table << ": record_size=" << hdr.record_size
                  << " num_records_per_page=" << hdr.num_records_per_page << " (was " << shape.records_per_page_at_40
                  << " at TUPLE_META_SIZE=40)" << std::endl;
    }
}

// append_to_context 以前是无边界的 memcpy 到固定 BUFFER_LENGTH 的 data_send_ 上。
// 宽表 join 的计划树轻松超过 8 KB，那就是一次缓冲区越界写：连接线程静默死亡、
// 日志里什么都没有。现在必须像 RecordPrinter 一样截断。ASan 下跑这个用例才是完整
// 验证（修复前是 stack-buffer-overflow）。
TEST_F(E2ETest, WideExplainAnalyzeOutputIsTruncatedNotOverflowing) {
    constexpr int kTables = 4;
    constexpr int kCols = 40;
    auto col_name = [](int t, int c) {
        return "explain_overflow_regression_wide_column_name_t" + std::to_string(t) + "_c" + std::to_string(c) +
               "_padding";
    };

    for (int t = 0; t < kTables; ++t) {
        std::string ddl = "create table wide_explain_t" + std::to_string(t) + " (";
        for (int c = 0; c < kCols; ++c) {
            ddl += (c == 0 ? "" : ", ") + col_name(t, c) + " int";
        }
        ddl += ");";
        ASSERT_NO_THROW(db_->exec_sql(ddl));
        std::string ins = "insert into wide_explain_t" + std::to_string(t) + " values (";
        for (int c = 0; c < kCols; ++c) {
            ins += (c == 0 ? "" : ", ") + std::string("1");
        }
        ins += ");";
        ASSERT_NO_THROW(db_->exec_sql(ins));
    }

    std::string select_list;
    std::string from_list;
    std::string where_list;
    for (int t = 0; t < kTables; ++t) {
        const std::string tab = "wide_explain_t" + std::to_string(t);
        from_list += (t == 0 ? "" : ", ") + tab;
        for (int c = 0; c < kCols; ++c) {
            select_list += (select_list.empty() ? "" : ", ") + tab + "." + col_name(t, c);
        }
        if (t > 0) {
            where_list += (where_list.empty() ? "" : " and ") + std::string("wide_explain_t0.") + col_name(0, 0) +
                          " = " + tab + "." + col_name(t, 0);
        }
    }
    const std::string sql =
        "explain analyze select " + select_list + " from " + from_list + " where " + where_list + ";";

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql(sql); });
    // 计划树的完整文本远超 8 KB：确认这个用例真的把缓冲写满了……
    EXPECT_GT(sql.size(), static_cast<size_t>(BUFFER_LENGTH));
    // ……而输出被截断在缓冲区内，且给尾部的 "Total record(s)" 行留了位置。
    EXPECT_LE(output.size(), static_cast<size_t>(BUFFER_LENGTH) - RECORD_COUNT_LENGTH);
    EXPECT_GT(output.size(), static_cast<size_t>(BUFFER_LENGTH) / 2);
    db_->clean_output_txt();
}

TEST_F(E2ETest, RecordSizeBoundaryAcceptsChar512AndRejectsOversized) {
    ASSERT_NO_THROW(db_->exec_sql("create table rs512 (x char(512));"));
    ASSERT_NO_THROW(db_->exec_sql("insert into rs512 values ('abc');"));
    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from rs512;"); });
    EXPECT_NE(output.find("1"), std::string::npos) << output;

    // 超过上限仍然必须明确报错（而不是静默截断或越界写）
    std::string err;
    ASSERT_NO_THROW({ err = db_->exec_sql_expect_error("create table rs_huge (x char(2048), y char(2048));"); });
    EXPECT_NE(err.find("record size"), std::string::npos) << err;
}

TEST_F(LoadCsvFixture, LoadedNullsSurviveIndexesAndUpdate) {
    // Delivery 事务的形状：装载出 NULL 的配送时间，之后 UPDATE 成非 NULL。
    const std::string path = write_csv("load_null_update.csv", "k,v,dts\n"
                                                               "1,10,\n"
                                                               "2,20,2026-02-02 00:00:00\n");
    ASSERT_NO_THROW(db_->exec_sql("create table lnu (k int, v int, dts datetime);"));
    ASSERT_NO_THROW(db_->exec_sql("create index lnu(k);"));
    ASSERT_NO_THROW(db_->exec_sql("load " + path + " into lnu;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from lnu where dts is null;"); });
    EXPECT_NE(output.find("1"), std::string::npos) << output;
    ASSERT_NO_THROW(db_->exec_sql("update lnu set dts = '2026-03-03 00:00:00' where k = 1;"));
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from lnu where dts is null;"); });
    EXPECT_NE(output.find("0"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from lnu where dts is not null;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;
    // 索引点查仍然可用
    ASSERT_NO_THROW({ output = db_->exec_sql("select v from lnu where k = 2;"); });
    EXPECT_NE(output.find("20"), std::string::npos) << output;
}

TEST_F(E2ETest, LoadCsvAllowsDuplicateSecondaryIndexKeys) {
    char cwd_buf[1024];
    getcwd(cwd_buf, sizeof(cwd_buf));
    std::string dest_csv = std::string(cwd_buf) + "/load_duplicate_index_test.csv";
    {
        std::ofstream out(dest_csv);
        out << "id,a\n";
        out << "1,10\n";
        out << "2,10\n";
        out << "3,20\n";
    }

    ASSERT_NO_THROW(db_->exec_sql("create table load_dup_idx (id int, a int);"));
    ASSERT_NO_THROW(db_->exec_sql("create index load_dup_idx(a);"));
    ASSERT_NO_THROW(db_->exec_sql("load ./load_duplicate_index_test.csv into load_dup_idx;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from load_dup_idx where a = 10;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;

    std::remove(dest_csv.c_str());
}

// `CREATE INDEX` builds an access path, not a uniqueness constraint
// (final.md:168), so it must not reject data that LOAD already accepted.
TEST_F(E2ETest, CreateIndexOnLoadedDuplicateSecondaryKeysSucceeds) {
    char cwd_buf[1024];
    getcwd(cwd_buf, sizeof(cwd_buf));
    std::string dest_csv = std::string(cwd_buf) + "/create_index_duplicate_test.csv";
    {
        std::ofstream out(dest_csv);
        out << "id,a\n";
        out << "1,10\n";
        out << "2,10\n";
        out << "3,20\n";
    }

    ASSERT_NO_THROW(db_->exec_sql("create table create_dup_idx (id int, a int);"));
    ASSERT_NO_THROW(db_->exec_sql("load ./create_index_duplicate_test.csv into create_dup_idx;"));
    // Index built after the duplicates are already in the heap.
    ASSERT_NO_THROW(db_->exec_sql("create index create_dup_idx(a);"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from create_dup_idx where a = 10;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from create_dup_idx where a = 20;"); });
    EXPECT_NE(output.find("1"), std::string::npos) << output;

    std::remove(dest_csv.c_str());
}

TEST_F(E2ETest, DeleteLoadedDuplicateSecondaryIndexKeyRemovesMatchingRidOnly) {
    char cwd_buf[1024];
    getcwd(cwd_buf, sizeof(cwd_buf));
    std::string dest_csv = std::string(cwd_buf) + "/load_duplicate_index_delete_test.csv";
    {
        std::ofstream out(dest_csv);
        out << "id,a\n";
        out << "1,10\n";
        out << "2,10\n";
    }

    ASSERT_NO_THROW(db_->exec_sql("create table load_dup_idx_delete (id int, a int);"));
    ASSERT_NO_THROW(db_->exec_sql("create index load_dup_idx_delete(a);"));
    ASSERT_NO_THROW(db_->exec_sql("load ./load_duplicate_index_delete_test.csv into load_dup_idx_delete;"));
    ASSERT_NO_THROW(db_->exec_sql("delete from load_dup_idx_delete where id = 2;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select id from load_dup_idx_delete where a = 10;"); });
    EXPECT_NE(output.find("|                1 |"), std::string::npos) << output;
    EXPECT_EQ(output.find("|                2 |"), std::string::npos) << output;

    std::remove(dest_csv.c_str());
}

TEST_F(E2ETest, UpdateLoadedDuplicateSecondaryIndexKeyRemovesMatchingRidOnly) {
    char cwd_buf[1024];
    getcwd(cwd_buf, sizeof(cwd_buf));
    std::string dest_csv = std::string(cwd_buf) + "/load_duplicate_index_update_test.csv";
    {
        std::ofstream out(dest_csv);
        out << "id,a\n";
        out << "1,10\n";
        out << "2,10\n";
    }

    ASSERT_NO_THROW(db_->exec_sql("create table load_dup_idx_update (id int, a int);"));
    ASSERT_NO_THROW(db_->exec_sql("create index load_dup_idx_update(a);"));
    ASSERT_NO_THROW(db_->exec_sql("load ./load_duplicate_index_update_test.csv into load_dup_idx_update;"));
    ASSERT_NO_THROW(db_->exec_sql("update load_dup_idx_update set a = 20 where id = 2;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select id from load_dup_idx_update where a = 10;"); });
    EXPECT_NE(output.find("|                1 |"), std::string::npos) << output;
    EXPECT_EQ(output.find("|                2 |"), std::string::npos) << output;

    ASSERT_NO_THROW({ output = db_->exec_sql("select id from load_dup_idx_update where a = 20;"); });
    EXPECT_NE(output.find("|                2 |"), std::string::npos) << output;

    std::remove(dest_csv.c_str());
}

TEST_F(E2ETest, AbortDeleteLoadedDuplicateSecondaryIndexKeyRestoresMatchingRid) {
    char cwd_buf[1024];
    getcwd(cwd_buf, sizeof(cwd_buf));
    std::string dest_csv = std::string(cwd_buf) + "/load_duplicate_index_abort_delete_test.csv";
    {
        std::ofstream out(dest_csv);
        out << "id,a\n";
        out << "1,10\n";
        out << "2,10\n";
    }

    ASSERT_NO_THROW(db_->exec_sql("create table load_dup_idx_abort_delete (id int, a int);"));
    ASSERT_NO_THROW(db_->exec_sql("create index load_dup_idx_abort_delete(a);"));
    ASSERT_NO_THROW(db_->exec_sql("load ./load_duplicate_index_abort_delete_test.csv into load_dup_idx_abort_delete;"));
    ASSERT_NO_THROW(db_->exec_sql("begin;"));
    ASSERT_NO_THROW(db_->exec_sql("delete from load_dup_idx_abort_delete where id = 2;"));
    ASSERT_NO_THROW(db_->exec_sql("abort;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select count(*) from load_dup_idx_abort_delete where a = 10;"); });
    EXPECT_NE(output.find("2"), std::string::npos) << output;

    ASSERT_NO_THROW({ output = db_->exec_sql("select id from load_dup_idx_abort_delete where a = 10;"); });
    EXPECT_NE(output.find("|                1 |"), std::string::npos) << output;
    EXPECT_NE(output.find("|                2 |"), std::string::npos) << output;

    std::remove(dest_csv.c_str());
}

TEST_F(E2ETest, LoadCsvBuildsUsableIndexForUnsortedKeys) {
    char cwd_buf[1024];
    getcwd(cwd_buf, sizeof(cwd_buf));
    std::string dest_csv = std::string(cwd_buf) + "/load_unsorted_index_test.csv";
    int expected_id = -1;
    {
        std::ofstream out(dest_csv);
        out << "id,a\n";
        for (int id = 0; id < 1000; ++id) {
            int a = (id * 919) % 1000;
            if (a == 777) {
                expected_id = id;
            }
            out << id << "," << a << "\n";
        }
    }
    ASSERT_GE(expected_id, 0);

    ASSERT_NO_THROW(db_->exec_sql("create table load_unsorted_idx (id int, a int);"));
    ASSERT_NO_THROW(db_->exec_sql("create index load_unsorted_idx(a);"));
    ASSERT_NO_THROW(db_->exec_sql("load ./load_unsorted_index_test.csv into load_unsorted_idx;"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select id from load_unsorted_idx where a = 777;"); });
    EXPECT_NE(output.find(std::to_string(expected_id)), std::string::npos) << output;

    std::remove(dest_csv.c_str());
}

// =============================================================================
// LoadCrashRecovery: loaded data must survive a crash (recovery redo replays
// committed batched inserts). Mirrors the CheckpointRecoveryTest pattern.
// =============================================================================

TEST(LoadCrashRecoveryTest, LoadedDataSurvivesRestart) {
    const std::string db_name = "e2e_load_crash_recovery";

    // Locate and copy the shipped item.csv (10 data rows) next to the DB.
    std::string src_csv;
#ifdef RMDB_SRC_DIR
    src_csv = std::string(RMDB_SRC_DIR) + "/src/test/performance_test/table_data/item.csv";
#else
    src_csv = "src/test/performance_test/table_data/item.csv";
#endif
    std::ifstream check(src_csv);
    ASSERT_TRUE(check.good()) << "missing sample CSV: " << src_csv;
    check.close();

    {
        EmbeddedDB db(db_name, true, false);
        char cwd_buf[1024];
        getcwd(cwd_buf, sizeof(cwd_buf));
        std::string dest_csv = std::string(cwd_buf) + "/item_crash_test.csv";
        {
            std::ifstream in(src_csv, std::ios::binary);
            std::ofstream out(dest_csv, std::ios::binary);
            out << in.rdbuf();
        }

        // item.csv columns: i_id,i_im_id,i_name,i_price,i_data
        ASSERT_NO_THROW(
            db.exec_sql("create table item (i_id int, i_im_id int, i_name char(40), i_price float, i_data char(50));"));
        ASSERT_NO_THROW(db.exec_sql("load ./item_crash_test.csv into item;"));

        // Verify the load worked before the "crash".
        std::string output;
        ASSERT_NO_THROW({ output = db.exec_sql("select count(*) from item;"); });
        EXPECT_NE(output.find("10"), std::string::npos) << output;
        // Destructor closes the DB (simulating a clean-ish exit); the batched
        // commits already flushed WAL, so recovery can redo them.
    }

    // Simulate restart: clear in-memory txn state so recovery runs fresh.
    TransactionManager::txn_map.clear();

    {
        EmbeddedDB recovered(db_name, false, true);
        std::string output;
        ASSERT_NO_THROW({ output = recovered.exec_sql("select count(*) from item;"); });
        // All 10 loaded rows must survive the restart.
        EXPECT_NE(output.find("10"), std::string::npos) << "loaded data lost after restart; output: " << output;

        // Spot-check content: the first item row has i_id=1.
        ASSERT_NO_THROW({ output = recovered.exec_sql("select i_id, i_name from item order by i_id limit 1;"); });
        EXPECT_NE(output.find("1"), std::string::npos) << output;
    }
}

// Verify show tables/index respect the output_file toggle: after "set output_file off",
// these commands must NOT append to output.txt.

TEST_F(E2ETest, SessionOutputFilePolicyIsIsolated) {
    ASSERT_NO_THROW(db_->exec_sql("create table t_session_output (a int);"));
    ASSERT_NO_THROW(db_->exec_sql("insert into t_session_output values (1);"));
    db_->clean_output_txt();

    bool wire_session_output = false;
    bool other_wire_session_output = false;
    ASSERT_NO_THROW(db_->exec_sql("select * from t_session_output;", &wire_session_output));
    EXPECT_FALSE(std::ifstream("output.txt").good());

    ASSERT_NO_THROW(db_->exec_sql("set output_file on", &wire_session_output));
    EXPECT_TRUE(wire_session_output);
    EXPECT_FALSE(other_wire_session_output);
    ASSERT_NO_THROW(db_->exec_sql("select * from t_session_output;", &wire_session_output));

    std::ifstream enabled_output("output.txt");
    std::string enabled_content((std::istreambuf_iterator<char>(enabled_output)), std::istreambuf_iterator<char>());
    EXPECT_NE(enabled_content.find("1"), std::string::npos);

    db_->clean_output_txt();
    ASSERT_NO_THROW(db_->exec_sql("select * from t_session_output;", &other_wire_session_output));
    EXPECT_FALSE(std::ifstream("output.txt").good());

    ASSERT_NO_THROW(db_->exec_sql("select * from t_session_output;"));
    EXPECT_TRUE(std::ifstream("output.txt").good());
}

TEST_F(E2ETest, ShowCommandsRespectOutputFileOff) {
    ASSERT_NO_THROW(db_->exec_sql("create table t_show (a int, b char(4));"));
    ASSERT_NO_THROW(db_->exec_sql("create index t_show(a);"));
    db_->clean_output_txt();

    // With output OFF, show commands should produce client output but no file output.
    ASSERT_NO_THROW(db_->exec_sql("set output_file off"));
    ASSERT_NO_THROW(db_->exec_sql("show tables;"));
    ASSERT_NO_THROW(db_->exec_sql("show index from t_show;"));

    // output.txt should not exist (or be empty) after show commands with output off.
    std::ifstream f("output.txt");
    std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    f.close();
    EXPECT_EQ(content, "") << "show commands wrote to output.txt despite output_file off";

    // With output ON, show commands should write to output.txt.
    ASSERT_NO_THROW(db_->exec_sql("set output_file on"));
    ASSERT_NO_THROW(db_->exec_sql("show tables;"));
    std::ifstream f2("output.txt");
    std::string content2((std::istreambuf_iterator<char>(f2)), std::istreambuf_iterator<char>());
    f2.close();
    EXPECT_NE(content2, "") << "show tables did not write to output.txt with output_file on";
    EXPECT_NE(content2.find("t_show"), std::string::npos);
}
