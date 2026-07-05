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
#include "access/load_data_service.h"
#include "access/recovery_access.h"
#include "access/table_write_service.h"
#include "common/config.h"
#include "errors.h"
#include "gtest/gtest.h"
#include "index/ix_manager.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "pager/pager.h"
#include "parser/parser.h"
#include "record/rm_manager.h"
#include "record/rm_scan.h"
#include "recovery/log_manager.h"
#include "recovery/log_recovery.h"
#include "server/output_sink.h"
#include "statement/statement_context.h"
#include "statement/statement_runner.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/schema_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

using namespace rmdb;
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
            ::system(cmd.c_str());
        }

        // 构建全局所需的管理器对象（同 src/rmdb.cpp）
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
        pager_ = std::make_unique<rmdb::pager::Pager>(buffer_pool_manager_.get(), log_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get(), pager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get(), pager_.get());
        schema_manager_ = std::make_unique<SchemaManager>(disk_manager_.get(), buffer_pool_manager_.get(),
                                                          rm_manager_.get(), ix_manager_.get(), pager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get(), schema_manager_.get());
        planner_ = std::make_unique<Planner>(&schema_manager_->catalog());
        optimizer_ = std::make_unique<Optimizer>(planner_.get());
        recovery_access_ = std::make_unique<rmdb::access::RecoveryAccess>(schema_manager_.get());
        recovery_ =
            std::make_unique<RecoveryManager>(disk_manager_.get(), buffer_pool_manager_.get(), schema_manager_.get(),
                                              log_manager_.get(), recovery_access_.get());
        write_service_ = std::make_unique<rmdb::access::TableWriteService>(schema_manager_.get(), lock_manager_.get(),
                                                                           log_manager_.get(), txn_manager_.get());
        load_data_service_ =
            std::make_unique<rmdb::access::LoadDataService>(schema_manager_.get(), write_service_.get());
        statement_runner_ = std::make_unique<StatementRunner>(
            schema_manager_.get(), write_service_.get(), planner_.get(), load_data_service_.get(), txn_manager_.get());
        analyze_ = std::make_unique<Analyze>(&schema_manager_->catalog());

        // Create and open the database
        if (!schema_manager_->is_dir(db_name_)) {
            schema_manager_->create_db(db_name_);
        }
        schema_manager_->open_db(db_name_);

        log_manager_->initialize_from_existing_log();
        // Phase 5: 通过 Pager 注入 WAL guard，替代 set_log_manager
        buffer_pool_manager_->set_wal_guard(pager_.get());

        // ARIES recovery
        recovery_->analyze();
        recovery_->redo();
        recovery_->undo();
    }

    ~EmbeddedDB() {
        // close_db() does chdir("..") internally — do NOT chdir before it.
        try {
            schema_manager_->close_db();
        } catch (...) {
            // If close_db() throws, we may be stuck inside the db dir.
            // Force-restore CWD so cleanup works.
            chdir(original_cwd_.c_str());
        }
        // Now CWD is back at original_cwd_ (one level above the db dir)
        if (cleanup_on_destroy_) {
            std::string cmd = "rm -rf " + original_cwd_ + "/" + db_name_;
            ::system(cmd.c_str());
        }
    }

    /// Execute a single SQL statement (must include trailing ';').
    /// Returns the captured text output. Throws RMDBError on failure.
    std::string exec_sql(const std::string& sql) {
        char data_send[BUFFER_LENGTH];
        memset(data_send, 0, BUFFER_LENGTH);
        int offset = 0;

        StatementContext stmt_ctx;
        stmt_ctx.lock_mgr = lock_manager_.get();
        stmt_ctx.log_mgr = log_manager_.get();
        stmt_ctx.txn_mgr = txn_manager_.get();
        // output_file toggle is now a database-global on SmManager; no per-session
        // mirror needed here.

        // Parse
        std::unique_ptr<rmdb::parser::ast::TreeNode> parse_tree;
        try {
            parse_tree = rmdb::parser::ast::parse_sql(sql);
        } catch (...) {
            abort_implicit_statement(&stmt_ctx);
            throw RMDBError("Parse error for: " + sql);
        }
        if (parse_tree == nullptr) {
            finish_statement(&stmt_ctx);
            return ""; // EXIT or EOF
        }
        bool is_checkpoint = parse_tree->type == rmdb::parser::ast::AstType::StaticCheckpoint;
        bool is_load = parse_tree->type == rmdb::parser::ast::AstType::LoadStmt;
        if (!is_checkpoint && !is_load) {
            set_transaction(&stmt_ctx);
        }

        // Analyze → Optimize → StatementRunner → Execute
        try {
            std::unique_ptr<Query> query = analyze_->do_analyze(std::move(parse_tree));
            std::unique_ptr<Plan> plan = optimizer_->plan_query(std::move(query));
            OutputSink sink{data_send, &offset, false};
            statement_runner_->run(std::move(plan), &stmt_ctx, &sink, &txn_id_);
            finish_statement(&stmt_ctx);
        } catch (...) {
            abort_implicit_statement(&stmt_ctx);
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

    TupleMeta first_tuple_meta(const std::string& table_name) {
        auto* fh = schema_manager_->get_table_handle(table_name);
        RmScan scan(fh);
        if (scan.is_end()) {
            throw InternalError("table has no tuple: " + table_name);
        }
        return fh->get_tuple_meta(scan.rid());
    }

private:
    void set_transaction(StatementContext* stmt_ctx) {
        stmt_ctx->txn = txn_id_ == INVALID_TXN_ID ? nullptr : txn_manager_->get_transaction(txn_id_);
        if (stmt_ctx->txn == nullptr || stmt_ctx->txn->get_state() == TransactionState::COMMITTED ||
            stmt_ctx->txn->get_state() == TransactionState::ABORTED) {
            stmt_ctx->txn = txn_manager_->begin(nullptr, stmt_ctx->log_mgr, stmt_ctx->isolation_level);
            txn_id_ = stmt_ctx->txn->get_transaction_id();
            stmt_ctx->txn->set_txn_mode(false);
        }
        txn_manager_->BeginStatement(stmt_ctx->txn);
    }

    void finish_statement(StatementContext* stmt_ctx) {
        if (stmt_ctx->txn != nullptr && !stmt_ctx->txn->get_txn_mode() &&
            stmt_ctx->txn->get_state() != TransactionState::COMMITTED &&
            stmt_ctx->txn->get_state() != TransactionState::ABORTED) {
            txn_manager_->commit(stmt_ctx->txn, stmt_ctx->log_mgr);
        }
        stmt_ctx->txn = nullptr;
    }

    void abort_implicit_statement(StatementContext* stmt_ctx) {
        if (stmt_ctx->txn != nullptr && !stmt_ctx->txn->get_txn_mode() &&
            stmt_ctx->txn->get_state() != TransactionState::COMMITTED &&
            stmt_ctx->txn->get_state() != TransactionState::ABORTED) {
            txn_manager_->abort(stmt_ctx->txn, stmt_ctx->log_mgr);
        }
        stmt_ctx->txn = nullptr;
    }

    std::string db_name_;
    std::string original_cwd_;
    bool cleanup_on_destroy_{true};
    txn_id_t txn_id_{INVALID_TXN_ID};
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<rmdb::pager::Pager> pager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SchemaManager> schema_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> txn_manager_;
    std::unique_ptr<Planner> planner_;
    std::unique_ptr<Optimizer> optimizer_;
    std::unique_ptr<rmdb::access::RecoveryAccess> recovery_access_;
    std::unique_ptr<RecoveryManager> recovery_;
    std::unique_ptr<rmdb::access::TableWriteService> write_service_;
    std::unique_ptr<rmdb::access::LoadDataService> load_data_service_;
    std::unique_ptr<StatementRunner> statement_runner_;
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
                EXPECT_THROW({ db_->exec_sql(tc.sql); }, RMDBError) << "Expected RMDBError but no exception thrown";
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

TEST_F(SltFileTest, StringMinMax) {
    run_slt_file("string_min_max.slt");
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

TEST_F(SltFileTest, RefactorGuardrails) {
    run_slt_file("refactor_guardrails.slt");
}

TEST_F(SltFileTest, WriteProtocolDifferential) {
    run_slt_file("write_protocol_differential.slt");
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
