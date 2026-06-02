/* Copyright (c) 2023 Renmin University of China
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
    EmbeddedDB(const std::string& db_name) : db_name_(db_name) {
        // Save original CWD for teardown
        char cwd_buf[1024];
        getcwd(cwd_buf, sizeof(cwd_buf));
        original_cwd_ = cwd_buf;

        // Clean up any leftover from previous runs (use absolute path)
        std::string cmd = "rm -rf " + original_cwd_ + "/" + db_name_;
        system(cmd.c_str());

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
        sm_manager_->create_db(db_name_);
        sm_manager_->open_db(db_name_);

        // ARIES recovery
        recovery_->analyze();
        recovery_->redo();
        recovery_->undo();
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
        std::string cmd = "rm -rf " + original_cwd_ + "/" + db_name_;
        system(cmd.c_str());
    }

    /// Execute a single SQL statement (must include trailing ';').
    /// Returns the captured text output. Throws RMDBError on failure.
    std::string exec_sql(const std::string& sql) {
        char data_send[BUFFER_LENGTH];
        memset(data_send, 0, BUFFER_LENGTH);
        int offset = 0;

        Context context(lock_manager_.get(), log_manager_.get(), nullptr, data_send, &offset);
        set_transaction(&context);

        // Parse
        YY_BUFFER_STATE buf = yy_scan_string(sql.c_str());
        if (yyparse() != 0) {
            yy_delete_buffer(buf);
            abort_implicit_statement(&context);
            throw RMDBError("Parse error for: " + sql);
        }
        if (ast::parse_tree == nullptr) {
            yy_delete_buffer(buf);
            finish_statement(&context);
            return ""; // EXIT or EOF
        }
        yy_delete_buffer(buf);

        // Analyze → Optimize → Portal → Execute
        try {
            std::shared_ptr<Query> query = analyze_->do_analyze(ast::parse_tree);
            std::shared_ptr<Plan> plan = optimizer_->plan_query(query, &context);
            std::shared_ptr<PortalStmt> portal_stmt = portal_->start(plan, &context);
            portal_->run(portal_stmt, ql_manager_.get(), &txn_id_, &context);
            portal_->drop();
            finish_statement(&context);
        } catch (...) {
            abort_implicit_statement(&context);
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

private:
    void set_transaction(Context* context) {
        context->txn_ = txn_id_ == INVALID_TXN_ID ? nullptr : txn_manager_->get_transaction(txn_id_);
        if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
            context->txn_->get_state() == TransactionState::ABORTED) {
            context->txn_ = txn_manager_->begin(nullptr, context->log_mgr_);
            txn_id_ = context->txn_->get_transaction_id();
            context->txn_->set_txn_mode(false);
        }
    }

    void finish_statement(Context* context) {
        if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
            context->txn_->get_state() != TransactionState::COMMITTED &&
            context->txn_->get_state() != TransactionState::ABORTED) {
            txn_manager_->commit(context->txn_, context->log_mgr_);
        }
    }

    void abort_implicit_statement(Context* context) {
        if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
            context->txn_->get_state() != TransactionState::COMMITTED &&
            context->txn_->get_state() != TransactionState::ABORTED) {
            txn_manager_->abort(context->txn_, context->log_mgr_);
        }
    }

    std::string db_name_;
    std::string original_cwd_;
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

TEST_F(SltFileTest, Union) {
    run_slt_file("union.slt");
}

TEST_F(SltFileTest, QueryOptimize) {
    run_slt_file("query_optimize.slt");
}

TEST_F(SltFileTest, NestNljInlj) {
    run_slt_file("nest_nlj_inlj.slt");
}
