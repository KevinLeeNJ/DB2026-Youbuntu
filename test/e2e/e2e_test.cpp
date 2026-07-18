/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include "analyze/analyze.h"
#include "common/config.h"
#include "common/context.h"
#include "compiled/program_verifier.h"
#include "compiled/program_cache.h"
#include "execution/runtime/program_dispatcher.h"
#include "errors.h"
#include "execution/execution_manager.h"
#include "gtest/gtest.h"
#include "index/ix_manager.h"
#ifdef RMDB_ENABLE_JIT
#include "jit/jit_types.h"
#include "jit/point_program_jit_manager.h"
#endif
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
        point_program_template_cache_ = std::make_unique<compiled::ProgramTemplateCache>();
        portal_ = std::make_unique<Portal>(sm_manager_.get(), point_program_template_cache_.get());
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

#ifdef RMDB_ENABLE_JIT
        point_program_jit_runtime_ = std::make_unique<jit::JitRuntime>();
        point_program_jit_manager_ = std::make_unique<jit::PointProgramJitManager>(
            jit::PointProgramJitConfig{},
            [this](const compiled::ProgramTemplateIdentity& identity) {
                const uint64_t catalog_generation = sm_manager_->get_catalog_generation();
                const uint64_t planner_generation = planner_->planner_knob_generation();
                const uint64_t statement_generation = catalog_generation ^ (planner_generation * 0x9e3779b97f4a7c15ULL);
                return identity.catalog_generation == catalog_generation &&
                       identity.planner_generation == planner_generation &&
                       identity.statement_generation == statement_generation;
            },
            [this](compiled::ProgramTemplatePtr program_template) {
                jit::JitCompileOptions options;
                options.force_compile_failure = force_jit_compile_failure_.load(std::memory_order_relaxed);
                return point_program_jit_runtime_->compile_program(program_template->program(), options);
            });
#endif
    }

    ~EmbeddedDB() {
        // close_db() does chdir("..") internally — do NOT chdir before it.
        try {
#ifdef RMDB_ENABLE_JIT
            point_program_jit_manager_->ShutdownAndDrain();
#endif
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
    std::string exec_sql(const std::string& sql) {
        char data_send[BUFFER_LENGTH];
        memset(data_send, 0, BUFFER_LENGTH);
        int offset = 0;

        Context context(lock_manager_.get(), log_manager_.get(), nullptr, data_send, &offset, txn_manager_.get());
        context.isolation_level_ = session_isolation_level_;
        // output_file toggle is now a database-global on SmManager; no per-session
        // mirror needed here.

        bool statement_started = false;
        last_top_level_program_hit_ = false;
        parser::OwnedTokenStream lexical;
        const char* cache_flag = std::getenv("ENABLE_POINT_PROGRAM_CACHE");
        if (cache_flag != nullptr && std::string(cache_flag) == "1") {
            lexical = parser::normalize_sql(sql, false);
            if (lexical) {
                context.has_statement_template_identity_ = true;
                context.statement_shape_high_ = lexical.key.high;
                context.statement_shape_low_ = lexical.key.low;
                context.statement_shape_canonical_ = lexical.key.canonical_bytes;
                context.planner_generation_ = planner_->planner_knob_generation();
                context.statement_template_generation_ =
                    sm_manager_->get_catalog_generation() ^
                    (context.planner_generation_ * 0x9e3779b97f4a7c15ULL);
                auto program_template = point_program_template_cache_->LookupAny(
                    lexical.key, context.statement_template_generation_, context.planner_generation_,
                    sm_manager_->get_catalog_generation());
                if (program_template != nullptr) {
                    try {
                        set_transaction(&context);
                        statement_started = true;
                        const auto dispatched = DispatchCachedPointProgram(
                            {point_program_template_cache_.get(), &lexical, context.statement_template_generation_,
                             context.planner_generation_, sm_manager_.get(), &context, program_template,
                             point_program_jit_manager()});
                        if (dispatched == ProgramDispatchStatus::HANDLED) {
                            last_top_level_program_hit_ = true;
                            finish_statement(&context);
                            return std::string(data_send, offset);
                        }
                    } catch (...) {
                        abort_failed_statement(&context);
                        throw;
                    }
                }
            }
        }

        // Parse
        std::unique_ptr<ast::TreeNode> parse_tree;
        try {
            ++parser_entries_;
            parse_tree = ast::parse_sql(sql);
        } catch (...) {
            abort_failed_statement(&context);
            throw RMDBError("Parse error for: " + sql);
        }
        if (parse_tree == nullptr) {
            finish_statement(&context);
            return ""; // EXIT or EOF
        }
        ast::assign_literal_slots(*parse_tree);
        bool is_checkpoint = parse_tree->type == ast::AstType::StaticCheckpoint;
        bool is_load = parse_tree->type == ast::AstType::LoadStmt;
        if (!is_checkpoint && !is_load && !statement_started) {
            set_transaction(&context);
        }

        // Analyze → Optimize → Portal → Execute
        try {
            ++analyzer_entries_;
            std::unique_ptr<Query> query = analyze_->do_analyze(std::move(parse_tree));
            ++planner_entries_;
            std::unique_ptr<Plan> plan = optimizer_->plan_query(std::move(query), &context);
            ++portal_entries_;
            std::unique_ptr<PortalStmt> portal_stmt = portal_->start(std::move(plan), &context);
            last_compiled_mutation_ = portal_stmt->compiled_mutation != nullptr;
            last_compiled_mutation_program_ = portal_stmt->compiled_mutation == nullptr
                                                  ? nullptr
                                                  : portal_stmt->compiled_mutation->program;
            auto verification = portal_stmt->compiled_mutation == nullptr
                                    ? compiled::VerifyResult{true, {}}
                                    : compiled::VerifyProgram(*portal_stmt->compiled_mutation->program);
            last_compiled_mutation_valid_ = static_cast<bool>(verification);
            last_compiled_mutation_error_ = verification.error;
            portal_->run(std::move(portal_stmt), ql_manager_.get(), &txn_id_, &context);
            portal_->drop();
            session_isolation_level_ = context.isolation_level_;
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

    TupleMeta first_tuple_meta(const std::string& table_name) {
        auto* fh = sm_manager_->fhs_.at(table_name).get();
        RmScan scan(fh);
        if (scan.is_end()) {
            throw InternalError("table has no tuple: " + table_name);
        }
        return fh->get_tuple_meta(scan.rid());
    }

    bool last_compiled_mutation() const {
        return last_compiled_mutation_;
    }

    bool last_compiled_mutation_valid() const {
        return last_compiled_mutation_valid_;
    }

    const std::string& last_compiled_mutation_error() const {
        return last_compiled_mutation_error_;
    }

    std::shared_ptr<const compiled::CompiledProgram> last_compiled_mutation_program() const {
        return last_compiled_mutation_program_;
    }

    bool last_top_level_program_hit() const {
        return last_top_level_program_hit_;
    }

    std::array<uint64_t, 4> frontend_entries() const {
        return {parser_entries_, analyzer_entries_, planner_entries_, portal_entries_};
    }

    compiled::ProgramCacheStats point_program_template_cache_stats() const {
        return point_program_template_cache_->Stats();
    }

#ifdef RMDB_ENABLE_JIT
    jit::PointProgramJitStats point_program_jit_stats() const {
        return point_program_jit_manager_->Stats();
    }

    bool point_program_jit_supported() const {
        return point_program_jit_runtime_->is_supported();
    }

    void force_jit_compile_failure(bool force) {
        force_jit_compile_failure_.store(force, std::memory_order_relaxed);
    }
#endif

private:
    jit::PointProgramJitManager* point_program_jit_manager() {
#ifdef RMDB_ENABLE_JIT
        return point_program_jit_manager_.get();
#else
        return nullptr;
#endif
    }

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
    IsolationLevel session_isolation_level_{DEFAULT_ISOLATION_LEVEL};
    bool last_compiled_mutation_{false};
    bool last_compiled_mutation_valid_{true};
    std::string last_compiled_mutation_error_;
    std::shared_ptr<const compiled::CompiledProgram> last_compiled_mutation_program_;
    bool last_top_level_program_hit_{false};
    uint64_t parser_entries_{0};
    uint64_t analyzer_entries_{0};
    uint64_t planner_entries_{0};
    uint64_t portal_entries_{0};
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
    std::unique_ptr<compiled::ProgramTemplateCache> point_program_template_cache_;
#ifdef RMDB_ENABLE_JIT
    std::unique_ptr<jit::JitRuntime> point_program_jit_runtime_;
    std::unique_ptr<jit::PointProgramJitManager> point_program_jit_manager_;
    std::atomic<bool> force_jit_compile_failure_{false};
#endif
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

TEST_F(SltFileTest, PointDml) {
    run_slt_file("point_dml.slt");
}

TEST_F(SltFileTest, PointSelect) {
    run_slt_file("point_select.slt");
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

TEST_F(SltFileTest, UpdateIndexOptimization) {
    run_slt_file("update_index_optimization.slt");
}

TEST_F(SltFileTest, InsertUniqueIndexRegression) {
    run_slt_file("insert_unique_index_regression.slt");
}

TEST_F(SltFileTest, TransactionRepeatedUpdateMerge) {
    run_slt_file("transaction_repeated_update_merge.slt");
}

#ifdef RMDB_ENABLE_JIT
namespace {

class ScopedPointProgramJitFlags {
public:
    ScopedPointProgramJitFlags()
        : select_(Read("ENABLE_POINT_SELECT_INTERPRETER")), mutation_(Read("ENABLE_POINT_MUTATION_INTERPRETER")),
          cache_(Read("ENABLE_POINT_PROGRAM_CACHE")), jit_(Read(rmdb_config::kPointProgramJitEnv)),
          jit_mode_(rmdb_config::jit_mode) {
        setenv("ENABLE_POINT_SELECT_INTERPRETER", "1", 1);
        setenv("ENABLE_POINT_MUTATION_INTERPRETER", "1", 1);
        setenv("ENABLE_POINT_PROGRAM_CACHE", "1", 1);
        setenv(rmdb_config::kPointProgramJitEnv, "1", 1);
        rmdb_config::jit_mode = rmdb_config::JitMode::FORCE;
    }

    ~ScopedPointProgramJitFlags() {
        Restore("ENABLE_POINT_SELECT_INTERPRETER", select_);
        Restore("ENABLE_POINT_MUTATION_INTERPRETER", mutation_);
        Restore("ENABLE_POINT_PROGRAM_CACHE", cache_);
        Restore(rmdb_config::kPointProgramJitEnv, jit_);
        rmdb_config::jit_mode = jit_mode_;
    }

    ScopedPointProgramJitFlags(const ScopedPointProgramJitFlags&) = delete;
    ScopedPointProgramJitFlags& operator=(const ScopedPointProgramJitFlags&) = delete;

private:
    static std::optional<std::string> Read(const char* name) {
        const char* value = std::getenv(name);
        return value == nullptr ? std::nullopt : std::optional<std::string>(value);
    }

    static void Restore(const char* name, const std::optional<std::string>& value) {
        if (value.has_value()) {
            setenv(name, value->c_str(), 1);
        } else {
            unsetenv(name);
        }
    }

    std::optional<std::string> select_;
    std::optional<std::string> mutation_;
    std::optional<std::string> cache_;
    std::optional<std::string> jit_;
    rmdb_config::JitMode jit_mode_;
};

} // namespace

TEST_F(E2ETest, PointProgramJitExecutesCachedSelectUpdateDeleteAndInsertNatively) {
    ScopedPointProgramJitFlags flags;
    ASSERT_TRUE(db_->point_program_jit_supported());

    ASSERT_NO_THROW(db_->exec_sql("create table jit_point_native (id int, value int, note char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("create index jit_point_native(id);"));

    ASSERT_NO_THROW(db_->exec_sql("insert into jit_point_native values(1, 10, 'one');"));
    auto frontend_before = db_->frontend_entries();
    auto stats_before = db_->point_program_jit_stats();
    ASSERT_NO_THROW(db_->exec_sql("insert into jit_point_native values(2, 20, 'two');"));
    EXPECT_TRUE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries(), frontend_before);
    EXPECT_EQ(db_->point_program_jit_stats().native_executions, stats_before.native_executions + 1);

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select note from jit_point_native where id = 1;"); });
    EXPECT_NE(output.find("one"), std::string::npos) << output;
    frontend_before = db_->frontend_entries();
    stats_before = db_->point_program_jit_stats();
    ASSERT_NO_THROW({ output = db_->exec_sql("select note from jit_point_native where id = 2;"); });
    EXPECT_NE(output.find("two"), std::string::npos) << output;
    EXPECT_EQ(output.find("one"), std::string::npos) << output;
    EXPECT_TRUE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries(), frontend_before);
    EXPECT_EQ(db_->point_program_jit_stats().native_executions, stats_before.native_executions + 1);

    ASSERT_NO_THROW(db_->exec_sql("update jit_point_native set value = 11 where id = 1;"));
    frontend_before = db_->frontend_entries();
    stats_before = db_->point_program_jit_stats();
    ASSERT_NO_THROW(db_->exec_sql("update jit_point_native set value = 22 where id = 2;"));
    EXPECT_TRUE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries(), frontend_before);
    EXPECT_EQ(db_->point_program_jit_stats().native_executions, stats_before.native_executions + 1);

    ASSERT_NO_THROW(db_->exec_sql("delete from jit_point_native where id = 99;"));
    frontend_before = db_->frontend_entries();
    stats_before = db_->point_program_jit_stats();
    ASSERT_NO_THROW(db_->exec_sql("delete from jit_point_native where id = 1;"));
    EXPECT_TRUE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries(), frontend_before);
    EXPECT_EQ(db_->point_program_jit_stats().native_executions, stats_before.native_executions + 1);

    ASSERT_NO_THROW({ output = db_->exec_sql("select value from jit_point_native where id = 2;"); });
    EXPECT_NE(output.find("22"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select value from jit_point_native where id = 1;"); });
    EXPECT_NE(output.find("Total record(s): 0"), std::string::npos) << output;
}

TEST_F(E2ETest, PointProgramJitRejectsRcMutationAfterSwitchToSnapshotIsolation) {
    ScopedPointProgramJitFlags flags;
    ASSERT_TRUE(db_->point_program_jit_supported());

    ASSERT_NO_THROW(db_->exec_sql("create table jit_snapshot_fallback (id int, value int);"));
    ASSERT_NO_THROW(db_->exec_sql("create index jit_snapshot_fallback(id);"));
    ASSERT_NO_THROW(db_->exec_sql("insert into jit_snapshot_fallback values(1, 10);"));
    ASSERT_NO_THROW(db_->exec_sql("update jit_snapshot_fallback set value = 11 where id = 1;"));
    ASSERT_NO_THROW(db_->exec_sql("update jit_snapshot_fallback set value = 12 where id = 1;"));
    ASSERT_TRUE(db_->last_top_level_program_hit());

    ASSERT_NO_THROW(db_->exec_sql("set transaction isolation level snapshot isolation;"));
    const auto frontend_before = db_->frontend_entries();
    const auto stats_before = db_->point_program_jit_stats();
    ASSERT_NO_THROW(db_->exec_sql("update jit_snapshot_fallback set value = 13 where id = 1;"));
    EXPECT_FALSE(db_->last_top_level_program_hit());
    EXPECT_FALSE(db_->last_compiled_mutation());
    EXPECT_EQ(db_->point_program_jit_stats().native_executions, stats_before.native_executions);
    const auto frontend_after = db_->frontend_entries();
    for (size_t i = 0; i < frontend_before.size(); ++i) {
        EXPECT_EQ(frontend_after[i], frontend_before[i] + 1);
    }

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select value from jit_snapshot_fallback where id = 1;"); });
    EXPECT_NE(output.find("13"), std::string::npos) << output;
}

TEST_F(E2ETest, PointProgramJitCompileFailureFallsBackToInterpreterOnce) {
    ScopedPointProgramJitFlags flags;
    ASSERT_TRUE(db_->point_program_jit_supported());

    ASSERT_NO_THROW(db_->exec_sql("create table jit_compile_fallback (id int, note char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("create index jit_compile_fallback(id);"));
    ASSERT_NO_THROW(db_->exec_sql("insert into jit_compile_fallback values(1, 'one');"));
    ASSERT_NO_THROW(db_->exec_sql("insert into jit_compile_fallback values(2, 'two');"));

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select note from jit_compile_fallback where id = 1;"); });
    EXPECT_NE(output.find("one"), std::string::npos) << output;
    const auto frontend_before = db_->frontend_entries();
    const auto stats_before = db_->point_program_jit_stats();
    db_->force_jit_compile_failure(true);
    ASSERT_NO_THROW({ output = db_->exec_sql("select note from jit_compile_fallback where id = 2;"); });
    db_->force_jit_compile_failure(false);

    EXPECT_NE(output.find("two"), std::string::npos) << output;
    EXPECT_EQ(output.find("one"), std::string::npos) << output;
    EXPECT_TRUE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries(), frontend_before);
    const auto stats_after = db_->point_program_jit_stats();
    EXPECT_EQ(stats_after.compile_attempts, stats_before.compile_attempts + 1);
    EXPECT_EQ(stats_after.compile_failures, stats_before.compile_failures + 1);
    EXPECT_EQ(stats_after.interpreter_executions, stats_before.interpreter_executions + 1);
    EXPECT_EQ(stats_after.native_executions, stats_before.native_executions);
}
#endif

TEST_F(E2ETest, PointMutationInterpreterBuildsVerifiedProgramsAndFallsBackForRanges) {
    const char* previous = std::getenv("ENABLE_POINT_MUTATION_INTERPRETER");
    const char* previous_cache = std::getenv("ENABLE_POINT_PROGRAM_CACHE");
    const std::optional<std::string> saved = previous == nullptr ? std::nullopt : std::optional<std::string>(previous);
    struct RestoreEnv {
        std::optional<std::string> value;
        std::optional<std::string> cache;
        ~RestoreEnv() {
            if (value.has_value()) {
                setenv("ENABLE_POINT_MUTATION_INTERPRETER", value->c_str(), 1);
            } else {
                unsetenv("ENABLE_POINT_MUTATION_INTERPRETER");
            }
            if (cache.has_value()) {
                setenv("ENABLE_POINT_PROGRAM_CACHE", cache->c_str(), 1);
            } else {
                unsetenv("ENABLE_POINT_PROGRAM_CACHE");
            }
        }
    } restore{saved, previous_cache == nullptr ? std::nullopt : std::optional<std::string>(previous_cache)};
    setenv("ENABLE_POINT_MUTATION_INTERPRETER", "0", 1);
    setenv("ENABLE_POINT_PROGRAM_CACHE", "1", 1);

    ASSERT_NO_THROW(db_->exec_sql("create table compiled_mutation (id int, value int, note char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("create index compiled_mutation(id);"));
    ASSERT_NO_THROW(db_->exec_sql("insert into compiled_mutation values(0, 0, 'legacy');"));
    EXPECT_FALSE(db_->last_compiled_mutation());
    setenv("ENABLE_POINT_MUTATION_INTERPRETER", "1", 1);

    ASSERT_NO_THROW(db_->exec_sql("insert into compiled_mutation values(1, 10, 'one');"));
    EXPECT_TRUE(db_->last_compiled_mutation());
    EXPECT_TRUE(db_->last_compiled_mutation_valid()) << db_->last_compiled_mutation_error();
    const auto before_cached_insert = db_->frontend_entries();
    ASSERT_NO_THROW(db_->exec_sql("insert into compiled_mutation values(2, 20, 'two');"));
    EXPECT_TRUE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries(), before_cached_insert);
    ASSERT_NO_THROW(db_->exec_sql("insert into compiled_mutation values(3, 20, 'three');"));
    const auto before_float_residual_hit = db_->frontend_entries();
    ASSERT_NO_THROW(
        db_->exec_sql("update compiled_mutation set note = 'float1' where id = 3 and value = 20.9;"));
    ASSERT_NO_THROW(
        db_->exec_sql("update compiled_mutation set note = 'float2' where id = 3 and value = 20.1;"));
    EXPECT_TRUE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries()[0], before_float_residual_hit[0] + 1);

    ASSERT_NO_THROW(
        db_->exec_sql("update compiled_mutation set value = value * 2, note = 'twenty' where id = 1;"));
    EXPECT_TRUE(db_->last_compiled_mutation());
    EXPECT_TRUE(db_->last_compiled_mutation_valid()) << db_->last_compiled_mutation_error();

    ASSERT_NO_THROW(db_->exec_sql("create table cached_datetime (id int, created_at datetime);"));
    ASSERT_NO_THROW(db_->exec_sql("create index cached_datetime(id);"));
    ASSERT_NO_THROW(db_->exec_sql("insert into cached_datetime values(1, '2026-07-01 00:00:00');"));
    ASSERT_NO_THROW(db_->exec_sql("update cached_datetime set created_at = '2026-07-02 00:00:00' "
                                  "where id = 1 and created_at = '2026-07-01 00:00:00';"));
    const auto before_datetime_hit = db_->frontend_entries();
    ASSERT_NO_THROW(db_->exec_sql("update cached_datetime set created_at = '2026-07-03 00:00:00' "
                                  "where id = 1 and created_at = '2026-07-02 00:00:00';"));
    EXPECT_TRUE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries(), before_datetime_hit);
    auto update_program = db_->last_compiled_mutation_program();
    ASSERT_NE(update_program, nullptr);
    size_t prepare_pc = update_program->instructions().size();
    uint32_t refreshed_tuple_reg = compiled::kNoOperand;
    for (size_t pc = 0; pc < update_program->instructions().size(); ++pc) {
        if (update_program->instructions()[pc].opcode == compiled::Opcode::PREPARE_UPDATE) {
            prepare_pc = pc;
            refreshed_tuple_reg = update_program->instructions()[pc].rhs;
            break;
        }
    }
    ASSERT_LT(prepare_pc, update_program->instructions().size());
    for (size_t pc = 0; pc < update_program->instructions().size(); ++pc) {
        const auto& instruction = update_program->instructions()[pc];
        if (instruction.opcode == compiled::Opcode::LOAD_COLUMN) {
            EXPECT_GT(pc, prepare_pc);
            EXPECT_EQ(instruction.lhs, refreshed_tuple_reg);
        }
    }
    ASSERT_NO_THROW(db_->exec_sql("update compiled_mutation set value += 3 where id = 1;"));
    EXPECT_TRUE(db_->last_compiled_mutation());
    EXPECT_TRUE(db_->last_compiled_mutation_valid()) << db_->last_compiled_mutation_error();
    const auto before_cached_update = db_->frontend_entries();
    ASSERT_NO_THROW(db_->exec_sql("update compiled_mutation set value += 4 where id = 1;"));
    EXPECT_TRUE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries(), before_cached_update);

    ASSERT_NO_THROW(db_->exec_sql("update compiled_mutation set value = 999 where id = 1 and value = 77;"));
    EXPECT_TRUE(db_->last_compiled_mutation());
    EXPECT_TRUE(db_->last_compiled_mutation_valid()) << db_->last_compiled_mutation_error();

    ASSERT_NO_THROW(db_->exec_sql("update compiled_mutation set note = 'changed' where id > 0;"));
    EXPECT_FALSE(db_->last_compiled_mutation());

    ASSERT_NO_THROW(db_->exec_sql("begin;"));
    ASSERT_NO_THROW(db_->exec_sql("update compiled_mutation set value = 100 where id = 1;"));
    EXPECT_TRUE(db_->last_compiled_mutation());
    EXPECT_TRUE(db_->last_compiled_mutation_valid()) << db_->last_compiled_mutation_error();
    ASSERT_NO_THROW(db_->exec_sql("rollback;"));

    EXPECT_THROW(db_->exec_sql("update compiled_mutation set id = 2 where id = 1;"), RMDBError);
    EXPECT_TRUE(db_->last_compiled_mutation());
    EXPECT_TRUE(db_->last_compiled_mutation_valid()) << db_->last_compiled_mutation_error();

    ASSERT_NO_THROW(db_->exec_sql("delete from compiled_mutation where id = 99;"));
    EXPECT_TRUE(db_->last_compiled_mutation());
    EXPECT_TRUE(db_->last_compiled_mutation_valid());
    const auto before_cached_delete = db_->frontend_entries();
    ASSERT_NO_THROW(db_->exec_sql("delete from compiled_mutation where id = 98;"));
    EXPECT_TRUE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries(), before_cached_delete);

    ASSERT_NO_THROW(db_->exec_sql("delete from compiled_mutation where id = 1 and value = 27;"));
    EXPECT_TRUE(db_->last_compiled_mutation());
    EXPECT_TRUE(db_->last_compiled_mutation_valid());

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select id from compiled_mutation where id = 1;"); });
    EXPECT_NE(output.find("Total record(s): 0"), std::string::npos) << output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select value from compiled_mutation where id = 2;"); });
    EXPECT_NE(output.find("20"), std::string::npos) << output;
}

TEST_F(E2ETest, CachedMutationFallsBackOutsideReadCommitted) {
    const char* previous_mutation = std::getenv("ENABLE_POINT_MUTATION_INTERPRETER");
    const char* previous_cache = std::getenv("ENABLE_POINT_PROGRAM_CACHE");
    struct RestoreFlags {
        std::optional<std::string> mutation;
        std::optional<std::string> cache;
        ~RestoreFlags() {
            mutation.has_value() ? setenv("ENABLE_POINT_MUTATION_INTERPRETER", mutation->c_str(), 1)
                                 : unsetenv("ENABLE_POINT_MUTATION_INTERPRETER");
            cache.has_value() ? setenv("ENABLE_POINT_PROGRAM_CACHE", cache->c_str(), 1)
                              : unsetenv("ENABLE_POINT_PROGRAM_CACHE");
        }
    } restore{previous_mutation == nullptr ? std::nullopt : std::optional<std::string>(previous_mutation),
              previous_cache == nullptr ? std::nullopt : std::optional<std::string>(previous_cache)};
    setenv("ENABLE_POINT_MUTATION_INTERPRETER", "1", 1);
    setenv("ENABLE_POINT_PROGRAM_CACHE", "1", 1);

    ASSERT_NO_THROW(db_->exec_sql("create table cached_isolation (id int, value int);"));
    ASSERT_NO_THROW(db_->exec_sql("create index cached_isolation(id);"));
    ASSERT_NO_THROW(db_->exec_sql("insert into cached_isolation values(1, 10);"));
    ASSERT_NO_THROW(db_->exec_sql("update cached_isolation set value = 11 where id = 1;"));
    ASSERT_NO_THROW(db_->exec_sql("update cached_isolation set value = 12 where id = 1;"));
    ASSERT_TRUE(db_->last_top_level_program_hit());

    ASSERT_NO_THROW(db_->exec_sql("set transaction isolation level snapshot isolation;"));
    const auto before_fallback = db_->frontend_entries();
    const auto stats_before_fallback = db_->point_program_template_cache_stats();
    ASSERT_NO_THROW(db_->exec_sql("update cached_isolation set value = 13 where id = 1;"));
    EXPECT_FALSE(db_->last_top_level_program_hit());
    EXPECT_FALSE(db_->last_compiled_mutation());
    const auto after_fallback = db_->frontend_entries();
    for (size_t i = 0; i < before_fallback.size(); ++i) {
        EXPECT_EQ(after_fallback[i], before_fallback[i] + 1);
    }
    EXPECT_EQ(db_->point_program_template_cache_stats().fallbacks, stats_before_fallback.fallbacks + 1);

    std::string output;
    ASSERT_NO_THROW({ output = db_->exec_sql("select value from cached_isolation where id = 1;"); });
    EXPECT_NE(output.find("13"), std::string::npos) << output;
}

TEST_F(E2ETest, PointProgramCacheHitBypassesFrontendAndBindsCurrentLexicalSlots) {
    const char* previous_select = std::getenv("ENABLE_POINT_SELECT_INTERPRETER");
    const char* previous_cache = std::getenv("ENABLE_POINT_PROGRAM_CACHE");
    struct RestoreFlags {
        std::optional<std::string> select;
        std::optional<std::string> cache;
        ~RestoreFlags() {
            select.has_value() ? setenv("ENABLE_POINT_SELECT_INTERPRETER", select->c_str(), 1)
                               : unsetenv("ENABLE_POINT_SELECT_INTERPRETER");
            cache.has_value() ? setenv("ENABLE_POINT_PROGRAM_CACHE", cache->c_str(), 1)
                              : unsetenv("ENABLE_POINT_PROGRAM_CACHE");
        }
    } restore{previous_select == nullptr ? std::nullopt : std::optional<std::string>(previous_select),
              previous_cache == nullptr ? std::nullopt : std::optional<std::string>(previous_cache)};
    setenv("ENABLE_POINT_SELECT_INTERPRETER", "1", 1);
    setenv("ENABLE_POINT_PROGRAM_CACHE", "1", 1);

    ASSERT_NO_THROW(db_->exec_sql("create table cached_select (id int, note char(8));"));
    ASSERT_NO_THROW(db_->exec_sql("create index cached_select(id);"));
    ASSERT_NO_THROW(db_->exec_sql("insert into cached_select values(1, 'one');"));
    ASSERT_NO_THROW(db_->exec_sql("insert into cached_select values(2, 'two');"));

    std::string output;
    const auto before_first = db_->frontend_entries();
    const auto entries_before_first = db_->point_program_template_cache_stats().entries;
    ASSERT_NO_THROW({ output = db_->exec_sql("select note from cached_select where id = 1;"); });
    EXPECT_NE(output.find("one"), std::string::npos) << output;
    EXPECT_FALSE(db_->last_top_level_program_hit());
    const auto after_first = db_->frontend_entries();
    EXPECT_EQ(db_->point_program_template_cache_stats().entries, entries_before_first + 1);
    for (size_t i = 0; i < before_first.size(); ++i) {
        EXPECT_EQ(after_first[i], before_first[i] + 1);
    }

    ASSERT_NO_THROW({ output = db_->exec_sql("select note from cached_select where id = 2;"); });
    EXPECT_NE(output.find("two"), std::string::npos) << output;
    EXPECT_EQ(output.find("one"), std::string::npos) << output;
    auto stats = db_->point_program_template_cache_stats();
    EXPECT_TRUE(db_->last_top_level_program_hit()) << "hits=" << stats.hits << " misses=" << stats.misses
                                                  << " fallbacks=" << stats.fallbacks;
    EXPECT_EQ(db_->frontend_entries(), after_first);
    stats = db_->point_program_template_cache_stats();
    EXPECT_GE(stats.hits, 1U);
    EXPECT_GE(stats.handled, 1U);

    ASSERT_NO_THROW(db_->exec_sql("create table cache_generation_bump (v int);"));
    const auto after_ddl = db_->frontend_entries();
    ASSERT_NO_THROW({ output = db_->exec_sql("select note from cached_select where id = 1;"); });
    EXPECT_FALSE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries()[0], after_ddl[0] + 1);

    setenv("ENABLE_POINT_PROGRAM_CACHE", "0", 1);
    const auto before_disabled = db_->frontend_entries();
    const auto stats_before_disabled = db_->point_program_template_cache_stats();
    ASSERT_NO_THROW({ output = db_->exec_sql("select note from cached_select where id = 2;"); });
    EXPECT_FALSE(db_->last_top_level_program_hit());
    EXPECT_EQ(db_->frontend_entries()[0], before_disabled[0] + 1);
    const auto stats_after_disabled = db_->point_program_template_cache_stats();
    EXPECT_EQ(stats_after_disabled.hits, stats_before_disabled.hits);
    EXPECT_EQ(stats_after_disabled.misses, stats_before_disabled.misses);
    EXPECT_EQ(stats_after_disabled.handled, stats_before_disabled.handled);
    EXPECT_EQ(stats_after_disabled.fallbacks, stats_before_disabled.fallbacks);
    EXPECT_EQ(stats_after_disabled.entries, stats_before_disabled.entries);
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
