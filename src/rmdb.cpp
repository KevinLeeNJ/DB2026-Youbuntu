/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <netinet/in.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <exception>
#include <fstream>
#include <functional>
#include <iomanip>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/runtime_config.h"
#include "errors.h"
#include "execution/execution_timing_diagnostics.h"
#include "execution/index_skip_scan_diagnostics.h"
#include "execution/prepared_jit.h"
#include "index/ix_scan.h"
#include "minilog.h"
#include "optimizer/optimizer.h"
#include "recovery/checkpoint_manager.h"
#include "recovery/log_recovery.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "analyze/analyze.h"
#include "protocol/wire_protocol.h"
#include "transaction/commit_timing_diagnostics.h"

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 128

static bool should_exit = false;

namespace {

BufferPoolRuntimeConfig load_buffer_pool_config_or_exit() {
    BufferPoolRuntimeConfig config;
    try {
        const char* configured_gib = std::getenv("RMDB_BUFFER_POOL_GIB");
        config = configured_gib == nullptr ? default_buffer_pool_config() : parse_buffer_pool_config(configured_gib);
    } catch (const std::exception& error) {
        std::fprintf(stderr, "[rmdb] configuration error: %s\n", error.what());
        std::fflush(stderr);
        std::exit(EXIT_FAILURE);
    }
    std::fprintf(stderr, "[rmdb] buffer pool: %zu GiB (%zu pages)\n", config.gibibytes, config.pages);
    std::fflush(stderr);
    return config;
}

} // namespace

// 构建全局所需的管理器对象
const auto buffer_pool_config = load_buffer_pool_config_or_exit();
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(buffer_pool_config.pages, disk_manager.get());
auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
auto ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
auto sm_manager =
    std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
auto lock_manager = std::make_unique<LockManager>();
auto txn_manager = std::make_unique<TransactionManager>(lock_manager.get(), sm_manager.get());
auto planner = std::make_unique<Planner>(sm_manager.get());
auto optimizer = std::make_unique<Optimizer>(sm_manager.get(), planner.get());
auto ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get(), planner.get());
// The server must not acknowledge a commit before the WAL is durable.  Keep
// PROCESS_CRASH available to focused LogManager tests, but never let an
// omitted or misspelled environment variable weaken the production path.
auto log_manager = std::make_unique<LogManager>(disk_manager.get(), DurabilityMode::STRICT);
auto recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(), sm_manager.get(),
                                                  log_manager.get());

auto portal = std::make_unique<Portal>(sm_manager.get());
auto analyze = std::make_unique<Analyze>(sm_manager.get());

// SIGUSR1 asks the server to publish its WAL/group-commit counters once. A
// signal handler may not format or log, and `final.md:416` forbids per-
// transaction logging on a measured run, so the handler only increments this
// lock-free request counter. The observability thread consumes every request
// and is the only path that flushes the logger for these snapshots.
static std::atomic<uint64_t> observability_requests{0};
static_assert(std::atomic<uint64_t>::is_always_lock_free,
              "SIGUSR1 observability requests require a lock-free atomic counter");
static std::atomic<uint64_t> observability_sequence{0};

void sigusr1_handler(int signo) {
    (void)signo;
    observability_requests.fetch_add(1, std::memory_order_relaxed);
}

// Counters are monotonic since startup: take one line before and one after a
// measurement window and subtract to get rates for that window alone.
void log_wal_statistics() {
    LOG_WARN("walstats t_ms=%lld commits=%llu fsyncs=%llu leaders=%llu batches=%llu waiters=%llu wait_ns=%llu "
             "pwrites=%llu pwrite_bytes=%llu write_ns=%llu fsync_ns=%llu txnmap_lookups=%llu",
             static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count()),
             static_cast<unsigned long long>(log_manager->get_commit_count()),
             static_cast<unsigned long long>(log_manager->get_fsync_count()),
             static_cast<unsigned long long>(log_manager->get_group_commit_leader_count()),
             static_cast<unsigned long long>(log_manager->get_group_commit_count()),
             static_cast<unsigned long long>(log_manager->get_group_commit_waiter_count()),
             static_cast<unsigned long long>(log_manager->get_group_commit_wait_ns()),
             static_cast<unsigned long long>(log_manager->get_pwrite_count()),
             static_cast<unsigned long long>(log_manager->get_pwrite_bytes()),
             static_cast<unsigned long long>(log_manager->get_wal_write_ns()),
             static_cast<unsigned long long>(log_manager->get_wal_fsync_ns()),
             static_cast<unsigned long long>(txn_manager->DebugTxnMapLookupCount()));
}

void log_observability_statistics() {
    const uint64_t sequence = observability_sequence.fetch_add(1, std::memory_order_relaxed) + 1;
    const auto now_ms = static_cast<long long>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
    const auto aborts = txn_manager->abort_observability();
    const auto record_locks = lock_manager->record_lock_observability();
    const auto unique_locks = lock_manager->unique_key_lock_observability();
    const auto checkpoint = txn_manager->checkpoint_observability();
    const auto bpm = buffer_pool_manager->observability_snapshot();

    LOG_WARN("obs_abort t_ms=%lld seq=%llu shrinking=%llu upgrade=%llu deadlock=%llu ww=%llu ssi=%llu unique=%llu",
             now_ms, static_cast<unsigned long long>(sequence),
             static_cast<unsigned long long>(aborts.lock_on_shrinking),
             static_cast<unsigned long long>(aborts.upgrade_conflict),
             static_cast<unsigned long long>(aborts.deadlock_prevention),
             static_cast<unsigned long long>(aborts.ww_conflict), static_cast<unsigned long long>(aborts.ssi_danger),
             static_cast<unsigned long long>(aborts.unique_key_conflict));
    const auto log_lock = [now_ms, sequence](const char* kind, const LockObservabilitySnapshot& stats) {
        LOG_WARN(
            "obs_lock t_ms=%lld seq=%llu kind=%s immediate_conflict=%llu "
            "wait_enqueued=%llu wait_granted=%llu wait_cancelled=%llu wait_ns=%llu backoff_waits=%llu "
            "completion_waits=%llu "
            "completion_aborts=%llu queue_depth_max=%llu cycle_checks=%llu cycle_victims=%llu",
            now_ms, static_cast<unsigned long long>(sequence), kind,
            static_cast<unsigned long long>(stats.immediate_conflict),
            static_cast<unsigned long long>(stats.wait_enqueued), static_cast<unsigned long long>(stats.wait_granted),
            static_cast<unsigned long long>(stats.wait_cancelled), static_cast<unsigned long long>(stats.wait_ns),
            static_cast<unsigned long long>(stats.backoff_waits),
            static_cast<unsigned long long>(stats.completion_waits),
            static_cast<unsigned long long>(stats.completion_aborts),
            static_cast<unsigned long long>(stats.queue_depth_max), static_cast<unsigned long long>(stats.cycle_checks),
            static_cast<unsigned long long>(stats.cycle_victims));
    };
    log_lock("record", record_locks);
    log_lock("unique", unique_locks);
    LOG_WARN(
        "obs_ckpt t_ms=%lld seq=%llu attempt=%llu preflush=%llu success=%llu drain_timeout=%llu deadline=%llu "
        "final_data_fail=%llu initial_ns=%llu preblock_ns=%llu block_ns=%llu drain_ns=%llu final_wal_ns=%llu "
        "final_data_ns=%llu meta_ns=%llu manifest_ns=%llu truncate_ns=%llu begin_blocked=%llu begin_wait_ns=%llu",
        now_ms, static_cast<unsigned long long>(sequence), static_cast<unsigned long long>(checkpoint.attempt),
        static_cast<unsigned long long>(checkpoint.preflush), static_cast<unsigned long long>(checkpoint.success),
        static_cast<unsigned long long>(checkpoint.drain_timeout), static_cast<unsigned long long>(checkpoint.deadline),
        static_cast<unsigned long long>(checkpoint.final_data_fail),
        static_cast<unsigned long long>(checkpoint.initial_ns), static_cast<unsigned long long>(checkpoint.preblock_ns),
        static_cast<unsigned long long>(checkpoint.block_ns), static_cast<unsigned long long>(checkpoint.drain_ns),
        static_cast<unsigned long long>(checkpoint.final_wal_ns),
        static_cast<unsigned long long>(checkpoint.final_data_ns), static_cast<unsigned long long>(checkpoint.meta_ns),
        static_cast<unsigned long long>(checkpoint.manifest_ns),
        static_cast<unsigned long long>(checkpoint.truncate_ns),
        static_cast<unsigned long long>(checkpoint.begin_blocked),
        static_cast<unsigned long long>(checkpoint.begin_wait_ns));
    LOG_WARN("obs_bpm t_ms=%lld seq=%llu fetch_miss=%llu inflight_wait=%llu inflight_wait_ns=%llu no_victim=%llu "
             "eviction_clean=%llu eviction_dirty=%llu page_reads=%llu page_writes=%llu",
             now_ms, static_cast<unsigned long long>(sequence), static_cast<unsigned long long>(bpm.fetch_miss),
             static_cast<unsigned long long>(bpm.inflight_wait), static_cast<unsigned long long>(bpm.inflight_wait_ns),
             static_cast<unsigned long long>(bpm.no_victim), static_cast<unsigned long long>(bpm.eviction_clean),
             static_cast<unsigned long long>(bpm.eviction_dirty),
             static_cast<unsigned long long>(disk_manager->get_page_read_count()),
             static_cast<unsigned long long>(disk_manager->get_page_write_count()));
    const auto jit_stats = prepared_jit_compiler().stats();
    LOG_WARN(
        "obs_jit t_ms=%lld seq=%llu attempts=%llu compiled=%llu unsupported=%llu failures=%llu code_bytes=%llu", now_ms,
        static_cast<unsigned long long>(sequence), static_cast<unsigned long long>(jit_stats.attempts),
        static_cast<unsigned long long>(jit_stats.compiled), static_cast<unsigned long long>(jit_stats.unsupported),
        static_cast<unsigned long long>(jit_stats.failures), static_cast<unsigned long long>(jit_stats.code_bytes));
    if (commit_timing_diagnostics::enabled()) {
        const auto commit = commit_timing_diagnostics::snapshot();
        LOG_WARN("obs_commit t_ms=%lld seq=%llu invocations=%llu failures=%llu total_ns=%llu prepare_ns=%llu "
                 "timestamp_csn_ns=%llu wal_ns=%llu tuple_ns=%llu frontier_publish_ns=%llu frontier_wait_ns=%llu "
                 "cleanup_ns=%llu",
                 now_ms, static_cast<unsigned long long>(sequence), static_cast<unsigned long long>(commit.invocations),
                 static_cast<unsigned long long>(commit.failures), static_cast<unsigned long long>(commit.total_ns),
                 static_cast<unsigned long long>(commit.prepare_publication_ns),
                 static_cast<unsigned long long>(commit.timestamp_csn_ns),
                 static_cast<unsigned long long>(commit.wal_ns),
                 static_cast<unsigned long long>(commit.tuple_publication_ns),
                 static_cast<unsigned long long>(commit.frontier_publication_ns),
                 static_cast<unsigned long long>(commit.frontier_wait_ns),
                 static_cast<unsigned long long>(commit.cleanup_ns));
    }
    if (index_skip_scan_diagnostics::enabled()) {
        for (const auto& stats : index_skip_scan_diagnostics::snapshots()) {
            LOG_WARN("obs_skipscan t_ms=%lld seq=%llu statement_id=%u plan_hash=%llu table=%s index=%s prefix_cols=%zu "
                     "invocations=%llu prefixes=%llu ranges=%llu descents=%llu build_ns=%llu build_ns_max=%llu",
                     now_ms, static_cast<unsigned long long>(sequence), static_cast<unsigned int>(stats.statement_id),
                     static_cast<unsigned long long>(stats.plan_hash), stats.table_name.c_str(),
                     stats.index_name.c_str(), stats.prefix_column_count,
                     static_cast<unsigned long long>(stats.invocations),
                     static_cast<unsigned long long>(stats.prefixes), static_cast<unsigned long long>(stats.ranges),
                     static_cast<unsigned long long>(stats.descents), static_cast<unsigned long long>(stats.build_ns),
                     static_cast<unsigned long long>(stats.build_ns_max));
        }
    }
    if (execution_timing_diagnostics::enabled()) {
        for (const auto& stats : execution_timing_diagnostics::snapshots()) {
            LOG_WARN(
                "obs_exec t_ms=%lld seq=%llu statement_id=%u plan_hash=%llu route=%s fallback_reason=%s "
                "invocations=%llu failures=%llu aborts=%llu errors=%llu total_ns=%llu bind_ns=%llu "
                "clone_bind_ns=%llu analyze_ns=%llu plan_ns=%llu instantiate_ns=%llu portal_run_ns=%llu "
                "executor_constructed=%llu executor_reused=%llu",
                now_ms, static_cast<unsigned long long>(sequence), static_cast<unsigned int>(stats.statement_id),
                static_cast<unsigned long long>(stats.plan_hash), execution_timing_diagnostics::route_name(stats.route),
                execution_timing_diagnostics::fallback_reason_name(stats.fallback_reason),
                static_cast<unsigned long long>(stats.invocations), static_cast<unsigned long long>(stats.failures),
                static_cast<unsigned long long>(stats.aborts), static_cast<unsigned long long>(stats.errors),
                static_cast<unsigned long long>(stats.total_ns), static_cast<unsigned long long>(stats.bind_ns),
                static_cast<unsigned long long>(stats.clone_bind_ns), static_cast<unsigned long long>(stats.analyze_ns),
                static_cast<unsigned long long>(stats.plan_ns), static_cast<unsigned long long>(stats.instantiate_ns),
                static_cast<unsigned long long>(stats.portal_run_ns),
                static_cast<unsigned long long>(stats.executor_constructed),
                static_cast<unsigned long long>(stats.executor_reused));
        }
    }
    minilog::Logger::get().flush();
}

static jmp_buf jmpbuf;
void sigint_handler(int signo) {
    (void)signo;
    should_exit = true;
    log_manager->flush_log_to_disk_with_sync();
    LOG_INFO("the server received Ctrl+C and will close");
    longjmp(jmpbuf, 1);
}

namespace {
using wire_protocol::Reader;
using wire_protocol::Tag;
using wire_protocol::Type;
using wire_protocol::Value;
using wire_protocol::Writer;

Type protocol_type(ColType type) {
    return type == TYPE_INT ? Type::INT32 : type == TYPE_FLOAT ? Type::FLOAT32 : Type::CHAR;
}

bool changes_catalog(ast::AstType type) {
    return type == ast::AstType::CreateTable || type == ast::AstType::DropTable || type == ast::AstType::CreateIndex ||
           type == ast::AstType::DropIndex || type == ast::AstType::LoadStmt;
}

bool descriptor_runtime_eligible(const PreparedPlanDescriptor* descriptor) {
    if (descriptor == nullptr || !descriptor->eligible()) {
        return false;
    }
    if (descriptor->statement_kind() == PreparedStatementKind::Update) {
        const DMLPlan* dml = descriptor->dml_plan();
        return dml != nullptr && dml->compiled_point_program_ == nullptr && dml->subplan_ != nullptr;
    }
    return true;
}

std::vector<Value> protocol_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) {
    std::vector<Value> row;
    row.reserve(columns.size());
    for (const auto& column : columns) {
        if (column.offset < 0 || static_cast<std::size_t>(column.offset) + column.len > size ||
            static_cast<std::size_t>(column.null_byte + 1) > size) {
            throw wire_protocol::ProtocolError("executor returned an invalid tuple");
        }
        Value value;
        value.type = protocol_type(column.type);
        const char* cell = data + column.offset;
        if (is_null(data, column)) {
            // present == 0 后不写任何值字节；NULL 不得编码为空字符串（final.md:761）
            value.present = false;
            row.push_back(std::move(value));
            continue;
        }
        if (column.type == TYPE_INT) {
            value.int32 = read_unaligned<int>(cell);
        } else if (column.type == TYPE_FLOAT) {
            float number = read_float(cell);
            std::memcpy(&value.float_bits, &number, sizeof(value.float_bits));
        } else {
            value.text.assign(cell, strnlen(cell, column.len));
        }
        row.push_back(std::move(value));
    }
    return row;
}

struct ProtocolCollector : QueryResultSink {
    std::vector<std::string> names;
    std::vector<Type> types;
    std::vector<std::vector<Value>> rows;
    bool query = false;
    // A batch response is a single frame.  When this pointer is set, rows
    // charge their encoded size against the batch-wide budget while the
    // executor is still running, instead of discovering an oversized result
    // only after the transaction has completed.
    std::size_t* encoded_bytes = nullptr;
    std::size_t encoded_limit = 0;

    void begin_query(const std::vector<ColMeta>& columns, const std::vector<std::string>& output_names) override {
        query = true;
        names = output_names;
        types.clear();
        for (const auto& column : columns) {
            types.push_back(protocol_type(column.type));
        }
    }

    void append_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) override {
        std::vector<Value> row = protocol_row(columns, data, size);
        if (encoded_bytes != nullptr) {
            Writer encoded_row;
            for (std::size_t i = 0; i < row.size(); ++i) {
                wire_protocol::encode_value(encoded_row, row[i], types[i]);
            }
            if (*encoded_bytes > encoded_limit || encoded_row.data().size() > encoded_limit - *encoded_bytes) {
                throw wire_protocol::ProtocolError("batch result exceeds protocol limit");
            }
            *encoded_bytes += encoded_row.data().size();
        }
        rows.push_back(std::move(row));
    }
};

struct SessionState {
    txn_id_t txn_id = INVALID_TXN_ID;
    IsolationLevel isolation = DEFAULT_ISOLATION_LEVEL;
    bool output_file_enabled = false;

    // Every operation used to resolve txn_id through TransactionManager's global
    // txn_map under a single process-wide mutex; at 50 connections and 57
    // operations per NewOrder that lookup cost 41.3 us per operation, four times
    // the entire compile pipeline. A session executes its operations one at a
    // time, so it can simply remember the transaction it is running.
    //
    // Lifetime rule, and the only reason this is safe: a Transaction object may
    // be freed by RetireTransactionIfSafe/GC the moment it reaches COMMITTED or
    // ABORTED, so a cached pointer must never outlive its running transaction.
    // The cache is therefore keyed by the id it was taken for and is only ever
    // consulted through running_transaction(), which drops it as soon as txn_id
    // changes — which is what ending a transaction does, whether this file ends
    // it (execute_tree/abort_session) or COMMIT/ROLLBACK/ABORT ends it through
    // the txn_id pointer handed to portal->run(). Nothing else can retire a
    // transaction this session is still running.
    Transaction* running_txn = nullptr;
    txn_id_t running_txn_id = INVALID_TXN_ID;

    Transaction* running_transaction() {
        if (running_txn == nullptr || running_txn_id != txn_id) {
            forget_running_transaction();
            return nullptr;
        }
        return running_txn;
    }

    // `txn` must not have reached COMMITTED or ABORTED yet.
    void remember_running_transaction(Transaction* txn) {
        running_txn = txn;
        running_txn_id = txn->get_transaction_id();
    }

    void forget_running_transaction() {
        running_txn = nullptr;
        running_txn_id = INVALID_TXN_ID;
    }
};

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(SessionState& session, Context* context) {
    Transaction* txn = session.running_transaction();
    if (txn == nullptr) {
        txn = txn_manager->get_transaction(session.txn_id);
        if (txn == nullptr || txn->get_state() == TransactionState::COMMITTED ||
            txn->get_state() == TransactionState::ABORTED) {
            txn = txn_manager->begin(nullptr, context->log_mgr_, context->isolation_level_);
            session.txn_id = txn->get_transaction_id();
            txn->set_txn_mode(false);
            txn->set_isolation_level(context->isolation_level_);
        }
        session.remember_running_transaction(txn);
    }
    context->txn_ = txn;
    txn_manager->BeginStatement(txn);
}

struct PreparedStatement {
    std::uint16_t id = 0;
    bool query = false;
    std::unique_ptr<ast::TreeNode> template_tree;
    std::vector<Type> parameters;
    std::vector<std::string> names;
    std::vector<Type> result_types;
    std::vector<std::string> diagnostic_plans;
    std::uint64_t diagnostic_plan_hash = 0;
    std::unique_ptr<const PreparedPlanDescriptor> descriptor;
    std::unique_ptr<PreparedJitProgram> jit_program;
    std::string database_identity;
    std::uint64_t catalog_generation = 0;
};

const char* diagnostic_plan_tag(PlanTag tag) {
    switch (tag) {
    case T_Insert:
        return "Insert";
    case T_Update:
        return "Update";
    case T_Delete:
        return "Delete";
    case T_select:
        return "Select";
    case T_SeqScan:
        return "SeqScan";
    case T_IndexScan:
        return "IndexScan";
    case T_IndexSkipScan:
        return "IndexSkipScan";
    case T_Filter:
        return "Filter";
    case T_NestLoop:
        return "NestedLoop";
    case T_SortMerge:
        return "SortMerge";
    case T_Sort:
        return "Sort";
    case T_Projection:
        return "Projection";
    case T_Aggregate:
        return "Aggregate";
    case T_Limit:
        return "Limit";
    case T_Union:
        return "Union";
    case T_ExplainAnalyze:
        return "ExplainAnalyze";
    case T_Transaction_begin:
        return "Begin";
    case T_Transaction_commit:
        return "Commit";
    case T_Transaction_abort:
        return "Abort";
    case T_Transaction_rollback:
        return "Rollback";
    default:
        return "Utility";
    }
}

std::string join_diagnostic_names(const std::vector<std::string>& names) {
    if (names.empty()) {
        return "-";
    }
    std::ostringstream out;
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (i != 0) {
            out << ',';
        }
        out << names[i];
    }
    return out.str();
}

std::string scan_equality_mask(const ScanPlan& scan) {
    if (scan.index_col_names_.empty()) {
        return "-";
    }
    std::string mask;
    mask.reserve(scan.index_col_names_.size());
    for (const auto& index_col : scan.index_col_names_) {
        const bool has_equality = std::any_of(scan.conds_.begin(), scan.conds_.end(), [&](const Condition& condition) {
            return condition.is_rhs_val && condition.op == OP_EQ && condition.lhs_col.col_name == index_col;
        });
        mask.push_back(has_equality ? '1' : '0');
    }
    return mask;
}

void collect_scan_diagnostics(const Plan* plan, const char* root, std::uint16_t statement_id,
                              std::vector<std::string>& mappings) {
    if (plan == nullptr) {
        return;
    }
    switch (plan->tag) {
    case T_SeqScan:
    case T_IndexScan:
    case T_IndexSkipScan: {
        const auto* scan = static_cast<const ScanPlan*>(plan);
        std::ostringstream line;
        line << "statement_id=" << statement_id << " root=" << root << " node=" << diagnostic_plan_tag(plan->tag)
             << " table=" << scan->tab_name_ << " index_cols=" << join_diagnostic_names(scan->index_col_names_)
             << " equality_mask=" << scan_equality_mask(*scan);
        mappings.push_back(line.str());
        return;
    }
    case T_select:
    case T_ExplainAnalyze:
    case T_Update:
    case T_Delete:
    case T_Insert:
        collect_scan_diagnostics(static_cast<const DMLPlan*>(plan)->subplan_.get(), root, statement_id, mappings);
        return;
    case T_Filter:
        collect_scan_diagnostics(static_cast<const FilterPlan*>(plan)->subplan_.get(), root, statement_id, mappings);
        return;
    case T_Projection:
        collect_scan_diagnostics(static_cast<const ProjectionPlan*>(plan)->subplan_.get(), root, statement_id,
                                 mappings);
        return;
    case T_Aggregate:
        collect_scan_diagnostics(static_cast<const AggregatePlan*>(plan)->subplan_.get(), root, statement_id, mappings);
        return;
    case T_Sort:
        collect_scan_diagnostics(static_cast<const SortPlan*>(plan)->subplan_.get(), root, statement_id, mappings);
        return;
    case T_Limit:
        collect_scan_diagnostics(static_cast<const LimitPlan*>(plan)->subplan_.get(), root, statement_id, mappings);
        return;
    case T_NestLoop:
    case T_SortMerge: {
        const auto* join = static_cast<const JoinPlan*>(plan);
        collect_scan_diagnostics(join->left_.get(), root, statement_id, mappings);
        collect_scan_diagnostics(join->right_.get(), root, statement_id, mappings);
        return;
    }
    case T_Union:
        for (const auto& branch : static_cast<const UnionPlan*>(plan)->branches_) {
            collect_scan_diagnostics(branch.get(), root, statement_id, mappings);
        }
        return;
    default:
        return;
    }
}

std::vector<std::string> describe_prepared_plan(std::uint16_t statement_id, const Plan* plan) {
    std::vector<std::string> mappings;
    collect_scan_diagnostics(plan, diagnostic_plan_tag(plan->tag), statement_id, mappings);
    if (mappings.empty()) {
        std::ostringstream line;
        line << "statement_id=" << statement_id << " root=" << diagnostic_plan_tag(plan->tag)
             << " node=None table=- index_cols=- equality_mask=-";
        mappings.push_back(line.str());
    }
    return mappings;
}

std::uint64_t hash_prepared_plan(const std::vector<std::string>& mappings) {
    // Stable FNV-1a is sufficient here: this is an attribution label, never a
    // plan-cache key or an execution decision. Exclude the connection-local
    // statement id so equal generic plans have the same diagnostic hash.
    std::uint64_t hash = 1469598103934665603ULL;
    for (const auto& mapping : mappings) {
        std::size_t plan_offset = 0;
        if (mapping.rfind("statement_id=", 0) == 0) {
            const auto delimiter = mapping.find(' ');
            if (delimiter != std::string::npos) {
                plan_offset = delimiter + 1;
            }
        }
        for (std::size_t i = plan_offset; i < mapping.size(); ++i) {
            const auto byte = static_cast<unsigned char>(mapping[i]);
            hash ^= byte;
            hash *= 1099511628211ULL;
        }
        hash ^= 0xff;
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::string diagnostic(const std::exception& exception) {
    std::string text = exception.what();
    if (text.size() > wire_protocol::kMaxDiagnostic) {
        text.resize(wire_protocol::kMaxDiagnostic);
    }
    return text;
}

bool is_valid_utf8(const std::string& text) {
    for (std::size_t i = 0; i < text.size();) {
        const auto first = static_cast<unsigned char>(text[i]);
        if (first <= 0x7f) {
            ++i;
            continue;
        }
        std::size_t width = 0;
        std::uint32_t code_point = 0;
        if (first >= 0xc2 && first <= 0xdf) {
            width = 2;
            code_point = first & 0x1f;
        } else if (first >= 0xe0 && first <= 0xef) {
            width = 3;
            code_point = first & 0x0f;
        } else if (first >= 0xf0 && first <= 0xf4) {
            width = 4;
            code_point = first & 0x07;
        } else {
            return false;
        }
        if (i + width > text.size()) {
            return false;
        }
        for (std::size_t j = 1; j < width; ++j) {
            const auto next = static_cast<unsigned char>(text[i + j]);
            if ((next & 0xc0) != 0x80) {
                return false;
            }
            code_point = (code_point << 6) | (next & 0x3f);
        }
        if ((width == 3 && code_point < 0x800) || (width == 4 && code_point < 0x10000) ||
            (code_point >= 0xd800 && code_point <= 0xdfff) || code_point > 0x10ffff) {
            return false;
        }
        i += width;
    }
    return true;
}

void abort_session(SessionState& session, Context* context) {
    Transaction* txn = context == nullptr ? nullptr : context->txn_;
    if (txn == nullptr) {
        txn = session.running_transaction();
    }
    if (txn == nullptr && session.txn_id != INVALID_TXN_ID) {
        txn = txn_manager->get_transaction(session.txn_id);
    }
    // The transaction is over either way, and abort may already have freed it.
    session.forget_running_transaction();
    if (txn != nullptr && txn->get_state() != TransactionState::ABORTED &&
        txn->get_state() != TransactionState::COMMITTED) {
        txn_manager->abort(txn, log_manager.get());
    }
    session.txn_id = INVALID_TXN_ID;
    if (context != nullptr) {
        context->txn_ = nullptr;
    }
}

struct ExecutionOutcome {
    bool query = false;
    bool catalog_changed = false;
};

template <bool TimingEnabled>
ExecutionOutcome execute_tree_impl(std::unique_ptr<ast::TreeNode> parse_tree, SessionState& session,
                                   QueryResultSink* result_sink, execution_timing_diagnostics::Sample* timing,
                                   bool catalog_guard_held = false) {
    std::vector<char> response(BUFFER_LENGTH, 0);
    int offset = 0;
    Context context(lock_manager.get(), log_manager.get(), nullptr, response.data(), &offset, txn_manager.get());
    context.isolation_level_ = session.isolation;
    context.result_sink_ = result_sink;
    context.output_file_enabled_ = &session.output_file_enabled;
    if (parse_tree == nullptr)
        return {};
    const auto parsed_type = parse_tree->type;
    const bool catalog_change = changes_catalog(parsed_type);
    if (catalog_change) {
        Transaction* active = session.running_transaction();
        if (active != nullptr && active->get_txn_mode() && active->get_state() != TransactionState::COMMITTED &&
            active->get_state() != TransactionState::ABORTED) {
            throw RMDBError("structural DDL and LOAD are not allowed inside an explicit transaction");
        }
    }
    std::optional<SmManager::CatalogSharedGuard> catalog_shared_guard;
    std::optional<SmManager::CatalogExclusiveGuard> catalog_exclusive_guard;
    if (!catalog_guard_held) {
        if (catalog_change) {
            catalog_exclusive_guard.emplace(sm_manager->acquire_catalog_exclusive());
        } else {
            catalog_shared_guard.emplace(sm_manager->acquire_catalog_shared());
        }
    }
    const bool is_checkpoint = parsed_type == ast::AstType::StaticCheckpoint;
    const bool is_load = parsed_type == ast::AstType::LoadStmt;
    if (!is_checkpoint && !is_load) {
        SetTransaction(session, &context);
    }
    try {
        std::unique_ptr<Query> query;
        if constexpr (TimingEnabled) {
            execution_timing_diagnostics::StageTimer timer(&timing->analyze_ns);
            query = analyze->do_analyze(std::move(parse_tree));
        } else {
            query = analyze->do_analyze(std::move(parse_tree));
        }
        std::unique_ptr<Plan> plan;
        if constexpr (TimingEnabled) {
            execution_timing_diagnostics::StageTimer timer(&timing->plan_ns);
            plan = optimizer->plan_query(std::move(query), &context);
        } else {
            plan = optimizer->plan_query(std::move(query), &context);
        }
        const bool catalog_changed = plan->tag == T_CreateTable || plan->tag == T_DropTable ||
                                     plan->tag == T_CreateIndex || plan->tag == T_DropIndex || plan->tag == T_LoadData;
        std::unique_ptr<PortalStmt> statement;
        if constexpr (TimingEnabled) {
            execution_timing_diagnostics::StageTimer timer(&timing->instantiate_ns);
            statement = portal->start(std::move(plan), &context, &timing->executor_constructed);
        } else {
            statement = portal->start(std::move(plan), &context);
        }
        const bool is_query = statement->tag == PORTAL_ONE_SELECT;
        if constexpr (TimingEnabled) {
            execution_timing_diagnostics::StageTimer timer(&timing->portal_run_ns);
            portal->run(std::move(statement), ql_manager.get(), &session.txn_id, &context);
        } else {
            portal->run(std::move(statement), ql_manager.get(), &session.txn_id, &context);
        }
        session.isolation = context.isolation_level_;
        portal->drop();
        if (context.txn_ != nullptr && !context.txn_->get_txn_mode() &&
            context.txn_->get_state() != TransactionState::COMMITTED &&
            context.txn_->get_state() != TransactionState::ABORTED) {
            txn_manager->commit(context.txn_, context.log_mgr_);
            // commit may already have freed the transaction object.
            session.forget_running_transaction();
            session.txn_id = INVALID_TXN_ID;
        }
        context.txn_ = nullptr;
        return {is_query, catalog_changed};
    } catch (TransactionAbortException& exception) {
        txn_manager->record_client_abort(exception.GetAbortReason());
        abort_session(session, &context);
        throw;
    } catch (...) {
        abort_session(session, &context);
        throw;
    }
}

ExecutionOutcome execute_tree(std::unique_ptr<ast::TreeNode> parse_tree, SessionState& session,
                              QueryResultSink* result_sink) {
    return execute_tree_impl<false>(std::move(parse_tree), session, result_sink, nullptr);
}

ExecutionOutcome execute_tree_under_catalog_guard(std::unique_ptr<ast::TreeNode> parse_tree, SessionState& session,
                                                  QueryResultSink* result_sink) {
    return execute_tree_impl<false>(std::move(parse_tree), session, result_sink, nullptr, true);
}

ExecutionOutcome execute_tree_timed(std::unique_ptr<ast::TreeNode> parse_tree, SessionState& session,
                                    QueryResultSink* result_sink, execution_timing_diagnostics::Sample* timing,
                                    bool catalog_guard_held = false) {
    return execute_tree_impl<true>(std::move(parse_tree), session, result_sink, timing, catalog_guard_held);
}

ParameterFrame make_parameter_frame(const std::vector<Value>& wire_values) {
    std::vector<::Value> values;
    values.reserve(wire_values.size());
    for (const auto& wire_value : wire_values) {
        ::Value value;
        value.type =
            wire_value.type == Type::INT32 ? TYPE_INT : (wire_value.type == Type::FLOAT32 ? TYPE_FLOAT : TYPE_STRING);
        if (!wire_value.present) {
            value.set_null();
            value.type = wire_value.type == Type::INT32 ? TYPE_INT
                                                        : (wire_value.type == Type::FLOAT32 ? TYPE_FLOAT : TYPE_STRING);
        } else if (wire_value.type == Type::INT32) {
            value.set_int(wire_value.int32);
        } else if (wire_value.type == Type::FLOAT32) {
            float number;
            std::memcpy(&number, &wire_value.float_bits, sizeof(number));
            value.set_float(number);
        } else {
            value.set_str(wire_value.text);
        }
        values.push_back(std::move(value));
    }
    return ParameterFrame(std::move(values));
}

ExecutionOutcome execute_prepared_operation(const PreparedPlanDescriptor& descriptor,
                                            const PreparedJitProgram* jit_program, const ParameterFrame& parameters,
                                            SessionState& session, QueryResultSink* result_sink,
                                            execution_timing_diagnostics::Sample* timing = nullptr) {
    std::vector<char> response(BUFFER_LENGTH, 0);
    int offset = 0;
    Context context(lock_manager.get(), log_manager.get(), nullptr, response.data(), &offset, txn_manager.get());
    context.isolation_level_ = session.isolation;
    context.result_sink_ = result_sink;
    context.output_file_enabled_ = &session.output_file_enabled;
    SetTransaction(session, &context);
    try {
        bool is_query = false;
        bool jit_handled = false;
        if (jit_program != nullptr) {
            PreparedJitExecutionStatus status;
            if (timing != nullptr) {
                execution_timing_diagnostics::StageTimer timer(&timing->portal_run_ns);
                status = jit_program->Execute(parameters, sm_manager.get(), &context, result_sink);
            } else {
                status = jit_program->Execute(parameters, sm_manager.get(), &context, result_sink);
            }
            jit_handled = status == PreparedJitExecutionStatus::Handled;
            if (jit_handled) {
                is_query = jit_program->is_query();
                if (timing != nullptr) {
                    timing->route = execution_timing_diagnostics::Route::PreparedRuntime;
                }
            }
        }
        std::unique_ptr<PortalStmt> statement;
        if (!jit_handled) {
            if (timing != nullptr) {
                timing->route = execution_timing_diagnostics::Route::PreparedPlan;
                execution_timing_diagnostics::StageTimer timer(&timing->instantiate_ns);
                statement = portal->start_prepared(descriptor, parameters, &context, &timing->executor_constructed);
            } else {
                statement = portal->start_prepared(descriptor, parameters, &context);
            }
            is_query = statement->tag == PORTAL_ONE_SELECT;
            if (timing != nullptr) {
                execution_timing_diagnostics::StageTimer timer(&timing->portal_run_ns);
                portal->run(std::move(statement), ql_manager.get(), &session.txn_id, &context);
            } else {
                portal->run(std::move(statement), ql_manager.get(), &session.txn_id, &context);
            }
            portal->drop();
        }
        session.isolation = context.isolation_level_;
        if (context.txn_ != nullptr && !context.txn_->get_txn_mode() &&
            context.txn_->get_state() != TransactionState::COMMITTED &&
            context.txn_->get_state() != TransactionState::ABORTED) {
            txn_manager->commit(context.txn_, context.log_mgr_);
            session.forget_running_transaction();
            session.txn_id = INVALID_TXN_ID;
        }
        context.txn_ = nullptr;
        return {is_query, false};
    } catch (TransactionAbortException& exception) {
        txn_manager->record_client_abort(exception.GetAbortReason());
        abort_session(session, &context);
        throw;
    } catch (...) {
        abort_session(session, &context);
        throw;
    }
}

ExecutionOutcome execute_sql(const std::string& sql, SessionState& session, QueryResultSink* result_sink) {
    auto parse_tree = ast::parse_sql(sql);
    return execute_tree(std::move(parse_tree), session, result_sink);
}

std::vector<std::unique_ptr<ast::Value>> make_typed_parameter_nodes(const std::vector<Type>& parameters) {
    std::vector<std::unique_ptr<ast::Value>> typed_parameters;
    typed_parameters.reserve(parameters.size());
    for (std::size_t i = 0; i < parameters.size(); ++i) {
        ast::SvType declared_type;
        switch (parameters[i]) {
        case Type::INT32:
            declared_type = ast::SV_TYPE_INT;
            break;
        case Type::FLOAT32:
            declared_type = ast::SV_TYPE_FLOAT;
            break;
        case Type::CHAR:
            declared_type = ast::SV_TYPE_STRING;
            break;
        default:
            throw wire_protocol::ProtocolError("unsupported prepared parameter type");
        }
        typed_parameters.push_back(std::make_unique<ast::Parameter>(i + 1, declared_type));
    }
    return typed_parameters;
}

PreparedStatement inspect_prepared(std::uint16_t id, bool query, std::vector<Type> parameters,
                                   std::unique_ptr<ast::TreeNode> template_tree, IsolationLevel isolation) {
    std::vector<char> response(BUFFER_LENGTH, 0);
    int offset = 0;
    Context context(lock_manager.get(), log_manager.get(), nullptr, response.data(), &offset, txn_manager.get());
    context.isolation_level_ = isolation;
    auto typed_parameters = make_typed_parameter_nodes(parameters);
    auto bound = ast::clone_bound_tree(*template_tree, typed_parameters);
    std::unique_ptr<Query> query_tree = analyze->do_analyze(std::move(bound));
    std::unique_ptr<Plan> plan = optimizer->plan_query(std::move(query_tree), &context);
    std::vector<std::string> diagnostic_plans;
    if (index_skip_scan_diagnostics::enabled() || execution_timing_diagnostics::enabled()) {
        diagnostic_plans = describe_prepared_plan(id, plan.get());
    }
    const bool actual_query = plan->tag == T_select;
    if (actual_query != query)
        throw wire_protocol::ProtocolError("prepared result kind does not match SQL");
    PreparedStatement result;
    result.id = id;
    result.query = query;
    result.template_tree = std::move(template_tree);
    result.parameters = std::move(parameters);
    result.diagnostic_plans = std::move(diagnostic_plans);
    if (!result.diagnostic_plans.empty()) {
        result.diagnostic_plan_hash = hash_prepared_plan(result.diagnostic_plans);
    }
    result.database_identity = sm_manager->get_database_identity_under_catalog_guard();
    result.catalog_generation = sm_manager->get_catalog_generation();
    PreparedStatementKind statement_kind = PreparedStatementKind::Unsupported;
    if (plan->tag == T_select) {
        statement_kind = PreparedStatementKind::Select;
    } else if (plan->tag == T_Insert) {
        statement_kind = PreparedStatementKind::Insert;
    } else if (plan->tag == T_Update) {
        statement_kind = PreparedStatementKind::Update;
    }
    if (actual_query) {
        auto [output_names, result_schema] = portal->inspect_select_plan(plan.get(), &context);
        result.names = output_names;
        for (const auto& column : result_schema) {
            result.result_types.push_back(column.type == TYPE_INT     ? Type::INT32
                                          : column.type == TYPE_FLOAT ? Type::FLOAT32
                                                                      : Type::CHAR);
        }
        result.descriptor = PreparedPlanDescriptor::Build(std::move(plan), statement_kind, std::move(output_names),
                                                          std::move(result_schema), result.database_identity,
                                                          result.catalog_generation);
    } else if (statement_kind != PreparedStatementKind::Unsupported) {
        result.descriptor = PreparedPlanDescriptor::Build(std::move(plan), statement_kind, {}, {},
                                                          result.database_identity, result.catalog_generation);
    } else {
        (void)portal->start(std::move(plan), &context);
    }
    if (result.descriptor != nullptr) {
        std::string jit_reason;
        result.jit_program = prepared_jit_compiler().Compile(*result.descriptor, sm_manager.get(), &jit_reason);
        if (result.jit_program != nullptr) {
            LOG_DEBUG("prepared JIT compiled statement_id=%u code_bytes=%zu", static_cast<unsigned int>(result.id),
                      result.jit_program->code_size());
        }
    }
    return result;
}

void revalidate_prepared(PreparedStatement& statement, IsolationLevel isolation) {
    auto typed_parameters = make_typed_parameter_nodes(statement.parameters);
    auto template_tree = ast::clone_bound_tree(*statement.template_tree, typed_parameters);
    PreparedStatement refreshed =
        inspect_prepared(statement.id, statement.query, statement.parameters, std::move(template_tree), isolation);
    if (refreshed.query != statement.query || refreshed.names != statement.names ||
        refreshed.result_types != statement.result_types) {
        throw wire_protocol::ProtocolError("prepared result schema changed after catalog update");
    }
    statement = std::move(refreshed);
}

void append_column_definition(Writer& writer, const std::string& name, Type type) {
    if (name.empty() || name.size() > UINT16_MAX || !is_valid_utf8(name)) {
        throw wire_protocol::ProtocolError("invalid column name");
    }
    writer.u16(static_cast<std::uint16_t>(name.size()));
    writer.bytes(name);
    writer.u8(static_cast<std::uint8_t>(type));
}

std::vector<std::uint8_t> make_row(const std::vector<Type>& types, const std::vector<Value>& row) {
    Writer writer;
    if (row.size() != types.size()) {
        throw wire_protocol::ProtocolError("row does not match query schema");
    }
    for (std::size_t i = 0; i < row.size(); ++i) {
        wire_protocol::encode_value(writer, row[i], types[i]);
    }
    return writer.take();
}

std::vector<std::uint8_t> make_error_payload(const std::string& text) {
    Writer writer;
    writer.bytes(text.substr(0, wire_protocol::kMaxDiagnostic));
    return writer.take();
}

struct ProtocolStreamSink : QueryResultSink {
    explicit ProtocolStreamSink(int socket_fd) : fd(socket_fd) {}

    void begin_query(const std::vector<ColMeta>& columns, const std::vector<std::string>& output_names) override {
        if (columns.empty() || columns.size() > UINT16_MAX || output_names.size() != columns.size()) {
            throw wire_protocol::ProtocolError("invalid query schema");
        }
        types.clear();
        Writer meta;
        meta.u16(static_cast<std::uint16_t>(columns.size()));
        for (std::size_t i = 0; i < columns.size(); ++i) {
            const Type type = protocol_type(columns[i].type);
            append_column_definition(meta, output_names[i], type);
            types.push_back(type);
        }
        wire_protocol::write_frame(fd, Tag::META, meta.take());
        query = true;
    }

    void append_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) override {
        if (!query) {
            throw wire_protocol::ProtocolError("query row emitted before META");
        }
        wire_protocol::write_frame(fd, Tag::ROW, make_row(types, protocol_row(columns, data, size)));
        ++row_count;
    }

    void finish() {
        Writer end;
        end.u64(row_count);
        wire_protocol::write_frame(fd, Tag::RESULT_END, end.take());
    }

    int fd;
    std::vector<Type> types;
    std::uint64_t row_count = 0;
    bool query = false;
};

std::vector<std::uint8_t> prepare_set(const std::vector<PreparedStatement>& statements) {
    std::vector<wire_protocol::PreparedSchema> schemas;
    schemas.reserve(statements.size());
    for (const auto& statement : statements) {
        if (statement.names.size() != statement.result_types.size()) {
            throw wire_protocol::ProtocolError("prepared schema name/type count mismatch");
        }
        wire_protocol::PreparedSchema schema;
        schema.statement_id = statement.id;
        schema.columns.reserve(statement.result_types.size());
        for (std::size_t i = 0; i < statement.result_types.size(); ++i) {
            schema.columns.push_back({statement.names[i], statement.result_types[i]});
        }
        schemas.push_back(std::move(schema));
    }
    return wire_protocol::encode_prepare_ok(schemas);
}

void handle_client_frame(int fd, const wire_protocol::Frame& frame, SessionState& session,
                         std::unordered_map<std::uint16_t, PreparedStatement>& prepared) {
    Reader reader(frame.payload);
    if (frame.tag == Tag::EXEC_STREAM) {
        if (frame.flags != 0 || frame.payload.empty()) {
            throw wire_protocol::ProtocolError("invalid EXEC_STREAM request");
        }
        const std::string sql = reader.bytes(reader.remaining());
        reader.require_end();
        if (sql.find('\0') != std::string::npos || !is_valid_utf8(sql)) {
            throw wire_protocol::ProtocolError("EXEC_STREAM SQL must be UTF-8 without NUL");
        }
        ProtocolStreamSink result(fd);
        const ExecutionOutcome outcome = execute_sql(sql, session, &result);
        if (outcome.query && result.query) {
            result.finish();
        } else {
            wire_protocol::write_frame(fd, Tag::COMMAND_OK, {});
        }
        if (outcome.catalog_changed) {
            prepared.clear();
        }
        return;
    }

    if (frame.tag == Tag::PREPARE_SET) {
        if (frame.flags != 0) {
            throw wire_protocol::ProtocolError("invalid PREPARE_SET flags");
        }
        const auto count = reader.u16();
        if (count == 0 || count > 256) {
            throw wire_protocol::ProtocolError("invalid prepared statement count");
        }
        auto catalog_guard = sm_manager->acquire_catalog_shared();
        std::vector<PreparedStatement> pending;
        std::unordered_map<std::uint16_t, bool> ids;
        for (std::uint16_t i = 0; i < count; ++i) {
            PreparedStatement statement;
            statement.id = reader.u16();
            if (statement.id == 0 || ids[statement.id]) {
                throw wire_protocol::ProtocolError("prepared statement ids must be unique and non-zero");
            }
            ids[statement.id] = true;
            const auto result_kind = reader.u8();
            if (result_kind > 1) {
                throw wire_protocol::ProtocolError("invalid prepared result kind");
            }
            statement.query = result_kind == 1;
            const auto parameter_count = reader.u16();
            statement.parameters.reserve(parameter_count);
            for (std::uint16_t p = 0; p < parameter_count; ++p) {
                const auto type = static_cast<Type>(reader.u8());
                if (type != Type::INT32 && type != Type::FLOAT32 && type != Type::CHAR) {
                    throw wire_protocol::ProtocolError("unknown prepared parameter type");
                }
                statement.parameters.push_back(type);
            }
            const auto sql_size = reader.u32();
            if (sql_size > reader.remaining() || sql_size > wire_protocol::kMaxPayload) {
                throw wire_protocol::ProtocolError("invalid prepared SQL length");
            }
            const std::string template_sql = reader.bytes(sql_size);
            if (template_sql.empty() || template_sql.find('\0') != std::string::npos || !is_valid_utf8(template_sql)) {
                throw wire_protocol::ProtocolError("prepared SQL must be non-empty UTF-8 without NUL");
            }
            auto template_tree = ast::parse_sql(template_sql);
            if (template_tree == nullptr)
                throw wire_protocol::ProtocolError("empty prepared SQL");
            if (changes_catalog(template_tree->type)) {
                throw wire_protocol::ProtocolError("PREPARE_SET does not allow structural DDL or LOAD");
            }
            std::vector<bool> seen(statement.parameters.size(), false);
            std::function<void(const ast::TreeNode&)> collect = [&](const ast::TreeNode& node) {
                if (node.type == ast::AstType::Parameter) {
                    auto ordinal = static_cast<const ast::Parameter&>(node).ordinal;
                    if (ordinal == 0 || ordinal > statement.parameters.size())
                        throw wire_protocol::ProtocolError("parameter marker is out of range");
                    seen[ordinal - 1] = true;
                }
                if (node.type == ast::AstType::SelectStmt) {
                    const auto& select = static_cast<const ast::SelectStmt&>(node);
                    if (select.limit_is_parameter) {
                        if (select.limit_parameter == 0 || select.limit_parameter > statement.parameters.size())
                            throw wire_protocol::ProtocolError("parameter marker is out of range");
                        seen[select.limit_parameter - 1] = true;
                    }
                    if (select.offset_is_parameter) {
                        if (select.offset_parameter == 0 || select.offset_parameter > statement.parameters.size())
                            throw wire_protocol::ProtocolError("parameter marker is out of range");
                        seen[select.offset_parameter - 1] = true;
                    }
                }
            };
            std::function<void(const ast::Expr&)> visit_expr = [&](const ast::Expr& expr) {
                if (expr.type == ast::AstType::Parameter) {
                    auto ordinal = static_cast<const ast::Parameter&>(expr).ordinal;
                    if (ordinal == 0 || ordinal > statement.parameters.size())
                        throw wire_protocol::ProtocolError("parameter marker is out of range");
                    seen[ordinal - 1] = true;
                }
            };
            std::function<void(const ast::TreeNode&)> walk = [&](const ast::TreeNode& node) {
                collect(node);
                switch (node.type) {
                case ast::AstType::InsertStmt:
                    for (const auto& v : static_cast<const ast::InsertStmt&>(node).vals)
                        visit_expr(*v);
                    break;
                case ast::AstType::DeleteStmt:
                    for (const auto& c : static_cast<const ast::DeleteStmt&>(node).conds) {
                        visit_expr(*c->lhs);
                        visit_expr(*c->rhs);
                    }
                    break;
                case ast::AstType::UpdateStmt: {
                    const auto& x = static_cast<const ast::UpdateStmt&>(node);
                    for (const auto& s : x.set_clauses) {
                        if (s->val)
                            visit_expr(*s->val);
                        for (const auto& term : s->additional_terms)
                            visit_expr(*term.val);
                    }
                    for (const auto& c : x.conds) {
                        visit_expr(*c->lhs);
                        visit_expr(*c->rhs);
                    }
                    break;
                }
                case ast::AstType::SelectStmt: {
                    const auto& x = static_cast<const ast::SelectStmt&>(node);
                    for (const auto& i : x.select_items)
                        visit_expr(*i->expr);
                    for (const auto& c : x.conds) {
                        visit_expr(*c->lhs);
                        visit_expr(*c->rhs);
                    }
                    for (const auto& h : x.having_conds) {
                        visit_expr(*h->lhs);
                        visit_expr(*h->rhs);
                    }
                    for (const auto& o : x.order_by_items)
                        visit_expr(*o->expr);
                    break;
                }
                default:
                    break;
                }
            };
            walk(*template_tree);
            for (bool marker_seen : seen)
                if (!marker_seen)
                    throw wire_protocol::ProtocolError("parameter markers must be dense");
            statement = inspect_prepared(statement.id, statement.query, std::move(statement.parameters),
                                         std::move(template_tree), session.isolation);
            pending.push_back(std::move(statement));
        }
        reader.require_end();
        if (sm_manager->get_catalog_generation() != pending.front().catalog_generation ||
            sm_manager->get_database_identity_under_catalog_guard() != pending.front().database_identity) {
            throw wire_protocol::ProtocolError("catalog changed during PREPARE_SET");
        }
        const auto response = prepare_set(pending);
        std::unordered_map<std::uint16_t, PreparedStatement> replacement;
        replacement.reserve(pending.size());
        for (auto& statement : pending) {
            replacement.emplace(statement.id, std::move(statement));
        }
        prepared.swap(replacement);
        if (index_skip_scan_diagnostics::enabled()) {
            try {
                for (const auto& [id, statement] : prepared) {
                    (void)id;
                    for (const auto& mapping : statement.diagnostic_plans) {
                        const std::string deduplication_key =
                            std::to_string(statement.diagnostic_plan_hash) + ":" + mapping;
                        if (index_skip_scan_diagnostics::register_prepared_plan(deduplication_key)) {
                            LOG_WARN("skipdiag_prepare plan_hash=%llu %s",
                                     static_cast<unsigned long long>(statement.diagnostic_plan_hash), mapping.c_str());
                        }
                    }
                }
                minilog::Logger::get().flush();
            } catch (...) {
                // Diagnostics must never alter PREPARE_SET behavior.
            }
        }
        wire_protocol::write_frame(fd, Tag::PREPARE_OK, response);
        return;
    }

    if (frame.tag != Tag::EXEC_BATCH || frame.flags != 1) {
        throw wire_protocol::ProtocolError("unknown request tag or flags");
    }
    const auto operation_count = reader.u16();
    if (operation_count == 0 || operation_count > 256) {
        throw wire_protocol::ProtocolError("invalid batch operation count");
    }
    struct Operation {
        PreparedStatement* statement;
        std::vector<Value> values;
    };
    std::vector<Operation> operations;
    operations.reserve(operation_count);
    for (std::uint16_t i = 0; i < operation_count; ++i) {
        const auto id = reader.u16();
        auto it = prepared.find(id);
        if (it == prepared.end()) {
            throw wire_protocol::ProtocolError("unknown prepared statement id");
        }
        if (it->second.template_tree == nullptr || changes_catalog(it->second.template_tree->type)) {
            throw wire_protocol::ProtocolError("prepared structural DDL or LOAD is not executable");
        }
        Operation operation{&it->second, {}};
        for (Type type : operation.statement->parameters) {
            operation.values.push_back(wire_protocol::decode_value(reader, type));
        }
        operations.push_back(std::move(operation));
    }
    reader.require_end();

    std::vector<std::pair<std::uint16_t, ProtocolCollector>> results;
    std::uint16_t failed = 0xffff;
    std::uint16_t executed = 0;
    constexpr std::size_t kBatchResultFixedBytes = 2 + 1 + 2 + 4 + 2;
    constexpr std::size_t kBatchResultEntryBytes = 2 + 4;
    const std::size_t result_budget = wire_protocol::kMaxPayload - kBatchResultFixedBytes -
                                      static_cast<std::size_t>(operation_count) * kBatchResultEntryBytes;
    std::size_t encoded_result_bytes = 0;
    const bool execution_timing_enabled = execution_timing_diagnostics::enabled();
    const bool skip_scan_diagnostics_enabled = index_skip_scan_diagnostics::enabled();
    try {
        for (std::uint16_t i = 0; i < operation_count; ++i) {
            auto* prepared_statement = operations[i].statement;
            std::optional<execution_timing_diagnostics::OperationScope> timing_scope;
            if (execution_timing_enabled) {
                timing_scope.emplace(prepared_statement->id, prepared_statement->diagnostic_plan_hash);
            }
            bool binding_parameters = false;
            try {
                auto catalog_guard = sm_manager->acquire_catalog_shared();
                if (prepared_statement->catalog_generation != sm_manager->get_catalog_generation() ||
                    prepared_statement->database_identity != sm_manager->get_database_identity_under_catalog_guard()) {
                    revalidate_prepared(*prepared_statement, session.isolation);
                }
                ProtocolCollector result;
                result.encoded_bytes = &encoded_result_bytes;
                result.encoded_limit = result_budget;
                const auto make_bindings = [&]() {
                    std::vector<std::unique_ptr<ast::Value>> values;
                    values.reserve(operations[i].values.size());
                    for (const auto& value : operations[i].values) {
                        if (value.type != prepared_statement->parameters[values.size()])
                            throw wire_protocol::ProtocolError("typed parameter does not match prepared declaration");
                        if (!value.present) {
                            // present == 0 绑定为 SQL NULL，与内联的 NULL 字面量等价
                            values.push_back(std::make_unique<ast::NullLit>());
                            continue;
                        }
                        if (value.type == Type::INT32)
                            values.push_back(std::make_unique<ast::IntLit>(value.int32));
                        else if (value.type == Type::FLOAT32) {
                            float number;
                            std::memcpy(&number, &value.float_bits, sizeof(number));
                            values.push_back(std::make_unique<ast::FloatLit>(number));
                        } else
                            values.push_back(std::make_unique<ast::StringLit>(value.text));
                    }
                    return values;
                };
                ExecutionOutcome outcome;
                if (descriptor_runtime_eligible(prepared_statement->descriptor.get())) {
                    binding_parameters = true;
                    std::optional<ParameterFrame> parameter_frame;
                    if (execution_timing_enabled) {
                        timing_scope->sample().route = execution_timing_diagnostics::Route::PreparedPlan;
                        execution_timing_diagnostics::StageTimer timer(&timing_scope->sample().bind_ns);
                        parameter_frame.emplace(make_parameter_frame(operations[i].values));
                    } else {
                        parameter_frame.emplace(make_parameter_frame(operations[i].values));
                    }
                    binding_parameters = false;
                    if (timing_scope.has_value()) {
                        timing_scope->sample().fallback_reason = execution_timing_diagnostics::FallbackReason::Bind;
                    }
                    outcome = execute_prepared_operation(
                        *prepared_statement->descriptor, prepared_statement->jit_program.get(), *parameter_frame,
                        session, &result, execution_timing_enabled ? &timing_scope->sample() : nullptr);
                    if (timing_scope.has_value()) {
                        timing_scope->sample().fallback_reason = execution_timing_diagnostics::FallbackReason::None;
                    }
                } else if (!execution_timing_enabled && !skip_scan_diagnostics_enabled) {
                    auto values = make_bindings();
                    outcome = execute_tree_under_catalog_guard(
                        ast::clone_bound_tree(*prepared_statement->template_tree, values), session, &result);
                } else {
                    if (timing_scope.has_value()) {
                        timing_scope->sample().fallback_reason =
                            prepared_statement->descriptor == nullptr
                                ? execution_timing_diagnostics::FallbackReason::Null
                                : execution_timing_diagnostics::FallbackReason::Shape;
                    }
                    std::optional<index_skip_scan_diagnostics::StatementScope> skip_scan_scope;
                    if (skip_scan_diagnostics_enabled) {
                        skip_scan_scope.emplace(prepared_statement->id, prepared_statement->diagnostic_plan_hash);
                    }
                    std::vector<std::unique_ptr<ast::Value>> values;
                    binding_parameters = true;
                    if (execution_timing_enabled) {
                        execution_timing_diagnostics::StageTimer timer(&timing_scope->sample().bind_ns);
                        values = make_bindings();
                    } else {
                        values = make_bindings();
                    }
                    std::unique_ptr<ast::TreeNode> bound_tree;
                    if (execution_timing_enabled) {
                        execution_timing_diagnostics::StageTimer timer(&timing_scope->sample().clone_bind_ns);
                        bound_tree = ast::clone_bound_tree(*prepared_statement->template_tree, values);
                    } else {
                        bound_tree = ast::clone_bound_tree(*prepared_statement->template_tree, values);
                    }
                    binding_parameters = false;
                    if (execution_timing_enabled) {
                        outcome =
                            execute_tree_timed(std::move(bound_tree), session, &result, &timing_scope->sample(), true);
                    } else {
                        outcome = execute_tree_under_catalog_guard(std::move(bound_tree), session, &result);
                    }
                }
                if (outcome.query) {
                    results.emplace_back(i, std::move(result));
                }
                ++executed;
                if (timing_scope.has_value()) {
                    timing_scope->finish();
                }
            } catch (TransactionAbortException&) {
                if (timing_scope.has_value()) {
                    if (binding_parameters) {
                        timing_scope->sample().fallback_reason = execution_timing_diagnostics::FallbackReason::Bind;
                    }
                    timing_scope->finish_abort();
                }
                throw;
            } catch (...) {
                if (timing_scope.has_value()) {
                    if (binding_parameters) {
                        timing_scope->sample().fallback_reason = execution_timing_diagnostics::FallbackReason::Bind;
                    }
                    timing_scope->finish_error();
                }
                throw;
            }
        }
    } catch (TransactionAbortException& exception) {
        failed = executed;
        abort_session(session, nullptr);
        Writer response;
        response.u16(executed);
        response.u8(1);
        response.u16(failed);
        // TransactionAbortException does not override what(); use the same
        // diagnostic text the EXEC_STREAM path reports.
        const auto text = exception.GetInfo();
        response.u32(static_cast<std::uint32_t>(text.size()));
        response.bytes(text);
        response.u16(0);
        wire_protocol::write_frame(fd, Tag::BATCH_RESULT, response.take());
        return;
    } catch (const std::exception& exception) {
        failed = executed;
        abort_session(session, nullptr);
        Writer response;
        response.u16(executed);
        response.u8(2);
        response.u16(failed);
        const auto text = diagnostic(exception);
        response.u32(static_cast<std::uint32_t>(text.size()));
        response.bytes(text);
        response.u16(0);
        wire_protocol::write_frame(fd, Tag::BATCH_RESULT, response.take());
        return;
    }

    Writer response;
    response.u16(executed);
    response.u8(0);
    response.u16(0xffff);
    response.u32(0);
    response.u16(static_cast<std::uint16_t>(results.size()));
    for (const auto& [index, result] : results) {
        response.u16(index);
        response.u32(static_cast<std::uint32_t>(result.rows.size()));
        for (const auto& row : result.rows) {
            const auto bytes = make_row(result.types, row);
            response.bytes(bytes.data(), bytes.size());
        }
    }
    wire_protocol::write_frame(fd, Tag::BATCH_RESULT, response.take());
}

void client_handler(int fd) {
    SessionState session;
    std::unordered_map<std::uint16_t, PreparedStatement> prepared;
    LOG_INFO("establish protocol connection, sockfd: %d", fd);
    try {
        wire_protocol::server_handshake(fd);
        wire_protocol::Frame frame;
        while (wire_protocol::read_frame(fd, frame)) {
            try {
                handle_client_frame(fd, frame, session, prepared);
            } catch (TransactionAbortException& exception) {
                abort_session(session, nullptr);
                wire_protocol::write_frame(fd, Tag::TRANSACTION_ABORT, make_error_payload(exception.GetInfo()));
            } catch (const std::exception& exception) {
                abort_session(session, nullptr);
                wire_protocol::write_frame(fd, Tag::ERROR, make_error_payload(diagnostic(exception)));
            }
        }
    } catch (const std::exception& exception) {
        LOG_WARN("protocol connection closed: %s", exception.what());
        abort_session(session, nullptr);
    }
    abort_session(session, nullptr);
    close(fd);
}
} // namespace

void start_server(std::uint16_t port) {
    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in {};

    // 初始化连接
    sockfd_server = socket(AF_INET, SOCK_STREAM, 0); // ipv4,TCP
    assert(sockfd_server != -1);
    int val = 1;
    setsockopt(sockfd_server, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // before bind(), set the attr of structure sockaddr.
    memset(&s_addr_in, 0, sizeof(s_addr_in));
    s_addr_in.sin_family = AF_INET;
    s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
    s_addr_in.sin_port = htons(port);
    fd_temp = bind(sockfd_server, (struct sockaddr*)(&s_addr_in), sizeof(s_addr_in));
    if (fd_temp == -1) {
        LOG_ERROR("bind failed: %s", strerror(errno));
        minilog::Logger::get().stop();
        exit(1);
    }

    fd_temp = listen(sockfd_server, MAX_CONN_LIMIT);
    if (fd_temp == -1) {
        LOG_ERROR("listen failed: %s", strerror(errno));
        minilog::Logger::get().stop();
        exit(1);
    }

    while (!should_exit) {
        LOG_DEBUG("waiting for new connection");
        struct sockaddr_in s_addr_client {};
        int client_length = sizeof(s_addr_client);

        if (setjmp(jmpbuf)) {
            LOG_INFO("break from server listen loop");
            break;
        }

        // Block here. Until server accepts a new connection.
        int sockfd = accept(sockfd_server, (struct sockaddr*)(&s_addr_client), (socklen_t*)(&client_length));
        if (sockfd == -1) {
            LOG_WARN("accept failed: %s", strerror(errno));
            continue; // ignore current socket ,continue while loop.
        }

        // 和客户端建立连接，并开启一个线程负责处理客户端请求
        std::thread(client_handler, sockfd).detach();
    }

    // Clear
    LOG_INFO("try to close all client connections");
    int ret = shutdown(sockfd_server, SHUT_WR); // shut down the all or part of a full-duplex connection.
    if (ret == -1) {
        LOG_ERROR("shutdown server socket failed: %s", strerror(errno));
    }
    //    assert(ret != -1);
    LOG_INFO("server shuts down");
}

int main(int argc, char** argv) {
    minilog::Logger::get().init("rmdb.log");
    minilog::Logger::get().set_level(minilog::LogLevel::WARN);

    if (argc != 2) {
        // 需要指定数据库名称
        LOG_ERROR("usage: %s <database>", argv[0]);
        minilog::Logger::get().stop();
        exit(1);
    }

    signal(SIGINT, sigint_handler);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGUSR1, sigusr1_handler);
    try {
        std::cout << "\n"
                     "  _____  __  __ _____  ____  \n"
                     " |  __ \\|  \\/  |  __ \\|  _ \\ \n"
                     " | |__) | \\  / | |  | | |_) |\n"
                     " |  _  /| |\\/| | |  | |  _ < \n"
                     " | | \\ \\| |  | | |__| | |_) |\n"
                     " |_|  \\_\\_|  |_|_____/|____/ \n"
                     "\n"
                     "Welcome to RMDB!\n"
                     "Type 'help;' for help.\n"
                     "\n";
        std::uint16_t server_port = SOCK_PORT;
        if (const char* configured_port = std::getenv("RMDB_PORT"); configured_port != nullptr) {
            const unsigned long parsed_port = std::stoul(configured_port);
            if (parsed_port == 0 || parsed_port > UINT16_MAX) {
                throw InternalError("RMDB_PORT must be between 1 and 65535");
            }
            server_port = static_cast<std::uint16_t>(parsed_port);
        }
        // Database name is passed by args
        std::string db_name = argv[1];
        LOG_INFO("RMDB server starting, database: %s", db_name.c_str());
        if (!sm_manager->is_dir(db_name)) {
            // Database not found, create a new one
            auto catalog_guard = sm_manager->acquire_catalog_exclusive();
            sm_manager->create_db(db_name);
            LOG_INFO("database created: %s", db_name.c_str());
        }
        // Open database
        {
            auto catalog_guard = sm_manager->acquire_catalog_exclusive();
            sm_manager->open_db(db_name);
        }
        LOG_INFO("database opened: %s", db_name.c_str());

        log_manager->initialize_from_existing_log();
        buffer_pool_manager->set_log_manager(log_manager.get());

        // recovery database
        // Per-phase cost is reported once, here, so recovery tuning is not
        // guesswork. Nothing below runs on the measured transaction path.
        {
            // The default level is WARN, which would swallow the numbers this
            // block exists to produce. The window is startup only and closes
            // before any transaction runs, so no measured path is affected.
            minilog::Logger::get().set_level(minilog::LogLevel::INFO);
            const auto phase_elapsed_ms = [](std::chrono::steady_clock::time_point begin) {
                return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                    .count();
            };
            const uint64_t page_reads_before = disk_manager->get_page_read_count();
            const uint64_t page_writes_before = disk_manager->get_page_write_count();
            const auto recovery_begin = std::chrono::steady_clock::now();

            auto phase_begin = recovery_begin;
            recovery->analyze();
            LOG_INFO("recovery analyze: %lld ms, wal reads: %llu (%llu bytes), page reads: %llu",
                     static_cast<long long>(phase_elapsed_ms(phase_begin)),
                     static_cast<unsigned long long>(disk_manager->get_log_read_count()),
                     static_cast<unsigned long long>(disk_manager->get_log_read_bytes()),
                     static_cast<unsigned long long>(disk_manager->get_page_read_count() - page_reads_before));

            phase_begin = std::chrono::steady_clock::now();
            const uint64_t redo_page_reads_before = disk_manager->get_page_read_count();
            recovery->redo();
            LOG_INFO("recovery redo: %lld ms, page reads: %llu", static_cast<long long>(phase_elapsed_ms(phase_begin)),
                     static_cast<unsigned long long>(disk_manager->get_page_read_count() - redo_page_reads_before));

            phase_begin = std::chrono::steady_clock::now();
            const uint64_t undo_page_reads_before = disk_manager->get_page_read_count();
            recovery->undo();
            LOG_INFO("recovery undo: %lld ms, page reads: %llu", static_cast<long long>(phase_elapsed_ms(phase_begin)),
                     static_cast<unsigned long long>(disk_manager->get_page_read_count() - undo_page_reads_before));

            // 必须在任何事务开始之前、恢复读完 WAL 与重启清单之后做：commit_ts_ 持久化
            // 在数据页里，而计数器只活在内存里。计数器从 0 重启会让上一世提交的行被
            // 判成“来自未来”而不可见（final.md:342 第 1 条）。取值的完整论证见
            // RecoveryManager::get_recovered_next_timestamp()。
            txn_manager->seed_counters_after_recovery(recovery->get_recovered_next_timestamp(),
                                                      recovery->get_recovered_next_txn_id());
            LOG_INFO("recovery seeded counters: next_timestamp %lld, next_txn_id %lld",
                     static_cast<long long>(recovery->get_recovered_next_timestamp()),
                     static_cast<long long>(recovery->get_recovered_next_txn_id()));

            phase_begin = std::chrono::steady_clock::now();
            sm_manager->refresh_index_residency();
            LOG_INFO("recovery index residency refresh: %lld ms",
                     static_cast<long long>(phase_elapsed_ms(phase_begin)));

            LOG_INFO("database recovery finished in %lld ms, page reads: %llu, page writes: %llu",
                     static_cast<long long>(phase_elapsed_ms(recovery_begin)),
                     static_cast<unsigned long long>(disk_manager->get_page_read_count() - page_reads_before),
                     static_cast<unsigned long long>(disk_manager->get_page_write_count() - page_writes_before));
            minilog::Logger::get().set_level(minilog::LogLevel::WARN);
        }

        {
            std::atomic<bool> checkpoint_thread_stop{false};
            std::atomic<bool> observability_thread_stop{false};
            std::thread observability_thread([&observability_thread_stop] {
                uint64_t served_requests = 0;
                constexpr auto observability_poll_interval = std::chrono::milliseconds(25);
                while (!observability_thread_stop.load(std::memory_order_acquire)) {
                    const uint64_t requested = observability_requests.load(std::memory_order_acquire);
                    while (served_requests != requested) {
                        log_wal_statistics();
                        log_observability_statistics();
                        ++served_requests;
                    }
                    std::this_thread::sleep_for(observability_poll_interval);
                }
            });
            std::thread checkpoint_thread([&checkpoint_thread_stop] {
                CheckpointManager checkpoint_mgr(txn_manager.get(), sm_manager.get(), log_manager.get());
                CheckpointOptions checkpoint_options;
                bool has_checkpoint_override = false;
                auto read_positive_int64 = [&](const char* name, int64_t* target) {
                    const char* value = std::getenv(name);
                    if (value == nullptr) {
                        return;
                    }
                    try {
                        const auto parsed = std::stoll(value);
                        if (parsed > 0) {
                            *target = parsed;
                            has_checkpoint_override = true;
                        }
                    } catch (const std::exception&) {
                        // Keep the default for malformed diagnostic overrides.
                    }
                };
                auto read_positive_size = [&](const char* name, size_t* target) {
                    const char* value = std::getenv(name);
                    if (value == nullptr) {
                        return;
                    }
                    try {
                        const auto parsed = std::stoull(value);
                        if (parsed > 0) {
                            *target = static_cast<size_t>(parsed);
                            has_checkpoint_override = true;
                        }
                    } catch (const std::exception&) {
                        // Keep the default for malformed diagnostic overrides.
                    }
                };
                read_positive_int64("RMDB_AUTO_CHECKPOINT_BYTES", &checkpoint_options.auto_checkpoint_bytes);
                read_positive_int64("RMDB_CHECKPOINT_PREFLUSH_BYTES", &checkpoint_options.preflush_trigger_bytes);
                read_positive_int64("RMDB_CHECKPOINT_DEFER_BYTES", &checkpoint_options.checkpoint_defer_bytes);
                read_positive_size("RMDB_CHECKPOINT_PREFLUSH_PAGES", &checkpoint_options.preflush_batch_pages);
                if (has_checkpoint_override) {
                    checkpoint_mgr.SetOptions(checkpoint_options);
                }
                // A small periodic batch keeps writeback below the foreground
                // latency horizon. RunCleanCheckpoint remains WAL-size driven;
                // this interval only controls non-blocking dirty-page preflush.
                constexpr auto checkpoint_poll_interval = std::chrono::milliseconds(100);
                auto next_checkpoint_poll = std::chrono::steady_clock::now() + checkpoint_poll_interval;
                while (!checkpoint_thread_stop.load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    if (checkpoint_thread_stop.load()) {
                        break;
                    }
                    const auto now = std::chrono::steady_clock::now();
                    if (now >= next_checkpoint_poll) {
                        checkpoint_mgr.RunIfNeeded();
                        next_checkpoint_poll = std::chrono::steady_clock::now() + checkpoint_poll_interval;
                    }
                }
            });

            // 开启服务端，开始接受客户端连接
            start_server(server_port);

            observability_thread_stop.store(true, std::memory_order_release);
            if (observability_thread.joinable()) {
                observability_thread.join();
            }
            checkpoint_thread_stop.store(true);
            if (checkpoint_thread.joinable()) {
                checkpoint_thread.join();
            }
        }

        {
            auto catalog_guard = sm_manager->acquire_catalog_exclusive();
            sm_manager->close_db();
        }
        LOG_INFO("database has been closed");
    } catch (RMDBError& e) {
        LOG_ERROR("RMDB error: %s", e.what());
        minilog::Logger::get().stop();
        exit(1);
    }
    minilog::Logger::get().stop();
    return 0;
}
