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
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <functional>
#include <iomanip>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "errors.h"
#include "common/transaction_abort_metrics.h"
#include "common/checkpoint_phase_metrics.h"
#include "common/transaction_phase_metrics.h"
#include "common/wal_flush_metrics.h"
#include "index/ix_scan.h"
#include "minilog.h"
#include "optimizer/optimizer.h"
#include "recovery/checkpoint_manager.h"
#include "recovery/log_recovery.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "execution/prepared_select_execution_frame.h"
#include "analyze/analyze.h"
#include "protocol/wire_protocol.h"

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 128

static volatile sig_atomic_t should_exit = 0;
static volatile sig_atomic_t listener_fd = -1;

// 构建全局所需的管理器对象
static constexpr size_t SERVER_BUFFER_POOL_PAGES = (size_t{3} << 30) / PAGE_SIZE;
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(SERVER_BUFFER_POOL_PAGES, disk_manager.get());
auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
auto ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
auto sm_manager =
    std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
auto transaction_phase_metrics = std::make_unique<TransactionPhaseMetrics>();
auto transaction_abort_metrics = std::make_unique<TransactionAbortMetrics>();
auto wal_flush_metrics = std::make_unique<WalFlushMetrics>();
auto checkpoint_phase_metrics = std::make_unique<CheckpointPhaseMetrics>();
CheckpointOptions checkpoint_options{};
auto lock_manager = std::make_unique<LockManager>(
    ShardAcquisitionMetrics::Config::FromEnvironment("RMDB_LOCK_SHARD_METRICS_SAMPLE_LOG2", "RMDB_LOCK_SHARD_SLOW_NS"),
    transaction_phase_metrics.get());
auto txn_manager = std::make_unique<TransactionManager>(
    lock_manager.get(), sm_manager.get(), ConcurrencyMode::TWO_PHASE_LOCKING, transaction_phase_metrics.get());
auto planner = std::make_unique<Planner>(sm_manager.get());
auto optimizer = std::make_unique<Optimizer>(sm_manager.get(), planner.get());
auto ql_manager =
    std::make_unique<QlManager>(sm_manager.get(), txn_manager.get(), planner.get(), checkpoint_phase_metrics.get());
// The server must not acknowledge a commit before the WAL is durable.  Keep
// PROCESS_CRASH available to focused LogManager tests, but never let an
// omitted or misspelled environment variable weaken the production path.
auto log_manager = std::make_unique<LogManager>(disk_manager.get(), DurabilityMode::STRICT, wal_flush_metrics.get());

auto portal = std::make_unique<Portal>(sm_manager.get());
auto analyze = std::make_unique<Analyze>(sm_manager.get());

namespace {
constexpr char kTxnPhaseSchema[] =
    "txn-phase schema=base62 cumulative=1 selected_nonexhaustive=1 additive=0 "
    "snapshot=relaxed_best_effort count_hist_may_be_transiently_inconsistent=1 "
    "record_lock_wait=inclusive_of_wait_for_graph_build final=1:graceful_final_snapshot "
    "entries=name:count/elapsed_ns/max_ns histogram_ns=1k,4k,16k,64k,256k,1m,4m,16m,64m,256m,1b,plus";
constexpr char kTxnPhaseActorSchemaA[] =
    "txn-phase actor-schema entries=exec_batch_wall:client_batch_thread/batch "
    "record_lock_wait:waiting_txn_thread/blocked_record_lock_request "
    "commit_prepare_sort_validate:commit_owner_thread/commit_call "
    "commit_wal_enqueue:commit_owner_thread/commit_record_append";
constexpr char kTxnPhaseActorSchemaB[] =
    "txn-phase actor-schema entries="
    "commit_wal_coverage:commit_owner_thread/durability_wait "
    "commit_tuple_finalize:publication_actor/publication_claim "
    "commit_frontier_wait:publication_actor/cv_wait "
    "commit_release:publication_actor/lock_release_claim";
constexpr char kTxnPhaseActorSchemaC[] =
    "txn-phase actor-schema entries=commit_owner_cleanup:commit_owner_thread/commit_call "
    "abort_wal:abort_owner_thread/abort_wal_call abort_heap_undo:abort_owner_thread/abort_with_writes "
    "abort_index_undo:abort_owner_thread/abort_with_index_undo";
constexpr char kTxnPhaseActorSchemaD[] =
    "txn-phase actor-schema entries=abort_ssi_cleanup:abort_owner_thread/serializable_abort "
    "abort_release:abort_owner_thread/lock_release_call wait_for_graph_build:waiting_txn_thread/graph_build "
    "helping=tuple_finalize,frontier_wait,commit_release_actor_may_differ_from_transaction_owner";
constexpr char kTxnOwnerSchema[] =
    "txn-phase-owner schema=base62 cumulative=1 snapshot=relaxed_best_effort "
    "terminal=post_cleanup latency=observation_to_cleanup_terminal_ns observers=conflicting_write_observations "
    "histogram_ns=1k,4k,16k,64k,256k,1m,4m,16m,64m,256m,1b,plus";
constexpr char kTxnWaitGraphSchema[] =
    "txn-phase-waitgraph schema=base62 cumulative=1 snapshot=relaxed_best_effort "
    "sample=stable_graph_build_only values=shards,queues,edges:sum/max";
constexpr char kTxnReadOnlyWalSchema[] =
    "txn-phase-readonly-wal schema=base62 cumulative=1 snapshot=relaxed_best_effort "
    "field=inferred_successful_begin_commit_pairs independent_append_counts=0";
constexpr char kRuntimeMetricBundleSchema[] =
    "runtime-metric-bundle schema=base62 cumulative=1 sequence_shared=1 snapshot=relaxed_best_effort "
    "fields_may_be_transiently_inconsistent=1 "
    "window_delta=selected_boundary_minus_selected_prior_boundary_for_monotonic_count,sum,bytes,hist,counters "
    "max_fields=cumulative_not_delta";
constexpr std::size_t kWarnHeaderMax = sizeof("WARN ") - 1 +
                                       // TimeCache accepts a seven-digit year prefix plus milliseconds.
                                       sizeof("0000000-00-00 00:00:00.000") - 1 + sizeof(" [rmdb.cpp:") - 1 + 10 +
                                       sizeof("] ") - 1;
constexpr char kTxnPhaseDataPrefix[] = "txn-phase seq=";
constexpr char kTxnPhaseDataFinal[] = " final=1 ";
constexpr char kReadViewShadowSchema[] =
    "readview-shadow schema=base62 cumulative=1 authority=legacy visibility=full_undo_chain "
    "scope=active_writer_read_shadow excluded=write_conflict,ssi_dependency "
    "classes=match/undo_missing/version_mismatch/delete_mismatch/payload_mismatch";
constexpr char kTxnPhaseLongestName[] = "commit_prepare_sort_validate";
constexpr char kWalFlushSchema[] =
    "wal-flush-metric schema=base62 cumulative=1 snapshot=relaxed_best_effort "
    "fields_may_be_transiently_inconsistent=1 leader_is_wal_flush_owner_not_transaction_owner "
    "lag=target_minus_durable invalid_or_reset=excluded rotation=handoff_after_owner_covered "
    "commit_initial_role=mutually_exclusive_first_schedule_decision "
    "batch_buckets=0,1,2,3,4,5,6,7,8,9_16,17_32,33_64,65_plus";
constexpr char kWalFlushCompletedPrefix[] = "wal-flush-metric seq=";
constexpr char kWalFlushTimingFixed[] =
    "wal-flush-metric seq= final=1 covered= leaders= rotations= max_batches_per_leader= followers= "
    "follower_wait=// coalescing_wait=// physical= pwrite=/// fdatasync=//";
constexpr char kWalSyncDepthSchema[] =
    "wal-flush-sync-depth schema=base62 cumulative=1 snapshot=relaxed_best_effort "
    "waves=count max_inflight=max_concurrent_fdatasync_slots";
constexpr char kCheckpointSchema[] =
    "checkpoint-metric schema=base62 cumulative=1 snapshot=relaxed_best_effort "
    "fields_may_be_transiently_inconsistent=1 clean=attempt/success/failure "
    "fuzzy=attempt/success/failure/cancel pages=marked/write_calls/written/remaining_max "
    "defer=retry/deadline budget_yield=io/time/zero_progress timing=count/sum_ns/max_ns timing_count_unit=phase_calls "
    "counter_unit=events_or_pages config=auto_bytes/tick_bytes/tick_time_us/io_quantum_pages";
constexpr char kCheckpointPhaseSchema[] =
    "checkpoint-metric phase-schema "
    "drain=admission_wait cut=headers_cohort_wal_cut page=cohort_flush clean_data=flush_all_data_index "
    "clean_meta=flush_meta fuzzy_final=headers_files_dbmeta manifest=restart_manifest wal_reset=reset_log "
    "lifetime=fuzzy_attempt_to_terminal terminal=exactly_one page_calls=returned remaining=max_returned";
constexpr char kCheckpointCounterFixed[] =
    "checkpoint-metric seq= final=1 clean=// fuzzy=/// pages=/// defer=/ budget_yield=//";
constexpr char kCheckpointTimingAFixed[] =
    "checkpoint-metric seq= final=1 drain=// cut=// page=// clean_data=// clean_meta=//";
constexpr char kCheckpointTimingBFixed[] =
    "checkpoint-metric seq= final=1 fuzzy_final=// manifest=// wal_reset=// lifetime=//";
constexpr char kCheckpointDependencySchema[] =
    "checkpoint-dependency-metric schema=base62 cumulative=1 snapshot=relaxed_best_effort "
    "unit=merged_checkpoint_dependency_claims classification=attempt_before_durable_flush "
    "failure_after_classification=counted failure_before_classification=not_counted "
    "counters_not=pages_or_fdatasyncs checkpoint_dependency=already_covered/coverage_requested";
constexpr char kCheckpointDependencyFixed[] = "checkpoint-dependency-metric seq= final=1 checkpoint_dependency=/";
constexpr char kDirtyPwriteSchema[] =
    "dirty-pwrite-metric schema=base62 cumulative=1 snapshot=relaxed_best_effort "
    "count_bytes_timing_may_be_transiently_inconsistent=1 classes=foreground/background/checkpoint "
    "fields=count/bytes/elapsed_ns/max_ns/runs_with_wal_dependency units=runs/bytes/ns/ns/runs "
    "runs_with_wal_dependency=run_contains_WalLsn_not_io_time_overlap";
constexpr char kBackgroundPrecleanIoSchema[] =
    "background-preclean-metric schema=base62 cumulative=1 snapshot=relaxed_best_effort "
    "foreground_timing=count/elapsed_ns/max_ns units=events/ns/ns";
constexpr char kBackgroundPrecleanControlSchema[] =
    "background-preclean-control schema=base62 cumulative=1 snapshot=relaxed_best_effort "
    "background_flush=call_count/page_count/timing_count/elapsed_ns/max_ns units=calls/pages/calls/ns/ns "
    "controller_and_victim_counters=events wal_recent_fsync_ns=best_effort_ns";
constexpr char kTxnOwnerDataFixed[] =
    "txn-phase-owner seq= final=1 cleanup_terminal=/ observers= observation_to_cleanup_terminal=// "
    "hist=///////////";
constexpr char kWalSyncDepthDataFixed[] = "wal-flush-sync-depth seq= final=1 waves= max_inflight=";
constexpr char kBackgroundPrecleanIoDataFixed[] =
    "background-preclean-metric seq= final=1 fg_evict=// dep_wait=// pwrite=// read=//";
constexpr char kBackgroundPrecleanControlDataFixed[] =
    "background-preclean-control seq= final=1 bg=//// paused= throttled= ramped= clean_victim= dirty_fallback= "
    "victim_scanned= wal_recent_fsync_ns=";
constexpr std::size_t kBase62Max = 11;

bool plan_observability_enabled() noexcept {
    const char* value = std::getenv("RMDB_PLAN_OBSERVABILITY");
    return value != nullptr &&
           (std::strcmp(value, "1") == 0 || std::strcmp(value, "true") == 0 || std::strcmp(value, "TRUE") == 0);
}

static_assert(sizeof(kTxnPhaseSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kTxnPhaseActorSchemaA) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kTxnPhaseActorSchemaB) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kTxnPhaseActorSchemaC) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kTxnPhaseActorSchemaD) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kTxnOwnerSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kTxnWaitGraphSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kTxnReadOnlyWalSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kRuntimeMetricBundleSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kReadViewShadowSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kTxnPhaseDataPrefix) - 1 + kBase62Max + sizeof(kTxnPhaseDataFinal) - 1 +
                  sizeof(kTxnPhaseLongestName) - 1 + 1 +
                  (3 + TransactionPhaseMetrics::kHistogramBuckets) * (kBase62Max + 1) + sizeof(" hist=") - 1 +
                  kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
static_assert(sizeof(kWalFlushSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kWalFlushTimingFixed) - 1 + 20 * kBase62Max + kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
static_assert(sizeof(kWalSyncDepthSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kWalSyncDepthDataFixed) - 1 + 3 * kBase62Max + kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
static_assert(sizeof(kCheckpointSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kCheckpointPhaseSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kCheckpointDependencySchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kCheckpointDependencyFixed) - 1 + 2 * kBase62Max + kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
static_assert(sizeof(kDirtyPwriteSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kBackgroundPrecleanIoSchema) - 1 + kWarnHeaderMax + 2 <= minilog::Logger::kLineBufferSize);
static_assert(sizeof(kBackgroundPrecleanControlSchema) - 1 + kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
static_assert(sizeof(kTxnOwnerDataFixed) - 1 + 19 * kBase62Max + kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
static_assert(sizeof(kBackgroundPrecleanIoDataFixed) - 1 + 13 * kBase62Max + kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
static_assert(sizeof(kBackgroundPrecleanControlDataFixed) - 1 + 13 * kBase62Max + kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
static_assert(sizeof(kCheckpointCounterFixed) - 1 + 17 * kBase62Max + kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
static_assert(sizeof(kCheckpointTimingAFixed) - 1 + 16 * kBase62Max + kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
static_assert(sizeof(kCheckpointTimingBFixed) - 1 + 13 * kBase62Max + kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
static_assert(sizeof(kWalFlushCompletedPrefix) - 1 + kBase62Max + sizeof(" final=1 completed=") - 1 +
                  13 * (kBase62Max + 1) + sizeof("lag=") - 1 + 5 * (kBase62Max + 1) + kWarnHeaderMax + 2 <=
              minilog::Logger::kLineBufferSize);
std::string Base62(uint64_t value) {
    constexpr char digits[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    char buffer[11];
    size_t size = 0;
    do {
        buffer[size++] = digits[value % 62];
        value /= 62;
    } while (value != 0);
    std::string result;
    while (size != 0) {
        result += buffer[--size];
    }
    return result;
}
void LogTransactionPhaseMetrics(uint64_t sequence) {
    if (!transaction_phase_metrics->enabled()) {
        return;
    }
    LOG_WARN("%s", kTxnPhaseSchema);
    LOG_WARN("%s", kTxnPhaseActorSchemaA);
    LOG_WARN("%s", kTxnPhaseActorSchemaB);
    LOG_WARN("%s", kTxnPhaseActorSchemaC);
    LOG_WARN("%s", kTxnPhaseActorSchemaD);
    LOG_WARN("%s", kTxnOwnerSchema);
    LOG_WARN("%s", kTxnReadOnlyWalSchema);
    constexpr const char* names[] = {
        "exec_batch_wall",
        "record_lock_wait",
        "commit_prepare_sort_validate",
        "commit_wal_enqueue",
        "commit_wal_coverage",
        "commit_tuple_finalize",
        "commit_frontier_wait",
        "commit_release",
        "commit_owner_cleanup",
        "abort_wal",
        "abort_heap_undo",
        "abort_index_undo",
        "abort_ssi_cleanup",
        "abort_release",
        "wait_for_graph_build",
    };
    static_assert(std::size(names) == static_cast<size_t>(TransactionPhaseMetrics::Phase::Count));
    for (size_t index = 0; index < static_cast<size_t>(TransactionPhaseMetrics::Phase::Count); ++index) {
        const auto snapshot = transaction_phase_metrics->snapshot(static_cast<TransactionPhaseMetrics::Phase>(index));
        LOG_WARN("txn-phase seq=%s final=%d %s:%s/%s/%s hist=%s/%s/%s/%s/%s/%s/%s/%s/%s/%s/%s/%s",
                 Base62(sequence).c_str(), sequence == UINT64_MAX, names[index], Base62(snapshot.count).c_str(),
                 Base62(snapshot.elapsed_ns).c_str(), Base62(snapshot.max_ns).c_str(),
                 Base62(snapshot.histogram[0]).c_str(), Base62(snapshot.histogram[1]).c_str(),
                 Base62(snapshot.histogram[2]).c_str(), Base62(snapshot.histogram[3]).c_str(),
                 Base62(snapshot.histogram[4]).c_str(), Base62(snapshot.histogram[5]).c_str(),
                 Base62(snapshot.histogram[6]).c_str(), Base62(snapshot.histogram[7]).c_str(),
                 Base62(snapshot.histogram[8]).c_str(), Base62(snapshot.histogram[9]).c_str(),
                 Base62(snapshot.histogram[10]).c_str(), Base62(snapshot.histogram[11]).c_str());
    }
    const auto owner = transaction_phase_metrics->owner_conflict_snapshot();
    LOG_WARN("txn-phase-owner seq=%s final=%d cleanup_terminal=%s/%s observers=%s "
             "observation_to_cleanup_terminal=%s/%s/%s hist=%s/%s/%s/%s/%s/%s/%s/%s/%s/%s/%s/%s",
             Base62(sequence).c_str(), sequence == UINT64_MAX, Base62(owner.commit_cleanup_terminals).c_str(),
             Base62(owner.abort_cleanup_terminals).c_str(), Base62(owner.observer_count).c_str(),
             Base62(owner.observation_to_cleanup_terminal.count).c_str(),
             Base62(owner.observation_to_cleanup_terminal.elapsed_ns).c_str(),
             Base62(owner.observation_to_cleanup_terminal.max_ns).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[0]).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[1]).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[2]).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[3]).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[4]).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[5]).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[6]).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[7]).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[8]).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[9]).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[10]).c_str(),
             Base62(owner.observation_to_cleanup_terminal.histogram[11]).c_str());
    const auto graph = transaction_phase_metrics->wait_for_graph_snapshot();
    LOG_WARN("%s", kTxnWaitGraphSchema);
    LOG_WARN("txn-phase-waitgraph seq=%s final=%d shards=%s/%s queues=%s/%s edges=%s/%s", Base62(sequence).c_str(),
             sequence == UINT64_MAX, Base62(graph.shards.sum).c_str(), Base62(graph.shards.max).c_str(),
             Base62(graph.queues.sum).c_str(), Base62(graph.queues.max).c_str(), Base62(graph.edges.sum).c_str(),
             Base62(graph.edges.max).c_str());
    const auto read_only_wal = transaction_phase_metrics->read_only_wal_snapshot();
    LOG_WARN("txn-phase-readonly-wal seq=%s final=%d inferred_successful_begin_commit_pairs=%s",
             Base62(sequence).c_str(), sequence == UINT64_MAX,
             Base62(read_only_wal.inferred_successful_begin_commit_pairs).c_str());
}
void LogReadViewShadowMetrics(uint64_t sequence) {
    if (!txn_manager->read_view_shadow_enabled())
        return;
    const auto snapshot = txn_manager->read_view_shadow_snapshot();
    LOG_WARN("%s", kReadViewShadowSchema);
    LOG_WARN("readview-shadow seq=%s final=%d capture=%s rc_replace=%s class=%s/%s/%s/%s/%s", Base62(sequence).c_str(),
             sequence == UINT64_MAX, Base62(snapshot.captures).c_str(), Base62(snapshot.rc_replacements).c_str(),
             Base62(snapshot.classification[0]).c_str(), Base62(snapshot.classification[1]).c_str(),
             Base62(snapshot.classification[2]).c_str(), Base62(snapshot.classification[3]).c_str(),
             Base62(snapshot.classification[4]).c_str());
}
AbortOperation AbortOperationFor(ast::AstType type) noexcept {
    switch (type) {
    case ast::AstType::SelectStmt:
    case ast::AstType::SelectFromUnionStmt:
    case ast::AstType::ExplainAnalyze:
        return AbortOperation::SELECT;
    case ast::AstType::InsertStmt:
        return AbortOperation::INSERT;
    case ast::AstType::UpdateStmt:
        return AbortOperation::UPDATE;
    case ast::AstType::DeleteStmt:
        return AbortOperation::DELETE;
    case ast::AstType::TxnBegin:
    case ast::AstType::TxnCommit:
    case ast::AstType::TxnAbort:
    case ast::AstType::TxnRollback:
        return AbortOperation::TXN_CONTROL;
    default:
        return AbortOperation::OTHER;
    }
}
void CaptureAbortObservation(TransactionAbortException& exception, const Context& context) noexcept {
    Transaction* txn = context.txn_;
    exception.SetObservation(context.abort_origin_,
                             txn != nullptr && txn->get_txn_mode() ? AbortTxnMode::EXPLICIT : AbortTxnMode::AUTOCOMMIT,
                             txn != nullptr ? txn->get_isolation_level() : context.isolation_level_,
                             context.abort_operation_);
}
void LogTransactionAbortMetrics(uint64_t sequence) {
    if (!transaction_abort_metrics->enabled())
        return;
    LOG_WARN("abort-metric schema=base62 cumulative=1 table_slots=64 overflow_id=0 fields=o/m/i/p/r/d:count");
    for (size_t o = 0; o < TransactionAbortMetrics::kOrigins; ++o)
        for (size_t m = 0; m < TransactionAbortMetrics::kModes; ++m)
            for (size_t i = 0; i < TransactionAbortMetrics::kIsolations; ++i)
                for (size_t p = 0; p < TransactionAbortMetrics::kOperations; ++p)
                    for (size_t r = 0; r < TransactionAbortMetrics::kReasons; ++r)
                        for (size_t d = 0; d < TransactionAbortMetrics::kDetails; ++d) {
                            const auto cell = transaction_abort_metrics->snapshot(
                                static_cast<AbortOrigin>(o), static_cast<AbortTxnMode>(m),
                                static_cast<IsolationLevel>(i), static_cast<AbortOperation>(p),
                                static_cast<AbortReason>(r), static_cast<AbortDetail>(d));
                            if (cell.count != 0)
                                LOG_WARN("abort-metric seq=%s final=%d %zu/%zu/%zu/%zu/%zu/%zu:%s",
                                         Base62(sequence).c_str(), sequence == UINT64_MAX, o, m, i, p, r, d,
                                         Base62(cell.count).c_str());
                        }
    for (size_t slot = 0; slot <= TransactionAbortMetrics::kOverflowSlot; ++slot)
        for (size_t r = 0; r < TransactionAbortMetrics::kReasons; ++r)
            for (size_t d = 0; d < TransactionAbortMetrics::kDetails; ++d) {
                const auto cell = transaction_abort_metrics->table_snapshot(slot, static_cast<AbortReason>(r),
                                                                            static_cast<AbortDetail>(d));
                if (cell.count != 0)
                    LOG_WARN("abort-metric-table seq=%s final=%d bucket=%s id=%s r=%zu d=%zu c=%s",
                             Base62(sequence).c_str(), sequence == UINT64_MAX,
                             slot == TransactionAbortMetrics::kUnknownSlot
                                 ? "unknown"
                                 : (slot == TransactionAbortMetrics::kOverflowSlot ? "overflow" : "table"),
                             Base62(cell.runtime_id).c_str(), r, d, Base62(cell.count).c_str());
            }
}
void LogWalFlushMetrics(uint64_t sequence) {
    if (!wal_flush_metrics->enabled())
        return;
    const auto snapshot = wal_flush_metrics->snapshot();
    LOG_WARN("%s", kWalFlushSchema);
    LOG_WARN("wal-flush-metric seq=%s final=%d covered=%s leaders=%s rotations=%s max_batches_per_leader=%s "
             "followers=%s follower_wait=%s/%s/%s "
             "coalescing_wait=%s/%s/%s physical=%s pwrite=%s/%s/%s/%s fdatasync=%s/%s/%s",
             Base62(sequence).c_str(), sequence == UINT64_MAX, Base62(snapshot.already_covered_fast_paths).c_str(),
             Base62(snapshot.leader_requests).c_str(), Base62(snapshot.leader_rotations).c_str(),
             Base62(snapshot.max_batches_per_leader).c_str(), Base62(snapshot.follower_requests).c_str(),
             Base62(snapshot.follower_wait.count).c_str(), Base62(snapshot.follower_wait.elapsed_ns).c_str(),
             Base62(snapshot.follower_wait.max_ns).c_str(), Base62(snapshot.coalescing_wait.count).c_str(),
             Base62(snapshot.coalescing_wait.elapsed_ns).c_str(), Base62(snapshot.coalescing_wait.max_ns).c_str(),
             Base62(snapshot.physical_flush_iterations).c_str(), Base62(snapshot.pwrite.count).c_str(),
             Base62(snapshot.pwrite_bytes).c_str(), Base62(snapshot.pwrite.elapsed_ns).c_str(),
             Base62(snapshot.pwrite.max_ns).c_str(), Base62(snapshot.fdatasync.count).c_str(),
             Base62(snapshot.fdatasync.elapsed_ns).c_str(), Base62(snapshot.fdatasync.max_ns).c_str());
    LOG_WARN(
        "wal-flush-metric seq=%s final=%d completed=%s/%s/%s/%s/%s/%s/%s/%s/%s/%s/%s/%s/%s "
        "lag=%s/%s/%s/%s/%s",
        Base62(sequence).c_str(), sequence == UINT64_MAX, Base62(snapshot.completed_batch_histogram[0]).c_str(),
        Base62(snapshot.completed_batch_histogram[1]).c_str(), Base62(snapshot.completed_batch_histogram[2]).c_str(),
        Base62(snapshot.completed_batch_histogram[3]).c_str(), Base62(snapshot.completed_batch_histogram[4]).c_str(),
        Base62(snapshot.completed_batch_histogram[5]).c_str(), Base62(snapshot.completed_batch_histogram[6]).c_str(),
        Base62(snapshot.completed_batch_histogram[7]).c_str(), Base62(snapshot.completed_batch_histogram[8]).c_str(),
        Base62(snapshot.completed_batch_histogram[9]).c_str(), Base62(snapshot.completed_batch_histogram[10]).c_str(),
        Base62(snapshot.completed_batch_histogram[11]).c_str(), Base62(snapshot.completed_batch_histogram[12]).c_str(),
        Base62(snapshot.durable_lag_samples).c_str(), Base62(snapshot.durable_lag_before_sum).c_str(),
        Base62(snapshot.durable_lag_before_max).c_str(), Base62(snapshot.durable_lag_after_sum).c_str(),
        Base62(snapshot.durable_lag_after_max).c_str());
    LOG_WARN("wal-flush-metric seq=%s final=%d commit_initial_role=covered:%s/leader:%s/follower:%s",
             Base62(sequence).c_str(), sequence == UINT64_MAX, Base62(snapshot.commit_requests.already_covered).c_str(),
             Base62(snapshot.commit_requests.leader_requests).c_str(),
             Base62(snapshot.commit_requests.follower_requests).c_str());
    LOG_WARN("%s", kWalSyncDepthSchema);
    LOG_WARN("wal-flush-sync-depth seq=%s final=%d waves=%s max_inflight=%s", Base62(sequence).c_str(),
             sequence == UINT64_MAX, Base62(snapshot.sync_depth_two_waves).c_str(),
             Base62(snapshot.sync_depth_two_max_inflight).c_str());
}
void LogCheckpointMetrics(uint64_t sequence) {
    if (!checkpoint_phase_metrics->enabled())
        return;
    const auto s = checkpoint_phase_metrics->snapshot();
    const auto dependency = buffer_pool_manager->checkpoint_dependency_metrics();
    LOG_WARN("%s", kCheckpointSchema);
    LOG_WARN("%s", kCheckpointPhaseSchema);
    LOG_WARN("%s", kCheckpointDependencySchema);
    LOG_WARN("checkpoint-metric config auto_bytes=%s tick_bytes=%s tick_time_us=%s io_quantum_pages=%s",
             Base62(static_cast<uint64_t>(checkpoint_options.auto_checkpoint_bytes)).c_str(),
             Base62(checkpoint_options.tick_bytes).c_str(), Base62(checkpoint_options.tick_time_us).c_str(),
             Base62(checkpoint_options.io_quantum_pages).c_str());
    LOG_WARN("checkpoint-metric seq=%s final=%d clean=%s/%s/%s fuzzy=%s/%s/%s/%s pages=%s/%s/%s/%s defer=%s/%s "
             "budget_yield=%s/%s/%s",
             Base62(sequence).c_str(), sequence == UINT64_MAX, Base62(s.clean_attempts).c_str(),
             Base62(s.clean_successes).c_str(), Base62(s.clean_failures).c_str(), Base62(s.fuzzy_attempts).c_str(),
             Base62(s.fuzzy_successes).c_str(), Base62(s.fuzzy_failures).c_str(), Base62(s.fuzzy_cancels).c_str(),
             Base62(s.pages_marked).c_str(), Base62(s.page_write_calls).c_str(), Base62(s.pages_written).c_str(),
             Base62(s.pages_remaining_max).c_str(), Base62(s.retry_deferrals).c_str(),
             Base62(s.deadline_deferrals).c_str(), Base62(s.budget_yields_io).c_str(),
             Base62(s.budget_yields_time).c_str(), Base62(s.zero_progress_yields).c_str());
    LOG_WARN(
        "checkpoint-metric seq=%s final=%d drain=%s/%s/%s cut=%s/%s/%s page=%s/%s/%s "
        "clean_data=%s/%s/%s clean_meta=%s/%s/%s",
        Base62(sequence).c_str(), sequence == UINT64_MAX, Base62(s.timing[0].count).c_str(),
        Base62(s.timing[0].elapsed_ns).c_str(), Base62(s.timing[0].max_ns).c_str(), Base62(s.timing[1].count).c_str(),
        Base62(s.timing[1].elapsed_ns).c_str(), Base62(s.timing[1].max_ns).c_str(), Base62(s.timing[2].count).c_str(),
        Base62(s.timing[2].elapsed_ns).c_str(), Base62(s.timing[2].max_ns).c_str(), Base62(s.timing[3].count).c_str(),
        Base62(s.timing[3].elapsed_ns).c_str(), Base62(s.timing[3].max_ns).c_str(), Base62(s.timing[4].count).c_str(),
        Base62(s.timing[4].elapsed_ns).c_str(), Base62(s.timing[4].max_ns).c_str());
    LOG_WARN(
        "checkpoint-metric seq=%s final=%d fuzzy_final=%s/%s/%s manifest=%s/%s/%s "
        "wal_reset=%s/%s/%s lifetime=%s/%s/%s",
        Base62(sequence).c_str(), sequence == UINT64_MAX, Base62(s.timing[5].count).c_str(),
        Base62(s.timing[5].elapsed_ns).c_str(), Base62(s.timing[5].max_ns).c_str(), Base62(s.timing[6].count).c_str(),
        Base62(s.timing[6].elapsed_ns).c_str(), Base62(s.timing[6].max_ns).c_str(), Base62(s.timing[7].count).c_str(),
        Base62(s.timing[7].elapsed_ns).c_str(), Base62(s.timing[7].max_ns).c_str(), Base62(s.timing[8].count).c_str(),
        Base62(s.timing[8].elapsed_ns).c_str(), Base62(s.timing[8].max_ns).c_str());
    LOG_WARN("checkpoint-dependency-metric seq=%s final=%d checkpoint_dependency=%s/%s", Base62(sequence).c_str(),
             sequence == UINT64_MAX, Base62(dependency.already_covered).c_str(),
             Base62(dependency.coverage_requested).c_str());
}

void LogBackgroundPrecleanMetrics(uint64_t sequence) {
    const auto& metrics = buffer_pool_manager->background_preclean_metrics();
    if (!metrics.enabled())
        return;
    const auto s = metrics.snapshot();
    LOG_WARN("%s", kBackgroundPrecleanIoSchema);
    LOG_WARN("%s", kBackgroundPrecleanControlSchema);
    LOG_WARN("%s", kDirtyPwriteSchema);
    LOG_WARN("background-preclean-metric seq=%s final=%d fg_evict=%s/%s/%s dep_wait=%s/%s/%s pwrite=%s/%s/%s "
             "read=%s/%s/%s",
             Base62(sequence).c_str(), sequence == UINT64_MAX, Base62(s.foreground_dirty_eviction.count).c_str(),
             Base62(s.foreground_dirty_eviction.elapsed_ns).c_str(), Base62(s.foreground_dirty_eviction.max_ns).c_str(),
             Base62(s.foreground_dependency_wait.count).c_str(),
             Base62(s.foreground_dependency_wait.elapsed_ns).c_str(),
             Base62(s.foreground_dependency_wait.max_ns).c_str(), Base62(s.foreground_pwrite.count).c_str(),
             Base62(s.foreground_pwrite.elapsed_ns).c_str(), Base62(s.foreground_pwrite.max_ns).c_str(),
             Base62(s.foreground_read.count).c_str(), Base62(s.foreground_read.elapsed_ns).c_str(),
             Base62(s.foreground_read.max_ns).c_str());
    LOG_WARN("background-preclean-control seq=%s final=%d bg=%s/%s/%s/%s/%s paused=%s throttled=%s ramped=%s "
             "clean_victim=%s dirty_fallback=%s victim_scanned=%s wal_recent_fsync_ns=%s",
             Base62(sequence).c_str(), sequence == UINT64_MAX, Base62(s.background_flush_calls).c_str(),
             Base62(s.background_pages).c_str(), Base62(s.background_flush.count).c_str(),
             Base62(s.background_flush.elapsed_ns).c_str(), Base62(s.background_flush.max_ns).c_str(),
             Base62(s.congestion_pauses).c_str(),
             Base62(s.congestion_throttles).c_str(), Base62(s.congestion_ramps).c_str(),
             Base62(s.clean_victims).c_str(), Base62(s.dirty_victim_fallbacks).c_str(),
             Base62(s.victim_search_scanned).c_str(), Base62(log_manager->recent_fdatasync_ns()).c_str());
    LOG_WARN("dirty-pwrite-metric seq=%s final=%d foreground=%s/%s/%s/%s/%s background=%s/%s/%s/%s/%s "
             "checkpoint=%s/%s/%s/%s/%s",
             Base62(sequence).c_str(), sequence == UINT64_MAX, Base62(s.foreground_dirty_pwrite.timing.count).c_str(),
             Base62(s.foreground_dirty_pwrite.bytes).c_str(),
             Base62(s.foreground_dirty_pwrite.timing.elapsed_ns).c_str(),
             Base62(s.foreground_dirty_pwrite.timing.max_ns).c_str(),
             Base62(s.foreground_dirty_pwrite.runs_with_wal_dependency).c_str(),
             Base62(s.background_dirty_pwrite.timing.count).c_str(), Base62(s.background_dirty_pwrite.bytes).c_str(),
             Base62(s.background_dirty_pwrite.timing.elapsed_ns).c_str(),
             Base62(s.background_dirty_pwrite.timing.max_ns).c_str(),
             Base62(s.background_dirty_pwrite.runs_with_wal_dependency).c_str(),
             Base62(s.checkpoint_dirty_pwrite.timing.count).c_str(), Base62(s.checkpoint_dirty_pwrite.bytes).c_str(),
             Base62(s.checkpoint_dirty_pwrite.timing.elapsed_ns).c_str(),
             Base62(s.checkpoint_dirty_pwrite.timing.max_ns).c_str(),
             Base62(s.checkpoint_dirty_pwrite.runs_with_wal_dependency).c_str());
}

void LogRuntimeMetricBundle(uint64_t sequence) {
    LOG_WARN("%s", kRuntimeMetricBundleSchema);
    LogTransactionPhaseMetrics(sequence);
    LogReadViewShadowMetrics(sequence);
    LogTransactionAbortMetrics(sequence);
    LogWalFlushMetrics(sequence);
    LogCheckpointMetrics(sequence);
    LogBackgroundPrecleanMetrics(sequence);
    lock_manager->log_shard_metrics(sequence);
    buffer_pool_manager->log_shard_metrics(sequence);
}

struct WalPhaseMarker {
    std::string phase;
    uint64_t window{};
    uint64_t planned_unix_ns{};
    uint64_t actual_send_unix_ns{};
    uint64_t monotonic_lateness_ns{};
};

bool ParseWalPhaseMarker(const char* data, size_t size, WalPhaseMarker* marker) {
    if (data == nullptr || marker == nullptr || size == 0 || size > 256)
        return false;
    const std::string_view message(data, size);
    constexpr std::string_view prefix = "v1 phase=";
    if (message.size() < prefix.size() || message.substr(0, prefix.size()) != prefix)
        return false;
    size_t offset = prefix.size();
    const size_t phase_end = message.find(' ', offset);
    if (phase_end == std::string_view::npos || phase_end == offset)
        return false;
    marker->phase.assign(message.substr(offset, phase_end - offset));
    offset = phase_end;
    const auto parse_field = [&](std::string_view name, uint64_t* output, bool final) {
        if (message.substr(offset, name.size()) != name)
            return false;
        offset += name.size();
        const size_t end = final ? message.size() : message.find(' ', offset);
        if (end == std::string_view::npos || end == offset)
            return false;
        const char* begin_ptr = message.data() + offset;
        const char* end_ptr = message.data() + end;
        const auto parsed = std::from_chars(begin_ptr, end_ptr, *output);
        if (parsed.ec != std::errc{} || parsed.ptr != end_ptr)
            return false;
        offset = end;
        return true;
    };
    return parse_field(" window=", &marker->window, false) &&
           parse_field(" planned_unix_ns=", &marker->planned_unix_ns, false) &&
           parse_field(" actual_send_unix_ns=", &marker->actual_send_unix_ns, false) &&
           parse_field(" monotonic_lateness_ns=", &marker->monotonic_lateness_ns, true) && offset == message.size();
}

uint64_t WallClockUnixNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

void LogWalPhaseSnapshot(uint64_t sequence, const WalPhaseMarker& marker, uint64_t receive_unix_ns) {
    LOG_WARN("wal-phase valid=1 seq=%s phase=%s window=%llu planned_unix_ns=%llu actual_send_unix_ns=%llu "
             "monotonic_lateness_ns=%llu receive_unix_ns=%llu",
             Base62(sequence).c_str(), marker.phase.c_str(), static_cast<unsigned long long>(marker.window),
             static_cast<unsigned long long>(marker.planned_unix_ns),
             static_cast<unsigned long long>(marker.actual_send_unix_ns),
             static_cast<unsigned long long>(marker.monotonic_lateness_ns),
             static_cast<unsigned long long>(receive_unix_ns));
    LogRuntimeMetricBundle(sequence);
}
} // namespace

void sigint_handler(int signo) {
    (void)signo;
    should_exit = 1;
    const int fd = listener_fd;
    if (fd >= 0) {
        shutdown(fd, SHUT_RDWR);
    }
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

class BatchResultBuilder final : public QueryResultSink {
public:
    BatchResultBuilder() {
        write_success_header();
    }

    void begin_operation(std::uint16_t operation_index) {
        if (operation_active_) {
            throw wire_protocol::ProtocolError("batch result operation was not finished");
        }
        operation_active_ = true;
        query_active_ = false;
        operation_index_ = operation_index;
        row_count_ = 0;
    }

    void begin_query(const std::vector<ColMeta>& columns, const std::vector<std::string>& output_names) override {
        if (!operation_active_ || query_active_ || columns.size() != output_names.size()) {
            throw wire_protocol::ProtocolError("invalid batch query schema");
        }
        query_active_ = true;
        writer_.u16(operation_index_);
        row_count_offset_ = writer_.size();
        writer_.u32(0);
        ++query_count_;
    }

    void append_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) override {
        if (!query_active_ || data == nullptr) {
            throw wire_protocol::ProtocolError("batch query row emitted before schema");
        }
        for (const auto& column : columns) {
            if (column.offset < 0 || column.len < 0 || static_cast<std::size_t>(column.offset) > size ||
                static_cast<std::size_t>(column.len) > size - static_cast<std::size_t>(column.offset) ||
                (column.null_byte >= 0 && static_cast<std::size_t>(column.null_byte) >= size)) {
                throw wire_protocol::ProtocolError("executor returned an invalid tuple");
            }
            const bool present = !is_null(data, column);
            const char* cell = data + column.offset;
            const Type type = protocol_type(column.type);
            const std::size_t encoded_size = type == Type::CHAR && present
                                                 ? strnlen(cell, static_cast<std::size_t>(column.len))
                                                 : static_cast<std::size_t>(column.len);
            wire_protocol::encode_raw_value(writer_, type, present, cell, encoded_size);
        }
        if (row_count_ == UINT32_MAX) {
            throw wire_protocol::ProtocolError("batch query row count exceeds protocol limit");
        }
        ++row_count_;
    }

    void finish_operation(bool query) {
        if (!operation_active_ || query != query_active_) {
            throw wire_protocol::ProtocolError("batch query result kind mismatch");
        }
        if (query_active_) {
            writer_.patch_u32(row_count_offset_, row_count_);
        }
        operation_active_ = false;
        query_active_ = false;
    }

    std::vector<std::uint8_t> success(std::uint16_t executed) {
        if (operation_active_) {
            throw wire_protocol::ProtocolError("batch result operation was not finished");
        }
        writer_.patch_u16(kExecutedOffset, executed);
        writer_.patch_u16(kQueryCountOffset, query_count_);
        return writer_.take();
    }

    std::vector<std::uint8_t> failure(std::uint16_t executed, std::uint8_t status, std::uint16_t failed,
                                      const std::string& diagnostic) {
        writer_.rewind(0);
        operation_active_ = false;
        query_active_ = false;
        query_count_ = 0;
        writer_.u16(executed);
        writer_.u8(status);
        writer_.u16(failed);
        writer_.u32(static_cast<std::uint32_t>(diagnostic.size()));
        writer_.bytes(diagnostic);
        writer_.u16(0);
        return writer_.take();
    }

private:
    static constexpr std::size_t kExecutedOffset = 0;
    static constexpr std::size_t kQueryCountOffset = 9;

    void write_success_header() {
        writer_.u16(0);
        writer_.u8(0);
        writer_.u16(0xffff);
        writer_.u32(0);
        writer_.u16(0);
    }

    Writer writer_;
    std::uint16_t operation_index_{0};
    std::size_t row_count_offset_{0};
    std::uint32_t row_count_{0};
    std::uint16_t query_count_{0};
    bool operation_active_{false};
    bool query_active_{false};
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

struct BatchExecutionContext {
    explicit BatchExecutionContext(SessionState& session)
        : context(lock_manager.get(), log_manager.get(), nullptr, nullptr, nullptr, txn_manager.get()),
          catalog_guard(sm_manager->acquire_catalog_shared()) {
        reset_for_operation(session, nullptr, true);
    }

    void reset_for_operation(SessionState& session, QueryResultSink* result_sink, bool typed_fast_path) {
        if (!typed_fast_path && legacy_response.empty()) {
            legacy_response.assign(BUFFER_LENGTH, 0);
        }
        if (legacy_offset > 0) {
            const std::size_t used =
                std::min(static_cast<std::size_t>(legacy_offset), static_cast<std::size_t>(legacy_response.size()));
            std::fill_n(legacy_response.data(), used, '\0');
        }
        legacy_offset = 0;
        context.txn_ = nullptr;
        context.data_send_ = typed_fast_path ? nullptr : legacy_response.data();
        context.offset_ = typed_fast_path ? nullptr : &legacy_offset;
        context.ellipsis_ = false;
        context.isolation_level_ = session.isolation;
        context.enable_ssi_read_tracking_ = false;
        context.abort_origin_ = AbortOrigin::EXEC_BATCH;
        context.abort_operation_ = AbortOperation::OTHER;
        context.abort_metrics_enabled_ = transaction_abort_metrics->enabled();
        context.result_sink_ = result_sink;
        context.output_file_enabled_ = &session.output_file_enabled;
    }

    std::vector<char> legacy_response;
    int legacy_offset{0};
    Context context;
    SmManager::CatalogSharedGuard catalog_guard;
    BatchResultBuilder result;
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
    std::unique_ptr<const PreparedPlanDescriptor> descriptor;
    // Declared after descriptor so normal destruction releases the frame's
    // borrowed metadata pointers before destroying their owner.
    std::unique_ptr<PreparedSelectExecutionFrame> select_frame;
    std::string database_identity;
    std::uint64_t catalog_generation = 0;
};

const char* prepared_plan_route(const PreparedStatement& statement) noexcept {
    if (statement.descriptor == nullptr) {
        return "descriptor_unavailable";
    }
    const PreparedPlanDescriptor& descriptor = *statement.descriptor;
    const DMLPlan* dml = descriptor.dml_plan();
    if (dml != nullptr && dml->compiled_point_program_ != nullptr) {
        return "compiled_point";
    }
    if (descriptor.update_executable() != nullptr && descriptor.update_executable()->point_update.has_value()) {
        return "prepared_point_update";
    }
    if (descriptor.select_executable() != nullptr) {
        return "prepared_select";
    }
    return descriptor.eligible() ? "bound_plan" : "fallback";
}

void log_prepared_plan_observability(const PreparedStatement& statement) {
    const PreparedPlanDescriptor* descriptor = statement.descriptor.get();
    const Plan* plan = nullptr;
    if (descriptor != nullptr && descriptor->dml_plan() != nullptr) {
        plan = descriptor->dml_plan()->subplan_.get();
    }
    std::string summary = Portal::render_plan_observability(plan);
    if (summary.empty()) {
        summary = "unavailable";
    }
    LOG_WARN("plan-observability event=prepare statement_id=%u query=%d eligible=%d route=%s plan=%s", statement.id,
             statement.query ? 1 : 0, descriptor != nullptr && descriptor->eligible() ? 1 : 0,
             prepared_plan_route(statement), summary.c_str());
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

ExecutionOutcome execute_tree_impl(std::unique_ptr<ast::TreeNode> parse_tree, SessionState& session,
                                   QueryResultSink* result_sink, bool catalog_guard_held = false,
                                   Context* reusable_context = nullptr) {
    std::vector<char> response;
    int offset = 0;
    std::optional<Context> owned_context;
    if (reusable_context == nullptr) {
        response.assign(BUFFER_LENGTH, 0);
        owned_context.emplace(lock_manager.get(), log_manager.get(), nullptr, response.data(), &offset,
                              txn_manager.get());
        reusable_context = &*owned_context;
    }
    Context& context = *reusable_context;
    context.isolation_level_ = session.isolation;
    context.result_sink_ = result_sink;
    context.output_file_enabled_ = &session.output_file_enabled;
    context.abort_metrics_enabled_ = transaction_abort_metrics->enabled();
    if (parse_tree == nullptr)
        return {};
    const auto parsed_type = parse_tree->type;
    context.abort_operation_ = AbortOperationFor(parsed_type);
    const bool catalog_change = changes_catalog(parsed_type);
    if (catalog_change) {
        Transaction* active = session.running_transaction();
        if (active != nullptr && active->get_txn_mode() && active->get_state() != TransactionState::COMMITTED &&
            active->get_state() != TransactionState::ABORTED) {
            throw RMDBError("structural DDL and LOAD are not allowed inside an explicit transaction");
        }
    }
    const bool is_checkpoint = parsed_type == ast::AstType::StaticCheckpoint;
    const bool is_load = parsed_type == ast::AstType::LoadStmt;
    std::optional<SmManager::CatalogSharedGuard> catalog_shared_guard;
    std::optional<SmManager::CatalogExclusiveGuard> catalog_exclusive_guard;
    // A clean checkpoint acquires checkpoint coordination before the catalog
    // guard. Do not reverse that order in the generic execution path.
    if (!catalog_guard_held && !is_checkpoint) {
        if (catalog_change) {
            catalog_exclusive_guard.emplace(sm_manager->acquire_catalog_exclusive());
        } else {
            catalog_shared_guard.emplace(sm_manager->acquire_catalog_shared());
        }
    }
    if (!is_checkpoint && !is_load) {
        SetTransaction(session, &context);
    }
    try {
        std::unique_ptr<Query> query = analyze->do_analyze(std::move(parse_tree));
        std::unique_ptr<Plan> plan = optimizer->plan_query(std::move(query), &context);
        const bool catalog_changed = plan->tag == T_CreateTable || plan->tag == T_DropTable ||
                                     plan->tag == T_CreateIndex || plan->tag == T_DropIndex || plan->tag == T_LoadData;
        std::unique_ptr<PortalStmt> statement = portal->start(std::move(plan), &context);
        const bool is_query = statement->tag == PORTAL_ONE_SELECT;
        portal->run(std::move(statement), ql_manager.get(), &session.txn_id, &context);
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
        CaptureAbortObservation(exception, context);
        abort_session(session, &context);
        throw;
    } catch (...) {
        abort_session(session, &context);
        throw;
    }
}

ExecutionOutcome execute_tree(std::unique_ptr<ast::TreeNode> parse_tree, SessionState& session,
                              QueryResultSink* result_sink) {
    return execute_tree_impl(std::move(parse_tree), session, result_sink);
}

ExecutionOutcome execute_tree_under_catalog_guard(std::unique_ptr<ast::TreeNode> parse_tree, SessionState& session,
                                                  QueryResultSink* result_sink, Context* reusable_context) {
    return execute_tree_impl(std::move(parse_tree), session, result_sink, true, reusable_context);
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

ExecutionOutcome execute_prepared_operation(PreparedStatement& prepared_statement, const ParameterFrame& parameters,
                                            SessionState& session, QueryResultSink* result_sink,
                                            Context* reusable_context) {
    if (reusable_context == nullptr) {
        throw InternalError("prepared operation requires a reusable Context");
    }
    if (prepared_statement.descriptor == nullptr) {
        throw InternalError("prepared operation requires a plan descriptor");
    }
    const PreparedPlanDescriptor& descriptor = *prepared_statement.descriptor;
    Context& context = *reusable_context;
    context.isolation_level_ = session.isolation;
    context.result_sink_ = result_sink;
    context.output_file_enabled_ = &session.output_file_enabled;
    SetTransaction(session, &context);
    try {
        if (parameters.size() != descriptor.parameter_layout().size()) {
            throw RMDBError("prepared parameter count does not match descriptor");
        }
        bool is_query = false;
        const PreparedSelectExecutable* select_executable = descriptor.select_executable();
        const bool reusable_select =
            descriptor.statement_kind() == PreparedStatementKind::Select && select_executable != nullptr &&
            select_executable->scan.sm_manager == sm_manager.get() &&
            descriptor.catalog_generation() == sm_manager->get_catalog_generation() &&
            descriptor.database_identity() == sm_manager->get_database_identity_under_catalog_guard();
        if (reusable_select) {
            if (prepared_statement.select_frame == nullptr) {
                prepared_statement.select_frame = PreparedSelectExecutionFrame::Build(*select_executable);
            }
            if (prepared_statement.select_frame != nullptr) {
                // The lease must unwind before commit/abort: index and heap
                // page guards cannot survive into transaction completion.
                auto lease = prepared_statement.select_frame->begin_operation(parameters, &context);
                ql_manager->select_from(lease.root(), descriptor.output_names(), &context);
                lease.close();
                is_query = true;
            } else {
                std::unique_ptr<PortalStmt> statement = portal->start_prepared(descriptor, parameters, &context);
                is_query = statement->tag == PORTAL_ONE_SELECT;
                portal->run(std::move(statement), ql_manager.get(), &session.txn_id, &context);
            }
        } else {
            std::unique_ptr<PortalStmt> statement = portal->start_prepared(descriptor, parameters, &context);
            is_query = statement->tag == PORTAL_ONE_SELECT;
            portal->run(std::move(statement), ql_manager.get(), &session.txn_id, &context);
        }
        session.isolation = context.isolation_level_;
        portal->drop();
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
        CaptureAbortObservation(exception, context);
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
    context.preparing_statement_ = true;
    auto typed_parameters = make_typed_parameter_nodes(parameters);
    auto bound = ast::clone_bound_tree(*template_tree, typed_parameters);
    std::unique_ptr<Query> query_tree = analyze->do_analyze(std::move(bound));
    std::unique_ptr<Plan> plan = optimizer->plan_query(std::move(query_tree), &context);
    const bool actual_query = plan->tag == T_select;
    if (actual_query != query)
        throw wire_protocol::ProtocolError("prepared result kind does not match SQL");
    PreparedStatement result;
    result.id = id;
    result.query = query;
    result.template_tree = std::move(template_tree);
    result.parameters = std::move(parameters);
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
    // The frame borrows binding metadata from the old descriptor. Destroy it
    // before move-assigning the refreshed descriptor.
    statement.select_frame.reset();
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
            if (changes_catalog(template_tree->type) || template_tree->type == ast::AstType::StaticCheckpoint) {
                throw wire_protocol::ProtocolError("PREPARE_SET does not allow structural DDL, LOAD, or checkpoint");
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
        std::vector<std::uint16_t> installed_ids;
        installed_ids.reserve(pending.size());
        for (auto& statement : pending) {
            installed_ids.push_back(statement.id);
            replacement.emplace(statement.id, std::move(statement));
        }
        prepared.swap(replacement);
        if (plan_observability_enabled()) {
            for (const std::uint16_t statement_id : installed_ids) {
                log_prepared_plan_observability(prepared.at(statement_id));
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
        if (it->second.template_tree == nullptr || changes_catalog(it->second.template_tree->type) ||
            it->second.template_tree->type == ast::AstType::StaticCheckpoint) {
            throw wire_protocol::ProtocolError("prepared structural DDL, LOAD, or checkpoint is not executable");
        }
        Operation operation{&it->second, {}};
        for (Type type : operation.statement->parameters) {
            operation.values.push_back(wire_protocol::decode_value(reader, type));
        }
        operations.push_back(std::move(operation));
    }
    reader.require_end();
    TransactionPhaseMetrics::Scope batch_timer(transaction_phase_metrics.get(),
                                               TransactionPhaseMetrics::Phase::ExecBatchWall);

    std::uint16_t failed = 0xffff;
    std::uint16_t executed = 0;
    BatchExecutionContext batch(session);
    try {
        for (std::uint16_t i = 0; i < operation_count; ++i) {
            auto* prepared_statement = operations[i].statement;
            if (prepared_statement->catalog_generation != sm_manager->get_catalog_generation() ||
                prepared_statement->database_identity != sm_manager->get_database_identity_under_catalog_guard()) {
                revalidate_prepared(*prepared_statement, session.isolation);
            }
            const bool prepared_fast_path = descriptor_runtime_eligible(prepared_statement->descriptor.get());
            batch.reset_for_operation(session, &batch.result, prepared_fast_path);
            batch.context.abort_operation_ = AbortOperationFor(prepared_statement->template_tree->type);
            batch.result.begin_operation(i);
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
            if (prepared_fast_path) {
                const auto parameter_frame = make_parameter_frame(operations[i].values);
                outcome = execute_prepared_operation(*prepared_statement, parameter_frame, session, &batch.result,
                                                     &batch.context);
            } else {
                auto values = make_bindings();
                outcome =
                    execute_tree_under_catalog_guard(ast::clone_bound_tree(*prepared_statement->template_tree, values),
                                                     session, &batch.result, &batch.context);
            }
            batch.result.finish_operation(outcome.query);
            ++executed;
        }
    } catch (TransactionAbortException& exception) {
        failed = executed;
        abort_session(session, &batch.context);
        // TransactionAbortException does not override what(); use the same
        // diagnostic text the EXEC_STREAM path reports.
        const auto text = exception.GetInfo();
        transaction_abort_metrics->record(exception);
        const auto payload = batch.result.failure(executed, 1, failed, text);
        batch_timer.Finish();
        wire_protocol::write_frame(fd, Tag::BATCH_RESULT, payload);
        return;
    } catch (const std::exception& exception) {
        failed = executed;
        abort_session(session, &batch.context);
        const auto text = diagnostic(exception);
        const auto payload = batch.result.failure(executed, 2, failed, text);
        batch_timer.Finish();
        wire_protocol::write_frame(fd, Tag::BATCH_RESULT, payload);
        return;
    }

    const auto payload = batch.result.success(executed);
    batch_timer.Finish();
    wire_protocol::write_frame(fd, Tag::BATCH_RESULT, payload);
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
                transaction_abort_metrics->record(exception);
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
}
} // namespace

void start_server(std::uint16_t port) {
    struct ClientThread {
        std::shared_ptr<std::atomic<bool>> done;
        std::thread thread;

        ClientThread(std::shared_ptr<std::atomic<bool>> completion, int fd, std::mutex* active_latch,
                     std::unordered_set<int>* active_fds)
            : done(std::move(completion)), thread([fd, completion = done, active_latch, active_fds] {
                  try {
                      client_handler(fd);
                  } catch (const std::exception& exception) {
                      LOG_WARN("client handler failed: %s", exception.what());
                  } catch (...) {
                      LOG_WARN("client handler failed with an unknown exception");
                  }
                  // Erase before close: a newly accepted connection can reuse
                  // the descriptor immediately after close(), so a late erase
                  // must never remove that new connection from the registry.
                  {
                      std::lock_guard<std::mutex> lock(*active_latch);
                      active_fds->erase(fd);
                  }
                  close(fd);
                  completion->store(true, std::memory_order_release);
              }) {}
    };

    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in {};
    std::mutex active_client_latch;
    std::unordered_set<int> active_client_fds;
    std::vector<ClientThread> client_threads;
    client_threads.reserve(MAX_CONN_LIMIT);

    const auto reap_finished_clients = [&] {
        auto it = client_threads.begin();
        while (it != client_threads.end()) {
            if (!it->done->load(std::memory_order_acquire)) {
                ++it;
                continue;
            }
            if (it->thread.joinable()) {
                it->thread.join();
            }
            it = client_threads.erase(it);
        }
    };

    const auto shutdown_active_clients = [&] {
        std::lock_guard<std::mutex> lock(active_client_latch);
        for (const int fd : active_client_fds) {
            shutdown(fd, SHUT_RDWR);
        }
    };

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
    listener_fd = sockfd_server;

    while (!should_exit) {
        reap_finished_clients();
        LOG_DEBUG("waiting for new connection");
        struct sockaddr_in s_addr_client {};
        int client_length = sizeof(s_addr_client);

        // Block here. Until server accepts a new connection.
        int sockfd = accept(sockfd_server, (struct sockaddr*)(&s_addr_client), (socklen_t*)(&client_length));
        if (sockfd == -1) {
            if (should_exit) {
                LOG_INFO("break from server listen loop");
                break;
            }
            LOG_WARN("accept failed: %s", strerror(errno));
            continue; // ignore current socket ,continue while loop.
        }

        reap_finished_clients();
        try {
            auto done = std::make_shared<std::atomic<bool>>(false);
            {
                std::lock_guard<std::mutex> lock(active_client_latch);
                active_client_fds.insert(sockfd);
            }
            client_threads.emplace_back(std::move(done), sockfd, &active_client_latch, &active_client_fds);
        } catch (const std::exception& exception) {
            {
                std::lock_guard<std::mutex> lock(active_client_latch);
                active_client_fds.erase(sockfd);
            }
            close(sockfd);
            LOG_ERROR("unable to register client handler: %s", exception.what());
            should_exit = 1;
            break;
        } catch (...) {
            {
                std::lock_guard<std::mutex> lock(active_client_latch);
                active_client_fds.erase(sockfd);
            }
            close(sockfd);
            LOG_ERROR("unable to register client handler");
            should_exit = 1;
            break;
        }
    }

    // Clear
    LOG_INFO("try to close all client connections");
    listener_fd = -1;
    int ret = shutdown(sockfd_server, SHUT_RDWR); // shut down the all or part of a full-duplex connection.
    if (ret == -1) {
        LOG_ERROR("shutdown server socket failed: %s", strerror(errno));
    }
    if (close(sockfd_server) == -1) {
        LOG_ERROR("close server socket failed: %s", strerror(errno));
    }
    shutdown_active_clients();
    for (auto& client : client_threads) {
        if (client.thread.joinable()) {
            client.thread.join();
        }
    }
    client_threads.clear();
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
        checkpoint_options = CheckpointOptions::FromEnvironment();
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

        minilog::Logger::get().set_level(minilog::LogLevel::INFO);
        const auto phase_elapsed_ms = [](std::chrono::steady_clock::time_point begin) {
            return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                .count();
        };
        const auto recovery_begin = std::chrono::steady_clock::now();
        LOG_INFO("recovery-phase phase=overall event=begin elapsed_ms=0");

        auto phase_begin = std::chrono::steady_clock::now();
        LOG_INFO("recovery-phase phase=prepare-existing-log event=begin elapsed_ms=0");
        log_manager->prepare_existing_log();
        LOG_INFO("recovery-phase phase=prepare-existing-log event=end elapsed_ms=%lld",
                 static_cast<long long>(phase_elapsed_ms(phase_begin)));
        buffer_pool_manager->set_log_manager(log_manager.get());

        // recovery database
        {
            auto recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(),
                                                              sm_manager.get(), log_manager.get());

            phase_begin = std::chrono::steady_clock::now();
            LOG_INFO("recovery-phase phase=analyze event=begin elapsed_ms=0");
            recovery->analyze();
            LOG_INFO("recovery-phase phase=analyze event=end elapsed_ms=%lld",
                     static_cast<long long>(phase_elapsed_ms(phase_begin)));
            LOG_INFO("recovery analyze wal reads: %llu (%llu bytes)",
                     static_cast<unsigned long long>(disk_manager->get_log_read_count()),
                     static_cast<unsigned long long>(disk_manager->get_log_read_bytes()));

            phase_begin = std::chrono::steady_clock::now();
            LOG_INFO("recovery-phase phase=finalize-existing-log event=begin elapsed_ms=0");
            log_manager->finalize_existing_log(recovery->get_scan_end_offset(), recovery->get_max_lsn(),
                                               recovery->get_latest_index_bindings());
            LOG_INFO("recovery-phase phase=finalize-existing-log event=end elapsed_ms=%lld",
                     static_cast<long long>(phase_elapsed_ms(phase_begin)));

            phase_begin = std::chrono::steady_clock::now();
            LOG_INFO("recovery-phase phase=page-preparation event=begin elapsed_ms=0");
            recovery->prepare_pages_for_redo();
            LOG_INFO("recovery-phase phase=page-preparation event=end elapsed_ms=%lld",
                     static_cast<long long>(phase_elapsed_ms(phase_begin)));

            phase_begin = std::chrono::steady_clock::now();
            LOG_INFO("recovery-phase phase=redo event=begin elapsed_ms=0");
            recovery->redo();
            LOG_INFO("recovery-phase phase=redo event=end elapsed_ms=%lld",
                     static_cast<long long>(phase_elapsed_ms(phase_begin)));

            phase_begin = std::chrono::steady_clock::now();
            LOG_INFO("recovery-phase phase=undo event=begin elapsed_ms=0");
            recovery->undo();
            LOG_INFO("recovery-phase phase=undo event=end elapsed_ms=%lld",
                     static_cast<long long>(phase_elapsed_ms(phase_begin)));

            // 必须在任何事务开始之前、恢复读完 WAL 与重启清单之后做：commit_ts_ 持久化
            // 在数据页里，而计数器只活在内存里。计数器从 0 重启会让上一世提交的行被
            // 判成“来自未来”而不可见（final.md:342 第 1 条）。取值的完整论证见
            // RecoveryManager::get_recovered_next_timestamp()。
            phase_begin = std::chrono::steady_clock::now();
            LOG_INFO("recovery-phase phase=seed-counters event=begin elapsed_ms=0");
            txn_manager->seed_counters_after_recovery(recovery->get_recovered_next_timestamp(),
                                                      recovery->get_recovered_next_txn_id());
            LOG_INFO("recovery-phase phase=seed-counters event=end elapsed_ms=%lld",
                     static_cast<long long>(phase_elapsed_ms(phase_begin)));
            LOG_INFO("recovery seeded counters: next_timestamp %lld, next_txn_id %lld",
                     static_cast<long long>(recovery->get_recovered_next_timestamp()),
                     static_cast<long long>(recovery->get_recovered_next_txn_id()));

            phase_begin = std::chrono::steady_clock::now();
            LOG_INFO("recovery-phase phase=index-residency event=begin elapsed_ms=0");
            sm_manager->refresh_index_residency();
            LOG_INFO("recovery-phase phase=index-residency event=end elapsed_ms=%lld",
                     static_cast<long long>(phase_elapsed_ms(phase_begin)));

            LOG_INFO("recovery-phase phase=overall event=end elapsed_ms=%lld",
                     static_cast<long long>(phase_elapsed_ms(recovery_begin)));
            minilog::Logger::get().set_level(minilog::LogLevel::WARN);
        }

        {
            std::atomic<bool> checkpoint_thread_stop{false};
            std::atomic<bool> runtime_metrics_reporter_stop{false};
            std::string wal_phase_marker_socket_name;
            int wal_phase_marker_socket_fd = -1;
            int wal_phase_marker_wake_fd = -1;
            bool wal_phase_marker_socket_ready = false;
            const char* configured_marker_socket = std::getenv("RMDB_WAL_PHASE_MARKER_SOCKET");
            if (wal_flush_metrics->enabled() && configured_marker_socket != nullptr &&
                *configured_marker_socket != '\0') {
                wal_phase_marker_socket_name = configured_marker_socket;
                sockaddr_un address{};
                if (wal_phase_marker_socket_name.size() < 2 || wal_phase_marker_socket_name.front() != '@') {
                    LOG_WARN("wal-phase invalid=1 reason=socket_name_not_abstract");
                } else if (wal_phase_marker_socket_name.size() > sizeof(address.sun_path)) {
                    LOG_WARN("wal-phase invalid=1 reason=socket_name_too_long length=%zu",
                             wal_phase_marker_socket_name.size());
                } else {
                    wal_phase_marker_socket_fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
                    wal_phase_marker_wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
                    if (wal_phase_marker_socket_fd < 0 || wal_phase_marker_wake_fd < 0) {
                        LOG_WARN("wal-phase invalid=1 reason=socket_setup errno=%d", errno);
                    } else {
                        address.sun_family = AF_UNIX;
                        address.sun_path[0] = '\0';
                        const size_t abstract_name_length = wal_phase_marker_socket_name.size() - 1;
                        std::memcpy(address.sun_path + 1, wal_phase_marker_socket_name.data() + 1,
                                    abstract_name_length);
                        const socklen_t address_size =
                            static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + 1 + abstract_name_length);
                        if (bind(wal_phase_marker_socket_fd, reinterpret_cast<sockaddr*>(&address), address_size) !=
                            0) {
                            LOG_WARN("wal-phase invalid=1 reason=socket_bind errno=%d", errno);
                        } else {
                            // Linux abstract Unix addresses disappear when this fd closes.
                            wal_phase_marker_socket_ready = true;
                        }
                    }
                }
                if (!wal_phase_marker_socket_ready) {
                    if (wal_phase_marker_socket_fd >= 0)
                        close(wal_phase_marker_socket_fd);
                    if (wal_phase_marker_wake_fd >= 0)
                        close(wal_phase_marker_wake_fd);
                    wal_phase_marker_socket_fd = -1;
                    wal_phase_marker_wake_fd = -1;
                }
            }
            const auto stop_runtime_metrics_reporter = [&] {
                runtime_metrics_reporter_stop.store(true, std::memory_order_release);
                if (wal_phase_marker_wake_fd >= 0) {
                    const uint64_t wake = 1;
                    (void)write(wal_phase_marker_wake_fd, &wake, sizeof(wake));
                }
            };
            const auto cleanup_wal_phase_markers = [&] {
                if (wal_phase_marker_socket_fd >= 0)
                    close(wal_phase_marker_socket_fd);
                if (wal_phase_marker_wake_fd >= 0)
                    close(wal_phase_marker_wake_fd);
                wal_phase_marker_socket_fd = -1;
                wal_phase_marker_wake_fd = -1;
            };
            std::mutex checkpoint_thread_latch;
            std::condition_variable checkpoint_thread_cv;
            std::thread checkpoint_thread([&checkpoint_thread_stop, &checkpoint_thread_latch, &checkpoint_thread_cv] {
                CheckpointManager checkpoint_mgr(txn_manager.get(), sm_manager.get(), log_manager.get(),
                                                 checkpoint_phase_metrics.get());
                checkpoint_mgr.SetOptions(checkpoint_options);
                std::unique_lock<std::mutex> stop_lock(checkpoint_thread_latch);
                while (!checkpoint_thread_stop.load(std::memory_order_acquire)) {
                    if (checkpoint_thread_cv.wait_for(stop_lock, std::chrono::milliseconds(100), [&] {
                            return checkpoint_thread_stop.load(std::memory_order_acquire);
                        })) {
                        break;
                    }
                    stop_lock.unlock();
                    checkpoint_mgr.Tick();
                    stop_lock.lock();
                }
            });
            std::thread runtime_metrics_reporter;
            try {
                if (transaction_phase_metrics->enabled() || transaction_abort_metrics->enabled() ||
                    wal_flush_metrics->enabled() || checkpoint_phase_metrics->enabled() ||
                    buffer_pool_manager->background_preclean_metrics().enabled() ||
                    lock_manager->shard_metrics_enabled() || buffer_pool_manager->shard_metrics_enabled() ||
                    txn_manager->read_view_shadow_enabled()) {
                    runtime_metrics_reporter = std::thread([&runtime_metrics_reporter_stop,
                                                            marker_socket_fd = wal_phase_marker_socket_fd,
                                                            marker_wake_fd = wal_phase_marker_wake_fd] {
                        uint64_t sequence = 0;
                        if (marker_socket_fd < 0) {
                            while (!runtime_metrics_reporter_stop.load(std::memory_order_acquire)) {
                                for (size_t elapsed = 0; elapsed < 50; ++elapsed) {
                                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                                    if (runtime_metrics_reporter_stop.load(std::memory_order_acquire))
                                        return;
                                }
                                ++sequence;
                                LogRuntimeMetricBundle(sequence);
                                minilog::Logger::get().flush();
                            }
                            return;
                        }

                        bool measure_start_seen = false;
                        uint64_t next_window = 1;
                        auto next_periodic = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                        while (!runtime_metrics_reporter_stop.load(std::memory_order_acquire)) {
                            const auto before_poll = std::chrono::steady_clock::now();
                            const auto remaining =
                                std::chrono::duration_cast<std::chrono::milliseconds>(next_periodic - before_poll);
                            const int timeout_ms = remaining.count() <= 0 ? 0 : static_cast<int>(remaining.count() + 1);
                            pollfd descriptors[2] = {{marker_socket_fd, POLLIN, 0}, {marker_wake_fd, POLLIN, 0}};
                            const int poll_result = poll(descriptors, 2, timeout_ms);
                            if (poll_result < 0) {
                                if (errno != EINTR)
                                    LOG_WARN("wal-phase invalid=1 reason=poll errno=%d", errno);
                                continue;
                            }
                            if ((descriptors[1].revents & POLLIN) != 0) {
                                uint64_t wake = 0;
                                (void)read(marker_wake_fd, &wake, sizeof(wake));
                                if (runtime_metrics_reporter_stop.load(std::memory_order_acquire))
                                    return;
                            }
                            if ((descriptors[0].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                                LOG_WARN("wal-phase invalid=1 reason=socket_poll revents=%d", descriptors[0].revents);
                            }
                            if ((descriptors[0].revents & POLLIN) != 0) {
                                for (;;) {
                                    char data[256];
                                    iovec payload{data, sizeof(data)};
                                    msghdr message{};
                                    message.msg_iov = &payload;
                                    message.msg_iovlen = 1;
                                    const ssize_t bytes = recvmsg(marker_socket_fd, &message, MSG_DONTWAIT);
                                    if (bytes < 0) {
                                        if (errno != EAGAIN && errno != EWOULDBLOCK) {
                                            LOG_WARN("wal-phase invalid=1 reason=recv errno=%d", errno);
                                        }
                                        break;
                                    }
                                    if ((message.msg_flags & MSG_TRUNC) != 0 || bytes > 256) {
                                        LOG_WARN("wal-phase invalid=1 reason=oversized bytes=%lld",
                                                 static_cast<long long>(bytes));
                                        continue;
                                    }
                                    WalPhaseMarker marker;
                                    if (!ParseWalPhaseMarker(data, static_cast<size_t>(bytes), &marker)) {
                                        LOG_WARN("wal-phase invalid=1 reason=malformed bytes=%lld",
                                                 static_cast<long long>(bytes));
                                        continue;
                                    }
                                    const char* invalid_reason = nullptr;
                                    if (marker.planned_unix_ns == 0 || marker.actual_send_unix_ns == 0) {
                                        invalid_reason = "sender_time";
                                    } else if (marker.phase == "measure_start") {
                                        if (marker.window != 0)
                                            invalid_reason = "start_window";
                                        else if (measure_start_seen)
                                            invalid_reason = "duplicate_start";
                                    } else if (marker.phase == "window_end") {
                                        if (!measure_start_seen)
                                            invalid_reason = "end_before_start";
                                        else if (marker.window == 0)
                                            invalid_reason = "end_window_zero";
                                        else if (marker.window < next_window)
                                            invalid_reason = "duplicate_or_out_of_order";
                                        else if (marker.window > next_window)
                                            invalid_reason = "window_gap";
                                    } else {
                                        invalid_reason = "phase";
                                    }
                                    if (invalid_reason != nullptr) {
                                        LOG_WARN(
                                            "wal-phase invalid=1 reason=%s phase=%s window=%llu expected_window=%llu",
                                            invalid_reason, marker.phase.c_str(),
                                            static_cast<unsigned long long>(marker.window),
                                            static_cast<unsigned long long>(next_window));
                                        continue;
                                    }
                                    if (marker.phase == "measure_start") {
                                        measure_start_seen = true;
                                    } else {
                                        ++next_window;
                                    }
                                    LogWalPhaseSnapshot(++sequence, marker, WallClockUnixNs());
                                    minilog::Logger::get().flush();
                                }
                            }
                            const auto after_events = std::chrono::steady_clock::now();
                            if (after_events >= next_periodic) {
                                ++sequence;
                                LogRuntimeMetricBundle(sequence);
                                minilog::Logger::get().flush();
                                do {
                                    next_periodic += std::chrono::seconds(5);
                                } while (next_periodic <= after_events);
                            }
                        }
                    });
                }

                // 开启服务端，开始接受客户端连接
                start_server(server_port);
            } catch (...) {
                checkpoint_thread_stop.store(true, std::memory_order_release);
                checkpoint_thread_cv.notify_all();
                stop_runtime_metrics_reporter();
                if (checkpoint_thread.joinable()) {
                    checkpoint_thread.join();
                }
                if (runtime_metrics_reporter.joinable()) {
                    runtime_metrics_reporter.join();
                }
                cleanup_wal_phase_markers();
                throw;
            }

            checkpoint_thread_stop.store(true, std::memory_order_release);
            checkpoint_thread_cv.notify_all();
            stop_runtime_metrics_reporter();
            if (checkpoint_thread.joinable()) {
                checkpoint_thread.join();
            }
            const bool runtime_metrics_reporter_started = runtime_metrics_reporter.joinable();
            if (runtime_metrics_reporter_started) {
                runtime_metrics_reporter.join();
            }
            cleanup_wal_phase_markers();
            // SIGINT only wakes the listener. Finish WAL durability here after
            // all client handlers, checkpoint activity, and reporting have stopped.
            log_manager->flush_log_to_disk_with_sync();
            if (runtime_metrics_reporter_started) {
                LogRuntimeMetricBundle(UINT64_MAX);
                minilog::Logger::get().flush();
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
