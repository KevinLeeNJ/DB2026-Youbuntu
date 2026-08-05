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

#include "errors.h"
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

static bool should_exit = false;

// 构建全局所需的管理器对象
static constexpr size_t SERVER_BUFFER_POOL_PAGES = (size_t{3} << 30) / PAGE_SIZE;
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(SERVER_BUFFER_POOL_PAGES, disk_manager.get());
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

auto portal = std::make_unique<Portal>(sm_manager.get());
auto analyze = std::make_unique<Analyze>(sm_manager.get());

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
    } catch (TransactionAbortException&) {
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
    } catch (TransactionAbortException&) {
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
        for (auto& statement : pending) {
            replacement.emplace(statement.id, std::move(statement));
        }
        prepared.swap(replacement);
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
        wire_protocol::write_frame(fd, Tag::BATCH_RESULT, batch.result.failure(executed, 1, failed, text));
        return;
    } catch (const std::exception& exception) {
        failed = executed;
        abort_session(session, &batch.context);
        const auto text = diagnostic(exception);
        wire_protocol::write_frame(fd, Tag::BATCH_RESULT, batch.result.failure(executed, 2, failed, text));
        return;
    }

    wire_protocol::write_frame(fd, Tag::BATCH_RESULT, batch.result.success(executed));
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
        {
            auto recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(),
                                                              sm_manager.get(), log_manager.get());
            minilog::Logger::get().set_level(minilog::LogLevel::INFO);
            const auto phase_elapsed_ms = [](std::chrono::steady_clock::time_point begin) {
                return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                    .count();
            };
            const auto recovery_begin = std::chrono::steady_clock::now();

            auto phase_begin = recovery_begin;
            recovery->analyze();
            LOG_INFO("recovery analyze: %lld ms, wal reads: %llu (%llu bytes)",
                     static_cast<long long>(phase_elapsed_ms(phase_begin)),
                     static_cast<unsigned long long>(disk_manager->get_log_read_count()),
                     static_cast<unsigned long long>(disk_manager->get_log_read_bytes()));

            phase_begin = std::chrono::steady_clock::now();
            recovery->redo();
            LOG_INFO("recovery redo: %lld ms", static_cast<long long>(phase_elapsed_ms(phase_begin)));

            phase_begin = std::chrono::steady_clock::now();
            recovery->undo();
            LOG_INFO("recovery undo: %lld ms", static_cast<long long>(phase_elapsed_ms(phase_begin)));

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

            LOG_INFO("database recovery finished in %lld ms", static_cast<long long>(phase_elapsed_ms(recovery_begin)));
            minilog::Logger::get().set_level(minilog::LogLevel::WARN);
        }

        {
            std::atomic<bool> checkpoint_thread_stop{false};
            std::thread checkpoint_thread([&checkpoint_thread_stop] {
                CheckpointManager checkpoint_mgr(txn_manager.get(), sm_manager.get(), log_manager.get());
                while (!checkpoint_thread_stop.load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    if (checkpoint_thread_stop.load()) {
                        break;
                    }
                    checkpoint_mgr.Tick();
                }
            });

            // 开启服务端，开始接受客户端连接
            start_server(server_port);

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
