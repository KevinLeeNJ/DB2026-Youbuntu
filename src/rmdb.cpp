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
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <exception>
#include <fstream>
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
#include "analyze/analyze.h"
#include "protocol/wire_protocol.h"

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 128

static bool should_exit = false;

// 构建全局所需的管理器对象
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
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

static jmp_buf jmpbuf;
void sigint_handler(int signo) {
    (void)signo;
    should_exit = true;
    log_manager->flush_log_to_disk_with_sync();
    LOG_INFO("the server received Ctrl+C and will close");
    longjmp(jmpbuf, 1);
}

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(txn_id_t* txn_id, Context* context) {
    context->txn_ = txn_manager->get_transaction(*txn_id);
    if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
        context->txn_->get_state() == TransactionState::ABORTED) {
        context->txn_ = txn_manager->begin(nullptr, context->log_mgr_, context->isolation_level_);
        *txn_id = context->txn_->get_transaction_id();
        context->txn_->set_txn_mode(false);
        context->txn_->set_isolation_level(context->isolation_level_);
    }
    txn_manager->BeginStatement(context->txn_);
}

namespace {
using wire_protocol::Reader;
using wire_protocol::Tag;
using wire_protocol::Type;
using wire_protocol::Value;
using wire_protocol::Writer;

// Prepared statements are session-owned, but their analyzed metadata depends
// on the process-wide catalog.  Bump this generation after every catalog
// mutation so a statement prepared by another connection cannot silently use
// stale column/index metadata.
std::atomic<std::uint64_t> catalog_generation{0};

Type protocol_type(ColType type) {
    return type == TYPE_INT ? Type::INT32 : type == TYPE_FLOAT ? Type::FLOAT32 : Type::CHAR;
}

std::vector<Value> protocol_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) {
    std::vector<Value> row;
    row.reserve(columns.size());
    for (const auto& column : columns) {
        if (column.offset < 0 || static_cast<std::size_t>(column.offset) + column.len > size) {
            throw wire_protocol::ProtocolError("executor returned an invalid tuple");
        }
        Value value;
        value.type = protocol_type(column.type);
        const char* cell = data + column.offset;
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
};

struct PreparedStatement {
    std::uint16_t id = 0;
    bool query = false;
    std::string sql;
    std::vector<Type> parameters;
    std::vector<std::string> names;
    std::vector<Type> result_types;
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
    if (txn == nullptr && session.txn_id != INVALID_TXN_ID) {
        txn = txn_manager->get_transaction(session.txn_id);
    }
    if (txn != nullptr && txn->get_state() != TransactionState::ABORTED &&
        txn->get_state() != TransactionState::COMMITTED) {
        txn_manager->abort(txn, log_manager.get());
    }
    session.txn_id = INVALID_TXN_ID;
    if (context != nullptr) {
        context->txn_ = nullptr;
    }
}

std::string sql_literal(const Value& value) {
    if (!value.present) {
        throw wire_protocol::ProtocolError("SQL NULL parameters are not supported by this storage engine");
    }
    if (value.type == Type::INT32) {
        return std::to_string(value.int32);
    }
    if (value.type == Type::FLOAT32) {
        float number;
        std::memcpy(&number, &value.float_bits, sizeof(number));
        std::ostringstream output;
        output << std::setprecision(9) << number;
        return output.str();
    }
    std::string escaped = "'";
    for (char c : value.text) {
        escaped += c;
        if (c == '\'') {
            escaped += '\'';
        }
    }
    escaped += '\'';
    return escaped;
}

std::string substitute_parameters(const std::string& sql, const std::vector<Value>* values, std::size_t parameter_count,
                                  const std::vector<Type>* parameter_types = nullptr) {
    std::string output;
    output.reserve(sql.size() + parameter_count * 4);
    std::vector<bool> seen(parameter_count, false);
    for (std::size_t i = 0; i < sql.size();) {
        if (sql[i] == '\'') {
            output += sql[i++];
            while (i < sql.size()) {
                const char quoted = sql[i++];
                output += quoted;
                if (quoted != '\'') {
                    continue;
                }
                if (i < sql.size() && sql[i] == '\'') {
                    output += sql[i++];
                    continue;
                }
                break;
            }
            continue;
        }
        if (sql[i] != '$') {
            output += sql[i++];
            continue;
        }
        ++i;
        if (i == sql.size() || !std::isdigit(static_cast<unsigned char>(sql[i]))) {
            throw wire_protocol::ProtocolError("invalid parameter marker");
        }
        std::size_t number = 0;
        while (i < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i]))) {
            number = number * 10 + static_cast<std::size_t>(sql[i++] - '0');
            if (number > parameter_count) {
                throw wire_protocol::ProtocolError("parameter marker is out of range");
            }
        }
        if (number == 0 || number > parameter_count) {
            throw wire_protocol::ProtocolError("parameter marker is out of range");
        }
        seen[number - 1] = true;
        if (values != nullptr) {
            if (parameter_types != nullptr && values->at(number - 1).type != parameter_types->at(number - 1)) {
                throw wire_protocol::ProtocolError("typed parameter does not match prepared declaration");
            }
            output += sql_literal(values->at(number - 1));
        } else if (parameter_types != nullptr && parameter_types->at(number - 1) == Type::CHAR) {
            output += "''";
        } else if (parameter_types != nullptr && parameter_types->at(number - 1) == Type::FLOAT32) {
            output += "0.0";
        } else {
            output += "0";
        }
    }
    for (bool marker_seen : seen) {
        if (!marker_seen) {
            throw wire_protocol::ProtocolError("parameter markers must be dense");
        }
    }
    return output;
}

struct ExecutionOutcome {
    bool query = false;
    bool catalog_changed = false;
};

ExecutionOutcome execute_sql(const std::string& sql, SessionState& session, QueryResultSink* result_sink) {
    std::vector<char> response(BUFFER_LENGTH, 0);
    int offset = 0;
    Context context(lock_manager.get(), log_manager.get(), nullptr, response.data(), &offset, txn_manager.get());
    context.isolation_level_ = session.isolation;
    context.result_sink_ = result_sink;

    std::unique_ptr<ast::TreeNode> parse_tree = ast::parse_sql(sql);
    if (parse_tree == nullptr) {
        return {};
    }
    const auto parsed_type = parse_tree->type;
    const bool is_checkpoint = parsed_type == ast::AstType::StaticCheckpoint;
    const bool is_load = parsed_type == ast::AstType::LoadStmt;
    if (!is_checkpoint && !is_load) {
        SetTransaction(&session.txn_id, &context);
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
            session.txn_id = INVALID_TXN_ID;
        }
        context.txn_ = nullptr;
        if (catalog_changed) {
            catalog_generation.fetch_add(1, std::memory_order_acq_rel);
        }
        return {is_query, catalog_changed};
    } catch (...) {
        abort_session(session, &context);
        throw;
    }
}

PreparedStatement inspect_prepared(std::uint16_t id, bool query, std::vector<Type> parameters, std::string sql) {
    std::vector<char> response(BUFFER_LENGTH, 0);
    int offset = 0;
    Context context(lock_manager.get(), log_manager.get(), nullptr, response.data(), &offset, txn_manager.get());
    std::unique_ptr<ast::TreeNode> parse_tree = ast::parse_sql(sql);
    if (parse_tree == nullptr) {
        throw wire_protocol::ProtocolError("empty prepared SQL");
    }
    std::unique_ptr<Query> query_tree = analyze->do_analyze(std::move(parse_tree));
    std::unique_ptr<Plan> plan = optimizer->plan_query(std::move(query_tree), &context);
    std::unique_ptr<PortalStmt> statement = portal->start(std::move(plan), &context);
    const bool actual_query = statement->tag == PORTAL_ONE_SELECT;
    if (actual_query != query) {
        throw wire_protocol::ProtocolError("prepared result kind does not match SQL");
    }
    PreparedStatement result;
    result.id = id;
    result.query = query;
    result.sql = std::move(sql);
    result.parameters = std::move(parameters);
    result.catalog_generation = catalog_generation.load(std::memory_order_acquire);
    if (actual_query) {
        result.names = statement->output_names;
        for (const auto& column : statement->root->cols()) {
            result.result_types.push_back(column.type == TYPE_INT     ? Type::INT32
                                          : column.type == TYPE_FLOAT ? Type::FLOAT32
                                                                      : Type::CHAR);
        }
    }
    return result;
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
            const std::string inspect_sql =
                substitute_parameters(template_sql, nullptr, statement.parameters.size(), &statement.parameters);
            statement = inspect_prepared(statement.id, statement.query, std::move(statement.parameters), inspect_sql);
            statement.sql = template_sql;
            pending.push_back(std::move(statement));
        }
        reader.require_end();
        if (catalog_generation.load(std::memory_order_acquire) != pending.front().catalog_generation) {
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
        const PreparedStatement* statement;
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
    try {
        for (std::uint16_t i = 0; i < operation_count; ++i) {
            if (operations[i].statement->catalog_generation != catalog_generation.load(std::memory_order_acquire)) {
                throw wire_protocol::ProtocolError("prepared statement is invalid after catalog change");
            }
            ProtocolCollector result;
            result.encoded_bytes = &encoded_result_bytes;
            result.encoded_limit = result_budget;
            const auto sql =
                substitute_parameters(operations[i].statement->sql, &operations[i].values,
                                      operations[i].statement->parameters.size(), &operations[i].statement->parameters);
            const auto outcome = execute_sql(sql, session, &result);
            if (outcome.query) {
                results.emplace_back(i, std::move(result));
            }
            ++executed;
        }
    } catch (TransactionAbortException& exception) {
        failed = executed;
        abort_session(session, nullptr);
        Writer response;
        response.u16(executed);
        response.u8(1);
        response.u16(failed);
        const auto text = diagnostic(exception);
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
            sm_manager->create_db(db_name);
            LOG_INFO("database created: %s", db_name.c_str());
        }
        // Open database
        sm_manager->open_db(db_name);
        LOG_INFO("database opened: %s", db_name.c_str());

        log_manager->initialize_from_existing_log();
        buffer_pool_manager->set_log_manager(log_manager.get());

        // recovery database
        recovery->analyze();
        recovery->redo();
        recovery->undo();
        sm_manager->refresh_index_residency();
        LOG_INFO("database recovery finished");

        {
            std::atomic<bool> checkpoint_thread_stop{false};
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
                read_positive_size("RMDB_CHECKPOINT_PREFLUSH_PAGES", &checkpoint_options.preflush_batch_pages);
                if (has_checkpoint_override) {
                    checkpoint_mgr.SetOptions(checkpoint_options);
                }
                while (!checkpoint_thread_stop.load()) {
                    std::this_thread::sleep_for(std::chrono::seconds(2));
                    if (checkpoint_thread_stop.load()) {
                        break;
                    }
                    checkpoint_mgr.RunIfNeeded();
                }
            });

            // 开启服务端，开始接受客户端连接
            start_server(server_port);

            checkpoint_thread_stop.store(true);
            if (checkpoint_thread.joinable()) {
                checkpoint_thread.join();
            }
        }

        sm_manager->close_db();
        LOG_INFO("database has been closed");
    } catch (RMDBError& e) {
        LOG_ERROR("RMDB error: %s", e.what());
        minilog::Logger::get().stop();
        exit(1);
    }
    minilog::Logger::get().stop();
    return 0;
}
