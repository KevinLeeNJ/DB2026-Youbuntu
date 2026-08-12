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

#include "server/wire_session_internal.h"

namespace wire_session_internal {
struct BatchExecutionContext {
    BatchExecutionContext(DatabaseInstance& database, SessionState& session)
        : context(&database.lock_manager, &database.log_manager, nullptr, &database.txn_manager),
          catalog_guard(database.sm_manager.acquire_catalog_shared()) {
        reset_for_operation(session, nullptr);
    }

    void reset_for_operation(SessionState& session, QueryResultSink* result_sink) {
        if (!legacy_response.empty() && legacy_offset > 0) {
            const std::size_t used =
                std::min(static_cast<std::size_t>(legacy_offset), static_cast<std::size_t>(legacy_response.size()));
            std::fill_n(legacy_response.data(), used, '\0');
        }
        legacy_offset = 0;
        context.txn_ = nullptr;
        context.isolation_level_ = session.isolation;
        context.enable_ssi_read_tracking_ = false;
        output.result_sink = result_sink;
        output.output_file_enabled = &session.output_file_enabled;
    }

    void set_output_mode(bool typed_fast_path) {
        if (!typed_fast_path && legacy_response.empty()) {
            legacy_response.assign(BUFFER_LENGTH, 0);
        }
        output.data_send = typed_fast_path ? nullptr : legacy_response.data();
        output.offset = typed_fast_path ? nullptr : &legacy_offset;
        output.ellipsis = false;
    }

    std::vector<char> legacy_response;
    int legacy_offset{0};
    Context context;
    ExecutionOutput output;
    SmManager::CatalogSharedGuard catalog_guard;
    BatchResultBuilder result;
};

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(DatabaseInstance& database, SessionState& session, Context* context) {
    Transaction* txn = session.running_transaction();
    if (txn == nullptr) {
        txn = database.txn_manager.get_transaction(session.txn_id);
        if (txn == nullptr || txn->get_state() == TransactionState::COMMITTED ||
            txn->get_state() == TransactionState::ABORTED) {
            txn = database.txn_manager.begin(nullptr, context->log_mgr_, context->isolation_level_);
            session.txn_id = txn->get_transaction_id();
            txn->set_txn_mode(false);
            txn->set_isolation_level(context->isolation_level_);
        }
        session.remember_running_transaction(txn);
    }
    context->txn_ = txn;
    database.txn_manager.BeginStatement(txn);
}

template <typename Execute>
ExecutionOutcome execute_operation(DatabaseInstance& database, SessionState& session, Context& context,
                                   ExecutionOutput& output, bool starts_transaction, Execute&& execute) {
    context.isolation_level_ = session.isolation;
    output.output_file_enabled = &session.output_file_enabled;
    try {
        if (starts_transaction) {
            SetTransaction(database, session, &context);
        }
        const ExecutionOutcome outcome = execute();
        session.isolation = context.isolation_level_;
        if (context.txn_ != nullptr && !context.txn_->get_txn_mode() &&
            context.txn_->get_state() != TransactionState::COMMITTED &&
            context.txn_->get_state() != TransactionState::ABORTED) {
            database.txn_manager.commit(context.txn_, context.log_mgr_);
            // commit may already have freed the transaction object.
            session.forget_running_transaction();
            session.txn_id = INVALID_TXN_ID;
        }
        if (context.txn_ == nullptr && session.txn_id == INVALID_TXN_ID) {
            session.forget_running_transaction();
        }
        context.txn_ = nullptr;
        return outcome;
    } catch (TransactionAbortException&) {
        abort_session(database, session, &context);
        throw;
    } catch (...) {
        abort_session(database, session, &context);
        throw;
    }
}

void abort_session(DatabaseInstance& database, SessionState& session, Context* context) {
    Transaction* txn = context == nullptr ? nullptr : context->txn_;
    if (txn == nullptr) {
        txn = session.running_transaction();
    }
    if (txn == nullptr && session.txn_id != INVALID_TXN_ID) {
        txn = database.txn_manager.get_transaction(session.txn_id);
    }
    // Clear every owner before physical undo: abort may throw after changing
    // transaction state, and a later protocol cleanup must not retry it.
    session.forget_running_transaction();
    session.txn_id = INVALID_TXN_ID;
    if (context != nullptr) {
        context->txn_ = nullptr;
    }
    if (txn != nullptr && txn->get_state() != TransactionState::ABORTED &&
        txn->get_state() != TransactionState::COMMITTED) {
        database.txn_manager.abort(txn, &database.log_manager);
    }
}

bool end_explicit_transaction(DatabaseInstance& database, PlanTag tag, SessionState& session, Context& context) {
    if (tag != T_Transaction_commit && tag != T_Transaction_rollback && tag != T_Transaction_abort) {
        return false;
    }
    Transaction* txn = context.txn_;
    if (tag == T_Transaction_commit) {
        if (txn != nullptr) {
            database.txn_manager.commit(txn, context.log_mgr_);
        }
        session.forget_running_transaction();
        session.txn_id = INVALID_TXN_ID;
        context.txn_ = nullptr;
        return true;
    }
    session.forget_running_transaction();
    session.txn_id = INVALID_TXN_ID;
    context.txn_ = nullptr;
    if (txn != nullptr) {
        database.txn_manager.abort(txn, context.log_mgr_);
    }
    return true;
}

ExecutionOutcome execute_plan(DatabaseInstance& database, std::unique_ptr<Plan> plan, SessionState& session,
                              Context& context, ExecutionOutput& output) {
    const bool catalog_changed = plan->tag == T_CreateTable || plan->tag == T_DropTable || plan->tag == T_CreateIndex ||
                                 plan->tag == T_DropIndex || plan->tag == T_LoadData;
    if (end_explicit_transaction(database, plan->tag, session, context)) {
        return {false, catalog_changed};
    }
    return {database.ql_manager.execute(std::move(plan), &session.txn_id, &context, &output), catalog_changed};
}

ExecutionOutcome execute_tree_with_catalog_guard(DatabaseInstance& database, std::unique_ptr<ast::TreeNode> parse_tree,
                                                 SessionState& session, ExecutionOutput& output, Context& context) {
    if (parse_tree == nullptr) {
        return {};
    }
    const auto parsed_type = parse_tree->type;
    const bool starts_transaction =
        parsed_type != ast::AstType::StaticCheckpoint && parsed_type != ast::AstType::LoadStmt;
    return execute_operation(database, session, context, output, starts_transaction, [&] {
        std::unique_ptr<Query> query = database.analyze.do_analyze(std::move(parse_tree));
        std::unique_ptr<Plan> plan = database.optimizer.plan_query(std::move(query), &context);
        return execute_plan(database, std::move(plan), session, context, output);
    });
}

ExecutionOutcome execute_tree(DatabaseInstance& database, std::unique_ptr<ast::TreeNode> parse_tree,
                              SessionState& session, QueryResultSink* result_sink) {
    std::vector<char> response(BUFFER_LENGTH, 0);
    int offset = 0;
    Context context(&database.lock_manager, &database.log_manager, nullptr, &database.txn_manager);
    ExecutionOutput output{response.data(), &offset, false, result_sink, &session.output_file_enabled};
    if (parse_tree == nullptr) {
        return {};
    }
    const bool catalog_change = changes_catalog(parse_tree->type);
    if (catalog_change) {
        Transaction* active = session.running_transaction();
        if (active != nullptr && active->get_txn_mode() && active->get_state() != TransactionState::COMMITTED &&
            active->get_state() != TransactionState::ABORTED) {
            throw RMDBError("structural DDL and LOAD are not allowed inside an explicit transaction");
        }
        auto guard = database.sm_manager.acquire_catalog_exclusive();
        return execute_tree_with_catalog_guard(database, std::move(parse_tree), session, output, context);
    }
    auto guard = database.sm_manager.acquire_catalog_shared();
    return execute_tree_with_catalog_guard(database, std::move(parse_tree), session, output, context);
}

ExecutionOutcome execute_sql(DatabaseInstance& database, const std::string& sql, SessionState& session,
                             QueryResultSink* result_sink) {
    auto parse_tree = ast::parse_sql(sql);
    return execute_tree(database, std::move(parse_tree), session, result_sink);
}

void handle_batch(DatabaseInstance& database, int fd, Reader& reader, SessionState& session,
                  std::unordered_map<std::uint16_t, PreparedStatement>& prepared) {
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

    std::uint16_t failed = 0xffff;
    std::uint16_t executed = 0;
    BatchExecutionContext batch(database, session);
    try {
        for (std::uint16_t i = 0; i < operation_count; ++i) {
            auto* prepared_statement = operations[i].statement;
            batch.reset_for_operation(session, &batch.result);
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
            execute_operation(database, session, batch.context, batch.output, true, [&] {
                if (prepared_statement->catalog_generation != database.sm_manager.get_catalog_generation() ||
                    prepared_statement->database_identity !=
                        database.sm_manager.get_database_identity_under_catalog_guard()) {
                    revalidate_prepared(database, *prepared_statement, session.isolation);
                }
                const bool prepared_fast_path = descriptor_runtime_eligible(prepared_statement->descriptor.get());
                batch.set_output_mode(prepared_fast_path);
                batch.result.begin_operation(i);
                bool is_query;
                if (prepared_fast_path) {
                    is_query = database.ql_manager.execute_prepared(*prepared_statement->descriptor,
                                                                    make_parameter_frame(operations[i].values),
                                                                    &batch.context, &batch.output);
                } else {
                    auto query = database.analyze.do_analyze(
                        ast::clone_bound_tree(*prepared_statement->template_tree, make_bindings()));
                    auto plan = database.optimizer.plan_query(std::move(query), &batch.context);
                    is_query = execute_plan(database, std::move(plan), session, batch.context, batch.output).query;
                }
                batch.result.finish_operation(is_query);
                return ExecutionOutcome{is_query, false};
            });
            ++executed;
        }
    } catch (TransactionAbortException& exception) {
        failed = executed;
        abort_session(database, session, &batch.context);
        // TransactionAbortException does not override what(); use the same
        // diagnostic text the EXEC_STREAM path reports.
        const auto text = exception.GetInfo();
        wire_protocol::write_frame(fd, Tag::BATCH_RESULT, batch.result.failure(executed, 1, failed, text));
        return;
    } catch (const std::exception& exception) {
        failed = executed;
        abort_session(database, session, &batch.context);
        const auto text = diagnostic(exception);
        wire_protocol::write_frame(fd, Tag::BATCH_RESULT, batch.result.failure(executed, 2, failed, text));
        return;
    }

    wire_protocol::write_frame(fd, Tag::BATCH_RESULT, batch.result.success(executed));
}

} // namespace wire_session_internal
