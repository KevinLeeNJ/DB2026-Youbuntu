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

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/context.h"
#include "errors.h"
#include "optimizer/optimizer.h"
#include "optimizer/plan.h"
#include "parser/parser.h"
#include "protocol/wire_protocol.h"
#include "server/database_instance.h"

namespace wire_session_internal {
using wire_protocol::Reader;
using wire_protocol::Tag;
using wire_protocol::Type;
using wire_protocol::Value;
using wire_protocol::Writer;

Type protocol_type(ColType type);
bool changes_catalog(ast::AstType type);
bool descriptor_runtime_eligible(const PreparedPlanDescriptor* descriptor);

class BatchResultBuilder final : public QueryResultSink {
public:
    BatchResultBuilder();
    void begin_operation(std::uint16_t operation_index);
    void begin_query(const std::vector<ColMeta>& columns, const std::vector<std::string>& output_names) override;
    void append_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) override;
    void finish_operation(bool query);
    std::vector<std::uint8_t> success(std::uint16_t executed);
    std::vector<std::uint8_t> failure(std::uint16_t executed, std::uint8_t status, std::uint16_t failed,
                                      const std::string& diagnostic);

private:
    static constexpr std::size_t kExecutedOffset = 0;
    static constexpr std::size_t kQueryCountOffset = 9;
    void write_success_header();
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
    deltakernel::DeltaSession delta_session;

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
    // changes — which is what ending a transaction does, whether execute_tree/
    // abort_session ends it or end_explicit_transaction handles COMMIT/ROLLBACK/
    // ABORT. Nothing else can retire a transaction this session is still running.
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

struct PreparedStatement {
    std::uint16_t id = 0;
    bool query = false;
    std::unique_ptr<ast::TreeNode> template_tree;
    std::vector<Type> parameters;
    std::vector<std::string> names;
    std::vector<Type> result_types;
    std::unique_ptr<deltakernel::DeltaPreparedProgram> delta_program;
    std::unique_ptr<const PreparedPlanDescriptor> descriptor;
    std::string database_identity;
    std::uint64_t catalog_generation = 0;
};
struct ExecutionOutcome {
    bool query = false;
    bool catalog_changed = false;
};

void abort_session(DatabaseInstance& database, SessionState& session, Context* context);
ExecutionOutcome execute_plan(DatabaseInstance& database, std::unique_ptr<Plan> plan, SessionState& session,
                              Context& context, ExecutionOutput& output);
ExecutionOutcome execute_sql(DatabaseInstance& database, const std::string& sql, SessionState& session,
                             QueryResultSink* result_sink);
ParameterFrame make_parameter_frame(const std::vector<Value>& wire_values);
PreparedStatement inspect_prepared(DatabaseInstance& database, std::uint16_t id, bool query,
                                   std::vector<Type> parameters, std::unique_ptr<ast::TreeNode> template_tree,
                                   IsolationLevel isolation);
void revalidate_prepared(DatabaseInstance& database, PreparedStatement& statement, IsolationLevel isolation);
std::string diagnostic(const std::exception& exception);
bool is_valid_utf8(const std::string& text);
std::vector<std::uint8_t> make_error_payload(const std::string& text);
void handle_exec_stream(DatabaseInstance& database, int fd, const wire_protocol::Frame& frame, SessionState& session,
                        std::unordered_map<std::uint16_t, PreparedStatement>& prepared);
void handle_prepare_set(DatabaseInstance& database, int fd, Reader& reader, SessionState& session,
                        std::unordered_map<std::uint16_t, PreparedStatement>& prepared);
void handle_batch(DatabaseInstance& database, int fd, Reader& reader, SessionState& session,
                  std::unordered_map<std::uint16_t, PreparedStatement>& prepared);

} // namespace wire_session_internal
