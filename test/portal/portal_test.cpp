/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#undef NDEBUG

#define private public
#include "portal.h"
#undef private

#include <memory>
#include <cmath>
#include <chrono>
#include <future>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "execution/prepared_select_execution_frame.h"
#include "gtest/gtest.h"
#include "index/ix.h"
#include "record/rm.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "transaction/transaction_manager.h"

namespace {

const std::string TEST_DB_NAME = "portal_test_db";

QueryExpr make_col_expr(const std::string& col_name) {
    QueryExpr expr;
    expr.type = QueryExprType::COLUMN;
    expr.col = {.tab_name = "grade", .col_name = col_name};
    expr.display_name = col_name;
    return expr;
}

QueryExpr make_agg_expr(AggType type, const std::string& col_name, const std::string& display_name) {
    QueryExpr expr;
    expr.type = QueryExprType::AGGREGATE;
    expr.agg.type = type;
    expr.agg.is_star = false;
    expr.agg.col = {.tab_name = "grade", .col_name = col_name};
    expr.agg.display_name = display_name;
    expr.display_name = display_name;
    return expr;
}

} // namespace

TEST(ParameterFrameTest, float_to_int_rejects_unrepresentable_boundaries_before_cast) {
    Value value;
    value.set_float(static_cast<float>(std::numeric_limits<int>::min()));
    EXPECT_EQ(ParameterFrame({value}).bind(1, TYPE_INT).int_val, std::numeric_limits<int>::min());

    value.set_float(std::nextafter(2147483648.0F, 0.0F));
    EXPECT_EQ(ParameterFrame({value}).bind(1, TYPE_INT).int_val, 2147483520);

    value.set_float(2147483648.0F);
    EXPECT_THROW((void)ParameterFrame({value}).bind(1, TYPE_INT), RMDBError);

    value.set_float(
        std::nextafter(static_cast<float>(std::numeric_limits<int>::min()), -std::numeric_limits<float>::infinity()));
    EXPECT_THROW((void)ParameterFrame({value}).bind(1, TYPE_INT), RMDBError);
}

class PortalAggregateTest : public ::testing::Test {
protected:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    std::unique_ptr<Portal> portal_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<TransactionManager> transaction_manager_;
    bool db_opened_ = false;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_manager_.get(),
                                                  ix_manager_.get());
        portal_ = std::make_unique<Portal>(sm_manager_.get());
        if (sm_manager_->is_dir(TEST_DB_NAME)) {
            sm_manager_->drop_db(TEST_DB_NAME);
        }
        sm_manager_->create_db(TEST_DB_NAME);
        sm_manager_->open_db(TEST_DB_NAME);
        lock_manager_ = std::make_unique<LockManager>();
        log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
        buffer_pool_manager_->set_log_manager(log_manager_.get());
        transaction_manager_ = std::make_unique<TransactionManager>(lock_manager_.get(), sm_manager_.get());
        db_opened_ = true;
        sm_manager_->create_table("grade", {{"id", TYPE_INT, 4}, {"score", TYPE_INT, 4}}, nullptr);
    }

    void TearDown() override {
        transaction_manager_.reset();
        if (db_opened_) {
            sm_manager_->close_db();
            db_opened_ = false;
        }
        buffer_pool_manager_->set_log_manager(nullptr);
        log_manager_.reset();
        lock_manager_.reset();
        if (sm_manager_->is_dir(TEST_DB_NAME)) {
            sm_manager_->drop_db(TEST_DB_NAME);
        }
    }

    template <typename Body> void run_logged_write(Body&& body) {
        Transaction* transaction =
            transaction_manager_->begin(nullptr, log_manager_.get(), IsolationLevel::READ_COMMITTED);
        char buffer[256]{};
        int offset = 0;
        Context context(lock_manager_.get(), log_manager_.get(), transaction, buffer, &offset,
                        transaction_manager_.get());
        try {
            body(&context);
            transaction_manager_->commit(transaction, log_manager_.get());
        } catch (...) {
            transaction_manager_->abort(transaction, log_manager_.get());
            throw;
        }
    }

    std::unique_ptr<Plan> make_select_subplan(bool with_limit) {
        auto scan = std::make_unique<ScanPlan>(T_SeqScan, sm_manager_.get(), "grade", std::vector<Condition>{},
                                               std::vector<std::string>{});

        std::vector<AggExpr> agg_exprs = {
            {.type = AggType::MAX,
             .is_star = false,
             .col = {.tab_name = "grade", .col_name = "score"},
             .display_name = "MAX(score)"},
        };
        std::vector<TabCol> group_by_cols = {
            {.tab_name = "grade", .col_name = "id"},
        };
        std::vector<HavingCondition> having_conds = {
            {.lhs = make_agg_expr(AggType::MAX, "score", "MAX(score)"),
             .op = OP_GT,
             .is_rhs_val = true,
             .rhs_expr = {},
             .rhs_val = {}},
        };
        having_conds[0].rhs_val.set_int(90);

        auto aggregate = std::make_unique<AggregatePlan>(T_Aggregate, std::move(scan), std::move(group_by_cols),
                                                         agg_exprs, having_conds);

        SelectItem group_item;
        group_item.expr = make_col_expr("id");
        group_item.output_name = "id";

        SelectItem agg_item;
        agg_item.expr = make_agg_expr(AggType::MAX, "score", "MAX(score)");
        agg_item.alias = "max_score";
        agg_item.output_name = "max_score";

        auto projection = std::make_unique<ProjectionPlan>(T_Projection, std::move(aggregate),
                                                           std::vector<SelectItem>{group_item, agg_item},
                                                           std::vector<std::string>{"id", "max_score"});

        OrderByItem order_by;
        order_by.expr = make_agg_expr(AggType::MAX, "score", "MAX(score)");
        order_by.is_desc = true;
        auto sort = std::make_unique<SortPlan>(T_Sort, std::move(projection), std::vector<OrderByItem>{order_by});

        if (!with_limit) {
            return sort;
        }
        return std::make_unique<LimitPlan>(T_Limit, std::move(sort), 3);
    }
};

TEST_F(PortalAggregateTest, get_plan_output_names_handles_aggregate_and_projection_aliases) {
    auto plan = make_select_subplan(false);
    auto* sort = static_cast<SortPlan*>(plan.get());

    auto projection_output_names = portal_->get_plan_output_names(sort->subplan_.get());
    auto aggregate_output_names = portal_->build_aggregate_output_names(
        *static_cast<AggregatePlan*>(static_cast<ProjectionPlan*>(sort->subplan_.get())->subplan_.get()));

    EXPECT_EQ(projection_output_names, (std::vector<std::string>{"id", "max_score"}));
    EXPECT_EQ(aggregate_output_names, (std::vector<std::string>{"id", "MAX(score)"}));
}

TEST_F(PortalAggregateTest, start_builds_limit_sort_projection_aggregate_executor_chain) {
    char buffer[256];
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, buffer, &offset);

    auto subplan = make_select_subplan(true);
    auto full_plan = std::make_unique<DMLPlan>(T_select, std::move(subplan), std::string(), std::vector<Value>{},
                                               std::vector<Condition>{}, std::vector<SetClause>{});

    auto stmt = portal_->start(std::move(full_plan), &context);

    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->tag, PORTAL_ONE_SELECT);
    EXPECT_EQ(stmt->output_names, (std::vector<std::string>{"id", "max_score"}));
    ASSERT_NE(stmt->root, nullptr);
    EXPECT_EQ(stmt->root->getType(), "LimitExecutor");

    auto* limit = dynamic_cast<LimitExecutor*>(stmt->root.get());
    ASSERT_NE(limit, nullptr);
    ASSERT_NE(limit->prev_, nullptr);
    EXPECT_EQ(limit->prev_->getType(), "SortExecutor");

    auto* sort = dynamic_cast<SortExecutor*>(limit->prev_.get());
    ASSERT_NE(sort, nullptr);
    ASSERT_NE(sort->prev_, nullptr);
    EXPECT_EQ(sort->prev_->getType(), "ProjectionExecutor");

    auto* projection = dynamic_cast<ProjectionExecutor*>(sort->prev_.get());
    ASSERT_NE(projection, nullptr);
    ASSERT_NE(projection->prev_, nullptr);
    EXPECT_EQ(projection->prev_->getType(), "AggregateExecutor");

    auto* aggregate = dynamic_cast<AggregateExecutor*>(projection->prev_.get());
    ASSERT_NE(aggregate, nullptr);
    ASSERT_NE(aggregate->prev_, nullptr);
    EXPECT_EQ(aggregate->prev_->getType(), "SeqScanExecutor");
}

TEST_F(PortalAggregateTest, prepared_select_binds_request_local_conditions_and_limit_offset) {
    Condition lower;
    lower.lhs_col = {.tab_name = "grade", .col_name = "id"};
    lower.op = OP_GE;
    lower.is_rhs_val = true;
    lower.rhs_val.set_int(0);
    lower.rhs_val.parameter_ordinal = 1;
    Condition upper = lower;
    upper.op = OP_LE;

    auto scan = std::make_unique<ScanPlan>(T_SeqScan, sm_manager_.get(), "grade", std::vector<Condition>{lower, upper},
                                           std::vector<std::string>{});
    SelectItem item;
    item.expr = make_col_expr("id");
    item.output_name = "id";
    auto projection = std::make_unique<ProjectionPlan>(T_Projection, std::move(scan), std::vector<SelectItem>{item},
                                                       std::vector<std::string>{"id"});
    auto limit = std::make_unique<LimitPlan>(T_Limit, std::move(projection), 0, 0, 2, 3);
    auto plan = std::make_unique<DMLPlan>(T_select, std::move(limit), std::string(), std::vector<Value>{},
                                          std::vector<Condition>{}, std::vector<SetClause>{});
    const auto result_schema = sm_manager_->db_.get_table("grade").cols;
    auto descriptor =
        PreparedPlanDescriptor::Build(std::move(plan), PreparedStatementKind::Select, std::vector<std::string>{"id"},
                                      result_schema, TEST_DB_NAME, sm_manager_->get_catalog_generation());
    ASSERT_TRUE(descriptor->eligible());

    Value id;
    id.set_int(7);
    Value runtime_limit;
    runtime_limit.set_int(5);
    Value runtime_offset;
    runtime_offset.set_int(2);
    ParameterFrame parameters({id, runtime_limit, runtime_offset});
    char buffer[256];
    int offset = 0;
    Context context(nullptr, nullptr, nullptr, buffer, &offset);
    auto statement = portal_->start_prepared(*descriptor, parameters, &context);

    ASSERT_NE(statement, nullptr);
    auto* runtime_limit_executor = dynamic_cast<LimitExecutor*>(statement->root.get());
    ASSERT_NE(runtime_limit_executor, nullptr);
    EXPECT_EQ(runtime_limit_executor->limit_, 5u);
    EXPECT_EQ(runtime_limit_executor->offset_, 2u);
    auto* runtime_projection = dynamic_cast<ProjectionExecutor*>(runtime_limit_executor->prev_.get());
    ASSERT_NE(runtime_projection, nullptr);
    auto* runtime_scan = dynamic_cast<SeqScanExecutor*>(runtime_projection->prev_.get());
    ASSERT_NE(runtime_scan, nullptr);
    ASSERT_EQ(runtime_scan->conds_.size(), 2u);
    EXPECT_EQ(runtime_scan->conds_[0].rhs_val.int_val, 7);
    EXPECT_EQ(runtime_scan->conds_[1].rhs_val.int_val, 7);
    EXPECT_EQ(runtime_scan->conds_[0].rhs_val.parameter_ordinal, 0u);
    EXPECT_NE(runtime_scan->conds_[0].rhs_val.raw, nullptr);

    const auto* immutable_select = static_cast<const DMLPlan*>(descriptor->plan());
    const auto* immutable_limit = static_cast<const LimitPlan*>(immutable_select->subplan_.get());
    const auto* immutable_projection = static_cast<const ProjectionPlan*>(immutable_limit->subplan_.get());
    const auto* immutable_scan = static_cast<const ScanPlan*>(immutable_projection->subplan_.get());
    EXPECT_EQ(immutable_scan->conds_[0].rhs_val.parameter_ordinal, 1u);
    EXPECT_EQ(immutable_scan->conds_[1].rhs_val.parameter_ordinal, 1u);
    EXPECT_EQ(immutable_scan->conds_[0].rhs_val.raw, nullptr);

    QlManager ql_manager(sm_manager_.get(), nullptr, nullptr);
    txn_id_t txn_id = INVALID_TXN_ID;
    EXPECT_NO_THROW(portal_->run(std::move(statement), &ql_manager, &txn_id, &context));
    EXPECT_TRUE(descriptor->eligible());
}

TEST_F(PortalAggregateTest, prepared_insert_uses_bound_metadata_without_visiting_plan_or_catalog) {
    sm_manager_->create_table("prepared_insert_rows",
                              {{"id", TYPE_INT, 4},
                               {"constant_text", TYPE_STRING, 8},
                               {"runtime_text", TYPE_STRING, 8},
                               {"nullable_text", TYPE_STRING, 8}},
                              nullptr);
    sm_manager_->create_index("prepared_insert_rows", {"id"}, nullptr);

    Value id_slot;
    id_slot.type = TYPE_INT;
    id_slot.parameter_ordinal = 1;
    Value constant_text;
    constant_text.set_str("fixed");
    Value runtime_text_slot;
    runtime_text_slot.type = TYPE_STRING;
    runtime_text_slot.parameter_ordinal = 2;
    Value nullable_text_slot;
    nullable_text_slot.type = TYPE_STRING;
    nullable_text_slot.parameter_ordinal = 3;
    auto plan =
        std::make_unique<DMLPlan>(T_Insert, nullptr, "prepared_insert_rows",
                                  std::vector<Value>{id_slot, constant_text, runtime_text_slot, nullable_text_slot},
                                  std::vector<Condition>{}, std::vector<SetClause>{});
    plan->sm_manager_ = sm_manager_.get();
    auto descriptor = PreparedPlanDescriptor::Build(std::move(plan), PreparedStatementKind::Insert, {}, {},
                                                    TEST_DB_NAME, sm_manager_->get_catalog_generation());

    ASSERT_TRUE(descriptor->eligible());
    const auto* executable = descriptor->insert_executable();
    ASSERT_NE(executable, nullptr);
    EXPECT_EQ(executable->table, &sm_manager_->db_.get_table("prepared_insert_rows"));
    EXPECT_EQ(executable->table_handle, sm_manager_->fhs_.at("prepared_insert_rows").get());
    ASSERT_EQ(executable->indexes.size(), 1u);
    ASSERT_EQ(executable->values.size(), 4u);
    EXPECT_EQ(executable->values[0].parameter_ordinal, 1u);
    EXPECT_EQ(executable->values[1].parameter_ordinal, 0u);
    EXPECT_EQ(executable->values[2].parameter_ordinal, 2u);
    EXPECT_EQ(executable->values[3].parameter_ordinal, 3u);

    Value id;
    id.set_int(7);
    Value runtime_text;
    runtime_text.set_str("runtime");
    Value nullable_text;
    nullable_text.type = TYPE_STRING;
    nullable_text.set_null();
    ParameterFrame parameters({id, runtime_text, nullable_text});
    char buffer[256];
    int offset = 0;
    Transaction* transaction = transaction_manager_->begin(nullptr, log_manager_.get(), IsolationLevel::READ_COMMITTED);
    Context context(lock_manager_.get(), log_manager_.get(), transaction, buffer, &offset, transaction_manager_.get());
    auto statement = portal_->start_prepared(*descriptor, parameters, &context);

    ASSERT_NE(statement, nullptr);
    EXPECT_EQ(statement->tag, PORTAL_DML_WITHOUT_SELECT);
    auto* insert = dynamic_cast<InsertExecutor*>(statement->root.get());
    ASSERT_NE(insert, nullptr);
    EXPECT_EQ(insert->tab_, executable->table);
    EXPECT_EQ(insert->fh_, executable->table_handle);
    EXPECT_EQ(insert->indexes_, &executable->indexes);
    ASSERT_EQ(insert->values_.size(), 4u);
    EXPECT_EQ(insert->values_[0].int_val, 7);
    EXPECT_EQ(insert->values_[1].str_val, "fixed");
    EXPECT_EQ(insert->values_[2].str_val, "runtime");
    EXPECT_TRUE(insert->values_[3].is_null);

    QlManager ql_manager(sm_manager_.get(), transaction_manager_.get(), nullptr);
    txn_id_t txn_id = transaction->get_transaction_id();
    EXPECT_NO_THROW(portal_->run(std::move(statement), &ql_manager, &txn_id, &context));
    transaction_manager_->commit(transaction, log_manager_.get());

    int index_key = 7;
    const auto lookup = executable->indexes[0].handle->lookup_unique(reinterpret_cast<const char*>(&index_key));
    ASSERT_EQ(lookup.status, UniqueLookupStatus::Unique);
    auto record = executable->table_handle->get_record(lookup.rid, &context);
    ASSERT_NE(record, nullptr);
    const auto& columns = executable->table->cols;
    ASSERT_EQ(columns.size(), 4u);
    EXPECT_EQ(read_unaligned<int>(record->data + columns[0].offset), 7);
    EXPECT_EQ(std::string(record->data + columns[1].offset,
                          strnlen(record->data + columns[1].offset, static_cast<std::size_t>(columns[1].len))),
              "fixed");
    EXPECT_EQ(std::string(record->data + columns[2].offset,
                          strnlen(record->data + columns[2].offset, static_cast<std::size_t>(columns[2].len))),
              "runtime");
    EXPECT_TRUE(is_null(record->data, columns[3]));
}

TEST_F(PortalAggregateTest, prepared_select_bound_path_is_cold_hot_deterministic_for_mixed_values_and_null) {
    sm_manager_->create_table("prepared_select_rows",
                              {{"id", TYPE_INT, 4}, {"code", TYPE_STRING, 8}, {"nullable_text", TYPE_STRING, 8}},
                              nullptr);
    sm_manager_->create_index("prepared_select_rows", {"id"}, nullptr);
    auto insert_row = [&](int id_value, const std::string& code_value, const std::optional<std::string>& nullable) {
        Value id;
        id.set_int(id_value);
        Value code;
        code.set_str(code_value);
        Value nullable_text;
        nullable_text.type = TYPE_STRING;
        if (nullable.has_value()) {
            nullable_text.set_str(*nullable);
        } else {
            nullable_text.set_null();
        }
        run_logged_write([&](Context* context) {
            InsertExecutor insert(sm_manager_.get(), "prepared_select_rows", {id, code, nullable_text}, context);
            insert.Next();
        });
    };
    insert_row(1, "aa", "x");
    insert_row(2, "aa", "x");
    insert_row(3, "aa", std::nullopt);

    Condition lower;
    lower.lhs_col = {.tab_name = "prepared_select_rows", .col_name = "id"};
    lower.op = OP_GE;
    lower.is_rhs_val = true;
    lower.rhs_val.type = TYPE_INT;
    lower.rhs_val.parameter_ordinal = 1;
    Condition upper = lower;
    upper.op = OP_LE;
    upper.rhs_val.set_int(2);
    upper.rhs_val.parameter_ordinal = 0;
    upper.rhs_val.init_raw(sizeof(int));
    Condition code_equals;
    code_equals.lhs_col = {.tab_name = "prepared_select_rows", .col_name = "code"};
    code_equals.op = OP_EQ;
    code_equals.is_rhs_val = true;
    code_equals.rhs_val.type = TYPE_STRING;
    code_equals.rhs_val.parameter_ordinal = 2;
    Condition nullable_equals = code_equals;
    nullable_equals.lhs_col.col_name = "nullable_text";
    nullable_equals.rhs_val.parameter_ordinal = 3;

    auto scan = std::make_unique<ScanPlan>(T_IndexScan, sm_manager_.get(), "prepared_select_rows",
                                           std::vector<Condition>{lower, upper, code_equals, nullable_equals},
                                           std::vector<std::string>{"id"});
    SelectItem id_item;
    id_item.expr.type = QueryExprType::COLUMN;
    id_item.expr.col = {.tab_name = "prepared_select_rows", .col_name = "id"};
    id_item.output_name = "id";
    SelectItem code_item;
    code_item.expr.type = QueryExprType::COLUMN;
    code_item.expr.col = {.tab_name = "prepared_select_rows", .col_name = "code"};
    code_item.output_name = "code";
    auto projection =
        std::make_unique<ProjectionPlan>(T_Projection, std::move(scan), std::vector<SelectItem>{id_item, code_item},
                                         std::vector<std::string>{"id", "code"});
    auto plan = std::make_unique<DMLPlan>(T_select, std::move(projection), std::string(), std::vector<Value>{},
                                          std::vector<Condition>{}, std::vector<SetClause>{});
    auto descriptor = PreparedPlanDescriptor::Build(std::move(plan), PreparedStatementKind::Select, {"id", "code"},
                                                    sm_manager_->db_.get_table("prepared_select_rows").cols,
                                                    TEST_DB_NAME, sm_manager_->get_catalog_generation());
    ASSERT_TRUE(descriptor->eligible());
    ASSERT_NE(descriptor->select_executable(), nullptr);
    ASSERT_TRUE(descriptor->select_executable()->scan.uses_index);
    auto execution_frame = PreparedSelectExecutionFrame::Build(*descriptor->select_executable());
    ASSERT_NE(execution_frame, nullptr);

    auto execute = [&](int lower_bound, const std::string& code_value,
                       std::optional<std::string> nullable) -> std::vector<int> {
        Value lower_value;
        lower_value.set_int(lower_bound);
        Value code;
        code.set_str(code_value);
        Value nullable_value;
        nullable_value.type = TYPE_STRING;
        if (nullable.has_value()) {
            nullable_value.set_str(*nullable);
        } else {
            nullable_value.set_null();
        }
        ParameterFrame parameters({lower_value, code, nullable_value});
        std::vector<int> ids;
        run_logged_write([&](Context* context) {
            auto lease = execution_frame->begin_operation(parameters, context);
            auto& root = lease.root();
            for (root.beginTuple(); !root.is_end(); root.nextTuple()) {
                auto record = root.Next();
                EXPECT_NE(record, nullptr);
                if (record == nullptr) {
                    break;
                }
                ids.push_back(read_unaligned<int>(record->data + root.cols()[0].offset));
            }
            lease.close();
        });
        return ids;
    };

    EXPECT_EQ(execute(1, "aa", "x"), (std::vector<int>{1, 2}));
    EXPECT_EQ(execute(2, "aa", "x"), (std::vector<int>{2}));
    EXPECT_TRUE(execute(1, "aa", std::nullopt).empty());
    EXPECT_THROW((void)execute(1, "string-too-long", "x"), StringOverflowError);
    EXPECT_EQ(execute(1, "aa", "x"), (std::vector<int>{1, 2}));

    class ThrowingQueryResultSink final : public QueryResultSink {
    public:
        void begin_query(const std::vector<ColMeta>&, const std::vector<std::string>&) override {}

        void append_row(const std::vector<ColMeta>&, const char*, std::size_t) override {
            throw ExecutionException("injected prepared SELECT result sink failure");
        }
    };

    Value error_lower;
    error_lower.set_int(1);
    Value error_code;
    error_code.set_str("aa");
    Value error_nullable;
    error_nullable.set_str("x");
    ParameterFrame error_parameters({error_lower, error_code, error_nullable});
    QlManager ql_manager(sm_manager_.get(), transaction_manager_.get(), nullptr);
    const std::vector<std::string> output_names{"id", "code"};

    EXPECT_THROW(run_logged_write([&](Context* context) {
                     ThrowingQueryResultSink sink;
                     context->result_sink_ = &sink;
                     auto lease = execution_frame->begin_operation(error_parameters, context);
                     ql_manager.select_from(lease.root(), output_names, context);
                 }),
                 ExecutionException);
    EXPECT_EQ(execute(1, "aa", "x"), (std::vector<int>{1, 2}));
}

TEST_F(PortalAggregateTest, prepared_update_bound_path_preserves_index_heap_mvcc_and_abort_rollback) {
    sm_manager_->create_table("prepared_update_rows",
                              {{"id", TYPE_INT, 4},
                               {"payload", TYPE_INT, 4},
                               {"amount", TYPE_FLOAT, 4},
                               {"code", TYPE_STRING, 8},
                               {"nullable_text", TYPE_STRING, 8}},
                              nullptr);
    sm_manager_->create_index("prepared_update_rows", {"id"}, nullptr);
    sm_manager_->create_index("prepared_update_rows", {"code"}, nullptr);

    Value original_id;
    original_id.set_int(1);
    Value original_payload;
    original_payload.set_int(10);
    Value original_amount;
    original_amount.set_float(1.25F);
    Value original_code;
    original_code.set_str("old");
    Value original_nullable;
    original_nullable.set_str("kept");
    run_logged_write([&](Context* context) {
        InsertExecutor insert(sm_manager_.get(), "prepared_update_rows",
                              {original_id, original_payload, original_amount, original_code, original_nullable},
                              context);
        insert.Next();
    });

    Condition match;
    match.lhs_col = {.tab_name = "prepared_update_rows", .col_name = "id"};
    match.op = OP_EQ;
    match.is_rhs_val = true;
    match.rhs_val.type = TYPE_INT;
    match.rhs_val.parameter_ordinal = 1;
    Condition residual;
    residual.lhs_col = {.tab_name = "prepared_update_rows", .col_name = "payload"};
    residual.op = OP_EQ;
    residual.is_rhs_val = true;
    residual.rhs_val.type = TYPE_INT;
    residual.rhs_val.parameter_ordinal = 2;
    auto scan = std::make_unique<ScanPlan>(T_IndexScan, sm_manager_.get(), "prepared_update_rows",
                                           std::vector<Condition>{match, residual}, std::vector<std::string>{"id"});
    SetClause set_id;
    set_id.lhs = {.tab_name = "prepared_update_rows", .col_name = "id"};
    set_id.rhs.type = TYPE_INT;
    set_id.rhs.parameter_ordinal = 5;
    SetClause set_payload;
    set_payload.lhs = {.tab_name = "prepared_update_rows", .col_name = "payload"};
    set_payload.is_self_ref = true;
    set_payload.rhs_col = set_payload.lhs;
    set_payload.op = UpdateOp::SELF_ADD;
    set_payload.rhs.type = TYPE_INT;
    set_payload.rhs.parameter_ordinal = 3;
    SetClause set_amount;
    set_amount.lhs = {.tab_name = "prepared_update_rows", .col_name = "amount"};
    set_amount.is_self_ref = true;
    set_amount.rhs_col = set_amount.lhs;
    set_amount.op = UpdateOp::SELF_ADD;
    set_amount.rhs.type = TYPE_FLOAT;
    set_amount.rhs.parameter_ordinal = 4;
    SetClause set_code;
    set_code.lhs = {.tab_name = "prepared_update_rows", .col_name = "code"};
    set_code.rhs.type = TYPE_STRING;
    set_code.rhs.parameter_ordinal = 6;
    SetClause set_nullable;
    set_nullable.lhs = {.tab_name = "prepared_update_rows", .col_name = "nullable_text"};
    set_nullable.rhs.type = TYPE_STRING;
    set_nullable.rhs.parameter_ordinal = 7;
    std::vector<SetClause> clauses{set_id, set_payload, set_amount, set_code, set_nullable};
    auto plan = std::make_unique<DMLPlan>(T_Update, std::move(scan), "prepared_update_rows", std::vector<Value>{},
                                          std::vector<Condition>{match, residual}, clauses);
    plan->point_access_ = PointAccessPath{{"id"}, {0}};
    auto descriptor = PreparedPlanDescriptor::Build(std::move(plan), PreparedStatementKind::Update, {}, {},
                                                    TEST_DB_NAME, sm_manager_->get_catalog_generation());
    ASSERT_TRUE(descriptor->eligible());
    const auto* executable = descriptor->update_executable();
    ASSERT_NE(executable, nullptr);
    ASSERT_EQ(executable->affected_index_bitmap.size(), 2U);
    EXPECT_TRUE(executable->affected_index_bitmap[0]);
    EXPECT_TRUE(executable->affected_index_bitmap[1]);
    ASSERT_TRUE(executable->point_update.has_value());
    EXPECT_FALSE(executable->point_update->lock_only);
    const int original_id_key = 1;
    const auto original_lookup =
        executable->indexes[0].handle->lookup_unique(reinterpret_cast<const char*>(&original_id_key));
    ASSERT_EQ(original_lookup.status, UniqueLookupStatus::Unique);
    const Rid original_rid = original_lookup.rid;

    LockManager& lock_manager = *lock_manager_;
    TransactionManager& transaction_manager = *transaction_manager_;
    LogManager& log_manager = *log_manager_;
    Transaction* transaction = transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    char buffer[256]{};
    int offset = 0;
    Context context(&lock_manager, &log_manager, transaction, buffer, &offset, &transaction_manager);
    const lsn_t global_lsn_before = log_manager.get_global_lsn();
    const int64_t log_offset_before = log_manager.current_log_offset();
    const lsn_t txn_prev_lsn_before = transaction->get_prev_lsn();
    const lsn_t page_lsn_before = executable->scan.table_handle->get_page_lsn(original_rid);
    Value key;
    key.set_int(1);
    Value expected_payload;
    expected_payload.set_int(10);
    Value int_delta;
    int_delta.set_int(67);
    Value float_delta;
    float_delta.set_float(0.5F);
    Value new_id;
    new_id.set_int(2);
    Value new_code;
    new_code.set_str("new");
    Value null_value;
    null_value.type = TYPE_STRING;
    null_value.set_null();
    ParameterFrame parameters({key, expected_payload, int_delta, float_delta, new_id, new_code, null_value});
    auto statement = portal_->start_prepared(*descriptor, parameters, &context);
    QlManager ql_manager(sm_manager_.get(), &transaction_manager, nullptr);
    txn_id_t txn_id = transaction->get_transaction_id();
    portal_->run(std::move(statement), &ql_manager, &txn_id, &context);
    EXPECT_GT(log_manager.get_global_lsn(), global_lsn_before);
    EXPECT_GT(log_manager.current_log_offset(), log_offset_before);
    EXPECT_NE(transaction->get_prev_lsn(), txn_prev_lsn_before);
    EXPECT_GT(executable->scan.table_handle->get_page_lsn(original_rid), page_lsn_before);

    const int changed_id = 2;
    auto changed_lookup = executable->indexes[0].handle->lookup_unique(reinterpret_cast<const char*>(&changed_id));
    ASSERT_EQ(changed_lookup.status, UniqueLookupStatus::Unique);
    std::vector<char> changed_code_key(executable->indexes[1].meta->col_tot_len);
    std::memcpy(changed_code_key.data(), "new", 3);
    const auto changed_code_lookup = executable->indexes[1].handle->lookup_unique(changed_code_key.data());
    ASSERT_EQ(changed_code_lookup.status, UniqueLookupStatus::Unique);
    EXPECT_EQ(changed_code_lookup.rid, changed_lookup.rid);
    auto visible = GetVisibleRecord(executable->scan.table_handle, changed_lookup.rid, &context);
    ASSERT_NE(visible, nullptr);
    const auto& columns = executable->scan.table->cols;
    EXPECT_EQ(read_unaligned<int>(visible->data + columns[0].offset), 2);
    EXPECT_EQ(read_unaligned<int>(visible->data + columns[1].offset), 77);
    EXPECT_FLOAT_EQ(read_float(visible->data + columns[2].offset), 1.75F);
    EXPECT_EQ(std::string(visible->data + columns[3].offset,
                          strnlen(visible->data + columns[3].offset, static_cast<std::size_t>(columns[3].len))),
              "new");
    EXPECT_TRUE(is_null(visible->data, columns[4]));

    transaction_manager.abort(transaction, &log_manager);

    const int restored_id = 1;
    auto restored_lookup = executable->indexes[0].handle->lookup_unique(reinterpret_cast<const char*>(&restored_id));
    EXPECT_EQ(restored_lookup.status, UniqueLookupStatus::Unique);
    EXPECT_EQ(executable->indexes[0].handle->lookup_unique(reinterpret_cast<const char*>(&changed_id)).status,
              UniqueLookupStatus::NotFound);
    std::vector<char> restored_code_key(executable->indexes[1].meta->col_tot_len);
    std::memcpy(restored_code_key.data(), "old", 3);
    const auto restored_code_lookup = executable->indexes[1].handle->lookup_unique(restored_code_key.data());
    ASSERT_EQ(restored_code_lookup.status, UniqueLookupStatus::Unique);
    EXPECT_EQ(restored_code_lookup.rid, restored_lookup.rid);
    EXPECT_EQ(executable->indexes[1].handle->lookup_unique(changed_code_key.data()).status,
              UniqueLookupStatus::NotFound);
    ASSERT_EQ(restored_lookup.status, UniqueLookupStatus::Unique);
    auto restored = executable->scan.table_handle->get_record(restored_lookup.rid, nullptr);
    ASSERT_NE(restored, nullptr);
    EXPECT_EQ(read_unaligned<int>(restored->data + columns[0].offset), 1);
    EXPECT_EQ(read_unaligned<int>(restored->data + columns[1].offset), 10);
    EXPECT_FLOAT_EQ(read_float(restored->data + columns[2].offset), 1.25F);
    EXPECT_EQ(std::string(restored->data + columns[3].offset,
                          strnlen(restored->data + columns[3].offset, static_cast<std::size_t>(columns[3].len))),
              "old");
    EXPECT_FALSE(is_null(restored->data, columns[4]));

    Transaction* miss_transaction =
        transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    Context miss_context(&lock_manager, &log_manager, miss_transaction, buffer, &offset, &transaction_manager);
    Value missing_key;
    missing_key.set_int(999);
    auto miss_statement = portal_->start_prepared(
        *descriptor,
        ParameterFrame({missing_key, expected_payload, int_delta, float_delta, new_id, new_code, null_value}),
        &miss_context);
    miss_statement->root->Next();
    EXPECT_TRUE(miss_transaction->get_write_set().empty());
    EXPECT_EQ(miss_transaction->GetUndoLogNum(), 0U);
    transaction_manager.abort(miss_transaction, &log_manager);

    Transaction* residual_transaction =
        transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    Context residual_context(&lock_manager, &log_manager, residual_transaction, buffer, &offset, &transaction_manager);
    Value false_residual;
    false_residual.set_int(999);
    auto residual_statement = portal_->start_prepared(
        *descriptor, ParameterFrame({key, false_residual, int_delta, float_delta, new_id, new_code, null_value}),
        &residual_context);
    residual_statement->root->Next();
    EXPECT_TRUE(residual_transaction->get_write_set().empty());
    EXPECT_EQ(residual_transaction->GetUndoLogNum(), 0U);
    transaction_manager.abort(residual_transaction, &log_manager);

    for (IsolationLevel level : {IsolationLevel::READ_COMMITTED, IsolationLevel::SERIALIZABLE}) {
        Transaction* fallback_transaction = transaction_manager.begin(nullptr, &log_manager, level);
        Context fallback_context(&lock_manager, &log_manager, fallback_transaction, buffer, &offset,
                                 &transaction_manager);
        auto fallback_statement = portal_->start_prepared(
            *descriptor, ParameterFrame({key, false_residual, int_delta, float_delta, new_id, new_code, null_value}),
            &fallback_context);
        fallback_statement->root->Next();
        EXPECT_TRUE(fallback_transaction->get_write_set().empty());
        transaction_manager.abort(fallback_transaction, &log_manager);
    }

    Transaction* stale_transaction =
        transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction* writer_transaction =
        transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    Context writer_context(&lock_manager, &log_manager, writer_transaction, buffer, &offset, &transaction_manager);
    auto writer_statement = portal_->start_prepared(
        *descriptor, ParameterFrame({key, expected_payload, int_delta, float_delta, new_id, new_code, null_value}),
        &writer_context);
    writer_statement->root->Next();
    transaction_manager.commit(writer_transaction, &log_manager);

    Context stale_context(&lock_manager, &log_manager, stale_transaction, buffer, &offset, &transaction_manager);
    auto stale_statement = portal_->start_prepared(
        *descriptor, ParameterFrame({key, expected_payload, int_delta, float_delta, new_id, new_code, null_value}),
        &stale_context);
    try {
        stale_statement->root->Next();
        FAIL() << "a newer committed point UPDATE must abort the stale writer";
    } catch (TransactionAbortException& error) {
        EXPECT_EQ(error.GetAbortReason(), AbortReason::WW_CONFLICT);
    }
    transaction_manager.abort(stale_transaction, &log_manager);

    Transaction* own_transaction = transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    Context own_context(&lock_manager, &log_manager, own_transaction, buffer, &offset, &transaction_manager);
    Value current_key;
    current_key.set_int(2);
    Value current_payload;
    current_payload.set_int(77);
    Value one;
    one.set_int(1);
    Value quarter;
    quarter.set_float(0.25F);
    Value own_code;
    own_code.set_str("own");
    auto own_first = portal_->start_prepared(
        *descriptor, ParameterFrame({current_key, current_payload, one, quarter, current_key, own_code, null_value}),
        &own_context);
    own_first->root->Next();

    Value next_payload;
    next_payload.set_int(78);
    auto own_second = portal_->start_prepared(
        *descriptor, ParameterFrame({current_key, next_payload, one, quarter, current_key, own_code, null_value}),
        &own_context);
    own_second->root->Next();
    auto own_visible = GetVisibleRecord(executable->scan.table_handle, changed_lookup.rid, &own_context);
    ASSERT_NE(own_visible, nullptr);
    EXPECT_EQ(read_unaligned<int>(own_visible->data + columns[1].offset), 79);
    EXPECT_FLOAT_EQ(read_float(own_visible->data + columns[2].offset), 2.25F);
    transaction_manager.abort(own_transaction, &log_manager);

    auto committed_after_own_abort = executable->scan.table_handle->get_record(changed_lookup.rid, nullptr);
    ASSERT_NE(committed_after_own_abort, nullptr);
    EXPECT_EQ(read_unaligned<int>(committed_after_own_abort->data + columns[1].offset), 77);
    EXPECT_FLOAT_EQ(read_float(committed_after_own_abort->data + columns[2].offset), 1.75F);

    Transaction* concurrent_transaction =
        transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction* uncommitted_writer =
        transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    Context uncommitted_context(&lock_manager, &log_manager, uncommitted_writer, buffer, &offset, &transaction_manager);
    auto uncommitted_statement = portal_->start_prepared(
        *descriptor, ParameterFrame({current_key, current_payload, one, quarter, current_key, own_code, null_value}),
        &uncommitted_context);
    uncommitted_statement->root->Next();

    Context concurrent_context(&lock_manager, &log_manager, concurrent_transaction, buffer, &offset,
                               &transaction_manager);
    auto concurrent_statement = portal_->start_prepared(
        *descriptor, ParameterFrame({current_key, current_payload, one, quarter, current_key, own_code, null_value}),
        &concurrent_context);

    auto waiter_result = std::async(std::launch::async, [&] { concurrent_statement->root->Next(); });
    const auto enqueue_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    bool waiter_enqueued = false;
    while (std::chrono::steady_clock::now() < enqueue_deadline) {
        if (lock_manager.has_record_waiters_for_test()) {
            waiter_enqueued = true;
            break;
        }
        std::this_thread::yield();
    }
    if (!waiter_enqueued) {
        transaction_manager.abort(uncommitted_writer, &log_manager);
        if (waiter_result.wait_for(std::chrono::seconds(2)) != std::future_status::ready) {
            transaction_manager.abort(concurrent_transaction, &log_manager);
        }
        ASSERT_EQ(waiter_result.wait_for(std::chrono::seconds(2)), std::future_status::ready)
            << "SI first-lock waiter was not enqueued before the deadline";
        EXPECT_NO_THROW(waiter_result.get());
        transaction_manager.abort(concurrent_transaction, &log_manager);
        return;
    }

    transaction_manager.abort(uncommitted_writer, &log_manager);
    ASSERT_EQ(waiter_result.wait_for(std::chrono::seconds(2)), std::future_status::ready)
        << "SI first-lock waiter did not finish after owner abort";
    EXPECT_NO_THROW(waiter_result.get());
    transaction_manager.abort(concurrent_transaction, &log_manager);

    const int post_wait_id = 2;
    const auto post_wait_lookup =
        executable->indexes[0].handle->lookup_unique(reinterpret_cast<const char*>(&post_wait_id));
    ASSERT_EQ(post_wait_lookup.status, UniqueLookupStatus::Unique);
    EXPECT_EQ(post_wait_lookup.rid, changed_lookup.rid);
    auto post_wait_record = executable->scan.table_handle->get_record(post_wait_lookup.rid, nullptr);
    ASSERT_NE(post_wait_record, nullptr);
    EXPECT_EQ(read_unaligned<int>(post_wait_record->data + columns[0].offset), 2);
    EXPECT_EQ(read_unaligned<int>(post_wait_record->data + columns[1].offset), 77);
    EXPECT_FLOAT_EQ(read_float(post_wait_record->data + columns[2].offset), 1.75F);

    Transaction* stale_delete_transaction =
        transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction* delete_transaction =
        transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    Context delete_context(&lock_manager, &log_manager, delete_transaction, buffer, &offset, &transaction_manager);
    Condition delete_by_id;
    delete_by_id.lhs_col = {.tab_name = "prepared_update_rows", .col_name = "id"};
    delete_by_id.op = OP_EQ;
    delete_by_id.is_rhs_val = true;
    delete_by_id.rhs_val = current_key;
    DeleteExecutor delete_executor(sm_manager_.get(), "prepared_update_rows", {delete_by_id},
                                   std::vector<Rid>{changed_lookup.rid}, &delete_context);
    delete_executor.Next();
    transaction_manager.commit(delete_transaction, &log_manager);

    Context stale_delete_context(&lock_manager, &log_manager, stale_delete_transaction, buffer, &offset,
                                 &transaction_manager);
    auto stale_delete_statement = portal_->start_prepared(
        *descriptor, ParameterFrame({current_key, current_payload, one, quarter, current_key, own_code, null_value}),
        &stale_delete_context);
    try {
        stale_delete_statement->root->Next();
        FAIL() << "a newer committed DELETE must abort the stale point writer";
    } catch (TransactionAbortException& error) {
        EXPECT_EQ(error.GetAbortReason(), AbortReason::WW_CONFLICT);
    }
    transaction_manager.abort(stale_delete_transaction, nullptr);

    const std::string historical_id_index =
        sm_manager_->get_ix_manager()->get_index_name("prepared_update_rows", std::vector<std::string>{"id"});
    ASSERT_TRUE(sm_manager_->has_historical_index_keys("prepared_update_rows", historical_id_index));
    sm_manager_->drop_table("prepared_update_rows", nullptr);
    sm_manager_->create_table("prepared_update_rows",
                              {{"id", TYPE_INT, 4},
                               {"payload", TYPE_INT, 4},
                               {"amount", TYPE_FLOAT, 4},
                               {"code", TYPE_STRING, 8},
                               {"nullable_text", TYPE_STRING, 8}},
                              nullptr);
    sm_manager_->create_index("prepared_update_rows", {"id"}, nullptr);
    sm_manager_->create_index("prepared_update_rows", {"code"}, nullptr);
    EXPECT_FALSE(sm_manager_->has_historical_index_keys("prepared_update_rows", historical_id_index));
    Value recreated_payload;
    recreated_payload.set_int(999);
    run_logged_write([&](Context* context) {
        InsertExecutor recreated_insert(sm_manager_.get(), "prepared_update_rows",
                                        {current_key, recreated_payload, original_amount, own_code, original_nullable},
                                        context);
        recreated_insert.Next();
    });
    Transaction* recreated_transaction =
        transaction_manager.begin(nullptr, nullptr, IsolationLevel::SNAPSHOT_ISOLATION);
    Context recreated_context(&lock_manager, nullptr, recreated_transaction, buffer, &offset, &transaction_manager);
    auto recreated_statement = portal_->start_prepared(
        *descriptor, ParameterFrame({current_key, current_payload, one, quarter, current_key, own_code, null_value}),
        &recreated_context);
    recreated_statement->root->Next();
    transaction_manager.abort(recreated_transaction, nullptr);
}

TEST_F(PortalAggregateTest, prepared_si_self_assignment_locks_without_mutation_side_effects) {
    sm_manager_->create_table("lock_only_rows",
                              {{"id", TYPE_INT, 4}, {"payload", TYPE_INT, 4}, {"code", TYPE_STRING, 8}}, nullptr);
    sm_manager_->create_index("lock_only_rows", {"id"}, nullptr);
    sm_manager_->create_index("lock_only_rows", {"code"}, nullptr);

    Value id;
    id.set_int(1);
    Value payload;
    payload.set_int(10);
    Value code;
    code.set_str("stable");
    run_logged_write([&](Context* context) {
        InsertExecutor insert(sm_manager_.get(), "lock_only_rows", {id, payload, code}, context);
        insert.Next();
    });

    Condition point;
    point.lhs_col = {.tab_name = "lock_only_rows", .col_name = "id"};
    point.op = OP_EQ;
    point.is_rhs_val = true;
    point.rhs_val.type = TYPE_INT;
    point.rhs_val.parameter_ordinal = 1;
    Condition residual;
    residual.lhs_col = {.tab_name = "lock_only_rows", .col_name = "payload"};
    residual.op = OP_EQ;
    residual.is_rhs_val = true;
    residual.rhs_val.type = TYPE_INT;
    residual.rhs_val.parameter_ordinal = 2;

    std::vector<SetClause> clauses;
    for (const char* column : {"id", "payload", "code"}) {
        SetClause clause;
        clause.lhs = {.tab_name = "lock_only_rows", .col_name = column};
        clause.is_self_ref = true;
        clause.rhs_col = clause.lhs;
        clause.op = UpdateOp::ASSIGNMENT;
        clauses.push_back(std::move(clause));
    }
    auto scan = std::make_unique<ScanPlan>(T_IndexScan, sm_manager_.get(), "lock_only_rows",
                                           std::vector<Condition>{point, residual}, std::vector<std::string>{"id"});
    auto plan = std::make_unique<DMLPlan>(T_Update, std::move(scan), "lock_only_rows", std::vector<Value>{},
                                          std::vector<Condition>{point, residual}, clauses);
    plan->point_access_ = PointAccessPath{{"id"}, {0}};
    plan->update_execution_mode_ = UpdateExecutionMode::LockOnlySelfAssignment;
    auto descriptor = PreparedPlanDescriptor::Build(std::move(plan), PreparedStatementKind::Update, {}, {},
                                                    TEST_DB_NAME, sm_manager_->get_catalog_generation());
    ASSERT_TRUE(descriptor->eligible());
    ASSERT_NE(descriptor->update_executable(), nullptr);
    ASSERT_TRUE(descriptor->update_executable()->point_update.has_value());

    const auto* executable = descriptor->update_executable();
    const int key = 1;
    const auto lookup = executable->point_update->index_handle->lookup_unique(reinterpret_cast<const char*>(&key));
    ASSERT_EQ(lookup.status, UniqueLookupStatus::Unique);
    const Rid rid = lookup.rid;
    const TupleMeta meta_before = executable->scan.table_handle->get_tuple_meta(rid);
    const lsn_t page_lsn_before = executable->scan.table_handle->get_page_lsn(rid);
    auto record_before = executable->scan.table_handle->get_record(rid, nullptr);
    ASSERT_NE(record_before, nullptr);

    LockManager& lock_manager = *lock_manager_;
    TransactionManager& transaction_manager = *transaction_manager_;
    Transaction* transaction = transaction_manager.begin(nullptr, nullptr, IsolationLevel::SNAPSHOT_ISOLATION);
    char buffer[256]{};
    int offset = 0;
    Context context(&lock_manager, nullptr, transaction, buffer, &offset, &transaction_manager);
    Value bound_id;
    bound_id.set_int(1);
    Value bound_payload;
    bound_payload.set_int(10);
    auto statement = portal_->start_prepared(*descriptor, ParameterFrame({bound_id, bound_payload}), &context);
    statement->root->Next();

    EXPECT_EQ(transaction->get_lock_set()->count(
                  LockDataId(executable->scan.table_handle->GetFd(), rid, LockDataType::RECORD)),
              1U);
    EXPECT_EQ(transaction->GetUndoLogNum(), 0U);
    EXPECT_TRUE(transaction->get_write_set().empty());
    EXPECT_TRUE(transaction->modified_slots_.empty());
    EXPECT_EQ(transaction->get_prev_lsn(), INVALID_LSN);
    EXPECT_EQ(executable->scan.table_handle->get_tuple_meta(rid), meta_before);
    EXPECT_EQ(executable->scan.table_handle->get_page_lsn(rid), page_lsn_before);
    auto record_after = executable->scan.table_handle->get_record(rid, nullptr);
    ASSERT_NE(record_after, nullptr);
    EXPECT_EQ(std::memcmp(record_after->data, record_before->data, record_before->size), 0);
    EXPECT_EQ(executable->point_update->index_handle->lookup_unique(reinterpret_cast<const char*>(&key)).rid, rid);

    Transaction competing(100000, IsolationLevel::SNAPSHOT_ISOLATION);
    const LockDataId record_lock(executable->scan.table_handle->GetFd(), rid, LockDataType::RECORD);
    auto contender_result = std::async(std::launch::async, [&] {
        return lock_manager.lock_exclusive_on_record(&competing, rid, executable->scan.table_handle->GetFd());
    });
    const auto enqueue_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    bool contender_enqueued = false;
    while (std::chrono::steady_clock::now() < enqueue_deadline) {
        if (lock_manager.has_record_waiters_for_test()) {
            contender_enqueued = true;
            break;
        }
        std::this_thread::yield();
    }
    if (!contender_enqueued) {
        transaction_manager.abort(transaction, nullptr);
        if (contender_result.wait_for(std::chrono::seconds(2)) != std::future_status::ready) {
            competing.mark_lock_cancellation_requested();
        }
        ASSERT_EQ(contender_result.wait_for(std::chrono::seconds(2)), std::future_status::ready)
            << "SI self-assignment contender was not enqueued before the deadline";
        EXPECT_FALSE(contender_result.get());
        return;
    }
    transaction_manager.abort(transaction, nullptr);
    ASSERT_EQ(contender_result.wait_for(std::chrono::seconds(2)), std::future_status::ready)
        << "SI self-assignment contender did not finish after owner abort";
    EXPECT_TRUE(contender_result.get());
    ASSERT_TRUE(lock_manager.unlock(&competing, record_lock));

    auto expect_context_fallback = [&](Context* incomplete_context) {
        auto fallback =
            portal_->start_prepared(*descriptor, ParameterFrame({bound_id, bound_payload}), incomplete_context);
        EXPECT_NE(fallback, nullptr);
    };
    Context null_transaction_context(&lock_manager, nullptr, nullptr, buffer, &offset, &transaction_manager);
    null_transaction_context.isolation_level_ = IsolationLevel::SNAPSHOT_ISOLATION;
    expect_context_fallback(&null_transaction_context);
    Transaction no_manager_transaction(100001, IsolationLevel::SNAPSHOT_ISOLATION);
    Context null_transaction_manager_context(&lock_manager, nullptr, &no_manager_transaction, buffer, &offset, nullptr);
    expect_context_fallback(&null_transaction_manager_context);
    Transaction* no_lock_transaction = transaction_manager.begin(nullptr, nullptr, IsolationLevel::SNAPSHOT_ISOLATION);
    Context null_lock_manager_context(nullptr, nullptr, no_lock_transaction, buffer, &offset, &transaction_manager);
    expect_context_fallback(&null_lock_manager_context);
    EXPECT_TRUE(no_lock_transaction->get_lock_set()->empty());
    transaction_manager.abort(no_lock_transaction, nullptr);

    LogManager& log_manager = *log_manager_;
    Transaction* wal_transaction = transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    Context wal_context(&lock_manager, &log_manager, wal_transaction, buffer, &offset, &transaction_manager);
    const lsn_t wal_global_before = log_manager.get_global_lsn();
    const int64_t wal_offset_before = log_manager.current_log_offset();
    const lsn_t txn_lsn_before = wal_transaction->get_prev_lsn();
    auto wal_statement = portal_->start_prepared(*descriptor, ParameterFrame({bound_id, bound_payload}), &wal_context);
    wal_statement->root->Next();
    EXPECT_EQ(log_manager.get_global_lsn(), wal_global_before);
    EXPECT_EQ(log_manager.current_log_offset(), wal_offset_before);
    EXPECT_EQ(wal_transaction->get_prev_lsn(), txn_lsn_before);
    EXPECT_EQ(executable->scan.table_handle->get_page_lsn(rid), page_lsn_before);
    transaction_manager.abort(wal_transaction, &log_manager);

    Transaction* miss_transaction = transaction_manager.begin(nullptr, nullptr, IsolationLevel::SNAPSHOT_ISOLATION);
    Context miss_context(&lock_manager, nullptr, miss_transaction, buffer, &offset, &transaction_manager);
    Value false_residual;
    false_residual.set_int(999);
    auto miss = portal_->start_prepared(*descriptor, ParameterFrame({bound_id, false_residual}), &miss_context);
    miss->root->Next();
    EXPECT_TRUE(miss_transaction->get_lock_set()->empty());
    EXPECT_TRUE(miss_transaction->get_write_set().empty());
    EXPECT_EQ(miss_transaction->GetUndoLogNum(), 0U);
    transaction_manager.abort(miss_transaction, nullptr);

    for (IsolationLevel level : {IsolationLevel::READ_COMMITTED, IsolationLevel::SERIALIZABLE}) {
        Transaction* fallback_transaction = transaction_manager.begin(nullptr, &log_manager, level);
        Context fallback_context(&lock_manager, &log_manager, fallback_transaction, buffer, &offset,
                                 &transaction_manager);
        auto fallback =
            portal_->start_prepared(*descriptor, ParameterFrame({bound_id, bound_payload}), &fallback_context);
        fallback->root->Next();
        EXPECT_GT(fallback_transaction->GetUndoLogNum(), 0U);
        EXPECT_FALSE(fallback_transaction->get_write_set().empty());
        transaction_manager.abort(fallback_transaction, &log_manager);
    }

    auto execute_relative_update = [&](Transaction* owner, int delta) {
        Condition by_id;
        by_id.lhs_col = {.tab_name = "lock_only_rows", .col_name = "id"};
        by_id.op = OP_EQ;
        by_id.is_rhs_val = true;
        by_id.rhs_val.set_int(1);
        by_id.rhs_val.init_raw(sizeof(int));
        SetClause add;
        add.lhs = {.tab_name = "lock_only_rows", .col_name = "payload"};
        add.is_self_ref = true;
        add.rhs_col = add.lhs;
        add.op = UpdateOp::SELF_ADD;
        add.rhs.set_int(delta);
        Context owner_context(&lock_manager, &log_manager, owner, buffer, &offset, &transaction_manager);
        UpdateExecutor update(sm_manager_.get(), "lock_only_rows", {add}, {by_id}, std::vector<Rid>{rid},
                              &owner_context);
        update.Next();
    };
    auto current_payload = [&] {
        auto record = executable->scan.table_handle->get_record(rid, nullptr);
        EXPECT_NE(record, nullptr);
        return record == nullptr ? -1 : read_unaligned<int>(record->data + executable->scan.table->cols[1].offset);
    };

    Transaction* stale = transaction_manager.begin(nullptr, nullptr, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction* writer = transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    execute_relative_update(writer, 1);
    transaction_manager.commit(writer, &log_manager);
    ASSERT_EQ(current_payload(), 11);

    Context stale_context(&lock_manager, nullptr, stale, buffer, &offset, &transaction_manager);
    auto stale_statement =
        portal_->start_prepared(*descriptor, ParameterFrame({bound_id, bound_payload}), &stale_context);
    try {
        stale_statement->root->Next();
        FAIL() << "a newer committed writer must abort the stale snapshot";
    } catch (TransactionAbortException& error) {
        EXPECT_EQ(error.GetAbortReason(), AbortReason::WW_CONFLICT);
    }
    transaction_manager.abort(stale, nullptr);

    Transaction* own_write = transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    execute_relative_update(own_write, 4);
    const std::size_t undo_after_update = own_write->GetUndoLogNum();
    const std::size_t writes_after_update = own_write->get_write_set().size();
    const std::size_t modified_after_update = own_write->modified_slots_.size();
    const lsn_t prev_lsn_after_update = own_write->get_prev_lsn();
    const TupleMeta own_meta_after_update = executable->scan.table_handle->get_tuple_meta(rid);
    const lsn_t own_page_lsn_after_update = executable->scan.table_handle->get_page_lsn(rid);
    Value payload_15;
    payload_15.set_int(15);
    Context own_context(&lock_manager, &log_manager, own_write, buffer, &offset, &transaction_manager);
    auto own_statement = portal_->start_prepared(*descriptor, ParameterFrame({bound_id, payload_15}), &own_context);
    own_statement->root->Next();
    EXPECT_EQ(own_write->GetUndoLogNum(), undo_after_update);
    EXPECT_EQ(own_write->get_write_set().size(), writes_after_update);
    EXPECT_EQ(own_write->modified_slots_.size(), modified_after_update);
    EXPECT_EQ(own_write->get_prev_lsn(), prev_lsn_after_update);
    EXPECT_EQ(executable->scan.table_handle->get_tuple_meta(rid), own_meta_after_update);
    EXPECT_EQ(executable->scan.table_handle->get_page_lsn(rid), own_page_lsn_after_update);
    transaction_manager.abort(own_write, &log_manager);
    EXPECT_EQ(current_payload(), 11) << "the earlier real UPDATE undo must remain effective";

    Transaction* committing_own_write =
        transaction_manager.begin(nullptr, &log_manager, IsolationLevel::SNAPSHOT_ISOLATION);
    execute_relative_update(committing_own_write, 4);
    Context commit_context(&lock_manager, &log_manager, committing_own_write, buffer, &offset, &transaction_manager);
    auto commit_statement =
        portal_->start_prepared(*descriptor, ParameterFrame({bound_id, payload_15}), &commit_context);
    commit_statement->root->Next();
    transaction_manager.commit(committing_own_write, &log_manager);
    EXPECT_EQ(current_payload(), 15);

    Value second_id;
    second_id.set_int(2);
    Value second_payload;
    second_payload.set_int(15);
    Value second_code;
    second_code.set_str("second");
    run_logged_write([&](Context* context) {
        InsertExecutor second_insert(sm_manager_.get(), "lock_only_rows", {second_id, second_payload, second_code},
                                     context);
        second_insert.Next();
    });
    const int second_key = 2;
    const auto second_lookup =
        executable->point_update->index_handle->lookup_unique(reinterpret_cast<const char*>(&second_key));
    ASSERT_EQ(second_lookup.status, UniqueLookupStatus::Unique);
    const Rid second_rid = second_lookup.rid;

    executable->point_update->index_handle->insert_entry(reinterpret_cast<const char*>(&key), second_rid,
                                                         IndexWriteWalContext::TestNoWal(), true);
    ASSERT_EQ(executable->point_update->index_handle->lookup_unique(reinterpret_cast<const char*>(&key)).status,
              UniqueLookupStatus::Duplicate);
    Transaction* duplicate_transaction =
        transaction_manager.begin(nullptr, nullptr, IsolationLevel::SNAPSHOT_ISOLATION);
    Context duplicate_context(&lock_manager, nullptr, duplicate_transaction, buffer, &offset, &transaction_manager);
    auto duplicate_statement =
        portal_->start_prepared(*descriptor, ParameterFrame({bound_id, payload_15}), &duplicate_context);
    transaction_manager.abort(duplicate_transaction, nullptr);
    ASSERT_TRUE(executable->point_update->index_handle->delete_entry(reinterpret_cast<const char*>(&key), second_rid,
                                                                     IndexWriteWalContext::TestNoWal()));

    auto second_record = executable->scan.table_handle->get_record(second_rid, nullptr);
    ASSERT_NE(second_record, nullptr);
    write_unaligned(second_record->data + executable->scan.table->cols[0].offset, key);
    executable->scan.table_handle->update_record(second_rid, second_record->data, nullptr);
    std::vector<char> historical_key(sizeof(int));
    std::memcpy(historical_key.data(), &key, sizeof(int));
    sm_manager_->remember_historical_index_key(executable->scan.table_name, executable->point_update->index_name,
                                               historical_key, second_rid, *executable->point_update->index);
    Transaction* multiple_transaction = transaction_manager.begin(nullptr, nullptr, IsolationLevel::SNAPSHOT_ISOLATION);
    Context multiple_context(&lock_manager, nullptr, multiple_transaction, buffer, &offset, &transaction_manager);
    auto multiple_statement =
        portal_->start_prepared(*descriptor, ParameterFrame({bound_id, payload_15}), &multiple_context);
    transaction_manager.abort(multiple_transaction, nullptr);
}

TEST_F(PortalAggregateTest, point_dml_is_enabled_by_default_and_non_rc_isolation_falls_back) {
    sm_manager_->create_table("point_rows", {{"id", TYPE_INT, 4}, {"payload", TYPE_INT, 4}}, nullptr);
    sm_manager_->create_index("point_rows", {"id"}, nullptr);

    char buffer[256]{};
    int offset = 0;
    Transaction* rc_transaction =
        transaction_manager_->begin(nullptr, log_manager_.get(), IsolationLevel::READ_COMMITTED);
    Context rc_context(lock_manager_.get(), log_manager_.get(), rc_transaction, buffer, &offset,
                       transaction_manager_.get());
    rc_context.isolation_level_ = IsolationLevel::READ_COMMITTED;
    Value id;
    id.set_int(7);
    Value payload;
    payload.set_int(11);
    InsertExecutor insert(sm_manager_.get(), "point_rows", {id, payload}, &rc_context);
    insert.Next();
    transaction_manager_->commit(rc_transaction, log_manager_.get());

    Condition condition;
    condition.lhs_col = {.tab_name = "point_rows", .col_name = "id"};
    condition.op = OP_EQ;
    condition.is_rhs_val = true;
    condition.rhs_val.set_int(7);
    DMLPlan plan(T_Delete, nullptr, "point_rows", {}, {condition}, {});
    plan.point_access_ = PointAccessPath{{"id"}, {0}};

    auto rc_result = portal_->resolve_point_rid(plan, &rc_context);
    ASSERT_TRUE(rc_result.has_value());
    EXPECT_TRUE(rc_result->has_value());

    Context si_context(nullptr, nullptr, nullptr, buffer, &offset);
    si_context.isolation_level_ = IsolationLevel::SNAPSHOT_ISOLATION;
    EXPECT_FALSE(portal_->resolve_point_rid(plan, &si_context).has_value());

    Context serializable_context(nullptr, nullptr, nullptr, buffer, &offset);
    serializable_context.isolation_level_ = IsolationLevel::SERIALIZABLE;
    EXPECT_FALSE(portal_->resolve_point_rid(plan, &serializable_context).has_value());

    auto& table = sm_manager_->db_.get_table("point_rows");
    auto index = table.get_index_meta({"id"});
    ASSERT_NE(index, table.indexes.end());
    const std::string index_name = ix_manager_->get_index_name("point_rows", index->cols);
    std::vector<char> historical_key(sizeof(int));
    std::memcpy(historical_key.data(), &id.int_val, sizeof(int));
    sm_manager_->remember_historical_index_key("point_rows", index_name, historical_key, Rid{99, 1}, *index);

    LockManager lock_manager;
    TransactionManager transaction_manager(&lock_manager, sm_manager_.get());
    Transaction transaction(17, IsolationLevel::READ_COMMITTED);
    Context historical_context(&lock_manager, nullptr, &transaction, buffer, &offset, &transaction_manager);
    EXPECT_FALSE(portal_->resolve_point_rid(plan, &historical_context).has_value())
        << "a conflicting historical RID must use the scan fallback";
}
