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

#include "execution_manager.h"
#include "executor_builder.h"
#include "executor_delete.h"
#include "executor_insert.h"
#include "executor_update.h"

#include <iomanip>
#include <set>

#include "index/ix.h"
#include "recovery/checkpoint_manager.h"
#include "recovery/log_manager.h"
#include "record_printer.h"

const char* help_info = "Supported SQL syntax:\n"
                        "  command ;\n"
                        "command:\n"
                        "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
                        "  DROP TABLE table_name\n"
                        "  CREATE INDEX table_name (column_name)\n"
                        "  DROP INDEX table_name (column_name)\n"
                        "  INSERT INTO table_name VALUES (value [, value ...])\n"
                        "  DELETE FROM table_name [WHERE where_clause]\n"
                        "  UPDATE table_name SET column_name = value [, column_name = value ...]"
                        " [WHERE where_clause]\n"
                        "  SELECT selector FROM table_name [WHERE where_clause]\n"
                        "type:\n"
                        "  {INT | FLOAT | CHAR(n)}\n"
                        "where_clause:\n"
                        "  condition [AND condition ...]\n"
                        "condition:\n"
                        "  column op {column | value}\n"
                        "column:\n"
                        "  [table_name.]column_name\n"
                        "op:\n"
                        "  {= | <> | < | > | <= | >=}\n"
                        "selector:\n"
                        "  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(Plan* plan, Context* context) {
    auto* x = static_cast<DDLPlan*>(plan);
    switch (plan->tag) {
    case T_CreateTable: {
        sm_manager_->create_table(x->tab_name_, x->cols_, context);
        break;
    }
    case T_DropTable: {
        sm_manager_->drop_table(x->tab_name_, context);
        break;
    }
    case T_CreateIndex: {
        sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
        break;
    }
    case T_DropIndex: {
        sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
        break;
    }
    default:
        throw InternalError("Unexpected field type");
        break;
    }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(Plan* plan, txn_id_t* txn_id, Context* context, ExecutionOutput* output) {
    switch (plan->tag) {
    case T_Help:
    case T_ShowTable:
    case T_ShowIndex:
    case T_DescTable:
    case T_Transaction_begin:
    case T_Transaction_commit:
    case T_Transaction_abort:
    case T_Transaction_rollback: {
        auto* x = static_cast<OtherPlan*>(plan);
        switch (plan->tag) {
        case T_Help: {
            memcpy(output->data_send + *output->offset, help_info, strlen(help_info));
            *output->offset = strlen(help_info);
            break;
        }
        case T_ShowTable: {
            sm_manager_->show_tables(output);
            break;
        }
        case T_ShowIndex: {
            sm_manager_->show_index(x->tab_name_, output);
            break;
        }
        case T_DescTable: {
            sm_manager_->desc_table(x->tab_name_, output);
            break;
        }
        case T_Transaction_begin: {
            // 显示开启一个事务
            if (context->txn_ == nullptr) {
                context->txn_ = txn_mgr_->begin(nullptr, context->log_mgr_, context->isolation_level_);
                *txn_id = context->txn_->get_transaction_id();
            }
            context->txn_->set_txn_mode(true);
            // Propagate isolation level from Context to Transaction
            context->txn_->set_isolation_level(context->isolation_level_);
            break;
        }
        default:
            throw InternalError("Unexpected field type");
            break;
        }
        break;
    }
    case T_SetTransaction: {
        auto* x = static_cast<SetTransactionPlan*>(plan);
        switch (x->isolation_level_) {
        case ast::IsolationLevelType::SNAPSHOT_ISOLATION: {
            context->isolation_level_ = IsolationLevel::SNAPSHOT_ISOLATION;
            break;
        }
        case ast::IsolationLevelType::SERIALIZABLE: {
            context->isolation_level_ = IsolationLevel::SERIALIZABLE;
            break;
        }
        }
        break;
    }
    case T_SetOutputFile: {
        auto* x = static_cast<SetOutputFilePlan*>(plan);
        if (output != nullptr && output->output_file_enabled != nullptr) {
            *output->output_file_enabled = x->enable_;
        } else {
            sm_manager_->output_file_enabled_ = x->enable_;
        }
        break;
    }
    case T_LoadData: {
        auto* x = static_cast<LoadDataPlan*>(plan);
        sm_manager_->load_csv_data(x->file_name_, x->tab_name_, context);
        break;
    }
    case T_StaticCheckpoint: {
        if (txn_id != nullptr) {
            Transaction* current_txn = txn_mgr_->get_transaction(*txn_id);
            if (current_txn != nullptr && current_txn->get_state() != TransactionState::COMMITTED &&
                current_txn->get_state() != TransactionState::ABORTED) {
                throw RMDBError("static checkpoint cannot run inside an active transaction");
            }
        }
        CheckpointManager checkpoint_mgr(txn_mgr_, sm_manager_, context == nullptr ? nullptr : context->log_mgr_);
        checkpoint_mgr.RunCleanCheckpoint();
        break;
    }
    default:
        throw InternalError("Unexpected field type");
        break;
    }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<std::string> output_names,
                            Context* context, ExecutionOutput* output) {
    const auto& result_cols = executorTreeRoot->cols();
    std::vector<std::string> captions = std::move(output_names);
    if (captions.size() != result_cols.size()) {
        captions.clear();
        captions.reserve(result_cols.size());
        for (const auto& col : result_cols) {
            captions.push_back(col.name);
        }
    }

    struct SsiReadTrackingGuard {
        Context* context_;
        bool old_value_;

        explicit SsiReadTrackingGuard(Context* context) : context_(context), old_value_(false) {
            if (context_ != nullptr) {
                old_value_ = context_->enable_ssi_read_tracking_;
                context_->enable_ssi_read_tracking_ = true;
            }
        }

        ~SsiReadTrackingGuard() {
            if (context_ != nullptr) {
                context_->enable_ssi_read_tracking_ = old_value_;
            }
        }
    } ssi_read_tracking_guard(context);

    if (output != nullptr && output->result_sink != nullptr) {
        output->result_sink->begin_query(result_cols, captions);
        for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
            TupleView tuple = executorTreeRoot->current();
            if (!tuple) {
                throw ExecutionException("executor returned an empty tuple");
            }
            output->result_sink->append_row(result_cols, tuple.data, tuple.size);
        }
        return;
    }

    // Print records
    size_t num_rec = 0;
    const int output_start = *output->offset;

    // Format the result directly into the request response buffer. If execution
    // aborts, client_handler replaces the buffer with the error response.
    RecordPrinter rec_printer(captions.size());
    rec_printer.print_separator(output);
    rec_printer.print_record(captions, output);
    rec_printer.print_separator(output);

    const bool output_file_enabled =
        output->output_file_enabled != nullptr ? *output->output_file_enabled : sm_manager_->output_file_enabled_;
    std::ostringstream out_file_stream;
    if (output_file_enabled) {
        out_file_stream << "|";
        for (const auto& cap : captions) {
            out_file_stream << " " << cap << " |";
        }
        out_file_stream << "\n";
    }

    // 执行query_plan
    std::vector<std::string> columns;
    columns.reserve(result_cols.size());
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        TupleView tuple = executorTreeRoot->current();
        columns.clear();
        for (auto& col : result_cols) {
            std::string col_str;
            const char* rec_buf = tuple.data + col.offset;
            if (is_null(tuple.data, col)) {
                // NULL 是新增的值形态，此前不可能出现，因此不影响 output.txt
                // 既有格式；不能写成空串（final.md:761）。
                col_str = NULL_DISPLAY_TEXT;
            } else if (col.type == TYPE_INT) {
                col_str = std::to_string(read_unaligned<int>(rec_buf));
            } else if (col.type == TYPE_FLOAT) {
                col_str = std::to_string(read_float(rec_buf));
            } else if (col.type == TYPE_STRING || col.type == TYPE_DATETIME) {
                col_str.assign(rec_buf, strnlen(rec_buf, col.len));
            }
            columns.push_back(col_str);
        }
        // print record into client buffer
        rec_printer.print_record(columns, output);
        // print record into output.txt (compact borderless)
        if (output_file_enabled) {
            out_file_stream << "|";
            for (const auto& col_str : columns) {
                out_file_stream << " " << col_str << " |";
            }
            out_file_stream << "\n";
        }
        num_rec++;
    }
    // Print footer into client buffer
    rec_printer.print_separator(output);
    // Print record count into client buffer
    RecordPrinter::print_record_count(num_rec, output);

    if (output_file_enabled && *output->offset > output_start) {
        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << out_file_stream.str();
        outfile.close();
    }
}

void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                            Context* context, ExecutionOutput* output) {
    std::vector<std::string> output_names;
    output_names.reserve(sel_cols.size());
    for (const auto& sel_col : sel_cols) {
        output_names.push_back(sel_col.col_name);
    }
    select_from(std::move(executorTreeRoot), std::move(output_names), context, output);
}

namespace {

std::vector<std::string> output_names_for(const Plan& plan) {
    switch (plan.tag) {
    case T_Projection: {
        const auto& projection = *static_cast<const ProjectionPlan*>(&plan);
        if (!projection.output_names_.empty()) {
            return projection.output_names_;
        }
        std::vector<std::string> names;
        for (const auto& item : projection.select_items_) {
            names.push_back(!item.output_name.empty()         ? item.output_name
                            : !item.alias.empty()             ? item.alias
                            : !item.expr.display_name.empty() ? item.expr.display_name
                                                              : item.expr.col.col_name);
        }
        return names;
    }
    case T_Sort:
        return output_names_for(*static_cast<const SortPlan*>(&plan)->subplan_);
    case T_Limit:
        return output_names_for(*static_cast<const LimitPlan*>(&plan)->subplan_);
    case T_Aggregate: {
        const auto& aggregate = *static_cast<const AggregatePlan*>(&plan);
        std::vector<std::string> names;
        for (const auto& col : aggregate.group_by_cols_) {
            names.push_back(col.col_name);
        }
        for (const auto& expression : aggregate.agg_exprs_) {
            names.push_back(expression.display_name);
        }
        return names;
    }
    case T_Union:
        return static_cast<const UnionPlan*>(&plan)->output_names_;
    case T_SeqScan:
    case T_IndexScan:
    case T_IndexSkipScan: {
        std::vector<std::string> names;
        for (const auto& col : static_cast<const ScanPlan*>(&plan)->cols_) {
            names.push_back(col.name);
        }
        return names;
    }
    case T_NestLoop: {
        const auto& join = *static_cast<const JoinPlan*>(&plan);
        auto names = output_names_for(*join.left_);
        auto right = output_names_for(*join.right_);
        names.insert(names.end(), right.begin(), right.end());
        return names;
    }
    default:
        return {};
    }
}

std::vector<Value> bind_insert_values(SmManager& sm_manager, const DMLPlan& dml, const ParameterFrame& parameters) {
    auto table = sm_manager.db_.get_table(dml.tab_name_);
    if (dml.values_.size() != table.cols.size()) {
        throw InvalidValueCountError();
    }
    std::vector<Value> values;
    values.reserve(dml.values_.size());
    for (size_t i = 0; i < dml.values_.size(); ++i) {
        Value value = dml.values_[i].parameter_ordinal == 0
                          ? dml.values_[i]
                          : parameters.bind(dml.values_[i].parameter_ordinal, table.cols[i].type);
        if (!value.is_null && (table.cols[i].type == TYPE_STRING || table.cols[i].type == TYPE_DATETIME) &&
            value.str_val.size() > static_cast<size_t>(table.cols[i].len)) {
            throw StringOverflowError();
        }
        values.push_back(std::move(value));
    }
    return values;
}

std::vector<Condition> bind_conditions(SmManager& sm_manager, const std::vector<Condition>& conditions,
                                       const ParameterFrame& parameters) {
    auto bound = conditions;
    for (auto& condition : bound) {
        if (condition.is_rhs_val && condition.rhs_val.parameter_ordinal != 0) {
            const auto column =
                sm_manager.db_.get_table(condition.lhs_col.tab_name).get_col(condition.lhs_col.col_name);
            condition.rhs_val = parameters.bind(condition.rhs_val.parameter_ordinal, condition.rhs_val.type);
            if (!condition.rhs_val.is_null) {
                condition.rhs_val.init_raw(column->len);
            }
        }
    }
    return bound;
}

std::vector<SetClause> bind_set_clauses(SmManager& sm_manager, const DMLPlan& dml, const ParameterFrame& parameters) {
    auto table = sm_manager.db_.get_table(dml.tab_name_);
    auto clauses = dml.set_clauses_;
    for (auto& clause : clauses) {
        const auto column = table.get_col(clause.lhs.col_name);
        if (clause.rhs.parameter_ordinal != 0) {
            clause.rhs = parameters.bind(clause.rhs.parameter_ordinal, clause.rhs.type);
        }
        for (auto& term : clause.additional_terms) {
            if (term.rhs.parameter_ordinal != 0) {
                term.rhs = parameters.bind(term.rhs.parameter_ordinal, term.rhs.type);
            }
        }
        const auto validate = [&](const Value& value) {
            if (!value.is_null && (column->type == TYPE_STRING || column->type == TYPE_DATETIME) &&
                value.str_val.size() > static_cast<size_t>(column->len)) {
                throw StringOverflowError();
            }
        };
        validate(clause.rhs);
        for (const auto& term : clause.additional_terms) {
            validate(term.rhs);
        }
    }
    return clauses;
}

void reset_runtime_rows(Plan* plan) {
    if (plan == nullptr) {
        return;
    }
    plan->runtime_rows_ = 0;
    if (plan->tag == T_Filter) {
        reset_runtime_rows(static_cast<FilterPlan*>(plan)->subplan_.get());
    } else if (plan->tag == T_Projection) {
        reset_runtime_rows(static_cast<ProjectionPlan*>(plan)->subplan_.get());
    } else if (plan->tag == T_NestLoop) {
        auto* join = static_cast<JoinPlan*>(plan);
        reset_runtime_rows(join->left_.get());
        reset_runtime_rows(join->right_.get());
    }
}

std::string join_explain(std::vector<std::string> values) {
    std::sort(values.begin(), values.end());
    std::ostringstream out;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i != 0)
            out << ", ";
        out << values[i];
    }
    return out.str();
}

std::string explain_column(const Plan& plan, const TabCol& col) {
    const auto pos = plan.table_name_to_display_.find(col.tab_name);
    return (pos == plan.table_name_to_display_.end() ? col.tab_name : pos->second) + "." + col.col_name;
}

std::string explain_condition(const Plan& plan, const Condition& condition) {
    const auto op = [&] {
        switch (condition.op) {
        case OP_EQ:
            return "=";
        case OP_NE:
            return "<>";
        case OP_LT:
            return "<";
        case OP_GT:
            return ">";
        case OP_LE:
            return "<=";
        case OP_GE:
            return ">=";
        }
        throw InternalError("Unexpected comparison operator");
    }();
    std::string result = explain_column(plan, condition.lhs_col) + op;
    if (!condition.is_rhs_val)
        return result + explain_column(plan, condition.rhs_col);
    if (!condition.rhs_display.empty())
        return result + condition.rhs_display;
    if (condition.rhs_val.type == TYPE_INT)
        return result + std::to_string(condition.rhs_val.int_val);
    if (condition.rhs_val.type == TYPE_FLOAT) {
        std::ostringstream out;
        out << std::fixed << std::setprecision(6) << condition.rhs_val.float_val;
        auto value = out.str();
        while (value.size() > 2 && value.back() == '0' && value[value.size() - 2] != '.')
            value.pop_back();
        return result + value;
    }
    return result + "'" + condition.rhs_val.str_val + "'";
}

void collect_explain_tables(Plan* plan, std::set<std::string>& tables) {
    if (plan->tag == T_SeqScan || plan->tag == T_IndexScan || plan->tag == T_IndexSkipScan) {
        tables.insert(static_cast<ScanPlan*>(plan)->tab_name_);
    } else if (plan->tag == T_Filter) {
        collect_explain_tables(static_cast<FilterPlan*>(plan)->subplan_.get(), tables);
    } else if (plan->tag == T_Projection) {
        collect_explain_tables(static_cast<ProjectionPlan*>(plan)->subplan_.get(), tables);
    } else if (plan->tag == T_NestLoop) {
        auto* join = static_cast<JoinPlan*>(plan);
        collect_explain_tables(join->left_.get(), tables);
        collect_explain_tables(join->right_.get(), tables);
    }
}

void render_explain_plan(Plan* plan, int depth, std::ostringstream& out) {
    out << std::string(static_cast<size_t>(depth), '\t');
    if (plan->tag == T_SeqScan || plan->tag == T_IndexScan || plan->tag == T_IndexSkipScan) {
        const auto* scan = static_cast<ScanPlan*>(plan);
        const char* type = plan->tag == T_SeqScan     ? "SeqScan"
                           : plan->tag == T_IndexScan ? "IndexScan"
                                                      : "IndexSkipScan";
        out << "Scan(table=" << scan->tab_name_ << ", type=" << type;
        if (plan->tag != T_SeqScan)
            out << ", using_index=(" << scan->index_col_names_[0] << ")";
        out << ", rows=" << plan->runtime_rows_ << ")\n";
    } else if (plan->tag == T_Filter) {
        auto* filter = static_cast<FilterPlan*>(plan);
        std::vector<std::string> conditions;
        for (const auto& condition : filter->conds_)
            conditions.push_back(explain_condition(*plan, condition));
        out << "Filter(condition=[" << join_explain(std::move(conditions)) << "], rows=" << plan->runtime_rows_
            << ")\n";
        render_explain_plan(filter->subplan_.get(), depth + 1, out);
    } else if (plan->tag == T_Projection) {
        auto* projection = static_cast<ProjectionPlan*>(plan);
        std::vector<std::string> columns;
        for (const auto& item : projection->select_items_) {
            if (item.expr.type == QueryExprType::COLUMN)
                columns.push_back(explain_column(*projection, item.expr.col));
        }
        if (projection->is_select_star_)
            columns = {"*"};
        out << "Project(columns=[" << join_explain(std::move(columns)) << "], rows=" << plan->runtime_rows_ << ")\n";
        render_explain_plan(projection->subplan_.get(), depth + 1, out);
    } else if (plan->tag == T_NestLoop) {
        auto* join = static_cast<JoinPlan*>(plan);
        std::set<std::string> table_set;
        collect_explain_tables(plan, table_set);
        std::vector<std::string> tables(table_set.begin(), table_set.end());
        std::vector<std::string> conditions;
        for (const auto& condition : join->conds_)
            conditions.push_back(explain_condition(*plan, condition));
        out << "Join(tables=[" << join_explain(std::move(tables)) << "], condition=["
            << join_explain(std::move(conditions)) << "], rows=" << plan->runtime_rows_ << ")\n";
        render_explain_plan(join->left_.get(), depth + 1, out);
        render_explain_plan(join->right_.get(), depth + 1, out);
    }
}

void write_explain_output(SmManager& sm_manager, const std::string& text, ExecutionOutput* output) {
    if (output != nullptr && output->data_send != nullptr && output->offset != nullptr) {
        const int offset = *output->offset;
        const int remaining = static_cast<int>(BUFFER_LENGTH) - RECORD_COUNT_LENGTH - offset;
        if (remaining <= 0) {
            output->ellipsis = true;
        } else {
            const int written = std::min(static_cast<int>(text.size()), remaining);
            memcpy(output->data_send + offset, text.data(), static_cast<size_t>(written));
            *output->offset = offset + written;
            if (written < static_cast<int>(text.size())) {
                output->ellipsis = true;
            }
        }
    }
    const bool enabled = output != nullptr && output->output_file_enabled != nullptr ? *output->output_file_enabled
                                                                                     : sm_manager.output_file_enabled_;
    if (enabled) {
        std::fstream outfile("output.txt", std::ios::out | std::ios::app);
        outfile << text;
    }
}

} // namespace

std::pair<std::vector<std::string>, std::vector<ColMeta>> QlManager::inspect_select_plan(const Plan& plan,
                                                                                         Context* context) {
    if (plan.tag != T_select) {
        throw InternalError("prepared SELECT inspection requires a SELECT plan");
    }
    const auto& select = static_cast<const DMLPlan&>(plan);
    auto root = BuildExecutorTree(*select.subplan_, *sm_manager_, context);
    return {output_names_for(*select.subplan_), root->cols()};
}

bool QlManager::execute(std::unique_ptr<Plan> plan, txn_id_t* txn_id, Context* context, ExecutionOutput* output) {
    switch (plan->tag) {
    case T_Help:
    case T_ShowTable:
    case T_ShowIndex:
    case T_DescTable:
    case T_Transaction_begin:
    case T_Transaction_commit:
    case T_Transaction_abort:
    case T_Transaction_rollback:
    case T_StaticCheckpoint:
    case T_SetTransaction:
    case T_SetOutputFile:
    case T_LoadData:
        run_cmd_utility(plan.get(), txn_id, context, output);
        return false;
    case T_CreateTable:
    case T_DropTable:
    case T_CreateIndex:
    case T_DropIndex:
        run_mutli_query(plan.get(), context);
        return false;
    case T_select: {
        auto& dml = static_cast<DMLPlan&>(*plan);
        select_from(BuildExecutorTree(*dml.subplan_, *sm_manager_, context), output_names_for(*dml.subplan_), context,
                    output);
        return true;
    }
    case T_ExplainAnalyze: {
        auto& dml = static_cast<DMLPlan&>(*plan);
        reset_runtime_rows(dml.subplan_.get());
        auto root = BuildExecutorTree(*dml.subplan_, *sm_manager_, context, nullptr, true);
        for (root->beginTuple(); !root->is_end(); root->nextTuple()) {
        }
        std::ostringstream out;
        render_explain_plan(dml.subplan_.get(), 0, out);
        write_explain_output(*sm_manager_, out.str(), output);
        return false;
    }
    case T_Insert: {
        auto& dml = static_cast<DMLPlan&>(*plan);
        InsertExecutor(sm_manager_, dml.tab_name_, dml.values_, context).Execute();
        return false;
    }
    case T_Update: {
        auto& dml = static_cast<DMLPlan&>(*plan);
        UpdateExecutor(sm_manager_, dml.tab_name_, dml.set_clauses_, dml.conds_,
                       BuildExecutorTree(*dml.subplan_, *sm_manager_, context), dml.point_access_,
                       dml.update_execution_mode_, context)
            .Execute();
        return false;
    }
    case T_Delete: {
        auto& dml = static_cast<DMLPlan&>(*plan);
        DeleteExecutor(sm_manager_, dml.tab_name_, dml.conds_, BuildExecutorTree(*dml.subplan_, *sm_manager_, context),
                       dml.point_access_, context)
            .Execute();
        return false;
    }
    default:
        throw InternalError("unexpected plan type");
    }
}

bool QlManager::execute_prepared(const PreparedPlanDescriptor& descriptor, const ParameterFrame& parameters,
                                 Context* context, ExecutionOutput* output) {
    if (!descriptor.eligible() || descriptor.dml_plan() == nullptr ||
        descriptor.database_identity() != sm_manager_->get_database_identity_under_catalog_guard() ||
        descriptor.catalog_generation() != sm_manager_->get_catalog_generation() ||
        parameters.size() != descriptor.parameter_layout().size()) {
        throw RMDBError("prepared descriptor is invalidated or has mismatched parameters");
    }
    const auto& dml = *descriptor.dml_plan();
    switch (descriptor.statement_kind()) {
    case PreparedStatementKind::Select:
        select_from(BuildExecutorTree(*dml.subplan_, *sm_manager_, context, &parameters), descriptor.output_names(),
                    context, output);
        return true;
    case PreparedStatementKind::Insert:
        InsertExecutor(sm_manager_, dml.tab_name_, bind_insert_values(*sm_manager_, dml, parameters), context)
            .Execute();
        return false;
    case PreparedStatementKind::Update:
        UpdateExecutor(sm_manager_, dml.tab_name_, bind_set_clauses(*sm_manager_, dml, parameters),
                       bind_conditions(*sm_manager_, dml.conds_, parameters),
                       BuildExecutorTree(*dml.subplan_, *sm_manager_, context, &parameters), dml.point_access_,
                       dml.update_execution_mode_, context)
            .Execute();
        return false;
    default:
        throw InternalError("unsupported prepared statement kind");
    }
}
