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

#include "executor_delete.h"
#include "executor_index_scan.h"
#include "executor_insert.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_update.h"
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
                        "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
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
void QlManager::run_cmd_utility(Plan* plan, txn_id_t* txn_id, Context* context) {
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
            memcpy(context->data_send_ + *(context->offset_), help_info, strlen(help_info));
            *(context->offset_) = strlen(help_info);
            break;
        }
        case T_ShowTable: {
            sm_manager_->show_tables(context);
            break;
        }
        case T_ShowIndex: {
            sm_manager_->show_index(x->tab_name_, context);
            break;
        }
        case T_DescTable: {
            sm_manager_->desc_table(x->tab_name_, context);
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
        case T_Transaction_commit: {
            context->txn_ = txn_mgr_->get_transaction(*txn_id);
            if (context->txn_ != nullptr) {
                txn_mgr_->commit(context->txn_, context->log_mgr_);
            }
            context->txn_ = nullptr;
            *txn_id = INVALID_TXN_ID;
            break;
        }
        case T_Transaction_rollback: {
            context->txn_ = txn_mgr_->get_transaction(*txn_id);
            if (context->txn_ != nullptr) {
                txn_mgr_->abort(context->txn_, context->log_mgr_);
            }
            context->txn_ = nullptr;
            *txn_id = INVALID_TXN_ID;
            break;
        }
        case T_Transaction_abort: {
            context->txn_ = txn_mgr_->get_transaction(*txn_id);
            if (context->txn_ != nullptr) {
                txn_mgr_->abort(context->txn_, context->log_mgr_);
            }
            context->txn_ = nullptr;
            *txn_id = INVALID_TXN_ID;
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
        if (context != nullptr && context->output_file_enabled_ != nullptr) {
            *context->output_file_enabled_ = x->enable_;
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
                            Context* context) {
    select_from(*executorTreeRoot, output_names, context);
}

void QlManager::select_from(AbstractExecutor& executorTreeRoot, const std::vector<std::string>& output_names,
                            Context* context) {
    const auto& result_cols = executorTreeRoot.cols();
    const std::vector<std::string>* captions = &output_names;
    std::vector<std::string> fallback_captions;
    if (captions->size() != result_cols.size()) {
        fallback_captions.reserve(result_cols.size());
        for (const auto& col : result_cols) {
            fallback_captions.push_back(col.name);
        }
        captions = &fallback_captions;
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

    if (context != nullptr && context->result_sink_ != nullptr) {
        context->result_sink_->begin_query(result_cols, *captions);
        for (executorTreeRoot.beginTuple(); !executorTreeRoot.is_end(); executorTreeRoot.nextTuple()) {
            TupleView tuple = executorTreeRoot.current();
            std::unique_ptr<RmRecord> fallback;
            if (!tuple) {
                fallback = executorTreeRoot.Next();
                if (fallback) {
                    tuple = TupleView{fallback->data, static_cast<uint32_t>(fallback->size)};
                }
            }
            if (!tuple) {
                throw ExecutionException("executor returned an empty tuple");
            }
            context->result_sink_->append_row(result_cols, tuple.data, tuple.size);
        }
        return;
    }

    // Print records
    size_t num_rec = 0;
    const int output_start = *context->offset_;

    // Format the result directly into the request response buffer. If execution
    // aborts, client_handler replaces the buffer with the error response.
    RecordPrinter rec_printer(captions->size());
    rec_printer.print_separator(context);
    rec_printer.print_record(*captions, context);
    rec_printer.print_separator(context);

    const bool output_file_enabled =
        context->output_file_enabled_ != nullptr ? *context->output_file_enabled_ : sm_manager_->output_file_enabled_;
    std::ostringstream out_file_stream;
    if (output_file_enabled) {
        out_file_stream << "|";
        for (const auto& cap : *captions) {
            out_file_stream << " " << cap << " |";
        }
        out_file_stream << "\n";
    }

    // 执行query_plan
    std::vector<std::string> columns;
    columns.reserve(result_cols.size());
    for (executorTreeRoot.beginTuple(); !executorTreeRoot.is_end(); executorTreeRoot.nextTuple()) {
        TupleView tuple = executorTreeRoot.current();
        std::unique_ptr<RmRecord> fallback;
        if (!tuple) {
            fallback = executorTreeRoot.Next();
            if (fallback) {
                tuple = TupleView{fallback->data, static_cast<uint32_t>(fallback->size)};
            }
        }
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
        rec_printer.print_record(columns, context);
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
    rec_printer.print_separator(context);
    // Print record count into client buffer
    RecordPrinter::print_record_count(num_rec, context);

    if (output_file_enabled && *context->offset_ > output_start) {
        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << out_file_stream.str();
        outfile.close();
    }
}

void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                            Context* context) {
    std::vector<std::string> output_names;
    output_names.reserve(sel_cols.size());
    for (const auto& sel_col : sel_cols) {
        output_names.push_back(sel_col.col_name);
    }
    select_from(std::move(executorTreeRoot), std::move(output_names), context);
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec) {
    exec->Next();
}
