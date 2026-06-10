/* Copyright (c) 2023 Renmin University of China
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
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context* context) {
    auto x = std::static_pointer_cast<DDLPlan>(plan);
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
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t* txn_id, Context* context) {
    switch (plan->tag) {
    case T_Help:
    case T_ShowTable:
    case T_ShowIndex:
    case T_DescTable:
    case T_Transaction_begin:
    case T_Transaction_commit:
    case T_Transaction_abort:
    case T_Transaction_rollback: {
        auto x = std::static_pointer_cast<OtherPlan>(plan);
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
                context->txn_ = txn_mgr_->begin(nullptr, context->log_mgr_);
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
            break;
        }
        case T_Transaction_rollback: {
            context->txn_ = txn_mgr_->get_transaction(*txn_id);
            if (context->txn_ != nullptr) {
                txn_mgr_->abort(context->txn_, context->log_mgr_);
            }
            context->txn_ = nullptr;
            break;
        }
        case T_Transaction_abort: {
            context->txn_ = txn_mgr_->get_transaction(*txn_id);
            if (context->txn_ != nullptr) {
                txn_mgr_->abort(context->txn_, context->log_mgr_);
            }
            context->txn_ = nullptr;
            break;
        }
        default:
            throw InternalError("Unexpected field type");
            break;
        }
        break;
    }
    case T_SetTransaction: {
        auto x = std::static_pointer_cast<SetTransactionPlan>(plan);
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
    case T_SetKnob: {
        auto x = std::static_pointer_cast<SetKnobPlan>(plan);
        switch (x->set_knob_type_) {
        case ast::SetKnobType::EnableNestLoop: {
            planner_->set_enable_nestedloop_join(x->bool_value_);
            break;
        }
        case ast::SetKnobType::EnableSortMerge: {
            planner_->set_enable_sortmerge_join(x->bool_value_);
            break;
        }
        default: {
            throw RMDBError("Not implemented!\n");
            break;
        }
        }
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

        struct CheckpointBlockGuard {
            TransactionManager* txn_mgr_;
            bool armed_{false};

            explicit CheckpointBlockGuard(TransactionManager* txn_mgr) : txn_mgr_(txn_mgr) {
                txn_mgr_->block_new_transactions_for_checkpoint();
                armed_ = true;
            }

            ~CheckpointBlockGuard() {
                if (armed_) {
                    txn_mgr_->unblock_new_transactions_after_checkpoint();
                }
            }
        } checkpoint_guard(txn_mgr_);

        auto active_txns = txn_mgr_->wait_active_transactions_drained_for_checkpoint();
        if (context != nullptr && context->log_mgr_ != nullptr) {
            context->log_mgr_->flush_log_to_disk_with_sync();
            CheckpointLogRecord checkpoint(active_txns);
            int checkpoint_offset = context->log_mgr_->current_log_offset();
            context->log_mgr_->add_log_to_buffer(&checkpoint);
            context->log_mgr_->flush_log_to_disk_with_sync();
            sm_manager_->flush_all_table_and_index_pages();
            sm_manager_->flush_meta();
            context->log_mgr_->write_restart_offset(checkpoint_offset);
        }
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
    const auto& result_cols = executorTreeRoot->cols();
    std::vector<std::string> captions = std::move(output_names);
    if (captions.size() != result_cols.size()) {
        captions.clear();
        captions.reserve(result_cols.size());
        for (const auto& col : result_cols) {
            captions.push_back(col.name);
        }
    }

    // Print records
    size_t num_rec = 0;
    std::vector<char> local_send(BUFFER_LENGTH, 0);
    int local_offset = 0;
    Context print_context(context->lock_mgr_, context->log_mgr_, context->txn_, local_send.data(), &local_offset,
                          context->txn_mgr_);
    print_context.isolation_level_ = context->isolation_level_;

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

    // Print header into a statement-local buffer. It is copied to the client
    // and output.txt only after the SELECT completes without aborting.
    RecordPrinter rec_printer(captions.size());
    rec_printer.print_separator(&print_context);
    rec_printer.print_record(captions, &print_context);
    rec_printer.print_separator(&print_context);

    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        auto Tuple = executorTreeRoot->Next();
        std::vector<std::string> columns;
        columns.reserve(result_cols.size());
        for (auto& col : result_cols) {
            std::string col_str;
            char* rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT) {
                col_str = std::to_string(*(int*)rec_buf);
            } else if (col.type == TYPE_FLOAT) {
                col_str = std::to_string(*(float*)rec_buf);
            } else if (col.type == TYPE_STRING) {
                col_str = std::string((char*)rec_buf, col.len);
                col_str.resize(strlen(col_str.c_str()));
            }
            columns.push_back(col_str);
        }
        // print record into buffer
        rec_printer.print_record(columns, &print_context);
        // print record into file
        num_rec++;
    }
    // Print footer into buffer
    rec_printer.print_separator(&print_context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, &print_context);

    if (local_offset > 0) {
        memcpy(context->data_send_ + *(context->offset_), local_send.data(), local_offset);
        *(context->offset_) += local_offset;

        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile.write(local_send.data(), local_offset);
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
