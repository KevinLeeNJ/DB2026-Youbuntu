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

/**
 * @brief 分派并执行 DDL 计划。
 * @param plan DDL 计划。
 * @param context 当前执行上下文。
 */
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

/**
 * @brief 分派帮助、元数据展示、事务控制、配置和检查点命令。
 * @param plan 工具命令计划。
 * @param
 * txn_id 当前会话事务 ID 指针。
 * @param context 当前执行上下文。
 */
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
    case T_SetKnob: {
        auto* x = static_cast<SetKnobPlan*>(plan);
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
    case T_SetOutputFile: {
        auto* x = static_cast<SetOutputFilePlan*>(plan);
        // output_file 是数据库级开关，保存在 SmManager 中以跨连接生效。
        sm_manager_->output_file_enabled_ = x->enable_;
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

/**
 * @brief 遍历 SELECT 结果并写入客户端缓冲区及可选的 output.txt。
 * @param executorTreeRoot
 * SELECT
 * 执行器树根节点所有权。
 * @param output_names 输出列标题。
 * @param context 当前执行上下文。
 *

 * * 函数使用局部输出缓冲区和 SSI 读追踪保护器：执行期间发生事务中止时，
 *
 * 局部结果不会提交到外部缓冲区，也不会修改 output.txt。
 */
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

    /**
     * @brief 在 SELECT 生命周期内临时打开 SSI 读追踪的 RAII 守卫。
     */
    struct SsiReadTrackingGuard {
        Context* context_;
        bool old_value_;

        /**
         * @brief 保存旧开关并启用 SSI 读追踪。
         * @param context
         * 当前执行上下文。

         */
        explicit SsiReadTrackingGuard(Context* context) : context_(context), old_value_(false) {
            if (context_ != nullptr) {
                old_value_ = context_->enable_ssi_read_tracking_;
                context_->enable_ssi_read_tracking_ = true;
            }
        }

        /**
         * @brief 恢复构造前的 SSI 读追踪开关。
         */
        ~SsiReadTrackingGuard() {
            if (context_ != nullptr) {
                context_->enable_ssi_read_tracking_ = old_value_;
            }
        }
    } ssi_read_tracking_guard(context);

    // 表头先写入语句级缓冲区，SELECT 成功结束后再提交到客户端和 output.txt。
    RecordPrinter rec_printer(captions.size());
    rec_printer.print_separator(&print_context);
    rec_printer.print_record(captions, &print_context);
    rec_printer.print_separator(&print_context);

    std::ostringstream out_file_stream;
    out_file_stream << "|";
    for (const auto& cap : captions) {
        out_file_stream << " " << cap << " |";
    }
    out_file_stream << "\n";

    // 遍历执行器树；每条记录根据列类型和 NULL 标记转换为展示字符串。
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        auto Tuple = executorTreeRoot->Next();
        if (Tuple == nullptr) {
            continue;
        }
        const auto& nulls = executorTreeRoot->nulls();
        std::vector<std::string> columns;
        columns.reserve(result_cols.size());
        for (size_t col_idx = 0; col_idx < result_cols.size(); ++col_idx) {
            auto& col = result_cols[col_idx];
            std::string col_str;
            if (col_idx < nulls.size() && nulls[col_idx]) {
                col_str = "NULL";
            } else {
                char* rec_buf = Tuple->data + col.offset;
                if (col.type == TYPE_INT) {
                    col_str = std::to_string(*(int*)rec_buf);
                } else if (col.type == TYPE_FLOAT) {
                    col_str = std::to_string(*(double*)rec_buf);
                } else if (col.type == TYPE_STRING || col.type == TYPE_DATETIME) {
                    col_str = std::string((char*)rec_buf, col.len);
                    col_str.resize(strlen(col_str.c_str()));
                }
            }
            columns.push_back(col_str);
        }
        // 结果同时写入客户端格式缓冲区和 output.txt 的紧凑格式流。
        rec_printer.print_record(columns, &print_context);
        // print record into output.txt (compact borderless)
        out_file_stream << "|";
        for (const auto& col_str : columns) {
            out_file_stream << " " << col_str << " |";
        }
        out_file_stream << "\n";
        num_rec++;
    }
    // 只有完整遍历成功后才追加分隔线和记录计数。
    rec_printer.print_separator(&print_context);
    // Print record count into client buffer
    RecordPrinter::print_record_count(num_rec, &print_context);

    if (local_offset > 0) {
        memcpy(context->data_send_ + *(context->offset_), local_send.data(), local_offset);
        *(context->offset_) += local_offset;

        if (sm_manager_->output_file_enabled_) {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << out_file_stream.str();
            outfile.close();
        }
    }
}

/**
 * @brief 根据 TabCol 选择项生成标题并委托到字符串标题重载。
 * @param executorTreeRoot SELECT
 * 执行器树根节点所有权。
 * @param sel_cols 选择列列表。
 * @param context 当前执行上下文。

 */
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                            Context* context) {
    std::vector<std::string> output_names;
    output_names.reserve(sel_cols.size());
    for (const auto& sel_col : sel_cols) {
        output_names.push_back(sel_col.col_name);
    }
    select_from(std::move(executorTreeRoot), std::move(output_names), context);
}

/**
 * @brief 执行 DML 根执行器。
 * @param exec DML 执行器所有权。
 */
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec) {
    exec->Next();
}
