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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "execution_defs.h"
#include "record/rm.h"
#include "system/sm.h"
#include "common/context.h"
#include "common/common.h"
#include "optimizer/plan.h"
#include "executor_abstract.h"
#include "transaction/transaction_manager.h"
#include "optimizer/planner.h"

class Planner;

/**
 * @brief 查询执行管理器，负责把计划交给具体执行器或系统管理模块。
 *
 * QlManager 不实现单个算子的记录处理，而是管理 DDL、事务/工具命令、SELECT
 * 结果输出以及 DML 执行的统一入口，并把执行上下文传递到执行器树。
 */
class QlManager {
private:
    SmManager* sm_manager_;
    TransactionManager* txn_mgr_;
    Planner* planner_;

public:
    /**
     * @brief 创建查询执行管理器。
     * @param sm_manager 系统管理器。
     * @param txn_mgr 事务管理器。
     * @param planner 查询规划器，用于处理运行时优化开关。
     */
    QlManager(SmManager* sm_manager, TransactionManager* txn_mgr, Planner* planner)
        : sm_manager_(sm_manager), txn_mgr_(txn_mgr), planner_(planner) {}

    /**
     * @brief 执行 CREATE/DROP TABLE 或 CREATE/DROP INDEX 等 DDL 计划。
     * @param plan DDL 计划。
     * @param context 当前执行上下文。
     */
    void run_mutli_query(Plan* plan, Context* context);

    /**
     * @brief 执行帮助、元数据、事务、配置和检查点等工具命令。
     * @param plan 工具命令计划。
     * @param txn_id 当前会话事务 ID 指针。
     * @param context 当前执行上下文。
     */
    void run_cmd_utility(Plan* plan, txn_id_t* txn_id, Context* context);

    /**
     * @brief 遍历 SELECT 执行器树并输出结果。
     * @param executorTreeRoot 执行器树根节点所有权。
     * @param output_names 输出列标题。
     * @param context 当前执行上下文。
     *
     * 结果先写入语句级缓冲区；只有整个 SELECT 成功完成后才复制到客户端缓冲区
     * 和可选的 output.txt，从而避免事务中止时留下半条结果。
     */
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<std::string> output_names,
                     Context* context);

    /**
     * @brief 使用表列列表生成标题后输出 SELECT 结果。
     * @param executorTreeRoot 执行器树根节点所有权。
     * @param sel_cols 选择列列表。
     * @param context 当前执行上下文。
     */
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                     Context* context);

    /**
     * @brief 执行 INSERT/DELETE/UPDATE 等 DML 根执行器。
     * @param exec DML 执行器所有权。
     */
    void run_dml(std::unique_ptr<AbstractExecutor> exec);
};
