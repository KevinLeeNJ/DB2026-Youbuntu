/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You may use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "optimizer/plan.h"
#include "server/output_sink.h"
#include "statement/statement_context.h"
#include "system/schema_manager.h"

namespace rmdb::access {
class TableWriteService;
class LoadDataService;
} // namespace rmdb::access

namespace rmdb::optimizer {
class Planner;
} // namespace rmdb::optimizer

namespace rmdb::exec {
class AbstractExecutor;
} // namespace rmdb::exec

namespace rmdb {
using optimizer::Plan;
using optimizer::Planner;
} // namespace rmdb

namespace rmdb::statement {

/// 命令分发器。接管旧 Portal + QlManager 的全部职责：构建执行器树、
/// 执行 SELECT/DML、处理 DDL/事务/工具/Load 语句、格式化输出。
/// 持有内核 manager 的非所有权指针，语句级状态由 StatementContext 承载。
class StatementRunner {
public:
    StatementRunner(SchemaManager* schema_manager, rmdb::access::TableWriteService* write_service, Planner* planner,
                    rmdb::access::LoadDataService* load_data_service, TransactionManager* txn_mgr)
        : schema_manager_(schema_manager), write_service_(write_service), planner_(planner),
          load_data_service_(load_data_service), txn_mgr_(txn_mgr) {}

    /// 执行一条已优化的计划。stmt_ctx 为语句级内核上下文，sink 为输出缓冲，
    /// txn_id 为连接级事务 id 指针（事务命令会读写它）。
    void run(std::unique_ptr<Plan> plan, StatementContext* stmt_ctx, OutputSink* sink, txn_id_t* txn_id);

    /// 将 DML 子计划转换为执行器树（nest_test 等需直接遍历执行器时使用）。
    std::unique_ptr<rmdb::exec::AbstractExecutor> build_executor_tree(Plan* plan, StatementContext* stmt_ctx,
                                                                      bool count_rows = false);

    // ---- 供测试/EXPLAIN 使用的静态辅助 ----

    /// 渲染 EXPLAIN ANALYZE 计划树为文本（nest_test 依赖）。
    static void render_explain_plan(Plan* plan, int depth, std::ostringstream& out);

    /// 返回计划根的输出列名（portal_test 依赖）。
    static std::vector<std::string> get_plan_output_names(Plan* plan);

    /// 聚合计划输出列名（portal_test 依赖）。
    static std::vector<std::string> build_aggregate_output_names(const rmdb::optimizer::AggregatePlan& plan);

private:
    SchemaManager* schema_manager_;
    rmdb::access::TableWriteService* write_service_;
    Planner* planner_;
    rmdb::access::LoadDataService* load_data_service_;
    TransactionManager* txn_mgr_;

    std::unique_ptr<rmdb::exec::AbstractExecutor> convert_plan_executor(Plan* plan, StatementContext* context,
                                                                        bool count_rows = false);
};

} // namespace rmdb::statement

namespace rmdb {
using statement::StatementRunner;
} // namespace rmdb
