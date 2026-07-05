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
using namespace rmdb;
#undef private

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "gtest/gtest.h"
#include "index/ix.h"
#include "record/rm.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"

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

class PortalAggregateTest : public ::testing::Test {
protected:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    std::unique_ptr<SchemaManager> schema_manager_;
    std::unique_ptr<rmdb::access::TableWriteService> write_service_;
    std::unique_ptr<Portal> portal_;
    bool db_opened_ = false;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_manager_.get(),
                                                  ix_manager_.get());
        schema_manager_ = std::make_unique<SchemaManager>(sm_manager_.get());
        write_service_ =
            std::make_unique<rmdb::access::TableWriteService>(schema_manager_.get(), nullptr, nullptr, nullptr);
        portal_ = std::make_unique<Portal>(schema_manager_.get(), write_service_.get());
        if (sm_manager_->is_dir(TEST_DB_NAME)) {
            sm_manager_->drop_db(TEST_DB_NAME);
        }
        sm_manager_->create_db(TEST_DB_NAME);
        sm_manager_->open_db(TEST_DB_NAME);
        db_opened_ = true;
        sm_manager_->create_table("grade", {{"id", TYPE_INT, 4}, {"score", TYPE_INT, 4}}, nullptr);
    }

    void TearDown() override {
        if (db_opened_) {
            sm_manager_->close_db();
            db_opened_ = false;
        }
        if (sm_manager_->is_dir(TEST_DB_NAME)) {
            sm_manager_->drop_db(TEST_DB_NAME);
        }
    }

    std::unique_ptr<Plan> make_select_subplan(bool with_limit) {
        auto scan = std::make_unique<ScanPlan>(T_SeqScan, schema_manager_.get(), "grade", std::vector<Condition>{},
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
