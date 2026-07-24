/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "execution/runtime/prepared_select_pipeline.h"

#include <array>
#include <cstdlib>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/context.h"
#include "execution/executor_insert.h"
#include "optimizer/plan.h"
#include "parser/token_stream.h"
#include "record/rm_manager.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include <gtest/gtest.h>

namespace {

class EnvironmentGuard {
public:
    explicit EnvironmentGuard(const char* name) : name_(name) {
        const char* value = std::getenv(name_);
        if (value != nullptr) {
            old_value_ = value;
        }
    }

    ~EnvironmentGuard() {
        if (old_value_.has_value()) {
            setenv(name_, old_value_->c_str(), 1);
        } else {
            unsetenv(name_);
        }
    }

private:
    const char* name_;
    std::optional<std::string> old_value_;
};

class PreparedSelectPipelineFixture : public ::testing::Test {
protected:
    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_manager_.get(),
                                                  ix_manager_.get());
        db_name_ = "prepared_select_pipeline_test_" + std::to_string(reinterpret_cast<std::uintptr_t>(this));
        if (sm_manager_->is_dir(db_name_)) {
            sm_manager_->drop_db(db_name_);
        }
        sm_manager_->create_db(db_name_);
        sm_manager_->open_db(db_name_);
        opened_ = true;
    }

    void TearDown() override {
        if (opened_) {
            sm_manager_->close_db();
            opened_ = false;
        }
        if (sm_manager_->is_dir(db_name_)) {
            sm_manager_->drop_db(db_name_);
        }
    }

    void create_warehouse() {
        sm_manager_->create_table("warehouse", {{"w_id", TYPE_INT, 4}, {"name", TYPE_STRING, 8}}, nullptr);
        insert_row(10, "qweruiop");
        insert_row(100, "qwerghjk");
        insert_row(500, "bgtyhnmj");
        sm_manager_->create_index("warehouse", {"w_id"}, nullptr);
    }

    void insert_row(int w_id, const std::string& name) {
        Value id;
        id.set_int(w_id);
        Value name_value;
        name_value.set_str(name);
        std::array<char, BUFFER_LENGTH> data_send{};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send.data(), &offset);
        InsertExecutor executor(sm_manager_.get(), "warehouse", {id, name_value}, &context);
        executor.Next();
    }

    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    std::string db_name_;
    bool opened_{false};
};

} // namespace

TEST(PreparedSelectPipelineTest, DisabledUnlessExplicitlyEnabled) {
    EnvironmentGuard guard("ENABLE_PREPARED_SELECT_PIPELINE");
    unsetenv("ENABLE_PREPARED_SELECT_PIPELINE");
    EXPECT_FALSE(prepared_select_pipeline::Enabled());

    setenv("ENABLE_PREPARED_SELECT_PIPELINE", "1", 1);
    EXPECT_TRUE(prepared_select_pipeline::Enabled());

    setenv("ENABLE_PREPARED_SELECT_PIPELINE", "true", 1);
    EXPECT_FALSE(prepared_select_pipeline::Enabled());
}

TEST(PreparedSelectPipelineTest, RejectsUnsupportedDescriptorShape) {
    PreparedSelectDescriptor descriptor;
    EXPECT_FALSE(prepared_select_pipeline::IsEligible(descriptor));
}

TEST_F(PreparedSelectPipelineFixture, RunsFilteredColumnProjectionForMultipleRequestsAndReusesPool) {
    EnvironmentGuard guard("ENABLE_PREPARED_SELECT_PIPELINE");
    setenv("ENABLE_PREPARED_SELECT_PIPELINE", "1", 1);
    create_warehouse();

    auto scan_condition = Condition{};
    scan_condition.lhs_col = {"warehouse", "w_id"};
    scan_condition.op = OP_GE;
    scan_condition.is_rhs_val = true;
    scan_condition.rhs_val.set_int(0);
    scan_condition.rhs_val.lexical_slot = 0;

    auto filter_condition = Condition{};
    filter_condition.lhs_col = {"warehouse", "name"};
    filter_condition.op = OP_EQ;
    filter_condition.is_rhs_val = true;
    filter_condition.rhs_val.set_str("");
    filter_condition.rhs_val.lexical_slot = 1;

    auto scan = std::make_unique<ScanPlan>(T_IndexScan, sm_manager_.get(), "warehouse",
                                           std::vector<Condition>{scan_condition}, std::vector<std::string>{"w_id"});
    auto filter = std::make_unique<FilterPlan>(T_Filter, std::move(scan), std::vector<Condition>{filter_condition});
    SelectItem item;
    item.expr.type = QueryExprType::COLUMN;
    item.expr.col = {"warehouse", "w_id"};
    item.output_name = "selected_id";
    auto projection = std::make_unique<ProjectionPlan>(T_Projection, std::move(filter), std::vector<SelectItem>{item},
                                                       std::vector<std::string>{"selected_id"});
    DMLPlan select(T_select, std::move(projection), "warehouse", {}, {}, {});
    auto descriptor = PreparedSelectDescriptor::Build(select, sm_manager_.get());
    ASSERT_NE(descriptor, nullptr);
    ASSERT_TRUE(prepared_select_pipeline::IsEligible(*descriptor));

    auto execute = [&](const std::string& sql) {
        auto lexical = parser::normalize_sql(sql, false);
        EXPECT_TRUE(lexical);
        std::array<char, BUFFER_LENGTH> data_send{};
        int offset = 0;
        Context context(nullptr, nullptr, nullptr, data_send.data(), &offset);
        const auto status = prepared_select_pipeline::Run(*descriptor, lexical, sm_manager_.get(), &context);
        EXPECT_EQ(status, prepared_select_pipeline::RunStatus::HANDLED);
        if (status != prepared_select_pipeline::RunStatus::HANDLED) {
            return std::string{};
        }
        return std::string(data_send.data(), static_cast<size_t>(offset));
    };

    const auto first = execute("select w_id from warehouse where w_id >= 10 and name = 'qweruiop';");
    EXPECT_NE(first.find("|               10 |"), std::string::npos) << first;
    EXPECT_EQ(first.find("|              100 |"), std::string::npos) << first;
    EXPECT_NE(first.find("Total record(s): 1"), std::string::npos) << first;

    const auto second = execute("select w_id from warehouse where w_id >= 100 and name = 'qwerghjk';");
    EXPECT_NE(second.find("|              100 |"), std::string::npos) << second;
    EXPECT_EQ(second.find("|               10 |"), std::string::npos) << second;
    EXPECT_NE(second.find("Total record(s): 1"), std::string::npos) << second;

    const auto pool_stats = descriptor->pool_stats();
    EXPECT_EQ(pool_stats.constructed, 1U);
    EXPECT_EQ(pool_stats.reused, 1U);
    EXPECT_EQ(pool_stats.available, 1U);
}
