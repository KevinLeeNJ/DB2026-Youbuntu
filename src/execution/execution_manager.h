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
#include "system/schema_manager.h"
#include "common/context.h"
#include "common/common.h"
#include "optimizer/plan.h"
#include "executor_abstract.h"
#include "transaction/transaction_manager.h"
#include "access/load_data_service.h"

namespace rmdb::optimizer {
class Planner;
}

namespace rmdb {
using optimizer::Planner;
}

namespace rmdb::exec {

class QlManager {
private:
    SchemaManager* schema_manager_;
    TransactionManager* txn_mgr_;
    Planner* planner_;
    rmdb::access::LoadDataService* load_data_service_;

public:
    QlManager(SchemaManager* schema_manager, TransactionManager* txn_mgr, Planner* planner,
              rmdb::access::LoadDataService* load_data_service)
        : schema_manager_(schema_manager), txn_mgr_(txn_mgr), planner_(planner), load_data_service_(load_data_service) {
    }

    void run_mutli_query(Plan* plan, Context* context);
    void run_cmd_utility(Plan* plan, txn_id_t* txn_id, Context* context);
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<std::string> output_names,
                     Context* context);
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                     Context* context);

    void run_dml(std::unique_ptr<AbstractExecutor> exec);
};

} // namespace rmdb::exec

namespace rmdb {
using exec::QlManager;
} // namespace rmdb
