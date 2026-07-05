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

#include "execution/execution_defs.h"
#include "execution/execution_manager.h"
#include "record/rm.h"
#include "system/sm_meta.h"
#include "system/schema_manager.h"
#include "common/context.h"
#include "plan.h"
#include "parser/parser.h"
#include "common/common.h"
#include "analyze/analyze.h"

namespace rmdb::optimizer {
class Planner {
private:
    SchemaManager* schema_manager_;
    Catalog* catalog_;

    bool enable_nestedloop_join = true;
    bool enable_sortmerge_join = false;

public:
    Planner(SchemaManager* schema_manager) : schema_manager_(schema_manager), catalog_(&schema_manager_->catalog()) {}

    std::unique_ptr<Plan> do_planner(std::unique_ptr<Query> query, Context* context);

    void set_enable_nestedloop_join(bool set_val) {
        enable_nestedloop_join = set_val;
    }

    void set_enable_sortmerge_join(bool set_val) {
        enable_sortmerge_join = set_val;
    }

private:
    std::unique_ptr<Query> logical_optimization(std::unique_ptr<Query> query, Context* context);
    std::unique_ptr<Plan> physical_optimization(Query* query, Context* context);

    std::unique_ptr<Plan> make_one_rel(Query* query);

    std::unique_ptr<Plan> generate_sort_plan(const Query* query, std::unique_ptr<Plan> plan);
    std::unique_ptr<Plan> generate_limit_plan(const Query* query, std::unique_ptr<Plan> plan);

    std::unique_ptr<Plan> generate_select_plan(std::unique_ptr<Query> query, Context* context);
    std::unique_ptr<Plan> generate_union_plan(std::unique_ptr<Query> query, Context* context);

    // int get_indexNo(std::string tab_name, std::vector<Condition> curr_conds);
    bool get_index_cols(std::string tab_name, std::vector<Condition>& curr_conds,
                        std::vector<std::string>& index_col_names);
    bool get_skip_scan_index_cols(std::string tab_name, std::vector<Condition>& curr_conds,
                                  std::vector<std::string>& index_col_names);
    PlanTag choose_scan_plan_tag(std::string tab_name, std::vector<Condition>& curr_conds,
                                 std::vector<std::string>& index_col_names);

    ColType interp_sv_type(rmdb::parser::ast::SvType sv_type) {
        std::map<rmdb::parser::ast::SvType, ColType> m = {{rmdb::parser::ast::SV_TYPE_INT, TYPE_INT},
                                                          {rmdb::parser::ast::SV_TYPE_FLOAT, TYPE_FLOAT},
                                                          {rmdb::parser::ast::SV_TYPE_STRING, TYPE_STRING},
                                                          {rmdb::parser::ast::SV_TYPE_DATETIME, TYPE_DATETIME}};
        return m.at(sv_type);
    }
};

} // namespace rmdb::optimizer

namespace rmdb {
using optimizer::Planner;
} // namespace rmdb
