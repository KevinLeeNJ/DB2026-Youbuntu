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

#include <map>

#include "errors.h"
#include "parser/parser.h"
#include "system/sm_meta.h"
#include "transaction/transaction_manager.h"
#include "planner.h"
#include "plan.h"

namespace rmdb::optimizer {
class Optimizer {
private:
    Planner* planner_;

public:
    explicit Optimizer(Planner* planner) : planner_(planner) {}

    std::unique_ptr<Plan> plan_query(std::unique_ptr<Query> query) {
        auto* parse = query->parse.get();
        if (parse == nullptr) {
            throw InternalError("Unexpected null AST root");
        }
        switch (parse->type) {
        case rmdb::parser::ast::AstType::Help:
            // help;
            return std::make_unique<OtherPlan>(T_Help, std::string());
        case rmdb::parser::ast::AstType::ShowTables:
            // show tables;
            return std::make_unique<OtherPlan>(T_ShowTable, std::string());
        case rmdb::parser::ast::AstType::ShowIndex: {
            auto x = static_cast<const rmdb::parser::ast::ShowIndex*>(parse);
            // show index from table;
            return std::make_unique<OtherPlan>(T_ShowIndex, x->tab_name);
        }
        case rmdb::parser::ast::AstType::DescTable: {
            auto x = static_cast<const rmdb::parser::ast::DescTable*>(parse);
            // desc table;
            return std::make_unique<OtherPlan>(T_DescTable, x->tab_name);
        }
        case rmdb::parser::ast::AstType::TxnBegin:
            // begin;
            return std::make_unique<OtherPlan>(T_Transaction_begin, std::string());
        case rmdb::parser::ast::AstType::TxnAbort:
            // abort;
            return std::make_unique<OtherPlan>(T_Transaction_abort, std::string());
        case rmdb::parser::ast::AstType::TxnCommit:
            // commit;
            return std::make_unique<OtherPlan>(T_Transaction_commit, std::string());
        case rmdb::parser::ast::AstType::TxnRollback:
            // rollback;
            return std::make_unique<OtherPlan>(T_Transaction_rollback, std::string());
        case rmdb::parser::ast::AstType::SetStmt: {
            auto x = static_cast<const rmdb::parser::ast::SetStmt*>(parse);
            // Set Knob Plan
            return std::make_unique<SetKnobPlan>(x->set_knob_type_, x->bool_val_);
        }
        case rmdb::parser::ast::AstType::SetTransaction: {
            auto x = static_cast<const rmdb::parser::ast::SetTransaction*>(parse);
            return std::make_unique<SetTransactionPlan>(x->isolation_level_);
        }
        case rmdb::parser::ast::AstType::SetOutputFile: {
            auto x = static_cast<const rmdb::parser::ast::SetOutputFile*>(parse);
            return std::make_unique<SetOutputFilePlan>(x->enable_);
        }
        case rmdb::parser::ast::AstType::LoadStmt: {
            auto x = static_cast<const rmdb::parser::ast::LoadStmt*>(parse);
            return std::make_unique<LoadDataPlan>(x->file_name_, x->tab_name_);
        }
        case rmdb::parser::ast::AstType::StaticCheckpoint:
            return std::make_unique<OtherPlan>(T_StaticCheckpoint, std::string());
        default:
            return planner_->do_planner(std::move(query));
        }
    }
};

} // namespace rmdb::optimizer

namespace rmdb {
using optimizer::Optimizer;
} // namespace rmdb
