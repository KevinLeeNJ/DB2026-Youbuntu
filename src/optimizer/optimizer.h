/* Copyright (c) 2023 Renmin University of China
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
#include "execution/execution.h"
#include "parser/parser.h"
#include "system/sm.h"
#include "common/context.h"
#include "transaction/transaction_manager.h"
#include "planner.h"
#include "plan.h"

class Optimizer {
private:
    SmManager* sm_manager_;
    Planner* planner_;

public:
    Optimizer(SmManager* sm_manager, Planner* planner) : sm_manager_(sm_manager), planner_(planner) {}

    std::unique_ptr<Plan> plan_query(std::unique_ptr<Query> query, Context* context) {
        auto* parse = query->parse.get();
        if (parse == nullptr) {
            throw InternalError("Unexpected null AST root");
        }
        switch (parse->type) {
        case ast::AstType::Help:
            // help;
            return std::make_unique<OtherPlan>(T_Help, std::string());
        case ast::AstType::ShowTables:
            // show tables;
            return std::make_unique<OtherPlan>(T_ShowTable, std::string());
        case ast::AstType::ShowIndex: {
            auto x = static_cast<const ast::ShowIndex*>(parse);
            // show index from table;
            return std::make_unique<OtherPlan>(T_ShowIndex, x->tab_name);
        }
        case ast::AstType::DescTable: {
            auto x = static_cast<const ast::DescTable*>(parse);
            // desc table;
            return std::make_unique<OtherPlan>(T_DescTable, x->tab_name);
        }
        case ast::AstType::TxnBegin:
            // begin;
            return std::make_unique<OtherPlan>(T_Transaction_begin, std::string());
        case ast::AstType::TxnAbort:
            // abort;
            return std::make_unique<OtherPlan>(T_Transaction_abort, std::string());
        case ast::AstType::TxnCommit:
            // commit;
            return std::make_unique<OtherPlan>(T_Transaction_commit, std::string());
        case ast::AstType::TxnRollback:
            // rollback;
            return std::make_unique<OtherPlan>(T_Transaction_rollback, std::string());
        case ast::AstType::SetStmt: {
            auto x = static_cast<const ast::SetStmt*>(parse);
            // Set Knob Plan
            return std::make_unique<SetKnobPlan>(x->set_knob_type_, x->bool_val_);
        }
        case ast::AstType::SetTransaction: {
            auto x = static_cast<const ast::SetTransaction*>(parse);
            return std::make_unique<SetTransactionPlan>(x->isolation_level_);
        }
        case ast::AstType::StaticCheckpoint:
            return std::make_unique<OtherPlan>(T_StaticCheckpoint, std::string());
        default:
            return planner_->do_planner(std::move(query), context);
        }
    }
};
