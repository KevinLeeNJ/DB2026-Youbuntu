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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "execution_common.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle* fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager* sm_manager_;

public:
    UpdateExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        int record_size = fh_->get_file_hdr().record_size;

        // Initialize raw data for SetClause values (once before loop)
        for (auto& sc : set_clauses_) {
            if (sc.rhs.raw == nullptr) {
                auto col_it = get_col(tab_.cols, sc.lhs);
                auto& col = *col_it;
                if (col.type != sc.rhs.type) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(sc.rhs.type));
                }
                sc.rhs.init_raw(col.len);
            }
        }

        for (auto& rid : rids_) {
            // Read old record
            auto old_rec = fh_->get_record(rid, context_);

            // Delete old index entries
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char* old_key = extract_index_key(*old_rec, index);
                ih->delete_entry(old_key, context_->txn_);
                delete[] old_key;
            }

            // Build new record: copy old, then apply SetClauses
            auto new_rec = std::make_unique<RmRecord>(record_size);
            memcpy(new_rec->data, old_rec->data, record_size);
            for (auto& sc : set_clauses_) {
                auto col_it = get_col(tab_.cols, sc.lhs);
                auto& col = *col_it;
                memcpy(new_rec->data + col.offset, sc.rhs.raw->data, col.len);
            }

            // Insert new index entries
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char* new_key = extract_index_key(*new_rec, index);
                ih->insert_entry(new_key, rid, context_->txn_);
                delete[] new_key;
            }

            // Update heap record
            fh_->update_record(rid, new_rec->data, context_);
        }
        return nullptr;
    }

    Rid& rid() override {
        return _abstract_rid;
    }
};