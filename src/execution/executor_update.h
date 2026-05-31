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
#include "index/ix.h"
#include "system/sm.h"
#include <algorithm>

class UpdateExecutor : public AbstractExecutor {
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle* fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager* sm_manager_;

    static std::vector<char> make_index_key(const IndexMeta& index, const char* rec_data) {
        std::vector<char> key(index.col_tot_len);
        int offset = 0;
        for (int i = 0; i < index.col_num; ++i) {
            std::memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        return key;
    }

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
        for (Rid& rid : rids_) {
            std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);
            if (rec == nullptr) {
                continue;
            }
            auto txn = context_ == nullptr ? nullptr : context_->txn_;
            if (txn != nullptr) {
                auto meta = fh_->get_tuple_meta(rid);
                if ((meta.owner_txn_ != INVALID_TXN_ID && meta.owner_txn_ != txn->get_transaction_id()) ||
                    (meta.owner_txn_ == INVALID_TXN_ID && meta.ts_ > txn->get_start_ts())) {
                    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WRITE_CONFLICT);
                }
            }
            bool match = true;
            for (auto cond : conds_) // 判断是否匹配
            {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                auto new_rec = std::make_unique<RmRecord>(*rec); // 对原记录进行拷贝
                update_record(new_rec.get());                    // 对记录更新

                for (const auto& index : tab_.indexes) {
                    auto old_key = make_index_key(index, rec->data);
                    auto new_key = make_index_key(index, new_rec->data);
                    if (old_key == new_key) {
                        continue;
                    }
                    auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                                  .get();
                    std::vector<Rid> result;
                    if (ih->get_value(new_key.data(), &result, txn) &&
                        std::any_of(result.begin(), result.end(), [&](const Rid& found) { return found != rid; })) {
                        throw IndexEntryExistsError();
                    }
                }
                if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                    context_->txn_mgr_->SsiCheckWrite(context_->txn_, tab_name_, rid, *rec, *new_rec);
                }
                if (txn == nullptr) {
                    struct AppliedIndexChange {
                        IndexMeta index;
                        std::vector<char> old_key;
                        std::vector<char> new_key;
                    };
                    std::vector<AppliedIndexChange> applied;
                    try {
                        for (const auto& index : tab_.indexes) {
                            auto old_key = make_index_key(index, rec->data);
                            auto new_key = make_index_key(index, new_rec->data);
                            if (old_key == new_key) {
                                continue;
                            }
                            auto ih = sm_manager_->ihs_
                                          .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                                          .get();
                            ih->delete_entry(old_key.data(), nullptr);
                            ih->insert_entry(new_key.data(), rid, nullptr);
                            applied.push_back(AppliedIndexChange{index, old_key, new_key});
                        }
                    } catch (...) {
                        for (auto it = applied.rbegin(); it != applied.rend(); ++it) {
                            auto ih = sm_manager_->ihs_
                                          .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, it->index.cols))
                                          .get();
                            ih->delete_entry(it->new_key.data(), nullptr);
                            ih->insert_entry(it->old_key.data(), rid, nullptr);
                        }
                        throw;
                    }
                }
                fh_->update_record(rid, new_rec->data, context_);
                if (context_ != nullptr && context_->txn_ != nullptr) {
                    context_->txn_->append_write_record(new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *rec));
                }
            }
        }
        return nullptr;
    }

    Rid& rid() override {
        return _abstract_rid;
    }
    std::string getType() override {
        return "UpdateExecutor"; // 返回执行器的名称
    }
    /**
     * @brief 更新记录
     * @param rec 需要更新的记录
     */
    void update_record(RmRecord* rec) {
        for (const auto& set_clause : set_clauses_) {
            auto col_meta = get_col_offset(set_clause.lhs);
            char* data = rec->data + col_meta.offset;
            if (can_cast(col_meta.type, set_clause.rhs.type) == false) {
                throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(set_clause.rhs.type));
            }
            switch (col_meta.type) {
            case TYPE_INT: {
                if (set_clause.rhs.type == TYPE_INT) {
                    *(int*)data = set_clause.rhs.int_val;
                } else {
                    *(int*)data = (int)set_clause.rhs.float_val;
                }
                break;
            }
            case TYPE_FLOAT: {
                if (set_clause.rhs.type == TYPE_FLOAT) {
                    *(float*)data = set_clause.rhs.float_val;
                } else {
                    *(float*)data = (float)set_clause.rhs.int_val;
                }
                break;
            }
            case TYPE_STRING: {
                int len = get_col_offset(set_clause.lhs).len;
                std::memset(data, 0, len);
                std::memcpy(data, set_clause.rhs.str_val.c_str(), std::min(len, (int)set_clause.rhs.str_val.size()));
                break;
            }
            }
        }
    }
    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : tab_.cols) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
};
