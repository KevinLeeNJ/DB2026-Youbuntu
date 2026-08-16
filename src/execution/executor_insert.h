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
#include <mutex>

#include "execution_common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 将一组已分析的 Value 插入目标表，并维护索引、日志和事务状态。
 *
 * 插入流程先构造并检查完整记录，再检查 MVCC/唯一索引冲突；物理记录写入
 * 成功后逐个插入索引，任何索引失败都会回滚已经完成的索引和记录写入。
 */
class InsertExecutor : public AbstractExecutor {
private:
    TabMeta tab_;               // 表的元数据
    std::vector<Value> values_; // 需要插入的数据
    RmFileHandle* fh_;          // 表的数据文件句柄
    std::string tab_name_;      // 表名称
    Rid rid_; // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager* sm_manager_;

    /**
     * @brief 按索引列顺序从记录中拼接复合索引键。
     * @param index 索引元数据。
     * @param rec_data 记录数据首地址。
     * @return 固定长度的索引键字节数组。
     */
    static std::vector<char> make_index_key(const IndexMeta& index, const char* rec_data) {
        std::vector<char> key(index.col_tot_len);
        int offset = 0;
        for (int i = 0; i < index.col_num; ++i) {
            memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        return key;
    }

    /**
     * @brief 检查历史索引键候选是否与当前事务形成写写冲突。
     * @param index 待检查索引。
     * @param key 新记录的索引键。
     * @throws TransactionAbortException 发现 MVCC 唯一键冲突时抛出。
     */
    void check_mvcc_unique_key_conflict(const IndexMeta& index, const std::vector<char>& key) {
        if (context_ == nullptr || context_->txn_ == nullptr || context_->txn_mgr_ == nullptr) {
            return;
        }

        const std::string index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
        auto candidate_rids = sm_manager_->get_historical_index_key_rids(tab_name_, index_name, key);

        // 当前 B+ 树只反映现行键，历史键表用于补齐尚未完全清理的版本候选。
        for (const auto& existing_rid : candidate_rids) {
            if (HistoricalIndexKeyConflictsWithTxn(fh_, existing_rid, index, key, context_)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
            }
        }
    }

public:
    /**
     * @brief 创建普通 INSERT 执行器。
     * @param sm_manager 系统管理器。
     * @param tab_name 目标表名。
     * @param values 按表列顺序排列的待插入值。
     * @param context 当前执行上下文。
     * @throws InvalidValueCountError 值数量与表列数不一致时抛出。
     */
    InsertExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<Value> values, Context* context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    /**
     * @brief 执行一次插入操作。
     * @return DML 不产生上层数据行，始终返回 nullptr。
     * @throws RMDBError 插入 NULL 或其他记录构造错误时抛出。
     * @throws IndexEntryExistsError 唯一索引冲突时抛出。
     * @throws TransactionAbortException MVCC/SSI 冲突时抛出。
     *
     * 该函数依次完成记录编码、删除候选冲突检查、唯一索引预检查、带 WAL 的
     * 物理插入、索引插入及失败回滚，最后登记事务写集合和 SSI 写依赖。
     */
    std::unique_ptr<RmRecord> Next() override {
        // 第一阶段：按目标列类型编码 Value，生成完整的物理记录。
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto& col = tab_.cols[i];
            auto& val = values_[i];
            if (val.is_null) {
                throw RMDBError("INSERT cannot store NULL values");
            }
            if (col.type != val.type) {
                if (!can_cast(col.type, val.type)) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
                // 数值和字符串类型在写入前转换为目标列表示。
                if (col.type == TYPE_FLOAT && val.type == TYPE_INT) {
                    val.set_float(static_cast<double>(val.int_val));
                } else if (col.type == TYPE_INT && val.type == TYPE_FLOAT) {
                    val.set_int(static_cast<int>(val.float_val));
                } else if ((col.type == TYPE_STRING || col.type == TYPE_DATETIME) &&
                           (val.type == TYPE_STRING || val.type == TYPE_DATETIME)) {
                    val.type = col.type;
                }
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        if (context_ != nullptr && context_->txn_ != nullptr &&
            context_->txn_->get_isolation_level() != IsolationLevel::READ_COMMITTED &&
            DeletedTupleCandidatesConflictWithInsert(fh_, sm_manager_, tab_name_, rec, context_)) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
        }

        // 第二阶段：在修改物理页前完成 MVCC 删除候选及全部唯一键检查。
        std::vector<std::vector<char>> index_keys;
        index_keys.reserve(tab_.indexes.size());
        for (const auto& index : tab_.indexes) {
            auto key = make_index_key(index, rec.data);
            check_mvcc_unique_key_conflict(index, key);
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            std::vector<Rid> result;
            if (ih->get_value(key.data(), &result, context_ == nullptr ? nullptr : context_->txn_)) {
                throw IndexEntryExistsError();
            }
            index_keys.push_back(std::move(key));
        }

        // 第三阶段：持有物理锁准备记录，并先写 WAL 再完成数据页写入。
        std::unique_lock<std::mutex> physical_lock(fh_->get_physical_latch());
        auto prepared_insert = fh_->prepare_insert_record();
        bool insert_finished = false;
        try {
            rid_ = prepared_insert.rid;
            TupleMeta pending_meta;
            if (context_ != nullptr && context_->txn_ != nullptr) {
                pending_meta.writer_txn_id_ = context_->txn_->get_transaction_id();
                pending_meta.is_committed_ = false;
                pending_meta.is_deleted_ = false;
                pending_meta.version_chain_head_ = UndoLink{};
            }
            if (context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr) {
                InsertLogRecord log_record(context_->txn_->get_transaction_id(), rec, rid_, tab_name_);
                log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                context_->txn_->set_prev_lsn(lsn);
                prepared_insert.page_handle.page->set_page_lsn(lsn);
            }
            fh_->finish_insert_record(prepared_insert, rec.data,
                                      context_ != nullptr && context_->txn_ != nullptr ? &pending_meta : nullptr);
            insert_finished = true;
        } catch (...) {
            if (!insert_finished) {
                fh_->abort_prepared_insert(prepared_insert);
            }
            throw;
        }
        physical_lock.unlock();

        // 第四阶段：记录写入成功后维护索引；中途失败时按逆序删除已插入索引，
        // 再删除物理记录，恢复到插入前状态。
        std::vector<size_t> inserted_indexes;
        try {
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                ih->insert_entry(index_keys[i].data(), rid_, context_ == nullptr ? nullptr : context_->txn_);
                inserted_indexes.push_back(i);
            }
        } catch (...) {
            for (auto it = inserted_indexes.rbegin(); it != inserted_indexes.rend(); ++it) {
                auto& index = tab_.indexes[*it];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                ih->delete_entry(index_keys[*it].data(), context_ == nullptr ? nullptr : context_->txn_);
            }
            fh_->delete_record(rid_, context_);
            throw;
        }
        // 第五阶段：登记事务写记录/修改槽位，并执行 Serializable SSI 检查。
        if (context_ != nullptr && context_->txn_ != nullptr) {
            context_->txn_->append_write_record(std::make_unique<WriteRecord>(WType::INSERT_TUPLE, tab_name_, rid_));
            context_->txn_->append_modified_slot(tab_name_, rid_);

            if (context_->txn_->get_isolation_level() == IsolationLevel::SERIALIZABLE &&
                context_->txn_mgr_ != nullptr) {
                txn_id_t writer_id = context_->txn_->get_transaction_id();
                if (context_->txn_mgr_->CheckWriteAgainstReaders(writer_id, rid_, tab_name_, std::nullopt,
                                                                 std::optional<RmRecord>(rec), tab_.cols)) {
                    throw TransactionAbortException(writer_id, AbortReason::SSI_DANGER);
                }
            }
        }
        return nullptr;
    }
    /**
     * @brief 返回最近一次插入记录的 RID。
     * @return 插入位置的 RID 引用。
     */
    Rid& rid() override {
        return rid_;
    }
};

/**
 * @brief 将查询子执行器产生的记录逐行插入目标表。
 *
 * 算子负责校验目标列映射并把源字段恢复为 Value，具体的记录编码、索引维护
 * 和事务处理委托给内部的 InsertExecutor。
 */
class InsertSelectExecutor : public AbstractExecutor {
private:
    SmManager* sm_manager_;
    std::string tab_name_;
    std::vector<std::string> target_col_names_;
    std::unique_ptr<AbstractExecutor> source_;
    Context* context_;
    Rid rid_;

    /**
     * @brief 从源记录字段读取一个 Value。
     * @param record 源记录。
     * @param col 源列元数据。
     * @param is_null 源列是否为 NULL。
     * @return 转换后的 Value。
     */
    static Value read_value(const RmRecord& record, const ColMeta& col, bool is_null) {
        Value value;
        if (is_null) {
            value.set_null();
            return value;
        }
        const char* data = record.data + col.offset;
        switch (col.type) {
        case TYPE_INT:
            value.set_int(*reinterpret_cast<const int*>(data));
            break;
        case TYPE_FLOAT:
            value.set_float(*reinterpret_cast<const double*>(data));
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            value.set_str(execution_scalar::trim_string(data, col.len));
            value.type = col.type;
            break;
        }
        return value;
    }

public:
    /**
     * @brief 创建 INSERT ... SELECT 执行器并初始化源游标。
     * @param sm_manager 系统管理器。
     * @param tab_name 目标表名。
     * @param target_col_names 显式目标列列表；为空时按目标表列顺序映射。
     * @param source 查询源执行器。
     * @param context 当前执行上下文。
     */
    InsertSelectExecutor(SmManager* sm_manager, std::string tab_name, std::vector<std::string> target_col_names,
                         std::unique_ptr<AbstractExecutor> source, Context* context)
        : sm_manager_(sm_manager), tab_name_(std::move(tab_name)), target_col_names_(std::move(target_col_names)),
          source_(std::move(source)), context_(context) {
        source_->beginTuple();
    }

    /**
     * @brief 消费源查询并将每条记录插入目标表。
     * @return DML 不产生上层数据行，始终返回 nullptr。
     * @throws InvalidValueCountError 源列与目标列数量不匹配时抛出。
     * @throws ColumnNotFoundError 目标列不存在时抛出。
     */
    std::unique_ptr<RmRecord> Next() override {
        // 先校验目标列定义，避免部分插入后才发现映射错误。
        const auto& table = sm_manager_->db_.get_table(tab_name_);
        if (target_col_names_.empty() && source_->cols().size() != table.cols.size()) {
            throw InvalidValueCountError();
        }
        if (!target_col_names_.empty() &&
            (target_col_names_.size() != table.cols.size() || target_col_names_.size() != source_->cols().size())) {
            throw InvalidValueCountError();
        }
        if (!target_col_names_.empty()) {
            for (size_t i = 0; i < target_col_names_.size(); ++i) {
                for (size_t j = 0; j < i; ++j) {
                    if (target_col_names_[i] == target_col_names_[j]) {
                        throw RMDBError("Duplicate INSERT target column: " + target_col_names_[i]);
                    }
                }
                auto target = std::find_if(table.cols.begin(), table.cols.end(), [&](const ColMeta& col) {
                    return col.name == target_col_names_[i];
                });
                if (target == table.cols.end()) {
                    throw ColumnNotFoundError(target_col_names_[i]);
                }
            }
        }

        // 逐行读取源结果，按目标列名组装 Value，再交给普通 INSERT 执行器。
        for (; !source_->is_end(); source_->nextTuple()) {
            auto source_record = source_->Next();
            if (source_record == nullptr) {
                continue;
            }
            std::vector<Value> values(table.cols.size());
            for (size_t source_idx = 0; source_idx < source_->cols().size(); ++source_idx) {
                std::string target_name = target_col_names_.empty() ? table.cols[source_idx].name
                                                                     : target_col_names_[source_idx];
                auto target = std::find_if(table.cols.begin(), table.cols.end(), [&](const ColMeta& col) {
                    return col.name == target_name;
                });
                if (target == table.cols.end()) {
                    throw ColumnNotFoundError(target_name);
                }
                size_t target_idx = static_cast<size_t>(target - table.cols.begin());
                bool is_null = source_idx < source_->nulls().size() && source_->nulls()[source_idx];
                values[target_idx] = read_value(*source_record, source_->cols()[source_idx], is_null);
            }
            InsertExecutor insert(sm_manager_, tab_name_, std::move(values), context_);
            insert.Next();
            rid_ = insert.rid();
        }
        return nullptr;
    }

    /**
     * @brief 返回最近一次 INSERT 子操作产生的 RID。
     * @return 最近插入位置的 RID 引用。
     */
    Rid& rid() override {
        return rid_;
    }
};
