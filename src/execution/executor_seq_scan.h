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

#include "execution_defs.h"
#include "execution_common.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "errors.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 使用表文件顺序遍历并输出满足条件的可见记录。
 *
 * 扫描器先按 RID 顺序寻找候选记录，再通过 GetVisibleRecord() 应用当前事务
 * 的 MVCC 可见性，最后执行谓词过滤并缓存第一条匹配记录。
 */
class SeqScanExecutor : public AbstractExecutor {
private:
    std::string tab_name_;             // 表的名称
    std::vector<Condition> conds_;     // scan的条件
    RmFileHandle* fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // scan后生成的记录的字段
    size_t len_;                       // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_; // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_; // table_iterator

    SmManager* sm_manager_;
    bool predicate_recorded_{false};
    std::unique_ptr<RmRecord> buffered_record_;

    /**
     * @brief 在 Serializable 事务中登记本次表谓词读取并检查 SSI 冲突。
     *
     * 谓词读取必须在扫描结果确定前登记，即使最终没有匹配记录，也要保留
     * 对整个条件范围的读取信息以检测幻读。
     * @throws TransactionAbortException 检测到 SSI 危险结构时抛出。
     */
    void record_predicate_read() {
        if (predicate_recorded_ || context_ == nullptr || !context_->enable_ssi_read_tracking_ ||
            context_->txn_ == nullptr || context_->txn_->get_isolation_level() != IsolationLevel::SERIALIZABLE ||
            context_->txn_mgr_ == nullptr) {
            return;
        }
        predicate_recorded_ = true;
        if (context_->txn_mgr_->RecordPredicateRead(context_->txn_, tab_name_, fed_conds_)) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::SSI_DANGER);
        }
        if (context_->txn_mgr_->CheckPredicateInvisibleWrites(context_->txn_->get_transaction_id(), tab_name_,
                                                              fed_conds_, fh_, cols_)) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::SSI_DANGER);
        }
    }

    /**
     * @brief 登记一次具体 RID 的 Serializable 读取并检查不可见写入边。
     * @param rid 已经通过可见性和谓词检查的记录标识。
     * @param force 是否忽略上下文中的普通读跟踪开关，供上层显式补登记使用。
     * @throws TransactionAbortException 形成 SSI 危险结构时抛出。
     */
    void record_tuple_read(const Rid& rid, bool force = false) {
        if (context_ == nullptr || (!force && !context_->enable_ssi_read_tracking_) || context_->txn_ == nullptr ||
            context_->txn_->get_isolation_level() != IsolationLevel::SERIALIZABLE || context_->txn_mgr_ == nullptr) {
            return;
        }
        auto* txn_mgr = context_->txn_mgr_;
        txn_id_t reader_id = context_->txn_->get_transaction_id();
        txn_mgr->RecordRead(reader_id, tab_name_, rid);

        TupleMeta meta = fh_->get_tuple_meta(rid);
        if (meta.writer_txn_id_ == reader_id || meta.writer_txn_id_ == INVALID_TXN_ID) {
            return;
        }
        bool invisible = !meta.is_committed_ || meta.commit_ts_ > context_->txn_->get_start_ts();
        if (invisible && txn_mgr->CheckInvisibleWriteEdge(reader_id, meta.writer_txn_id_)) {
            throw TransactionAbortException(reader_id, AbortReason::SSI_DANGER);
        }
    }

public:
    /**
     * @brief 创建一个表顺序扫描执行器。
     * @param sm_manager 系统管理器，用于取得表元数据和记录文件句柄。
     * @param tab_name 要扫描的表名。
     * @param conds 初始扫描条件。
     * @param context 当前执行上下文，可携带事务和锁信息。
     * @throws std::out_of_range 表句柄不存在时可能由句柄映射访问抛出。
     */
    SeqScanExecutor(SmManager* sm_manager, std::string tab_name, std::vector<Condition> conds, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta& tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }
    /**
     * @brief 获取指定 RID 对当前事务可见的记录版本。
     * @param rid 待读取的记录标识。
     * @return 可见记录副本；没有可见版本时返回 nullptr。
     */
    std::unique_ptr<RmRecord> visible_record(const Rid& rid) {
        return GetVisibleRecord(fh_, rid, context_);
    }

    /**
     * @brief 创建表迭代器并定位到第一条可见且满足所有谓词的记录。
     *
     * 函数先登记谓词读取，再从表扫描器顺序尝试 RID；不可见记录和谓词不匹配
     * 的记录都会被跳过，首条匹配记录保存在 buffered_record_ 中供 Next() 返回。
     */
    void beginTuple() override {
        record_predicate_read();
        buffered_record_.reset();
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = visible_record(rid_);
            if (rec == nullptr) {
                scan_->next();
                continue;
            }
            bool match = true;
            for (const auto& cond : fed_conds_) {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                record_tuple_read(rid_);
                buffered_record_ = std::move(rec);
                break;
            }
            scan_->next();
        }
    }
    /**
     * @brief 从当前记录之后继续寻找下一条可见且满足谓词的记录。
     *
     * 函数先丢弃上一条缓存记录并推进底层扫描器，然后重复可见性检查和条件
     * 判断，直到找到新结果或扫描器结束。
     */
    void nextTuple() override {
        buffered_record_.reset();
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = visible_record(rid_);
            if (rec == nullptr) {
                scan_->next();
                continue;
            }
            bool match = true;
            for (const auto& cond : fed_conds_) {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                record_tuple_read(rid_);
                buffered_record_ = std::move(rec);
                break;
            }
            scan_->next();
        }
    }
    /**
     * @brief 返回 beginTuple()/nextTuple() 已经定位并缓存的当前记录。
     * @return 当前记录的副本；扫描结束或没有缓存记录时返回 nullptr。
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end() || buffered_record_ == nullptr)
            return nullptr;
        return std::make_unique<RmRecord>(*buffered_record_);
    }

    /**
     * @brief 返回当前扫描位置的 RID。
     * @return 当前记录标识的引用。
     */
    Rid& rid() override {
        return rid_;
    }

    /**
     * @brief 判断底层表扫描器是否耗尽。
     * @return 扫描器已到末尾时返回 true。
     */
    bool is_end() const override {
        return scan_->is_end();
    }
    /**
     * @brief 返回执行器类型名称。
     * @return "SeqScanExecutor"。
     */
    std::string getType() override {
        return "SeqScanExecutor"; // 返回执行器的名称
    }
    /**
     * @brief 返回顺序扫描输出的列元数据。
     * @return 表的列元数据引用。
     */
    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }
    /**
     * @brief 查找表扫描输出中的目标列。
     * @param target 目标表列。
     * @return 目标列的元数据副本。
     * @throws ColumnNotFoundError 找不到目标列时抛出。
     */
    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : cols_) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.col_name);
    }
    /**
     * @brief 返回顺序扫描输出记录的长度。
     * @return 一条完整表记录占用的字节数。
     */
    size_t tupleLen() const override {
        return len_;
    }
    /**
     * @brief 返回扫描对应的表名。
     * @return 被顺序扫描的表名。
     */
    std::string scan_table_name() const override {
        return tab_name_;
    }
    /**
     * @brief 返回当前实际生效的扫描条件。
     * @return 条件副本。
     */
    std::vector<Condition> scan_conditions() const override {
        return fed_conds_;
    }
    /**
     * @brief 在当前结果已被上层消费但需要补记 SSI 读时登记当前 RID。
     */
    void record_current_read_for_ssi() override {
        if (!is_end()) {
            record_tuple_read(rid_, true);
        }
    }
};
