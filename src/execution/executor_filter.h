/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "executor_abstract.h"
#include "executor_expr.h"
#include "transaction/transaction_manager.h"

/**
 * @brief 对子执行器产生的记录执行谓词过滤。
 *
 * 过滤器既支持旧式的 Condition 数组，也支持包含逻辑表达式、子查询和
 * CASE 的 QueryExpr。它不改变输出列布局，只缓存当前满足条件的记录。
 */
class FilterExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<Condition> conds_;
    std::shared_ptr<QueryExpr> expr_;
    QueryExprEvaluator::SubqueryRunner subquery_runner_;
    const QueryExprOuterContext* outer_context_ = nullptr;
    std::vector<ColMeta> cols_;
    size_t len_;
    std::unique_ptr<RmRecord> buffered_record_;
    bool isend_ = true;
    bool predicate_recorded_ = false;

    /**
     * @brief 判断当前过滤器是否需要登记 Serializable 下的谓词读。
     * @return 满足 SSI 读追踪条件且能确定扫描表时返回 true。
     */
    bool should_track_ssi_reads() const {
        return context_ != nullptr && context_->enable_ssi_read_tracking_ && context_->txn_ != nullptr &&
               context_->txn_->get_isolation_level() == IsolationLevel::SERIALIZABLE && context_->txn_mgr_ != nullptr &&
               !scan_table_name().empty();
    }

    /**
     * @brief 为当前过滤谓词登记一次 SSI predicate read。
     *
     * 过滤器负责整个逻辑谓词的读集合，因此只登记一次；如果事务管理器
     * 发现该读与已有写依赖形成危险结构，则立即中止事务。
     */
    void record_predicate_read() {
        if (predicate_recorded_ || !should_track_ssi_reads()) {
            return;
        }
        predicate_recorded_ = true;
        if (context_->txn_mgr_->RecordPredicateRead(context_->txn_, scan_table_name(), scan_conditions())) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::SSI_DANGER);
        }
    }

    /**
     * @brief 判断一条子执行器记录是否满足过滤条件。
     * @param rec 待判断的记录。
     * @return QueryExpr 或全部 Condition 均成立时返回 true。
     */
    bool matches(const RmRecord& rec) {
        // 新式表达式统一交给求值器处理；旧式条件仍沿用逐条件 AND 语义。
        if (expr_ != nullptr) {
            QueryExprEvaluator evaluator(prev_->cols(), prev_->nulls(), &subquery_runner_, outer_context_);
            return evaluator.matches(*expr_, rec);
        }
        for (const auto& cond : conds_) {
            if (!compare(cond, rec, prev_->nulls())) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief 从子执行器当前位置向后寻找下一条满足条件的记录。
     *
     * 每次取出子记录后先完成谓词判断；命中时缓存记录和 RID，未命中时
     * 推进子执行器。这样 Next() 只负责返回已筛选的当前记录。
     */
    void advance_to_match() {
        buffered_record_ = nullptr;
        while (!prev_->is_end()) {
            auto rec = prev_->Next();
            if (rec != nullptr && matches(*rec)) {
                buffered_record_ = std::move(rec);
                _abstract_rid = prev_->rid();
                prev_->record_current_read_for_ssi();
                isend_ = false;
                return;
            }
            prev_->nextTuple();
        }
        isend_ = true;
    }

public:
    /**
     * @brief 创建使用旧式 Condition 列表的过滤执行器。
     * @param prev 子执行器。
     * @param conds 需要同时满足的过滤条件。
     */
    FilterExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<Condition> conds) {
        prev_ = std::move(prev);
        context_ = prev_->context_;
        conds_ = std::move(conds);
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
    }

    /**
     * @brief 创建使用 QueryExpr 的过滤执行器。
     * @param prev 子执行器。
     * @param expr 过滤表达式。
     * @param subquery_runner 执行表达式内部子查询的回调。
     * @param outer_context 相关子查询访问的外层行上下文。
     */
    FilterExecutor(std::unique_ptr<AbstractExecutor> prev, std::shared_ptr<QueryExpr> expr,
                   QueryExprEvaluator::SubqueryRunner subquery_runner = {},
                   const QueryExprOuterContext* outer_context = nullptr) {
        prev_ = std::move(prev);
        context_ = prev_->context_;
        expr_ = std::move(expr);
        subquery_runner_ = std::move(subquery_runner);
        outer_context_ = outer_context;
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
    }

    /**
     * @brief 初始化子执行器并定位到第一条满足过滤条件的记录。
     *
     * 在 Serializable SSI 模式下，过滤器先登记完整谓词，再临时关闭子节点
     * 的读追踪，避免同一逻辑扫描被重复登记；子节点完成定位后恢复原设置。
     */
    void beginTuple() override {
        record_predicate_read();
        bool old_tracking = context_ != nullptr ? context_->enable_ssi_read_tracking_ : false;
        bool suppress_child_tracking = should_track_ssi_reads();
        if (context_ != nullptr && suppress_child_tracking) {
            context_->enable_ssi_read_tracking_ = false;
        }
        prev_->beginTuple();
        if (context_ != nullptr && suppress_child_tracking) {
            context_->enable_ssi_read_tracking_ = old_tracking;
        }
        advance_to_match();
    }

    /**
     * @brief 跳过当前命中记录并定位下一条满足条件的记录。
     */
    void nextTuple() override {
        bool old_tracking = context_ != nullptr ? context_->enable_ssi_read_tracking_ : false;
        bool suppress_child_tracking = should_track_ssi_reads();
        if (context_ != nullptr && suppress_child_tracking) {
            context_->enable_ssi_read_tracking_ = false;
        }
        if (!prev_->is_end()) {
            prev_->nextTuple();
        }
        if (context_ != nullptr && suppress_child_tracking) {
            context_->enable_ssi_read_tracking_ = old_tracking;
        }
        advance_to_match();
    }

    /**
     * @brief 返回当前缓存的过滤结果副本。
     * @return 当前记录的独立副本；执行结束或没有缓存记录时返回 nullptr。
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end() || buffered_record_ == nullptr) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*buffered_record_);
    }

    /**
     * @brief 返回当前过滤结果对应的 RID。
     * @return 当前抽象记录号的引用。
     */
    Rid& rid() override {
        return _abstract_rid;
    }

    /**
     * @brief 判断过滤结果是否已经耗尽。
     * @return 子执行器没有更多记录时返回 true。
     */
    bool is_end() const override {
        return isend_;
    }

    /**
     * @brief 返回执行器类型名称。
     * @return "FilterExecutor"。
     */
    std::string getType() override {
        return "FilterExecutor";
    }

    /**
     * @brief 返回过滤器输出列元数据。
     * @return 与子执行器相同的列元数据引用。
     */
    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    /**
     * @brief 返回当前记录的 NULL 标记。
     * @return 子执行器当前记录的 NULL 标记引用。
     */
    const std::vector<bool>& nulls() const override {
        return prev_->nulls();
    }

    /**
     * @brief 返回过滤器输出元组长度。
     * @return 子执行器元组长度。
     */
    size_t tupleLen() const override {
        return len_;
    }

    /**
     * @brief 查找输出列的元数据及其偏移。
     * @param target 目标表列。
     * @return 匹配的列元数据。
     * @throws ColumnNotFoundError 找不到目标列时抛出。
     */
    ColMeta get_col_offset(const TabCol& target) override {
        auto pos = get_col(cols_, target);
        return *pos;
    }

    /**
     * @brief 将计数开关透传给子执行器。
     * @param enabled 是否启用计数。
     */
    void set_counting_enabled(bool enabled) override {
        prev_->set_counting_enabled(enabled);
    }

    /**
     * @brief 将索引键条件透传给子执行器。
     * @param key_conds 可用于索引定位的条件。
     */
    void set_key_conditions(std::vector<Condition> key_conds) override {
        prev_->set_key_conditions(std::move(key_conds));
    }

    /**
     * @brief 返回底层扫描表名。
     * @return 子执行器报告的表名。
     */
    std::string scan_table_name() const override {
        return prev_->scan_table_name();
    }

    /**
     * @brief 汇总底层扫描条件和当前过滤条件。
     * @return 用于 SSI 谓词读登记的条件列表。
     */
    std::vector<Condition> scan_conditions() const override {
        auto conds = prev_->scan_conditions();
        conds.insert(conds.end(), conds_.begin(), conds_.end());
        return conds;
    }
};
