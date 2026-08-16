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

#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "execution_defs.h"
#include "execution_common.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "record/rm_scan.h"
#include "system/sm.h"

/**
 * @brief 利用 B+ 树索引定位候选 RID，并输出满足 MVCC 与谓词条件的记录。
 *
 * 索引只负责缩小候选范围，真正返回记录前仍需经过 GetVisibleRecord() 和
 * fed_conds_ 的完整检查；在快照/Serializable 场景下还会补充历史索引候选。
 */
class IndexScanExecutor : public AbstractExecutor {
protected:
    /**
     * @brief 将一组已经收集好的 RID 包装成 RecScan 接口。
     *
     * 用于合并当前索引候选与历史索引候选，保持上层匹配逻辑无需区分来源。
     */
    class RidVectorScan : public RecScan {
    public:
        /**
         * @brief 创建从第一个 RID 开始的向量扫描器。
         * @param rids 待扫描的 RID 列表，所有权移动到扫描器中。
         */
        explicit RidVectorScan(std::vector<Rid> rids) : rids_(std::move(rids)) {}

        /**
         * @brief 将向量扫描位置推进一个 RID。
         */
        void next() override {
            ++position_;
        }

        /**
         * @brief 判断向量扫描是否已经到达末尾。
         * @return 当前位置超出 RID 列表时返回 true。
         */
        bool is_end() const override {
            return position_ >= rids_.size();
        }

        /**
         * @brief 返回当前向量扫描位置的 RID。
         * @return 当前 RID；调用者需先确保 is_end() 为 false。
         */
        Rid rid() const override {
            return rids_[position_];
        }

    private:
        std::vector<Rid> rids_;
        size_t position_{0};
    };

    std::string tab_name_;              // 表名称
    TabMeta tab_;                       // 表的元数据
    std::vector<Condition> conds_;      // 扫描条件
    RmFileHandle* fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // 需要读取的字段
    size_t len_;                        // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;  // 扫描条件，和conds_字段相同
    std::vector<Condition> base_conds_; // original conditions from construction, for INLJ key injection

    std::vector<std::string> index_col_names_; // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                     // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;
    bool predicate_recorded_{false};
    bool use_historical_index_candidates_{false};
    std::unique_ptr<RmRecord> buffered_record_;

    SmManager* sm_manager_;

    /**
     * @brief 在 Serializable 事务中登记索引扫描的谓词范围并检查 SSI 冲突。
     * @throws TransactionAbortException 已存在危险读写依赖或不可见写入影响谓词时抛出。
     *
     * 该登记覆盖整个谓词范围，即使 B+ 树没有返回任何候选 RID，也不能省略，
     * 否则后续插入可能绕过幻读检测。
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
     * @brief 登记当前事务对一个具体 RID 的读取并检查其不可见写入边。
     * @param rid 已通过可见性和完整谓词检查的记录标识。
     * @param force 是否忽略普通 SSI 读跟踪开关，供上层显式补登记。
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

    /**
     * @brief 保存一个索引范围端点及其开闭属性。
     */
    struct BoundValue {
        std::vector<char> data;
        bool inclusive = true;
    };

    /**
     * @brief 保存单个索引列的等值、下界和上界约束。
     */
    struct ColumnConstraint {
        std::optional<std::vector<char>> eq;
        std::optional<BoundValue> lower;
        std::optional<BoundValue> upper;
    };

    /**
     * @brief 将索引键的一列编码为该类型的最小值。
     * @param dest 目标复合键中该列的起始地址。
     * @param col 列类型和长度元数据。
     */
    static void write_min(char* dest, const ColMeta& col) {
        switch (col.type) {
        case TYPE_INT: {
            int value = std::numeric_limits<int>::min();
            memcpy(dest, &value, col.len);
            break;
        }
        case TYPE_FLOAT: {
            double value = std::numeric_limits<double>::lowest();
            memcpy(dest, &value, col.len);
            break;
        }
        case TYPE_STRING:
        case TYPE_DATETIME:
            memset(dest, 0, col.len);
            break;
        }
    }

    /**
     * @brief 将索引键的一列编码为该类型的最大值。
     * @param dest 目标复合键中该列的起始地址。
     * @param col 列类型和长度元数据。
     */
    static void write_max(char* dest, const ColMeta& col) {
        switch (col.type) {
        case TYPE_INT: {
            int value = std::numeric_limits<int>::max();
            memcpy(dest, &value, col.len);
            break;
        }
        case TYPE_FLOAT: {
            double value = std::numeric_limits<double>::max();
            memcpy(dest, &value, col.len);
            break;
        }
        case TYPE_STRING:
        case TYPE_DATETIME:
            memset(dest, 0xFF, col.len);
            break;
        }
    }

    /**
     * @brief 将 SQL 常量转换为索引列使用的定长键片段。
     * @param value 条件中的右值常量。
     * @param col 对应索引列的类型和长度。
     * @return 按列长度填充并完成数值提升/截断的键片段。
     */
    static std::vector<char> value_to_key_part(const Value& value, const ColMeta& col) {
        std::vector<char> data(col.len, 0);
        switch (col.type) {
        case TYPE_INT: {
            int converted = value.type == TYPE_FLOAT ? static_cast<int>(value.float_val) : value.int_val;
            memcpy(data.data(), &converted, col.len);
            break;
        }
        case TYPE_FLOAT: {
            double converted = value.type == TYPE_INT ? static_cast<double>(value.int_val) : value.float_val;
            memcpy(data.data(), &converted, col.len);
            break;
        }
        case TYPE_STRING:
        case TYPE_DATETIME:
            memcpy(data.data(), value.str_val.c_str(), std::min(static_cast<int>(value.str_val.size()), col.len));
            break;
        }
        return data;
    }

    /**
     * @brief 使用索引比较规则比较两个单列键片段。
     * @param lhs 左键片段。
     * @param rhs 右键片段。
     * @param col 决定比较类型和长度的列元数据。
     * @return 三路比较结果，负数/0/正数分别表示 lhs 更小/相等/更大。
     */
    static int compare_key_part(const std::vector<char>& lhs, const std::vector<char>& rhs, const ColMeta& col) {
        return ix_compare(lhs.data(), rhs.data(), col.type, col.len);
    }

    /**
     * @brief 从当前生效条件中提取可用于索引边界的列约束。
     * @return 按索引列名组织的等值/上下界约束。
     *
     * 这里只提取“本表索引列与常量”的可交换比较；不能直接形成连续索引区间
     * 的条件仍保留在 fed_conds_ 中，稍后由 advance_to_match() 做精确过滤。
     */
    std::map<std::string, ColumnConstraint> build_constraints() const {
        std::map<std::string, ColMeta> col_meta;
        for (const auto& col : index_meta_.cols) {
            col_meta[col.name] = col;
        }

        std::map<std::string, ColumnConstraint> constraints;
        for (const auto& cond : conds_) {
            if (!cond.is_rhs_val || cond.lhs_col.tab_name != tab_name_) {
                continue;
            }
            auto meta_it = col_meta.find(cond.lhs_col.col_name);
            if (meta_it == col_meta.end() || cond.op == OP_NE) {
                continue;
            }
            if (!is_swappable_comp_op(cond.op)) {
                continue;
            }
            const auto& col = meta_it->second;
            auto value = value_to_key_part(cond.rhs_val, col);
            auto& constraint = constraints[col.name];
            switch (cond.op) {
            case OP_EQ:
                constraint.eq = value;
                break;
            case OP_GT:
            case OP_GE: {
                bool inclusive = cond.op == OP_GE;
                if (!constraint.lower || compare_key_part(value, constraint.lower->data, col) > 0 ||
                    (compare_key_part(value, constraint.lower->data, col) == 0 && !inclusive)) {
                    constraint.lower = BoundValue{value, inclusive};
                }
                break;
            }
            case OP_LT:
            case OP_LE: {
                bool inclusive = cond.op == OP_LE;
                if (!constraint.upper || compare_key_part(value, constraint.upper->data, col) < 0 ||
                    (compare_key_part(value, constraint.upper->data, col) == 0 && !inclusive)) {
                    constraint.upper = BoundValue{value, inclusive};
                }
                break;
            }
            case OP_NE:
            case OP_LIKE:
            case OP_IN:
            case OP_BETWEEN:
                break;
            }
        }
        return constraints;
    }

    /**
     * @brief 判断当前事务是否需要把历史索引键对应的 RID 加入候选集合。
     * @return 在需要旧快照的隔离级别且系统存在历史索引键时返回 true。
     *
     * 当前 B+ 树可能已经删除或替换了旧索引键；历史候选只负责补齐可能漏掉的
     * RID，最终仍由 MVCC 可见性和完整谓词检查决定是否返回。
     */
    bool needs_historical_index_candidates() const {
        if (context_ == nullptr || context_->txn_ == nullptr) {
            return false;
        }
        IsolationLevel level = context_->txn_->get_isolation_level();
        if (level != IsolationLevel::SNAPSHOT_ISOLATION && level != IsolationLevel::REPEATABLE_READ &&
            !(level == IsolationLevel::SERIALIZABLE && context_->txn_->get_txn_mode())) {
            return false;
        }

        // 当前索引只有在索引键被更新/删除后才可能漏掉旧快照可见的元组；旧键由
        // SmManager 维护。非索引列更新和快照之后的插入由 GetVisibleRecord() 处理。
        const std::string index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols);
        return sm_manager_->has_historical_index_keys(tab_name_, index_name);
    }

public:
    /**
     * @brief 创建一个针对指定索引的扫描执行器。
     * @param sm_manager 系统管理器，用于访问表、索引和缓冲池。
     * @param tab_name 要扫描的表名。
     * @param conds 初始扫描条件；构造时会将可交换的跨表条件规范化到本表左侧。
     * @param index_col_names 参与索引扫描的列名顺序。
     * @param context 当前执行上下文，可携带事务和 SSI 状态。
     * @throws ColumnNotFoundError 索引列或表列不存在时可能抛出。
     */
    IndexScanExecutor(SmManager* sm_manager, std::string tab_name, std::vector<Condition> conds,
                      std::vector<std::string> index_col_names, Context* context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;

        for (auto& cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_ && is_swappable_comp_op(cond.op)) {
                // 如果条件左侧属于另一张表，则先把本表列换到左侧，便于后续统一提取索引约束。
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                std::swap(cond.lhs_col, cond.rhs_col);
                if (is_swappable_comp_op(cond.op)) {
                    cond.op = swap_comp_op(cond.op);
                }
            }
        }
        fed_conds_ = conds_;
        // 保留未注入连接键的原始条件，供 Nested Loop Join 每次重新绑定内层扫描时恢复。
        base_conds_ = conds_;
    }

    /**
     * @brief 初始化索引扫描并定位到第一条可见且满足条件的记录。
     *
     * 流程依次完成 SSI 谓词登记、历史候选判断、索引共享锁获取、复合键上下界
     * 构造、当前/历史 RID 扫描器创建，最后由 advance_to_match() 预取第一条结果。
     * 索引边界只用于缩小候选范围，所有 fed_conds_ 仍会在记录上再次验证。
     * @throws TransactionAbortException SSI 检测发现危险结构时抛出。
     */
    void beginTuple() override {
        // 谓词登记必须先于索引扫描，即使范围为空也要让 SSI 看到本次范围读取。
        record_predicate_read();

        use_historical_index_candidates_ = needs_historical_index_candidates();

        auto ih =
            sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols)).get();
        auto index_latch_guard = ih->lock_shared();
        auto constraints = build_constraints();

        // 先把复合键初始化为整个索引域，再用左前缀条件逐列收紧上下界。
        std::vector<char> lower_key(index_meta_.col_tot_len);
        std::vector<char> upper_key(index_meta_.col_tot_len);
        int offset = 0;
        for (const auto& col : index_meta_.cols) {
            write_min(lower_key.data() + offset, col);
            write_max(upper_key.data() + offset, col);
            offset += col.len;
        }

        bool lower_exclusive = false;
        bool upper_inclusive = true;
        bool saw_range = false;
        offset = 0;
        for (const auto& col : index_meta_.cols) {
            auto constraint_it = constraints.find(col.name);
            if (constraint_it == constraints.end() || saw_range) {
                break;
            }

            const auto& constraint = constraint_it->second;
            // 等值条件可以继续利用后续索引列；第一个范围条件则会终止左前缀收紧。
            if (constraint.eq.has_value()) {
                memcpy(lower_key.data() + offset, constraint.eq->data(), col.len);
                memcpy(upper_key.data() + offset, constraint.eq->data(), col.len);
                offset += col.len;
                continue;
            }

            if (constraint.lower.has_value()) {
                memcpy(lower_key.data() + offset, constraint.lower->data.data(), col.len);
                lower_exclusive = !constraint.lower->inclusive;
            }
            if (constraint.upper.has_value()) {
                memcpy(upper_key.data() + offset, constraint.upper->data.data(), col.len);
                upper_inclusive = constraint.upper->inclusive;
            }

            // 开区间需要用后缀极值把边界扩展到正确的复合键位置，避免误包含/遗漏边界键。
            int suffix_offset = offset + col.len;
            if (constraint.lower.has_value() && lower_exclusive) {
                for (size_t i = (&col - index_meta_.cols.data()) + 1; i < index_meta_.cols.size(); ++i) {
                    write_max(lower_key.data() + suffix_offset, index_meta_.cols[i]);
                    suffix_offset += index_meta_.cols[i].len;
                }
            }
            suffix_offset = offset + col.len;
            if (constraint.upper.has_value() && !upper_inclusive) {
                for (size_t i = (&col - index_meta_.cols.data()) + 1; i < index_meta_.cols.size(); ++i) {
                    write_min(upper_key.data() + suffix_offset, index_meta_.cols[i]);
                    suffix_offset += index_meta_.cols[i].len;
                }
            }
            saw_range = true;
            break;
        }

        // 精确等值范围使用 equal_range；一般范围根据开闭属性选择 bound 函数。
        Iid lower, upper;
        if (!lower_exclusive && upper_inclusive && lower_key == upper_key) {
            auto [lo, hi] = ih->equal_range(lower_key.data());
            lower = lo;
            upper = hi;
        } else {
            lower = lower_exclusive ? ih->upper_bound(lower_key.data()) : ih->lower_bound(lower_key.data());
            upper = upper_inclusive ? ih->upper_bound(upper_key.data()) : ih->lower_bound(upper_key.data());
        }
        // READ COMMITTED 通常只读当前索引，但并发写者可能先移除精确键再发布元数据，
        // 使读者无法从当前索引触达 Undo 版本。因此 RC 只额外探测精确键，不走 SI 的全量历史路径。
        bool use_rc_exact_historical_key = false;
        std::vector<Rid> rc_exact_historical_rids;
        if (!use_historical_index_candidates_ && context_ != nullptr && context_->txn_ != nullptr &&
            context_->txn_->get_isolation_level() == IsolationLevel::READ_COMMITTED && !lower_exclusive &&
            upper_inclusive && lower_key == upper_key) {
            const std::string index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols);
            rc_exact_historical_rids = sm_manager_->get_historical_index_key_rids(tab_name_, index_name, lower_key);
            use_rc_exact_historical_key = !rc_exact_historical_rids.empty();
        }

        // 需要历史候选时先物化当前索引 RID，再合并历史 RID 并去重，统一交给匹配逻辑。
        if (use_historical_index_candidates_ || use_rc_exact_historical_key) {
            std::vector<Rid> rids;
            for (IxScan index_scan(ih, lower, upper, sm_manager_->get_bpm(), std::move(index_latch_guard));
                 !index_scan.is_end(); index_scan.next()) {
                rids.push_back(index_scan.rid());
            }

            std::vector<Rid> historical_rids = std::move(rc_exact_historical_rids);
            if (use_historical_index_candidates_) {
                const std::string index_name =
                    sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols);
                historical_rids = sm_manager_->get_historical_index_rids(tab_name_, index_name);
            }
            for (const Rid& historical_rid : historical_rids) {
                if (std::find(rids.begin(), rids.end(), historical_rid) == rids.end()) {
                    rids.push_back(historical_rid);
                }
            }
            scan_ = std::make_unique<RidVectorScan>(std::move(rids));
        } else {
            scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm(), std::move(index_latch_guard));
        }
        // 扫描器建立后立即预取首条有效记录，使调用者可以直接检查 is_end()/Next()。
        advance_to_match();
    }

    /**
     * @brief 将索引扫描推进一位，并定位下一条可见且满足条件的记录。
     */
    void nextTuple() override {
        scan_->next();
        advance_to_match();
    }

    /**
     * @brief 从当前索引候选位置跳过不可见或不匹配记录，缓存下一条有效记录。
     *
     * 函数先读取当前事务可见版本，再对 fed_conds_ 做完整谓词判断；只有两者都
     * 成功时才登记记录级 SSI 读取并填充 buffered_record_。
     */
    void advance_to_match() {
        buffered_record_.reset();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            // 索引候选只代表键范围命中，必须先恢复当前事务可见版本。
            auto rec = GetVisibleRecord(fh_, rid_, context_);
            if (rec == nullptr) {
                scan_->next();
                continue;
            }
            // 对索引未能表达的条件（如复杂谓词、额外连接键）做最终精确过滤。
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
     * @brief 返回 advance_to_match() 已缓存的当前索引扫描结果。
     * @return 当前记录副本；扫描结束或没有缓存记录时返回 nullptr。
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end() || buffered_record_ == nullptr) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*buffered_record_);
    }

    /**
     * @brief 返回当前索引候选记录的 RID。
     * @return 当前 RID 引用。
     */
    Rid& rid() override {
        return rid_;
    }

    /**
     * @brief 判断底层索引或历史 RID 扫描器是否结束。
     * @return 扫描器为空或已耗尽时返回 true。
     */
    bool is_end() const override {
        return scan_ == nullptr || scan_->is_end();
    }

    /**
     * @brief 返回索引扫描输出的完整表列元数据。
     * @return 表列元数据引用。
     */
    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    /**
     * @brief 返回执行器类型名称。
     * @return "IndexScanExecutor"。
     */
    std::string getType() override {
        return "IndexScanExecutor";
    }

    /**
     * @brief 查找索引扫描输出中的目标列。
     * @param target 目标表列。
     * @return 目标列元数据副本。
     * @throws ColumnNotFoundError 找不到目标列时抛出。
     */
    ColMeta get_col_offset(const TabCol& target) override {
        return *get_col(cols_, target);
    }

    /**
     * @brief 返回索引扫描输出记录的字节长度。
     * @return 完整表记录长度。
     */
    size_t tupleLen() const override {
        return len_;
    }

    /**
     * @brief 用新的运行时连接键条件重建当前扫描条件集合。
     * @param key_conds 上层 Nested Loop Join 注入的键条件，所有权移动到内部条件列表。
     *
     * 每次调用都会从 base_conds_ 恢复原始谓词，避免多次绑定同一内层扫描时
     * 累积过期连接键；跨表条件会先规范化为本表列在左侧。
     */
    void set_key_conditions(std::vector<Condition> key_conds) override {
        conds_ = base_conds_;
        for (auto& kc : key_conds) {
            // 确保注入条件的左侧指向当前被索引扫描的表。
            if (kc.lhs_col.tab_name != tab_name_ && !kc.is_rhs_val && kc.rhs_col.tab_name == tab_name_ &&
                is_swappable_comp_op(kc.op)) {
                std::swap(kc.lhs_col, kc.rhs_col);
                if (is_swappable_comp_op(kc.op)) {
                    kc.op = swap_comp_op(kc.op);
                }
            }
            conds_.push_back(std::move(kc));
        }
        fed_conds_ = conds_;
    }

    /**
     * @brief 返回索引扫描对应的表名。
     * @return 被扫描的表名。
     */
    std::string scan_table_name() const override {
        return tab_name_;
    }

    /**
     * @brief 返回当前包含运行时键条件的有效扫描条件。
     * @return 条件副本。
     */
    std::vector<Condition> scan_conditions() const override {
        return fed_conds_;
    }
    /**
     * @brief 在当前 RID 已被上层算子确认读取时补登记 SSI 记录读。
     */
    void record_current_read_for_ssi() override {
        if (!is_end()) {
            record_tuple_read(rid_, true);
        }
    }

    /**
     * @brief 判断当前索引扫描是否能为指定列提供递增顺序。
     * @param col 要判断的聚合列。
     * @return 当列属于索引且其前置索引列都被等值条件固定时返回 true。
     *
     * 历史候选会破坏当前索引的严格顺序，因此启用历史候选时主动返回 false。
     */
    bool provides_min_order(const TabCol& col) const override {
        if (use_historical_index_candidates_) {
            return false;
        }
        if (!col.tab_name.empty() && col.tab_name != tab_name_) {
            return false;
        }
        // 先定位目标列在索引列序列中的位置。
        size_t col_pos = index_meta_.cols.size();
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            if (index_meta_.cols[i].name == col.col_name) {
                col_pos = i;
                break;
            }
        }
        if (col_pos == index_meta_.cols.size()) {
            return false;
        }
        // 目标列之前的每个索引列都必须有本表等值条件，否则输出不保证按目标列单调。
        for (size_t i = 0; i < col_pos; ++i) {
            const std::string& before_name = index_meta_.cols[i].name;
            bool has_eq = false;
            for (const auto& cond : fed_conds_) {
                if (cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.col_name == before_name &&
                    (cond.lhs_col.tab_name.empty() || cond.lhs_col.tab_name == tab_name_)) {
                    has_eq = true;
                    break;
                }
            }
            if (!has_eq) {
                return false;
            }
        }
        return true;
    }
};
