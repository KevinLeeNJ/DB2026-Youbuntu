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
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

#include "execution_defs.h"
#include "execution_common.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index_scan_descriptor.h"
#include "index/ix.h"
#include "record/rm_scan.h"
#include "system/sm.h"
#ifdef RMDB_ENABLE_JIT
#include "jit/jit_predicate.h"
#endif

class IndexScanExecutor : public AbstractExecutor {
protected:
    class RidVectorScan : public RecScan {
    public:
        explicit RidVectorScan(const std::vector<Rid>* rids) : rids_(rids) {}

        void next() override {
            ++position_;
        }

        bool is_end() const override {
            return position_ >= rids_->size();
        }

        Rid rid() const override {
            return (*rids_)[position_];
        }

    private:
        const std::vector<Rid>* rids_;
        size_t position_{0};
    };

    class SingleRidScan : public RecScan {
    public:
        explicit SingleRidScan(std::optional<Rid> rid) : rid_(rid) {}

        void next() override {
            consumed_ = true;
        }

        bool is_end() const override {
            return consumed_ || !rid_.has_value();
        }

        Rid rid() const override {
            return *rid_;
        }

    private:
        std::optional<Rid> rid_;
        bool consumed_{false};
    };

    std::string tab_name_;             // 表名称
    TabMeta tab_;                      // 表的元数据
    std::vector<Condition> conds_;     // 扫描条件
    RmFileHandle* fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // 需要读取的字段
    size_t len_;                       // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_; // 扫描条件，和conds_字段相同
    std::vector<ConditionAddress> condition_addresses_;
#ifdef RMDB_ENABLE_JIT
    std::unique_ptr<jit::PredicateKernel> jit_predicate_;
#endif
    std::vector<Condition> base_conds_; // original conditions from construction, for INLJ key injection

    std::vector<std::string> index_col_names_; // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                     // index scan涉及到的索引元数据

    struct CompiledIndexCondition {
        size_t index_col_ordinal;
        CompOp op;
        Value literal;
    };

    struct BoundValue {
        std::vector<char> data;
        bool present = false;
        bool inclusive = true;
    };

    struct ColumnConstraint {
        std::vector<char> eq;
        bool eq_present = false;
        BoundValue lower;
        BoundValue upper;
    };

    std::vector<CompiledIndexCondition> compiled_index_conditions_;
    std::vector<ColumnConstraint> constraints_;
    std::string index_name_;
    IxIndexHandle* ih_ = nullptr;
    std::vector<char> lower_key_;
    std::vector<char> upper_key_;
    std::vector<char> value_key_scratch_;
    std::vector<char> lookup_key_;
    size_t lookup_key_ordinal_{std::numeric_limits<size_t>::max()};
    bool lookup_key_valid_{false};
    ScanDirection direction_{ScanDirection::Forward};
    std::vector<Rid> rid_scan_rids_;
    std::vector<Rid> historical_rids_;
    std::unordered_set<uint64_t> seen_rids_;

    Rid rid_;
    // Cursor storage belongs to the executor. Exact lookups and range scans
    // are reconstructed in place for each beginTuple() instead of allocating
    // a new polymorphic cursor on every lookup.
    std::optional<RidVectorScan> rid_vector_cursor_;
    std::optional<SingleRidScan> single_rid_cursor_;
    std::optional<IxScan> index_scan_cursor_;
    RecScan* scan_ = nullptr;
    bool predicate_recorded_{false};
    bool use_historical_index_candidates_{false};
    bool historical_candidates_merged_{false};
    RmRecordViewWithMeta buffered_tuple_;

    SmManager* sm_manager_;

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

    static void write_value_to_key_part(char* dest, const Value& value, const ColMeta& col) {
        memset(dest, 0, col.len);
        switch (col.type) {
        case TYPE_INT: {
            int converted = value.type == TYPE_FLOAT ? static_cast<int>(value.float_val) : value.int_val;
            memcpy(dest, &converted, col.len);
            break;
        }
        case TYPE_FLOAT: {
            double converted = value.type == TYPE_INT ? static_cast<double>(value.int_val) : value.float_val;
            memcpy(dest, &converted, col.len);
            break;
        }
        case TYPE_STRING:
        case TYPE_DATETIME:
            memcpy(dest, value.str_val.c_str(), std::min(static_cast<int>(value.str_val.size()), col.len));
            break;
        }
    }

    static int compare_key_part(const char* lhs, const char* rhs, const ColMeta& col) {
        return ix_compare(lhs, rhs, col.type, col.len);
    }

    size_t find_index_col_ordinal(const std::string& col_name) const {
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            if (index_meta_.cols[i].name == col_name) {
                return i;
            }
        }
        return index_meta_.cols.size();
    }

    void initialize_constraint_storage() {
        constraints_.resize(index_meta_.cols.size());
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            auto& constraint = constraints_[i];
            constraint.eq.resize(index_meta_.cols[i].len);
            constraint.lower.data.resize(index_meta_.cols[i].len);
            constraint.upper.data.resize(index_meta_.cols[i].len);
        }
        lower_key_.resize(index_meta_.col_tot_len);
        upper_key_.resize(index_meta_.col_tot_len);
        size_t max_col_len = 0;
        for (const auto& col : index_meta_.cols) {
            max_col_len = std::max(max_col_len, static_cast<size_t>(col.len));
        }
        value_key_scratch_.resize(max_col_len);
        lookup_key_.resize(max_col_len);
    }

    void compile_index_conditions() {
        compiled_index_conditions_.clear();
        compiled_index_conditions_.reserve(conds_.size());
        for (const auto& cond : conds_) {
            if (!cond.is_rhs_val || cond.lhs_col.tab_name != tab_name_ || cond.op == OP_NE) {
                continue;
            }
            size_t ordinal = find_index_col_ordinal(cond.lhs_col.col_name);
            if (ordinal == index_meta_.cols.size()) {
                continue;
            }
            compiled_index_conditions_.push_back(CompiledIndexCondition{ordinal, cond.op, cond.rhs_val});
        }
    }

#ifdef RMDB_ENABLE_JIT
    void rebuild_jit_predicate() {
        const bool exact_index_lookup = compiled_index_conditions_.size() == fed_conds_.size() &&
                                        std::all_of(constraints_.begin(), constraints_.end(),
                                                    [](const auto& constraint) { return constraint.eq_present; });
        if (exact_index_lookup || !jit::predicate_jit_available()) {
            jit_predicate_.reset();
            return;
        }
        auto predicate = std::make_unique<jit::PredicateKernel>(
            T_IndexScan, fed_conds_, jit::JitTupleLayout{static_cast<uint32_t>(len_), cols_}, std::nullopt,
            sm_manager_->get_catalog_generation(), context_);
        jit_predicate_ = *predicate ? std::move(predicate) : nullptr;
    }
#endif

    void rebuild_constraints() {
        for (auto& constraint : constraints_) {
            constraint.eq_present = false;
            constraint.lower.present = false;
            constraint.upper.present = false;
            constraint.lower.inclusive = true;
            constraint.upper.inclusive = true;
        }

        for (const auto& compiled : compiled_index_conditions_) {
            const auto& col = index_meta_.cols[compiled.index_col_ordinal];
            auto& constraint = constraints_[compiled.index_col_ordinal];
            switch (compiled.op) {
            case OP_EQ:
                write_value_to_key_part(constraint.eq.data(), compiled.literal, col);
                constraint.eq_present = true;
                break;
            case OP_GT:
            case OP_GE: {
                bool inclusive = compiled.op == OP_GE;
                write_value_to_key_part(value_key_scratch_.data(), compiled.literal, col);
                bool replace = !constraint.lower.present;
                if (!replace) {
                    int cmp = compare_key_part(value_key_scratch_.data(), constraint.lower.data.data(), col);
                    replace = cmp > 0 || (cmp == 0 && !inclusive);
                }
                if (replace) {
                    memcpy(constraint.lower.data.data(), value_key_scratch_.data(), col.len);
                    constraint.lower.present = true;
                    constraint.lower.inclusive = inclusive;
                }
                break;
            }
            case OP_LT:
            case OP_LE: {
                bool inclusive = compiled.op == OP_LE;
                write_value_to_key_part(value_key_scratch_.data(), compiled.literal, col);
                bool replace = !constraint.upper.present;
                if (!replace) {
                    int cmp = compare_key_part(value_key_scratch_.data(), constraint.upper.data.data(), col);
                    replace = cmp < 0 || (cmp == 0 && !inclusive);
                }
                if (replace) {
                    memcpy(constraint.upper.data.data(), value_key_scratch_.data(), col.len);
                    constraint.upper.present = true;
                    constraint.upper.inclusive = inclusive;
                }
                break;
            }
            case OP_NE:
                break;
            }
        }
    }

    void reset_key_bounds() {
        int offset = 0;
        for (const auto& col : index_meta_.cols) {
            write_min(lower_key_.data() + offset, col);
            write_max(upper_key_.data() + offset, col);
            offset += col.len;
        }
    }

    bool needs_historical_index_candidates() const {
        if (context_ == nullptr || context_->txn_ == nullptr) {
            return false;
        }
        IsolationLevel level = context_->txn_->get_isolation_level();
        if (level != IsolationLevel::READ_COMMITTED && level != IsolationLevel::SNAPSHOT_ISOLATION &&
            level != IsolationLevel::REPEATABLE_READ &&
            !(level == IsolationLevel::SERIALIZABLE && context_->txn_->get_txn_mode())) {
            return false;
        }

        // The current index can miss a tuple visible to an older snapshot only
        // after its indexed key was changed or deleted. Those old keys are
        // tracked by SmManager; non-index updates and post-snapshot inserts are
        // handled by GetVisibleRecord.
        return sm_manager_->has_historical_index_keys(tab_name_, index_name_);
    }

public:
    IndexScanExecutor(SmManager* sm_manager, const IndexScanDescriptor& descriptor, Context* context)
        : IndexScanExecutor(sm_manager, descriptor, descriptor.conditions(), context) {}

    IndexScanExecutor(SmManager* sm_manager, const IndexScanDescriptor& descriptor,
                      std::vector<Condition> bound_conditions, Context* context) {
        if (descriptor.catalog_generation() != sm_manager->get_catalog_generation()) {
            throw InternalError("stale index scan descriptor");
        }
        if (bound_conditions.size() != descriptor.conditions().size()) {
            throw InternalError("bound index scan condition shape mismatch");
        }

        sm_manager_ = sm_manager;
        context_ = context;
        direction_ = descriptor.direction();
        tab_name_ = descriptor.table_name();
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(bound_conditions);
        index_col_names_ = descriptor.index_column_names();
        index_meta_ = descriptor.index_meta();
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = descriptor.columns();
        len_ = descriptor.tuple_len();
        fed_conds_ = conds_;
        base_conds_ = conds_;
        index_name_ = descriptor.index_name();
        ih_ = sm_manager_->ihs_.at(index_name_).get();

        condition_addresses_.reserve(descriptor.condition_layouts().size());
        for (const auto& layout : descriptor.condition_layouts()) {
            ConditionAddress address;
            address.lhs = ColumnAddress{layout.lhs.offset, layout.lhs.len, layout.lhs.type};
            address.rhs = ColumnAddress{layout.rhs.offset, layout.rhs.len, layout.rhs.type};
            condition_addresses_.push_back(address);
        }
        compiled_index_conditions_.reserve(descriptor.compiled_index_conditions().size());
        for (const auto& condition : descriptor.compiled_index_conditions()) {
            if (condition.condition_position >= conds_.size()) {
                throw InternalError("bound index scan condition position is invalid");
            }
            compiled_index_conditions_.push_back(
                {condition.index_col_ordinal, condition.op, conds_[condition.condition_position].rhs_val});
        }

        initialize_constraint_storage();
        rid_scan_rids_.reserve(1);
        rebuild_constraints();
#ifdef RMDB_ENABLE_JIT
        rebuild_jit_predicate();
#endif
    }

    IndexScanExecutor(SmManager* sm_manager, std::string tab_name, std::vector<Condition> conds,
                      std::vector<std::string> index_col_names, Context* context,
                      ScanDirection direction = ScanDirection::Forward) {
        sm_manager_ = sm_manager;
        context_ = context;
        direction_ = direction;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;

        for (auto& cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_comp_op(cond.op);
            }
        }
        fed_conds_ = conds_;
        condition_addresses_ = cache_condition_addresses(fed_conds_);
        base_conds_ = conds_; // save original conditions before any key injection
        index_name_ = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols);
        ih_ = sm_manager_->ihs_.at(index_name_).get();
        initialize_constraint_storage();
        rid_scan_rids_.reserve(1);
        compile_index_conditions();
        rebuild_constraints();
#ifdef RMDB_ENABLE_JIT
        rebuild_jit_predicate();
#endif
    }

    void beginTuple() override {
        scan_ = nullptr;
        index_scan_cursor_.reset();
        single_rid_cursor_.reset();
        rid_vector_cursor_.reset();
        rid_scan_rids_.clear();
        historical_rids_.clear();
        seen_rids_.clear();
        historical_candidates_merged_ = false;
        record_predicate_read();

        std::optional<IxIndexHandle::SharedIndexLatch> index_latch_guard;
        const bool historical_candidates_available = needs_historical_index_candidates();
        use_historical_index_candidates_ = historical_candidates_available;

        if (lookup_key_valid_) {
            auto& lookup_constraint = constraints_[lookup_key_ordinal_];
            const auto& lookup_col = index_meta_.cols[lookup_key_ordinal_];
            memcpy(lookup_constraint.eq.data(), lookup_key_.data(), lookup_col.len);
            lookup_constraint.eq_present = true;
            lookup_constraint.lower.present = false;
            lookup_constraint.upper.present = false;
        }

        bool exact_key_lookup = !constraints_.empty();
        for (size_t ordinal = 0; ordinal < constraints_.size(); ++ordinal) {
            const auto& constraint = constraints_[ordinal];
            if (constraint.eq_present) {
                continue;
            }
            const bool closed_degenerate_range =
                constraint.lower.present && constraint.upper.present && constraint.lower.inclusive &&
                constraint.upper.inclusive &&
                compare_key_part(constraint.lower.data.data(), constraint.upper.data.data(),
                                 index_meta_.cols[ordinal]) == 0;
            if (!closed_degenerate_range) {
                exact_key_lookup = false;
                break;
            }
        }
        bool lower_exclusive = false;
        bool upper_inclusive = true;
        bool saw_range = false;
        int offset = 0;
        if (!exact_key_lookup) {
            reset_key_bounds();
        }
        for (size_t ordinal = 0; ordinal < index_meta_.cols.size(); ++ordinal) {
            const auto& col = index_meta_.cols[ordinal];
            const auto& constraint = constraints_[ordinal];
            if (exact_key_lookup) {
                const char* exact_value = constraint.eq_present ? constraint.eq.data() : constraint.lower.data.data();
                memcpy(lower_key_.data() + offset, exact_value, col.len);
                offset += col.len;
                continue;
            }
            if (saw_range) {
                break;
            }

            if (!constraint.eq_present && !constraint.lower.present && !constraint.upper.present) {
                break;
            }
            if (constraint.eq_present) {
                memcpy(lower_key_.data() + offset, constraint.eq.data(), col.len);
                memcpy(upper_key_.data() + offset, constraint.eq.data(), col.len);
                offset += col.len;
                continue;
            }

            if (constraint.lower.present) {
                memcpy(lower_key_.data() + offset, constraint.lower.data.data(), col.len);
                lower_exclusive = !constraint.lower.inclusive;
            }
            if (constraint.upper.present) {
                memcpy(upper_key_.data() + offset, constraint.upper.data.data(), col.len);
                upper_inclusive = constraint.upper.inclusive;
            }

            int suffix_offset = offset + col.len;
            if (constraint.lower.present && lower_exclusive) {
                for (size_t i = ordinal + 1; i < index_meta_.cols.size(); ++i) {
                    write_max(lower_key_.data() + suffix_offset, index_meta_.cols[i]);
                    suffix_offset += index_meta_.cols[i].len;
                }
            }
            suffix_offset = offset + col.len;
            if (constraint.upper.present && !upper_inclusive) {
                for (size_t i = ordinal + 1; i < index_meta_.cols.size(); ++i) {
                    write_min(upper_key_.data() + suffix_offset, index_meta_.cols[i]);
                    suffix_offset += index_meta_.cols[i].len;
                }
            }
            saw_range = true;
            break;
        }

        Iid lower, upper;
        if (!exact_key_lookup) {
            index_latch_guard.emplace(ih_->lock_shared());
            lower = lower_exclusive ? ih_->upper_bound(lower_key_.data()) : ih_->lower_bound(lower_key_.data());
            upper = upper_inclusive ? ih_->upper_bound(upper_key_.data()) : ih_->lower_bound(upper_key_.data());
        }
        // READ COMMITTED normally reads only the current index. A writer can
        // nevertheless remove an exact key before publishing its replacement
        // tuple meta, leaving a reader unable to reach the writer's undo
        // version. Probe that one historical key without adopting SI's broad
        // historical-index scan on the RC hot path.
        bool use_rc_exact_historical_key = false;
        if (historical_candidates_available && context_ != nullptr && context_->txn_ != nullptr &&
            context_->txn_->get_isolation_level() == IsolationLevel::READ_COMMITTED && exact_key_lookup) {
            historical_rids_ = sm_manager_->get_historical_index_key_rids(tab_name_, index_name_, lower_key_);
            use_rc_exact_historical_key = !historical_rids_.empty();
            // Exact RC probes only need history for the requested key. Range,
            // prefix and skip-scan paths retain the broad candidate set.
            use_historical_index_candidates_ = false;
        }

        const bool use_single_rid_lookup =
            exact_key_lookup && !use_historical_index_candidates_ && !use_rc_exact_historical_key;
        if (use_single_rid_lookup) {
            const auto lookup = ih_->lookup_unique(lower_key_.data());
            if (lookup.status == UniqueLookupStatus::Unique) {
                single_rid_cursor_.emplace(lookup.rid);
                scan_ = &*single_rid_cursor_;
            } else if (lookup.status == UniqueLookupStatus::NotFound) {
                single_rid_cursor_.emplace(std::nullopt);
                scan_ = &*single_rid_cursor_;
            } else {
                // LOAD may intentionally append duplicate keys to an index.
                ih_->lookup_equal(lower_key_.data(), rid_scan_rids_);
                rid_vector_cursor_.emplace(&rid_scan_rids_);
                scan_ = &*rid_vector_cursor_;
            }
        } else if (exact_key_lookup || use_historical_index_candidates_ || use_rc_exact_historical_key) {
            historical_candidates_merged_ = use_historical_index_candidates_ || use_rc_exact_historical_key;
            if (exact_key_lookup) {
                ih_->lookup_equal(lower_key_.data(), rid_scan_rids_);
            } else {
                for (IxScan index_scan(ih_, lower, upper, sm_manager_->get_bpm(), std::move(*index_latch_guard));
                     !index_scan.is_end(); index_scan.next()) {
                    rid_scan_rids_.push_back(index_scan.rid());
                }
            }

            if (use_historical_index_candidates_) {
                historical_rids_ = sm_manager_->get_historical_index_rids_in_range(
                    tab_name_, index_name_, lower_key_, exact_key_lookup ? lower_key_ : upper_key_, lower_exclusive,
                    upper_inclusive);
            }
            if (!historical_rids_.empty()) {
                seen_rids_.reserve(rid_scan_rids_.size() + historical_rids_.size());
                for (const Rid& rid : rid_scan_rids_) {
                    seen_rids_.insert((static_cast<uint64_t>(static_cast<uint32_t>(rid.page_no)) << 32) |
                                      static_cast<uint32_t>(rid.slot_no));
                }
                for (const Rid& historical_rid : historical_rids_) {
                    uint64_t rid_key = (static_cast<uint64_t>(static_cast<uint32_t>(historical_rid.page_no)) << 32) |
                                       static_cast<uint32_t>(historical_rid.slot_no);
                    if (seen_rids_.insert(rid_key).second) {
                        rid_scan_rids_.push_back(historical_rid);
                    }
                }
            }
            rid_vector_cursor_.emplace(&rid_scan_rids_);
            scan_ = &*rid_vector_cursor_;
        } else {
            index_scan_cursor_.emplace(ih_, lower, upper, sm_manager_->get_bpm(), std::move(*index_latch_guard), false,
                                       direction_);
            scan_ = &*index_scan_cursor_;
        }
        advance_to_match();
    }

    void nextTuple() override {
        scan_->next();
        advance_to_match();
    }

    void advance_to_match() {
        buffered_tuple_ = {};
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto tuple = GetVisibleTuple(fh_, rid_, context_);
            if (tuple.view.data == nullptr) {
                scan_->next();
                continue;
            }
            const TupleView view{tuple.view.data, tuple.view.size};
            const bool match = matches(view);
            if (match) {
                record_tuple_read(rid_);
                buffered_tuple_ = std::move(tuple);
                break;
            }
            scan_->next();
        }
    }

    bool matches(const TupleView& tuple) const {
#ifdef RMDB_ENABLE_JIT
        if (jit_predicate_ != nullptr) {
            auto result = jit_predicate_->evaluate(tuple.data, tuple.size);
            if (result.has_value()) {
                return *result;
            }
        }
#endif
        return conditions_match(fed_conds_, condition_addresses_, tuple);
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end() || buffered_tuple_.view.data == nullptr) {
            return nullptr;
        }
        auto result = std::make_unique<RmRecord>(static_cast<int>(buffered_tuple_.view.size));
        memcpy(result->data, buffered_tuple_.view.data, buffered_tuple_.view.size);
        return result;
    }

    TupleView current() const override {
        if (is_end() || buffered_tuple_.view.data == nullptr) {
            return {};
        }
        return TupleView{buffered_tuple_.view.data, buffered_tuple_.view.size};
    }

    Rid& rid() override {
        return rid_;
    }

    bool is_end() const override {
        return scan_ == nullptr || scan_->is_end();
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    std::string getType() override {
        return "IndexScanExecutor";
    }

    ColMeta get_col_offset(const TabCol& target) override {
        return *get_col(cols_, target);
    }

    size_t tupleLen() const override {
        return len_;
    }

    uint64_t catalog_generation() const override {
        return sm_manager_->get_catalog_generation();
    }

    void set_key_conditions(std::vector<Condition> key_conds) override {
        // Combine base filter conditions with injected key conditions
        conds_ = base_conds_;
        for (auto& kc : key_conds) {
            // Ensure lhs points to this table
            if (kc.lhs_col.tab_name != tab_name_ && !kc.is_rhs_val && kc.rhs_col.tab_name == tab_name_) {
                std::swap(kc.lhs_col, kc.rhs_col);
                kc.op = swap_comp_op(kc.op);
            }
            conds_.push_back(std::move(kc));
        }
        fed_conds_ = conds_;
        condition_addresses_ = cache_condition_addresses(fed_conds_);
        compile_index_conditions();
        rebuild_constraints();
#ifdef RMDB_ENABLE_JIT
        rebuild_jit_predicate();
#endif
        lookup_key_valid_ = false;
    }

    void set_lookup_key(const TabCol& target, const char* key, size_t len) override {
        const size_t ordinal = find_index_col_ordinal(target.col_name);
        if (ordinal == index_meta_.cols.size()) {
            throw ColumnNotFoundError(target.col_name);
        }
        const auto& col = index_meta_.cols[ordinal];
        if (len != static_cast<size_t>(col.len)) {
            throw InternalError("INLJ lookup key length does not match index column");
        }
        memcpy(lookup_key_.data(), key, len);
        lookup_key_ordinal_ = ordinal;
        lookup_key_valid_ = true;
    }

    std::string scan_table_name() const override {
        return tab_name_;
    }
    std::string_view scan_table_name_view() const override {
        return tab_name_;
    }

    std::vector<Condition> scan_conditions() const override {
        return fed_conds_;
    }
    const std::vector<Condition>& scan_conditions_ref() const override {
        return fed_conds_;
    }
    void record_current_read_for_ssi() override {
        if (!is_end()) {
            record_tuple_read(rid_, true);
        }
    }

    // An ascending index range scan yields rows ordered by the index columns.
    // A min(col) aggregate on `col` can be answered from the first visible
    // matching row when `col` is an index column and every index column before
    // it is constrained to a single equality value (so the remaining order is
    // monotonic in `col`).
    bool provides_min_order(const TabCol& col) const override {
        if (historical_candidates_merged_) {
            return false;
        }
        if (!col.tab_name.empty() && col.tab_name != tab_name_) {
            return false;
        }
        // Locate col within the index column sequence.
        size_t col_pos = index_meta_.cols.size();
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            if (index_meta_.cols[i].name == col.col_name) {
                col_pos = i;
                break;
            }
        }
        if (col_pos == index_meta_.cols.size()) {
            return false; // col not part of this index
        }
        // Every index column preceding col must have an equality predicate on
        // this table, otherwise the scan is not monotonic in col alone.
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
