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

#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>

#include "execution_defs.h"
#include "common/common.h"
#include "common/phase_metrics.h"
#include "index/ix.h"
#include "system/sm.h"

struct TupleView {
    const char* data = nullptr;
    uint32_t size = 0;

    explicit operator bool() const {
        return data != nullptr;
    }
};

template <typename T> class ScopedValueOverride {
public:
    ScopedValueOverride(T& value, T replacement) : value_(value), old_value_(value) {
        value_ = std::move(replacement);
    }

    ~ScopedValueOverride() {
        value_ = std::move(old_value_);
    }

    ScopedValueOverride(const ScopedValueOverride&) = delete;
    ScopedValueOverride& operator=(const ScopedValueOverride&) = delete;

private:
    T& value_;
    T old_value_;
};

class AbstractExecutor {
public:
    Rid _abstract_rid;

    Context* context_ = nullptr;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const {
        return 0;
    };

    virtual const std::vector<ColMeta>& cols() const {
        static const std::vector<ColMeta> empty_cols;
        return empty_cols;
    };

    virtual std::string getType() {
        return "AbstractExecutor";
    };

    virtual void beginTuple() {};

    virtual void nextTuple() {};

    virtual bool is_end() const {
        return true;
    };

    virtual Rid& rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    // The view is valid until the next beginTuple()/nextTuple() call.
    // Executors without a borrowed representation keep the legacy Next API.
    virtual TupleView current() const {
        return {};
    }

    virtual ColMeta get_col_offset(const TabCol& target) {
        (void)target;
        return ColMeta();
    };

    virtual void set_counting_enabled(bool enabled) {
        (void)enabled;
    }

    virtual void set_key_conditions(std::vector<Condition> /*key_conds*/) {
        // no-op default; only IndexScanExecutor overrides
    }

    virtual void set_lookup_key(const TabCol& /*target*/, const char* /*key*/, size_t /*len*/) {
        // no-op default; only index-backed scans need dynamic lookup bytes
    }

    virtual std::string scan_table_name() const {
        return "";
    }

    // Stable view used by SSI plumbing; the legacy string-returning API stays
    // available for compatibility with older executor implementations.
    virtual std::string_view scan_table_name_view() const {
        return {};
    }

    virtual std::vector<Condition> scan_conditions() const {
        return {};
    }

    virtual const std::vector<Condition>& scan_conditions_ref() const {
        static const std::vector<Condition> empty_conditions;
        return empty_conditions;
    }

    virtual void record_current_read_for_ssi() {}

    // Returns true if this executor yields rows in ascending order of `col`
    // (i.e. an index-ordered scan whose range is monotonic on `col`), so that
    // a min(col) aggregate can be answered from the first matching row alone.
    // Default: not supported.
    virtual bool provides_min_order(const TabCol& /*col*/) const {
        return false;
    }

    virtual uint64_t catalog_generation() const {
        return 0;
    }

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta>& rec_cols, const TabCol& target) const {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta& col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

protected:
    struct ColumnAddress {
        int offset{0};
        int len{0};
        ColType type{TYPE_INT};
    };

    struct ConditionAddress {
        ColumnAddress lhs;
        ColumnAddress rhs;
    };

    static ColumnAddress make_column_address(const ColMeta& col) {
        return ColumnAddress{col.offset, col.len, col.type};
    }

    std::vector<ConditionAddress> cache_condition_addresses(const std::vector<Condition>& conditions) {
        std::vector<ConditionAddress> addresses;
        addresses.reserve(conditions.size());
        for (const auto& condition : conditions) {
            ConditionAddress address;
            address.lhs = make_column_address(get_col_offset(condition.lhs_col));
            if (condition.is_rhs_val) {
                address.rhs.type = condition.rhs_val.type;
            } else {
                address.rhs = make_column_address(get_col_offset(condition.rhs_col));
            }
            addresses.push_back(address);
        }
        return addresses;
    }

    bool conditions_match(const std::vector<Condition>& conditions, const std::vector<ConditionAddress>& addresses,
                          const TupleView& tuple) const {
        phase_metrics::ScopedSample metrics_sample(phase_metrics::Phase::PREDICATE,
                                                   phase_metrics::sample_rate(phase_metrics::Phase::PREDICATE));
        if (conditions.size() != addresses.size()) {
            throw InternalError("condition address cache is out of date");
        }
        for (size_t i = 0; i < conditions.size(); ++i) {
            if (!compare(conditions[i], tuple, addresses[i])) {
                return false;
            }
        }
        return true;
    }

    bool conditions_match(const std::vector<Condition>& conditions, const std::vector<ConditionAddress>& addresses,
                          const RmRecord& rec) const {
        return conditions_match(conditions, addresses, TupleView{rec.data, static_cast<uint32_t>(rec.size)});
    }

    /**
     * @brief 比较条件cond与记录rec是否匹配
     * @param cond 条件
     * @param rec 记录
     * @return true if rec matches cond, false otherwise
     */
    bool compare(const Condition& cond, const TupleView& tuple, const ConditionAddress& address) const {
        const ColType lhs_type = address.lhs.type;
        const ColType rhs_type = cond.is_rhs_val ? cond.rhs_val.type : address.rhs.type;
        const char* lhs_data = tuple.data + address.lhs.offset;
        const char* rhs_data = cond.is_rhs_val ? nullptr : tuple.data + address.rhs.offset;
        if (can_cast(lhs_type, rhs_type) == false) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
        switch (lhs_type) {
        case TYPE_INT:
        case TYPE_FLOAT: {
            const double lhs_val = lhs_type == TYPE_INT ? static_cast<double>(read_unaligned<int>(lhs_data))
                                                        : read_unaligned<double>(lhs_data);
            double rhs_val;
            if (cond.is_rhs_val) {
                rhs_val = rhs_type == TYPE_INT ? static_cast<double>(cond.rhs_val.int_val) : cond.rhs_val.float_val;
            } else {
                rhs_val = rhs_type == TYPE_INT ? static_cast<double>(read_unaligned<int>(rhs_data))
                                               : read_unaligned<double>(rhs_data);
            }
            switch (cond.op) {
            case OP_EQ:
                return lhs_val == rhs_val;
            case OP_NE:
                return lhs_val != rhs_val;
            case OP_LT:
                return lhs_val < rhs_val;
            case OP_GT:
                return lhs_val > rhs_val;
            case OP_LE:
                return lhs_val <= rhs_val;
            case OP_GE:
                return lhs_val >= rhs_val;
            }
            break;
        }
        case TYPE_STRING:
        case TYPE_DATETIME: {
            const std::string_view lhs_val(lhs_data, strnlen(lhs_data, address.lhs.len));
            const std::string_view rhs_val = cond.is_rhs_val
                                                 ? std::string_view(cond.rhs_val.str_val)
                                                 : std::string_view(rhs_data, strnlen(rhs_data, address.rhs.len));
            switch (cond.op) {
            case OP_EQ:
                return lhs_val == rhs_val;
            case OP_NE:
                return lhs_val != rhs_val;
            case OP_LT:
                return lhs_val < rhs_val;
            case OP_GT:
                return lhs_val > rhs_val;
            case OP_LE:
                return lhs_val <= rhs_val;
            case OP_GE:
                return lhs_val >= rhs_val;
            }
        }
        }
        return false;
    }

    bool compare(const Condition& cond, const RmRecord& rec, const ConditionAddress& address) const {
        return compare(cond, TupleView{rec.data, static_cast<uint32_t>(rec.size)}, address);
    }

    bool compare(const Condition& cond, const RmRecord& rec) {
        ConditionAddress address;
        address.lhs = make_column_address(get_col_offset(cond.lhs_col));
        if (cond.is_rhs_val) {
            address.rhs.type = cond.rhs_val.type;
        } else {
            address.rhs = make_column_address(get_col_offset(cond.rhs_col));
        }
        return compare(cond, rec, address);
    }
    /**
     * @brief 判断两个列类型是否可以进行转换
     * @param lhs 左侧列类型
     * @param rhs 右侧列类型
     * @return true if can cast, false otherwise
     */
    static inline bool can_cast(const ColType& lhs, const ColType& rhs) {
        if (lhs == rhs)
            return true;
        if (lhs == TYPE_INT && rhs == TYPE_FLOAT)
            return true;
        if (lhs == TYPE_FLOAT && rhs == TYPE_INT)
            return true;
        if ((lhs == TYPE_STRING && rhs == TYPE_DATETIME) || (lhs == TYPE_DATETIME && rhs == TYPE_STRING))
            return true;
        return false;
    }
};
