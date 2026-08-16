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
#include "execution_scalar.h"
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief 所有物理执行器的统一接口和通用条件比较实现。
 *
 * 执行器采用迭代器式接口：调用者先用 beginTuple() 定位首条结果，
 * 再通过 Next() 读取当前结果，并用 nextTuple() 推进到下一条结果。
 * 子类可以只重写自己需要的能力；默认实现表示“不支持”或“当前没有结果”。
 */
class AbstractExecutor {
public:
    Rid _abstract_rid;

    Context* context_ = nullptr;

    /**
     * @brief 通过基类指针销毁具体执行器，并释放子类资源。
     */
    virtual ~AbstractExecutor() = default;

    /**
     * @brief 返回当前执行器输出记录的定长字节数。
     * @return 输出记录的总长度；基类默认返回 0。
     */
    virtual size_t tupleLen() const {
        return 0;
    };

    /**
     * @brief 返回当前执行器输出记录的列元数据。
     * @return 按记录布局顺序排列的列元数据；基类默认返回空列表。
     */
    virtual const std::vector<ColMeta>& cols() const {
        static const std::vector<ColMeta> no_cols;
        return no_cols;
    };

    /**
     * @brief 返回执行器的诊断名称。
     * @return 执行器类型名称；基类返回 "AbstractExecutor"。
     */
    virtual std::string getType() {
        return "AbstractExecutor";
    };

    /**
     * @brief 初始化迭代状态并定位到第一条候选记录。
     *
     * 基类不持有可迭代数据，因此默认实现为空操作；具体执行器应在子类中
     * 建立扫描器、物化输入或重置缓存状态。
     */
    virtual void beginTuple() {};

    /**
     * @brief 将执行器推进到下一条候选记录。
     *
     * 基类默认不推进任何状态，具体执行器负责实现自己的迭代协议。
     */
    virtual void nextTuple() {};

    /**
     * @brief 判断当前迭代器是否已经没有更多结果。
     * @return 已结束返回 true；基类默认返回 true。
     */
    virtual bool is_end() const {
        return true;
    };

    /**
     * @brief 返回当前结果对应的记录标识。
     * @return 当前记录的 RID 引用。
     */
    virtual Rid& rid() = 0;

    /**
     * @brief 读取当前迭代位置的记录。
     * @return 当前记录的独立副本；没有当前记录时返回 nullptr。
     */
    virtual std::unique_ptr<RmRecord> Next() = 0;

    /**
     * @brief 返回当前记录的逐列 NULL 状态。
     * @return 与 cols() 对齐的 NULL 位图；普通表记录通常返回空位图，
     *         外连接或表达式执行器可以返回临时的 NULL 元数据。
     */
    virtual const std::vector<bool>& nulls() const {
        static const std::vector<bool> no_nulls;
        return no_nulls;
    }

    /**
     * @brief 查找目标列并返回其在当前记录中的布局元数据。
     * @param target 要查找的表名和列名。
     * @return 目标列的类型、长度和偏移量。
     * @throws ColumnNotFoundError 当前执行器输出中不存在目标列时抛出。
     */
    virtual ColMeta get_col_offset(const TabCol& target) {
        (void)target;
        return ColMeta();
    };

    /**
     * @brief 开启或关闭执行器的行数统计。
     * @param enabled 是否启用统计。
     *
     * 基类不需要统计，默认忽略该设置；需要统计的具体执行器自行重写。
     */
    virtual void set_counting_enabled(bool enabled) {
        (void)enabled;
    }

    /**
     * @brief 向子执行器传递运行时注入的索引键条件。
     * @param key_conds 由连接等上层算子追加的扫描条件。
     *
     * 默认实现为空操作；索引扫描器会将这些条件与原始扫描条件合并。
     */
    virtual void set_key_conditions(std::vector<Condition> /*key_conds*/) {
    }

    /**
     * @brief 返回底层扫描所对应的表名。
     * @return 表名；非扫描算子或基类默认返回空字符串。
     */
    virtual std::string scan_table_name() const {
        return "";
    }

    /**
     * @brief 返回底层扫描实际使用的条件集合。
     * @return 扫描条件副本；非扫描算子或基类默认返回空列表。
     */
    virtual std::vector<Condition> scan_conditions() const {
        return {};
    }

    /**
     * @brief 在需要时记录当前记录的 Serializable 读取。
     *
     * 基类没有具体记录可登记，因此默认为空操作；扫描器会在找到当前匹配
     * 记录后重写该接口，用于 SSI 的记录级读写依赖跟踪。
     */
    virtual void record_current_read_for_ssi() {}

    /**
     * @brief 判断输出是否按指定列单调递增。
     * @param col 要判断是否有序的列。
     * @return 如果可以利用当前输出顺序快速计算该列的 MIN，则返回 true。
     *
     * 该能力主要由满足左前缀约束的索引扫描提供，基类默认不支持。
     */
    virtual bool provides_min_order(const TabCol& /*col*/) const {
        return false;
    }

    /**
     * @brief 在执行器输出列中查找完全匹配的表名和列名。
     * @param rec_cols 要搜索的列元数据列表。
     * @param target 目标表列。
     * @return 指向匹配列的常量迭代器。
     * @throws ColumnNotFoundError 找不到目标列时抛出。
     */
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
    /**
     * @brief 从记录的原始字节中读取一个类型化标量值。
     * @param data 指向字段起始位置的记录数据指针。
     * @param col 字段类型、长度和布局元数据。
     * @return 按字段类型填充的标量值；定长字符串会去除末尾填充。
     */
    static execution_scalar::CellValue read_cell(const char* data, const ColMeta& col) {
        execution_scalar::CellValue value;
        value.type = col.type;
        switch (col.type) {
        case TYPE_INT:
            value.int_val = *reinterpret_cast<const int*>(data);
            break;
        case TYPE_FLOAT:
            value.float_val = *reinterpret_cast<const double*>(data);
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            value.str_val = execution_scalar::trim_string(data, col.len);
            break;
        }
        return value;
    }

    /**
     * @brief 判断当前记录中的目标列是否由 NULL 位图标记为 NULL。
     * @param nulls 当前记录的逐列 NULL 位图。
     * @param target 要查询的表列。
     * @return 目标列被标记为 NULL 时返回 true。
     * @throws ColumnNotFoundError 当前输出中不存在目标列时抛出。
     */
    bool is_null(const std::vector<bool>& nulls, const TabCol& target) const {
        auto pos = get_col(cols(), target);
        size_t index = static_cast<size_t>(pos - cols().begin());
        return index < nulls.size() && nulls[index];
    }

    /**
     * @brief 将三路比较结果按照 SQL 比较操作符转换为布尔结果。
     * @param op 比较操作符，只接受 EQ/NE/LT/GT/LE/GE。
     * @param cmp 左右值比较结果，负数表示左值更小，0 表示相等，正数表示更大。
     * @return 条件成立返回 true，否则返回 false。
     * @throws InternalError 收到 LIKE、IN 等非三路比较操作符时抛出。
     */
    static bool compare_order(CompOp op, int cmp) {
        switch (op) {
        case OP_EQ:
            return cmp == 0;
        case OP_NE:
            return cmp != 0;
        case OP_LT:
            return cmp < 0;
        case OP_GT:
            return cmp > 0;
        case OP_LE:
            return cmp <= 0;
        case OP_GE:
            return cmp >= 0;
        default:
            throw InternalError("Unexpected comparison operator");
        }
    }

    /**
     * @brief 在没有 NULL 位图时判断记录是否满足传统扫描条件。
     * @param cond 要判断的条件。
     * @param rec 待判断的记录。
     * @return 记录满足条件时返回 true。
     */
    bool compare(const Condition& cond, const RmRecord& rec) {
        static const std::vector<bool> no_nulls;
        return compare(cond, rec, no_nulls);
    }

    /**
     * @brief 使用字段元数据和 NULL 位图判断一条记录是否满足扫描条件。
     * @param cond 条件，支持普通比较、LIKE、IN、BETWEEN 和 NULL 判断。
     * @param rec 待判断的记录。
     * @param nulls 与当前执行器输出列对齐的 NULL 位图，可以为空。
     * @return 条件成立返回 true；NULL 参与普通谓词比较时按过滤语义返回 false。
     * @throws ColumnNotFoundError 条件引用的列不存在时抛出。
     * @throws IncompatibleTypeError 左右值类型不能比较或 LIKE 作用于非字符串时抛出。
     * @throws InternalError BETWEEN 缺少上界时抛出。
     *
     * 函数先处理 IS NULL/IS NOT NULL 等不需要读取普通值的分支，再读取左值。
     * 对 IN 和 BETWEEN 分别展开候选值或上下界，最后统一走三路比较；这种顺序
     * 同时保证 NULL 过滤和类型检查不会被普通读取路径绕过。
     */
    bool compare(const Condition& cond, const RmRecord& rec, const std::vector<bool>& nulls) {
        ColMeta lhs_col_meta = get_col_offset(cond.lhs_col);
        bool lhs_is_null = !nulls.empty() && is_null(nulls, cond.lhs_col);
        if (cond.op == OP_IS_NULL) {
            return lhs_is_null;
        }
        if (cond.op == OP_IS_NOT_NULL) {
            return !lhs_is_null;
        }
        if (cond.op == OP_EXISTS) {
            return false;
        }
        if (lhs_is_null) {
            return false;
        }

        const auto lhs = read_cell(rec.data + lhs_col_meta.offset, lhs_col_meta);
        auto value_from_literal = [](const Value& value) {
            execution_scalar::CellValue result;
            result.type = value.type;
            if (value.type == TYPE_INT) {
                result.int_val = value.int_val;
            } else if (value.type == TYPE_FLOAT) {
                result.float_val = value.float_val;
            } else {
                result.str_val = value.str_val;
            }
            return result;
        };

        auto matches_simple = [&](const execution_scalar::CellValue& rhs, CompOp op) {
            if (!can_cast(lhs.type, rhs.type)) {
                throw IncompatibleTypeError(coltype2str(lhs.type), coltype2str(rhs.type));
            }
            if (op == OP_LIKE) {
                if ((lhs.type != TYPE_STRING && lhs.type != TYPE_DATETIME) ||
                    (rhs.type != TYPE_STRING && rhs.type != TYPE_DATETIME)) {
                    throw IncompatibleTypeError(coltype2str(lhs.type), coltype2str(rhs.type));
                }
                bool matches = execution_scalar::like_match(lhs.str_val, rhs.str_val);
                return cond.negated ? !matches : matches;
            }
            return compare_order(op, execution_scalar::compare_cells(lhs, rhs));
        };

        // IN 需要逐个比较候选值；候选列表中出现 NULL 且没有命中时，过滤结果为 false。
        if (cond.op == OP_IN) {
            bool matched = false;
            bool has_null = false;
            for (const auto& rhs_val : cond.rhs_vals) {
                if (rhs_val.is_null) {
                    has_null = true;
                    continue;
                }
                if (matches_simple(value_from_literal(rhs_val), OP_EQ)) {
                    matched = true;
                    break;
                }
            }
            if (has_null && !matched) {
                return false;
            }
            return cond.negated ? !matched : matched;
        }

        // BETWEEN 等价于同一列上的闭区间上下界判断，negated 在两个比较完成后统一处理。
        if (cond.op == OP_BETWEEN) {
            if (!cond.has_rhs_upper) {
                throw InternalError("BETWEEN predicate is missing its upper bound");
            }
            if (cond.rhs_val.is_null || cond.rhs_upper.is_null) {
                return false;
            }
            bool matches = matches_simple(value_from_literal(cond.rhs_val), OP_GE) &&
                           matches_simple(value_from_literal(cond.rhs_upper), OP_LE);
            return cond.negated ? !matches : matches;
        }

        execution_scalar::CellValue rhs;
        if (cond.is_rhs_val) {
            if (cond.rhs_val.is_null) {
                return false;
            }
            rhs = value_from_literal(cond.rhs_val);
        } else {
            ColMeta rhs_col_meta = get_col_offset(cond.rhs_col);
            if (!nulls.empty() && is_null(nulls, cond.rhs_col)) {
                return false;
            }
            rhs = read_cell(rec.data + rhs_col_meta.offset, rhs_col_meta);
        }
        return matches_simple(rhs, cond.op);
    }
    /**
     * @brief 判断两个列类型是否允许在执行阶段进行比较或转换。
     * @param lhs 左侧列类型。
     * @param rhs 右侧列类型。
     * @return 类型相同、INT/FLOAT 数值互转或 STRING/DATETIME 互转时返回 true。
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
