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
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "errors.h"
#include "sm_defs.h"

/* 字段元数据 */
struct ColMeta {
    std::string tab_name; // 字段所属表名称
    std::string name;     // 字段名称
    // 以下四个标量给默认值，是因为 `ColMeta x;` 这种默认构造在代码里很常见
    // （如 AggregateSpec::input_col、init_aggregate_cols 的局部变量），而
    // ColMeta 的拷贝构造会逐成员读取它们；不初始化会让 UBSan 报出
    // "load of value N, which is not a valid value for type 'bool'/'ColType'"。
    ColType type = TYPE_INT; // 字段类型
    int len = 0;             // 字段长度
    int offset = 0;          // 字段位于记录中的偏移量
    bool index = false;      /** unused */

    // 以下两个字段只存在于内存中，不参与下面的磁盘序列化：元组内该列 NULL 位的
    // 绝对地址，由 TabMeta::bind_null_positions 推导。详见 defs.h 的布局说明。
    // null_byte < 0 表示该列永远非 NULL。
    int null_byte = -1;
    uint8_t null_mask = 0;

    friend std::ostream& operator<<(std::ostream& os, const ColMeta& col) {
        // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
        return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset << ' '
                  << col.index;
    }

    friend std::istream& operator>>(std::istream& is, ColMeta& col) {
        return is >> col.tab_name >> col.name >> col.type >> col.len >> col.offset >> col.index;
    }
};

/* 元组中该列当前是否为 SQL NULL */
inline bool is_null(const char* tuple, const ColMeta& col) {
    return is_null_at(tuple, col.null_byte, col.null_mask);
}

inline void set_null(char* tuple, const ColMeta& col) {
    set_null_at(tuple, col.null_byte, col.null_mask);
}

inline void clear_null(char* tuple, const ColMeta& col) {
    clear_null_at(tuple, col.null_byte, col.null_mask);
}

/* 一组连续列的 null bitmap 字节数（每列 1 bit） */
inline int null_bitmap_bytes(size_t num_cols) {
    return static_cast<int>((num_cols + 7) / 8);
}

/* 为一段自成一体的元组布局（数据区紧跟尾部 bitmap）绑定各列的 NULL 位地址。
   Projection / Aggregate / Union 这类重新打包元组的执行器用它给输出列建址。 */
inline void bind_null_positions(std::vector<ColMeta>& cols, int data_len) {
    for (size_t i = 0; i < cols.size(); ++i) {
        cols[i].null_byte = data_len + static_cast<int>(i / 8);
        cols[i].null_mask = static_cast<uint8_t>(0x80u >> (i % 8));
    }
}

/* 索引元数据 */
struct IndexMeta {
    std::string tab_name;      // 索引所属表名称
    int col_tot_len;           // 索引字段长度总和
    int col_num;               // 索引字段数量
    std::vector<ColMeta> cols; // 索引包含的字段

    friend std::ostream& operator<<(std::ostream& os, const IndexMeta& index) {
        os << index.tab_name << " " << index.col_tot_len << " " << index.col_num;
        for (auto& col : index.cols) {
            os << "\n" << col;
        }
        return os;
    }

    friend std::istream& operator>>(std::istream& is, IndexMeta& index) {
        is >> index.tab_name >> index.col_tot_len >> index.col_num;
        for (int i = 0; i < index.col_num; ++i) {
            ColMeta col;
            is >> col;
            index.cols.push_back(col);
        }
        return is;
    }
};

/* 表元数据 */
struct TabMeta {
    std::string name;               // 表名称
    std::vector<ColMeta> cols;      // 表包含的字段
    std::vector<IndexMeta> indexes; // 表上建立的索引

    TabMeta() = default;
    TabMeta(const TabMeta& other) = default;
    TabMeta& operator=(const TabMeta& other) = default;

    /* 记录数据区的长度，不含尾部 null bitmap */
    int data_len() const {
        return cols.empty() ? 0 : cols.back().offset + cols.back().len;
    }

    /* 记录总长度：数据区 + 尾部 null bitmap，即数据文件头里的 record_size */
    int record_len() const {
        return data_len() + null_bitmap_bytes(cols.size());
    }

    /* 由数据文件头的 record_size 推导各列的 NULL 位地址。
       record_size 容不下 bitmap 说明这是 NULL 支持之前写入的旧文件，
       此时把所有列标记为不可为 NULL，避免读写越界。 */
    void bind_null_positions(int record_size) {
        if (record_size < record_len()) {
            for (auto& col : cols) {
                col.null_byte = -1;
                col.null_mask = 0;
            }
            return;
        }
        ::bind_null_positions(cols, data_len());
    }

    /* 判断当前表中是否存在名为col_name的字段 */
    bool is_col(const std::string& col_name) const {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta& col) { return col.name == col_name; });
        return pos != cols.end();
    }

    /* 判断当前表上是否建有指定索引，索引包含的字段为col_names */
    bool is_index(const std::vector<std::string>& col_names) const {
        for (auto& index : indexes) {
            if (static_cast<size_t>(index.col_num) == col_names.size()) {
                size_t i = 0;
                for (; i < static_cast<size_t>(index.col_num); ++i) {
                    if (index.cols[i].name.compare(col_names[i]) != 0)
                        break;
                }
                if (i == static_cast<size_t>(index.col_num))
                    return true;
            }
        }

        return false;
    }

    /* 根据字段名称集合获取索引元数据 */
    std::vector<IndexMeta>::iterator get_index_meta(const std::vector<std::string>& col_names) {
        for (auto index = indexes.begin(); index != indexes.end(); ++index) {
            if (static_cast<size_t>((*index).col_num) != col_names.size())
                continue;
            auto& index_cols = (*index).cols;
            size_t i = 0;
            for (; i < col_names.size(); ++i) {
                if (index_cols[i].name.compare(col_names[i]) != 0)
                    break;
            }
            if (i == col_names.size())
                return index;
        }
        throw IndexNotFoundError(name, col_names);
    }

    /* 根据字段名称获取字段元数据 */
    std::vector<ColMeta>::iterator get_col(const std::string& col_name) {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta& col) { return col.name == col_name; });
        if (pos == cols.end()) {
            throw ColumnNotFoundError(col_name);
        }
        return pos;
    }

    friend std::ostream& operator<<(std::ostream& os, const TabMeta& tab) {
        os << tab.name << '\n' << tab.cols.size() << '\n';
        for (auto& col : tab.cols) {
            os << col << '\n'; // col是ColMeta类型，然后调用重载的ColMeta的操作符<<
        }
        os << tab.indexes.size() << "\n";
        for (auto& index : tab.indexes) {
            os << index << "\n";
        }
        return os;
    }

    friend std::istream& operator>>(std::istream& is, TabMeta& tab) {
        size_t n;
        is >> tab.name >> n;
        for (size_t i = 0; i < n; i++) {
            ColMeta col;
            is >> col;
            tab.cols.push_back(col);
        }
        is >> n;
        for (size_t i = 0; i < n; ++i) {
            IndexMeta index;
            is >> index;
            tab.indexes.push_back(index);
        }
        return is;
    }
};

// 注意重载了操作符 << 和 >>，这需要更底层同样重载TabMeta、ColMeta的操作符 << 和 >>
/* 数据库元数据 */
class DbMeta {
    friend class SmManager;

private:
    std::string name_;                    // 数据库名称
    std::map<std::string, TabMeta> tabs_; // 数据库中包含的表

public:
    // DbMeta(std::string name) : name_(name) {}

    /* 判断数据库中是否存在指定名称的表 */
    bool is_table(const std::string& tab_name) const {
        return tabs_.find(tab_name) != tabs_.end();
    }

    void SetTabMeta(const std::string& tab_name, const TabMeta& meta) {
        tabs_[tab_name] = meta;
    }

    /* 获取指定名称表的元数据 */
    TabMeta& get_table(const std::string& tab_name) {
        auto pos = tabs_.find(tab_name);
        if (pos == tabs_.end()) {
            throw TableNotFoundError(tab_name);
        }

        return pos->second;
    }

    // 重载操作符 <<
    friend std::ostream& operator<<(std::ostream& os, const DbMeta& db_meta) {
        os << db_meta.name_ << '\n' << db_meta.tabs_.size() << '\n';
        for (auto& entry : db_meta.tabs_) {
            os << entry.second << '\n';
        }
        return os;
    }

    friend std::istream& operator>>(std::istream& is, DbMeta& db_meta) {
        size_t n = 0;
        if (!(is >> db_meta.name_ >> n)) {
            return is;
        }
        for (size_t i = 0; i < n; i++) {
            TabMeta tab;
            if (!(is >> tab)) {
                return is;
            }
            db_meta.tabs_[tab.name] = tab;
        }
        return is;
    }
};
