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

#include <algorithm>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "defs.h"

namespace rmdb::txn {
/// SSI 辅助数据：历史索引键 + 已删除元组候选。
/// 由 TransactionManager 持有，executor 通过 TransactionManager::ssi_registry() 访问。
class SSIRegistry {
public:
    /// 记录某索引键被某事务删除/更新过（用于 WW 冲突检测）。
    void remember_historical_index_key(const std::string& tab_name, const std::string& index_name,
                                       const std::vector<char>& key, const Rid& rid) {
        std::lock_guard<std::mutex> lock(historical_index_keys_latch_);
        auto& rids = historical_index_keys_[make_historical_index_key(tab_name, index_name, key)];
        if (std::find(rids.begin(), rids.end(), rid) == rids.end()) {
            rids.push_back(rid);
        }
    }

    /// 查询某索引键的历史候选 RID 列表。
    std::vector<Rid> get_historical_index_key_rids(const std::string& tab_name, const std::string& index_name,
                                                   const std::vector<char>& key) const {
        std::lock_guard<std::mutex> lock(historical_index_keys_latch_);
        auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, key));
        if (it == historical_index_keys_.end()) {
            return {};
        }
        return it->second;
    }

    /// 记录某表上被删除的元组候选（用于 insert 检测与并发 delete 的冲突）。
    void remember_deleted_tuple_candidate(const std::string& tab_name, const Rid& rid) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto& rids = deleted_tuple_candidates_[tab_name];
        if (std::find(rids.begin(), rids.end(), rid) == rids.end()) {
            rids.push_back(rid);
        }
    }

    /// 获取某表的已删除元组候选列表。
    std::vector<Rid> get_deleted_tuple_candidates(const std::string& tab_name) const {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto it = deleted_tuple_candidates_.find(tab_name);
        if (it == deleted_tuple_candidates_.end()) {
            return {};
        }
        return it->second;
    }

    /// 移除某表上指定的已删除元组候选（slot 已被复用或不再 deleted）。
    void remove_deleted_tuple_candidate(const std::string& tab_name, const Rid& rid) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto it = deleted_tuple_candidates_.find(tab_name);
        if (it == deleted_tuple_candidates_.end()) {
            return;
        }
        auto& rids = it->second;
        rids.erase(std::remove(rids.begin(), rids.end(), rid), rids.end());
        if (rids.empty()) {
            deleted_tuple_candidates_.erase(it);
        }
    }

    /// 清空全部 SSI 辅助数据（数据库切换/重置时调用）。
    void clear() {
        std::lock_guard<std::mutex> lk1(historical_index_keys_latch_);
        std::lock_guard<std::mutex> lk2(deleted_tuple_candidates_latch_);
        historical_index_keys_.clear();
        deleted_tuple_candidates_.clear();
    }

private:
    // 拼接 historical_index_keys_ 的复合 key：tab\0index\0key_bytes
    static std::string make_historical_index_key(const std::string& tab_name, const std::string& index_name,
                                                 const std::vector<char>& key) {
        std::string combined;
        combined.reserve(tab_name.size() + index_name.size() + key.size() + 2);
        combined.append(tab_name);
        combined.push_back('\0');
        combined.append(index_name);
        combined.push_back('\0');
        combined.append(key.data(), key.size());
        return combined;
    }

    mutable std::mutex historical_index_keys_latch_;
    std::unordered_map<std::string, std::vector<Rid>> historical_index_keys_;
    mutable std::mutex deleted_tuple_candidates_latch_;
    std::unordered_map<std::string, std::vector<Rid>> deleted_tuple_candidates_;
};

} // namespace rmdb::txn

namespace rmdb {
using txn::SSIRegistry;
} // namespace rmdb
