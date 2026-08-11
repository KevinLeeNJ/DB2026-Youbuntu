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

#include "server/database_instance.h"

#include <chrono>

#include "minilog.h"

void DatabaseInstance::open_and_recover(const std::string& db_name) {
    LOG_INFO("RMDB server starting, database: %s", db_name.c_str());
    if (!sm_manager.is_dir(db_name)) {
        auto catalog_guard = sm_manager.acquire_catalog_exclusive();
        sm_manager.create_db(db_name);
        LOG_INFO("database created: %s", db_name.c_str());
    }
    {
        auto catalog_guard = sm_manager.acquire_catalog_exclusive();
        sm_manager.open_db(db_name);
    }
    open_ = true;
    LOG_INFO("database opened: %s", db_name.c_str());
    log_manager.initialize_from_existing_log();
    buffer_pool_manager.set_log_manager(&log_manager);
    {
        minilog::Logger::get().set_level(minilog::LogLevel::INFO);
        const auto phase_elapsed_ms = [](std::chrono::steady_clock::time_point begin) {
            return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                .count();
        };
        const auto recovery_begin = std::chrono::steady_clock::now();

        auto phase_begin = recovery_begin;
        recovery.analyze();
        LOG_INFO("recovery analyze: %lld ms, wal reads: %llu (%llu bytes)",
                 static_cast<long long>(phase_elapsed_ms(phase_begin)),
                 static_cast<unsigned long long>(disk_manager.get_log_read_count()),
                 static_cast<unsigned long long>(disk_manager.get_log_read_bytes()));

        phase_begin = std::chrono::steady_clock::now();
        recovery.redo();
        LOG_INFO("recovery redo: %lld ms", static_cast<long long>(phase_elapsed_ms(phase_begin)));

        phase_begin = std::chrono::steady_clock::now();
        recovery.undo();
        LOG_INFO("recovery undo: %lld ms", static_cast<long long>(phase_elapsed_ms(phase_begin)));

        // 必须在任何事务开始之前、恢复读完 WAL 与重启清单之后做：commit_ts_ 持久化
        // 在数据页里，而计数器只活在内存里。计数器从 0 重启会让上一世提交的行被
        // 判成“来自未来”而不可见（final.md:342 第 1 条）。取值的完整论证见
        // RecoveryManager::get_recovered_next_timestamp()。
        txn_manager.seed_counters_after_recovery(recovery.get_recovered_next_timestamp(),
                                                 recovery.get_recovered_next_txn_id());
        LOG_INFO("recovery seeded counters: next_timestamp %lld, next_txn_id %lld",
                 static_cast<long long>(recovery.get_recovered_next_timestamp()),
                 static_cast<long long>(recovery.get_recovered_next_txn_id()));

        phase_begin = std::chrono::steady_clock::now();
        sm_manager.refresh_index_residency();
        LOG_INFO("recovery index residency refresh: %lld ms", static_cast<long long>(phase_elapsed_ms(phase_begin)));

        LOG_INFO("database recovery finished in %lld ms", static_cast<long long>(phase_elapsed_ms(recovery_begin)));
        minilog::Logger::get().set_level(minilog::LogLevel::WARN);
    }
}

void DatabaseInstance::close() {
    if (!open_) {
        return;
    }
    open_ = false;
    auto catalog_guard = sm_manager.acquire_catalog_exclusive();
    sm_manager.close_db();
}
