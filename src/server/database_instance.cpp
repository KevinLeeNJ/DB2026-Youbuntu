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
#include <cstdlib>
#include <filesystem>

#include "minilog.h"

LegacyDatabase& DatabaseInstance::legacy() {
    if (!legacy_database)
        throw RMDBError("legacy engine is not available for a DeltaKernel database");
    return *legacy_database;
}

const LegacyDatabase& DatabaseInstance::legacy() const {
    if (!legacy_database)
        throw RMDBError("legacy engine is not available for a DeltaKernel database");
    return *legacy_database;
}

void DatabaseInstance::open_and_recover(const std::string& db_name) {
    LOG_INFO("RMDB server starting, database: %s", db_name.c_str());
    const char* requested = std::getenv("RMDB_STORAGE_ENGINE");
    const bool request_delta = requested != nullptr && std::string(requested) == "delta";
    const bool request_legacy = requested != nullptr && std::string(requested) == "legacy";
    if (requested != nullptr && !request_delta && !request_legacy)
        throw RMDBError("RMDB_STORAGE_ENGINE must be delta or legacy when set");
    const bool has_delta_marker = deltakernel::DeltaDatabase::IsDeltaDirectory(db_name);
    const bool has_legacy_marker = std::filesystem::exists(db_name + "/db.meta");
    if (has_delta_marker && has_legacy_marker)
        throw RMDBError("database directory has conflicting DeltaKernel and legacy format markers");
    if (has_delta_marker) {
        if (requested != nullptr && !request_delta)
            throw RMDBError("RMDB_STORAGE_ENGINE conflicts with Delta database format");
        delta_database = deltakernel::DeltaDatabase::Open(db_name);
        open_ = true;
        LOG_INFO("DeltaKernel database opened: %s", db_name.c_str());
        return;
    }
    if (has_legacy_marker) {
        if (request_delta)
            throw RMDBError("RMDB_STORAGE_ENGINE conflicts with legacy database format");
    } else if (!std::filesystem::exists(db_name)) {
        if (!request_legacy) {
            if (!std::filesystem::create_directories(db_name))
                throw RMDBError("failed to create Delta database directory");
            delta_database = deltakernel::DeltaDatabase::Create(db_name);
            open_ = true;
            LOG_INFO("DeltaKernel database created: %s", db_name.c_str());
            return;
        }
    } else {
        throw RMDBError("database directory is neither DeltaKernel nor legacy");
    }
    legacy_database = std::make_unique<LegacyDatabase>();
    auto& legacy_db = legacy();
    if (!legacy_db.sm_manager.is_dir(db_name)) {
        auto catalog_guard = legacy_db.sm_manager.acquire_catalog_exclusive();
        legacy_db.sm_manager.create_db(db_name);
        LOG_INFO("database created: %s", db_name.c_str());
    }
    {
        auto catalog_guard = legacy_db.sm_manager.acquire_catalog_exclusive();
        legacy_db.sm_manager.open_db(db_name);
    }
    open_ = true;
    LOG_INFO("database opened: %s", db_name.c_str());
    legacy_db.log_manager.initialize_from_existing_log();
    legacy_db.buffer_pool_manager.set_log_manager(&legacy_db.log_manager);
    {
        minilog::Logger::get().set_level(minilog::LogLevel::INFO);
        const auto phase_elapsed_ms = [](std::chrono::steady_clock::time_point begin) {
            return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                .count();
        };
        const auto recovery_begin = std::chrono::steady_clock::now();

        auto phase_begin = recovery_begin;
        legacy_db.recovery.analyze();
        LOG_INFO("recovery analyze: %lld ms, wal reads: %llu (%llu bytes)",
                 static_cast<long long>(phase_elapsed_ms(phase_begin)),
                 static_cast<unsigned long long>(legacy_db.disk_manager.get_log_read_count()),
                 static_cast<unsigned long long>(legacy_db.disk_manager.get_log_read_bytes()));

        phase_begin = std::chrono::steady_clock::now();
        legacy_db.recovery.redo();
        LOG_INFO("recovery redo: %lld ms", static_cast<long long>(phase_elapsed_ms(phase_begin)));

        phase_begin = std::chrono::steady_clock::now();
        legacy_db.recovery.undo();
        LOG_INFO("recovery undo: %lld ms", static_cast<long long>(phase_elapsed_ms(phase_begin)));

        // 必须在任何事务开始之前、恢复读完 WAL 与重启清单之后做：commit_ts_ 持久化
        // 在数据页里，而计数器只活在内存里。计数器从 0 重启会让上一世提交的行被
        // 判成“来自未来”而不可见（final.md:342 第 1 条）。取值的完整论证见
        // RecoveryManager::get_recovered_next_timestamp()。
        legacy_db.txn_manager.seed_counters_after_recovery(legacy_db.recovery.get_recovered_next_timestamp(),
                                                           legacy_db.recovery.get_recovered_next_txn_id());
        LOG_INFO("recovery seeded counters: next_timestamp %lld, next_txn_id %lld",
                 static_cast<long long>(legacy_db.recovery.get_recovered_next_timestamp()),
                 static_cast<long long>(legacy_db.recovery.get_recovered_next_txn_id()));

        phase_begin = std::chrono::steady_clock::now();
        legacy_db.sm_manager.refresh_index_residency();
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
    if (delta_database) {
        delta_database.reset();
        return;
    }
    auto& legacy_db = legacy();
    auto catalog_guard = legacy_db.sm_manager.acquire_catalog_exclusive();
    legacy_db.sm_manager.close_db();
}
