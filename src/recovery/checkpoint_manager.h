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

#include <atomic>
#include <cstdint>

namespace rmdb::system {
class SchemaManager;
}

namespace rmdb::txn {
class TransactionManager;
}

namespace rmdb {
using system::SchemaManager;
using txn::TransactionManager;
} // namespace rmdb

namespace rmdb::recovery {
class LogManager;

struct CheckpointOptions {
    int64_t auto_checkpoint_bytes = 256LL * 1024 * 1024;
};

class CheckpointManager {
public:
    CheckpointManager(TransactionManager* txn_mgr, SchemaManager* schema_mgr, LogManager* log_mgr);

    bool RunCleanCheckpoint();
    bool RunIfNeeded();
    void SetOptions(CheckpointOptions options);

private:
    TransactionManager* txn_mgr_;
    SchemaManager* schema_mgr_;
    LogManager* log_mgr_;
    CheckpointOptions options_{};
    std::atomic<bool> running_{false};
};

} // namespace rmdb::recovery

namespace rmdb {
using recovery::CheckpointManager;
using recovery::CheckpointOptions;
} // namespace rmdb
