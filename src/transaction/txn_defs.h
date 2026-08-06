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

#include <atomic>
#include <cstdint>

#include "common/config.h"
#include "defs.h"
#include "record/rm_defs.h"

/* 标识事务状态 */
enum class TransactionState { DEFAULT, GROWING, SHRINKING, COMMITTING, COMMITTED, ABORTED };

/* 系统的隔离级别 */
enum class IsolationLevel { READ_UNCOMMITTED, REPEATABLE_READ, READ_COMMITTED, SNAPSHOT_ISOLATION, SERIALIZABLE };

// The benchmark and cloud deployment use READ COMMITTED unless a session
// explicitly selects a stronger isolation level.
constexpr IsolationLevel DEFAULT_ISOLATION_LEVEL = IsolationLevel::READ_COMMITTED;

/* 事务写操作类型，包括插入、删除、更新三种操作 */
enum class WType { INSERT_TUPLE = 0, DELETE_TUPLE, UPDATE_TUPLE };

/**
 * @brief 事务的写操作记录，用于事务的回滚
 * INSERT
 * --------------------------------
 * | wtype | tab_name | tuple_rid |
 * --------------------------------
 * DELETE / UPDATE
 * ----------------------------------------------
 * | wtype | tab_name | tuple_rid | tuple_value |
 * ----------------------------------------------
 */
class WriteRecord {
public:
    WriteRecord() = default;

    // constructor for insert operation
    WriteRecord(WType wtype, const std::string& tab_name, const Rid& rid)
        : wtype_(wtype), tab_name_(tab_name), rid_(rid) {}

    // constructor for delete & update operation
    WriteRecord(WType wtype, const std::string& tab_name, const Rid& rid, const RmRecord& record)
        : wtype_(wtype), tab_name_(tab_name), rid_(rid), record_(record) {}

    ~WriteRecord() = default;

    inline RmRecord& GetRecord() {
        return record_;
    }

    inline Rid& GetRid() {
        return rid_;
    }

    inline WType& GetWriteType() {
        return wtype_;
    }

    inline std::string& GetTableName() {
        return tab_name_;
    }

private:
    WType wtype_;
    std::string tab_name_;
    Rid rid_;
    RmRecord record_;
};

/* 多粒度锁，加锁对象的类型，包括记录和表 */
enum class LockDataType { TABLE = 0, RECORD = 1 };

/**
 * @description: 加锁对象的唯一标识
 */
class LockDataId {
public:
    /* 表级锁 */
    LockDataId(int fd, LockDataType type) {
        assert(type == LockDataType::TABLE);
        fd_ = fd;
        type_ = type;
        rid_.page_no = -1;
        rid_.slot_no = -1;
    }

    /* 行级锁 */
    LockDataId(int fd, const Rid& rid, LockDataType type) {
        assert(type == LockDataType::RECORD);
        fd_ = fd;
        rid_ = rid;
        type_ = type;
    }

    inline int64_t Get() const {
        if (type_ == LockDataType::TABLE) {
            // fd_
            return static_cast<int64_t>(fd_);
        } else {
            // fd_, rid_.page_no, rid.slot_no
            return ((static_cast<int64_t>(type_)) << 63) | ((static_cast<int64_t>(fd_)) << 31) |
                   ((static_cast<int64_t>(rid_.page_no)) << 16) | rid_.slot_no;
        }
    }

    bool operator==(const LockDataId& other) const {
        if (type_ != other.type_)
            return false;
        if (fd_ != other.fd_)
            return false;
        return rid_ == other.rid_;
    }
    int fd_;
    Rid rid_;
    LockDataType type_;
};

template <> struct std::hash<LockDataId> {
    size_t operator()(const LockDataId& obj) const noexcept {
        constexpr uint64_t kTypeMultiplier = 0x9e3779b185ebca87ULL;
        constexpr uint64_t kFdMultiplier = 0xc2b2ae3d27d4eb4fULL;
        constexpr uint64_t kPageMultiplier = 0x165667b19e3779f9ULL;
        constexpr uint64_t kSlotMultiplier = 0x85ebca77c2b2ae63ULL;

        const uint64_t type = static_cast<uint32_t>(obj.type_);
        const uint64_t fd = static_cast<uint32_t>(obj.fd_);
        const uint64_t page_no = static_cast<uint32_t>(obj.rid_.page_no);
        const uint64_t slot_no = static_cast<uint32_t>(obj.rid_.slot_no);
        uint64_t mixed =
            type * kTypeMultiplier ^ fd * kFdMultiplier ^ page_no * kPageMultiplier ^ slot_no * kSlotMultiplier;
        mixed += 0x9e3779b97f4a7c15ULL;
        mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
        mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
        return static_cast<size_t>(mixed ^ (mixed >> 31));
    }
};

/* 事务回滚原因 */
enum class AbortReason {
    LOCK_ON_SHIRINKING = 0,
    UPGRADE_CONFLICT,
    DEADLOCK_PREVENTION,
    LOCK_CANCELLED,
    WW_CONFLICT,
    SSI_DANGER,
    UNIQUE_KEY_CONFLICT
};

// POD-only classification attached at the abort point.  These values are
// intentionally stable: runtime observability must never need SQL text or a
// table name on the abort path.
enum class AbortDetail : uint8_t { UNKNOWN = 0, IMMEDIATE_ACTIVE_OWNER, WAITED_THEN_STALE, STALE_WITHOUT_WAIT };
enum class AbortOrigin : uint8_t { EXEC_STREAM = 0, EXEC_BATCH };
enum class AbortTxnMode : uint8_t { AUTOCOMMIT = 0, EXPLICIT };
enum class AbortOperation : uint8_t { SELECT = 0, INSERT, UPDATE, DELETE, TXN_CONTROL, OTHER };

/* 事务回滚异常，在rmdb.cpp中进行处理 */
class TransactionAbortException : public std::exception {
    txn_id_t txn_id_;
    AbortReason abort_reason_;
    AbortDetail abort_detail_{AbortDetail::UNKNOWN};
    uint64_t triggering_table_runtime_id_{0};
    AbortOrigin abort_origin_{AbortOrigin::EXEC_STREAM};
    AbortTxnMode abort_txn_mode_{AbortTxnMode::AUTOCOMMIT};
    IsolationLevel abort_isolation_{DEFAULT_ISOLATION_LEVEL};
    AbortOperation abort_operation_{AbortOperation::OTHER};

public:
    explicit TransactionAbortException(txn_id_t txn_id, AbortReason abort_reason)
        : txn_id_(txn_id), abort_reason_(abort_reason) {}
    TransactionAbortException(txn_id_t txn_id, AbortReason abort_reason, AbortDetail detail,
                              uint64_t table_runtime_id = 0)
        : txn_id_(txn_id), abort_reason_(abort_reason), abort_detail_(detail),
          triggering_table_runtime_id_(table_runtime_id) {}

    txn_id_t get_transaction_id() const {
        return txn_id_;
    }
    AbortReason GetAbortReason() const {
        return abort_reason_;
    }
    AbortDetail GetAbortDetail() const noexcept { return abort_detail_; }
    uint64_t GetTriggeringTableRuntimeId() const noexcept { return triggering_table_runtime_id_; }
    AbortOrigin GetAbortOrigin() const noexcept { return abort_origin_; }
    AbortTxnMode GetAbortTxnMode() const noexcept { return abort_txn_mode_; }
    IsolationLevel GetAbortIsolation() const noexcept { return abort_isolation_; }
    AbortOperation GetAbortOperation() const noexcept { return abort_operation_; }
    void SetObservation(AbortOrigin origin, AbortTxnMode mode, IsolationLevel isolation,
                        AbortOperation operation) noexcept {
        abort_origin_ = origin;
        abort_txn_mode_ = mode;
        abort_isolation_ = isolation;
        abort_operation_ = operation;
    }
    std::string GetInfo() {
        switch (abort_reason_) {
        case AbortReason::LOCK_ON_SHIRINKING: {
            return "Transaction " + std::to_string(txn_id_) +
                   " aborted because it cannot request locks on SHRINKING phase";
        } break;

        case AbortReason::UPGRADE_CONFLICT: {
            return "Transaction " + std::to_string(txn_id_) +
                   " aborted because another transaction is waiting for upgrading";
        } break;

        case AbortReason::DEADLOCK_PREVENTION: {
            return "Transaction " + std::to_string(txn_id_) + " aborted for deadlock prevention";
        } break;

        case AbortReason::LOCK_CANCELLED: {
            return "Transaction " + std::to_string(txn_id_) + " aborted because its lock wait was cancelled";
        } break;

        case AbortReason::WW_CONFLICT: {
            return "Transaction " + std::to_string(txn_id_) + " aborted because of write-write conflict";
        } break;

        case AbortReason::SSI_DANGER: {
            return "Transaction " + std::to_string(txn_id_) + " aborted because of SSI danger structure";
        } break;
        case AbortReason::UNIQUE_KEY_CONFLICT: {
            return "Transaction " + std::to_string(txn_id_) + " aborted because of unique-key conflict";
        } break;

        default: {
            return "Transaction aborted";
        } break;
        }
    }
};
