/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// =============================================================================
// Snapshot Isolation & Serializable Isolation — Concurrent Tests
//
// These tests validate the MVCC-based SI and SSI-based SER isolation levels
// as specified in .doc/Snapshot.md and docs/测试说明文档2026.pdf.
//
// ALL TESTS CURRENTLY FAIL — the feature is not yet implemented (TDD RED phase).
// =============================================================================

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "analyze/analyze.h"
#include "common/config.h"
#include "common/context.h"
#include "errors.h"
#include "execution/execution_manager.h"
#include "gtest/gtest.h"
#include "index/ix_manager.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parser.h"
#include "portal.h"
#include "record/rm_manager.h"
#include "recovery/log_manager.h"
#include "recovery/log_recovery.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

// =============================================================================
// SimpleThreadBarrier — reusable thread barrier (C++17 compatible)
// Avoids std::barrier (C++20) and naming conflict with ::sync() from unistd.h.
// =============================================================================

class SimpleThreadBarrier {
public:
    explicit SimpleThreadBarrier(int count) : count_(count), total_(count) {}

    void arrive_and_wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        int gen = generation_;
        if (--count_ == 0) {
            generation_++;
            count_ = total_;
            cv_.notify_all();
        } else {
            cv_.wait(lock, [this, gen] { return gen != generation_; });
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    int count_;
    int total_;
    int generation_ = 0;
};

TEST(SnapshotIsolationConcurrencyTest, ExclusiveRecordLockWaitsForSecondWriter) {
    LockManager lock_manager;
    Transaction t1(1001);
    Transaction t2(1002);
    Rid rid{1, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);

    EXPECT_TRUE(lock_manager.lock_exclusive_on_record(&t1, rid, 42));
    std::atomic<bool> started{false};
    std::atomic<bool> acquired{false};
    std::thread waiter([&] {
        started.store(true);
        acquired.store(lock_manager.lock_exclusive_on_record(&t2, rid, 42));
    });
    while (!started.load()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(acquired.load());
    ASSERT_TRUE(lock_manager.unlock(&t1, lock_id));
    waiter.join();

    EXPECT_TRUE(acquired.load());
    EXPECT_TRUE(lock_manager.unlock(&t2, lock_id));
}

TEST(SnapshotIsolationConcurrencyTest, ExplicitReadCommittedWriterWaitsForRecordLock) {
    LockManager lock_manager;
    Transaction owner(1001, IsolationLevel::READ_COMMITTED);
    Transaction waiter(1002, IsolationLevel::READ_COMMITTED);
    waiter.set_txn_mode(true);
    Rid rid{1, 0};

    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));
    std::atomic<bool> started{false};
    std::atomic<bool> acquired{false};
    std::thread waiter_thread([&] {
        started.store(true);
        acquired.store(lock_manager.lock_exclusive_on_record(&waiter, rid, 42));
    });

    while (!started.load()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(acquired.load());
    ASSERT_TRUE(lock_manager.unlock(&owner, LockDataId(42, rid, LockDataType::RECORD)));
    waiter_thread.join();

    EXPECT_TRUE(acquired.load());
    EXPECT_EQ(waiter.get_lock_set()->count(LockDataId(42, rid, LockDataType::RECORD)), 1u);
    EXPECT_TRUE(lock_manager.unlock(&waiter, LockDataId(42, rid, LockDataType::RECORD)));
}

TEST(SnapshotIsolationConcurrencyTest, RecordLockWaitersAreGrantedInFifoOrder) {
    LockManager lock_manager;
    Transaction owner(1001, IsolationLevel::READ_COMMITTED);
    Transaction first_waiter(1002, IsolationLevel::READ_COMMITTED);
    Transaction second_waiter(1003, IsolationLevel::READ_COMMITTED);
    first_waiter.set_txn_mode(true);
    second_waiter.set_txn_mode(true);
    Rid rid{2, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);
    std::mutex order_latch;
    std::vector<txn_id_t> grant_order;

    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));
    auto wait_and_release = [&](Transaction* txn) {
        ASSERT_TRUE(lock_manager.lock_exclusive_on_record(txn, rid, 42));
        {
            std::scoped_lock<std::mutex> lock(order_latch);
            grant_order.push_back(txn->get_transaction_id());
        }
        EXPECT_TRUE(lock_manager.unlock(txn, lock_id));
    };

    std::thread first_thread(wait_and_release, &first_waiter);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    std::thread second_thread(wait_and_release, &second_waiter);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(lock_manager.unlock(&owner, lock_id));
    first_thread.join();
    second_thread.join();

    ASSERT_EQ(grant_order.size(), 2u);
    EXPECT_EQ(grant_order[0], first_waiter.get_transaction_id());
    EXPECT_EQ(grant_order[1], second_waiter.get_transaction_id());
}

TEST(SnapshotIsolationConcurrencyTest, CancelledRecordLockWaiterReturnsWithoutAcquiring) {
    LockManager lock_manager;
    Transaction owner(1001, IsolationLevel::READ_COMMITTED);
    Transaction waiter(1002, IsolationLevel::READ_COMMITTED);
    waiter.set_txn_mode(true);
    Rid rid{3, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);

    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));
    std::atomic<bool> started{false};
    std::atomic<bool> acquired{true};
    std::thread waiter_thread([&] {
        started.store(true);
        acquired.store(lock_manager.lock_exclusive_on_record(&waiter, rid, 42));
    });

    while (!started.load()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    lock_manager.cancel_transaction(&waiter);
    waiter_thread.join();

    EXPECT_FALSE(acquired.load());
    EXPECT_EQ(waiter.get_lock_set()->count(lock_id), 0u);
    EXPECT_TRUE(lock_manager.unlock(&owner, lock_id));
    EXPECT_FALSE(lock_manager.unlock(&waiter, lock_id));
}

TEST(SnapshotIsolationConcurrencyTest, WaitForCycleCancelsYoungestVictim) {
    LockManager lock_manager;
    Transaction older(1001, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction younger(1002, IsolationLevel::SNAPSHOT_ISOLATION);
    Rid first{4, 0};
    Rid second{5, 0};
    LockDataId first_lock(42, first, LockDataType::RECORD);
    LockDataId second_lock(42, second, LockDataType::RECORD);

    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&older, first, 42));
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&younger, second, 42));

    std::atomic<bool> younger_started{false};
    std::atomic<bool> younger_acquired{true};
    std::thread younger_waiter([&] {
        younger_started.store(true);
        younger_acquired.store(lock_manager.lock_exclusive_on_record(&younger, first, 42));
        if (!younger_acquired.load()) {
            lock_manager.cancel_transaction(&younger);
            lock_manager.unlock(&younger, second_lock);
        }
    });
    while (!younger_started.load()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // The older requester closes the cycle. The youngest transaction must be
    // cancelled and release its second lock so the older transaction wins.
    const bool older_acquired = lock_manager.lock_exclusive_on_record(&older, second, 42);
    younger_waiter.join();

    EXPECT_TRUE(older_acquired);
    EXPECT_FALSE(younger_acquired.load());
    EXPECT_GE(lock_manager.wait_cycle_abort_count(), 1u);
    EXPECT_TRUE(lock_manager.unlock(&older, second_lock));
    EXPECT_TRUE(lock_manager.unlock(&older, first_lock));
}

TEST(SnapshotIsolationConcurrencyTest, UniqueKeyCycleCancelsYoungestVictim) {
    LockManager lock_manager;
    Transaction older(1101, IsolationLevel::READ_COMMITTED);
    Transaction younger(1102, IsolationLevel::READ_COMMITTED);
    std::vector<char> first_key{'a'};
    std::vector<char> second_key{'b'};
    const int index_fd = 7;
    std::string first_lock(sizeof(index_fd), '\0');
    std::string second_lock(sizeof(index_fd), '\0');
    std::memcpy(first_lock.data(), &index_fd, sizeof(index_fd));
    std::memcpy(second_lock.data(), &index_fd, sizeof(index_fd));
    first_lock.push_back('a');
    second_lock.push_back('b');

    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(&older, index_fd, first_key));
    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(&younger, index_fd, second_key));

    std::atomic<bool> younger_started{false};
    std::atomic<bool> younger_acquired{true};
    std::thread younger_waiter([&] {
        younger_started.store(true);
        younger_acquired.store(lock_manager.lock_exclusive_on_unique_key(&younger, index_fd, first_key));
        if (!younger_acquired.load()) {
            lock_manager.unlock_unique_key(&younger, second_lock);
        }
    });
    while (!younger_started.load()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    const bool older_acquired = lock_manager.lock_exclusive_on_unique_key(&older, index_fd, second_key);
    younger_waiter.join();

    EXPECT_TRUE(older_acquired);
    EXPECT_FALSE(younger_acquired.load());
    EXPECT_GE(lock_manager.wait_cycle_abort_count(), 1u);
    EXPECT_TRUE(lock_manager.unlock_unique_key(&older, second_lock));
    EXPECT_TRUE(lock_manager.unlock_unique_key(&older, first_lock));
}

TEST(SnapshotIsolationConcurrencyTest, WaitForGraphRebuildsAfterOwnerHandoff) {
    LockManager lock_manager;
    Transaction owner(1201, IsolationLevel::READ_COMMITTED);
    Transaction older(1202, IsolationLevel::READ_COMMITTED);
    Transaction younger(1203, IsolationLevel::READ_COMMITTED);
    Rid first{6, 0};
    Rid second{7, 0};
    LockDataId first_lock(42, first, LockDataType::RECORD);
    LockDataId second_lock(42, second, LockDataType::RECORD);

    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, first, 42));
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&younger, second, 42));

    std::atomic<bool> older_started{false};
    std::atomic<bool> younger_started{false};
    std::atomic<bool> older_has_first{false};
    std::atomic<bool> older_has_second{false};
    std::thread older_thread([&] {
        older_started.store(true);
        if (!lock_manager.lock_exclusive_on_record(&older, first, 42)) {
            return;
        }
        older_has_first.store(true);
        while (!older_has_second.load()) {
            std::this_thread::yield();
        }
    });
    while (!older_started.load()) {
        std::this_thread::yield();
    }

    std::thread younger_thread([&] {
        younger_started.store(true);
        const bool acquired = lock_manager.lock_exclusive_on_record(&younger, first, 42);
        if (!acquired) {
            lock_manager.unlock(&younger, second_lock);
            return;
        }
        lock_manager.unlock(&younger, first_lock);
    });
    while (!younger_started.load()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(lock_manager.unlock(&owner, first_lock));
    while (!older_has_first.load()) {
        std::this_thread::yield();
    }

    // T3 is still queued behind T2 on the first lock. T2 now waits for T3's
    // second lock, so the graph must be rebuilt as T2 -> T3 -> T2.
    older_has_second.store(lock_manager.lock_exclusive_on_record(&older, second, 42));
    older_thread.join();
    younger_thread.join();

    EXPECT_TRUE(older_has_second.load());
    EXPECT_GE(lock_manager.wait_cycle_abort_count(), 1u);
    EXPECT_TRUE(lock_manager.unlock(&older, second_lock));
    EXPECT_TRUE(lock_manager.unlock(&older, first_lock));
}

TEST(SnapshotIsolationConcurrencyTest, UniqueKeyOwnerHandoffPreservesFifoOrder) {
    LockManager lock_manager;
    Transaction owner(1301, IsolationLevel::READ_COMMITTED);
    Transaction first_waiter(1302, IsolationLevel::READ_COMMITTED);
    Transaction second_waiter(1303, IsolationLevel::READ_COMMITTED);
    const std::vector<char> key{'h', 'a', 'n', 'd', 'o', 'f', 'f'};
    std::string lock_id(sizeof(int), '\0');
    const int index_fd = 77;
    std::memcpy(lock_id.data(), &index_fd, sizeof(index_fd));
    lock_id.append(key.data(), key.size());

    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(&owner, index_fd, key));
    std::atomic<bool> first_acquired{false};
    std::atomic<bool> second_acquired{false};
    std::atomic<bool> release_first{false};
    std::thread first_thread([&] {
        first_acquired.store(lock_manager.lock_exclusive_on_unique_key(&first_waiter, index_fd, key));
        while (!release_first.load()) {
            std::this_thread::yield();
        }
        if (first_acquired.load()) {
            EXPECT_TRUE(lock_manager.unlock_unique_key(&first_waiter, lock_id));
        }
    });
    std::thread second_thread([&] {
        second_acquired.store(lock_manager.lock_exclusive_on_unique_key(&second_waiter, index_fd, key));
        if (second_acquired.load()) {
            EXPECT_TRUE(lock_manager.unlock_unique_key(&second_waiter, lock_id));
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(lock_manager.unlock_unique_key(&owner, lock_id));
    while (!first_acquired.load())
        std::this_thread::yield();
    release_first.store(true);
    first_thread.join();
    second_thread.join();
    EXPECT_TRUE(second_acquired.load());
}

TEST(SnapshotIsolationConcurrencyTest, IndependentRecordLocksProgressConcurrently) {
    LockManager lock_manager;
    constexpr int thread_count = 8;
    constexpr int iterations = 1000;
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;

    for (int thread_no = 0; thread_no < thread_count; ++thread_no) {
        threads.emplace_back([&, thread_no] {
            Transaction txn(2000 + thread_no);
            Rid rid{thread_no + 10, 0};
            LockDataId lock_id(42, rid, LockDataType::RECORD);
            for (int iteration = 0; iteration < iterations; ++iteration) {
                if (!lock_manager.lock_exclusive_on_record(&txn, rid, 42) || !lock_manager.unlock(&txn, lock_id)) {
                    failures.fetch_add(1);
                    return;
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(failures.load(), 0);
}

// =============================================================================
// SharedTestDB — shared in-process database engine for concurrent session tests
// =============================================================================

class SharedTestDB {
public:
    SharedTestDB(const std::string& db_name) : db_name_(db_name) {
        char cwd_buf[1024];
        getcwd(cwd_buf, sizeof(cwd_buf));
        original_cwd_ = cwd_buf;

        std::string cmd = "rm -rf " + original_cwd_ + "/" + db_name_;
        system(cmd.c_str());

        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_manager_.get(),
                                                  ix_manager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get(), sm_manager_.get());
        planner_ = std::make_unique<Planner>(sm_manager_.get());
        optimizer_ = std::make_unique<Optimizer>(sm_manager_.get(), planner_.get());
        ql_manager_ =
            std::make_unique<QlManager>(sm_manager_.get(), txn_manager_.get(), static_cast<Planner*>(nullptr));
        log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
        recovery_ =
            std::make_unique<RecoveryManager>(disk_manager_.get(), buffer_pool_manager_.get(), sm_manager_.get());
        portal_ = std::make_unique<Portal>(sm_manager_.get());
        analyze_ = std::make_unique<Analyze>(sm_manager_.get());

        sm_manager_->create_db(db_name_);
        sm_manager_->open_db(db_name_);

        recovery_->analyze();
        recovery_->redo();
        recovery_->undo();
    }

    ~SharedTestDB() {
        try {
            sm_manager_->close_db();
        } catch (...) {
            chdir(original_cwd_.c_str());
        }
        std::string cmd = "rm -rf " + original_cwd_ + "/" + db_name_;
        system(cmd.c_str());
    }

    // Managers accessible to sessions
    SmManager* sm() {
        return sm_manager_.get();
    }
    LockManager* lock() {
        return lock_manager_.get();
    }
    TransactionManager* txn() {
        return txn_manager_.get();
    }
    LogManager* log() {
        return log_manager_.get();
    }
    Planner* planner() {
        return planner_.get();
    }
    Optimizer* optimizer() {
        return optimizer_.get();
    }
    QlManager* ql() {
        return ql_manager_.get();
    }
    Portal* portal() {
        return portal_.get();
    }
    Analyze* analyze() {
        return analyze_.get();
    }

private:
    std::string db_name_;
    std::string original_cwd_;
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> txn_manager_;
    std::unique_ptr<Planner> planner_;
    std::unique_ptr<Optimizer> optimizer_;
    std::unique_ptr<QlManager> ql_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<RecoveryManager> recovery_;
    std::unique_ptr<Portal> portal_;
    std::unique_ptr<Analyze> analyze_;
};

// =============================================================================
// TestSession — a single client session sharing a SharedTestDB
// =============================================================================

class TestSession {
public:
    explicit TestSession(SharedTestDB* db, IsolationLevel isolation_level = DEFAULT_ISOLATION_LEVEL)
        : db_(db), session_isolation_(isolation_level) {}

    /// Execute a single SQL statement (must include trailing ';').
    /// Returns the captured text output.
    /// Throws TransactionAbortException on abort, RMDBError on failure.
    std::string exec_sql(const std::string& sql) {
        return exec_sql_with_portal_ready_hook(sql, {});
    }

    /// Execute SQL after allowing a test to observe the fully resolved portal.
    std::string exec_sql_with_portal_ready_hook(const std::string& sql,
                                                const std::function<void(const PortalStmt&)>& portal_ready_hook) {
        char data_send[BUFFER_LENGTH];
        memset(data_send, 0, BUFFER_LENGTH);
        int offset = 0;

        Context context(db_->lock(), db_->log(), nullptr, data_send, &offset, db_->txn());
        context.isolation_level_ = session_isolation_;
        setup_transaction(&context);

        auto parse_tree = ast::parse_sql(sql);
        if (parse_tree == nullptr) {
            finish_statement(&context);
            return "";
        }

        try {
            std::unique_ptr<Query> query = db_->analyze()->do_analyze(std::move(parse_tree));
            std::unique_ptr<Plan> plan = db_->optimizer()->plan_query(std::move(query), &context);
            std::unique_ptr<PortalStmt> portal_stmt = db_->portal()->start(std::move(plan), &context);
            if (portal_ready_hook) {
                portal_ready_hook(*portal_stmt);
            }
            db_->portal()->run(std::move(portal_stmt), db_->ql(), &txn_id_, &context);
            db_->portal()->drop();
            // Persist isolation level change (SET TRANSACTION ISOLATION LEVEL)
            session_isolation_ = context.isolation_level_;
            finish_statement(&context);
        } catch (TransactionAbortException&) {
            handle_abort(&context);
            throw;
        } catch (...) {
            abort_statement(&context);
            throw;
        }

        return std::string(data_send, offset);
    }

    /// Execute SQL and expect TransactionAbortException (returns "abort\n").
    /// Returns empty string if no abort occurred.
    std::string exec_sql_expect_abort(const std::string& sql) {
        try {
            exec_sql(sql);
            return ""; // should have aborted
        } catch (TransactionAbortException&) {
            // expected: return simulated abort output
            return "abort\n";
        }
    }

    /// Execute SQL and expect success (no exception).
    bool exec_sql_ok(const std::string& sql) {
        try {
            exec_sql(sql);
            return true;
        } catch (RMDBError&) {
            return false;
        } catch (TransactionAbortException&) {
            return false;
        }
    }

    /// Strip trailing newlines for comparison
    static std::string trim_output(std::string s) {
        while (!s.empty() && s.back() == '\n')
            s.pop_back();
        return s;
    }

private:
    void setup_transaction(Context* context) {
        context->txn_ = txn_id_ == INVALID_TXN_ID ? nullptr : db_->txn()->get_transaction(txn_id_);
        if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
            context->txn_->get_state() == TransactionState::ABORTED) {
            context->txn_ = db_->txn()->begin(nullptr, context->log_mgr_, context->isolation_level_);
            txn_id_ = context->txn_->get_transaction_id();
            context->txn_->set_txn_mode(false);
            context->txn_->set_isolation_level(context->isolation_level_);
        }
        db_->txn()->BeginStatement(context->txn_);
    }

    void finish_statement(Context* context) {
        if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
            context->txn_->get_state() != TransactionState::COMMITTED &&
            context->txn_->get_state() != TransactionState::ABORTED) {
            db_->txn()->commit(context->txn_, context->log_mgr_);
        }
        context->txn_ = nullptr;
    }

    void abort_statement(Context* context) {
        if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
            context->txn_->get_state() != TransactionState::COMMITTED &&
            context->txn_->get_state() != TransactionState::ABORTED) {
            db_->txn()->abort(context->txn_, context->log_mgr_);
        }
        context->txn_ = nullptr;
    }

    void handle_abort(Context* context) {
        if (context->txn_ != nullptr && context->txn_->get_state() != TransactionState::ABORTED &&
            context->txn_->get_state() != TransactionState::COMMITTED) {
            db_->txn()->abort(context->txn_, context->log_mgr_);
        }
        context->txn_ = nullptr;
    }

    SharedTestDB* db_;
    txn_id_t txn_id_{INVALID_TXN_ID};
    IsolationLevel session_isolation_{DEFAULT_ISOLATION_LEVEL};
};

// =============================================================================
// SnapshotTest — test fixture
// =============================================================================

class SnapshotTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        std::string db_name = std::string("snapshot_") + test_info->name();
        db_ = std::make_unique<SharedTestDB>(db_name);
    }

    void TearDown() override {
        db_.reset();
    }

    std::unique_ptr<TestSession> create_session(IsolationLevel isolation_level = DEFAULT_ISOLATION_LEVEL) {
        return std::make_unique<TestSession>(db_.get(), isolation_level);
    }

    std::unique_ptr<SharedTestDB> db_;
};

TEST_F(SnapshotTest, DefaultIsolationIsReadCommitted) {
    EXPECT_EQ(DEFAULT_ISOLATION_LEVEL, IsolationLevel::READ_COMMITTED);
}

TEST_F(SnapshotTest, RC_DefaultStatementSeesNewCommittedVersion) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table rc_counter (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into rc_counter values (1, 100);"));

    auto t1 = create_session(IsolationLevel::READ_COMMITTED);
    auto t2 = create_session(IsolationLevel::READ_COMMITTED);

    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    std::string first_read = t1->exec_sql("select * from rc_counter where id = 1;");

    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("update rc_counter set val = 200 where id = 1;"));
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    std::string second_read = t1->exec_sql("select * from rc_counter where id = 1;");
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    std::string expected_first = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "|                1 |              100 |\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 1";
    std::string expected_second = "+------------------+------------------+\n"
                                  "|               id |              val |\n"
                                  "+------------------+------------------+\n"
                                  "|                1 |              200 |\n"
                                  "+------------------+------------------+\n"
                                  "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(first_read), expected_first);
    EXPECT_EQ(TestSession::trim_output(second_read), expected_second);
}

TEST_F(SnapshotTest, RC_DefaultUpdateUsesLatestCommittedVersion) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table rc_update_latest (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into rc_update_latest values (1, 100);"));

    auto t1 = create_session(IsolationLevel::READ_COMMITTED);
    auto t2 = create_session(IsolationLevel::READ_COMMITTED);
    auto verifier = create_session(IsolationLevel::READ_COMMITTED);

    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("select * from rc_update_latest where id = 1;"));

    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("update rc_update_latest set val = 200 where id = 1;"));
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    ASSERT_TRUE(t1->exec_sql_ok("update rc_update_latest set val = 300 where id = 1;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    std::string final_state = verifier->exec_sql("select * from rc_update_latest where id = 1;");
    std::string expected_final = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "|                1 |              300 |\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(final_state), expected_final);
}

TEST_F(SnapshotTest, RC_UpdateRechecksLatestVersionAfterWaitingForLock) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table rc_update_recheck (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into rc_update_recheck values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into rc_update_recheck values (2, 100);"));

    auto older = create_session(IsolationLevel::READ_COMMITTED);
    auto writer = create_session(IsolationLevel::READ_COMMITTED);
    auto verifier = create_session(IsolationLevel::READ_COMMITTED);

    ASSERT_TRUE(older->exec_sql_ok("begin;"));
    ASSERT_TRUE(older->exec_sql_ok("update rc_update_recheck set val = val + 1 where id = 2;"));

    ASSERT_TRUE(writer->exec_sql_ok("begin;"));
    ASSERT_TRUE(writer->exec_sql_ok("update rc_update_recheck set val = val + 1 where id = 1;"));

    std::atomic<bool> older_update_started{false};
    std::atomic<bool> older_update_ok{false};
    std::thread waiting_update([&]() {
        older_update_started = true;
        older_update_ok = older->exec_sql_ok("update rc_update_recheck set val = val + 1 where id = 1;");
    });

    while (!older_update_started) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_TRUE(writer->exec_sql_ok("commit;"));
    waiting_update.join();
    ASSERT_TRUE(older_update_ok);
    ASSERT_TRUE(older->exec_sql_ok("commit;"));

    std::string final_state = verifier->exec_sql("select * from rc_update_recheck where id = 1;");
    std::string expected_final = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "|                1 |              102 |\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(final_state), expected_final);
}

TEST_F(SnapshotTest, RC_PointDmlRechecksResidualPredicateAfterRecordLockWait) {
    ASSERT_EQ(std::getenv("ENABLE_POINT_DML"), nullptr) << "test requires default-on point DML semantics";

    auto run_case = [&](const std::string& table, const std::string& mutation) {
        auto setup = create_session();
        ASSERT_TRUE(setup->exec_sql_ok("create table " + table + " (id int, eligible int, payload int);"));
        ASSERT_TRUE(setup->exec_sql_ok("create index " + table + " (id);"));
        ASSERT_TRUE(setup->exec_sql_ok("insert into " + table + " values (1, 1, 10);"));

        auto owner = create_session(IsolationLevel::READ_COMMITTED);
        auto waiter = create_session(IsolationLevel::READ_COMMITTED);
        auto verifier = create_session(IsolationLevel::READ_COMMITTED);

        ASSERT_TRUE(owner->exec_sql_ok("begin;"));
        ASSERT_TRUE(owner->exec_sql_ok("update " + table + " set payload = 11 where id = 1;"));
        ASSERT_TRUE(waiter->exec_sql_ok("begin;"));

        std::promise<void> candidate_resolved;
        std::promise<void> release_waiter;
        std::shared_future<void> release = release_waiter.get_future().share();
        std::atomic<bool> has_point_access{false};
        auto mutation_result = std::async(std::launch::async, [&]() {
            try {
                waiter->exec_sql_with_portal_ready_hook(mutation, [&](const PortalStmt& portal) {
                    const auto* dml = static_cast<const DMLPlan*>(portal.plan.get());
                    has_point_access.store(dml->point_access_.has_value(), std::memory_order_release);
                    candidate_resolved.set_value();
                    release.wait();
                });
                return true;
            } catch (const RMDBError&) {
                return false;
            } catch (const TransactionAbortException&) {
                return false;
            }
        });

        auto candidate = candidate_resolved.get_future();
        const bool resolved = candidate.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
        EXPECT_TRUE(resolved) << "point DML did not resolve its indexed candidate";
        EXPECT_TRUE(has_point_access.load(std::memory_order_acquire))
            << "mutation must use indexed point access with ENABLE_POINT_DML unset";

        release_waiter.set_value();
        const bool blocked = mutation_result.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout;
        EXPECT_TRUE(blocked) << "mutation must wait for the owner's row lock";

        const bool owner_changed = owner->exec_sql_ok("update " + table + " set eligible = 0 where id = 1;");
        const bool owner_committed = owner->exec_sql_ok("commit;");
        const bool mutation_ok = mutation_result.get();
        const bool waiter_committed = waiter->exec_sql_ok("commit;");
        EXPECT_TRUE(owner_changed);
        EXPECT_TRUE(owner_committed);
        EXPECT_TRUE(mutation_ok);
        EXPECT_TRUE(waiter_committed);

        const std::string final_state = verifier->exec_sql("select * from " + table + " where id = 1;");
        const std::string expected = "+------------------+------------------+------------------+\n"
                                     "|               id |         eligible |          payload |\n"
                                     "+------------------+------------------+------------------+\n"
                                     "|                1 |                0 |               11 |\n"
                                     "+------------------+------------------+------------------+\n"
                                     "Total record(s): 1";
        EXPECT_EQ(TestSession::trim_output(final_state), expected);
    };

    run_case("rc_point_update_recheck",
             "update rc_point_update_recheck set payload = 99 where id = 1 and eligible = 1;");
    run_case("rc_point_delete_recheck", "delete from rc_point_delete_recheck where id = 1 and eligible = 1;");
}

TEST_F(SnapshotTest, SER_PureAutoCommitInsertsDoNotRetainSsiHistory) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table lifecycle_insert (id int, val int);"));

    constexpr int kInsertCount = 2000;
    for (int i = 0; i < kInsertCount; ++i) {
        ASSERT_TRUE(s->exec_sql_ok("insert into lifecycle_insert values (" + std::to_string(i) + ", " +
                                   std::to_string(i * 10) + ");"));
    }

    EXPECT_EQ(db_->txn()->DebugSsiWriteCount(), 0);
    EXPECT_LE(db_->txn()->DebugTxnMapSize(), 2);
}

TEST_F(SnapshotTest, SER_LatePredicateReadSeesOverlappingCommittedInsert) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table late_predicate (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into late_predicate values (1, 100);"));

    auto t1 = create_session();
    auto t2 = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));

    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from late_predicate where id = 1;"));
    ASSERT_TRUE(t2->exec_sql_ok("insert into late_predicate values (2, 200);"));
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    ASSERT_TRUE(t1->exec_sql_ok("select * from late_predicate where id = 2;"));
    std::string abort = t1->exec_sql_expect_abort("update late_predicate set val = 101 where id = 1;");
    EXPECT_EQ(TestSession::trim_output(abort), "abort")
        << "T1 must see T2's overlapping committed insert as an invisible predicate write";
}

TEST_F(SnapshotTest, DefaultIsolationAllowsWriteSkew) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table duty_default_iso (doctor_id int, on_call int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty_default_iso values (1, 1);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty_default_iso values (2, 1);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto verifier = create_session();

    SimpleThreadBarrier both_read(2), t1_updated(2), updates_done(2);
    bool t1_update_ok = false;
    bool t2_update_ok = false;
    bool t1_commit_ok = false;
    bool t2_commit_ok = false;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        ASSERT_TRUE(t1->exec_sql_ok("select * from duty_default_iso where doctor_id = 2;"));

        both_read.arrive_and_wait();

        t1_update_ok = t1->exec_sql_ok("update duty_default_iso set on_call = 0 where doctor_id = 1;");

        t1_updated.arrive_and_wait();
        updates_done.arrive_and_wait();

        t1_commit_ok = t1->exec_sql_ok("commit;");
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));
        ASSERT_TRUE(t2->exec_sql_ok("select * from duty_default_iso where doctor_id = 1;"));

        both_read.arrive_and_wait();
        t1_updated.arrive_and_wait();

        t2_update_ok = t2->exec_sql_ok("update duty_default_iso set on_call = 0 where doctor_id = 2;");

        updates_done.arrive_and_wait();

        t2_commit_ok = t2->exec_sql_ok("commit;");
    });

    th1.join();
    th2.join();

    EXPECT_TRUE(t1_update_ok);
    EXPECT_TRUE(t2_update_ok);
    EXPECT_TRUE(t1_commit_ok);
    EXPECT_TRUE(t2_commit_ok);

    std::string result = verifier->exec_sql("select * from duty_default_iso;");
    std::string expected = "+------------------+------------------+\n"
                           "|        doctor_id |          on_call |\n"
                           "+------------------+------------------+\n"
                           "|                1 |                0 |\n"
                           "|                2 |                0 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(result), expected);
}

// =============================================================================
// Example 1: Write-Write Conflict (SI and SER — same result)
// From: .doc/Snapshot.md 示例一, docs/测试说明文档2026.pdf pp.22-24
// =============================================================================

TEST_F(SnapshotTest, Example1_WriteWriteConflict_SI) {
    auto s = create_session();
    // Phase 1: Initialization
    ASSERT_TRUE(s->exec_sql_ok("create table account (id int, balance int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into account values (1, 100);"));

    // Two sessions for concurrent transactions
    auto t1 = create_session();
    auto t2 = create_session();
    auto t3 = create_session();

    // Thread synchronization: 2 threads (T1 + T2)
    SimpleThreadBarrier barrier(2);

    std::string t1_output, t2_output, t3_output;
    bool t1_ok = true, t2_ok = true;

    // ---- Thread T1 ----
    std::thread th1([&]() {
        t1_ok = t1->exec_sql_ok("set transaction isolation level snapshot isolation;"); // step 1
        t1_ok = t1_ok && t1->exec_sql_ok("begin;");                                     // step 3

        barrier.arrive_and_wait(); // barrier 0: after T2 step 2 (SET TRANSACTION)

        t1_ok = t1_ok && t1->exec_sql_ok("update account set balance = 120 where id = 1;"); // step 4

        barrier.arrive_and_wait(); // barrier 1: after T2 step 5 (BEGIN)

        barrier.arrive_and_wait(); // barrier 2: after T2 step 6 (UPDATE — WW conflict in T2)

        t1_ok = t1_ok && t1->exec_sql_ok("commit;"); // step 7

        barrier.arrive_and_wait(); // barrier 3: after T2 step 8 (COMMIT)
    });

    // ---- Thread T2 ----
    std::thread th2([&]() {
        t2_ok = t2->exec_sql_ok("set transaction isolation level snapshot isolation;"); // step 2

        barrier.arrive_and_wait(); // barrier 0

        t2_ok = t2_ok && t2->exec_sql_ok("begin;"); // step 5

        barrier.arrive_and_wait(); // barrier 1

        // step 6: this UPDATE should ABORT due to WW conflict with T1's uncommitted update
        t2_output = t2->exec_sql_expect_abort("update account set balance = 90 where id = 1;");

        barrier.arrive_and_wait(); // barrier 2

        // step 8: COMMIT after abort — should not make T2's write visible
        t2_ok = t2_ok && t2->exec_sql_ok("commit;");

        barrier.arrive_and_wait(); // barrier 3
    });

    th1.join();
    th2.join();

    ASSERT_TRUE(t1_ok) << "T1 should complete without errors";
    EXPECT_EQ(TestSession::trim_output(t2_output), "abort") << "T2 should abort at step 6";

    // ---- T3: verify final state (step 9) ----
    t3_output = t3->exec_sql("select * from account where id = 1;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |          balance |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              120 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(t3_output), expected) << "T3 should see (1, 120) — only T1's committed update";
}

// =============================================================================
// Example 1: Same test in SER mode
// =============================================================================

TEST_F(SnapshotTest, Example1_WriteWriteConflict_SER) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table account (id int, balance int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into account values (1, 100);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto t3 = create_session();

    SimpleThreadBarrier barrier(2);
    std::string t2_output, t3_output;
    bool t1_ok = true, t2_ok = true;

    std::thread th1([&]() {
        t1_ok = t1->exec_sql_ok("set transaction isolation level serializable;"); // step 1
        t1_ok = t1_ok && t1->exec_sql_ok("begin;");                               // step 3

        barrier.arrive_and_wait(); // barrier 0

        t1_ok = t1_ok && t1->exec_sql_ok("update account set balance = 120 where id = 1;"); // step 4

        barrier.arrive_and_wait(); // barrier 1
        barrier.arrive_and_wait(); // barrier 2

        t1_ok = t1_ok && t1->exec_sql_ok("commit;"); // step 7

        barrier.arrive_and_wait(); // barrier 3
    });

    std::thread th2([&]() {
        t2_ok = t2->exec_sql_ok("set transaction isolation level serializable;"); // step 2

        barrier.arrive_and_wait(); // barrier 0

        t2_ok = t2_ok && t2->exec_sql_ok("begin;"); // step 5

        barrier.arrive_and_wait(); // barrier 1

        t2_output = t2->exec_sql_expect_abort("update account set balance = 90 where id = 1;"); // step 6

        barrier.arrive_and_wait(); // barrier 2

        t2_ok = t2_ok && t2->exec_sql_ok("commit;"); // step 8

        barrier.arrive_and_wait(); // barrier 3
    });

    th1.join();
    th2.join();

    ASSERT_TRUE(t1_ok) << "T1 should complete without errors";
    EXPECT_EQ(TestSession::trim_output(t2_output), "abort") << "T2 should abort at step 6";

    t3_output = t3->exec_sql("select * from account where id = 1;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |          balance |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              120 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(t3_output), expected);
}

// =============================================================================
// Example 2: Transaction-Level Snapshot Consistency (SI)
// From: .doc/Snapshot.md 示例二, docs/测试说明文档2026.pdf pp.24-25
// =============================================================================

TEST_F(SnapshotTest, Example2_SnapshotConsistency_SI) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table counter_test (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into counter_test values (1, 100);"));

    auto t1 = create_session();
    auto t2 = create_session();

    SimpleThreadBarrier first_select_done(2), commit_done(2);
    std::string t1b_output, t1c_output;

    std::thread th1([&]() {
        // t1s
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        // t1a begin;
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        // t1b: first SELECT — should see val=100
        t1b_output = t1->exec_sql("select * from counter_test where id = 1;");

        first_select_done.arrive_and_wait(); // T2 may update only after T1's snapshot is established.
        commit_done.arrive_and_wait();       // T2 has committed its UPDATE.

        // t1c: second SELECT — should STILL see val=100 (snapshot consistency)
        t1c_output = t1->exec_sql("select * from counter_test where id = 1;");
        // t1d commit;
        ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    });

    std::thread th2([&]() {
        // t2a begin;
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));
        first_select_done.arrive_and_wait();
        // t2b: UPDATE
        ASSERT_TRUE(t2->exec_sql_ok("update counter_test set val = 200 where id = 1;"));
        // t2c commit;
        ASSERT_TRUE(t2->exec_sql_ok("commit;"));

        commit_done.arrive_and_wait(); // signal T1 that T2 is done
    });

    th1.join();
    th2.join();

    std::string expected_select = "+------------------+------------------+\n"
                                  "|               id |              val |\n"
                                  "+------------------+------------------+\n"
                                  "|                1 |              100 |\n"
                                  "+------------------+------------------+\n"
                                  "Total record(s): 1";

    EXPECT_EQ(TestSession::trim_output(t1b_output), expected_select) << "T1's first SELECT should see val=100";
    EXPECT_EQ(TestSession::trim_output(t1c_output), expected_select)
        << "T1's second SELECT should STILL see val=100 (snapshot consistency — no dirty read)";
}

// =============================================================================
// Example 3: Write-Skew — SI mode (both T1 and T2 succeed)
// From: .doc/Snapshot.md 示例三, docs/测试说明文档2026.pdf pp.25-28
// =============================================================================

TEST_F(SnapshotTest, Example3_WriteSkew_SI) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table duty (doctor_id int, on_call int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty values (1, 1);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty values (2, 1);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto t3 = create_session();

    SimpleThreadBarrier barrier(2);
    std::string t1_select, t2_select, t3_select;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        t1_select = t1->exec_sql("select * from duty where doctor_id = 2;"); // t1b

        barrier.arrive_and_wait(); // barrier: both have done their SELECTs

        ASSERT_TRUE(t1->exec_sql_ok("update duty set on_call = 0 where doctor_id = 1;")); // t1c
        ASSERT_TRUE(t1->exec_sql_ok("commit;"));                                          // t1d
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));
        t2_select = t2->exec_sql("select * from duty where doctor_id = 1;"); // t2b

        barrier.arrive_and_wait(); // barrier

        ASSERT_TRUE(t2->exec_sql_ok("update duty set on_call = 0 where doctor_id = 2;")); // t2c
        ASSERT_TRUE(t2->exec_sql_ok("commit;"));                                          // t2d
    });

    th1.join();
    th2.join();

    // ---- Verify intermediate SELECT outputs ----
    std::string expected_t1_select = "+------------------+------------------+\n"
                                     "|        doctor_id |          on_call |\n"
                                     "+------------------+------------------+\n"
                                     "|                2 |                1 |\n"
                                     "+------------------+------------------+\n"
                                     "Total record(s): 1";
    std::string expected_t2_select = "+------------------+------------------+\n"
                                     "|        doctor_id |          on_call |\n"
                                     "+------------------+------------------+\n"
                                     "|                1 |                1 |\n"
                                     "+------------------+------------------+\n"
                                     "Total record(s): 1";

    EXPECT_EQ(TestSession::trim_output(t1_select), expected_t1_select);
    EXPECT_EQ(TestSession::trim_output(t2_select), expected_t2_select);

    // ---- SI mode: both updates succeed, both doctors are off-call ----
    t3_select = t3->exec_sql("select * from duty;");
    std::string expected_si = "+------------------+------------------+\n"
                              "|        doctor_id |          on_call |\n"
                              "+------------------+------------------+\n"
                              "|                1 |                0 |\n"
                              "|                2 |                0 |\n"
                              "+------------------+------------------+\n"
                              "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(t3_select), expected_si) << "SI: write-skew allowed — both doctors off-call";
}

// =============================================================================
// Example 3: Write-Skew — SER mode (T2 aborts at t2c)
// =============================================================================

TEST_F(SnapshotTest, Example3_WriteSkew_SER) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table duty (doctor_id int, on_call int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty values (1, 1);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty values (2, 1);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto t3 = create_session();

    // Three barriers to enforce spec schedule: T1 UPDATE first, then T2 UPDATE
    SimpleThreadBarrier b0(2), b1(2), b2(2);
    std::string t1_select, t2_select, t2_output, t3_select;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));                              // t1a
        t1_select = t1->exec_sql("select * from duty where doctor_id = 2;"); // t1b

        b0.arrive_and_wait(); // b0: both SELECTs done

        // t1c: UPDATE — establishes T2 ->rw T1
        ASSERT_TRUE(t1->exec_sql_ok("update duty set on_call = 0 where doctor_id = 1;"));

        b1.arrive_and_wait(); // b1: T1 UPDATE done; T2 can now UPDATE
        b2.arrive_and_wait(); // b2: T2 UPDATE done; both can now COMMIT

        ASSERT_TRUE(t1->exec_sql_ok("commit;")); // t1d — T1 commits
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));                              // t2a
        t2_select = t2->exec_sql("select * from duty where doctor_id = 1;"); // t2b

        b0.arrive_and_wait(); // b0
        b1.arrive_and_wait(); // b1: wait for T1's UPDATE

        // t2c: UPDATE — T1 ->rw T2 (T1 read id_2, T2 writes id_2)
        // + existing T2 ->rw T1 → bidirectional ring → T2 aborts!
        t2_output = t2->exec_sql_expect_abort("update duty set on_call = 0 where doctor_id = 2;");

        b2.arrive_and_wait(); // b2: signal T1

        ASSERT_TRUE(t2->exec_sql_ok("commit;")); // t2d

        ASSERT_TRUE(t2->exec_sql_ok("commit;")); // t2d — should be no-op after abort
    });

    th1.join();
    th2.join();

    // ---- Verify intermediate SELECT outputs (same snapshot for both levels) ----
    std::string expected_t1_select = "+------------------+------------------+\n"
                                     "|        doctor_id |          on_call |\n"
                                     "+------------------+------------------+\n"
                                     "|                2 |                1 |\n"
                                     "+------------------+------------------+\n"
                                     "Total record(s): 1";
    std::string expected_t2_select = "+------------------+------------------+\n"
                                     "|        doctor_id |          on_call |\n"
                                     "+------------------+------------------+\n"
                                     "|                1 |                1 |\n"
                                     "+------------------+------------------+\n"
                                     "Total record(s): 1";

    EXPECT_EQ(TestSession::trim_output(t1_select), expected_t1_select);
    EXPECT_EQ(TestSession::trim_output(t2_select), expected_t2_select);

    // ---- SER mode: T2's t2c should abort ----
    EXPECT_EQ(TestSession::trim_output(t2_output), "abort") << "SER: t2c should abort due to SSI danger structure";

    // ---- T3: verify final state — only T1's update visible ----
    t3_select = t3->exec_sql("select * from duty;");
    std::string expected_ser = "+------------------+------------------+\n"
                               "|        doctor_id |          on_call |\n"
                               "+------------------+------------------+\n"
                               "|                1 |                0 |\n"
                               "|                2 |                1 |\n"
                               "+------------------+------------------+\n"
                               "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(t3_select), expected_ser)
        << "SER: T2 rolled back — only T1's update applied, doctor 2 still on-call";
}

// =============================================================================
// si/DirtyReadTest — uncommitted writes invisible to other transactions
// =============================================================================

TEST_F(SnapshotTest, SI_DirtyReadPrevented) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session(); // writer
    auto t2 = create_session(); // reader

    SimpleThreadBarrier barrier(2);
    std::string t2_read;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        // Update but don't commit
        ASSERT_TRUE(t1->exec_sql_ok("update t set val = 999 where id = 1;"));

        barrier.arrive_and_wait(); // allow T2 to read
        barrier.arrive_and_wait(); // wait for T2 to finish reading

        ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));

        barrier.arrive_and_wait(); // wait for T1's uncommitted update

        t2_read = t2->exec_sql("select * from t where id = 1;");

        barrier.arrive_and_wait(); // signal T1

        ASSERT_TRUE(t2->exec_sql_ok("commit;"));
    });

    th1.join();
    th2.join();

    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              100 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(t2_read), expected)
        << "T2 should NOT see T1's uncommitted update (dirty read prevented)";
}

// =============================================================================
// si/InsertTest — insert self-visibility + commit visibility
// =============================================================================

TEST_F(SnapshotTest, SI_InsertSelfVisibility) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, name char(8));"));

    auto t1 = create_session();
    auto t2 = create_session();

    SimpleThreadBarrier barrier(2);
    std::string t1_self_read, t2_concurrent_read, t2_after_commit_read;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        // Insert but don't commit yet
        ASSERT_TRUE(t1->exec_sql_ok("insert into t values (1, 'alice');"));
        // Self-read: should see own uncommitted insert
        t1_self_read = t1->exec_sql("select * from t;");

        barrier.arrive_and_wait(); // allow T2 concurrent read
        barrier.arrive_and_wait(); // wait for T2 concurrent read

        ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));

        barrier.arrive_and_wait(); // wait for T1 insert

        // Concurrent read while T1 uncommitted
        t2_concurrent_read = t2->exec_sql("select * from t;");

        barrier.arrive_and_wait(); // signal T1

        ASSERT_TRUE(t2->exec_sql_ok("commit;"));
    });

    th1.join();
    th2.join();

    // T1 should see own uncommitted insert
    std::string expected_with_data = "+------------------+------------------+\n"
                                     "|               id |             name |\n"
                                     "+------------------+------------------+\n"
                                     "|                1 |            alice |\n"
                                     "+------------------+------------------+\n"
                                     "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(t1_self_read), expected_with_data) << "T1 should see its own uncommitted insert";

    // T2 should NOT see T1's uncommitted insert
    std::string expected_empty = "+------------------+------------------+\n"
                                 "|               id |             name |\n"
                                 "+------------------+------------------+\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 0";
    EXPECT_EQ(TestSession::trim_output(t2_concurrent_read), expected_empty)
        << "T2 should NOT see T1's uncommitted insert";

    // After T1 commit, T2's new transaction should see the data
    t2_after_commit_read = t2->exec_sql("select * from t;");
    EXPECT_EQ(TestSession::trim_output(t2_after_commit_read), expected_with_data)
        << "After T1 commit, a new transaction should see the insert";
}

// =============================================================================
// si/WriteWriteConflictUpdateTest — two concurrent updates on same record
// =============================================================================

TEST_F(SnapshotTest, SI_WriteWriteConflict_Update) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 200);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto t3 = create_session();

    SimpleThreadBarrier barrier(2);
    std::string t2_result;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        ASSERT_TRUE(t1->exec_sql_ok("update t set val = 150 where id = 1;"));

        barrier.arrive_and_wait(); // allow T2 to attempt update
        barrier.arrive_and_wait(); // wait for T2 result

        ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));

        barrier.arrive_and_wait(); // wait for T1's update

        t2_result = t2->exec_sql_expect_abort("update t set val = 199 where id = 1;");

        barrier.arrive_and_wait(); // signal T1

        ASSERT_TRUE(t2->exec_sql_ok("commit;"));
    });

    th1.join();
    th2.join();

    EXPECT_EQ(TestSession::trim_output(t2_result), "abort") << "T2 should abort — WW conflict on id=1";

    // Verify only T1's update is visible
    std::string final_state = t3->exec_sql("select * from t where id = 1;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              150 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(final_state), expected) << "Only T1's update should be visible";
}

// =============================================================================
// ser/UnrepeatableReadTest — repeatable reads under SER with SSI dependency
// =============================================================================

TEST_F(SnapshotTest, SER_UnrepeatableRead) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session(); // reader
    auto t2 = create_session(); // writer

    SimpleThreadBarrier barrier(2);
    std::string t1_first, t1_second;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        t1_first = t1->exec_sql("select * from t where id = 1;");

        barrier.arrive_and_wait(); // allow T2 to update+commit
        barrier.arrive_and_wait(); // wait for T2

        // Second read — should return same snapshot value (repeatable read)
        t1_second = t1->exec_sql("select * from t where id = 1;");
        ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));

        barrier.arrive_and_wait(); // wait for T1's first read

        ASSERT_TRUE(t2->exec_sql_ok("update t set val = 200 where id = 1;"));
        ASSERT_TRUE(t2->exec_sql_ok("commit;"));

        barrier.arrive_and_wait(); // signal T1
    });

    th1.join();
    th2.join();

    std::string expected_first = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "|                1 |              100 |\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 1";

    EXPECT_EQ(TestSession::trim_output(t1_first), expected_first);
    EXPECT_EQ(TestSession::trim_output(t1_second), expected_first)
        << "T1's second read MUST see the same snapshot value (repeatable read)";
}

// =============================================================================
// ser/PhantomReadTest — range read + predicate dependency
// =============================================================================

TEST_F(SnapshotTest, SER_PhantomRead) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (3, 300);"));

    auto t1 = create_session(); // range reader
    auto t2 = create_session(); // inserter

    SimpleThreadBarrier barrier(2);
    std::string t1_first, t1_second, t2_result;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        // Range read: SELECT * FROM t WHERE val > 150
        t1_first = t1->exec_sql("select * from t where val > 150;");

        barrier.arrive_and_wait(); // allow T2 to insert
        barrier.arrive_and_wait(); // wait for T2

        // Repeat range read — should see same snapshot
        t1_second = t1->exec_sql("select * from t where val > 150;");
        ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));

        barrier.arrive_and_wait(); // wait for T1's range read

        // Insert a row that falls into T1's range predicate (val > 150)
        ASSERT_TRUE(t2->exec_sql_ok("insert into t values (2, 250);"));
        ASSERT_TRUE(t2->exec_sql_ok("commit;"));

        barrier.arrive_and_wait(); // signal T1
    });

    th1.join();
    th2.join();

    // T1's first range read: only id=3 (val=300 > 150)
    std::string expected_first = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "|                3 |              300 |\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 1";

    EXPECT_EQ(TestSession::trim_output(t1_first), expected_first);
    EXPECT_EQ(TestSession::trim_output(t1_second), expected_first)
        << "T1's second range read MUST not see phantom insert";
}

// =============================================================================
// ser/PhantomReadTest — empty result predicate dependency
// =============================================================================

TEST_F(SnapshotTest, SER_PhantomRead_EmptyResult) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session(); // reader (empty result)
    auto t2 = create_session(); // inserter

    SimpleThreadBarrier barrier(2);
    std::string t1_first, t1_second, t2_result;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        // Query that returns empty — predicate read still saved
        t1_first = t1->exec_sql("select * from t where id = 2;");

        barrier.arrive_and_wait(); // allow T2 to insert
        barrier.arrive_and_wait(); // wait for T2

        t1_second = t1->exec_sql("select * from t where id = 2;");
        ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));

        barrier.arrive_and_wait(); // wait for T1

        // Insert id=2 (matches T1's empty-result predicate)
        ASSERT_TRUE(t2->exec_sql_ok("insert into t values (2, 200);"));
        ASSERT_TRUE(t2->exec_sql_ok("commit;"));

        barrier.arrive_and_wait(); // signal T1
    });

    th1.join();
    th2.join();

    std::string expected_empty = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 0";

    EXPECT_EQ(TestSession::trim_output(t1_first), expected_empty);
    EXPECT_EQ(TestSession::trim_output(t1_second), expected_empty)
        << "T1's repeat empty-result query should remain empty (snapshot consistency)";
}

// =============================================================================
// ser/SelectDangerousTest — SELECT triggering SSI danger structure
// =============================================================================
// Scenario:
// T1 reads id=1, T2 writes id=1 (T1 ->rw T2). T2 reads id=2.
// T1 writes id=3 (no one read id=3 → no new rw-edge from write-side check).
// T2 SELECTs id=3 → invisible write check: T1 wrote id=3 → T2 ->rw T1.
// Now T1 ->rw T2 AND T2 ->rw T1 → bidirectional ring → T2's SELECT aborts.
// This tests the reader-side invisible write check (spec §3).

TEST_F(SnapshotTest, SER_SelectDangerousStructure) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 200);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (3, 300);"));

    // Sequential version: T1 reads id=1, then T2 writes id=1 + reads id=2,
    // then T1 writes id=3 (should succeed — no one read id=3),
    // then T2 SELECTs id=3 (should abort via invisible write check).
    auto t1 = create_session();
    auto t2 = create_session();

    // T1: SER, BEGIN, SELECT id=1
    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("select * from t where id = 1;"));

    // T2: SER, BEGIN, UPDATE id=1 (creates T1 ->rw T2), SELECT id=2
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("update t set val = 888 where id = 1;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from t where id = 2;"));

    // T1: UPDATE id=3 — should succeed (no one read id=3)
    ASSERT_TRUE(t1->exec_sql_ok("update t set val = 999 where id = 3;"));

    // T2: SELECT id=3 — invisible write check: T1 wrote id=3 → T2 ->rw T1
    // Now T1 ->rw T2 AND T2 ->rw T1 → danger → T2 aborts!
    std::string t2_result = t2->exec_sql_expect_abort("select * from t where id = 3;");

    EXPECT_EQ(TestSession::trim_output(t2_result), "abort")
        << "T2's SELECT should abort — SSI danger structure via invisible write check";

    ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));
}

// =============================================================================
// Regression: rollback must restore the original commit timestamp, not make
// a post-snapshot row visible to an older transaction.
// =============================================================================

TEST_F(SnapshotTest, SI_AbortRestoresOriginalCommitTimestampForEarlierSnapshots) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));

    auto old_reader = create_session();
    auto inserter = create_session();
    auto updater = create_session();

    ASSERT_TRUE(old_reader->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(old_reader->exec_sql_ok("begin;"));

    ASSERT_TRUE(inserter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(inserter->exec_sql_ok("begin;"));
    ASSERT_TRUE(inserter->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(inserter->exec_sql_ok("commit;"));

    ASSERT_TRUE(updater->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(updater->exec_sql_ok("begin;"));
    ASSERT_TRUE(updater->exec_sql_ok("update t set val = 200 where id = 1;"));
    ASSERT_TRUE(updater->exec_sql_ok("abort;"));

    std::string out = old_reader->exec_sql("select * from t where id = 1;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 0";
    EXPECT_EQ(TestSession::trim_output(out), expected)
        << "The row was inserted after old_reader started, so aborting a later update must not make it visible";

    ASSERT_TRUE(old_reader->exec_sql_ok("commit;"));
}

// =============================================================================
// Regression: SI delete must preserve old versions for active snapshots
// =============================================================================

TEST_F(SnapshotTest, SI_DeleteKeepsOldVersionVisibleToEarlierSnapshot) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session();
    auto t2 = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));

    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("delete from t where id = 1;"));
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    std::string out = t1->exec_sql("select * from t where id = 1;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              100 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(out), expected)
        << "A delete committed after T1 starts must not remove the old version from T1's snapshot";

    ASSERT_TRUE(t1->exec_sql_ok("commit;"));
}

TEST_F(SnapshotTest, SI_StaleSnapshotDeleteAfterCommittedDeleteAborts) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto stale_txn = create_session();
    auto deleter = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(stale_txn->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(stale_txn->exec_sql_ok("begin;"));
    std::string stale_read = stale_txn->exec_sql("select * from t where id = 1;");

    ASSERT_TRUE(deleter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(deleter->exec_sql_ok("begin;"));
    ASSERT_TRUE(deleter->exec_sql_ok("delete from t where id = 1;"));
    ASSERT_TRUE(deleter->exec_sql_ok("commit;"));

    std::string delete_abort = stale_txn->exec_sql_expect_abort("delete from t where id = 1;");
    EXPECT_EQ(TestSession::trim_output(delete_abort), "abort")
        << "Deleting a row from an old snapshot must abort after another transaction committed its delete";
    ASSERT_TRUE(stale_txn->exec_sql_ok("commit;"));

    std::string expected_stale_read = "+------------------+------------------+\n"
                                      "|               id |              val |\n"
                                      "+------------------+------------------+\n"
                                      "|                1 |              100 |\n"
                                      "+------------------+------------------+\n"
                                      "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(stale_read), expected_stale_read);

    std::string final_state = verifier->exec_sql("select * from t;");
    std::string expected_final = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 0";
    EXPECT_EQ(TestSession::trim_output(final_state), expected_final);
}

TEST_F(SnapshotTest, SI_StaleSnapshotUpdateAfterCommittedDeleteAborts) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto stale_txn = create_session();
    auto deleter = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(stale_txn->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(stale_txn->exec_sql_ok("begin;"));
    ASSERT_TRUE(stale_txn->exec_sql_ok("select * from t where id = 1;"));

    ASSERT_TRUE(deleter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(deleter->exec_sql_ok("begin;"));
    ASSERT_TRUE(deleter->exec_sql_ok("delete from t where id = 1;"));
    ASSERT_TRUE(deleter->exec_sql_ok("commit;"));

    std::string update_abort = stale_txn->exec_sql_expect_abort("update t set val = 200 where id = 1;");
    EXPECT_EQ(TestSession::trim_output(update_abort), "abort")
        << "Updating a row from an old snapshot must abort after another transaction committed its delete";
    ASSERT_TRUE(stale_txn->exec_sql_ok("commit;"));

    std::string final_state = verifier->exec_sql("select * from t;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 0";
    EXPECT_EQ(TestSession::trim_output(final_state), expected);
}

TEST_F(SnapshotTest, SI_DeleteSelfVisibilityAndRollback) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 200);"));

    auto deleter = create_session();
    ASSERT_TRUE(deleter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(deleter->exec_sql_ok("begin;"));
    ASSERT_TRUE(deleter->exec_sql_ok("delete from t where id = 1;"));

    std::string self_read = deleter->exec_sql("select * from t where id = 1;");
    std::string expected_empty = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 0";
    EXPECT_EQ(TestSession::trim_output(self_read), expected_empty)
        << "A transaction must not see a row after deleting it itself";

    ASSERT_TRUE(deleter->exec_sql_ok("abort;"));

    auto updater = create_session();
    ASSERT_TRUE(updater->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(updater->exec_sql_ok("begin;"));
    ASSERT_TRUE(updater->exec_sql_ok("update t set val = 111 where id = 1;"));
    ASSERT_TRUE(updater->exec_sql_ok("delete from t where id = 2;"));
    ASSERT_TRUE(updater->exec_sql_ok("insert into t values (3, 300);"));
    ASSERT_TRUE(updater->exec_sql_ok("abort;"));

    auto verifier = create_session();
    std::string final_state = verifier->exec_sql("select * from t;");
    std::string expected_final = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "|                1 |              100 |\n"
                                 "|                2 |              200 |\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(final_state), expected_final)
        << "Explicit SI abort must roll back INSERT, UPDATE, and DELETE effects";
}

// =============================================================================
// Regression: SER empty-result predicate reads participate in SSI danger checks
// =============================================================================

TEST_F(SnapshotTest, SER_EmptyPredicateInsertCompletesDangerousStructure) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session();
    auto t2 = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("select * from t where id = 2;"));

    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from t where id = 1;"));

    ASSERT_TRUE(t1->exec_sql_ok("update t set val = 101 where id = 1;"));

    std::string t2_abort = t2->exec_sql_expect_abort("insert into t values (2, 200);");
    EXPECT_EQ(TestSession::trim_output(t2_abort), "abort")
        << "T2's INSERT matches T1's empty predicate read and completes T2 ->rw T1 ->rw T2";

    ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));
}

TEST_F(SnapshotTest, SI_IndexScanFindsHistoricalIndexedKeyVersion) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create index t (id);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto reader = create_session();
    auto writer = create_session();

    ASSERT_TRUE(reader->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(reader->exec_sql_ok("begin;"));

    ASSERT_TRUE(writer->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(writer->exec_sql_ok("begin;"));
    ASSERT_TRUE(writer->exec_sql_ok("update t set id = 2 where id = 1;"));
    ASSERT_TRUE(writer->exec_sql_ok("commit;"));

    std::string out = reader->exec_sql("select * from t where id = 1;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              100 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(out), expected)
        << "Index scans in a transaction must not miss the visible old version after the current index key changes";

    ASSERT_TRUE(reader->exec_sql_ok("commit;"));
}

TEST_F(SnapshotTest, RC_IndexScanSeesOldKeyDuringUncommittedUpdate) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create index t (id);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto writer = create_session(IsolationLevel::READ_COMMITTED);
    auto reader = create_session(IsolationLevel::READ_COMMITTED);

    ASSERT_TRUE(writer->exec_sql_ok("begin;"));
    ASSERT_TRUE(writer->exec_sql_ok("update t set id = 2 where id = 1;"));

    std::string out = reader->exec_sql("select * from t where id = 1;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              100 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(out), expected)
        << "RC index scans must retain an uncommitted writer's old indexed key";

    ASSERT_TRUE(writer->exec_sql_ok("rollback;"));
}

TEST_F(SnapshotTest, RC_IndexRangeScanSeesOldKeyDuringUncommittedUpdate) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create index t (id);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 200);"));

    auto writer = create_session(IsolationLevel::READ_COMMITTED);
    auto reader = create_session(IsolationLevel::READ_COMMITTED);
    ASSERT_TRUE(writer->exec_sql_ok("begin;"));
    ASSERT_TRUE(writer->exec_sql_ok("update t set id = 20 where id = 2;"));

    std::string out = reader->exec_sql("select * from t where id >= 1 and id <= 10;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              100 |\n"
                           "|                2 |              200 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(out), expected)
        << "RC range scans must merge historical RIDs after an indexed key change";

    ASSERT_TRUE(writer->exec_sql_ok("rollback;"));
}

TEST_F(SnapshotTest, RC_IndexScanSeesDeletedKeyDuringUncommittedDelete) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create index t (id);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto writer = create_session(IsolationLevel::READ_COMMITTED);
    auto reader = create_session(IsolationLevel::READ_COMMITTED);

    ASSERT_TRUE(writer->exec_sql_ok("begin;"));
    ASSERT_TRUE(writer->exec_sql_ok("delete from t where id = 1;"));

    std::string out = reader->exec_sql("select * from t where id = 1;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              100 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(out), expected)
        << "RC index scans must retain an uncommitted delete's indexed key";

    ASSERT_TRUE(writer->exec_sql_ok("rollback;"));
}

TEST_F(SnapshotTest, SER_IndexScanFindsHistoricalIndexedKeyVersion) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create index t (id);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto reader = create_session();
    auto writer = create_session();

    ASSERT_TRUE(reader->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(reader->exec_sql_ok("begin;"));

    ASSERT_TRUE(writer->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(writer->exec_sql_ok("begin;"));
    ASSERT_TRUE(writer->exec_sql_ok("update t set id = 2 where id = 1;"));
    ASSERT_TRUE(writer->exec_sql_ok("commit;"));

    std::string out = reader->exec_sql("select * from t where id = 1;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              100 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(out), expected)
        << "Serializable index scans in an explicit transaction must not miss the visible old key";

    ASSERT_TRUE(reader->exec_sql_ok("commit;"));
}

TEST_F(SnapshotTest, SER_EmptySelectDetectsInvisibleInsertAndDoesNotWriteOutput) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session();
    auto t2 = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("select * from t where id = 1;"));

    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("update t set val = 101 where id = 1;"));

    ASSERT_TRUE(t1->exec_sql_ok("insert into t values (2, 200);"));

    std::remove("output.txt");
    std::string abort_out = t2->exec_sql_expect_abort("select * from t where id = 2;");
    EXPECT_EQ(TestSession::trim_output(abort_out), "abort");

    std::ifstream output_file("output.txt");
    std::stringstream contents;
    contents << output_file.rdbuf();
    EXPECT_EQ(contents.str(), "") << "An aborting SELECT must not leave a partial result table in output.txt";

    ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));
}

TEST_F(SnapshotTest, SER_CommittedOverlappingReaderStillParticipatesInDangerCheck) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session();
    auto t2 = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("select * from t where id = 2;"));

    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from t where id = 1;"));

    ASSERT_TRUE(t1->exec_sql_ok("update t set val = 101 where id = 1;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    std::string abort_out = t2->exec_sql_expect_abort("insert into t values (2, 200);");
    EXPECT_EQ(TestSession::trim_output(abort_out), "abort")
        << "T1 committed after T2 started, so its predicate read must still be retained for SSI";

    ASSERT_TRUE(t2->exec_sql_ok("commit;"));
}

TEST_F(SnapshotTest, SER_NewEdgeToWriterWithCommittedOutgoingEdgeIsDangerous) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto reader = create_session();
    auto writer = create_session();
    auto third = create_session();

    ASSERT_TRUE(reader->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(reader->exec_sql_ok("begin;"));
    ASSERT_TRUE(reader->exec_sql_ok("select * from t where id = 3;"));

    ASSERT_TRUE(writer->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(writer->exec_sql_ok("begin;"));
    ASSERT_TRUE(writer->exec_sql_ok("select * from t where id = 1;"));

    ASSERT_TRUE(third->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(third->exec_sql_ok("begin;"));
    ASSERT_TRUE(third->exec_sql_ok("update t set val = 101 where id = 1;"));
    ASSERT_TRUE(third->exec_sql_ok("commit;"));

    std::string abort_out = writer->exec_sql_expect_abort("insert into t values (3, 300);");
    EXPECT_EQ(TestSession::trim_output(abort_out), "abort")
        << "Adding reader -> writer must abort when writer already has an outgoing edge to an earlier-committed Tout";

    ASSERT_TRUE(reader->exec_sql_ok("commit;"));
    ASSERT_TRUE(writer->exec_sql_ok("commit;"));
}

// =============================================================================
// si/Deadlock — concurrent updates on same record: detects WW conflict
// without deadlocking (regression: buffer pool pin leak)
// =============================================================================

TEST_F(SnapshotTest, SI_Deadlock_ConcurrentUpdatesSameRecord) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto t3 = create_session();

    SimpleThreadBarrier b0(2), b1(2);
    std::string t1_result, t2_result;
    bool t1_ok = true, t2_ok = true;

    std::thread th1([&]() {
        t1_ok = t1->exec_sql_ok("set transaction isolation level snapshot isolation;");
        t1_ok = t1_ok && t1->exec_sql_ok("begin;");
        b0.arrive_and_wait();
        t1_result = t1->exec_sql_expect_abort("update t set val = 200 where id = 1;");
        b1.arrive_and_wait();
        t1_ok = t1_ok && t1->exec_sql_ok("commit;");
    });

    std::thread th2([&]() {
        t2_ok = t2->exec_sql_ok("set transaction isolation level snapshot isolation;");
        t2_ok = t2_ok && t2->exec_sql_ok("begin;");
        b0.arrive_and_wait();
        t2_result = t2->exec_sql_expect_abort("update t set val = 300 where id = 1;");
        b1.arrive_and_wait();
        t2_ok = t2_ok && t2->exec_sql_ok("commit;");
    });

    th1.join();
    th2.join();

    ASSERT_TRUE(t1_ok && t2_ok) << "Both threads should complete without exceptions";
    // At least one must abort due to WW conflict — not deadlock
    bool one_aborted =
        (TestSession::trim_output(t1_result) == "abort") != (TestSession::trim_output(t2_result) == "abort");
    EXPECT_TRUE(one_aborted) << "Exactly one transaction should abort (WW conflict), both got: t1=" << t1_result
                             << " t2=" << t2_result;

    std::string final_state = t3->exec_sql("select * from t;");
    bool has_200 = final_state.find("200") != std::string::npos;
    bool has_300 = final_state.find("300") != std::string::npos;
    EXPECT_NE(has_200, has_300) << "Exactly one update should be visible";
}

// =============================================================================
// si/Deadlock — two transactions each update different records, then try
// the other's record; one should be aborted by WW conflict
// =============================================================================

TEST_F(SnapshotTest, SI_Deadlock_CrossRecordUpdates) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 200);"));

    auto t1 = create_session();
    auto t2 = create_session();

    SimpleThreadBarrier b0(2), b1(2);
    std::string t1_result, t2_result;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        ASSERT_TRUE(t1->exec_sql_ok("update t set val = 150 where id = 1;"));
        b0.arrive_and_wait();
        t1_result = t1->exec_sql_expect_abort("update t set val = 250 where id = 2;");
        b1.arrive_and_wait();
        ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));
        ASSERT_TRUE(t2->exec_sql_ok("update t set val = 299 where id = 2;"));
        b0.arrive_and_wait();
        t2_result = t2->exec_sql_expect_abort("update t set val = 199 where id = 1;");
        b1.arrive_and_wait();
        ASSERT_TRUE(t2->exec_sql_ok("commit;"));
    });

    th1.join();
    th2.join();

    bool t1_aborted = TestSession::trim_output(t1_result) == "abort";
    bool t2_aborted = TestSession::trim_output(t2_result) == "abort";
    EXPECT_TRUE(t1_aborted || t2_aborted)
        << "Cross-record conflict should abort at least one transaction, not deadlock";
}

TEST_F(SnapshotTest, SI_Deadlock_CrossRecordUpdatesExactlyOneVictim) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 200);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto verifier = create_session();

    SimpleThreadBarrier b0(2), b1(2);
    std::string t1_result, t2_result;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        ASSERT_TRUE(t1->exec_sql_ok("update t set val = 150 where id = 1;"));
        b0.arrive_and_wait();
        t1_result = t1->exec_sql_expect_abort("update t set val = 250 where id = 2;");
        b1.arrive_and_wait();
        ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));
        ASSERT_TRUE(t2->exec_sql_ok("update t set val = 299 where id = 2;"));
        b0.arrive_and_wait();
        t2_result = t2->exec_sql_expect_abort("update t set val = 199 where id = 1;");
        b1.arrive_and_wait();
        ASSERT_TRUE(t2->exec_sql_ok("commit;"));
    });

    th1.join();
    th2.join();

    int abort_count = 0;
    if (TestSession::trim_output(t1_result) == "abort") {
        abort_count++;
    }
    if (TestSession::trim_output(t2_result) == "abort") {
        abort_count++;
    }
    EXPECT_EQ(abort_count, 1) << "Deadlock-style cross writes should pick exactly one aborting transaction";

    std::string final_state = TestSession::trim_output(verifier->exec_sql("select * from t;"));
    std::string t1_wins = "+------------------+------------------+\n"
                          "|               id |              val |\n"
                          "+------------------+------------------+\n"
                          "|                1 |              150 |\n"
                          "|                2 |              250 |\n"
                          "+------------------+------------------+\n"
                          "Total record(s): 2";
    std::string t2_wins = "+------------------+------------------+\n"
                          "|               id |              val |\n"
                          "+------------------+------------------+\n"
                          "|                1 |              199 |\n"
                          "|                2 |              299 |\n"
                          "+------------------+------------------+\n"
                          "Total record(s): 2";
    EXPECT_TRUE(final_state == t1_wins || final_state == t2_wins)
        << "Final state should contain exactly the surviving transaction's writes, got:\n"
        << final_state;
}

// =============================================================================
// si/Non_Repeatable_Read_Lost_Update — T1 reads, T2 updates+commits,
// T1 tries stale update → abort
// =============================================================================

TEST_F(SnapshotTest, SI_NonRepeatableRead_LostUpdate) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto t3 = create_session();

    SimpleThreadBarrier b0(2), b1(2);
    std::string t1_read, t1_result;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        t1_read = t1->exec_sql("select * from t where id = 1;");
        b0.arrive_and_wait();
        b1.arrive_and_wait();
        // T1's snapshot sees val=100; update based on stale read must abort
        t1_result = t1->exec_sql_expect_abort("update t set val = 150 where id = 1;");
        ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));
        b0.arrive_and_wait();
        ASSERT_TRUE(t2->exec_sql_ok("update t set val = 200 where id = 1;"));
        ASSERT_TRUE(t2->exec_sql_ok("commit;"));
        b1.arrive_and_wait();
    });

    th1.join();
    th2.join();

    std::string expected_read = "+------------------+------------------+\n"
                                "|               id |              val |\n"
                                "+------------------+------------------+\n"
                                "|                1 |              100 |\n"
                                "+------------------+------------------+\n"
                                "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(t1_read), expected_read);
    EXPECT_EQ(TestSession::trim_output(t1_result), "abort") << "T1's update based on stale snapshot should abort";

    std::string final_state = t3->exec_sql("select * from t where id = 1;");
    std::string expected_final = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "|                1 |              200 |\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(final_state), expected_final);
}

// =============================================================================
// si/UpdateTest — self-visibility: transaction sees own uncommitted UPDATE
// =============================================================================

TEST_F(SnapshotTest, SI_UpdateTest_SelfVisibility) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session();
    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("update t set val = 200 where id = 1;"));
    std::string self_read = t1->exec_sql("select * from t where id = 1;");
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              200 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(self_read), expected) << "T1 should see its own uncommitted UPDATE";
}

// =============================================================================
// si/UpdateTest — post-commit visibility: T2 starts after T1 commits
// =============================================================================

TEST_F(SnapshotTest, SI_UpdateTest_PostCommitVisibility) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session();
    auto t2 = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("update t set val = 200 where id = 1;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    std::string t2_read = t2->exec_sql("select * from t where id = 1;");
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              200 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(t2_read), expected);
}

// =============================================================================
// si/UpdateTest — self-referential UPDATE uses the original visible row
// =============================================================================

TEST_F(SnapshotTest, SI_UpdateTest_SelfReferentialUpdate) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table self_update_test (id int, score int, bonus float);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into self_update_test values (1, 10, 1.5);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into self_update_test values (2, 20, 2.5);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into self_update_test values (3, 30, 3.5);"));

    auto txn = create_session();
    ASSERT_TRUE(txn->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(txn->exec_sql_ok("begin;"));
    ASSERT_TRUE(txn->exec_sql_ok("update self_update_test set score = score + 5 where id < 3;"));
    ASSERT_TRUE(txn->exec_sql_ok("update self_update_test set bonus = bonus - 0.5 where id = 1;"));

    std::string in_txn = txn->exec_sql("select * from self_update_test;");
    ASSERT_TRUE(txn->exec_sql_ok("commit;"));

    std::string expected = "+------------------+------------------+------------------+\n"
                           "|               id |            score |            bonus |\n"
                           "+------------------+------------------+------------------+\n"
                           "|                1 |               15 |         1.000000 |\n"
                           "|                2 |               25 |         2.500000 |\n"
                           "|                3 |               30 |         3.500000 |\n"
                           "+------------------+------------------+------------------+\n"
                           "Total record(s): 3";
    EXPECT_EQ(TestSession::trim_output(in_txn), expected);

    auto check = create_session();
    std::string after_commit = check->exec_sql("select * from self_update_test;");
    EXPECT_EQ(TestSession::trim_output(after_commit), expected);
}

TEST_F(SnapshotTest, SI_UpdateTest_SelfReferentialUpdateReadsOriginalRowForEachClause) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table self_update_order_test (id int, a int, b int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into self_update_order_test values (1, 10, 0);"));

    auto txn = create_session();
    ASSERT_TRUE(txn->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(txn->exec_sql_ok("begin;"));
    ASSERT_TRUE(txn->exec_sql_ok("update self_update_order_test set a = a + 1, b = a + 2 where id = 1;"));
    std::string in_txn = txn->exec_sql("select * from self_update_order_test where id = 1;");
    ASSERT_TRUE(txn->exec_sql_ok("commit;"));

    std::string expected = "+------------------+------------------+------------------+\n"
                           "|               id |                a |                b |\n"
                           "+------------------+------------------+------------------+\n"
                           "|                1 |               11 |               12 |\n"
                           "+------------------+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(in_txn), expected);
}

TEST_F(SnapshotTest, SI_UpdateTest_CompoundAssignment) {
    // col += num / col -= num 与 col = col ± num 语义一致
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table compound_update_test (id int, score int, bonus float);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into compound_update_test values (1, 10, 1.5);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into compound_update_test values (2, 20, 2.5);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into compound_update_test values (3, 30, 3.5);"));

    auto txn = create_session();
    ASSERT_TRUE(txn->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(txn->exec_sql_ok("begin;"));
    ASSERT_TRUE(txn->exec_sql_ok("update compound_update_test set score += 5 where id < 3;"));
    ASSERT_TRUE(txn->exec_sql_ok("update compound_update_test set bonus -= 0.5 where id = 1;"));

    std::string in_txn = txn->exec_sql("select * from compound_update_test;");
    ASSERT_TRUE(txn->exec_sql_ok("commit;"));

    std::string expected = "+------------------+------------------+------------------+\n"
                           "|               id |            score |            bonus |\n"
                           "+------------------+------------------+------------------+\n"
                           "|                1 |               15 |         1.000000 |\n"
                           "|                2 |               25 |         2.500000 |\n"
                           "|                3 |               30 |         3.500000 |\n"
                           "+------------------+------------------+------------------+\n"
                           "Total record(s): 3";
    EXPECT_EQ(TestSession::trim_output(in_txn), expected);

    auto check = create_session();
    std::string after_commit = check->exec_sql("select * from compound_update_test;");
    EXPECT_EQ(TestSession::trim_output(after_commit), expected);
}

// =============================================================================
// si/WriteWriteConflictDeleteInsertTest — WW conflict on concurrent DELETE
// =============================================================================

TEST_F(SnapshotTest, SI_WriteWriteConflict_DeleteConflict) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 200);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto t3 = create_session();

    SimpleThreadBarrier b0(2), b1(2);
    std::string t2_result;

    std::thread th1([&]() {
        ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t1->exec_sql_ok("begin;"));
        ASSERT_TRUE(t1->exec_sql_ok("delete from t where id = 1;"));
        b0.arrive_and_wait();
        b1.arrive_and_wait();
        ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    });

    std::thread th2([&]() {
        ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level snapshot isolation;"));
        ASSERT_TRUE(t2->exec_sql_ok("begin;"));
        b0.arrive_and_wait();
        t2_result = t2->exec_sql_expect_abort("delete from t where id = 1;");
        b1.arrive_and_wait();
        ASSERT_TRUE(t2->exec_sql_ok("commit;"));
    });

    th1.join();
    th2.join();

    EXPECT_EQ(TestSession::trim_output(t2_result), "abort") << "T2 should abort: WW conflict on delete of same record";

    std::string final_state = t3->exec_sql("select * from t;");
    // T1 deleted id=1, id=2 remains
    bool has_id2 = final_state.find("200") != std::string::npos;
    EXPECT_TRUE(has_id2) << "Record id=2 should still be present";
}

// =============================================================================
// si/WriteWriteConflictDeleteInsertTest — delete then insert in different txns
// =============================================================================

TEST_F(SnapshotTest, SI_WriteWriteConflict_DeleteThenInsertDifferentTxn) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto t3 = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("delete from t where id = 1;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    // T2 inserts after T1 commits — new slot, no conflict
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("insert into t values (1, 200);"));
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    std::string final_state = t3->exec_sql("select * from t;");
    bool has_200 = final_state.find("200") != std::string::npos;
    EXPECT_TRUE(has_200) << "T2's insert should be visible after T1's delete";
}

TEST_F(SnapshotTest, SI_WriteWriteConflict_UncommittedDeleteThenInsertSameTupleNoIndex) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto deleter = create_session();
    auto inserter = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(deleter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(deleter->exec_sql_ok("begin;"));
    ASSERT_TRUE(deleter->exec_sql_ok("delete from t where id = 1 and val = 100;"));

    ASSERT_TRUE(inserter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(inserter->exec_sql_ok("begin;"));
    std::string insert_abort = inserter->exec_sql_expect_abort("insert into t values (1, 100);");
    EXPECT_EQ(TestSession::trim_output(insert_abort), "abort")
        << "A concurrent insert of the exact tuple deleted by another txn must abort even without indexes";
    ASSERT_TRUE(inserter->exec_sql_ok("commit;"));

    ASSERT_TRUE(deleter->exec_sql_ok("commit;"));

    std::string final_state = verifier->exec_sql("select * from t;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 0";
    EXPECT_EQ(TestSession::trim_output(final_state), expected);
}

TEST_F(SnapshotTest, SI_WriteWriteConflict_UncommittedDeleteThenInsertDifferentTupleNoIndexAllowed) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto deleter = create_session();
    auto inserter = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(deleter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(deleter->exec_sql_ok("begin;"));
    ASSERT_TRUE(deleter->exec_sql_ok("delete from t where id = 1 and val = 100;"));

    ASSERT_TRUE(inserter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(inserter->exec_sql_ok("begin;"));
    ASSERT_TRUE(inserter->exec_sql_ok("insert into t values (1, 200);"));
    ASSERT_TRUE(inserter->exec_sql_ok("commit;"));

    ASSERT_TRUE(deleter->exec_sql_ok("commit;"));

    std::string final_state = verifier->exec_sql("select * from t;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              200 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(final_state), expected);
}

TEST_F(SnapshotTest, SI_WriteWriteConflict_PostSnapshotDeleteThenInsertSameTupleNoIndex) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto stale_txn = create_session();
    auto deleter = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(stale_txn->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(stale_txn->exec_sql_ok("begin;"));
    std::string stale_read = stale_txn->exec_sql("select * from t where id = 1 and val = 100;");

    ASSERT_TRUE(deleter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(deleter->exec_sql_ok("begin;"));
    ASSERT_TRUE(deleter->exec_sql_ok("delete from t where id = 1 and val = 100;"));
    ASSERT_TRUE(deleter->exec_sql_ok("commit;"));

    std::string insert_abort = stale_txn->exec_sql_expect_abort("insert into t values (1, 100);");
    EXPECT_EQ(TestSession::trim_output(insert_abort), "abort")
        << "A stale snapshot cannot reinsert the exact tuple deleted by a concurrent transaction";
    ASSERT_TRUE(stale_txn->exec_sql_ok("commit;"));

    std::string expected_stale_read = "+------------------+------------------+\n"
                                      "|               id |              val |\n"
                                      "+------------------+------------------+\n"
                                      "|                1 |              100 |\n"
                                      "+------------------+------------------+\n"
                                      "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(stale_read), expected_stale_read);

    std::string final_state = verifier->exec_sql("select * from t;");
    std::string expected_final = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 0";
    EXPECT_EQ(TestSession::trim_output(final_state), expected_final);
}

TEST_F(SnapshotTest, SI_WriteWriteConflict_UncommittedDeleteThenInsertSameUniqueKey) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create index t (id);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto deleter = create_session();
    auto inserter = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(deleter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(deleter->exec_sql_ok("begin;"));
    ASSERT_TRUE(deleter->exec_sql_ok("delete from t where id = 1;"));

    ASSERT_TRUE(inserter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(inserter->exec_sql_ok("begin;"));
    std::string insert_abort = inserter->exec_sql_expect_abort("insert into t values (1, 200);");
    EXPECT_EQ(TestSession::trim_output(insert_abort), "abort")
        << "A concurrent insert of the same unique key must conflict with the uncommitted delete";
    ASSERT_TRUE(inserter->exec_sql_ok("commit;"));

    ASSERT_TRUE(deleter->exec_sql_ok("commit;"));

    std::string final_state = verifier->exec_sql("select * from t;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 0";
    EXPECT_EQ(TestSession::trim_output(final_state), expected)
        << "The aborted insert must not resurrect the deleted key";
}

TEST_F(SnapshotTest, SI_WriteWriteConflict_PostSnapshotDeleteThenInsertSameUniqueKey) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create index t (id);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto stale_txn = create_session();
    auto deleter = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(stale_txn->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(stale_txn->exec_sql_ok("begin;"));
    std::string stale_read = stale_txn->exec_sql("select * from t where id = 1;");

    ASSERT_TRUE(deleter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(deleter->exec_sql_ok("begin;"));
    ASSERT_TRUE(deleter->exec_sql_ok("delete from t where id = 1;"));
    ASSERT_TRUE(deleter->exec_sql_ok("commit;"));

    std::string insert_abort = stale_txn->exec_sql_expect_abort("insert into t values (1, 200);");
    EXPECT_EQ(TestSession::trim_output(insert_abort), "abort")
        << "A transaction that still sees the old key cannot insert the same logical key after a post-snapshot delete";
    ASSERT_TRUE(stale_txn->exec_sql_ok("commit;"));

    std::string expected_stale_read = "+------------------+------------------+\n"
                                      "|               id |              val |\n"
                                      "+------------------+------------------+\n"
                                      "|                1 |              100 |\n"
                                      "+------------------+------------------+\n"
                                      "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(stale_read), expected_stale_read);

    std::string final_state = verifier->exec_sql("select * from t;");
    std::string expected_final = "+------------------+------------------+\n"
                                 "|               id |              val |\n"
                                 "+------------------+------------------+\n"
                                 "+------------------+------------------+\n"
                                 "Total record(s): 0";
    EXPECT_EQ(TestSession::trim_output(final_state), expected_final);
}

TEST_F(SnapshotTest, SI_WriteWriteConflict_UncommittedUniqueKeyUpdateThenInsertOldKey) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create index t (id);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));

    auto updater = create_session();
    auto inserter = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(updater->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(updater->exec_sql_ok("begin;"));
    ASSERT_TRUE(updater->exec_sql_ok("update t set id = 2 where id = 1;"));

    ASSERT_TRUE(inserter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(inserter->exec_sql_ok("begin;"));
    std::string insert_abort = inserter->exec_sql_expect_abort("insert into t values (1, 200);");
    EXPECT_EQ(TestSession::trim_output(insert_abort), "abort")
        << "The old unique key remains part of the in-progress logical record and must conflict";
    ASSERT_TRUE(inserter->exec_sql_ok("commit;"));

    ASSERT_TRUE(updater->exec_sql_ok("commit;"));

    std::string final_state = verifier->exec_sql("select * from t;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                2 |              100 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(final_state), expected);
}

TEST_F(SnapshotTest, SI_StaleSnapshotUpdateToPostSnapshotDeletedUniqueKeyAborts) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create index t (id);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 200);"));

    auto stale_txn = create_session();
    auto deleter = create_session();

    ASSERT_TRUE(stale_txn->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(stale_txn->exec_sql_ok("begin;"));
    std::string stale_read = stale_txn->exec_sql("select * from t where id = 1;");

    ASSERT_TRUE(deleter->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(deleter->exec_sql_ok("begin;"));
    ASSERT_TRUE(deleter->exec_sql_ok("delete from t where id = 1;"));
    ASSERT_TRUE(deleter->exec_sql_ok("commit;"));

    std::string update_abort = stale_txn->exec_sql_expect_abort("update t set id = 1 where id = 2;");
    EXPECT_EQ(TestSession::trim_output(update_abort), "abort")
        << "A stale snapshot still sees key 1, so updating another indexed row to key 1 must conflict";
    ASSERT_TRUE(stale_txn->exec_sql_ok("commit;"));

    std::string expected_stale_read = "+------------------+------------------+\n"
                                      "|               id |              val |\n"
                                      "+------------------+------------------+\n"
                                      "|                1 |              100 |\n"
                                      "+------------------+------------------+\n"
                                      "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(stale_read), expected_stale_read);
}

TEST_F(SnapshotTest, SI_InsertHistoricalKeyOlderThanVisibleVersionIsAllowed) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create index t (id);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("update t set id = 2 where id = 1;"));

    auto reader = create_session();
    auto writer = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(reader->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(reader->exec_sql_ok("begin;"));
    std::string reader_visible = reader->exec_sql("select * from t where id = 2;");

    ASSERT_TRUE(writer->exec_sql_ok("set transaction isolation level snapshot isolation;"));
    ASSERT_TRUE(writer->exec_sql_ok("begin;"));
    ASSERT_TRUE(writer->exec_sql_ok("update t set id = 3 where id = 2;"));
    ASSERT_TRUE(writer->exec_sql_ok("commit;"));

    ASSERT_TRUE(reader->exec_sql_ok("insert into t values (1, 999);"))
        << "Key 1 is older than reader's visible version key 2, so it should be reusable";
    ASSERT_TRUE(reader->exec_sql_ok("commit;"));

    std::string expected_reader_visible = "+------------------+------------------+\n"
                                          "|               id |              val |\n"
                                          "+------------------+------------------+\n"
                                          "|                2 |              100 |\n"
                                          "+------------------+------------------+\n"
                                          "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(reader_visible), expected_reader_visible);

    std::string final_state = verifier->exec_sql("select * from t;");
    EXPECT_NE(final_state.find("|                1 |              999 |"), std::string::npos);
    EXPECT_NE(final_state.find("|                3 |              100 |"), std::string::npos);
}

// =============================================================================
// ser/write_skew_test — T1 commits before T2's write, SSI danger still detected.
// Sequential version: no threading, validates predicate-read-based SSI detection
// when the reader (T1) committed before the writer (T2) executes its write.
// =============================================================================

TEST_F(SnapshotTest, SER_WriteSkew_CommittedReaderEdge) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table duty (doctor_id int, on_call int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty values (1, 1);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty values (2, 1);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto t3 = create_session();

    // T1: SER, BEGIN, SELECT id=2 (predicate read saved)
    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("select * from duty where doctor_id = 2;"));

    // T2: SER, BEGIN, SELECT id=1 (predicate read saved)
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from duty where doctor_id = 1;"));

    // T1: UPDATE id=1 → CheckWriteAgainstReaders: T2's predicate id=1 matches
    // → creates T2 ->rw T1. No danger (T2 has no incoming, T1 has no outgoing).
    ASSERT_TRUE(t1->exec_sql_ok("update duty set on_call = 0 where doctor_id = 1;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    // T2: UPDATE id=2 → CheckWriteAgainstReaders: T1 (COMMITTED) has predicate id=2
    // that matches → creates T1 ->rw T2.
    // Now T2 ->rw T1 AND T1 ->rw T2 → bidirectional danger → T2 aborts!
    std::string t2_abort = t2->exec_sql_expect_abort("update duty set on_call = 0 where doctor_id = 2;");
    EXPECT_EQ(TestSession::trim_output(t2_abort), "abort")
        << "T2 should abort: T1's committed predicate read still forces SSI detection";

    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    // Final state: only T1's update applied
    std::string final_state = t3->exec_sql("select * from duty;");
    std::string expected = "+------------------+------------------+\n"
                           "|        doctor_id |          on_call |\n"
                           "+------------------+------------------+\n"
                           "|                1 |                0 |\n"
                           "|                2 |                1 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(final_state), expected);
}

// =============================================================================
// ser/write_skew_test — invisible write during SELECT creates rw-dependency.
// Sequential version: T1 writes first, T2's SELECT detects invisible write,
// then T2's write completes the bidirectional cycle → abort.
// =============================================================================

TEST_F(SnapshotTest, SER_WriteSkew_InvisibleWriteDuringSelect) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 200);"));

    auto t1 = create_session();
    auto t2 = create_session();

    // T1 writes id=1 (uncommitted)
    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("update t set val = 101 where id = 1;"));

    // T2 reads id=1 — invisible write check creates T2 ->rw T1
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from t where id = 1;"));

    // T1 reads id=2 (predicate read saved)
    ASSERT_TRUE(t1->exec_sql_ok("select * from t where id = 2;"));

    // T2 writes id=2 — matches T1's predicate read → T1 ->rw T2
    // Now T2 ->rw T1 AND T1 ->rw T2 → danger → T2 aborts
    std::string t2_abort = t2->exec_sql_expect_abort("update t set val = 201 where id = 2;");
    EXPECT_EQ(TestSession::trim_output(t2_abort), "abort")
        << "T2's UPDATE on id=2 should abort (bidirectional rw-cycle via predicate reads)";

    ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));
}

// =============================================================================
// ser/write_skew_test — UPDATE checks both old and new records atomically.
// An UPDATE's old record matches reader A's predicate, new record matches
// reader B's predicate. Both rw-edges must be created, and the write must
// be stored atomically (single ssi_writes_ entry) so that no intermediate
// state is visible to concurrent RecordPredicateRead calls.
// =============================================================================

TEST_F(SnapshotTest, SER_UpdateBothOldAndNewRecordsChecked) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 10);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 20);"));

    auto t1 = create_session(); // reader with predicate [val=10]
    auto t2 = create_session(); // reader with predicate [val=30]
    auto t3 = create_session(); // final state check

    // T1: reads row where val=10
    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("select * from t where val = 10;"));

    // T2: reads row where val=30 (empty result — predicate read saved)
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from t where val = 30;"));

    // T1: UPDATE id=1 — old=(1,10) matches T1's own read_rids (self, skip)
    //                     — new=(1,30) matches T2's predicate [val=30]
    //   → T2 ->rw T1. No danger (T2 has no incoming edges).
    ASSERT_TRUE(t1->exec_sql_ok("update t set val = 30 where id = 1;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    // T2: UPDATE id=2 — old=(2,20) doesn't match T1's predicate [val=10]
    //   → no new edges. No danger. T2 commits normally.
    ASSERT_TRUE(t2->exec_sql_ok("update t set val = 40 where id = 2;"));
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    // Verify final state
    std::string result = t3->exec_sql("select * from t;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |               30 |\n"
                           "|                2 |               40 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(result), expected);
}

TEST_F(SnapshotTest, SER_DeleteOldRecordCanCompleteDangerousStructure) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table duty (doctor_id int, on_call int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty values (1, 1);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty values (2, 1);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));

    ASSERT_TRUE(t1->exec_sql_ok("select * from duty where doctor_id = 2;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from duty where doctor_id = 1;"));

    ASSERT_TRUE(t1->exec_sql_ok("update duty set on_call = 0 where doctor_id = 1;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    std::string t2_abort = t2->exec_sql_expect_abort("delete from duty where doctor_id = 2;");
    EXPECT_EQ(TestSession::trim_output(t2_abort), "abort")
        << "DELETE must check its old record image and abort when it completes an SSI danger structure";
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    std::string result = verifier->exec_sql("select * from duty;");
    std::string expected = "+------------------+------------------+\n"
                           "|        doctor_id |          on_call |\n"
                           "+------------------+------------------+\n"
                           "|                1 |                0 |\n"
                           "|                2 |                1 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(result), expected);
}

TEST_F(SnapshotTest, SER_UpdateNewRecordCanCompleteDangerousStructure) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 10);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 20);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));

    ASSERT_TRUE(t1->exec_sql_ok("select * from t where val = 30;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from t where id = 2;"));

    ASSERT_TRUE(t1->exec_sql_ok("update t set val = 21 where id = 2;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    std::string t2_abort = t2->exec_sql_expect_abort("update t set val = 30 where id = 1;");
    EXPECT_EQ(TestSession::trim_output(t2_abort), "abort")
        << "UPDATE must compare its new record image against prior predicate reads";
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    std::string result = verifier->exec_sql("select * from t;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |               10 |\n"
                           "|                2 |               21 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(result), expected);
}

TEST_F(SnapshotTest, SER_SelectDetectsCommittedInvisibleWriteDanger) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 200);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (3, 300);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));

    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t1->exec_sql_ok("select * from t where id = 1;"));

    ASSERT_TRUE(t2->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("update t set val = 101 where id = 1;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from t where id = 2;"));

    ASSERT_TRUE(t1->exec_sql_ok("update t set val = 301 where id = 3;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    std::string t2_abort = t2->exec_sql_expect_abort("select * from t where id = 3;");
    EXPECT_EQ(TestSession::trim_output(t2_abort), "abort")
        << "A SELECT must detect writes committed after the reader's snapshot as invisible SSI writes";
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    std::string result = verifier->exec_sql("select * from t;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              100 |\n"
                           "|                2 |              200 |\n"
                           "|                3 |              301 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 3";
    EXPECT_EQ(TestSession::trim_output(result), expected)
        << "The aborting SELECT must roll back T2's earlier write while preserving T1's committed write";
}

TEST_F(SnapshotTest, SER_AbortedTransactionStateDoesNotCreateFutureDanger) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table t (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (1, 100);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into t values (2, 200);"));

    auto aborted_reader = create_session();
    ASSERT_TRUE(aborted_reader->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(aborted_reader->exec_sql_ok("begin;"));
    ASSERT_TRUE(aborted_reader->exec_sql_ok("select * from t where id = 1;"));
    ASSERT_TRUE(aborted_reader->exec_sql_ok("abort;"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));

    ASSERT_TRUE(t1->exec_sql_ok("select * from t where id = 2;"));
    ASSERT_TRUE(t2->exec_sql_ok("update t set val = 101 where id = 1;"))
        << "The aborted reader's old read of id=1 must not create a stale incoming SSI edge";
    ASSERT_TRUE(t2->exec_sql_ok("update t set val = 201 where id = 2;"))
        << "A later write must not abort because of stale SSI state from the aborted transaction";

    ASSERT_TRUE(t1->exec_sql_ok("commit;"));
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    std::string result = verifier->exec_sql("select * from t;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |              101 |\n"
                           "|                2 |              201 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(result), expected);
}

// =============================================================================
// Regression: record-level SSI reads must be scoped by table name.
// Different tables commonly use the same physical RID (page 1, slot 0). A read
// from table a must not conflict with a write to table c just because the RID
// numbers match.
// =============================================================================

TEST_F(SnapshotTest, SER_RecordReadRidCollisionAcrossTablesDoesNotCreateFalseDanger) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table a (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create table b (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("create table c (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into a values (1, 10);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into b values (1, 20);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into c values (1, 30);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));

    ASSERT_TRUE(t1->exec_sql_ok("select * from a where id = 1;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from b where id = 1;"));

    ASSERT_TRUE(t1->exec_sql_ok("update b set val = 21 where id = 1;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    ASSERT_TRUE(t2->exec_sql_ok("update c set val = 31 where id = 1;"))
        << "T2 writes table c only; T1 read table a, so matching physical RIDs must not create T1->rw T2";
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    std::string result = verifier->exec_sql("select * from c where id = 1;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |              val |\n"
                           "+------------------+------------------+\n"
                           "|                1 |               31 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 1";
    EXPECT_EQ(TestSession::trim_output(result), expected);
}

// =============================================================================
// Regression: SERIALIZABLE reads inside joins must participate in SSI.
// SSI read tracking must happen at base-table scans, not only at the SELECT root,
// otherwise a join SELECT can miss the row/predicate reads that make write skew
// dangerous.
// =============================================================================

TEST_F(SnapshotTest, SER_JoinReadParticipatesInWriteSkewDetection) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table doctor (id int, active int);"));
    ASSERT_TRUE(s->exec_sql_ok("create table duty (doctor_id int, on_call int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into doctor values (1, 1);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into doctor values (2, 1);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty values (1, 1);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into duty values (2, 1);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));

    ASSERT_TRUE(t1->exec_sql_ok("select * from doctor, duty where doctor.id = duty.doctor_id and duty.doctor_id = 2;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from doctor, duty where doctor.id = duty.doctor_id and duty.doctor_id = 1;"));

    ASSERT_TRUE(t1->exec_sql_ok("update duty set on_call = 0 where doctor_id = 1;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    std::string t2_abort = t2->exec_sql_expect_abort("update duty set on_call = 0 where doctor_id = 2;");
    EXPECT_EQ(TestSession::trim_output(t2_abort), "abort")
        << "T2 should abort: T1's join read of duty.doctor_id=2 must be tracked for SSI";
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    std::string result = verifier->exec_sql("select * from duty;");
    std::string expected = "+------------------+------------------+\n"
                           "|        doctor_id |          on_call |\n"
                           "+------------------+------------------+\n"
                           "|                1 |                0 |\n"
                           "|                2 |                1 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(result), expected);
}

TEST_F(SnapshotTest, SER_AggregateReadParticipatesInWriteSkewDetection) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table agg_duty (id int, on_call int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into agg_duty values (1, 1);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into agg_duty values (2, 1);"));

    auto t1 = create_session();
    auto t2 = create_session();
    auto verifier = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));

    ASSERT_TRUE(t1->exec_sql_ok("select count(*) from agg_duty where id = 2;"));
    ASSERT_TRUE(t2->exec_sql_ok("select count(*) from agg_duty where id = 1;"));

    ASSERT_TRUE(t1->exec_sql_ok("update agg_duty set on_call = 0 where id = 1;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    std::string t2_abort = t2->exec_sql_expect_abort("update agg_duty set on_call = 0 where id = 2;");
    EXPECT_EQ(TestSession::trim_output(t2_abort), "abort")
        << "Aggregate SELECT must still register base-table SSI reads";
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));

    std::string result = verifier->exec_sql("select * from agg_duty;");
    std::string expected = "+------------------+------------------+\n"
                           "|               id |          on_call |\n"
                           "+------------------+------------------+\n"
                           "|                1 |                0 |\n"
                           "|                2 |                1 |\n"
                           "+------------------+------------------+\n"
                           "Total record(s): 2";
    EXPECT_EQ(TestSession::trim_output(result), expected);
}

TEST_F(SnapshotTest, SER_EmptyJoinPredicateReadDetectsLaterInsert) {
    auto s = create_session();
    ASSERT_TRUE(s->exec_sql_ok("create table a (id int);"));
    ASSERT_TRUE(s->exec_sql_ok("create table b (id int);"));
    ASSERT_TRUE(s->exec_sql_ok("create table guard (id int, val int);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into a values (2);"));
    ASSERT_TRUE(s->exec_sql_ok("insert into guard values (1, 0);"));

    auto t1 = create_session();
    auto t2 = create_session();

    ASSERT_TRUE(t1->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t2->exec_sql_ok("set transaction isolation level serializable;"));
    ASSERT_TRUE(t1->exec_sql_ok("begin;"));
    ASSERT_TRUE(t2->exec_sql_ok("begin;"));

    ASSERT_TRUE(t1->exec_sql_ok("select * from a, b where a.id = b.id and a.id = 2;"));
    ASSERT_TRUE(t2->exec_sql_ok("select * from guard where id = 1;"));

    ASSERT_TRUE(t1->exec_sql_ok("update guard set val = 1 where id = 1;"));
    ASSERT_TRUE(t1->exec_sql_ok("commit;"));

    std::string t2_abort = t2->exec_sql_expect_abort("insert into b values (2);");
    EXPECT_EQ(TestSession::trim_output(t2_abort), "abort")
        << "Empty join must save predicate reads so a later matching insert completes the SSI danger structure";
    ASSERT_TRUE(t2->exec_sql_ok("commit;"));
}

// =============================================================================
// Grammar Test: SET TRANSACTION ISOLATION LEVEL syntax
// From: docs/测试说明文档2026.pdf — syntax test point
// =============================================================================

TEST_F(SnapshotTest, Syntax_SetTransaction_SnapshotIsolation) {
    auto s = create_session();
    // Should parse and execute without error
    EXPECT_TRUE(s->exec_sql_ok("set transaction isolation level snapshot isolation;"));
}

TEST_F(SnapshotTest, Syntax_SetTransaction_Serializable) {
    auto s = create_session();
    EXPECT_TRUE(s->exec_sql_ok("set transaction isolation level serializable;"));
}

TEST_F(SnapshotTest, Syntax_SetTransaction_CaseInsensitive) {
    auto s = create_session();
    // Keywords should be case-insensitive (like all SQL in RMDB)
    EXPECT_TRUE(s->exec_sql_ok("SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;"));
    EXPECT_TRUE(s->exec_sql_ok("set transaction isolation level serializable;"));
}
