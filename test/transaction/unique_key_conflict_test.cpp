#include "execution/cursor_test_helper.h"
/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution/executor_insert.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record/rm.h"
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "system/sm.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace {

class ScopedUniqueKeyTestDir {
public:
    explicit ScopedUniqueKeyTestDir(std::string dir)
        : old_path_(std::filesystem::current_path()), dir_(std::move(dir)) {
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directory(dir_);
        std::filesystem::current_path(dir_);
    }

    ~ScopedUniqueKeyTestDir() {
        std::filesystem::current_path(old_path_);
        std::filesystem::remove_all(dir_);
    }

private:
    std::filesystem::path old_path_;
    std::filesystem::path dir_;
};

class UniqueKeyConflictTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = std::make_unique<ScopedUniqueKeyTestDir>("unique_key_conflict_root");
        disk_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(64, disk_.get());
        rm_mgr_ = std::make_unique<RmManager>(disk_.get(), bpm_.get());
        ix_mgr_ = std::make_unique<IxManager>(disk_.get(), bpm_.get());
        sm_mgr_ = std::make_unique<SmManager>(disk_.get(), bpm_.get(), rm_mgr_.get(), ix_mgr_.get());
        log_mgr_ = std::make_unique<LogManager>(disk_.get());
        bpm_->set_log_manager(log_mgr_.get());
        lock_mgr_ = std::make_unique<LockManager>();
        sm_mgr_->create_db("unique_key_conflict_db");
        sm_mgr_->open_db("unique_key_conflict_db");
        sm_mgr_->create_table("t", {{"id", TYPE_INT, sizeof(int)}, {"v", TYPE_INT, sizeof(int)}}, nullptr);
        sm_mgr_->create_index("t", {"id"}, nullptr);
        txn_mgr_ = std::make_unique<TransactionManager>(lock_mgr_.get(), sm_mgr_.get());
    }

    void TearDown() override {
        txn_mgr_.reset();
        sm_mgr_->close_db();
        sm_mgr_.reset();
        test_dir_.reset();
    }

    static Value IntValue(int value) {
        Value result;
        result.set_int(value);
        return result;
    }

    // The ranking workload always opens an explicit transaction (BEGIN is the
    // first operation of every EXEC_BATCH), which is what txn_mode models.
    Transaction* BeginExplicit() {
        Transaction* txn = txn_mgr_->begin(nullptr, log_mgr_.get());
        txn->set_txn_mode(true);
        return txn;
    }

    void InsertRow(Transaction* txn, int id, int v) {
        Context context(lock_mgr_.get(), log_mgr_.get(), txn, txn_mgr_.get());
        InsertExecutor executor(sm_mgr_.get(), "t", {IntValue(id), IntValue(v)}, &context);
        CopyCurrentTuple(executor);
    }

    int offset_{0};
    std::unique_ptr<ScopedUniqueKeyTestDir> test_dir_;
    std::unique_ptr<DiskManager> disk_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<RmManager> rm_mgr_;
    std::unique_ptr<IxManager> ix_mgr_;
    std::unique_ptr<SmManager> sm_mgr_;
    std::unique_ptr<LogManager> log_mgr_;
    std::unique_ptr<LockManager> lock_mgr_;
    std::unique_ptr<TransactionManager> txn_mgr_;
};

} // namespace

// A transaction that loses a race for a unique index key must see a retryable
// TransactionAbortException. IndexEntryExistsError reaches the client as a
// non-retryable server ERROR (status=2) on the EXEC_BATCH path.
TEST_F(UniqueKeyConflictTest, InsertDuplicateKeyInTransactionAborts) {
    Transaction* writer = BeginExplicit();
    InsertRow(writer, 1, 10);
    txn_mgr_->commit(writer, log_mgr_.get());

    Transaction* loser = BeginExplicit();
    EXPECT_THROW(InsertRow(loser, 1, 20), TransactionAbortException);
    txn_mgr_->abort(loser, log_mgr_.get());
}

// An autocommit statement has nothing to retry, so a deterministic duplicate
// must stay a permanent SQL error instead of becoming TRANSACTION_ABORT.
TEST_F(UniqueKeyConflictTest, InsertDuplicateKeyInAutocommitStatementStaysSqlError) {
    Transaction* writer = txn_mgr_->begin(nullptr, log_mgr_.get());
    InsertRow(writer, 1, 10);
    txn_mgr_->commit(writer, log_mgr_.get());

    Transaction* second = txn_mgr_->begin(nullptr, log_mgr_.get());
    EXPECT_THROW(InsertRow(second, 1, 20), IndexEntryExistsError);
    txn_mgr_->abort(second, log_mgr_.get());
}

// Same requirement for the UPDATE path, where the conflict is detected by the
// MVCC pre-check in RowMutationEngine::UpdateOne instead of the B+tree.
TEST_F(UniqueKeyConflictTest, UpdateIntoExistingKeyInTransactionAborts) {
    Transaction* writer = BeginExplicit();
    InsertRow(writer, 1, 10);
    InsertRow(writer, 2, 20);
    txn_mgr_->commit(writer, log_mgr_.get());

    Transaction* loser = BeginExplicit();
    Context context(lock_mgr_.get(), log_mgr_.get(), loser, txn_mgr_.get());
    SetClause set_clause;
    set_clause.lhs = TabCol{"t", "id"};
    set_clause.rhs = IntValue(2);
    UpdateExecutor update_executor(
        sm_mgr_.get(), "t", {set_clause}, {},
        std::make_unique<SeqScanExecutor>(sm_mgr_.get(), "t", std::vector<Condition>{}, &context), std::nullopt,
        UpdateExecutionMode::Mutating, &context);
    EXPECT_THROW(CopyCurrentTuple(update_executor), TransactionAbortException);
    txn_mgr_->abort(loser, log_mgr_.get());
}
