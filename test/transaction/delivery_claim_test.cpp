/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Two concurrent transactions may not both claim the same work-queue row.
//
// This is the engine-side invariant behind TPC-C Delivery: a queue item must be
// claimed exactly once, no matter how the two claimants interleave. The claim
// protocol under test is the general "lock the row, then confirm it is still
// there" pattern:
//
//   1. read the queue head
//   2. UPDATE the row to take its exclusive lock
//   3. re-read the row to confirm the claim
//   4. if confirmed: DELETE it and credit the owner
//
// Step 3 is what makes the protocol safe, and it is only safe if the engine
// gives the re-read a snapshot that already contains every commit that had to
// finish before step 2 could acquire the lock. A lagging READ COMMITTED
// statement snapshot would resurrect the row the winner already deleted, and
// both transactions would credit the owner while only one row disappeared.
//
// The second thing these tests pin down is the shape of the answer a lost claim
// gets. When the confirmation read is written as an aggregate --
// `SELECT MIN(id) ... WHERE id = <claimed>` -- a lost claim is NOT an empty
// result: an aggregate with no GROUP BY always returns exactly one row, and
// over an empty input that row is SQL NULL. A claim protocol must therefore
// compare the confirmed value against the id it tried to claim. Testing the
// result for emptiness silently confirms every claim and double-credits every
// contended queue row, with the tell-tale signature of a queue that shrinks
// more slowly than the credit counter grows.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "analyze/analyze.h"
#include "common/config.h"
#include "common/context.h"
#include "errors.h"
#include "execution/execution_manager.h"
#include "index/ix_manager.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parser.h"
#include "portal.h"
#include "record/rm_manager.h"
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

#include <gtest/gtest.h>

namespace {

/// Multi-session in-process engine. Mirrors the statement pipeline of
/// src/rmdb.cpp (including the per-statement BeginStatement that installs the
/// READ COMMITTED statement snapshot) but keeps one transaction id per session
/// so several sessions can run concurrently on shared managers, exactly as
/// connection threads do in the server.
class MultiSessionDB {
public:
    explicit MultiSessionDB(const std::string& db_name) : db_name_(db_name) {
        char cwd_buf[1024];
        if (getcwd(cwd_buf, sizeof(cwd_buf)) == nullptr) {
            throw std::runtime_error("getcwd failed");
        }
        original_cwd_ = cwd_buf;
        std::string cleanup = "rm -rf " + original_cwd_ + "/" + db_name_;
        (void)!system(cleanup.c_str());

        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(1024, disk_manager_.get());
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
        portal_ = std::make_unique<Portal>(sm_manager_.get());
        analyze_ = std::make_unique<Analyze>(sm_manager_.get());

        sm_manager_->create_db(db_name_);
        sm_manager_->open_db(db_name_);
        log_manager_->initialize_from_existing_log();
        buffer_pool_manager_->set_log_manager(log_manager_.get());
    }

    ~MultiSessionDB() {
        try {
            sm_manager_->close_db();
        } catch (...) {
            (void)!chdir(original_cwd_.c_str());
        }
        std::string cleanup = "rm -rf " + original_cwd_ + "/" + db_name_;
        (void)!system(cleanup.c_str());
    }

    /// One client connection: its own transaction id, its own response buffer.
    class Session {
    public:
        Session(MultiSessionDB* db) : db_(db), response_(BUFFER_LENGTH, 0) {}

        std::string exec(const std::string& sql) {
            std::fill(response_.begin(), response_.end(), 0);
            int offset = 0;
            Context context(db_->lock_manager_.get(), db_->log_manager_.get(), nullptr, response_.data(), &offset,
                            db_->txn_manager_.get());
            auto parse_tree = ast::parse_sql(sql);
            if (parse_tree == nullptr) {
                return "";
            }
            set_transaction(&context);
            try {
                std::unique_ptr<Query> query = db_->analyze_->do_analyze(std::move(parse_tree));
                std::unique_ptr<Plan> plan = db_->optimizer_->plan_query(std::move(query), &context);
                std::unique_ptr<PortalStmt> statement = db_->portal_->start(std::move(plan), &context);
                db_->portal_->run(std::move(statement), db_->ql_manager_.get(), &txn_id_, &context);
                db_->portal_->drop();
                finish_statement(&context);
            } catch (...) {
                abort_failed_statement(&context);
                throw;
            }
            return std::string(response_.data(), offset);
        }

    private:
        void set_transaction(Context* context) {
            context->txn_ = txn_id_ == INVALID_TXN_ID ? nullptr : db_->txn_manager_->get_transaction(txn_id_);
            if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
                context->txn_->get_state() == TransactionState::ABORTED) {
                context->txn_ = db_->txn_manager_->begin(nullptr, context->log_mgr_, context->isolation_level_);
                txn_id_ = context->txn_->get_transaction_id();
                context->txn_->set_txn_mode(false);
            }
            db_->txn_manager_->BeginStatement(context->txn_);
        }

        void finish_statement(Context* context) {
            if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
                context->txn_->get_state() != TransactionState::COMMITTED &&
                context->txn_->get_state() != TransactionState::ABORTED) {
                db_->txn_manager_->commit(context->txn_, context->log_mgr_);
                txn_id_ = INVALID_TXN_ID;
            }
            context->txn_ = nullptr;
        }

        void abort_failed_statement(Context* context) {
            Transaction* txn = context->txn_;
            if (txn == nullptr && txn_id_ != INVALID_TXN_ID) {
                txn = db_->txn_manager_->get_transaction(txn_id_);
            }
            if (txn != nullptr && txn->get_state() != TransactionState::COMMITTED &&
                txn->get_state() != TransactionState::ABORTED) {
                db_->txn_manager_->abort(txn, context->log_mgr_);
            }
            txn_id_ = INVALID_TXN_ID;
            context->txn_ = nullptr;
        }

        MultiSessionDB* db_;
        txn_id_t txn_id_{INVALID_TXN_ID};
        std::vector<char> response_;
    };

    Session new_session() {
        return Session(this);
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
    std::unique_ptr<Portal> portal_;
    std::unique_ptr<Analyze> analyze_;
};

/// The scalar payload of a single-row single-column result: the rendered value
/// ("NULL" for a SQL NULL), or "" when the query returned no row at all. The
/// distinction between those two is exactly what a claim protocol has to get
/// right, so the helper must not collapse them.
std::string ScalarOf(const std::string& output) {
    // The embedded pipeline renders results as the client-facing text table.
    // Grab the last non-separator, non-header token.
    std::vector<std::string> lines;
    std::string current;
    for (char c : output) {
        if (c == '\n') {
            lines.push_back(current);
            current.clear();
        } else {
            current.push_back(c);
        }
    }
    if (!current.empty()) {
        lines.push_back(current);
    }
    std::string last_value;
    bool first = true;
    for (const auto& line : lines) {
        if (line.empty() || line.front() != '|') {
            continue;
        }
        std::string inner = line.substr(1);
        while (!inner.empty() && (inner.back() == '|' || inner.back() == ' ')) {
            inner.pop_back();
        }
        size_t begin = inner.find_first_not_of(' ');
        if (begin == std::string::npos) {
            continue;
        }
        inner = inner.substr(begin);
        if (first) {
            first = false; // header row repeats the column name
            continue;
        }
        last_value = inner;
    }
    return last_value;
}

class DeliveryClaimTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        db_ = std::make_unique<MultiSessionDB>(std::string("delivery_claim_") + info->name());
        auto setup = db_->new_session();
        setup.exec("create table new_orders (no_o_id int, no_d_id int, no_w_id int);");
        setup.exec("create index new_orders(no_w_id, no_d_id, no_o_id);");
        setup.exec("create table customer (c_id int, c_d_id int, c_w_id int, cnt int);");
        setup.exec("create index customer(c_w_id, c_d_id, c_id);");
        setup.exec("insert into new_orders values (1, 1, 1);");
        setup.exec("insert into customer values (1, 1, 1, 0);");
    }

    void TearDown() override {
        db_.reset();
    }

    /// Step 2 plus step 3: take the row lock and read the claim back. Returns
    /// the rendered confirmation value.
    static std::string LockAndConfirm(MultiSessionDB::Session& session, int o_id) {
        session.exec("update new_orders set no_o_id = no_o_id where no_w_id = 1 and no_d_id = 1 and no_o_id = " +
                     std::to_string(o_id) + ";");
        return ScalarOf(
            session.exec("select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1 and no_o_id = " +
                         std::to_string(o_id) + ";"));
    }

    /// Steps 2-4 of the claim protocol. Returns true when this session claimed
    /// the row (and therefore credited the customer).
    static bool ConfirmAndFinishClaim(MultiSessionDB::Session& session, int o_id) {
        if (LockAndConfirm(session, o_id) != std::to_string(o_id)) {
            return false;
        }
        session.exec("delete from new_orders where no_w_id = 1 and no_d_id = 1 and no_o_id = " + std::to_string(o_id) +
                     ";");
        session.exec("update customer set cnt = cnt + 1 where c_w_id = 1 and c_d_id = 1 and c_id = 1;");
        return true;
    }

    int CountQueue() {
        auto session = db_->new_session();
        return std::atoi(ScalarOf(session.exec("select count(*) as cnt from new_orders;")).c_str());
    }

    int SmallestQueuedId() {
        auto session = db_->new_session();
        const std::string head =
            ScalarOf(session.exec("select min(no_o_id) as cnt from new_orders where no_w_id = 1 and no_d_id = 1;"));
        return head == "NULL" ? -1 : std::atoi(head.c_str());
    }

    int CreditedCount() {
        auto session = db_->new_session();
        return std::atoi(
            ScalarOf(session.exec("select cnt from customer where c_w_id = 1 and c_d_id = 1 and c_id = 1;")).c_str());
    }

    std::unique_ptr<MultiSessionDB> db_;
};

// The loser of the row-lock race must observe the winner's committed DELETE in
// its confirmation read. Deterministic ordering: the loser reads the queue head
// and then blocks in the UPDATE's lock wait; only after it is provably blocked
// does the winner commit.
TEST_F(DeliveryClaimTest, LockLoserSeesWinnersCommittedDelete) {
    auto winner = db_->new_session();
    auto loser = db_->new_session();

    winner.exec("begin;");
    loser.exec("begin;");

    // Both see the same queue head: neither commit has happened yet.
    ASSERT_EQ("1", ScalarOf(winner.exec("select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;")));
    ASSERT_EQ("1", ScalarOf(loser.exec("select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;")));

    // The winner takes the row lock, confirms and deletes, but does not commit.
    ASSERT_TRUE(ConfirmAndFinishClaim(winner, 1));

    // The loser now issues its locking UPDATE. It must block behind the winner.
    std::atomic<bool> loser_finished{false};
    std::atomic<bool> loser_claimed{false};
    std::string loser_confirmation;
    std::string loser_error;
    std::thread loser_thread([&] {
        try {
            loser_confirmation = LockAndConfirm(loser, 1);
            loser_claimed.store(loser_confirmation == "1");
            loser.exec("commit;");
        } catch (const std::exception& error) {
            loser_error = error.what();
        } catch (...) {
            loser_error = "unknown exception";
        }
        loser_finished.store(true);
    });

    // Give the loser time to reach the lock wait. If it finished early it did
    // not contend at all and the scenario under test did not happen.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_FALSE(loser_finished.load()) << "loser did not block on the row lock; scenario not exercised";

    winner.exec("commit;");
    loser_thread.join();

    EXPECT_TRUE(loser_error.empty()) << "loser failed unexpectedly: " << loser_error;
    // The whole point: the statement that runs once the lock is finally granted
    // must see the winner's committed DELETE, so MIN over the now-empty match
    // set is the finalv3 A.3 integer zero rather than the value the loser read
    // before it blocked.
    EXPECT_EQ("0", loser_confirmation) << "the loser's confirmation read resurrected the deleted queue row";
    EXPECT_FALSE(loser_claimed.load()) << "both transactions claimed the same queue row";
    EXPECT_EQ(0, CountQueue());
    EXPECT_EQ(1, CreditedCount()) << "the queue row was credited more than once";
}

// A claim that lost the race is reported as one row holding the finalv3 A.3
// integer zero, never as an empty result. The claim protocol must compare that
// value with the candidate order id.
TEST_F(DeliveryClaimTest, LostClaimConfirmationIsOneZeroRowNotAnEmptyResult) {
    auto session = db_->new_session();
    session.exec("begin;");
    ASSERT_TRUE(ConfirmAndFinishClaim(session, 1));
    session.exec("commit;");

    const std::string output =
        session.exec("select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1 and no_o_id = 1;");
    EXPECT_NE(std::string::npos, output.find("Total record(s): 1"))
        << "an aggregate without GROUP BY must return exactly one row\n"
        << output;
    EXPECT_EQ("0", ScalarOf(output));
}

// Same invariant without any hand-placed ordering: two threads racing over a
// whole queue. Every credit must correspond to exactly one row leaving the
// queue, and the claims must consume the queue from its head, never skipping a
// still-present smaller id (TPC-C Delivery's "oldest undelivered" rule).
TEST_F(DeliveryClaimTest, ConcurrentClaimantsConsumeTheQueueHeadExactlyOnce) {
    constexpr int kQueueRows = 61; // row 1 already exists
    constexpr int kAttemptsPerThread = 20;
    auto seed = db_->new_session();
    for (int o_id = 2; o_id <= kQueueRows; ++o_id) {
        seed.exec("insert into new_orders values (" + std::to_string(o_id) + ", 1, 1);");
    }

    std::mutex claims_latch;
    std::vector<int> claimed_ids;
    std::atomic<int> failures{0};

    auto claimant = [&] {
        auto session = db_->new_session();
        for (int attempt = 0; attempt < kAttemptsPerThread; ++attempt) {
            try {
                session.exec("begin;");
                const std::string head =
                    ScalarOf(session.exec("select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;"));
                if (head == "NULL" || head.empty()) {
                    session.exec("commit;");
                    continue;
                }
                const int o_id = std::atoi(head.c_str());
                if (ConfirmAndFinishClaim(session, o_id)) {
                    std::lock_guard<std::mutex> guard(claims_latch);
                    claimed_ids.push_back(o_id);
                }
                session.exec("commit;");
            } catch (...) {
                failures.fetch_add(1);
            }
        }
    };

    std::thread a(claimant);
    std::thread b(claimant);
    a.join();
    b.join();

    ASSERT_EQ(0, failures.load()) << "a claimant failed; the run does not measure what it should";
    const int remaining = CountQueue();
    const int removed = kQueueRows - remaining;
    ASSERT_GT(removed, 0) << "no row was ever claimed; the run does not measure what it should";
    ASSERT_GT(remaining, 0) << "the queue drained completely, so the ordering check below is vacuous";

    // Exactly once: one credit and one distinct claimed id per removed row.
    EXPECT_EQ(removed, CreditedCount()) << "credits do not match the rows that left the queue";
    EXPECT_EQ(static_cast<size_t>(removed), claimed_ids.size());
    std::sort(claimed_ids.begin(), claimed_ids.end());
    EXPECT_EQ(claimed_ids.end(), std::unique(claimed_ids.begin(), claimed_ids.end()))
        << "the same queue row was claimed twice";

    // Oldest first: what was claimed is exactly the prefix of the queue, so no
    // claim ever jumped over a row that is still sitting there.
    EXPECT_EQ(removed, SmallestQueuedId() - 1) << "a claim skipped a smaller id that is still queued";
}

} // namespace
