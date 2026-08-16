#include "deltakernel/delta_database.h"
#include "parser/parser.h"
#include "server/database_instance.h"
#include "system/sm_meta.h"

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <thread>
#include <sys/wait.h>
#include <unistd.h>

namespace {
struct CapturingSink : QueryResultSink {
    void begin_query(const std::vector<ColMeta>& columns, const std::vector<std::string>& names) override {
        ++begin_count;
        metadata = columns;
        column_names = names;
    }
    void append_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) override {
        ASSERT_EQ(columns.size(), metadata.size());
        std::vector<std::string> row;
        for (const auto& column : columns) {
            ASSERT_LE(static_cast<size_t>(column.offset + column.len), size);
            if (is_null(data, column))
                row.emplace_back("NULL");
            else if (column.type == TYPE_INT)
                row.push_back(std::to_string(read_unaligned<int32_t>(data + column.offset)));
            else if (column.type == TYPE_FLOAT) {
                uint32_t bits;
                const float value = read_float(data + column.offset);
                std::memcpy(&bits, &value, sizeof(bits));
                row.push_back(std::to_string(bits));
            } else
                row.emplace_back(data + column.offset, static_cast<size_t>(column.len));
        }
        rows.push_back(std::move(row));
    }
    std::vector<ColMeta> metadata;
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> rows;
    int begin_count = 0;
};

class TempDelta {
public:
    TempDelta() {
        char path[] = "/tmp/rmdb-delta-test-XXXXXX";
        if (mkdtemp(path) == nullptr)
            throw std::runtime_error("mkdtemp failed");
        path_ = path;
    }
    ~TempDelta() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    const std::string& path() const {
        return path_;
    }

private:
    std::string path_;
};

class ScopedStorageEngine {
public:
    explicit ScopedStorageEngine(const char* value) {
        if (const char* previous = std::getenv("RMDB_STORAGE_ENGINE")) {
            had_previous_ = true;
            previous_ = previous;
        }
        if ((value == nullptr ? unsetenv("RMDB_STORAGE_ENGINE") : setenv("RMDB_STORAGE_ENGINE", value, 1)) != 0)
            throw std::runtime_error("set RMDB_STORAGE_ENGINE failed");
    }
    ~ScopedStorageEngine() {
        if (had_previous_)
            setenv("RMDB_STORAGE_ENGINE", previous_.c_str(), 1);
        else
            unsetenv("RMDB_STORAGE_ENGINE");
    }

private:
    bool had_previous_{false};
    std::string previous_;
};

void RunSql(deltakernel::DeltaDatabase& db, deltakernel::DeltaSession& session, const char* sql) {
    db.Execute(ast::parse_sql(sql), session, nullptr);
}

CapturingSink Query(deltakernel::DeltaDatabase& db, deltakernel::DeltaSession& session, const char* sql) {
    CapturingSink sink;
    EXPECT_TRUE(db.Execute(ast::parse_sql(sql), session, &sink));
    return sink;
}

bool WaitForCommitQueueDepth(deltakernel::DeltaDatabase& db, size_t depth) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (db.CommitQueueDepthForTest() == depth)
            return true;
        std::this_thread::yield();
    }
    return db.CommitQueueDepthForTest() == depth;
}

uint32_t CatalogCrc32(const std::string& bytes) {
    uint32_t crc = 0xffffffffU;
    for (unsigned char byte : bytes) {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1U) ^ (0xedb88320U & (0U - (crc & 1U)));
    }
    return ~crc;
}

void WriteCatalog(const std::string& path, const std::string& body) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output << body << "CRC32 " << CatalogCrc32(body) << '\n';
}

TEST(DeltaDatabaseTest, PrivateCommitAbortCheckpointAndRestart) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "insert into t values(1, 7);");
    RunSql(*db, session, "rollback;");
    RunSql(*db, session, "insert into t values(1, 9);");
    db->Checkpoint();
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "update t set v = 10 where k = 1;");
    RunSql(*db, session, "commit;");
}

TEST(DeltaDatabaseTest, StaleSameKeyCommitAborts) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, first, stale;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(1, 1);");
    RunSql(*db, first, "begin;");
    RunSql(*db, stale, "begin;");
    RunSql(*db, first, "update t set v = 2 where k = 1;");
    RunSql(*db, stale, "update t set v = 3 where k = 1;");
    RunSql(*db, first, "commit;");
    EXPECT_THROW(RunSql(*db, stale, "commit;"), deltakernel::DeltaTransactionAbort);
}

TEST(DeltaDatabaseTest, ConcurrentCommitsShareOneWalFrame) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    deltakernel::DeltaSession first;
    deltakernel::DeltaSession second;
    RunSql(*db, first, "begin;");
    RunSql(*db, first, "insert into t values(1, 10);");
    RunSql(*db, second, "begin;");
    RunSql(*db, second, "insert into t values(1, 20);");
    std::mutex barrier_mutex;
    std::condition_variable barrier;
    bool leader_waiting = false;
    bool release = false;
    db->SetCommitBatchHookForTest([&] {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        leader_waiting = true;
        barrier.notify_all();
        barrier.wait(lock, [&] { return release; });
    });
    const size_t frames_before = db->WalFrameCountForTest();
    std::thread first_committer([&] { RunSql(*db, first, "commit;"); });
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        barrier.wait(lock, [&] { return leader_waiting; });
    }
    std::thread second_committer([&] { RunSql(*db, second, "commit;"); });
    const bool both_queued = WaitForCommitQueueDepth(*db, 2);
    {
        std::lock_guard<std::mutex> lock(barrier_mutex);
        release = true;
    }
    barrier.notify_all();
    first_committer.join();
    second_committer.join();
    db->SetCommitBatchHookForTest({});
    ASSERT_TRUE(both_queued);
    EXPECT_EQ(db->WalFrameCountForTest(), frames_before + 1);
    EXPECT_EQ(Query(*db, setup, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}, {"20"}}));
}

TEST(DeltaDatabaseTest, ConcurrentConflictCommitsSharePublication) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, first, stale, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1, 1);");
    RunSql(*db, first, "begin;");
    RunSql(*db, stale, "begin;");
    RunSql(*db, first, "update t set v = 2 where k = 1;");
    RunSql(*db, stale, "update t set v = 3 where k = 1;");
    std::mutex barrier_mutex;
    std::condition_variable barrier;
    bool leader_waiting = false;
    bool release = false;
    db->SetCommitBatchHookForTest([&] {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        leader_waiting = true;
        barrier.notify_all();
        barrier.wait(lock, [&] { return release; });
    });
    const size_t frames_before = db->WalFrameCountForTest();
    std::exception_ptr first_error;
    std::exception_ptr stale_error;
    std::thread first_committer([&] {
        try {
            RunSql(*db, first, "commit;");
        } catch (...) {
            first_error = std::current_exception();
        }
    });
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        barrier.wait(lock, [&] { return leader_waiting; });
    }
    std::thread stale_committer([&] {
        try {
            RunSql(*db, stale, "commit;");
        } catch (...) {
            stale_error = std::current_exception();
        }
    });
    const bool both_queued = WaitForCommitQueueDepth(*db, 2);
    {
        std::lock_guard<std::mutex> lock(barrier_mutex);
        release = true;
    }
    barrier.notify_all();
    first_committer.join();
    stale_committer.join();
    db->SetCommitBatchHookForTest({});
    ASSERT_TRUE(both_queued);
    EXPECT_FALSE(first_error);
    EXPECT_TRUE(stale_error);
    EXPECT_EQ(db->WalFrameCountForTest(), frames_before + 1);
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"2"}}));
}

TEST(DeltaDatabaseTest, IndexedCommitPublishesRowsAndOverlayTogether) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, writer, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "insert into t values(7, 9);");
    std::mutex barrier_mutex;
    std::condition_variable barrier;
    bool installing = false;
    bool release = false;
    std::atomic<bool> reader_done{false};
    CapturingSink result;
    db->SetCommitInstallHookForTest([&] {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        installing = true;
        barrier.notify_all();
        barrier.wait(lock, [&] { return release; });
    });
    bool reader_blocked = false;
    db->SetExecuteBlockedHookForTest([&] {
        std::lock_guard<std::mutex> lock(barrier_mutex);
        reader_blocked = true;
        barrier.notify_all();
    });
    std::thread committer([&] { RunSql(*db, writer, "commit;"); });
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        barrier.wait(lock, [&] { return installing; });
    }
    std::thread reader_thread([&] {
        result = Query(*db, reader, "select v from t where k = 7;");
        reader_done = true;
    });
    bool observed_blocked;
    bool completed_before_release;
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        observed_blocked = barrier.wait_for(lock, std::chrono::seconds(2), [&] { return reader_blocked; });
        completed_before_release = reader_done;
        release = true;
    }
    barrier.notify_all();
    committer.join();
    reader_thread.join();
    db->SetCommitInstallHookForTest({});
    db->SetExecuteBlockedHookForTest({});
    EXPECT_TRUE(observed_blocked);
    EXPECT_FALSE(completed_before_release);
    EXPECT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"9"}}));
}

TEST(DeltaDatabaseTest, CommitLeaderFailureWakesQueuedWaiters) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, first, second;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, first, "begin;");
    RunSql(*db, first, "insert into t values(1);");
    RunSql(*db, second, "begin;");
    RunSql(*db, second, "insert into t values(2);");
    std::mutex barrier_mutex;
    std::condition_variable barrier;
    bool leader_waiting = false;
    bool release = false;
    db->SetCommitBatchHookForTest([&] {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        leader_waiting = true;
        barrier.notify_all();
        barrier.wait(lock, [&] { return release; });
        throw std::runtime_error("test leader failure");
    });
    std::exception_ptr first_error;
    std::exception_ptr second_error;
    std::thread first_committer([&] {
        try {
            RunSql(*db, first, "commit;");
        } catch (...) {
            first_error = std::current_exception();
        }
    });
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        barrier.wait(lock, [&] { return leader_waiting; });
    }
    std::thread second_committer([&] {
        try {
            RunSql(*db, second, "commit;");
        } catch (...) {
            second_error = std::current_exception();
        }
    });
    const bool both_queued = WaitForCommitQueueDepth(*db, 2);
    {
        std::lock_guard<std::mutex> lock(barrier_mutex);
        release = true;
    }
    barrier.notify_all();
    first_committer.join();
    second_committer.join();
    db->SetCommitBatchHookForTest({});
    ASSERT_TRUE(both_queued);
    EXPECT_TRUE(first_error);
    EXPECT_TRUE(second_error);
    EXPECT_EQ(db->CommitQueueDepthForTest(), 0U);
    EXPECT_TRUE(Query(*db, setup, "select k from t;").rows.empty());
}

TEST(DeltaDatabaseTest, DurableInstallFailureTerminates) {
    TempDelta temp;
    EXPECT_DEATH(
        {
            auto db = deltakernel::DeltaDatabase::Create(temp.path());
            deltakernel::DeltaSession session;
            RunSql(*db, session, "create table t(k int);");
            RunSql(*db, session, "create index t_k on t(k);");
            RunSql(*db, session, "begin;");
            RunSql(*db, session, "insert into t values(1);");
            db->SetCommitInstallHookForTest([] { throw std::runtime_error("test install failure"); });
            RunSql(*db, session, "commit;");
        },
        "");
}

TEST(DeltaDatabaseTest, StreamingAggregateAndBoundedOrder) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v float, s char(8));");
    RunSql(*db, session, "insert into t values(2, 1.25, 'b');");
    RunSql(*db, session, "insert into t values(1, 2.50, 'a');");
    RunSql(*db, session, "insert into t values(3, 1.25, 'a');");
    auto aggregate = Query(*db, session, "select count(*), count(distinct s), min(k), sum(v) from t where k >= 1;");
    ASSERT_EQ(aggregate.rows.size(), 1);
    ASSERT_EQ(aggregate.rows[0][0], "3");
    ASSERT_EQ(aggregate.rows[0][1], "2");
    ASSERT_EQ(aggregate.rows[0][2], "1");
    uint32_t sum_bits;
    float sum = 5.0F;
    std::memcpy(&sum_bits, &sum, sizeof(sum_bits));
    ASSERT_EQ(aggregate.rows[0][3], std::to_string(sum_bits));
    auto ordered = Query(*db, session, "select k from t order by s asc, k desc limit 1 offset 1;");
    ASSERT_EQ(ordered.rows, (std::vector<std::vector<std::string>>{{"1"}}));
    const auto description =
        db->DescribePrepared(*ast::parse_sql("select min(k), sum(v), count(*) from t order by k desc limit 1;"), {});
    ASSERT_EQ(description.types, (std::vector<deltakernel::DeltaValueType>{deltakernel::DeltaValueType::Int,
                                                                           deltakernel::DeltaValueType::Float,
                                                                           deltakernel::DeltaValueType::Int}));
}

TEST(DeltaDatabaseTest, MultiTableDistinctCount) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table order_line(ol_w_id int, ol_d_id int, ol_o_id int, ol_i_id int);");
    RunSql(*db, session, "create table stock(s_w_id int, s_i_id int, s_quantity int);");
    RunSql(*db, session, "insert into order_line values(1, 1, 1, 10);");
    RunSql(*db, session, "insert into order_line values(1, 1, 2, 10);");
    RunSql(*db, session, "insert into order_line values(1, 1, 3, 11);");
    RunSql(*db, session, "insert into stock values(1, 10, 9);");
    RunSql(*db, session, "insert into stock values(1, 11, 20);");
    auto result = Query(*db, session,
                        "select count(distinct ol_i_id) from order_line, stock where ol_w_id = 1 and ol_d_id = 1 "
                        "and ol_o_id < 20 and s_w_id = 1 and ol_i_id = s_i_id and s_quantity < 15;");
    ASSERT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"1"}}));
}

TEST(DeltaDatabaseTest, MultiTableSelfJoinKeepsAmbiguousLiteralForFullResolution) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int);");
    RunSql(*db, session, "insert into t values(1);");
    EXPECT_THROW(Query(*db, session, "select count(distinct k) from t, t where t.k < 0;"), std::runtime_error);
}

TEST(DeltaDatabaseTest, MultiTableStreamingProjection) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table a(k int, s char(8));");
    RunSql(*db, session, "create table b(k int, v float);");
    RunSql(*db, session, "insert into a values(1, 'one');");
    RunSql(*db, session, "insert into a values(2, 'two');");
    RunSql(*db, session, "insert into b values(2, 3.5);");
    RunSql(*db, session, "insert into b values(1, 9.5);");
    auto result = Query(*db, session, "select a.s, b.v from a, b where a.k = b.k and a.k > 1 and b.v < 9.0;");
    ASSERT_EQ(result.rows.size(), 1);
    ASSERT_EQ(result.rows[0][0], "two");
    EXPECT_EQ(Query(*db, session, "select a.s from a, b where a.k = b.k and a.k = 3;").begin_count, 1);
    CapturingSink invalid;
    EXPECT_THROW(db->Execute(ast::parse_sql("select a.s from a, b where a.missing = b.k;"), session, &invalid),
                 std::runtime_error);
    EXPECT_EQ(invalid.begin_count, 0);
}

TEST(DeltaDatabaseTest, MultiTablePairLoopContinuesAfterProjectionAndDistinctDuplicate) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table a(k int, v int);");
    RunSql(*db, session, "create table b(k int);");
    RunSql(*db, session, "insert into a values(1, 10);");
    RunSql(*db, session, "insert into a values(1, 10);");
    RunSql(*db, session, "insert into a values(1, 20);");
    RunSql(*db, session, "insert into a values(1, null);");
    RunSql(*db, session, "insert into b values(1);");
    ASSERT_EQ(Query(*db, session, "select a.v from a, b where a.k = b.k;").rows,
              (std::vector<std::vector<std::string>>{{"10"}, {"10"}, {"20"}, {"NULL"}}));
    ASSERT_EQ(Query(*db, session, "select count(distinct a.v) from a, b where a.k = b.k;").rows,
              (std::vector<std::vector<std::string>>{{"2"}}));
}

TEST(DeltaDatabaseTest, MultiTableJoinAccountsForUniqueRowClaims) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table a(k int);");
    RunSql(*db, session, "create table b(k int);");
    db.reset();
    WriteCatalog(temp.path() + "/DELTA_CATALOG",
                 "DELTAKERNEL\n3 3 2 2\nTABLE 1 1 a 1 1\nCOLUMN 0 4 1 k\nINDEX 1 1 1 0\n"
                 "TABLE 2 1 b 1 0\nCOLUMN 0 4 1 k\n");
    db = deltakernel::DeltaDatabase::Open(temp.path());
    RunSql(*db, session, "insert into a values(7);");
    RunSql(*db, session, "insert into b values(7);");
    ASSERT_EQ(Query(*db, session, "select a.k from a, b where a.k = b.k;").rows,
              (std::vector<std::vector<std::string>>{{"7"}}));
}

TEST(DeltaDatabaseTest, MultiTableCustomerWarehouseProjection) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table customer(c_w_id int, c_id int, c_last char(8));");
    RunSql(*db, session, "create table warehouse(w_id int, w_name char(8));");
    RunSql(*db, session, "insert into customer values(1, 7, 'smith');");
    RunSql(*db, session, "insert into customer values(2, 8, 'jones');");
    RunSql(*db, session, "insert into warehouse values(1, 'north');");
    RunSql(*db, session, "insert into warehouse values(2, 'south');");
    const auto result = Query(*db, session,
                              "select c.c_last, w.w_name from customer c, warehouse w "
                              "where c.c_w_id = w.w_id and c.c_id = 7;");
    ASSERT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"smith", "north"}}));
}

TEST(DeltaDatabaseTest, GroupByCountAndMax) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table g(w int, d int, v int);");
    RunSql(*db, session, "insert into g values(1, 1, 2);");
    RunSql(*db, session, "insert into g values(1, 1, 5);");
    RunSql(*db, session, "insert into g values(1, 2, 3);");
    auto result = Query(*db, session, "select w, d, count(*), min(v), max(v), sum(v) from g group by w, d;");
    ASSERT_EQ(result.rows,
              (std::vector<std::vector<std::string>>{{"1", "1", "2", "2", "5", "7"}, {"1", "2", "1", "3", "3", "3"}}));
    EXPECT_TRUE(Query(*db, session, "select w, count(*) from g where v < 0 group by w;").rows.empty());
}

TEST(DeltaDatabaseTest, EmptyStreamingSelectStartsQueryOnce) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int);");
    RunSql(*db, session, "insert into t values(1);");
    const auto empty = Query(*db, session, "select * from t where k = 2;");
    EXPECT_EQ(empty.begin_count, 1);
    EXPECT_EQ(empty.column_names, (std::vector<std::string>{"k"}));
    EXPECT_TRUE(empty.rows.empty());
    EXPECT_EQ(Query(*db, session, "select * from t;").begin_count, 1);
}

TEST(DeltaDatabaseTest, DeltaTracesNeverFallBackToLegacy) {
    ScopedStorageEngine storage_engine(nullptr);
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    db.reset();
    ASSERT_TRUE(std::filesystem::remove(temp.path() + "/DELTA_CATALOG"));
    EXPECT_TRUE(deltakernel::DeltaDatabase::IsDeltaDirectory(temp.path()));
    DatabaseInstance instance;
    EXPECT_FALSE(instance.has_legacy());
    EXPECT_THROW(instance.open_and_recover(temp.path()), std::runtime_error);
    EXPECT_FALSE(instance.has_legacy());
    EXPECT_FALSE(std::filesystem::exists(temp.path() + "/db.meta"));

    TempDelta truncated;
    db = deltakernel::DeltaDatabase::Create(truncated.path());
    db.reset();
    std::ofstream catalog(truncated.path() + "/DELTA_CATALOG", std::ios::trunc);
    catalog << "OLD_DELTA_FORMAT\n2\n1";
    catalog.close();
    EXPECT_TRUE(deltakernel::DeltaDatabase::IsDeltaDirectory(truncated.path()));
    EXPECT_THROW(deltakernel::DeltaDatabase::Open(truncated.path()), std::runtime_error);

    TempDelta legacy_shape;
    std::ofstream(legacy_shape.path() + "/MANIFEST");
    EXPECT_FALSE(deltakernel::DeltaDatabase::IsDeltaDirectory(legacy_shape.path()));
    std::ofstream(legacy_shape.path() + "/DELTA_CATALOG");
    EXPECT_FALSE(deltakernel::DeltaDatabase::IsDeltaDirectory(legacy_shape.path()));
    std::ofstream(legacy_shape.path() + "/wal.0");
    EXPECT_TRUE(deltakernel::DeltaDatabase::IsDeltaDirectory(legacy_shape.path()));

    TempDelta catalog_only;
    std::ofstream(catalog_only.path() + "/DELTA_CATALOG");
    EXPECT_FALSE(deltakernel::DeltaDatabase::IsDeltaDirectory(catalog_only.path()));
}

TEST(DeltaDatabaseTest, FreshDeltaInstanceDoesNotConstructLegacy) {
    ScopedStorageEngine storage_engine(nullptr);
    TempDelta temp;
    const std::string database_path = temp.path() + "/fresh";
    DatabaseInstance instance;
    instance.open_and_recover(database_path);
    EXPECT_TRUE(instance.is_delta());
    EXPECT_FALSE(instance.has_legacy());
    EXPECT_TRUE(deltakernel::DeltaDatabase::IsDeltaDirectory(database_path));
    EXPECT_FALSE(std::filesystem::exists(database_path + "/db.meta"));
    instance.close();
}

TEST(DeltaDatabaseTest, ExistingLegacyInstanceDoesNotRequireEnvironment) {
    TempDelta temp;
    const std::string database_path = temp.path() + "/legacy";
    {
        ScopedStorageEngine storage_engine("legacy");
        DatabaseInstance instance;
        instance.open_and_recover(database_path);
        EXPECT_FALSE(instance.is_delta());
        EXPECT_TRUE(instance.has_legacy());
    }

    {
        ScopedStorageEngine storage_engine(nullptr);
        DatabaseInstance instance;
        instance.open_and_recover(database_path);
        EXPECT_FALSE(instance.is_delta());
        EXPECT_TRUE(instance.has_legacy());
    }
}

TEST(DeltaDatabaseTest, StorageEngineConflictsFailClosed) {
    ScopedStorageEngine storage_engine(nullptr);
    TempDelta temp;
    const std::string database_path = temp.path() + "/delta";
    DatabaseInstance instance;
    instance.open_and_recover(database_path);
    instance.close();

    {
        ScopedStorageEngine delta("delta");
        DatabaseInstance compatible;
        compatible.open_and_recover(database_path);
        EXPECT_TRUE(compatible.is_delta());
    }

    {
        ScopedStorageEngine legacy("legacy");
        DatabaseInstance conflicting;
        EXPECT_THROW(conflicting.open_and_recover(database_path), RMDBError);
    }
}

TEST(DeltaDatabaseTest, DualStorageMarkersFailClosed) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    db.reset();
    std::ofstream(temp.path() + "/db.meta");

    const auto expect_rejected = [&](const char* engine) {
        ScopedStorageEngine storage_engine(engine);
        DatabaseInstance instance;
        EXPECT_THROW(instance.open_and_recover(temp.path()), RMDBError);
        EXPECT_FALSE(instance.is_delta());
        EXPECT_FALSE(instance.has_legacy());
    };
    expect_rejected(nullptr);
    expect_rejected("delta");
    expect_rejected("legacy");
}

TEST(DeltaDatabaseTest, CatalogRejectsReusedNextTableId) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    db.reset();
    WriteCatalog(temp.path() + "/DELTA_CATALOG",
                 "DELTAKERNEL\n2 1 1 1\nTABLE 1 1 t 2 0 0\nCOLUMN 0 4 1 k\nCOLUMN 0 4 1 v\n");
    EXPECT_THROW(deltakernel::DeltaDatabase::Open(temp.path()), std::runtime_error);
}

TEST(DeltaDatabaseTest, CatalogChecksumAndGenerationExhaustionFailClosed) {
    TempDelta checksum;
    auto db = deltakernel::DeltaDatabase::Create(checksum.path());
    db.reset();
    {
        std::fstream catalog(checksum.path() + "/DELTA_CATALOG", std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(catalog);
        catalog.seekp(0);
        catalog.put('X');
    }
    EXPECT_THROW(deltakernel::DeltaDatabase::Open(checksum.path()), std::runtime_error);

    TempDelta exhausted;
    db = deltakernel::DeltaDatabase::Create(exhausted.path());
    db.reset();
    WriteCatalog(exhausted.path() + "/DELTA_CATALOG", "DELTAKERNEL\n18446744073709551615 1 1 0\n");
    db = deltakernel::DeltaDatabase::Open(exhausted.path());
    deltakernel::DeltaSession session;
    EXPECT_THROW(RunSql(*db, session, "create table no_id(k int);"), std::runtime_error);
}

TEST(DeltaDatabaseTest, FailedCatalogSaveDoesNotPublishTable) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    db->SetCatalogSaveFailureForTest(true);
    EXPECT_THROW(RunSql(*db, session, "create table missing(k int, v int);"), std::runtime_error);
    EXPECT_THROW(RunSql(*db, session, "insert into missing values(1, 1);"), std::runtime_error);
    db->SetCatalogSaveFailureForTest(false);
    RunSql(*db, session, "create table live(k int, v int);");
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    RunSql(*db, session, "insert into live values(1, 1);");
}

TEST(DeltaDatabaseTest, PostRenameCatalogFailurePoisonsUntilReopen) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    db->SetCatalogPostRenameFailureForTest(true);
    EXPECT_THROW(RunSql(*db, session, "create table durable(k int);"), std::runtime_error);
    EXPECT_THROW(RunSql(*db, session, "show tables;"), std::logic_error);
    EXPECT_THROW(db->Checkpoint(), std::logic_error);
    EXPECT_THROW(db->CatalogGeneration(), std::logic_error);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    RunSql(*db, session, "insert into durable values(1);");
}

TEST(DeltaDatabaseTest, AbortSerializesWithExecute) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "insert into t values(1, 1);");
    std::mutex barrier_mutex;
    std::condition_variable barrier;
    bool entered = false;
    bool release = false;
    std::atomic<bool> abort_done{false};
    db->SetExecuteLockHookForTest([&] {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        entered = true;
        barrier.notify_all();
        barrier.wait(lock, [&] { return release; });
    });
    std::thread executor([&] { RunSql(*db, session, "select * from t;"); });
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        barrier.wait(lock, [&] { return entered; });
    }
    std::thread aborter([&] {
        db->Abort(session);
        abort_done.store(true);
    });
    std::this_thread::yield();
    EXPECT_FALSE(abort_done.load());
    {
        std::lock_guard<std::mutex> lock(barrier_mutex);
        release = true;
    }
    barrier.notify_all();
    executor.join();
    aborter.join();
    db->SetExecuteLockHookForTest({});
    EXPECT_FALSE(session.txn.has_value());
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "rollback;");
}

TEST(DeltaDatabaseTest, TypedRowsNullArithmeticAndCatalogSurviveRestart) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    const uint64_t initial_generation = db->CatalogGeneration();
    RunSql(*db, session, "create table typed(id int, amount float, note char(8), missing int);");
    EXPECT_EQ(db->CatalogGeneration(), initial_generation + 1);
    RunSql(*db, session, "set transaction isolation level snapshot isolation;");
    RunSql(*db, session, "insert into typed values(1, 1.5, 'ab', null);");
    RunSql(*db, session, "update typed set amount = amount + 0.25, note = 'xy', missing = 7 where id = 1;");
    auto result = Query(*db, session, "select note, amount, missing from typed where id = 1;");
    ASSERT_EQ(result.column_names, (std::vector<std::string>{"note", "amount", "missing"}));
    ASSERT_EQ(result.rows.size(), 1U);
    uint32_t expected_bits;
    const float expected = 1.75F;
    std::memcpy(&expected_bits, &expected, sizeof(expected_bits));
    EXPECT_EQ(result.rows[0], (std::vector<std::string>{"xy", std::to_string(expected_bits), "7"}));
    RunSql(*db, session, "insert into typed values(2, 2.5, 'nullrow', null);");
    result = Query(*db, session, "select missing from typed where id = 2;");
    ASSERT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"NULL"}}));
    db->Checkpoint();
    const uint64_t generation = db->CatalogGeneration();
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(db->CatalogGeneration(), generation);
    result = Query(*db, session, "select note, amount, missing from typed where id = 1;");
    EXPECT_EQ(result.rows[0], (std::vector<std::string>{"xy", std::to_string(expected_bits), "7"}));
    auto tables = Query(*db, session, "show tables;");
    EXPECT_EQ(tables.rows, (std::vector<std::vector<std::string>>{{"typed"}}));
}

TEST(DeltaDatabaseTest, NonUniqueIndexAllowsDuplicatesAndScanFallback) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table duplicate(id int, label char(8));");
    RunSql(*db, session, "create index duplicate(label);");
    RunSql(*db, session, "insert into duplicate values(1, 'same');");
    RunSql(*db, session, "insert into duplicate values(2, 'same');");
    auto result = Query(*db, session, "select id from duplicate where label = 'same';");
    EXPECT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"1"}, {"2"}}));
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    result = Query(*db, session, "select id from duplicate where label = 'same';");
    EXPECT_EQ(result.rows.size(), 2U);
}

TEST(DeltaDatabaseTest, CsvLoadUsesTypedRowCodec) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "1,1.25,alpha\n2,2.5,beta\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table loaded(id int, amount float, note char(8));");
    RunSql(*db, session, ("load " + csv + " into loaded;").c_str());
    auto result = Query(*db, session, "select id, note from loaded where amount >= 1.25;");
    EXPECT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"1", "alpha"}, {"2", "beta"}}));
}

TEST(DeltaDatabaseTest, CsvLoadResolvesRelativePathsFromDatabaseDirectory) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "1,alpha\n2,beta\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table relative_loaded(id int, note char(8));");
    RunSql(*db, session, "load ../rows.csv into relative_loaded;");
    EXPECT_EQ(Query(*db, session, "select id, note from relative_loaded;").rows,
              (std::vector<std::vector<std::string>>{{"1", "alpha"}, {"2", "beta"}}));

    RunSql(*db, session, "create table absolute_loaded(id int, note char(8));");
    RunSql(*db, session, ("load " + csv + " into absolute_loaded;").c_str());
    EXPECT_EQ(Query(*db, session, "select id, note from absolute_loaded;").rows,
              (std::vector<std::vector<std::string>>{{"1", "alpha"}, {"2", "beta"}}));
}

TEST(DeltaDatabaseTest, LoadPublishesImmutableTable) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv, std::ios::binary);
        output << " note ,id,missing,amount,\r\n"
                  "\"a,b\",1,,1.25,\r\n"
                  "\"a\"\"b\",2,7,2.5,\r\n"
                  ",3,,3.5,\r\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table loaded(id int, amount float, note char(8), missing int);");
    const uint64_t before = db->CatalogGeneration();
    RunSql(*db, session, ("load " + csv + " into loaded;").c_str());
    EXPECT_EQ(db->CatalogGeneration(), before);
    auto result = Query(*db, session, "select id, note, missing from loaded;");
    EXPECT_EQ(result.rows,
              (std::vector<std::vector<std::string>>{{"1", "a,b", "NULL"}, {"2", "a\"b", "7"}, {"3", "", "NULL"}}));
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path() + "/db");
    result = Query(*db, session, "select id from loaded;");
    EXPECT_EQ(result.rows.size(), 3U);
}

TEST(DeltaDatabaseTest, FailedLoadLeavesTableEmpty) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "id,note\n1,one\n2,two\n3,three\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table loaded(id int, note char(8));");
    {
        std::ofstream output(csv, std::ios::trunc);
        output << "id,note\n1,one\nbad,two\n";
    }
    EXPECT_THROW(RunSql(*db, session, ("load " + csv + " into loaded;").c_str()), std::runtime_error);
    EXPECT_TRUE(Query(*db, session, "select * from loaded;").rows.empty());
}

TEST(DeltaDatabaseTest, IndexedLoadUsesImmutableTable) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "id,note\n1,one\n2,two\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table loaded(id int, note char(8));");
    RunSql(*db, session, "create index loaded_id on loaded(id);");
    RunSql(*db, session, ("load " + csv + " into loaded;").c_str());
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path() + "/db");
    EXPECT_EQ(Query(*db, session, "select id from loaded;").rows,
              (std::vector<std::vector<std::string>>{{"1"}, {"2"}}));
}

TEST(DeltaDatabaseTest, LoadedCompositeIndexRechecksAndOverlayIsSafe) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "a,b,v\n1,2,10\n1,2,11\n1,3,12\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table loaded(a int, b int, v int);");
    RunSql(*db, session, "create index loaded_ab on loaded(a, b);");
    RunSql(*db, session, ("load " + csv + " into loaded;").c_str());
    const size_t validations = db->SidecarValidationCountForTest();
    EXPECT_EQ(Query(*db, session, "select v from loaded where b = 2 and a = 1 and v = 11;").rows,
              (std::vector<std::vector<std::string>>{{"11"}}));
    EXPECT_EQ(Query(*db, session, "select v from loaded where b = 2 and a = 1;").rows.size(), 2U);
    EXPECT_EQ(db->SidecarValidationCountForTest(), validations);
    EXPECT_EQ(Query(*db, session, "select v from loaded where a = 1;").rows.size(), 3U);
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "insert into loaded values(1, 2, 13);");
    EXPECT_EQ(Query(*db, session, "select v from loaded where a = 1 and b = 2;").rows.size(), 3U);
    RunSql(*db, session, "rollback;");
    RunSql(*db, session, "update loaded set b = 4 where a = 1 and b = 3;");
    EXPECT_TRUE(Query(*db, session, "select v from loaded where a = 1 and b = 3;").rows.empty());
    EXPECT_EQ(Query(*db, session, "select v from loaded where a = 1 and b = 4;").rows,
              (std::vector<std::vector<std::string>>{{"12"}}));
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path() + "/db");
    EXPECT_EQ(Query(*db, session, "select v from loaded where a = 1 and b = 4;").rows,
              (std::vector<std::vector<std::string>>{{"12"}}));
}

TEST(DeltaDatabaseTest, OrderedSidecarCoversPrefixRangeAggregateAndJoinSources) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table line(w int, d int, o int, n int, item int);");
    RunSql(*db, session, "create table stock(w int, item int, quantity int);");
    RunSql(*db, session, "create index line_wdo on line(w, d, o, n);");
    RunSql(*db, session, "create index stock_wi on stock(w, item);");
    RunSql(*db, session, "insert into line values(1, 1, 10, 1, 4);");
    RunSql(*db, session, "insert into line values(1, 1, 11, 1, 5);");
    RunSql(*db, session, "insert into line values(1, 1, 12, 1, 6);");
    RunSql(*db, session, "insert into line values(2, 1, 10, 1, 4);");
    RunSql(*db, session, "insert into stock values(1, 4, 7);");
    RunSql(*db, session, "insert into stock values(1, 5, 30);");
    db->Checkpoint();
    EXPECT_EQ(Query(*db, session, "select item from line where w = 1 and d = 1 and o >= 10 and o < 12;").rows,
              (std::vector<std::vector<std::string>>{{"4"}, {"5"}}));
    EXPECT_EQ(Query(*db, session, "select min(o) from line where w = 1 and d = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_EQ(Query(*db, session, "select o from line where w = 1 and d = 1 order by o desc limit 1;").rows,
              (std::vector<std::vector<std::string>>{{"12"}}));
    EXPECT_EQ(
        Query(*db, session,
              "select count(distinct line.item) from line, stock where line.w = 1 and line.d = 1 and line.o >= 10 "
              "and line.o < 12 and stock.w = 1 and stock.item = line.item and stock.quantity < 20;")
            .rows,
        (std::vector<std::vector<std::string>>{{"1"}}));
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, session, "select item from line where w = 1 and d = 1 and o >= 10 and o < 12;").rows.size(),
              2U);
}

TEST(DeltaDatabaseTest, IndexedTransactionOverlayPublishesOnlyOnCommit) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession writer, reader;
    RunSql(*db, writer, "create table t(k int, v int);");
    RunSql(*db, writer, "create index t_k on t(k);");
    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "insert into t values(7, 1);");
    EXPECT_EQ(Query(*db, writer, "select v from t where k = 7;").rows, (std::vector<std::vector<std::string>>{{"1"}}));
    EXPECT_TRUE(Query(*db, reader, "select v from t where k = 7;").rows.empty());
    RunSql(*db, writer, "rollback;");
    EXPECT_TRUE(Query(*db, reader, "select v from t where k = 7;").rows.empty());
    RunSql(*db, writer, "insert into t values(7, 2);");
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 7;").rows, (std::vector<std::vector<std::string>>{{"2"}}));
}

TEST(DeltaDatabaseTest, OpenRebuildsMissingSidecarOnce) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "k,v\n1,9\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, ("load " + csv + " into t;").c_str());
    db.reset();
    ASSERT_TRUE(std::filesystem::remove(temp.path() + "/db/deltaidx.1.1"));
    db = deltakernel::DeltaDatabase::Open(temp.path() + "/db");
    const size_t validations = db->SidecarValidationCountForTest();
    EXPECT_EQ(Query(*db, session, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"9"}}));
    EXPECT_EQ(Query(*db, session, "select v from t where k = 1;").rows.size(), 1U);
    EXPECT_EQ(db->SidecarValidationCountForTest(), validations);
}

TEST(DeltaDatabaseTest, CheckpointRebuildsDirtyIndexedTable) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, "insert into t values(3, 8);");
    db->Checkpoint();
    EXPECT_EQ(Query(*db, session, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"8"}}));
}

TEST(DeltaDatabaseTest, CreateIndexRebuildsExistingTable) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "insert into t values(4, 6);");
    RunSql(*db, session, "create index t_k on t(k);");
    EXPECT_EQ(Query(*db, session, "select v from t where k = 4;").rows, (std::vector<std::vector<std::string>>{{"6"}}));
}

TEST(DeltaDatabaseTest, PreparedDescriptionDryValidatesSchemaAndParameters) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table typed(id int, amount float, note char(8));");
    auto select = ast::parse_sql("select note, amount from typed where id = $1;");
    auto description = db->DescribePrepared(*select, {deltakernel::DeltaValueType::Int});
    EXPECT_TRUE(description.query);
    EXPECT_EQ(description.names, (std::vector<std::string>{"note", "amount"}));
    EXPECT_EQ(description.types, (std::vector<deltakernel::DeltaValueType>{deltakernel::DeltaValueType::Char,
                                                                           deltakernel::DeltaValueType::Float}));
    EXPECT_THROW(db->DescribePrepared(*select, {deltakernel::DeltaValueType::Char}), std::runtime_error);
    auto unknown = ast::parse_sql("select note from typed where missing = $1;");
    EXPECT_THROW(db->DescribePrepared(*unknown, {deltakernel::DeltaValueType::Int}), std::runtime_error);
    auto ordered = ast::parse_sql("select note from typed order by id;");
    EXPECT_NO_THROW(db->DescribePrepared(*ordered, {}));
    auto insert = ast::parse_sql("insert into typed values($1, $2, $3);");
    EXPECT_NO_THROW(db->DescribePrepared(*insert, {deltakernel::DeltaValueType::Int, deltakernel::DeltaValueType::Float,
                                                   deltakernel::DeltaValueType::Char}));
}

TEST(DeltaDatabaseTest, PreparedDescriptionResolvesNewOrderMultiTableColumns) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(
        *db, session,
        "create table customer(c_discount float, c_last char(16), c_credit char(2), c_w_id int, c_d_id int, c_id int, "
        "shared int);");
    RunSql(*db, session, "create table warehouse(w_id int, w_tax float, shared int);");
    auto select = ast::parse_sql("select c_discount,c_last,c_credit,w_tax from customer,warehouse where w_id=$1 and "
                                 "c_w_id=w_id and c_d_id=$2 and c_id=$3;");
    const auto description =
        db->DescribePrepared(*select, {deltakernel::DeltaValueType::Int, deltakernel::DeltaValueType::Int,
                                       deltakernel::DeltaValueType::Int});
    EXPECT_EQ(description.names, (std::vector<std::string>{"c_discount", "c_last", "c_credit", "w_tax"}));
    EXPECT_EQ(description.types, (std::vector<deltakernel::DeltaValueType>{
                                     deltakernel::DeltaValueType::Float, deltakernel::DeltaValueType::Char,
                                     deltakernel::DeltaValueType::Char, deltakernel::DeltaValueType::Float}));
    EXPECT_THROW(db->DescribePrepared(*select, {deltakernel::DeltaValueType::Char, deltakernel::DeltaValueType::Int,
                                                deltakernel::DeltaValueType::Int}),
                 std::runtime_error);
    auto ambiguous = ast::parse_sql("select shared from customer, warehouse where c_w_id = w_id;");
    EXPECT_THROW(db->DescribePrepared(*ambiguous, {}), std::runtime_error);
    auto star = ast::parse_sql("select * from customer, warehouse where c_w_id = w_id;");
    EXPECT_THROW(db->DescribePrepared(*star, {}), std::runtime_error);
    auto invalid_count = ast::parse_sql("select count(distinct missing) from customer, warehouse where c_w_id = w_id;");
    EXPECT_THROW(db->DescribePrepared(*invalid_count, {}), std::runtime_error);
}

TEST(DeltaDatabaseTest, CharResultPreservesEmptyAndEmbeddedNulLengths) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table chars(id int, value char(8));");
    std::vector<std::unique_ptr<ast::Value>> values;
    values.push_back(std::make_unique<ast::IntLit>(1));
    values.push_back(std::make_unique<ast::StringLit>(std::string("a\0b", 3)));
    db->Execute(std::make_unique<ast::InsertStmt>("chars", std::move(values)), session, nullptr);
    RunSql(*db, session, "insert into chars values(2, '');");
    auto result = Query(*db, session, "select value from chars;");
    ASSERT_EQ(result.rows.size(), 2U);
    EXPECT_EQ(result.rows[0][0], std::string("a\0b", 3));
    EXPECT_TRUE(result.rows[1][0].empty());
}
} // namespace
