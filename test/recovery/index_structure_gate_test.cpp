/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Recovery's index structure gate walks only the root->leaf descent paths of the
// keys the WAL names, instead of the whole tree. These tests pin down what makes
// that worth doing, and the three things it must not do:
//
//   1. a broken parent back pointer is repaired in place - the whole-tree
//      checker's answer to the same damage is a full index rebuild, and the
//      negative control here runs that path in the same process to prove it;
//   2. an *empty leaf* is a legal steady state and must not be reported as
//      damage. TPC-C reaches this on every index over new_orders;
//   3. an orphan leaf - one the chain names and the descent cannot reach - is
//      the failure that actually loses keys, and a change-set-sized gate must
//      still catch it;
//   4. recovering the same crash state twice must produce the same tree;
//   5. a header that is behind the tree must make the gate decline rather than
//      "repair" pointers on the authority of a root page number it cannot trust.
//
// Every corruption below is applied to the closed database's pages, which is the
// state a restart after SIGKILL inherits.

#include "index/ix.h"
#include "record/rm.h"
#include "recovery/log_manager.h"
#include "recovery/log_recovery.h"
#include "storage/buffer_pool_manager.h"
#include "system/sm.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace {

// One INT key gives btree_order 338, so 2000 rows build a two-level tree with a
// handful of leaves under an internal root - the smallest shape that has a
// parent back pointer to break at all.
constexpr int kRowCount = 2000;

class ScopedGateTestDir {
public:
    explicit ScopedGateTestDir(std::string dir) : old_path_(std::filesystem::current_path()), dir_(std::move(dir)) {
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directory(dir_);
        std::filesystem::current_path(dir_);
    }

    ~ScopedGateTestDir() {
        std::filesystem::current_path(old_path_);
        std::filesystem::remove_all(dir_);
    }

private:
    std::filesystem::path old_path_;
    std::filesystem::path dir_;
};

// Must match the record_size create_table computes for two INT columns.
RmRecord MakeRow(int id, int value) {
    RmRecord rec(static_cast<int>(sizeof(int) * 2) + null_bitmap_bytes(2));
    memset(rec.data, 0, static_cast<size_t>(rec.size));
    memcpy(rec.data, &id, sizeof(int));
    memcpy(rec.data + sizeof(int), &value, sizeof(int));
    return rec;
}

std::vector<char> MakeKey(int value) {
    std::vector<char> key(sizeof(int));
    memcpy(key.data(), &value, sizeof(int));
    return key;
}

class OpenDb {
public:
    explicit OpenDb(const std::string& db_name)
        : bpm_(256, &disk_), rm_mgr_(&disk_, &bpm_), ix_mgr_(&disk_, &bpm_), sm_mgr_(&disk_, &bpm_, &rm_mgr_, &ix_mgr_),
          log_mgr_(std::make_unique<LogManager>(&disk_)) {
        sm_mgr_.open_db(db_name);
    }

    ~OpenDb() {
        sm_mgr_.close_db();
    }

    IxIndexHandle* index() {
        return sm_mgr_.ihs_.at(sm_mgr_.get_ix_manager()->get_index_name("t", {"id"})).get();
    }

    DiskManager disk_;
    BufferPoolManager bpm_;
    RmManager rm_mgr_;
    IxManager ix_mgr_;
    SmManager sm_mgr_;
    std::unique_ptr<LogManager> log_mgr_;
};

// Creates the database and loads kRowCount rows through the heap and the index,
// then closes it so every page and both headers are on disk. Returns rid[i] for
// key i.
std::vector<Rid> CreateLoadedDb(const std::string& db_name) {
    std::vector<Rid> rids;
    {
        DiskManager disk;
        BufferPoolManager bpm(256, &disk);
        RmManager rm_mgr(&disk, &bpm);
        IxManager ix_mgr(&disk, &bpm);
        SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);
        sm_mgr.create_db(db_name);
        sm_mgr.open_db(db_name);
        sm_mgr.create_table("t", {{"id", TYPE_INT, sizeof(int)}, {"v", TYPE_INT, sizeof(int)}}, nullptr);
        sm_mgr.create_index("t", {"id"}, nullptr);

        auto* file_handle = sm_mgr.fhs_.at("t").get();
        auto* index = sm_mgr.ihs_.at(sm_mgr.get_ix_manager()->get_index_name("t", {"id"})).get();
        rids.reserve(kRowCount);
        for (int i = 0; i < kRowCount; ++i) {
            auto row = MakeRow(i, i * 10);
            const Rid rid = file_handle->insert_record(row.data, nullptr);
            index->insert_entry(MakeKey(i).data(), rid, IndexWriteWalContext::TestNoWal());
            rids.push_back(rid);
        }
        sm_mgr.close_db();
    }
    return rids;
}

std::string IndexFileName(const std::string& db_name) {
    return db_name + "/t_id.idx";
}

// Opens the index file outside any SmManager and hands the caller the header plus
// a live buffer-pool page, the same way rmdb_verify reads an index. Everything
// the caller dirties is flushed on destruction, so the corruption lands on disk.
class RawIndexFile {
public:
    explicit RawIndexFile(const std::string& index_path) : bpm_(64, &disk_) {
        fd_ = disk_.open_file(index_path);
        std::vector<char> header_page(PAGE_SIZE, 0);
        disk_.read_page(fd_, IX_FILE_HDR_PAGE, header_page.data(), PAGE_SIZE);
        hdr_.deserialize(header_page.data());
    }

    ~RawIndexFile() {
        bpm_.flush_all_pages(fd_);
        disk_.close_file(fd_);
    }

    const IxFileHdr& hdr() const {
        return hdr_;
    }

    // Rewrites page 0 with a different root page number, which is how "the header
    // is behind the tree" is constructed on purpose.
    void repoint_root(page_id_t root_page) {
        std::vector<char> header_page(PAGE_SIZE, 0);
        hdr_.root_page_ = root_page;
        hdr_.serialize(header_page.data());
        disk_.write_page(fd_, IX_FILE_HDR_PAGE, header_page.data(), PAGE_SIZE);
    }

    // Runs `body` over the node on `page_no`; the page is marked dirty when
    // `dirty` is true, so the change survives the destructor's flush.
    template <typename Body> void with_node(page_id_t page_no, bool dirty, Body body) {
        Page* raw = bpm_.fetch_page(PageId{fd_, page_no});
        ASSERT_NE(raw, nullptr);
        IxNodeHandle node(&hdr_, raw);
        body(node);
        bpm_.unpin_page(raw->get_page_id(), dirty);
    }

private:
    DiskManager disk_;
    BufferPoolManager bpm_;
    IxFileHdr hdr_;
    int fd_{-1};
};

// Committed single-record UPDATE that rewrites the row with its own content. It
// puts the row's key into the change set without changing anything, which is
// exactly what is needed to make the gate descend to a chosen leaf.
void AppendCommittedSelfUpdate(LogManager& log_mgr, const Rid& rid, int key, int value) {
    BeginLogRecord begin(1);
    lsn_t prev = log_mgr.add_log_to_buffer(&begin);
    auto old_row = MakeRow(key, value);
    auto new_row = MakeRow(key, value);
    Rid log_rid = rid;
    UpdateLogRecord update(1, old_row, new_row, log_rid, "t");
    update.prev_lsn_ = prev;
    prev = log_mgr.add_log_to_buffer(&update);
    CommitLogRecord commit(1);
    commit.prev_lsn_ = prev;
    log_mgr.add_log_to_buffer(&commit);
    log_mgr.flush_log_to_disk();
}

// Committed single-record DELETE, used to name a key whose row is already gone
// from the heap so the change set reaches an emptied leaf.
void AppendCommittedDelete(LogManager& log_mgr, const Rid& rid, int key, int value) {
    BeginLogRecord begin(1);
    lsn_t prev = log_mgr.add_log_to_buffer(&begin);
    auto row = MakeRow(key, value);
    Rid log_rid = rid;
    DeleteLogRecord del(1, row, log_rid, "t");
    del.prev_lsn_ = prev;
    prev = log_mgr.add_log_to_buffer(&del);
    CommitLogRecord commit(1);
    commit.prev_lsn_ = prev;
    log_mgr.add_log_to_buffer(&commit);
    log_mgr.flush_log_to_disk();
}

struct RecoveryOutcome {
    uint64_t rebuilds{0};
    uint64_t parent_pointer_repairs{0};
    bool structure_valid{false};
    // Every key 0..kRowCount-1 resolves to exactly the rid it was loaded with.
    bool all_keys_resolve{false};
};

RecoveryOutcome RunRecoveryAndInspect(const std::string& db_name, const std::vector<Rid>& rids,
                                      const std::vector<int>& missing_keys = {}) {
    RecoveryOutcome outcome;
    OpenDb db(db_name);
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    recovery.redo();
    recovery.undo();
    outcome.rebuilds = recovery.get_index_rebuild_count();
    outcome.parent_pointer_repairs = recovery.get_index_parent_pointer_repair_count();
    outcome.structure_valid = db.index()->validate_structure();

    outcome.all_keys_resolve = true;
    for (int i = 0; i < kRowCount; ++i) {
        const bool expected_missing = std::find(missing_keys.begin(), missing_keys.end(), i) != missing_keys.end();
        std::vector<Rid> found;
        db.index()->get_value(MakeKey(i).data(), &found, nullptr);
        if (expected_missing) {
            if (!found.empty()) {
                outcome.all_keys_resolve = false;
            }
            continue;
        }
        if (found.size() != 1 || !(found[0] == rids[static_cast<size_t>(i)])) {
            outcome.all_keys_resolve = false;
        }
    }
    return outcome;
}

} // namespace

// A parent back pointer that names the wrong page is the one invariant a real
// SIGKILL was measured to break. Reaching the page by descending from its parent
// proves what the pointer should say, so the gate writes it and keeps going.
TEST(IndexStructureGateTest, BrokenParentBackPointerIsRepairedInPlace) {
    ScopedGateTestDir test_dir("index_gate_parent_pointer_root");
    const std::string db_name = "index_gate_parent_pointer_db";
    const std::vector<Rid> rids = CreateLoadedDb(db_name);

    int first_key_under_broken_child = -1;
    {
        RawIndexFile raw(IndexFileName(db_name));
        page_id_t broken_child = IX_NO_PAGE;
        raw.with_node(raw.hdr().root_page_, false, [&](IxNodeHandle& root) {
            ASSERT_FALSE(root.is_leaf_page()) << "the fixture did not build a multi-level tree";
            ASSERT_GE(root.get_size(), 2);
            broken_child = root.value_at(1);
        });
        ASSERT_NE(broken_child, IX_NO_PAGE);
        raw.with_node(broken_child, true, [&](IxNodeHandle& child) {
            ASSERT_GT(child.get_size(), 0);
            memcpy(&first_key_under_broken_child, child.get_key(0), sizeof(int));
            // IX_NO_PAGE makes the child claim to be a root, which is what a
            // half-persisted root split leaves behind.
            child.set_parent_page_no(IX_NO_PAGE);
        });
    }
    ASSERT_GT(first_key_under_broken_child, 0);
    ASSERT_LT(first_key_under_broken_child, kRowCount);

    {
        OpenDb db(db_name);
        AppendCommittedSelfUpdate(*db.log_mgr_, rids[static_cast<size_t>(first_key_under_broken_child)],
                                  first_key_under_broken_child, first_key_under_broken_child * 10);
    }

    const RecoveryOutcome outcome = RunRecoveryAndInspect(db_name, rids);
    EXPECT_EQ(outcome.parent_pointer_repairs, 1u) << "the gate did not notice the wrong parent back pointer";
    EXPECT_EQ(outcome.rebuilds, 0u) << "the index was rebuilt from the heap instead of being repaired in place";
    EXPECT_TRUE(outcome.structure_valid) << "the whole-tree checker still rejects the repaired tree";
    EXPECT_TRUE(outcome.all_keys_resolve);
}

// An emptied leaf stays in the tree and in the leaf chain forever, because
// coalesce_or_redistribute() is unreachable from the delete path. Rejecting it
// would send every index over new_orders into a rebuild after every crash, so
// both the gate and the whole-tree checker have to accept it.
TEST(IndexStructureGateTest, AnEmptiedLeafIsNotReportedAsDamage) {
    ScopedGateTestDir test_dir("index_gate_empty_leaf_root");
    const std::string db_name = "index_gate_empty_leaf_db";
    const std::vector<Rid> rids = CreateLoadedDb(db_name);

    // Empty the leftmost leaf exactly the way Delivery does: delete its keys in
    // ascending order and let the leaf stay where it is.
    int emptied_leaf_key_count = 0;
    {
        RawIndexFile raw(IndexFileName(db_name));
        raw.with_node(raw.hdr().root_page_, false, [&](IxNodeHandle& root) {
            ASSERT_FALSE(root.is_leaf_page());
            const page_id_t leftmost = root.value_at(0);
            raw.with_node(leftmost, false, [&](IxNodeHandle& leaf) { emptied_leaf_key_count = leaf.get_size(); });
        });
    }
    ASSERT_GT(emptied_leaf_key_count, 0);
    ASSERT_LT(emptied_leaf_key_count, kRowCount);

    std::vector<int> deleted_keys;
    {
        OpenDb db(db_name);
        auto* file_handle = db.sm_mgr_.fhs_.at("t").get();
        for (int i = 0; i < emptied_leaf_key_count; ++i) {
            ASSERT_TRUE(db.index()->delete_entry(MakeKey(i).data(), rids[static_cast<size_t>(i)],
                                                 IndexWriteWalContext::TestNoWal()));
            file_handle->delete_record(rids[static_cast<size_t>(i)], nullptr);
            deleted_keys.push_back(i);
        }
    }

    // Verify the fixture really produced an empty leaf; without that this test
    // proves nothing.
    {
        RawIndexFile raw(IndexFileName(db_name));
        int leftmost_size = -1;
        raw.with_node(raw.hdr().root_page_, false, [&](IxNodeHandle& root) {
            const page_id_t leftmost = root.value_at(0);
            raw.with_node(leftmost, false, [&](IxNodeHandle& leaf) {
                ASSERT_TRUE(leaf.is_leaf_page());
                leftmost_size = leaf.get_size();
            });
        });
        ASSERT_EQ(leftmost_size, 0) << "the fixture did not leave an empty leaf in the tree";
    }

    // Name one of the deleted keys in the WAL, so the change set makes the gate
    // descend into the empty leaf rather than merely past it.
    {
        OpenDb db(db_name);
        AppendCommittedDelete(*db.log_mgr_, rids[0], 0, 0);
    }

    const RecoveryOutcome outcome = RunRecoveryAndInspect(db_name, rids, deleted_keys);
    EXPECT_EQ(outcome.rebuilds, 0u) << "an empty leaf was mistaken for damage";
    EXPECT_EQ(outcome.parent_pointer_repairs, 0u);
    EXPECT_TRUE(outcome.structure_valid) << "the whole-tree checker disagrees with the gate about the empty leaf";
    EXPECT_TRUE(outcome.all_keys_resolve);
}

// The orphan leaf: a leaf the chain names and no descent can reach. Its keys are
// invisible to every lookup, so this is the one gate failure that really loses
// data - and a gate that only walks the change set still has to catch it. The
// fallback is the full rebuild, which is what makes the keys reachable again.
TEST(IndexStructureGateTest, AnOrphanLeafIsCaughtAndFallsBackToRebuild) {
    ScopedGateTestDir test_dir("index_gate_orphan_leaf_root");
    const std::string db_name = "index_gate_orphan_leaf_db";
    const std::vector<Rid> rids = CreateLoadedDb(db_name);

    int first_key_of_orphan = -1;
    {
        RawIndexFile raw(IndexFileName(db_name));
        page_id_t orphan = IX_NO_PAGE;
        raw.with_node(raw.hdr().root_page_, true, [&](IxNodeHandle& root) {
            ASSERT_FALSE(root.is_leaf_page());
            ASSERT_GE(root.get_size(), 3);
            orphan = root.value_at(1);
            // Drop the separator that makes this leaf reachable, leaving it in
            // the leaf chain - the on-disk shape of a leaf split whose parent
            // update never landed.
            root.erase_pair(1);
        });
        ASSERT_NE(orphan, IX_NO_PAGE);
        raw.with_node(orphan, false, [&](IxNodeHandle& leaf) {
            ASSERT_GT(leaf.get_size(), 0);
            memcpy(&first_key_of_orphan, leaf.get_key(0), sizeof(int));
        });
    }
    ASSERT_GT(first_key_of_orphan, 0);

    {
        OpenDb db(db_name);
        AppendCommittedSelfUpdate(*db.log_mgr_, rids[static_cast<size_t>(first_key_of_orphan)], first_key_of_orphan,
                                  first_key_of_orphan * 10);
    }

    const RecoveryOutcome outcome = RunRecoveryAndInspect(db_name, rids);
    EXPECT_EQ(outcome.rebuilds, 1u) << "the change-set gate missed a leaf that no descent can reach";
    EXPECT_TRUE(outcome.structure_valid);
    EXPECT_TRUE(outcome.all_keys_resolve) << "the rebuild did not make the orphaned keys reachable again";
}

// Recovery is retried from the identical WAL whenever it does not finish, so the
// gate's in-place repair has to be idempotent: the same crash state recovered
// twice must give the same tree and the same verdict.
TEST(IndexStructureGateTest, TheInPlaceRepairIsIdempotentAcrossRepeatedRecovery) {
    ScopedGateTestDir test_dir("index_gate_idempotent_root");
    const std::string db_name = "index_gate_idempotent_db";
    const std::vector<Rid> rids = CreateLoadedDb(db_name);

    int first_key_under_broken_child = -1;
    {
        RawIndexFile raw(IndexFileName(db_name));
        page_id_t broken_child = IX_NO_PAGE;
        raw.with_node(raw.hdr().root_page_, false, [&](IxNodeHandle& root) {
            ASSERT_GE(root.get_size(), 2);
            broken_child = root.value_at(1);
        });
        raw.with_node(broken_child, true, [&](IxNodeHandle& child) {
            memcpy(&first_key_under_broken_child, child.get_key(0), sizeof(int));
            child.set_parent_page_no(IX_NO_PAGE);
        });
    }

    {
        OpenDb db(db_name);
        AppendCommittedSelfUpdate(*db.log_mgr_, rids[static_cast<size_t>(first_key_under_broken_child)],
                                  first_key_under_broken_child, first_key_under_broken_child * 10);
    }

    // Snapshot the crash state so the second recovery starts from the identical
    // input, WAL included. Recovery truncates the WAL on success, so replaying
    // without the snapshot would not exercise the same path at all.
    const std::string snapshot = db_name + "_snapshot";
    std::filesystem::remove_all(snapshot);
    std::filesystem::copy(db_name, snapshot, std::filesystem::copy_options::recursive);

    const RecoveryOutcome first = RunRecoveryAndInspect(db_name, rids);
    std::filesystem::remove_all(db_name);
    std::filesystem::copy(snapshot, db_name, std::filesystem::copy_options::recursive);
    const RecoveryOutcome second = RunRecoveryAndInspect(db_name, rids);

    EXPECT_EQ(first.parent_pointer_repairs, second.parent_pointer_repairs);
    EXPECT_EQ(first.rebuilds, second.rebuilds);
    EXPECT_EQ(first.structure_valid, second.structure_valid);
    EXPECT_EQ(first.all_keys_resolve, second.all_keys_resolve);
    EXPECT_EQ(first.parent_pointer_repairs, 1u);
    EXPECT_EQ(first.rebuilds, 0u);
    EXPECT_TRUE(first.structure_valid);
    EXPECT_TRUE(first.all_keys_resolve);
}

// The gate reads the root page number from page 0 of the index file. If page 0
// were ever behind the tree - a root split whose header write was lost - the
// page it calls the root would still carry a parent pointer, and "repairing"
// that pointer to IX_NO_PAGE would cut the real root off the tree. The gate has
// to recognize that it is the header it cannot trust, decline, and let the
// whole-tree checker (which derives the root from the handle's own state) decide.
TEST(IndexStructureGateTest, AHeaderBehindTheTreeMakesTheGateDeclineInsteadOfRepairing) {
    ScopedGateTestDir test_dir("index_gate_stale_header_root");
    const std::string db_name = "index_gate_stale_header_db";
    const std::vector<Rid> rids = CreateLoadedDb(db_name);

    int first_key_under_child = -1;
    {
        RawIndexFile raw(IndexFileName(db_name));
        page_id_t child = IX_NO_PAGE;
        raw.with_node(raw.hdr().root_page_, false, [&](IxNodeHandle& root) {
            ASSERT_FALSE(root.is_leaf_page());
            ASSERT_GE(root.get_size(), 2);
            child = root.value_at(1);
        });
        ASSERT_NE(child, IX_NO_PAGE);
        raw.with_node(child, false, [&](IxNodeHandle& leaf) {
            ASSERT_GT(leaf.get_size(), 0);
            memcpy(&first_key_under_child, leaf.get_key(0), sizeof(int));
        });
        // Page 0 now names a page that is a child, not the root.
        raw.repoint_root(child);
    }

    {
        OpenDb db(db_name);
        AppendCommittedSelfUpdate(*db.log_mgr_, rids[static_cast<size_t>(first_key_under_child)], first_key_under_child,
                                  first_key_under_child * 10);
    }

    const RecoveryOutcome outcome = RunRecoveryAndInspect(db_name, rids);
    EXPECT_EQ(outcome.parent_pointer_repairs, 0u)
        << "the gate rewrote a parent pointer on the authority of a header it should not have trusted";
    // Declining hands the decision to validate_structure(), which sees a root
    // that does not reach most of the tree and rebuilds. Slow, but correct.
    EXPECT_EQ(outcome.rebuilds, 1u);
    EXPECT_TRUE(outcome.structure_valid);
    EXPECT_TRUE(outcome.all_keys_resolve);
}
