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

#include "index/ix_index_handle.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * Recovery's B+tree structure gate, priced against the WAL's change set instead
 * of against the tree.
 *
 * Why a second checker exists next to IxIndexHandle::validate_structure().
 * ------------------------------------------------------------------------
 * validate_structure() is a whole-tree DFS plus a whole-leaf-chain walk: it
 * reads every page of the index (leaves twice) and holds two page-number sets
 * sized by the tree. Measured, that is 0.5-6.4 us per page depending on where
 * the page comes from, i.e. 3.5-5 s for a 2-3 GB index with a warm page cache
 * and read-bandwidth-bound beyond that. Recovery's own budget is 90 s
 * (`final.md:53`), so the absolute number was survivable - but the *shape* was
 * wrong twice over: the cost grows with the database rather than with the crash,
 * and the pages it reads evict exactly the pages redo and the key-level repair
 * are about to need.
 *
 * The change set is provably sufficient. Only a structure-modification
 * operation (a split, a new root, a separator update) can leave the persisted
 * tree inconsistent, and every SMO is triggered by an insert or a delete that
 * is in the retained WAL. So the distinct keys that
 * RecoveryManager::collect_index_repair_keys() already produces cover every
 * subtree an interrupted SMO could have damaged. Validating the root->leaf
 * descent path of each of those keys therefore inspects a superset of the
 * damaged pages, at O(distinct leaves x height) instead of O(tree).
 *
 * This is what InnoDB does: it never traverses a tree during crash recovery
 * (CHECK TABLE does), relying on page-level redo, page checksums and page LSNs
 * for page-level correctness. Our index pages carry no LSN - IxPageHdr occupies
 * Page::OFFSET_LSN - so the descent-path check is the substitute for the page
 * LSN, and commit 64bd297 (SMO whole-dirty-set write-out) is the substitute for
 * physical redo.
 *
 * Division of labour, so nobody has to guess later:
 *   - this class runs on every recovery and covers the change set;
 *   - validate_structure() stays as the whole-tree assertion and is reachable
 *     from tests (test/index/) and from recovery under
 *     RMDB_RECOVERY_FULL_INDEX_VALIDATION=1. It is the reference oracle this
 *     class is checked against, and the escape hatch when the gate cannot even
 *     set itself up.
 *
 * How to use it: construct one per index, then feed it every distinct repair key
 * in ascending B+tree order and stop at the first false. `Ready` means the
 * descents can run; `EmptyTree` means there is nothing to check; `Unusable`
 * means the index header itself could not be interpreted, and the caller must
 * fall back to validate_structure() rather than assume either answer.
 *
 * Thread safety: none. It is built for the single-threaded recovery window,
 * before the server accepts connections.
 */
class RecoveryIndexGate {
public:
    enum class Setup {
        Ready,     // descents can run
        EmptyTree, // no root page yet, so there is nothing to validate
        Unusable,  // the header could not be interpreted; caller must fall back
    };

    struct Stats {
        // root->leaf walks actually performed. Compare against keys_covered to
        // see how well the change set clusters into leaves.
        uint64_t descents = 0;
        // Keys answered from the leaf the previous descent already validated.
        uint64_t keys_covered = 0;
        // Distinct pages whose invariants were checked. This is the honest "how
        // much of the tree did the gate really look at" number.
        uint64_t pages_validated = 0;
        // Buffer-pool fetches issued, including repeat visits and neighbour
        // leaves. The gate's I/O cost is bounded by this.
        uint64_t page_fetches = 0;
        // In-place A-1 repairs: a child whose parent back pointer named the
        // wrong page. Non-zero is an event worth reading the log for, but it is
        // a repair, not a failure.
        uint64_t parent_pointers_repaired = 0;
        // Leaves whose successor in the chain was empty, so the separator/chain
        // agreement check (INV-7) had to be skipped for them. Expected to be
        // small and non-zero on TPC-C: new_orders accumulates empty leaves.
        uint64_t chain_bounds_unknown = 0;
    };

    RecoveryIndexGate(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, IxIndexHandle* index,
                      std::string index_name);

    Setup setup() const {
        return setup_;
    }

    /**
     * Validate the root->leaf descent path for one key.
     *
     * Returns false the first time an invariant is violated that cannot be
     * repaired in place, which means the index has to be rebuilt from the heap.
     * A violated parent back pointer is repaired here and does not fail the
     * gate. Once false is returned the gate must not be used again.
     *
     * After a false, re-read setup(): if it flipped to Unusable the gate is not
     * accusing the tree, it is declining to judge it (see decline()), and the
     * caller must fall back to validate_structure() rather than rebuild.
     *
     * Keys are expected in ascending B+tree order; that is what lets a run of
     * keys inside one leaf share a single descent. Out-of-order keys stay
     * *correct* (the skip test is guarded by the key that established the
     * current leaf), they only cost extra descents.
     */
    bool check_key(const char* key);

    const Stats& stats() const {
        return stats_;
    }

private:
    // A tree deeper than this is not a tree; the bound turns a cycle in the
    // child pointers into a rejection instead of an infinite descent. With a
    // fan-out of at least two, 64 levels already exceeds any addressable file.
    static constexpr int kMaxDescentLevels = 64;
    // How far the separator/chain agreement check may hop over empty leaves to
    // find a usable upper bound. Empty leaves are a legal steady state (see
    // INV-1) and TPC-C's new_orders accumulates a run of them at the left edge,
    // but that run must not turn one descent into a chain walk.
    static constexpr int kMaxEmptyLeafHops = 4;

    bool reject(const char* invariant, page_id_t page_no);
    // "The gate cannot trust its own inputs", as opposed to reject()'s "the tree
    // is broken". Flips setup() to Unusable so the caller falls back to
    // validate_structure() instead of rebuilding the index.
    bool decline(const char* reason);
    Page* fetch(page_id_t page_no);
    void release(Page* page, bool dirty);
    bool page_in_range(page_id_t page_no) const;
    int compare(const char* left, const char* right) const;

    bool descend(const char* key);
    // Per-page invariants that hold for every node on a descent path. Sets
    // *dirty when it repaired the parent back pointer in place.
    bool validate_node(IxNodeHandle& node, page_id_t page_no, page_id_t expected_parent, bool* dirty);
    // Leaf-only invariants plus the bookkeeping that lets the next key skip its
    // descent. `key` is the key this descent was issued for.
    bool validate_leaf_and_remember(IxNodeHandle& leaf, page_id_t page_no, const char* key);
    bool covered_by_current_leaf(const char* key) const;
    void remember_bound(std::vector<char>* slot, bool* have, const char* key);

    DiskManager* disk_manager_;
    BufferPoolManager* buffer_pool_manager_;
    IxIndexHandle* index_;
    std::string index_name_;
    int fd_{-1};
    Setup setup_{Setup::Unusable};
    // Deserialized from page 0 of the index file, the same way rmdb_verify does
    // it. At this point in recovery it is identical to the handle's in-memory
    // header: redo and undo touch heap pages only, so no SMO has run since
    // IxManager::open_index() read this very page, and the geometry fields are
    // cross-checked against the handle's public accessors in the constructor.
    IxFileHdr hdr_;

    // Pages already validated, mapped to the parent they were reached from.
    // Sized by the change set, not by the tree. The mapping - rather than a
    // plain set - is what detects a page reachable from two different parents,
    // which is validate_structure()'s "page reachable more than once".
    std::unordered_map<page_id_t, page_id_t> validated_parent_;

    // The leaf the last descent validated, and the two bounds that let a
    // following key skip its own descent. See covered_by_current_leaf().
    bool have_leaf_{false};
    std::vector<char> leaf_descent_key_; // the key that reached this leaf
    std::vector<char> leaf_max_key_;     // leaf.get_key(size - 1), if size > 0
    bool have_leaf_max_{false};
    std::vector<char> leaf_upper_key_; // first key of the next non-empty leaf
    bool have_leaf_upper_{false};

    Stats stats_;
};
