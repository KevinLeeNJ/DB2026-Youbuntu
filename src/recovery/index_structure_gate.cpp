/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "index_structure_gate.h"

#include "minilog.h"

#include <cstring>
#include <utility>
#include <vector>

RecoveryIndexGate::RecoveryIndexGate(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager,
                                     IxIndexHandle* index, std::string index_name)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), index_(index),
      index_name_(std::move(index_name)), fd_(index->GetFd()) {
    // Read the index header straight from page 0, exactly like rmdb_verify's
    // VerifyIndex(). The handle's file_hdr_ is private and the gate lives
    // outside src/index/ on purpose, but the two are the same bytes here: page 0
    // is what IxManager::open_index() parsed into file_hdr_, and nothing between
    // that call and this one performs an SMO (recovery's redo and undo touch
    // heap pages only). refresh_leaf_chain_endpoint() does update the in-memory
    // first_leaf_/last_leaf_ - which is why this gate never reads those two
    // fields and recomputes every bound from the tree itself.
    std::vector<char> header_page(PAGE_SIZE, 0);
    try {
        disk_manager_->read_page(fd_, IX_FILE_HDR_PAGE, header_page.data(), PAGE_SIZE);
        hdr_.deserialize(header_page.data());
    } catch (const std::exception& error) {
        LOG_WARN("recovery index gate %s cannot read the index header: %s", index_name_.c_str(), error.what());
        setup_ = Setup::Unusable;
        return;
    }

    // Cross-check the geometry against the live handle. If page 0 were ever
    // stale or misparsed, every later key comparison and every rids[] offset
    // would be computed from the wrong layout, and the gate would "repair"
    // parent pointers to garbage. Declining to run is the only safe answer;
    // the caller then falls back to validate_structure().
    const bool geometry_agrees =
        hdr_.col_tot_len_ == index_->get_col_tot_len() && hdr_.col_types_ == index_->get_col_types() &&
        hdr_.col_lens_ == index_->get_col_lens() && hdr_.btree_order_ > 0 && hdr_.keys_size_ >= hdr_.col_tot_len_;
    if (!geometry_agrees) {
        LOG_WARN("recovery index gate %s: the header on page 0 disagrees with the open index handle",
                 index_name_.c_str());
        setup_ = Setup::Unusable;
        return;
    }
    if (hdr_.num_pages_ < IX_INIT_NUM_PAGES) {
        LOG_WARN("recovery index gate %s: page count %d is below the initial layout", index_name_.c_str(),
                 hdr_.num_pages_);
        setup_ = Setup::Unusable;
        return;
    }
    if (hdr_.root_page_ == IX_NO_PAGE) {
        setup_ = Setup::EmptyTree;
        return;
    }
    if (!page_in_range(hdr_.root_page_)) {
        LOG_WARN("recovery index gate %s: root page %d is outside the index file", index_name_.c_str(),
                 hdr_.root_page_);
        setup_ = Setup::Unusable;
        return;
    }

    leaf_descent_key_.resize(static_cast<size_t>(hdr_.col_tot_len_));
    leaf_max_key_.resize(static_cast<size_t>(hdr_.col_tot_len_));
    leaf_upper_key_.resize(static_cast<size_t>(hdr_.col_tot_len_));
    setup_ = Setup::Ready;
}

bool RecoveryIndexGate::reject(const char* invariant, page_id_t page_no) {
    // Every rejection names the invariant, because the only consequence of a
    // rejection is a full index rebuild - a decision nobody can review after the
    // fact from a bare "structurally invalid" line.
    LOG_ERROR("recovery index gate %s rejected page %d: %s", index_name_.c_str(), static_cast<int>(page_no), invariant);
    return false;
}

bool RecoveryIndexGate::decline(const char* reason) {
    // Distinct from reject(): reject() means "this tree is broken, rebuild it",
    // decline() means "the gate cannot trust its own inputs". Conflating the two
    // would turn a gate bug into an index rebuild.
    LOG_WARN("recovery index gate %s declines to judge index %s: %s", index_name_.c_str(), index_name_.c_str(), reason);
    setup_ = Setup::Unusable;
    return false;
}

Page* RecoveryIndexGate::fetch(page_id_t page_no) {
    ++stats_.page_fetches;
    return buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
}

void RecoveryIndexGate::release(Page* page, bool dirty) {
    // A repaired parent pointer only needs to be marked dirty here:
    // SmManager::flush_recovery_pages() flushes and fsyncs every index of every
    // touched table before RecoveryManager::reset_wal_if_needed() truncates the
    // WAL, so the repair is durable before the evidence for it is discarded. And
    // it is idempotent anyway - a crash in between simply has the next recovery
    // detect and redo the same five-byte fix.
    buffer_pool_manager_->unpin_page(page->get_page_id(), dirty);
}

bool RecoveryIndexGate::page_in_range(page_id_t page_no) const {
    return page_no >= IX_INIT_ROOT_PAGE && page_no < hdr_.num_pages_;
}

int RecoveryIndexGate::compare(const char* left, const char* right) const {
    return ix_compare(left, right, hdr_.col_types_, hdr_.col_lens_);
}

void RecoveryIndexGate::remember_bound(std::vector<char>* slot, bool* have, const char* key) {
    memcpy(slot->data(), key, static_cast<size_t>(hdr_.col_tot_len_));
    *have = true;
}

bool RecoveryIndexGate::check_key(const char* key) {
    switch (setup_) {
    case Setup::EmptyTree:
        return true;
    case Setup::Unusable:
        return false;
    case Setup::Ready:
        break;
    }
    if (covered_by_current_leaf(key)) {
        ++stats_.keys_covered;
        return true;
    }
    return descend(key);
}

/**
 * Can this key be answered by the leaf the previous descent already validated?
 *
 * Sound because the guard is anchored on `leaf_descent_key_`, the key that
 * actually reached the current leaf: for any key in [leaf_descent_key_, upper)
 * the descent would follow the identical path, because the descent is monotone
 * in the key and `upper` is the first key of the leaf's successor, i.e. the
 * separator that would send the descent one leaf further right. Anchoring on the
 * descent key rather than assuming sorted input is what keeps this correct if a
 * caller ever feeds keys out of order - it then costs extra descents instead of
 * silently skipping leaves.
 */
bool RecoveryIndexGate::covered_by_current_leaf(const char* key) const {
    if (!have_leaf_ || compare(key, leaf_descent_key_.data()) < 0) {
        return false;
    }
    if (have_leaf_upper_ && compare(key, leaf_upper_key_.data()) < 0) {
        return true;
    }
    // Fallback for the case where the upper bound is unknown because the
    // successor leaf is empty: a key inside the leaf's own key range is
    // certainly in this leaf. Strictly weaker than the bound above, and only
    // ever reached when that one is unavailable.
    return have_leaf_max_ && compare(key, leaf_max_key_.data()) <= 0;
}

bool RecoveryIndexGate::descend(const char* key) {
    ++stats_.descents;
    have_leaf_ = false;
    have_leaf_max_ = false;
    have_leaf_upper_ = false;

    page_id_t page_no = hdr_.root_page_;
    page_id_t expected_parent = IX_NO_PAGE;
    for (int level = 0; level <= kMaxDescentLevels; ++level) {
        Page* raw = fetch(page_no);
        if (raw == nullptr) {
            return reject("page could not be fetched", page_no);
        }
        IxNodeHandle node(&hdr_, raw);
        bool dirty = false;
        if (!validate_node(node, page_no, expected_parent, &dirty)) {
            release(raw, dirty);
            return false;
        }
        if (node.is_leaf_page()) {
            const bool valid = validate_leaf_and_remember(node, page_no, key);
            release(raw, dirty);
            return valid;
        }
        // Deliberately IxNodeHandle::internal_lookup(): the gate must validate
        // the very path a lookup takes, and that is the routine
        // find_leaf_page()/lookup_equal()/get_value() and every insert use.
        // Re-deriving the child index here would let the two drift apart.
        const page_id_t child = node.internal_lookup(key);
        release(raw, dirty);
        if (!page_in_range(child)) {
            return reject("INV-0 child pointer leaves the index file", child);
        }
        expected_parent = page_no;
        page_no = child;
    }
    return reject("INV-0 descent exceeded the maximum tree height (cycle in the child pointers)", page_no);
}

bool RecoveryIndexGate::validate_node(IxNodeHandle& node, page_id_t page_no, page_id_t expected_parent, bool* dirty) {
    auto seen = validated_parent_.find(page_no);
    if (seen != validated_parent_.end()) {
        // INV-4 single parent. A page reached from two different parents means
        // one of the two subtrees is a stale copy, so a key can be found or
        // lost depending on which way the descent went, and the same physical
        // page would be split under two different separators. Not repairable
        // in place: which parent is the real one is exactly the information the
        // crash destroyed. This is validate_structure()'s "page reachable more
        // than once", localized to the change set.
        if (seen->second != expected_parent) {
            return reject("INV-4 page is reachable from two different parents", page_no);
        }
        return true;
    }

    ++stats_.pages_validated;

    // INV-1 node occupancy. An *empty leaf* is a legal steady state, not damage:
    // delete_entry() only erases the key and coalesce_or_redistribute() is
    // unreachable from the delete path, so a leaf whose keys have all been
    // removed stays in the tree and in the leaf chain. TPC-C reaches this on
    // every index over new_orders, whose leftmost leaf is emptied by Delivery
    // taking MIN(no_o_id). Descent, lookup_equal() and the scans all step past a
    // zero-key leaf. An internal node with no children, in contrast, cannot be
    // descended through at all. Neither case is repairable here.
    const int size = node.get_size();
    const int min_size = node.is_leaf_page() ? 0 : 1;
    if (size < min_size || size > hdr_.btree_order_ + 1) {
        return reject("INV-1 key count out of range for this node kind", page_no);
    }

    // INV-2 in-page key order. Every search inside a node is a binary search, so
    // a page whose keys are not ascending answers lookups wrongly without ever
    // looking corrupt. Not repairable: sorting the page would invent a
    // key-to-rid mapping we have no evidence for.
    for (int i = 1; i < size; ++i) {
        if (compare(node.get_key(i - 1), node.get_key(i)) > 0) {
            return reject("INV-2 keys are not ascending inside the page", page_no);
        }
    }

    // INV-3a the header's root really is the tree's root. Everything below rests
    // on hdr_.root_page_, which was read from page 0 of the file, so this is the
    // one place where a *stale header* rather than a broken tree has to be ruled
    // out - and it has to be ruled out before INV-3 repairs anything, because a
    // root split whose page-0 write was lost leaves the old root pointing up at
    // the new one, and "repairing" that back to IX_NO_PAGE would cut the real
    // root off the tree. Today the situation is unreachable (recovery's redo and
    // undo touch heap pages only, so no SMO runs between IxManager::open_index()
    // and this gate), but the whole design would otherwise depend on that call
    // order staying true, which is not a property anyone reading this function
    // can check. Declining makes the caller fall back to the whole-tree checker,
    // which derives the root from the handle's own state.
    if (expected_parent == IX_NO_PAGE && node.get_parent_page_no() != IX_NO_PAGE) {
        return decline("the page the header calls the root has a parent, so page 0 is behind the tree");
    }

    // INV-3 parent back pointer -- REPAIRABLE IN PLACE, and the whole point of
    // A-1. Reaching this page by descending from `expected_parent` *proves* that
    // the parent names it as a child, so the correct value of the back pointer is
    // known with certainty and no key is missing: the pointer is only read
    // upwards, by maintain_parent() and insert_into_parent(). A stale value makes
    // a later separator update walk the wrong spine; it does not hide any data
    // today. This is the one invariant a real SIGKILL was measured to break, and
    // before A-1 it cost a full rebuild of a multi-GB index.
    if (node.get_parent_page_no() != expected_parent) {
        LOG_WARN("recovery index gate %s repaired the parent back pointer of page %d: %d -> %d", index_name_.c_str(),
                 static_cast<int>(page_no), static_cast<int>(node.get_parent_page_no()),
                 static_cast<int>(expected_parent));
        node.set_parent_page_no(expected_parent);
        *dirty = true;
        ++stats_.parent_pointers_repaired;
    }

    validated_parent_.emplace(page_no, expected_parent);
    return true;
}

bool RecoveryIndexGate::validate_leaf_and_remember(IxNodeHandle& leaf, page_id_t page_no, const char* key) {
    const int size = leaf.get_size();
    const page_id_t previous = leaf.get_prev_leaf();
    const page_id_t next = leaf.get_next_leaf();

    // INV-5 leaf links stay inside the file. IX_LEAF_HEADER_PAGE is the sentinel
    // for both ends of the chain. An out-of-range link makes the sequential scans
    // and delete_entry()'s forward walk parse an arbitrary page as a node.
    const bool previous_valid = previous == IX_LEAF_HEADER_PAGE || page_in_range(previous);
    const bool next_valid = next == IX_LEAF_HEADER_PAGE || page_in_range(next);
    if (!previous_valid || !next_valid) {
        return reject("INV-5 leaf chain link leaves the index file", page_no);
    }

    remember_bound(&leaf_descent_key_, &have_leaf_, key);
    if (size > 0) {
        remember_bound(&leaf_max_key_, &have_leaf_max_, leaf.get_key(size - 1));
    }
    if (next == IX_LEAF_HEADER_PAGE) {
        return true;
    }

    // Walk forward to the first non-empty successor to obtain the upper bound.
    // Empty leaves carry no usable get_key(0) (erase_pair leaves the bytes
    // behind and delete_entry_unlocked deliberately skips maintain_parent for
    // them), so their separator says nothing about where the key belongs. The hop
    // count is capped so a long run of empty leaves - which new_orders grows at
    // its left edge - cannot turn one descent into a chain walk.
    page_id_t cursor = next;
    page_id_t behind = page_no;
    for (int hop = 0; hop < kMaxEmptyLeafHops; ++hop) {
        Page* raw = fetch(cursor);
        if (raw == nullptr) {
            return reject("INV-5 leaf chain successor could not be fetched", cursor);
        }
        IxNodeHandle successor(&hdr_, raw);

        // INV-6 the chain is a doubly linked list of leaves. `next` pointing at
        // an internal node, or a successor whose prev_leaf names someone else,
        // means the chain was rewritten by a split whose other half never
        // reached disk. delete_entry()'s forward walk and every range scan
        // depend on this. Repairing it would mean choosing which of the two
        // halves of a torn split to believe, so it goes to the rebuild.
        if (!successor.is_leaf_page()) {
            release(raw, false);
            return reject("INV-6 leaf chain successor is not a leaf", cursor);
        }
        if (successor.get_prev_leaf() != behind) {
            release(raw, false);
            return reject("INV-6 leaf chain successor does not link back", cursor);
        }

        const int successor_size = successor.get_size();
        const page_id_t successor_next = successor.get_next_leaf();
        if (successor_size > 0) {
            // INV-7 the tree and the leaf chain agree on where this key lives.
            // The descent landed in `page_no`, so every separator on the path
            // says the key belongs at or before it; the chain says the first key
            // of this successor is <= our key, i.e. the key belongs further
            // right. Exactly one thing produces that: a leaf that a split
            // created and linked into the chain while the separator that would
            // make it reachable never reached its parent (the orphan leaf), or
            // the mirror image, a separator that was updated without its leaf.
            // Either way the keys in between are unreachable by descent - they
            // are lost to every lookup - so this is the one gate failure that
            // actually loses data. Repairing it means re-inserting the
            // successor's first key as a separator in its left neighbour's
            // parent; that is A-1's second half and is deliberately not
            // implemented, because after 64bd297 (SMO whole-dirty-set write-out)
            // it was not observed once in 10 real SIGKILL recoveries. The
            // fallback is the full rebuild, which is correct but slow.
            const bool agrees = compare(key, successor.get_key(0)) < 0;
            if (agrees) {
                remember_bound(&leaf_upper_key_, &have_leaf_upper_, successor.get_key(0));
            }
            release(raw, false);
            return agrees ? true
                          : reject("INV-7 the leaf chain places this key right of where the descent landed "
                                   "(orphan leaf or stale separator)",
                                   cursor);
        }

        release(raw, false);
        if (successor_next == IX_LEAF_HEADER_PAGE) {
            return true;
        }
        if (!page_in_range(successor_next)) {
            return reject("INV-5 leaf chain link leaves the index file", cursor);
        }
        behind = cursor;
        cursor = successor_next;
    }
    // Gave up looking for a non-empty successor. The gate keeps no upper bound,
    // so the next key simply pays for its own descent; nothing is asserted and
    // nothing is skipped.
    ++stats_.chain_bounds_unknown;
    return true;
}
