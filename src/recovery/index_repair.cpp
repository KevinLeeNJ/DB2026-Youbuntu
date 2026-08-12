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

#include "log_recovery.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <map>
#include <unordered_set>
#include <vector>

#include "index/ix_index_handle.h"
#include "minilog.h"

namespace {

// Caps how many copies of one (key, rid) pair the repair will remove. The probe
// result says how many there are, so this only bounds the work if that result is
// itself absurd. A key can legitimately hold only a handful of duplicates.
constexpr int kMaxDuplicateDrain = 16;

// Number of repaired keys re-probed afterwards. A repair that did not take
// effect means the tree is not in a state this repair can fix, and the index
// has to be rebuilt.
constexpr size_t kRepairSpotCheckLimit = 64;
void BuildIndexKey(const IndexMeta& index, const char* record_data, char* key_out) {
    int offset = 0;
    for (const auto& col : index.cols) {
        std::memcpy(key_out + offset, record_data + col.offset, col.len);
        offset += col.len;
    }
}

} // namespace

void RecoveryManager::collect_wal_index_keys() {
    // Every image the WAL mentions is a candidate stale entry: without index
    // page LSNs recovery cannot tell whether the matching index write reached
    // disk, so each one has to be reconciled against the tree.
    WalReader reader(disk_manager_, scan_begin_offset_, scan_end_offset_);
    WalRecordView record;
    WalDmlView dml;
    while (reader.next(&record)) {
        switch (record.log_type) {
        case LogType::INSERT:
        case LogType::DELETE:
        case LogType::UPDATE:
            break;
        default:
            continue;
        }
        if (!ParseWalDml(record, &dml)) {
            throw InternalError("recovery failed to re-parse the DML payload at WAL offset " +
                                std::to_string(record.offset) + " that analyze accepted; WAL retained");
        }
        // index_plans was resolved once per table; this loop runs once per WAL
        // record, and rebuilding the index name here used to cost a string
        // concatenation plus a map lookup per record per index.
        const RecoveryTable& table = tables_[intern_table(dml.table_name)];
        for (IndexRepairPlan* plan : table.index_plans) {
            for (const char* image : {dml.before_image, dml.after_image}) {
                if (image == nullptr) {
                    continue;
                }
                const auto key_slot = static_cast<uint32_t>(plan->key_arena.size() / plan->key_len);
                plan->key_arena.resize(plan->key_arena.size() + static_cast<size_t>(plan->key_len));
                BuildIndexKey(*plan->index_meta, image,
                              plan->key_arena.data() + static_cast<size_t>(key_slot) * plan->key_len);
                plan->entries.push_back(IndexRepairEntry{key_slot, dml.rid, false});
            }
        }
    }
}

void RecoveryManager::collect_heap_index_keys() {
    // Read the final tuple of every touched RID one page at a time. The RIDs
    // are already ordered by page, so this is a sequential sweep and the keys
    // of every index on the table come out of the same page pin.
    for (size_t begin = 0; begin < touched_sorted_.size();) {
        const uint16_t table_id = touched_sorted_[begin].table_id;
        size_t end = begin;
        while (end < touched_sorted_.size() && touched_sorted_[end].table_id == table_id) {
            ++end;
        }
        RecoveryTable& table = tables_[table_id];
        if (table.file_handle == nullptr || table.index_plans.empty()) {
            begin = end;
            continue;
        }
        const auto& table_plans = table.index_plans;

        const int num_pages = table.file_handle->get_file_hdr().num_pages;
        for (size_t i = begin; i < end;) {
            const int32_t page_no = touched_sorted_[i].page_no;
            size_t page_end = i;
            while (page_end < end && touched_sorted_[page_end].page_no == page_no) {
                ++page_end;
            }
            if (page_no < 0 || page_no >= num_pages) {
                i = page_end;
                continue;
            }

            RmPageHandle page_handle = table.file_handle->fetch_page_handle(page_no);
            {
                std::shared_lock<std::shared_mutex> page_lock(page_handle.page->latch());
                for (size_t j = i; j < page_end; ++j) {
                    const int slot_no = touched_sorted_[j].slot_no;
                    if (slot_no < 0 || slot_no >= page_handle.file_hdr->num_records_per_page) {
                        continue;
                    }
                    if (!Bitmap::is_set(page_handle.bitmap, slot_no) || page_handle.get_meta(slot_no).is_deleted_) {
                        continue;
                    }
                    const char* row = page_handle.get_slot(slot_no);
                    const Rid rid{page_no, slot_no};
                    for (IndexRepairPlan* plan : table_plans) {
                        const auto key_slot = static_cast<uint32_t>(plan->key_arena.size() / plan->key_len);
                        plan->key_arena.resize(plan->key_arena.size() + static_cast<size_t>(plan->key_len));
                        BuildIndexKey(*plan->index_meta, row,
                                      plan->key_arena.data() + static_cast<size_t>(key_slot) * plan->key_len);
                        plan->entries.push_back(IndexRepairEntry{key_slot, rid, true});
                    }
                }
            }
            buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            i = page_end;
        }
        begin = end;
    }
}

void RecoveryManager::plan_touched_indexes(std::map<std::string, IndexRepairPlan>* plans) {
    // One plan per index that a touched table owns and that is actually open.
    // Every entry of tables_ was interned from a DML record, so iterating it
    // visits each touched table exactly once; the previous version iterated
    // touched_sorted_ instead and rebuilt every index name once per touched RID.
    for (const auto& table : tables_) {
        if (table.meta == nullptr) {
            continue;
        }
        for (const auto& index_meta : table.meta->indexes) {
            auto index_name = sm_manager_->get_ix_manager()->get_index_name(table.name, index_meta.cols);
            auto handle_it = sm_manager_->ihs_.find(index_name);
            if (handle_it == sm_manager_->ihs_.end() || plans->count(index_name) != 0) {
                continue;
            }
            IndexRepairPlan plan;
            plan.index_name = index_name;
            plan.index_meta = &index_meta;
            plan.index = handle_it->second.get();
            plan.key_len = index_meta.col_tot_len;
            plans->emplace(std::move(index_name), std::move(plan));
        }
    }
}

void RecoveryManager::bind_index_plans(std::map<std::string, IndexRepairPlan>* plans) {
    // Once per table per index, so the two collectors below can walk pointers.
    // plans is not mutated after this, so the addresses stay valid; the plans are
    // unbound again before anything can invalidate them.
    for (auto& table : tables_) {
        table.index_plans.clear();
        if (table.meta == nullptr) {
            continue;
        }
        for (const auto& index_meta : table.meta->indexes) {
            auto plan_it = plans->find(sm_manager_->get_ix_manager()->get_index_name(table.name, index_meta.cols));
            if (plan_it != plans->end()) {
                table.index_plans.push_back(&plan_it->second);
            }
        }
    }
}

void RecoveryManager::collect_index_repair_keys(std::map<std::string, IndexRepairPlan>* plans) {
    if (plans->empty()) {
        return;
    }
    bind_index_plans(plans);
    collect_wal_index_keys();
    collect_heap_index_keys();
}

void RecoveryManager::sort_index_repair_entries(IndexRepairPlan* plan) {
    IxIndexHandle* index = plan->index;
    const int key_len = plan->key_len;
    const char* arena = plan->key_arena.data();
    const auto key_of = [arena, key_len](const IndexRepairEntry& entry) {
        return arena + static_cast<size_t>(entry.key_slot) * key_len;
    };

    // Group by key in B+tree order so the leaves are visited left to right and
    // the internal nodes stay hot, then let each group make one decision. Both
    // the structure gate and the repair below consume this order, and the gate
    // depends on it for its "the previous descent already covers this key" skip,
    // so it is established once, here, before either of them runs.
    std::sort(
        plan->entries.begin(), plan->entries.end(), [&](const IndexRepairEntry& left, const IndexRepairEntry& right) {
            const int cmp = ix_compare(key_of(left), key_of(right), index->get_col_types(), index->get_col_lens());
            if (cmp != 0) {
                return cmp < 0;
            }
            if (!(left.rid == right.rid)) {
                return left.rid.page_no != right.rid.page_no ? left.rid.page_no < right.rid.page_no
                                                             : left.rid.slot_no < right.rid.slot_no;
            }
            return static_cast<int>(left.from_heap) < static_cast<int>(right.from_heap);
        });
}

bool RecoveryManager::gate_index_change_set(IndexRepairPlan* plan, RecoveryIndexGate::Stats* totals) {
    IxIndexHandle* index = plan->index;
    const int key_len = plan->key_len;
    const char* arena = plan->key_arena.data();

    bool valid = true;
    bool gate_declined = false;
    {
        // Recovery is single threaded and runs before the listener opens, so the
        // latch is documentation rather than mutual exclusion - but the gate does
        // write to index pages, and taking the same latch every writer takes keeps
        // that from becoming an exception to the rule. It is released before
        // validate_structure() runs below: index_latch_ is a plain shared_mutex,
        // so re-entering it for a shared hold would deadlock.
        auto structure_guard = index->lock_exclusive();

        RecoveryIndexGate gate(disk_manager_, buffer_pool_manager_, index, plan->index_name);
        const char* previous_key = nullptr;
        for (const IndexRepairEntry& entry : plan->entries) {
            const char* key = arena + static_cast<size_t>(entry.key_slot) * key_len;
            // plan->entries holds one element per (key, rid, source); the gate
            // only cares about distinct keys, and they arrive grouped by sort
            // order.
            if (previous_key != nullptr &&
                ix_compare(previous_key, key, index->get_col_types(), index->get_col_lens()) == 0) {
                continue;
            }
            previous_key = key;
            if (!gate.check_key(key)) {
                valid = false;
                break;
            }
        }

        const RecoveryIndexGate::Stats& stats = gate.stats();
        totals->descents += stats.descents;
        totals->keys_covered += stats.keys_covered;
        totals->pages_validated += stats.pages_validated;
        totals->page_fetches += stats.page_fetches;
        totals->parent_pointers_repaired += stats.parent_pointers_repaired;
        totals->chain_bounds_unknown += stats.chain_bounds_unknown;
        index_parent_pointer_repair_count_ += stats.parent_pointers_repaired;
        // Unusable is not a verdict on the tree: the gate is saying it cannot
        // trust its own inputs (an unreadable or outdated page 0). Rebuilding on
        // that would turn a gate limitation into a table-sized recovery.
        gate_declined = gate.setup() == RecoveryIndexGate::Setup::Unusable;
    }
    if (gate_declined) {
        LOG_WARN("recovery index gate %s declined; falling back to whole-tree structure validation",
                 plan->index_name.c_str());
        return index->validate_structure();
    }
    return valid;
}

bool RecoveryManager::apply_index_repair_plan(IndexRepairPlan* plan) {
    IxIndexHandle* index = plan->index;
    const int key_len = plan->key_len;
    const char* arena = plan->key_arena.data();
    const auto key_of = [arena, key_len](const IndexRepairEntry& entry) {
        return arena + static_cast<size_t>(entry.key_slot) * key_len;
    };

    std::vector<Rid> existing;
    std::vector<Rid> required;   // must be present when the group is done
    std::vector<Rid> candidates; // WAL images that may be stale entries
    std::vector<const char*> spot_check_keys;
    std::vector<Rid> spot_check_rids;

    for (size_t begin = 0; begin < plan->entries.size();) {
        size_t end = begin + 1;
        const char* key = key_of(plan->entries[begin]);
        while (end < plan->entries.size() &&
               ix_compare(key_of(plan->entries[end]), key, index->get_col_types(), index->get_col_lens()) == 0) {
            ++end;
        }

        required.clear();
        candidates.clear();
        for (size_t i = begin; i < end; ++i) {
            auto& target = plan->entries[i].from_heap ? required : candidates;
            if (target.empty() || !(target.back() == plan->entries[i].rid)) {
                target.push_back(plan->entries[i].rid);
            }
        }

        existing.clear();
        ++index_probe_count_;
        index->get_value(key, &existing, nullptr);

        // `existing` is a multiset, not a set: lookup_equal() pushes back every
        // matching slot, insert_entry(..., allow_duplicate=true) will store the
        // same (key, rid) twice, and delete_entry() removes one copy per call.
        // Treating it as a set is what let E = {r, r}, C = {r}, R = {r} pass the
        // skip test below and leave a duplicated entry in the tree forever --
        // which makes an index scan return the same heap row twice and inflates
        // the per-partition row counts `final.md:345` cross-checks.
        const auto contains = [](const std::vector<Rid>& haystack, const Rid& needle) {
            return std::find(haystack.begin(), haystack.end(), needle) != haystack.end();
        };
        const auto multiplicity = [](const std::vector<Rid>& haystack, const Rid& needle) {
            return static_cast<int>(std::count(haystack.begin(), haystack.end(), needle));
        };
        for (size_t i = 0; i < existing.size(); ++i) {
            // Count each duplicated RID once, at its first occurrence. This is
            // the only measurement of how often a real crash leaves a duplicate;
            // the skip predicate used to hide them from every counter.
            if (multiplicity(existing, existing[i]) > 1 &&
                std::find(existing.begin(), existing.begin() + static_cast<std::ptrdiff_t>(i), existing[i]) ==
                    existing.begin() + static_cast<std::ptrdiff_t>(i)) {
                ++index_duplicate_entry_count_;
            }
        }

        // Draining every WAL image and then reinstalling the live keys leaves
        // this key holding exactly `required`, once each. When the tree already
        // holds exactly that, the sequence is a no-op and the traversals, page
        // dirtying and node merges it would cause are all pure waste.
        bool already_correct = true;
        for (const Rid& rid : required) {
            if (multiplicity(existing, rid) != 1) {
                already_correct = false;
                break;
            }
        }
        if (already_correct) {
            for (const Rid& rid : existing) {
                if (contains(candidates, rid) && !contains(required, rid)) {
                    already_correct = false;
                    break;
                }
            }
        }
        if (already_correct) {
            ++index_unchanged_key_count_;
            begin = end;
            continue;
        }

        // An interrupted index write can leave the same pair more than once, so
        // every removal drains rather than deleting a single copy.
        const auto drain = [&](const Rid& rid, int keep) {
            const int surplus = std::min(multiplicity(existing, rid) - keep, kMaxDuplicateDrain);
            for (int removed = 0; removed < surplus; ++removed) {
                if (!index->delete_entry(key, rid, IndexWriteWalContext::RecoveryDurable())) {
                    break;
                }
                ++index_mutation_count_;
            }
        };
        for (const Rid& rid : candidates) {
            drain(rid, 0);
        }
        for (const Rid& rid : required) {
            if (contains(candidates, rid)) {
                // Drained to zero above, so exactly one copy has to go back.
                index->insert_entry(key, rid, IndexWriteWalContext::RecoveryDurable(), true);
                ++index_mutation_count_;
            } else if (multiplicity(existing, rid) == 0) {
                index->insert_entry(key, rid, IndexWriteWalContext::RecoveryDurable(), true);
                ++index_mutation_count_;
            } else {
                // Present and never deleted; only the surplus copies have to go.
                drain(rid, 1);
                continue;
            }
            if (spot_check_keys.size() < kRepairSpotCheckLimit) {
                spot_check_keys.push_back(key);
                spot_check_rids.push_back(rid);
            }
        }
        begin = end;
    }

    // A repair that did not take is evidence the tree cannot be fixed in place.
    for (size_t i = 0; i < spot_check_keys.size(); ++i) {
        existing.clear();
        index->get_value(spot_check_keys[i], &existing, nullptr);
        if (std::find(existing.begin(), existing.end(), spot_check_rids[i]) == existing.end()) {
            LOG_WARN("recovery index %s did not accept a repaired key", plan->index_name.c_str());
            return false;
        }
    }
    return true;
}

void RecoveryManager::repair_touched_indexes() {
    std::map<std::string, IndexRepairPlan> plans;
    // Only names and key widths at this point. Collecting the keys costs a WAL
    // pass and a heap sweep, so it waits until the spine check below has dropped
    // the indexes that cannot be repaired in place at all.
    plan_touched_indexes(&plans);
    if (plans.empty()) {
        return;
    }
    const size_t total_indexes = plans.size();

    std::unordered_set<std::string> indexes_to_rebuild;

    // Stage 1: the spine. last_leaf_/first_leaf_ are append hints that reach disk
    // only when a checkpoint publishes the header, so after a crash they
    // routinely name leaves that have since been split - which alone makes the
    // leaf chain look broken and makes delete_entry stop scanning early.
    // refresh_leaf_chain_endpoint() recomputes both by descending the left and
    // right edges, one page read per level, and fails only when even that spine
    // is unusable.
    const auto spine_begin = std::chrono::steady_clock::now();
    size_t repaired_endpoints = 0;
    for (const auto& [index_name, plan] : plans) {
        if (!plan.index->refresh_leaf_chain_endpoint()) {
            LOG_ERROR("recovery could not follow the leaf chain of index %s", index_name.c_str());
            indexes_to_rebuild.insert(index_name);
            continue;
        }
        ++repaired_endpoints;
    }
    const auto spine_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - spine_begin).count();

    // Drop the plans for indexes that are going to be rebuilt anyway, then pay
    // for the key collection only for the rest.
    for (auto it = plans.begin(); it != plans.end();) {
        it = indexes_to_rebuild.count(it->first) != 0 ? plans.erase(it) : std::next(it);
    }
    collect_index_repair_keys(&plans);

    // Stage 2: the structure gate proper, over the change set rather than over
    // the tree. See src/recovery/index_structure_gate.h for why the distinct
    // repair keys are a sufficient cover for everything an interrupted SMO can
    // damage. It runs before any mutation, so a tree the repair cannot fix key
    // by key never gets written to.
    const auto gate_begin = std::chrono::steady_clock::now();
    RecoveryIndexGate::Stats gate_totals;
    for (auto& [index_name, plan] : plans) {
        sort_index_repair_entries(&plan);
        try {
            if (!gate_index_change_set(&plan, &gate_totals)) {
                LOG_ERROR("recovery found structurally invalid index %s", index_name.c_str());
                indexes_to_rebuild.insert(index_name);
            }
        } catch (const std::exception& error) {
            LOG_ERROR("recovery could not validate the structure of index %s: %s", index_name.c_str(), error.what());
            indexes_to_rebuild.insert(index_name);
        }
    }
    LOG_INFO("recovery index structure gate: %zu indexes, spine: %zu leaf endpoints refreshed, %lld ms; "
             "change set: %llu descents, "
             "%llu keys covered, %llu pages validated, %llu page fetches, "
             "%llu parent pointers repaired, %llu leaves with an empty successor, %zu to rebuild, %lld ms",
             total_indexes, repaired_endpoints, static_cast<long long>(spine_ms),
             static_cast<unsigned long long>(gate_totals.descents),
             static_cast<unsigned long long>(gate_totals.keys_covered),
             static_cast<unsigned long long>(gate_totals.pages_validated),
             static_cast<unsigned long long>(gate_totals.page_fetches),
             static_cast<unsigned long long>(gate_totals.parent_pointers_repaired),
             static_cast<unsigned long long>(gate_totals.chain_bounds_unknown), indexes_to_rebuild.size(),
             static_cast<long long>(
                 std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - gate_begin)
                     .count()));

    for (auto it = plans.begin(); it != plans.end();) {
        it = indexes_to_rebuild.count(it->first) != 0 ? plans.erase(it) : std::next(it);
    }

    for (auto& [index_name, plan] : plans) {
        try {
            if (!apply_index_repair_plan(&plan)) {
                indexes_to_rebuild.insert(index_name);
            }
        } catch (const std::exception& error) {
            LOG_ERROR("recovery found structurally inconsistent index %s: %s", index_name.c_str(), error.what());
            indexes_to_rebuild.insert(index_name);
        }
    }

    LOG_INFO("recovery index repair: %llu probes, %llu mutations, %llu keys already correct, %llu duplicated entries",
             static_cast<unsigned long long>(index_probe_count_),
             static_cast<unsigned long long>(index_mutation_count_),
             static_cast<unsigned long long>(index_unchanged_key_count_),
             static_cast<unsigned long long>(index_duplicate_entry_count_));

    // index_plans points into `plans`, which dies with this function.
    for (auto& table : tables_) {
        table.index_plans.clear();
    }
    rebuild_indexes(indexes_to_rebuild);
}

void RecoveryManager::rebuild_indexes(const std::unordered_set<std::string>& index_names) {
    if (index_names.empty()) {
        return;
    }
    // Logged at ERROR, not INFO. A rebuild reads the whole heap once per index,
    // so recovery time stops being proportional to the WAL and starts being
    // proportional to the table -- at ten million rows no rebuild fits the 90 s
    // readiness budget at all. It is a correctness backstop, not a normal
    // outcome, and the default log level is WARN, so anything quieter than this
    // would be invisible outside the recovery window.
    LOG_ERROR("recovery must rebuild %zu index(es) from the heap; this is NOT the normal recovery path and makes "
              "recovery time proportional to the table rather than to the WAL",
              index_names.size());
    for (const auto& index_name : index_names) {
        const auto begin = std::chrono::steady_clock::now();
        sm_manager_->rebuild_indexes({index_name});
        ++index_rebuild_count_;
        LOG_ERROR("recovery rebuilt index %s in %lld ms", index_name.c_str(),
                  static_cast<long long>(
                      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                          .count()));
    }
}
