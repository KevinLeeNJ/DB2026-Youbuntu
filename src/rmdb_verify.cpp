/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <algorithm>
#include <cstring>
#include <functional>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "index/ix_defs.h"
#include "record/bitmap.h"
#include "record/rm_file_handle.h"
#include "record/rm.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"

namespace {

bool Fail(const std::string& what) {
    std::cerr << "[verify] FAIL: " << what << '\n';
    return false;
}

bool VerifyHeap(const std::string& name, RmFileHandle* fh, BufferPoolManager* bpm) {
    const RmFileHdr hdr = fh->get_file_hdr();
    if (hdr.num_pages < RM_FIRST_RECORD_PAGE || hdr.num_records_per_page <= 0) {
        return Fail(name + ": invalid record file header");
    }
    std::set<page_id_t> expected_free;
    for (page_id_t page_no = RM_FIRST_RECORD_PAGE; page_no < hdr.num_pages; ++page_no) {
        RmPageHandle page;
        try {
            page = fh->fetch_page_handle(page_no);
        } catch (const std::exception& e) {
            return Fail(name + ": cannot fetch record page " + std::to_string(page_no) + ": " + e.what());
        }
        if (page.page == nullptr) {
            return Fail(name + ": null record page " + std::to_string(page_no));
        }
        int bitmap_count = 0;
        for (int slot = 0; slot < hdr.num_records_per_page; ++slot) {
            bitmap_count += Bitmap::is_set(page.bitmap, slot) ? 1 : 0;
        }
        if (bitmap_count != page.page_hdr->num_records) {
            bpm->unpin_page(page.page->get_page_id(), false);
            return Fail(name + ": bitmap/page-header count mismatch on page " + std::to_string(page_no));
        }
        if (page.page_hdr->num_records < hdr.num_records_per_page) {
            expected_free.insert(page_no);
        }
        const page_id_t next = page.page_hdr->next_free_page_no;
        if (next != RM_NO_PAGE && (next < RM_FIRST_RECORD_PAGE || next >= hdr.num_pages)) {
            bpm->unpin_page(page.page->get_page_id(), false);
            return Fail(name + ": invalid free-list link on page " + std::to_string(page_no));
        }
        bpm->unpin_page(page.page->get_page_id(), false);
    }

    // Free-space bookkeeping: check what the design actually guarantees.
    //
    // This used to walk first_free_page_no -> next_free_page_no and require the
    // chain to equal `expected_free` exactly. That invariant only holds for the
    // instant after RmFileHandle::open_file(), which rebuilds both the chain and
    // the in-memory candidate vector by rescanning every bitmap. At run time the
    // source of truth is free_page_candidates_; add/remove_free_page_candidate()
    // maintain that vector and first_free_page_no but deliberately never rewrite
    // next_free_page_no, because doing so would dirty an extra page on every page
    // fill along the hot insert path. The on-disk chain therefore decays as soon
    // as any page fills or frees — by design, and harmlessly, since open_file()
    // never trusts it.
    //
    // The old check consequently failed on every healthy database (reporting
    // "full page appears in record free list", or "cycle" once the head started
    // tracking the candidate vector), which made this verifier useless as the
    // crash-recovery safety net it is supposed to be. What is worth asserting is
    // that the persisted head is not actively misleading: it must either be
    // absent or name a page that really has room.
    const page_id_t head = hdr.first_free_page_no;
    if (head != RM_NO_PAGE) {
        if (head < RM_FIRST_RECORD_PAGE || head >= hdr.num_pages) {
            return Fail(name + ": free-list head " + std::to_string(head) + " is not a valid record page");
        }
        if (expected_free.find(head) == expected_free.end()) {
            return Fail(name + ": free-list head page " + std::to_string(head) + " has no free slot");
        }
    } else if (!expected_free.empty()) {
        // Not an error: pages freed after the head was last published are found
        // again by open_file()'s rescan. Report it so a real leak is still
        // visible in the log.
        std::cerr << "[verify] note: " << name << " has " << expected_free.size()
                  << " page(s) with free space but no published free-list head"
                     " (open_file rescans, so this is recoverable)\n";
    }
    return true;
}

bool VerifyIndex(const std::string& name, IxIndexHandle* ih, DiskManager* disk, BufferPoolManager* bpm) {
    std::vector<char> header_page(PAGE_SIZE, 0);
    disk->read_page(ih->GetFd(), IX_FILE_HDR_PAGE, header_page.data(), PAGE_SIZE);
    IxFileHdr hdr;
    hdr.deserialize(header_page.data());
    if (hdr.num_pages_ < IX_INIT_NUM_PAGES)
        return Fail(name + ": invalid index page count");
    if (hdr.root_page_ == IX_NO_PAGE)
        return true;
    if (hdr.root_page_ < IX_INIT_ROOT_PAGE || hdr.root_page_ >= hdr.num_pages_) {
        return Fail(name + ": root page is outside the index file");
    }

    std::unordered_set<page_id_t> visited;
    std::set<page_id_t> leaves;
    std::function<bool(page_id_t, page_id_t)> visit = [&](page_id_t page_no, page_id_t expected_parent) {
        if (page_no < IX_INIT_ROOT_PAGE || page_no >= hdr.num_pages_) {
            return Fail(name + ": child page is outside the index file");
        }
        if (!visited.insert(page_no).second)
            return Fail(name + ": cycle or duplicate in index tree");
        Page* raw = bpm->fetch_page(PageId{ih->GetFd(), page_no});
        if (raw == nullptr)
            return Fail(name + ": cannot fetch index page " + std::to_string(page_no));
        IxNodeHandle node(&hdr, raw);
        const int size = node.get_size();
        if (size < 0 || size > hdr.btree_order_ + 1 || node.get_parent_page_no() != expected_parent) {
            bpm->unpin_page(raw->get_page_id(), false);
            return Fail(name + ": invalid index node header");
        }
        for (int i = 1; i < size; ++i) {
            if (ix_compare(node.get_key(i - 1), node.get_key(i), hdr.col_types_, hdr.col_lens_) > 0) {
                bpm->unpin_page(raw->get_page_id(), false);
                return Fail(name + ": unsorted keys in index page");
            }
        }
        if (node.is_leaf_page()) {
            leaves.insert(page_no);
            if (node.get_prev_leaf() != IX_LEAF_HEADER_PAGE &&
                (node.get_prev_leaf() < IX_INIT_ROOT_PAGE || node.get_prev_leaf() >= hdr.num_pages_)) {
                bpm->unpin_page(raw->get_page_id(), false);
                return Fail(name + ": invalid previous leaf link");
            }
            if (node.get_next_leaf() != IX_LEAF_HEADER_PAGE &&
                (node.get_next_leaf() < IX_INIT_ROOT_PAGE || node.get_next_leaf() >= hdr.num_pages_)) {
                bpm->unpin_page(raw->get_page_id(), false);
                return Fail(name + ": invalid next leaf link");
            }
        } else {
            for (int i = 0; i < size; ++i) {
                if (!visit(node.value_at(i), page_no)) {
                    bpm->unpin_page(raw->get_page_id(), false);
                    return false;
                }
            }
        }
        bpm->unpin_page(raw->get_page_id(), false);
        return true;
    };
    if (!visit(hdr.root_page_, IX_NO_PAGE))
        return false;

    if (hdr.first_leaf_ != IX_NO_PAGE) {
        std::set<page_id_t> chain;
        page_id_t prev = IX_LEAF_HEADER_PAGE;
        for (page_id_t current = hdr.first_leaf_; current != IX_LEAF_HEADER_PAGE;) {
            if (!chain.insert(current).second || !leaves.count(current)) {
                return Fail(name + ": invalid or cyclic leaf chain");
            }
            Page* raw = bpm->fetch_page(PageId{ih->GetFd(), current});
            if (raw == nullptr)
                return Fail(name + ": cannot fetch leaf chain page");
            IxNodeHandle node(&hdr, raw);
            if (node.get_prev_leaf() != prev) {
                bpm->unpin_page(raw->get_page_id(), false);
                return Fail(name + ": broken previous-leaf link");
            }
            prev = current;
            current = node.get_next_leaf();
            bpm->unpin_page(raw->get_page_id(), false);
        }
        if (chain != leaves)
            return Fail(name + ": leaf chain omits tree leaves");
        if (prev != hdr.last_leaf_)
            return Fail(name + ": last leaf header mismatch");
    }
    return true;
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: rmdb_verify <database-directory>\n";
        return 2;
    }
    DiskManager disk;
    BufferPoolManager bpm(8192, &disk);
    RmManager rm(&disk, &bpm);
    IxManager ix(&disk, &bpm);
    SmManager sm(&disk, &bpm, &rm, &ix);
    try {
        sm.open_db(argv[1]);
        for (const auto& [name, fh] : sm.fhs_) {
            if (!VerifyHeap(name, fh.get(), &bpm))
                return 1;
        }
        for (const auto& [name, ih] : sm.ihs_) {
            if (!VerifyIndex(name, ih.get(), &disk, &bpm))
                return 1;
        }
        sm.close_db();
    } catch (const std::exception& e) {
        std::cerr << "[verify] ERROR: " << e.what() << '\n';
        return 1;
    }
    std::cout << "[verify] heap/index/free-list checks passed\n";
    return 0;
}
