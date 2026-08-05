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

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "ix_defs.h"

// Make unused slots in an INDEX_SMO WAL copy deterministic without changing
// the on-disk page format. The complete page header, live keys/RIDs and bytes
// outside the known node layout remain byte-for-byte intact. A malformed or
// unfamiliar layout is left untouched so WAL capture retains the raw image.
inline bool TryCanonicalizeIxPageImageForWal(const IxFileHdr& file_hdr, page_id_t page_no,
                                             std::array<char, PAGE_SIZE>* image) {
    if (image == nullptr || page_no < IX_LEAF_HEADER_PAGE || file_hdr.col_tot_len_ <= 0 || file_hdr.btree_order_ <= 0 ||
        file_hdr.keys_size_ < 0) {
        return false;
    }

    constexpr uint64_t header_bytes = sizeof(IxPageHdr);
    constexpr uint64_t rid_alignment = alignof(Rid);
    const uint64_t slot_capacity = static_cast<uint64_t>(file_hdr.btree_order_) + 1;
    const uint64_t raw_key_end = header_bytes + slot_capacity * static_cast<uint64_t>(file_hdr.col_tot_len_);
    if (raw_key_end > PAGE_SIZE) {
        return false;
    }
    const uint64_t rid_begin = ((raw_key_end + rid_alignment - 1) / rid_alignment) * rid_alignment;
    const uint64_t expected_keys_bytes = rid_begin - header_bytes;
    const uint64_t rid_capacity_end = rid_begin + slot_capacity * sizeof(Rid);
    if (expected_keys_bytes != static_cast<uint64_t>(file_hdr.keys_size_) || rid_capacity_end > PAGE_SIZE) {
        return false;
    }

    int num_key = 0;
    std::memcpy(&num_key, image->data() + offsetof(IxPageHdr, num_key), sizeof(num_key));
    const uint8_t is_leaf = static_cast<uint8_t>((*image)[offsetof(IxPageHdr, is_leaf)]);
    if (num_key < 0 || static_cast<uint64_t>(num_key) > slot_capacity || is_leaf > 1 ||
        (page_no == IX_LEAF_HEADER_PAGE && (num_key != 0 || is_leaf == 0))) {
        return false;
    }

    const uint64_t live_key_end = header_bytes + static_cast<uint64_t>(num_key) * file_hdr.col_tot_len_;
    const uint64_t live_rid_end = rid_begin + static_cast<uint64_t>(num_key) * sizeof(Rid);
    std::fill(image->begin() + static_cast<size_t>(live_key_end), image->begin() + static_cast<size_t>(rid_begin), 0);
    std::fill(image->begin() + static_cast<size_t>(live_rid_end),
              image->begin() + static_cast<size_t>(rid_capacity_end), 0);
    return true;
}
