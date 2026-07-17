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

#include "ix_scan.h"

/**
 * @brief 前进到下一个索引项。当前叶子保持 pinned，跨叶时换页。
 *        leaf_ 与 pinned_leaf_page_ 始终与 iid_.page_no 同步（非 end 时）。
 */
void IxScan::next() {
    assert(!is_end());
    if (!coupled_mode_) {
        if (direction_ == ScanDirection::Backward) {
            if (iid_ == end_) {
                move_legacy_to_end();
                return;
            }
            assert(pinned_leaf_page_ != nullptr);
            if (iid_.slot_no > 0) {
                --iid_.slot_no;
                return;
            } else {
                const page_id_t prev_leaf = leaf_.get_prev_leaf();
                if (prev_leaf == IX_LEAF_HEADER_PAGE || prev_leaf == IX_NO_PAGE) {
                    move_legacy_to_end();
                    return;
                }
                unpin_current_leaf();
                iid_ = Iid{prev_leaf, std::numeric_limits<int>::max()};
            }
            normalize_backward_position();
            return;
        }
        normalize_legacy_position();
        if (!is_end()) {
            assert(pinned_leaf_page_ != nullptr);
            ++iid_.slot_no;
            normalize_legacy_position();
        }
        return;
    }

    ++batch_pos_;
    if (batch_pos_ < batch_.size()) {
        return;
    }
    remember_completed_batch_tail();
    load_coupled_batch();
}
