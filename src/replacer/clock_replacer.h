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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>

#include "replacer/replacer.h"

class ClockReplacer : public Replacer {
public:
    explicit ClockReplacer(size_t num_pages) : capacity_(num_pages) {
        in_replacer_ = std::unique_ptr<std::atomic<bool>[]>{new std::atomic<bool>[capacity_] {}};
        usage_count_ = std::unique_ptr<std::atomic<uint8_t>[]>{new std::atomic<uint8_t>[capacity_] {}};
        for (size_t i = 0; i < capacity_; ++i) {
            in_replacer_[i].store(false, std::memory_order_relaxed);
            usage_count_[i].store(0, std::memory_order_relaxed);
        }
    }

    ~ClockReplacer() override = default;

    bool victim(frame_id_t* frame_id) override {
        std::lock_guard<std::mutex> lock(hand_latch_);
        if (capacity_ == 0 || size_.load(std::memory_order_acquire) == 0) {
            if (frame_id != nullptr) {
                *frame_id = INVALID_FRAME_ID;
            }
            return false;
        }

        const size_t max_scan = capacity_ * (static_cast<size_t>(kMaxUsageCount) + 1);
        for (size_t scanned = 0; scanned < max_scan; ++scanned) {
            const frame_id_t fid = static_cast<frame_id_t>(hand_);
            hand_ = (hand_ + 1) % capacity_;

            if (!in_replacer_[fid].load(std::memory_order_acquire)) {
                continue;
            }

            uint8_t usage = usage_count_[fid].load(std::memory_order_acquire);
            if (usage > 0) {
                usage_count_[fid].compare_exchange_strong(usage, static_cast<uint8_t>(usage - 1),
                                                          std::memory_order_acq_rel);
                continue;
            }

            bool expected = true;
            if (in_replacer_[fid].compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
                size_.fetch_sub(1, std::memory_order_acq_rel);
                if (frame_id != nullptr) {
                    *frame_id = fid;
                }
                return true;
            }
        }

        if (frame_id != nullptr) {
            *frame_id = INVALID_FRAME_ID;
        }
        return false;
    }

    void pin(frame_id_t frame_id) override {
        if (!is_valid_frame(frame_id)) {
            return;
        }
        bool expected = true;
        if (in_replacer_[frame_id].compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            size_.fetch_sub(1, std::memory_order_acq_rel);
        }
    }

    void unpin(frame_id_t frame_id) override {
        if (!is_valid_frame(frame_id)) {
            return;
        }

        bump_usage_count(frame_id);
        bool expected = false;
        if (in_replacer_[frame_id].compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            size_.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    void restore(frame_id_t frame_id) override {
        if (!is_valid_frame(frame_id)) {
            return;
        }
        bool expected = false;
        if (in_replacer_[frame_id].compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            size_.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    size_t Size() override {
        return size_.load(std::memory_order_acquire);
    }

private:
    static constexpr uint8_t kMaxUsageCount = 5;

    bool is_valid_frame(frame_id_t frame_id) const {
        return frame_id >= 0 && static_cast<size_t>(frame_id) < capacity_;
    }

    void bump_usage_count(frame_id_t frame_id) {
        uint8_t old = usage_count_[frame_id].load(std::memory_order_acquire);
        while (old < kMaxUsageCount && !usage_count_[frame_id].compare_exchange_weak(old, static_cast<uint8_t>(old + 1),
                                                                                     std::memory_order_acq_rel)) {
        }
    }

    size_t capacity_;
    std::unique_ptr<std::atomic<bool>[]> in_replacer_;
    std::unique_ptr<std::atomic<uint8_t>[]> usage_count_;
    std::atomic<size_t> size_{0};
    size_t hand_{0};
    std::mutex hand_latch_;
};
