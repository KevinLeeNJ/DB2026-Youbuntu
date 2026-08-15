/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cinttypes>
#include <cstring>

static constexpr int BITMAP_WIDTH = 8;
static constexpr unsigned BITMAP_HIGHEST_BIT = 0x80u; // 128 (2^7)

class Bitmap {
public:
    // 从地址bm开始的size个字节全部置0
    static void init(char* bm, int size) {
        memset(bm, 0, size);
    }

    // pos位 置1
    static void set(char* bm, int pos) {
        bm[get_bucket(pos)] |= get_bit(pos);
    }

    // pos位 置0
    static void reset(char* bm, int pos) {
        bm[get_bucket(pos)] &= static_cast<char>(~get_bit(pos));
    }

    // 如果pos位是1，则返回true
    static bool is_set(const char* bm, int pos) {
        return (bm[get_bucket(pos)] & get_bit(pos)) != 0;
    }

    /**
     * @brief 找下一个为0 or 1的位
     * @param bit false表示要找下一个为0的位，true表示要找下一个为1的位
     * @param bm 要找的起始地址为bm
     * @param max_n 要找的从起始地址开始的偏移为[curr+1,max_n)
     * @param curr 要找的从起始地址开始的偏移为[curr+1,max_n)
     * @return 找到了就返回偏移位置，没找到就返回max_n
     */
    static int next_bit(bool bit, const char* bm, int max_n, int curr) {
        const int start = curr + 1;
        if (start >= max_n) {
            return max_n;
        }
        const auto* bytes = reinterpret_cast<const unsigned char*>(bm);
        int byte = start / 8;
        const int limit_byte = (max_n + 7) / 8;
        // Within a byte, bit index 0 is the most significant bit, so the first
        // matching bit is the highest set bit of the (possibly inverted) byte:
        // p = 7 - highest_value_bit = clz(byte) - 24.
        unsigned mask = (1u << (8 - (start % 8))) - 1;
        auto try_byte = [&](int index) -> int {
            unsigned target = bit ? bytes[index] : static_cast<unsigned char>(~bytes[index]);
            const unsigned bits = target & mask;
            if (bits == 0) {
                return -1;
            }
            const int found = index * 8 + (__builtin_clz(bits) - 24);
            return found < max_n ? found : max_n;
        };
        const int first = try_byte(byte);
        if (first >= 0) {
            return first;
        }
        ++byte;
        mask = 0xFFu;
        // Scan 64-bit words: byte k of the bitmap is the k-th least significant
        // byte of the word, so ctzll finds the lowest matching bit in order.
        while (byte + 8 <= limit_byte) {
            uint64_t word = 0;
            std::memcpy(&word, bytes + byte, sizeof(word));
            const uint64_t bits = bit ? word : ~word;
            if (bits != 0) {
                // The lowest set bit picks the first (lowest memory-order)
                // byte with a match; within that byte the first matching bit
                // is its most significant bit.
                const int rel = __builtin_ctzll(bits);
                const int byte_rel = rel / 8;
                const unsigned char byte_val =
                    static_cast<unsigned char>((bits >> (8 * byte_rel)) & 0xFF);
                const int found = (byte + byte_rel) * 8 + (__builtin_clz(byte_val) - 24);
                return found < max_n ? found : max_n;
            }
            byte += 8;
        }
        while (byte < limit_byte) {
            const int found = try_byte(byte);
            if (found >= 0) {
                return found;
            }
            ++byte;
        }
        return max_n;
    }

    // 找第一个为0 or 1的位
    static int first_bit(bool bit, const char* bm, int max_n) {
        return next_bit(bit, bm, max_n, -1);
    }

    // for example:
    // rid_.slot_no = Bitmap::next_bit(true, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page,
    // rid_.slot_no); int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);

private:
    static int get_bucket(int pos) {
        return pos / BITMAP_WIDTH;
    }

    static char get_bit(int pos) {
        return BITMAP_HIGHEST_BIT >> static_cast<char>(pos % BITMAP_WIDTH);
    }
};
