#pragma once

#include <cstdlib>

namespace test {

/**
 * @brief Fill buffer with random bytes.
 */
inline void rand_buf(int size, char* buf) {
    for (int i = 0; i < size; i++) {
        int rand_ch = rand() & 0xff;
        buf[i] = rand_ch;
    }
}

} // namespace test
