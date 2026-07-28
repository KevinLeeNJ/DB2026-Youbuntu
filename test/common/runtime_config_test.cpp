/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <gtest/gtest.h>

#include <string>

#include "common/runtime_config.h"

TEST(RuntimeConfigTest, DefaultBufferPoolIsThreeGiB) {
    const auto config = default_buffer_pool_config();
    EXPECT_EQ(config.gibibytes, 3);
    EXPECT_EQ(config.pages, 3 * PAGES_PER_GIB);
}

TEST(RuntimeConfigTest, ParsesAuthorizedBufferPoolSizes) {
    for (size_t gibibytes = 1; gibibytes <= MAX_BUFFER_POOL_GIB; ++gibibytes) {
        const auto config = parse_buffer_pool_config(std::to_string(gibibytes));
        EXPECT_EQ(config.gibibytes, gibibytes);
        EXPECT_EQ(config.pages, gibibytes * PAGES_PER_GIB);
    }
}

TEST(RuntimeConfigTest, RejectsMalformedOrOutOfRangeBufferPoolSizes) {
    for (const std::string_view value : {"", "0", "7", "-1", "+3", "3x", " 3", "3 ", "18446744073709551616"}) {
        EXPECT_THROW((void)parse_buffer_pool_config(value), std::invalid_argument);
    }
}

TEST(RuntimeConfigTest, ParsesSiConflictBackoff) {
    EXPECT_EQ(default_si_conflict_backoff(), std::chrono::microseconds(1000));
    EXPECT_EQ(parse_si_conflict_backoff("0"), std::chrono::microseconds(0));
    EXPECT_EQ(parse_si_conflict_backoff("250"), std::chrono::microseconds(250));
    EXPECT_EQ(parse_si_conflict_backoff("2000"), std::chrono::microseconds(2000));
}

TEST(RuntimeConfigTest, RejectsMalformedOrOutOfRangeSiConflictBackoff) {
    for (const std::string_view value : {"", "2001", "-1", "+3", "3x", " 3", "3 ", "4294967296"}) {
        EXPECT_THROW((void)parse_si_conflict_backoff(value), std::invalid_argument);
    }
}
