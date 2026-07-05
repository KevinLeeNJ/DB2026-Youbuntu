/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "gtest/gtest.h"
#include "replacer/clock_replacer.h"

using namespace rmdb;
TEST(ClockReplacerTest, FrequentlyUsedFrameGetsMoreSecondChances) {
    ClockReplacer replacer(3);

    replacer.unpin(0);
    replacer.unpin(0);
    replacer.unpin(1);
    replacer.unpin(2);

    frame_id_t victim = INVALID_FRAME_ID;
    ASSERT_TRUE(replacer.victim(&victim));
    EXPECT_EQ(1, victim);

    ASSERT_TRUE(replacer.victim(&victim));
    EXPECT_EQ(2, victim);

    ASSERT_TRUE(replacer.victim(&victim));
    EXPECT_EQ(0, victim);

    EXPECT_FALSE(replacer.victim(&victim));
}

TEST(ClockReplacerTest, PinRemovesFrameFromCandidates) {
    ClockReplacer replacer(2);

    replacer.unpin(0);
    replacer.unpin(1);
    replacer.pin(0);

    frame_id_t victim = INVALID_FRAME_ID;
    ASSERT_TRUE(replacer.victim(&victim));
    EXPECT_EQ(1, victim);
    EXPECT_FALSE(replacer.victim(&victim));
    EXPECT_EQ(0, replacer.Size());
}
