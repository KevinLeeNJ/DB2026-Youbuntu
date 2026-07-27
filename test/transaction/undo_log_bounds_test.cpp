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

#include <gtest/gtest.h>

#include "errors.h"
#include "transaction/transaction.h"

namespace {

UndoLog MakeUndoLog(char payload) {
    UndoLog log;
    log.is_deleted_ = false;
    log.old_tuple_data_.assign(4, payload);
    return log;
}

// A version chain that reads a corrupted or stale UndoLink used to index past
// the end of the undo buffer and construct an UndoLog from whatever followed it,
// which took the whole server down with SIGSEGV. The bad link must fail the one
// statement that followed it instead.
TEST(UndoLogBoundsTest, OutOfRangeUndoLogIdThrowsInsteadOfCrashing) {
    Transaction txn(1);
    const UndoLink link = txn.AppendUndoLog(MakeUndoLog('a'));
    ASSERT_EQ(txn.GetUndoLogNum(), 1U);

    EXPECT_EQ(txn.GetUndoLog(static_cast<size_t>(link.undo_slot_offset_)).old_tuple_data_, std::vector<char>(4, 'a'));

    // undo_slot_offset_ == 2 is the index observed in the reported crash.
    EXPECT_THROW(txn.GetUndoLog(2), InternalError);
    EXPECT_THROW(txn.GetUndoLog(1), InternalError);
    EXPECT_THROW(txn.GetUndoLog(static_cast<size_t>(-1)), InternalError);
}

TEST(UndoLogBoundsTest, EmptyUndoBufferRejectsEveryUndoLogId) {
    Transaction txn(2);
    ASSERT_EQ(txn.GetUndoLogNum(), 0U);
    EXPECT_THROW(txn.GetUndoLog(0), InternalError);
}

} // namespace
