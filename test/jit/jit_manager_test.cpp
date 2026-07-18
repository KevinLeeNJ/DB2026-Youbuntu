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

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "jit/jit_manager.h"

namespace {

jit::JitProgram make_program(CompOp op, uint64_t generation) {
    ColMeta column{"t", "v", TYPE_INT, static_cast<int>(sizeof(int32_t)), 0, false};
    Condition condition;
    condition.lhs_col = {"t", "v"};
    condition.op = op;
    condition.is_rhs_val = true;
    condition.rhs_val.set_int(1);
    auto result =
        jit::build_predicate_program(T_SeqScan, {condition}, {sizeof(int32_t), {column}}, std::nullopt, generation);
    EXPECT_TRUE(result) << result.error;
    return std::move(*result.program);
}

jit::JitProgram make_unique_program(uint32_t id, uint64_t generation) {
    const std::string table = "t" + std::to_string(id);
    ColMeta column{table, "v", TYPE_INT, static_cast<int>(sizeof(int32_t)), static_cast<int>(id % 16), false};
    Condition condition;
    condition.lhs_col = {table, "v"};
    condition.op = OP_EQ;
    condition.is_rhs_val = true;
    condition.rhs_val.set_int(static_cast<int>(id));
    auto result = jit::build_predicate_program(T_SeqScan, {condition},
                                               {static_cast<uint32_t>(column.offset + column.len), {column}},
                                               std::nullopt, generation);
    EXPECT_TRUE(result) << result.error;
    return std::move(*result.program);
}

jit::JitManagerConfig eager_config() {
    jit::JitManagerConfig config;
    config.min_executions = 1;
    config.min_tuple_evaluations = 1;
    config.min_interpreted_ns = 1;
    return config;
}

} // namespace

TEST(JitManagerTest, ConcurrentAutoObservationsCompileOneShapeOnce) {
    std::atomic<uint64_t> generation{5};
    std::atomic<int> compile_count{0};
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    auto config = eager_config();
    jit::JitManager manager(
        config, [&] { return generation.load(); },
        [&](const jit::JitProgram&) {
            ++compile_count;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            return runtime.compile_test_add_i32();
        });
    const jit::JitProgram program = make_program(OP_EQ, generation.load());
    std::vector<std::thread> threads;
    for (int i = 0; i < 32; ++i) {
        threads.emplace_back([&] { EXPECT_EQ(manager.observe(program, jit::JitMode::AUTO, {1, 1}), nullptr); });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_TRUE(manager.wait_until_idle(std::chrono::seconds(2)));
    EXPECT_EQ(compile_count.load(), 1);
    auto code = manager.observe(program, jit::JitMode::AUTO, {1, 1});
    ASSERT_NE(code, nullptr);
    EXPECT_EQ(code->test_add_i32(4, 6), 10);
    EXPECT_EQ(manager.stats().cache_hits, 1U);
}

TEST(JitManagerTest, FailureCooldownAndQueueCapacityFallbackWithoutBlocking) {
    std::atomic<uint64_t> generation{8};
    std::atomic<int> compile_count{0};
    auto config = eager_config();
    config.max_queue_size = 1;
    config.failure_cooldown = std::chrono::seconds(60);
    jit::JitManager manager(
        config, [&] { return generation.load(); },
        [&](const jit::JitProgram&) {
            ++compile_count;
            return jit::JitCompileResult{jit::JitStatus::COMPILE_ERROR, {}, "test failure"};
        });
    const auto first = make_program(OP_EQ, generation.load());
    const auto second = make_program(OP_NE, generation.load());
    EXPECT_EQ(manager.observe(first, jit::JitMode::AUTO, {1, 1}), nullptr);
    EXPECT_EQ(manager.observe(second, jit::JitMode::AUTO, {1, 1}), nullptr);
    EXPECT_LE(manager.stats().queued_count, 1U);
    ASSERT_TRUE(manager.wait_until_idle(std::chrono::seconds(2)));
    EXPECT_EQ(compile_count.load(), 1);
    EXPECT_EQ(manager.observe(first, jit::JitMode::FORCE, {1, 1}), nullptr);
    EXPECT_EQ(compile_count.load(), 1);
    EXPECT_GE(manager.stats().fallbacks, 3U);
}

TEST(JitManagerTest, InvalidatesEpochAndReleasesEvictedCodeOutsideTheCache) {
    std::atomic<uint64_t> generation{13};
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    auto config = eager_config();
    config.max_entries = 1;
    jit::JitManager manager(
        config, [&] { return generation.load(); },
        [&](const jit::JitProgram&) { return runtime.compile_test_add_i32(); });
    const auto first_program = make_program(OP_EQ, generation.load());
    auto first_code = manager.observe(first_program, jit::JitMode::FORCE, {1, 1});
    ASSERT_NE(first_code, nullptr);
    EXPECT_EQ(runtime.active_code_count(), 1U);

    generation.store(14);
    EXPECT_EQ(manager.observe(first_program, jit::JitMode::AUTO, {1, 1}), nullptr);
    EXPECT_EQ(first_code->test_add_i32(2, 9), 11);

    const auto replacement_program = make_program(OP_NE, generation.load());
    auto replacement = manager.observe(replacement_program, jit::JitMode::FORCE, {1, 1});
    ASSERT_NE(replacement, nullptr);
    EXPECT_EQ(manager.stats().entry_count, 1U);
    first_code.reset();
    EXPECT_EQ(runtime.active_code_count(), 1U);
    manager.shutdown_and_drain();
    EXPECT_EQ(manager.stats().entry_count, 0U);
    replacement.reset();
    EXPECT_EQ(runtime.active_code_count(), 0U);
}

TEST(JitManagerTest, DropsCompilationWhenCatalogChangesBeforePublication) {
    std::atomic<uint64_t> generation{21};
    std::atomic<bool> started{false};
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    auto config = eager_config();
    jit::JitManager manager(
        config, [&] { return generation.load(); },
        [&](const jit::JitProgram&) {
            started.store(true);
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            return runtime.compile_test_add_i32();
        });
    const auto program = make_program(OP_EQ, generation.load());
    EXPECT_EQ(manager.observe(program, jit::JitMode::AUTO, {1, 1}), nullptr);
    while (!started.load()) {
        std::this_thread::yield();
    }
    generation.store(22);
    ASSERT_TRUE(manager.wait_until_idle(std::chrono::seconds(2)));
    EXPECT_EQ(manager.observe(program, jit::JitMode::AUTO, {1, 1}), nullptr);
    EXPECT_EQ(runtime.active_code_count(), 0U);
}

TEST(JitManagerTest, KeepsEntryAndCodeUsageWithinConfiguredBoundsUnderShapeChurn) {
    std::atomic<uint64_t> generation{34};
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    auto config = eager_config();
    config.max_entries = 4;
    config.max_code_bytes = 1024U * 1024U;
    jit::JitManager manager(
        config, [&] { return generation.load(); },
        [&](const jit::JitProgram&) { return runtime.compile_test_add_i32(); });
    for (uint32_t id = 0; id < 128; ++id) {
        EXPECT_EQ(manager.observe(make_unique_program(id, generation.load()), jit::JitMode::AUTO, {1, 1}), nullptr);
    }
    ASSERT_TRUE(manager.wait_until_idle(std::chrono::seconds(5)));
    const auto stats = manager.stats();
    EXPECT_LE(stats.entry_count, config.max_entries);
    EXPECT_LE(stats.code_bytes, config.max_code_bytes);
    EXPECT_LE(runtime.active_code_count(), config.max_entries);
}

TEST(JitManagerTest, ShutdownWaitsForActiveExecutionScopeBeforeReleasingCode) {
    std::atomic<uint64_t> generation{55};
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    jit::JitManager manager(
        eager_config(), [&] { return generation.load(); },
        [&](const jit::JitProgram&) { return runtime.compile_test_add_i32(); });
    auto code = manager.observe(make_program(OP_EQ, generation.load()), jit::JitMode::FORCE, {1, 1});
    ASSERT_NE(code, nullptr);
    std::atomic<bool> shutdown_finished{false};
    std::thread shutdown_thread;
    {
        auto scope = manager.enter_execution();
        shutdown_thread = std::thread([&] {
            manager.shutdown_and_drain();
            shutdown_finished.store(true);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        EXPECT_FALSE(shutdown_finished.load());
    }
    shutdown_thread.join();
    EXPECT_TRUE(shutdown_finished.load());
    code.reset();
    EXPECT_EQ(runtime.active_code_count(), 0U);
}
