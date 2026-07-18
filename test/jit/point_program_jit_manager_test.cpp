/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "jit/point_program_jit_manager.h"

namespace {

compiled::ProgramTemplatePtr MakeTemplate(std::string canonical, uint64_t statement_generation = 11,
                                          uint64_t planner_generation = 13, uint64_t catalog_generation = 17) {
    auto program = std::make_shared<const compiled::CompiledProgram>(
        compiled::COMPILED_IR_VERSION, compiled::COMPILED_ABI_VERSION, compiled::ProgramKind::POINT_SELECT,
        catalog_generation, std::vector<compiled::ParameterDesc>{}, std::vector<compiled::RegisterDesc>{},
        std::vector<compiled::TupleLayout>{}, std::vector<compiled::Instruction>{{compiled::Opcode::HALT}});
    compiled::ProgramBindingTemplate bindings;
    bindings.table.table_name = "accounts";
    bindings.table.tuple_width = sizeof(int32_t);
    bindings.table.columns.push_back({"accounts", "id", compiled::ValueType::INT32, 0, sizeof(int32_t)});
    bindings.point_indexes.push_back({"accounts", "accounts_id", {"id"}, {0}});
    bindings.output_columns.push_back({{"accounts", "id", compiled::ValueType::INT32, 0, sizeof(int32_t)}, "id"});
    bindings.conditions.push_back({{"accounts", "id"}, compiled::CompareOp::EQ, true, {}, 0});
    compiled::ProgramTemplateIdentity identity{{101, 202, std::move(canonical)},
                                               catalog_generation,
                                               statement_generation,
                                               planner_generation,
                                               compiled::ProgramKind::POINT_SELECT};
    std::string error;
    auto result = compiled::ProgramTemplate::Create(std::move(identity), std::move(program),
                                                    {{compiled::kNoOperand, 0, compiled::ValueType::INT32, 0}},
                                                    std::move(bindings), &error);
    EXPECT_NE(result, nullptr) << error;
    return result;
}

jit::PointProgramJitConfig EagerConfig() {
    jit::PointProgramJitConfig config;
    config.min_executions = 1;
    config.min_interpreted_ns = 1;
    config.failure_cooldown = std::chrono::seconds(60);
    return config;
}

} // namespace

TEST(PointProgramJitManagerTest, KeyIncludesCanonicalShapeEveryGenerationKindAndAbi) {
    const auto base = jit::MakePointProgramJitKey(*MakeTemplate("select id where id = ?"));
    auto changed = base;
    changed.shape.canonical_bytes = "select id where other = ?";
    EXPECT_NE(base, changed);
    changed = base;
    ++changed.shape.high;
    EXPECT_NE(base, changed);
    changed = base;
    ++changed.shape.low;
    EXPECT_NE(base, changed);
    changed = base;
    ++changed.statement_generation;
    EXPECT_NE(base, changed);
    changed = base;
    ++changed.planner_generation;
    EXPECT_NE(base, changed);
    changed = base;
    ++changed.catalog_generation;
    EXPECT_NE(base, changed);
    changed = base;
    changed.kind = compiled::ProgramKind::POINT_UPDATE;
    EXPECT_NE(base, changed);
    changed = base;
    ++changed.ir_version;
    EXPECT_NE(base, changed);
    changed = base;
    ++changed.abi_version;
    EXPECT_NE(base, changed);
    changed = base;
    ++changed.helper_abi_version;
    EXPECT_NE(base, changed);

    const auto same_hash_different_canonical = jit::MakePointProgramJitKey(*MakeTemplate("other canonical bytes"));
    EXPECT_EQ(base.shape.high, same_hash_different_canonical.shape.high);
    EXPECT_EQ(base.shape.low, same_hash_different_canonical.shape.low);
    EXPECT_NE(base, same_hash_different_canonical);
}

TEST(PointProgramJitManagerTest, ForceCompilesOnceAndManagersAreOwnerScoped) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    std::atomic<int> first_compiles{0};
    std::atomic<int> second_compiles{0};
    auto current = [](const compiled::ProgramTemplateIdentity&) { return true; };
    jit::PointProgramJitManager first(EagerConfig(), current, [&](compiled::ProgramTemplatePtr) {
        ++first_compiles;
        return runtime.compile_test_add_i32();
    });
    jit::PointProgramJitManager second(EagerConfig(), current, [&](compiled::ProgramTemplatePtr) {
        ++second_compiles;
        return runtime.compile_test_add_i32();
    });
    auto program_template = MakeTemplate("force owner ?");

    auto first_code = first.Acquire(program_template, rmdb_config::JitMode::FORCE);
    ASSERT_NE(first_code, nullptr);
    EXPECT_EQ(first_code->test_add_i32(2, 7), 9);
    EXPECT_EQ(first.Acquire(program_template, rmdb_config::JitMode::FORCE), first_code);
    EXPECT_EQ(first_compiles.load(), 1);

    auto second_code = second.Acquire(program_template, rmdb_config::JitMode::FORCE);
    ASSERT_NE(second_code, nullptr);
    EXPECT_EQ(second_compiles.load(), 1);
    EXPECT_NE(first_code, second_code);
    EXPECT_EQ(first.Stats().native_cache_hits, 1U);
}

TEST(PointProgramJitManagerTest, ConcurrentAutoObservationsQueueOneCompilation) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    std::atomic<int> compile_count{0};
    jit::PointProgramJitManager manager(
        EagerConfig(), [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) {
            ++compile_count;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            return runtime.compile_test_add_i32();
        });
    auto program_template = MakeTemplate("auto concurrent ?");
    std::vector<std::thread> threads;
    for (int i = 0; i < 32; ++i) {
        threads.emplace_back([&] {
            auto code = manager.Acquire(program_template, rmdb_config::JitMode::AUTO);
            if (code == nullptr) {
                manager.ObserveInterpreter(program_template, 1, rmdb_config::JitMode::AUTO);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_TRUE(manager.WaitUntilIdle(std::chrono::seconds(2)));
    auto code = manager.Acquire(program_template, rmdb_config::JitMode::AUTO);
    ASSERT_NE(code, nullptr);
    EXPECT_EQ(code->test_add_i32(3, 8), 11);
    EXPECT_EQ(compile_count.load(), 1);
    EXPECT_EQ(manager.Stats().compile_attempts, 1U);
    EXPECT_GE(manager.Stats().interpreter_executions, 1U);
}

TEST(PointProgramJitManagerTest, CompileExceptionEntersCooldownAndRetriesAfterExpiry) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    auto config = EagerConfig();
    config.failure_cooldown = std::chrono::seconds(0);
    std::atomic<int> compile_count{0};
    jit::PointProgramJitManager manager(
        config, [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) -> jit::JitCompileResult {
            if (++compile_count == 1) {
                throw std::runtime_error("synthetic compiler exception");
            }
            return runtime.compile_test_add_i32();
        });
    auto program_template = MakeTemplate("compile failure ?");

    EXPECT_EQ(manager.Acquire(program_template, rmdb_config::JitMode::FORCE), nullptr);
    auto code = manager.Acquire(program_template, rmdb_config::JitMode::FORCE);
    ASSERT_NE(code, nullptr);
    EXPECT_EQ(code->test_add_i32(4, 9), 13);
    EXPECT_EQ(compile_count.load(), 2);
    EXPECT_EQ(manager.Stats().compile_attempts, 2U);
    EXPECT_EQ(manager.Stats().compile_failures, 1U);
}

TEST(PointProgramJitManagerTest, FailedCompilationDoesNotStormDuringCooldown) {
    auto config = EagerConfig();
    config.failure_cooldown = std::chrono::seconds(60);
    std::atomic<int> compile_count{0};
    jit::PointProgramJitManager manager(
        config, [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) {
            ++compile_count;
            return jit::JitCompileResult{jit::JitStatus::COMPILE_ERROR, {}, "synthetic failure"};
        });
    auto program_template = MakeTemplate("cooldown ?");
    EXPECT_EQ(manager.Acquire(program_template, rmdb_config::JitMode::FORCE), nullptr);
    for (int i = 0; i < 16; ++i) {
        EXPECT_EQ(manager.Acquire(program_template, rmdb_config::JitMode::FORCE), nullptr);
    }
    EXPECT_EQ(compile_count.load(), 1);
    EXPECT_EQ(manager.Stats().compile_failures, 1U);
}

TEST(PointProgramJitManagerTest, DropsStaleAsynchronousPublicationAndReleasesCode) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    std::atomic<bool> identity_current{true};
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool compile_started = false;
    bool release_compile = false;
    jit::PointProgramJitManager manager(
        EagerConfig(), [&](const compiled::ProgramTemplateIdentity&) { return identity_current.load(); },
        [&](compiled::ProgramTemplatePtr) {
            std::unique_lock<std::mutex> lock(gate_mutex);
            compile_started = true;
            gate_cv.notify_all();
            gate_cv.wait(lock, [&] { return release_compile; });
            return runtime.compile_test_add_i32();
        });
    auto program_template = MakeTemplate("stale publish ?");
    manager.ObserveInterpreter(program_template, 1, rmdb_config::JitMode::AUTO);
    {
        std::unique_lock<std::mutex> lock(gate_mutex);
        ASSERT_TRUE(gate_cv.wait_for(lock, std::chrono::seconds(2), [&] { return compile_started; }));
        identity_current.store(false);
        release_compile = true;
    }
    gate_cv.notify_all();
    ASSERT_TRUE(manager.WaitUntilIdle(std::chrono::seconds(2)));
    EXPECT_EQ(manager.Acquire(program_template, rmdb_config::JitMode::AUTO), nullptr);
    EXPECT_EQ(manager.Stats().entry_count, 0U);
    EXPECT_EQ(runtime.active_code_count(), 0U);
}

TEST(PointProgramJitManagerTest, EvictionAndShutdownPreserveExternalSharedCodeLease) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    auto config = EagerConfig();
    config.max_entries = 1;
    jit::PointProgramJitManager manager(
        config, [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) { return runtime.compile_test_add_i32(); });
    auto first = manager.Acquire(MakeTemplate("first ?", 1), rmdb_config::JitMode::FORCE);
    ASSERT_NE(first, nullptr);
    auto second = manager.Acquire(MakeTemplate("second ?", 2), rmdb_config::JitMode::FORCE);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(manager.Stats().entry_count, 1U);
    EXPECT_GE(manager.Stats().evictions, 1U);
    EXPECT_EQ(runtime.active_code_count(), 2U);
    EXPECT_EQ(first->test_add_i32(5, 6), 11);

    manager.ShutdownAndDrain();
    EXPECT_EQ(manager.Stats().entry_count, 0U);
    EXPECT_EQ(second->test_add_i32(7, 8), 15);
    EXPECT_EQ(runtime.active_code_count(), 2U);
    first.reset();
    second.reset();
    EXPECT_EQ(runtime.active_code_count(), 0U);
}

TEST(PointProgramJitManagerTest, ShutdownWaitsForInFlightCompileAndRejectsNewWork) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool compile_started = false;
    bool release_compile = false;
    jit::PointProgramJitManager manager(
        EagerConfig(), [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) {
            std::unique_lock<std::mutex> lock(gate_mutex);
            compile_started = true;
            gate_cv.notify_all();
            gate_cv.wait(lock, [&] { return release_compile; });
            return runtime.compile_test_add_i32();
        });
    auto program_template = MakeTemplate("shutdown ?");
    manager.ObserveInterpreter(program_template, 1, rmdb_config::JitMode::AUTO);
    {
        std::unique_lock<std::mutex> lock(gate_mutex);
        ASSERT_TRUE(gate_cv.wait_for(lock, std::chrono::seconds(2), [&] { return compile_started; }));
    }

    std::atomic<bool> shutdown_finished{false};
    std::thread shutdown([&] {
        manager.ShutdownAndDrain();
        shutdown_finished.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(shutdown_finished.load());
    {
        std::lock_guard<std::mutex> lock(gate_mutex);
        release_compile = true;
    }
    gate_cv.notify_all();
    shutdown.join();
    EXPECT_TRUE(shutdown_finished.load());
    EXPECT_EQ(manager.Acquire(program_template, rmdb_config::JitMode::FORCE), nullptr);
    EXPECT_EQ(runtime.active_code_count(), 0U);
}

TEST(PointProgramJitManagerTest, ShutdownWaitsForSynchronousForceCompile) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool compile_started = false;
    bool release_compile = false;
    jit::PointProgramJitManager manager(
        EagerConfig(), [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) {
            std::unique_lock<std::mutex> lock(gate_mutex);
            compile_started = true;
            gate_cv.notify_all();
            gate_cv.wait(lock, [&] { return release_compile; });
            return runtime.compile_test_add_i32();
        });
    auto program_template = MakeTemplate("force shutdown ?");
    std::shared_ptr<const jit::JitCode> force_result;
    std::thread force([&] { force_result = manager.Acquire(program_template, rmdb_config::JitMode::FORCE); });
    {
        std::unique_lock<std::mutex> lock(gate_mutex);
        ASSERT_TRUE(gate_cv.wait_for(lock, std::chrono::seconds(2), [&] { return compile_started; }));
    }

    std::atomic<bool> shutdown_finished{false};
    std::thread shutdown([&] {
        manager.ShutdownAndDrain();
        shutdown_finished.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(shutdown_finished.load());
    {
        std::lock_guard<std::mutex> lock(gate_mutex);
        release_compile = true;
    }
    gate_cv.notify_all();
    force.join();
    shutdown.join();
    EXPECT_TRUE(shutdown_finished.load());
    EXPECT_EQ(force_result, nullptr);
    EXPECT_EQ(manager.Stats().entry_count, 0U);
    EXPECT_EQ(runtime.active_code_count(), 0U);
}

TEST(PointProgramJitManagerTest, IdentityCallbackCanReenterStatsWithoutDeadlock) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    jit::PointProgramJitManager* manager_ptr = nullptr;
    jit::PointProgramJitManager manager(
        EagerConfig(),
        [&](const compiled::ProgramTemplateIdentity&) {
            if (manager_ptr != nullptr) {
                (void)manager_ptr->Stats();
            }
            return true;
        },
        [&](compiled::ProgramTemplatePtr) { return runtime.compile_test_add_i32(); });
    manager_ptr = &manager;
    auto program_template = MakeTemplate("reentrant identity ?");
    auto code = manager.Acquire(program_template, rmdb_config::JitMode::FORCE);
    ASSERT_NE(code, nullptr);
    manager.ObserveInterpreter(program_template, 1, rmdb_config::JitMode::AUTO);
    EXPECT_EQ(code->test_add_i32(10, 5), 15);
}

TEST(PointProgramJitManagerTest, StaleReadyEntryIsRemovedWithoutInvalidatingLease) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    std::atomic<bool> current{true};
    jit::PointProgramJitManager manager(
        EagerConfig(), [&](const compiled::ProgramTemplateIdentity&) { return current.load(); },
        [&](compiled::ProgramTemplatePtr) { return runtime.compile_test_add_i32(); });
    auto program_template = MakeTemplate("stale ready ?");
    auto lease = manager.Acquire(program_template, rmdb_config::JitMode::FORCE);
    ASSERT_NE(lease, nullptr);
    ASSERT_EQ(manager.Stats().entry_count, 1U);
    current.store(false);
    EXPECT_EQ(manager.Acquire(program_template, rmdb_config::JitMode::AUTO), nullptr);
    EXPECT_EQ(manager.Stats().entry_count, 0U);
    EXPECT_EQ(lease->test_add_i32(6, 7), 13);
    EXPECT_EQ(runtime.active_code_count(), 1U);
    lease.reset();
    EXPECT_EQ(runtime.active_code_count(), 0U);
}

TEST(PointProgramJitManagerTest, ConcurrentFailuresConvergeToEntryLimit) {
    auto config = EagerConfig();
    config.max_entries = 1;
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    int compile_started = 0;
    bool release_compile = false;
    jit::PointProgramJitManager manager(
        config, [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) {
            std::unique_lock<std::mutex> lock(gate_mutex);
            ++compile_started;
            gate_cv.notify_all();
            gate_cv.wait(lock, [&] { return release_compile; });
            return jit::JitCompileResult{jit::JitStatus::COMPILE_ERROR, {}, "concurrent failure"};
        });
    auto first = MakeTemplate("failure one ?", 1);
    auto second = MakeTemplate("failure two ?", 2);
    std::thread first_compile([&] { EXPECT_EQ(manager.Acquire(first, rmdb_config::JitMode::FORCE), nullptr); });
    std::thread second_compile([&] { EXPECT_EQ(manager.Acquire(second, rmdb_config::JitMode::FORCE), nullptr); });
    {
        std::unique_lock<std::mutex> lock(gate_mutex);
        ASSERT_TRUE(gate_cv.wait_for(lock, std::chrono::seconds(2), [&] { return compile_started == 2; }));
        release_compile = true;
    }
    gate_cv.notify_all();
    first_compile.join();
    second_compile.join();
    EXPECT_EQ(manager.Stats().compile_attempts, 2U);
    EXPECT_EQ(manager.Stats().compile_failures, 2U);
    EXPECT_LE(manager.Stats().entry_count, config.max_entries);
    EXPECT_GE(manager.Stats().evictions, 1U);
}
