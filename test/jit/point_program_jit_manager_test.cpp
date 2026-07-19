/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <new>
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
    compiled::ProgramTemplateIdentity identity{{101, 202, static_cast<uint32_t>(canonical.size())},
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
    ++changed.shape.canonical_size;
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

TEST(PointProgramJitManagerTest, CurrentTemplateUsesThreadLocalRecentCodeWithoutIdentityCallback) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    std::atomic<uint64_t> identity_calls{0};
    jit::PointProgramJitManager manager(
        EagerConfig(),
        [&](const compiled::ProgramTemplateIdentity&) {
            identity_calls.fetch_add(1, std::memory_order_relaxed);
            return true;
        },
        [&](compiled::ProgramTemplatePtr) { return runtime.compile_test_add_i32(); });
    auto program_template = MakeTemplate("thread local recent ?");

    auto first = manager.AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true);
    ASSERT_NE(first, nullptr);
    const uint64_t calls_after_compile = identity_calls.load(std::memory_order_relaxed);
    auto second = manager.AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true);
    EXPECT_EQ(second, first);
    EXPECT_EQ(identity_calls.load(std::memory_order_relaxed), calls_after_compile);
    EXPECT_EQ(manager.Stats().native_cache_hits, 1U);
}

TEST(PointProgramJitManagerTest, ThreadLocalRecentCodePeriodicallyRefreshesManagerLru) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    std::atomic<uint64_t> identity_calls{0};
    jit::PointProgramJitManager manager(
        EagerConfig(),
        [&](const compiled::ProgramTemplateIdentity&) {
            identity_calls.fetch_add(1, std::memory_order_relaxed);
            return true;
        },
        [&](compiled::ProgramTemplatePtr) { return runtime.compile_test_add_i32(); });
    auto program_template = MakeTemplate("thread local lru refresh ?");

    ASSERT_NE(manager.AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true), nullptr);
    const uint64_t calls_after_compile = identity_calls.load(std::memory_order_relaxed);
    for (size_t i = 0; i < 64; ++i) {
        ASSERT_NE(manager.AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true), nullptr);
    }
    EXPECT_EQ(identity_calls.load(std::memory_order_relaxed), calls_after_compile + 1);
    EXPECT_EQ(manager.Stats().native_cache_hits, 64U);
    ASSERT_NE(manager.AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true), nullptr);
    EXPECT_EQ(identity_calls.load(std::memory_order_relaxed), calls_after_compile + 1);
    EXPECT_EQ(manager.Stats().native_cache_hits, 65U);
}

TEST(PointProgramJitManagerTest, PeriodicRefreshProtectsHotThreadLocalEntryFromLruEviction) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    auto config = EagerConfig();
    config.max_entries = 2;
    std::atomic<uint64_t> compile_count{0};
    jit::PointProgramJitManager manager(
        config, [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) {
            compile_count.fetch_add(1, std::memory_order_relaxed);
            return runtime.compile_test_add_i32();
        });
    auto hot = MakeTemplate("thread local hot ?", 1);
    auto cold = MakeTemplate("thread local cold ?", 2);
    auto incoming = MakeTemplate("thread local incoming ?", 3);

    ASSERT_NE(manager.AcquireCurrent(hot, rmdb_config::JitMode::FORCE, true), nullptr);
    std::thread cold_thread(
        [&] { EXPECT_NE(manager.AcquireCurrent(cold, rmdb_config::JitMode::FORCE, true), nullptr); });
    cold_thread.join();
    for (size_t i = 0; i < 64; ++i) {
        ASSERT_NE(manager.AcquireCurrent(hot, rmdb_config::JitMode::FORCE, true), nullptr);
    }
    std::thread incoming_thread(
        [&] { EXPECT_NE(manager.AcquireCurrent(incoming, rmdb_config::JitMode::FORCE, true), nullptr); });
    incoming_thread.join();
    ASSERT_EQ(compile_count.load(std::memory_order_relaxed), 3U);

    ASSERT_NE(manager.AcquireCurrent(hot, rmdb_config::JitMode::FORCE, true), nullptr);
    EXPECT_EQ(compile_count.load(std::memory_order_relaxed), 3U);
    ASSERT_NE(manager.AcquireCurrent(cold, rmdb_config::JitMode::FORCE, true), nullptr);
    EXPECT_EQ(compile_count.load(std::memory_order_relaxed), 4U);
}

TEST(PointProgramJitManagerTest, ThreadLocalRecentCodeInvalidatesOnTemplateSwitchAndEviction) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    auto config = EagerConfig();
    config.max_entries = 1;
    std::atomic<uint64_t> compile_count{0};
    jit::PointProgramJitManager manager(
        config, [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) {
            compile_count.fetch_add(1, std::memory_order_relaxed);
            return runtime.compile_test_add_i32();
        });
    auto first_template = MakeTemplate("thread local first ?", 1);
    auto second_template = MakeTemplate("thread local second ?", 2);

    ASSERT_NE(manager.AcquireCurrent(first_template, rmdb_config::JitMode::FORCE, true), nullptr);
    ASSERT_NE(manager.AcquireCurrent(second_template, rmdb_config::JitMode::FORCE, true), nullptr);
    ASSERT_NE(manager.AcquireCurrent(first_template, rmdb_config::JitMode::FORCE, true), nullptr);
    EXPECT_EQ(compile_count.load(std::memory_order_relaxed), 3U);
    EXPECT_EQ(manager.Stats().entry_count, 1U);
    EXPECT_GE(manager.Stats().evictions, 2U);
}

TEST(PointProgramJitManagerTest, ThreadLocalRecentCodeInvalidatesOnModeOffAndShutdown) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    jit::PointProgramJitManager manager(
        EagerConfig(), [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) { return runtime.compile_test_add_i32(); });
    auto program_template = MakeTemplate("thread local shutdown ?");

    ASSERT_NE(manager.AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true), nullptr);
    EXPECT_EQ(manager.AcquireCurrent(program_template, rmdb_config::JitMode::OFF, true), nullptr);
    auto lease = manager.AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true);
    ASSERT_NE(lease, nullptr);
    std::weak_ptr<const jit::JitCode> weak_code = lease;
    lease.reset();
    manager.ShutdownAndDrain();
    EXPECT_TRUE(weak_code.expired());
    EXPECT_EQ(runtime.active_code_count(), 0U);
    EXPECT_EQ(manager.AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true), nullptr);
}

TEST(PointProgramJitManagerTest, ThreadLocalHintDoesNotRetainCodeAfterEviction) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    auto config = EagerConfig();
    config.max_entries = 1;
    std::atomic<uint64_t> compile_count{0};
    jit::PointProgramJitManager manager(
        config, [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) {
            compile_count.fetch_add(1, std::memory_order_relaxed);
            return runtime.compile_test_add_i32();
        });
    auto first_template = MakeTemplate("thread local weak first ?", 1);
    auto second_template = MakeTemplate("thread local weak second ?", 2);

    auto first = manager.AcquireCurrent(first_template, rmdb_config::JitMode::FORCE, true);
    ASSERT_NE(first, nullptr);
    std::weak_ptr<const jit::JitCode> first_weak = first;
    first.reset();
    ASSERT_NE(manager.AcquireCurrent(first_template, rmdb_config::JitMode::FORCE, true), nullptr);

    auto second = manager.AcquireCurrent(second_template, rmdb_config::JitMode::FORCE, true);
    ASSERT_NE(second, nullptr);
    EXPECT_TRUE(first_weak.expired());
    EXPECT_EQ(runtime.active_code_count(), 1U);
    EXPECT_EQ(compile_count.load(std::memory_order_relaxed), 2U);

    auto replacement = manager.AcquireCurrent(first_template, rmdb_config::JitMode::FORCE, true);
    ASSERT_NE(replacement, nullptr);
    EXPECT_EQ(compile_count.load(std::memory_order_relaxed), 3U);
    second.reset();
    replacement.reset();
    manager.ShutdownAndDrain();
    EXPECT_EQ(runtime.active_code_count(), 0U);
}

TEST(PointProgramJitManagerTest, ThreadLocalRecentCodeRejectsStaleIdentityWithoutInvalidatingLease) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    std::atomic<bool> current{true};
    jit::PointProgramJitManager manager(
        EagerConfig(), [&](const compiled::ProgramTemplateIdentity&) { return current.load(); },
        [&](compiled::ProgramTemplatePtr) { return runtime.compile_test_add_i32(); });
    auto program_template = MakeTemplate("thread local stale ?");

    auto lease = manager.AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true);
    ASSERT_NE(lease, nullptr);
    current.store(false);
    EXPECT_EQ(manager.AcquireCurrent(program_template, rmdb_config::JitMode::AUTO, false), nullptr);
    EXPECT_EQ(manager.Stats().entry_count, 0U);
    EXPECT_EQ(manager.Stats().code_bytes, 0U);
    EXPECT_EQ(manager.Stats().evictions, 1U);
    EXPECT_EQ(lease->test_add_i32(8, 9), 17);
}

TEST(PointProgramJitManagerTest, RequestIdentityMismatchNeverFallsBackToExecutableCode) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    jit::PointProgramJitManager manager(
        EagerConfig(), [](const compiled::ProgramTemplateIdentity&) { return true; },
        [&](compiled::ProgramTemplatePtr) { return runtime.compile_test_add_i32(); });
    auto program_template = MakeTemplate("thread local request mismatch ?");

    ASSERT_NE(manager.AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true), nullptr);
    EXPECT_EQ(manager.AcquireCurrent(program_template, rmdb_config::JitMode::AUTO, false), nullptr);
    EXPECT_EQ(manager.Stats().entry_count, 1U);
}

TEST(PointProgramJitManagerTest, ThreadLocalRecentCodeRejectsReusedManagerAddress) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    std::atomic<uint64_t> compile_count{0};
    auto current = [](const compiled::ProgramTemplateIdentity&) { return true; };
    auto compile = [&](compiled::ProgramTemplatePtr) {
        compile_count.fetch_add(1, std::memory_order_relaxed);
        return runtime.compile_test_add_i32();
    };
    auto program_template = MakeTemplate("thread local manager address ?");
    alignas(jit::PointProgramJitManager) unsigned char storage[sizeof(jit::PointProgramJitManager)];

    auto* first = new (storage) jit::PointProgramJitManager(EagerConfig(), current, compile);
    auto first_code = first->AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true);
    ASSERT_NE(first_code, nullptr);
    first->~PointProgramJitManager();

    auto* second = new (storage) jit::PointProgramJitManager(EagerConfig(), current, compile);
    auto second_code = second->AcquireCurrent(program_template, rmdb_config::JitMode::FORCE, true);
    ASSERT_NE(second_code, nullptr);
    EXPECT_NE(second_code, first_code);
    EXPECT_EQ(compile_count.load(std::memory_order_relaxed), 2U);
    second->~PointProgramJitManager();
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

TEST(PointProgramJitManagerTest, ConcurrentNativeExecutionAccountingIsExact) {
    jit::PointProgramJitManager manager(
        EagerConfig(), [](const compiled::ProgramTemplateIdentity&) { return true; },
        [](compiled::ProgramTemplatePtr) { return jit::JitCompileResult{}; });
    constexpr size_t kThreads = 8;
    constexpr size_t kExecutionsPerThread = 10000;
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (size_t i = 0; i < kThreads; ++i) {
        workers.emplace_back([&manager] {
            for (size_t j = 0; j < kExecutionsPerThread; ++j) {
                manager.RecordNativeExecution();
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_EQ(manager.Stats().native_executions, kThreads * kExecutionsPerThread);
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
