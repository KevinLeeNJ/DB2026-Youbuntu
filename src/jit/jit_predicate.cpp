/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of the Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "jit/jit_predicate.h"

#include <atomic>
#include <memory>
#include <mutex>

namespace jit {
namespace {

std::mutex service_mutex;
std::unique_ptr<JitRuntime> service_runtime;
std::unique_ptr<JitManager> service_manager;
std::atomic<JitManager*> service_manager_fast{nullptr};
thread_local uint32_t shadow_counter = 0;

bool valid_frame(const JitProgram& program, const JitCallFrame& frame) {
    if (frame.tuple0 == nullptr || frame.tuple0_len < program.tuple0.tuple_len ||
        frame.param_count != program.parameters.size() || (frame.param_count != 0 && frame.params == nullptr)) {
        return false;
    }
    if (program.tuple1.has_value() && (frame.tuple1 == nullptr || frame.tuple1_len < program.tuple1->tuple_len)) {
        return false;
    }
    for (uint32_t index = 0; index < frame.param_count; ++index) {
        const auto& descriptor = program.parameters[index];
        const auto& value = frame.params[index];
        if (value.type != descriptor.type ||
            ((value.type == TYPE_STRING || value.type == TYPE_DATETIME) && value.bytes_len > descriptor.max_len)) {
            return false;
        }
    }
    return true;
}

} // namespace

PredicateKernel::PredicateKernel(PlanTag plan_tag, const std::vector<Condition>& conditions, JitTupleLayout tuple0,
                                 std::optional<JitTupleLayout> tuple1, uint64_t catalog_generation) {
    JitBuildResult result =
        build_predicate_program(plan_tag, conditions, std::move(tuple0), std::move(tuple1), catalog_generation);
    if (result) {
        params_ = std::move(result.params);
        program_ = std::move(result.program);
    }
}

std::optional<bool> PredicateKernel::evaluate(const char* tuple0, uint32_t tuple0_len, const char* tuple1,
                                              uint32_t tuple1_len) const {
    const auto mode = rmdb_config::jit_mode;
    if (!program_.has_value() || mode == JitMode::OFF) {
        return std::nullopt;
    }
    JitManager* manager = service_manager_fast.load(std::memory_order_acquire);
    if (manager == nullptr) {
        return std::nullopt;
    }
    JitCallFrame frame{
        tuple0, tuple0_len, tuple1, tuple1_len, params_.values.data(), static_cast<uint32_t>(params_.values.size()),
        false};
    if (!valid_frame(*program_, frame)) {
        return std::nullopt;
    }
    if (code_ == nullptr) {
        ++pending_evaluations_;
        if (!observed_once_ || pending_evaluations_ >= rmdb_config::kJitObserveBatchSize) {
            code_ = manager->observe(*program_, mode, {1, pending_evaluations_});
            pending_evaluations_ = 0;
            observed_once_ = true;
        }
    }
    if (code_ == nullptr || code_->invoke_predicate(&frame) != JitStatus::OK) {
        return std::nullopt;
    }
    if (++shadow_counter == rmdb_config::kJitShadowSampleRate) {
        shadow_counter = 0;
        JitCallFrame interpreted = frame;
        if (interpret_predicate(*program_, &interpreted) != JitStatus::OK || interpreted.match != frame.match) {
            manager->discard(*program_);
            code_.reset();
            return std::nullopt;
        }
    }
    return frame.match;
}

void initialize_predicate_jit(std::function<uint64_t()> catalog_generation) {
    std::lock_guard<std::mutex> lock(service_mutex);
    service_runtime = std::make_unique<JitRuntime>();
    JitManagerConfig config;
    config.min_interpreted_ns = 0;
    service_manager =
        std::make_unique<JitManager>(config, std::move(catalog_generation), [](const JitProgram& program) {
            return service_runtime->compile_predicate(program);
        });
    service_manager_fast.store(service_manager.get(), std::memory_order_release);
}

bool predicate_jit_available() {
    std::lock_guard<std::mutex> lock(service_mutex);
    return service_manager != nullptr;
}

JitManager::ExecutionScope enter_predicate_jit_execution() {
    std::lock_guard<std::mutex> lock(service_mutex);
    return service_manager == nullptr ? JitManager::ExecutionScope{} : service_manager->enter_execution();
}

JitManagerStats predicate_jit_stats() {
    std::lock_guard<std::mutex> lock(service_mutex);
    return service_manager == nullptr ? JitManagerStats{} : service_manager->stats();
}

void shutdown_predicate_jit() {
    std::unique_ptr<JitManager> manager;
    std::unique_ptr<JitRuntime> runtime;
    {
        std::lock_guard<std::mutex> lock(service_mutex);
        service_manager_fast.store(nullptr, std::memory_order_release);
        manager = std::move(service_manager);
        runtime = std::move(service_runtime);
    }
    if (manager != nullptr) {
        manager->shutdown_and_drain();
    }
}

} // namespace jit
