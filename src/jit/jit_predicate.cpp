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

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/context.h"

namespace jit {
namespace {

std::mutex service_mutex;
std::unique_ptr<JitRuntime> service_runtime;
std::unique_ptr<JitManager> service_manager;
std::atomic<JitManager*> service_manager_fast{nullptr};
thread_local uint32_t shadow_counter = 0;

struct PredicateProgramCacheKey {
    uint64_t statement_shape_high{0};
    uint64_t statement_shape_low{0};
    uint64_t statement_template_generation{0};
    uint64_t catalog_generation{0};
    uint32_t ordinal{0};
    PlanTag plan_tag{T_Invalid};

    friend bool operator==(const PredicateProgramCacheKey& lhs, const PredicateProgramCacheKey& rhs) {
        return lhs.statement_shape_high == rhs.statement_shape_high &&
               lhs.statement_shape_low == rhs.statement_shape_low &&
               lhs.statement_template_generation == rhs.statement_template_generation &&
               lhs.catalog_generation == rhs.catalog_generation && lhs.ordinal == rhs.ordinal &&
               lhs.plan_tag == rhs.plan_tag;
    }
};

struct PredicateProgramCacheKeyHash {
    size_t operator()(const PredicateProgramCacheKey& key) const {
        size_t hash = static_cast<size_t>(key.statement_shape_high ^ (key.statement_shape_low << 1U));
        hash ^= static_cast<size_t>(key.statement_template_generation + 0x9e3779b97f4a7c15ULL + (hash << 6U) +
                                    (hash >> 2U));
        hash ^= static_cast<size_t>(key.catalog_generation + (hash << 6U) + (hash >> 2U));
        hash ^= static_cast<size_t>(key.ordinal) << 1U;
        hash ^= static_cast<size_t>(key.plan_tag) << 17U;
        return hash;
    }
};

class PredicateProgramCache {
public:
    std::shared_ptr<const JitProgram> lookup(const PredicateProgramCacheKey& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = entries_.find(key);
        if (found == entries_.end()) {
            return nullptr;
        }
        found->second.last_use = ++clock_;
        return found->second.program;
    }

    std::shared_ptr<const JitProgram> publish(const PredicateProgramCacheKey& key,
                                              std::shared_ptr<const JitProgram> program) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = entries_.find(key);
        if (found != entries_.end()) {
            found->second.last_use = ++clock_;
            return found->second.program;
        }
        entries_.emplace(key, Entry{std::move(program), ++clock_});
        if (entries_.size() > rmdb_config::kJitPredicateProgramCacheCapacity) {
            auto victim = std::min_element(entries_.begin(), entries_.end(), [](const auto& lhs, const auto& rhs) {
                return lhs.second.last_use < rhs.second.last_use;
            });
            entries_.erase(victim);
        }
        return entries_.find(key)->second.program;
    }

    void erase(const PredicateProgramCacheKey& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        entries_.erase(key);
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        entries_.clear();
        clock_ = 0;
    }

private:
    struct Entry {
        std::shared_ptr<const JitProgram> program;
        uint64_t last_use{0};
    };

    std::mutex mutex_;
    std::unordered_map<PredicateProgramCacheKey, Entry, PredicateProgramCacheKeyHash> entries_;
    uint64_t clock_{0};
};

PredicateProgramCache predicate_program_cache;

std::optional<PredicateProgramCacheKey> next_program_cache_key(Context* context, PlanTag plan_tag,
                                                               uint64_t catalog_generation) {
    if (context == nullptr || !context->has_statement_template_identity_) {
        return std::nullopt;
    }
    return PredicateProgramCacheKey{context->statement_shape_high_,          context->statement_shape_low_,
                                    context->statement_template_generation_, catalog_generation,
                                    context->jit_predicate_ordinal_++,       plan_tag};
}

bool valid_tuple_frame(const JitProgram& program, const JitCallFrame& frame) {
    if (frame.tuple0 == nullptr || frame.tuple0_len < program.tuple0.tuple_len ||
        (frame.param_count != 0 && frame.params == nullptr)) {
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
                                 std::optional<JitTupleLayout> tuple1, uint64_t catalog_generation, Context* context) {
    const auto cache_key = next_program_cache_key(context, plan_tag, catalog_generation);
    if (cache_key.has_value()) {
        program_ = predicate_program_cache.lookup(*cache_key);
        if (program_ != nullptr) {
            JitBindResult binding = bind_parameters(*program_, conditions);
            if (binding) {
                params_ = std::move(*binding.params);
                return;
            }
            program_.reset();
            predicate_program_cache.erase(*cache_key);
        }
    }
    JitBuildResult result =
        build_predicate_program(plan_tag, conditions, std::move(tuple0), std::move(tuple1), catalog_generation);
    if (result) {
        auto program = std::make_shared<const JitProgram>(std::move(*result.program));
        if (cache_key.has_value()) {
            program_ = predicate_program_cache.publish(*cache_key, std::move(program));
            JitBindResult binding = bind_parameters(*program_, conditions);
            if (!binding) {
                program_.reset();
                predicate_program_cache.erase(*cache_key);
                return;
            }
            params_ = std::move(*binding.params);
        } else {
            params_ = std::move(result.params);
            program_ = std::move(program);
        }
    }
}

std::optional<bool> PredicateKernel::evaluate(const char* tuple0, uint32_t tuple0_len, const char* tuple1,
                                              uint32_t tuple1_len) const {
    const auto mode = rmdb_config::jit_mode;
    if (program_ == nullptr || mode == JitMode::OFF) {
        return std::nullopt;
    }
    JitManager* manager = service_manager_fast.load(std::memory_order_acquire);
    if (manager == nullptr) {
        return std::nullopt;
    }
    JitCallFrame frame{
        tuple0, tuple0_len, tuple1, tuple1_len, params_.values.data(), static_cast<uint32_t>(params_.values.size()),
        false};
    if (!valid_tuple_frame(*program_, frame)) {
        return std::nullopt;
    }
    if (code_ == nullptr) {
        ++pending_evaluations_;
        if (!observed_once_ || pending_evaluations_ >= rmdb_config::kJitObserveBatchSize) {
            code_ = manager->observe_verified(*program_, mode, {1, pending_evaluations_});
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
    predicate_program_cache.clear();
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
    return service_manager_fast.load(std::memory_order_acquire) != nullptr;
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
    predicate_program_cache.clear();
}

} // namespace jit
