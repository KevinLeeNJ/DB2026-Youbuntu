/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of the Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "jit/jit_manager.h"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace jit {

struct JitManager::Entry {
    explicit Entry(JitProgram initial_program) : program(std::move(initial_program)) {}

    JitProgram program;
    JitEntryState state{JitEntryState::COLD};
    uint64_t execution_count{0};
    uint64_t tuple_evaluation_count{0};
    uint64_t interpreted_ns{0};
    uint64_t last_seen{0};
    uint64_t last_compiled{0};
    uint64_t compile_ns{0};
    uint64_t cache_hits{0};
    uint64_t fallbacks{0};
    uint64_t compile_failures{0};
    std::chrono::steady_clock::time_point cooldown_until{};
    size_t code_bytes{0};
    std::shared_ptr<const JitCode> code;
};

struct JitManager::Impl {
    mutable std::mutex mutex;
    std::condition_variable queue_cv;
    std::condition_variable idle_cv;
    std::unordered_map<std::string, std::shared_ptr<Entry>> entries;
    std::deque<std::shared_ptr<Entry>> queue;
    std::thread worker;
    bool accepting{true};
    bool stop_worker{false};
    size_t active_execution_count{0};
    size_t compiling_count{0};
    uint64_t clock{0};
    size_t code_bytes{0};
    uint64_t cache_hits{0};
    uint64_t fallbacks{0};
    uint64_t compile_attempts{0};
    uint64_t compile_failures{0};
    uint64_t evictions{0};
};

JitManager::JitManager(JitManagerConfig config, CatalogGenerationFunction catalog_generation, CompileFunction compile)
    : config_(std::move(config)), catalog_generation_(std::move(catalog_generation)), compile_(std::move(compile)),
      impl_(std::make_unique<Impl>()) {
    config_.max_entries = std::max<size_t>(1, config_.max_entries);
    config_.max_code_bytes = std::max<size_t>(1, config_.max_code_bytes);
    config_.max_queue_size = std::max<size_t>(1, config_.max_queue_size);
    impl_->worker = std::thread(&JitManager::worker_loop, this);
}

JitManager::~JitManager() {
    shutdown_and_drain();
}

JitManager::ExecutionScope::~ExecutionScope() {
    reset();
}

JitManager::ExecutionScope::ExecutionScope(ExecutionScope&& other) noexcept : manager_(other.manager_) {
    other.manager_ = nullptr;
}

JitManager::ExecutionScope& JitManager::ExecutionScope::operator=(ExecutionScope&& other) noexcept {
    if (this != &other) {
        reset();
        manager_ = other.manager_;
        other.manager_ = nullptr;
    }
    return *this;
}

void JitManager::ExecutionScope::reset() {
    if (manager_ != nullptr) {
        manager_->leave_execution();
        manager_ = nullptr;
    }
}

JitManager::ExecutionScope JitManager::enter_execution() {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (!impl_->accepting) {
        return {};
    }
    ++impl_->active_execution_count;
    return ExecutionScope(this);
}

void JitManager::discard(const JitProgram& program) {
    std::shared_ptr<const JitCode> released_code;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        auto position = impl_->entries.find(program.key.canonical_bytes);
        if (position == impl_->entries.end() || position->second->program.key != program.key) {
            return;
        }
        auto entry = std::move(position->second);
        entry->state = JitEntryState::EVICTED;
        if (entry->code != nullptr) {
            released_code = std::move(entry->code);
            impl_->code_bytes -= entry->code_bytes;
        }
        impl_->entries.erase(position);
        ++impl_->evictions;
    }
}

void JitManager::leave_execution() {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    --impl_->active_execution_count;
    impl_->idle_cv.notify_all();
}

std::shared_ptr<const JitCode> JitManager::observe(const JitProgram& program, JitMode mode,
                                                   JitObservation observation) {
    if (mode == JitMode::OFF || !verify_program(program) || catalog_generation_() != program.catalog_generation) {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        ++impl_->fallbacks;
        return {};
    }

    std::shared_ptr<Entry> entry;
    bool compile_now = false;
    std::vector<std::shared_ptr<const JitCode>> released_code;
    {
        std::unique_lock<std::mutex> lock(impl_->mutex);
        if (!impl_->accepting) {
            ++impl_->fallbacks;
            return {};
        }
        const std::string& key = program.key.canonical_bytes;
        auto position = impl_->entries.find(key);
        if (position == impl_->entries.end()) {
            entry = std::make_shared<Entry>(program);
            entry->state = JitEntryState::OBSERVING;
            impl_->entries.emplace(key, entry);
            evict_locked(entry, &released_code);
        } else {
            entry = position->second;
        }
        entry->last_seen = ++impl_->clock;
        if (entry->program.catalog_generation != catalog_generation_()) {
            entry->state = JitEntryState::EVICTED;
            if (entry->code != nullptr) {
                released_code.push_back(std::move(entry->code));
                impl_->code_bytes -= entry->code_bytes;
                entry->code_bytes = 0;
            }
            impl_->entries.erase(key);
            ++impl_->fallbacks;
            return {};
        }
        if (entry->state == JitEntryState::READY && entry->code != nullptr) {
            ++entry->cache_hits;
            ++impl_->cache_hits;
            return entry->code;
        }

        ++entry->execution_count;
        entry->tuple_evaluation_count += observation.tuple_evaluations;
        entry->interpreted_ns += observation.interpreted_ns;
        const auto now = std::chrono::steady_clock::now();
        if (entry->state == JitEntryState::FAILED_COOLDOWN && now >= entry->cooldown_until) {
            entry->state = JitEntryState::OBSERVING;
        }
        if (entry->state == JitEntryState::OBSERVING) {
            if (mode == JitMode::FORCE) {
                entry->state = JitEntryState::COMPILING;
                ++impl_->compiling_count;
                compile_now = true;
            } else if (entry->execution_count >= config_.min_executions &&
                       entry->tuple_evaluation_count >= config_.min_tuple_evaluations &&
                       entry->interpreted_ns >= config_.min_interpreted_ns &&
                       impl_->queue.size() < config_.max_queue_size) {
                entry->state = JitEntryState::QUEUED;
                impl_->queue.push_back(entry);
                impl_->queue_cv.notify_one();
            }
        }
        ++entry->fallbacks;
        ++impl_->fallbacks;
    }
    released_code.clear();
    if (compile_now) {
        return compile_immediately(entry);
    }
    return {};
}

std::shared_ptr<const JitCode> JitManager::compile_immediately(const std::shared_ptr<Entry>& entry) {
    const auto started = std::chrono::steady_clock::now();
    JitCompileResult result = compile_(entry->program);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    publish_compile_result(entry, std::move(result), std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed));
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return entry->state == JitEntryState::READY ? entry->code : std::shared_ptr<const JitCode>{};
}

void JitManager::worker_loop() {
    while (true) {
        std::shared_ptr<Entry> entry;
        {
            std::unique_lock<std::mutex> lock(impl_->mutex);
            impl_->queue_cv.wait(lock, [&] { return impl_->stop_worker || !impl_->queue.empty(); });
            if (impl_->stop_worker) {
                return;
            }
            entry = impl_->queue.front();
            impl_->queue.pop_front();
            if (entry->state != JitEntryState::QUEUED) {
                continue;
            }
            entry->state = JitEntryState::COMPILING;
            ++impl_->compiling_count;
        }
        const auto started = std::chrono::steady_clock::now();
        JitCompileResult result = compile_(entry->program);
        const auto elapsed = std::chrono::steady_clock::now() - started;
        publish_compile_result(entry, std::move(result), std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed));
    }
}

void JitManager::publish_compile_result(const std::shared_ptr<Entry>& entry, JitCompileResult result,
                                        std::chrono::nanoseconds compile_time) {
    std::vector<std::shared_ptr<const JitCode>> released_code;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        ++impl_->compile_attempts;
        --impl_->compiling_count;
        entry->compile_ns = static_cast<uint64_t>(compile_time.count());
        entry->last_compiled = ++impl_->clock;
        const bool current_epoch = impl_->accepting && entry->program.catalog_generation == catalog_generation_();
        const auto position = impl_->entries.find(entry->program.key.canonical_bytes);
        const bool still_cached = position != impl_->entries.end() && position->second == entry;
        const bool fits_cache = result && result.code && result.code.code_size() <= config_.max_code_bytes;
        if (current_epoch && still_cached && fits_cache) {
            auto code = std::make_shared<JitCode>(std::move(result.code));
            entry->code_bytes = code->code_size();
            entry->code = std::move(code);
            entry->state = JitEntryState::READY;
            impl_->code_bytes += entry->code_bytes;
            evict_locked(entry, &released_code);
        } else {
            if (result.code) {
                released_code.push_back(std::make_shared<JitCode>(std::move(result.code)));
            }
            entry->state = current_epoch && still_cached ? JitEntryState::FAILED_COOLDOWN : JitEntryState::EVICTED;
            if (entry->state == JitEntryState::FAILED_COOLDOWN) {
                ++entry->compile_failures;
                ++impl_->compile_failures;
                entry->cooldown_until = std::chrono::steady_clock::now() + config_.failure_cooldown;
            }
        }
        impl_->idle_cv.notify_all();
    }
    released_code.clear();
}

void JitManager::evict_locked(const std::shared_ptr<Entry>& protected_entry,
                              std::vector<std::shared_ptr<const JitCode>>* released_code) {
    while (impl_->entries.size() > config_.max_entries || impl_->code_bytes > config_.max_code_bytes) {
        auto victim = impl_->entries.end();
        for (auto candidate = impl_->entries.begin(); candidate != impl_->entries.end(); ++candidate) {
            const auto& entry = candidate->second;
            if (entry == protected_entry || entry->state == JitEntryState::COMPILING ||
                entry->state == JitEntryState::QUEUED) {
                continue;
            }
            if (victim == impl_->entries.end() || entry->last_seen < victim->second->last_seen) {
                victim = candidate;
            }
        }
        if (victim == impl_->entries.end()) {
            return;
        }
        auto entry = victim->second;
        entry->state = JitEntryState::EVICTED;
        if (entry->code != nullptr) {
            released_code->push_back(std::move(entry->code));
            impl_->code_bytes -= entry->code_bytes;
            entry->code_bytes = 0;
        }
        impl_->entries.erase(victim);
        ++impl_->evictions;
    }
}

bool JitManager::wait_until_idle(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(impl_->mutex);
    return impl_->idle_cv.wait_for(lock, timeout, [&] { return impl_->queue.empty() && impl_->compiling_count == 0; });
}

void JitManager::shutdown_and_drain() {
    std::thread worker;
    std::vector<std::shared_ptr<const JitCode>> released_code;
    {
        std::unique_lock<std::mutex> lock(impl_->mutex);
        if (!impl_->accepting && impl_->stop_worker) {
            return;
        }
        impl_->accepting = false;
        impl_->stop_worker = true;
        for (const auto& entry : impl_->queue) {
            entry->state = JitEntryState::EVICTED;
        }
        impl_->queue.clear();
        impl_->queue_cv.notify_all();
        worker = std::move(impl_->worker);
    }
    if (worker.joinable()) {
        worker.join();
    }
    {
        std::unique_lock<std::mutex> lock(impl_->mutex);
        impl_->idle_cv.wait(lock, [&] { return impl_->active_execution_count == 0; });
        for (auto& [_, entry] : impl_->entries) {
            entry->state = JitEntryState::EVICTED;
            if (entry->code != nullptr) {
                released_code.push_back(std::move(entry->code));
            }
        }
        impl_->entries.clear();
        impl_->queue.clear();
        impl_->code_bytes = 0;
        impl_->idle_cv.notify_all();
    }
    released_code.clear();
}

JitManagerStats JitManager::stats() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return {impl_->entries.size(), impl_->queue.size(),     impl_->code_bytes,       impl_->cache_hits,
            impl_->fallbacks,      impl_->compile_attempts, impl_->compile_failures, impl_->evictions};
}

} // namespace jit
