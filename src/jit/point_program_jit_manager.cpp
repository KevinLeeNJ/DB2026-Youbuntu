/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "jit/point_program_jit_manager.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>

namespace jit {
namespace {

enum class EntryState { OBSERVING, QUEUED, COMPILING, READY, FAILED_COOLDOWN, EVICTED };

std::atomic<uint64_t> next_manager_instance_id{1};
constexpr uint32_t kRecentCodeRefreshInterval = 64;

void MixHash(size_t* hash, uint64_t value) noexcept {
    *hash ^= static_cast<size_t>(value) + 0x9e3779b97f4a7c15ULL + (*hash << 6U) + (*hash >> 2U);
}

void RegisterPerfMapSymbol(const JitCode& code, const PointProgramJitKey& key) noexcept {
#if defined(__linux__)
    try {
        std::ofstream output("/tmp/perf-" + std::to_string(getpid()) + ".map", std::ios::out | std::ios::app);
        if (!output.is_open()) {
            return;
        }
        output << std::hex << reinterpret_cast<uintptr_t>(code.entry_address()) << ' ' << code.code_size()
               << " rmdb_point_" << static_cast<uint32_t>(key.kind) << '_' << key.shape.high << key.shape.low << '\n';
    } catch (...) {
    }
#else
    (void)code;
    (void)key;
#endif
}

} // namespace

size_t PointProgramJitKeyHash::operator()(const PointProgramJitKey& key) const noexcept {
    size_t hash = parser::TokenShapeKeyHash{}(key.shape);
    MixHash(&hash, key.statement_generation);
    MixHash(&hash, key.planner_generation);
    MixHash(&hash, key.catalog_generation);
    MixHash(&hash, static_cast<uint64_t>(key.kind));
    MixHash(&hash, key.ir_version);
    MixHash(&hash, key.abi_version);
    MixHash(&hash, key.helper_abi_version);
    return hash;
}

PointProgramJitKey MakePointProgramJitKey(const compiled::ProgramTemplate& program_template) {
    const auto& identity = program_template.identity();
    const auto& program = program_template.program();
    return {identity.token_shape,
            identity.statement_generation,
            identity.planner_generation,
            identity.catalog_generation,
            identity.kind,
            program.ir_version(),
            program.abi_version(),
            POINT_PROGRAM_NATIVE_HELPER_ABI_VERSION};
}

struct PointProgramJitManager::Entry {
    explicit Entry(compiled::ProgramTemplatePtr value)
        : key(MakePointProgramJitKey(*value)), program_template(std::move(value)) {}

    PointProgramJitKey key;
    compiled::ProgramTemplatePtr program_template;
    EntryState state{EntryState::OBSERVING};
    uint64_t execution_count{0};
    uint64_t interpreted_ns{0};
    uint64_t last_use{0};
    std::chrono::steady_clock::time_point cooldown_until{};
    size_t code_bytes{0};
    std::shared_ptr<const JitCode> code;
};

struct PointProgramJitManager::Impl {
    mutable std::mutex mutex;
    std::condition_variable queue_cv;
    std::condition_variable idle_cv;
    std::unordered_map<PointProgramJitKey, std::shared_ptr<Entry>, PointProgramJitKeyHash> entries;
    std::deque<std::shared_ptr<Entry>> queue;
    std::thread worker;
    bool accepting{true};
    bool stop_worker{false};
    size_t compiling_count{0};
    size_t active_force_calls{0};
    uint64_t clock{0};
    size_t code_bytes{0};
    uint64_t interpreter_executions{0};
    std::atomic<uint64_t> native_executions{0};
    std::atomic<uint64_t> native_cache_hits{0};
    std::atomic<uint64_t> cache_epoch{1};
    uint64_t compile_attempts{0};
    uint64_t compile_failures{0};
    uint64_t compile_ns{0};
    uint64_t evictions{0};
};

PointProgramJitManager::PointProgramJitManager(PointProgramJitConfig config, IdentityCurrentFunction identity_current,
                                               CompileFunction compile)
    : config_(std::move(config)), identity_current_(std::move(identity_current)), compile_(std::move(compile)),
      instance_id_(next_manager_instance_id.fetch_add(1, std::memory_order_relaxed)), impl_(std::make_unique<Impl>()) {
    config_.max_entries = std::max<size_t>(1, config_.max_entries);
    config_.max_code_bytes = std::max<size_t>(1, config_.max_code_bytes);
    config_.max_queue_size = std::max<size_t>(1, config_.max_queue_size);
    config_.min_executions = std::max<uint64_t>(1, config_.min_executions);
    impl_->worker = std::thread(&PointProgramJitManager::WorkerLoop, this);
}

PointProgramJitManager::~PointProgramJitManager() {
    ShutdownAndDrain();
}

bool PointProgramJitManager::IdentityCurrent(const compiled::ProgramTemplateIdentity& identity) const noexcept {
    try {
        return identity_current_ && identity_current_(identity);
    } catch (...) {
        return false;
    }
}

JitCompileResult PointProgramJitManager::Compile(const compiled::ProgramTemplatePtr& program_template) const noexcept {
    try {
        if (!compile_) {
            return {JitStatus::COMPILE_ERROR, {}, "point-program compile callback is unavailable"};
        }
        return compile_(program_template);
    } catch (const std::exception& error) {
        return {JitStatus::COMPILE_ERROR, {}, std::string("point-program compile exception: ") + error.what()};
    } catch (...) {
        return {JitStatus::COMPILE_ERROR, {}, "unknown point-program compile exception"};
    }
}

void PointProgramJitManager::RemoveStale(const PointProgramJitKey& key) {
    std::shared_ptr<const JitCode> released_code;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        auto position = impl_->entries.find(key);
        if (position == impl_->entries.end() || position->second->state == EntryState::QUEUED ||
            position->second->state == EntryState::COMPILING) {
            return;
        }
        auto entry = position->second;
        entry->state = EntryState::EVICTED;
        if (entry->code != nullptr) {
            released_code = std::move(entry->code);
            impl_->code_bytes -= entry->code_bytes;
            entry->code_bytes = 0;
        }
        impl_->entries.erase(position);
        ++impl_->evictions;
        impl_->cache_epoch.fetch_add(1, std::memory_order_release);
    }
}

std::shared_ptr<const JitCode> PointProgramJitManager::Acquire(compiled::ProgramTemplatePtr program_template,
                                                               rmdb_config::JitMode mode) {
    if (mode == rmdb_config::JitMode::OFF || program_template == nullptr) {
        return {};
    }
    const PointProgramJitKey key = MakePointProgramJitKey(*program_template);
    if (!IdentityCurrent(program_template->identity())) {
        RemoveStale(key);
        return {};
    }

    std::shared_ptr<Entry> entry;
    bool compile_now = false;
    std::vector<std::shared_ptr<const JitCode>> released_code;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        if (!impl_->accepting) {
            return {};
        }
        auto position = impl_->entries.find(key);
        if (position == impl_->entries.end()) {
            entry = std::make_shared<Entry>(std::move(program_template));
            impl_->entries.emplace(entry->key, entry);
            EvictLocked(entry, &released_code);
        } else {
            entry = position->second;
        }
        entry->last_use = ++impl_->clock;
        if (entry->state == EntryState::READY && entry->code != nullptr) {
            impl_->native_cache_hits.fetch_add(1, std::memory_order_relaxed);
            return entry->code;
        }
        const auto now = std::chrono::steady_clock::now();
        if (entry->state == EntryState::FAILED_COOLDOWN && now >= entry->cooldown_until) {
            entry->state = EntryState::OBSERVING;
        }
        if (mode == rmdb_config::JitMode::FORCE && entry->state == EntryState::OBSERVING) {
            entry->state = EntryState::COMPILING;
            ++impl_->compiling_count;
            ++impl_->active_force_calls;
            compile_now = true;
        }
    }
    released_code.clear();
    if (!compile_now) {
        return {};
    }
    std::shared_ptr<const JitCode> code;
    try {
        code = CompileImmediately(entry);
    } catch (...) {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        --impl_->active_force_calls;
        impl_->idle_cv.notify_all();
        throw;
    }
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        --impl_->active_force_calls;
        impl_->idle_cv.notify_all();
    }
    return code;
}

std::shared_ptr<const JitCode> PointProgramJitManager::AcquireCurrent(compiled::ProgramTemplatePtr program_template,
                                                                      rmdb_config::JitMode mode,
                                                                      bool identity_current) {
    struct RecentCode {
        PointProgramJitManager* manager{nullptr};
        uint64_t manager_instance_id{0};
        uint64_t cache_epoch{0};
        rmdb_config::JitMode mode{rmdb_config::JitMode::OFF};
        uint32_t hits_until_refresh{0};
        // A live manager entry owns the template while this epoch is valid;
        // eviction/stale removal advances the epoch before releasing it.
        const compiled::ProgramTemplate* program_template{nullptr};
        std::weak_ptr<const JitCode> code;
    };
    thread_local RecentCode recent;

    if (mode == rmdb_config::JitMode::OFF || program_template == nullptr) {
        recent = {};
        return {};
    }
    if (!identity_current) {
        recent = {};
        if (!IdentityCurrent(program_template->identity())) {
            RemoveStale(MakePointProgramJitKey(*program_template));
        }
        return {};
    }

    const uint64_t epoch = impl_->cache_epoch.load(std::memory_order_acquire);
    if (recent.manager == this && recent.manager_instance_id == instance_id_ && recent.cache_epoch == epoch &&
        recent.mode == mode && recent.program_template == program_template.get()) {
        auto code = recent.code.lock();
        if (code != nullptr && --recent.hits_until_refresh != 0) {
            impl_->native_cache_hits.fetch_add(1, std::memory_order_relaxed);
            return code;
        }
    }

    const uint64_t epoch_before = epoch;
    auto code = Acquire(program_template, mode);
    const uint64_t epoch_after = impl_->cache_epoch.load(std::memory_order_acquire);
    if (code != nullptr && epoch_before == epoch_after) {
        recent = {this, instance_id_, epoch_after, mode, kRecentCodeRefreshInterval, program_template.get(), code};
    } else {
        recent = {};
    }
    return code;
}

void PointProgramJitManager::ObserveInterpreter(compiled::ProgramTemplatePtr program_template, uint64_t elapsed_ns,
                                                rmdb_config::JitMode mode) {
    if (mode == rmdb_config::JitMode::OFF || program_template == nullptr) {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        ++impl_->interpreter_executions;
        return;
    }
    const PointProgramJitKey key = MakePointProgramJitKey(*program_template);
    const bool current = IdentityCurrent(program_template->identity());
    if (!current) {
        {
            std::lock_guard<std::mutex> lock(impl_->mutex);
            ++impl_->interpreter_executions;
        }
        RemoveStale(key);
        return;
    }

    std::vector<std::shared_ptr<const JitCode>> released_code;
    std::lock_guard<std::mutex> lock(impl_->mutex);
    ++impl_->interpreter_executions;
    if (!impl_->accepting) {
        return;
    }

    auto position = impl_->entries.find(key);
    std::shared_ptr<Entry> entry;
    if (position == impl_->entries.end()) {
        entry = std::make_shared<Entry>(std::move(program_template));
        impl_->entries.emplace(entry->key, entry);
        EvictLocked(entry, &released_code);
    } else {
        entry = position->second;
    }
    entry->last_use = ++impl_->clock;
    ++entry->execution_count;
    entry->interpreted_ns += elapsed_ns;
    const auto now = std::chrono::steady_clock::now();
    if (entry->state == EntryState::FAILED_COOLDOWN && now >= entry->cooldown_until) {
        entry->state = EntryState::OBSERVING;
    }
    if (mode == rmdb_config::JitMode::AUTO && entry->state == EntryState::OBSERVING &&
        entry->execution_count >= config_.min_executions && entry->interpreted_ns >= config_.min_interpreted_ns &&
        impl_->queue.size() < config_.max_queue_size) {
        entry->state = EntryState::QUEUED;
        impl_->queue.push_back(entry);
        impl_->queue_cv.notify_one();
    }
}

void PointProgramJitManager::RecordNativeExecution() noexcept {
    // This counter is independent of entry lifetime and can be updated without
    // contending with the manager mutex on every native dispatch.
    impl_->native_executions.fetch_add(1, std::memory_order_relaxed);
}

std::shared_ptr<const JitCode> PointProgramJitManager::CompileImmediately(const std::shared_ptr<Entry>& entry) {
    const auto started = std::chrono::steady_clock::now();
    JitCompileResult result = Compile(entry->program_template);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    PublishCompileResult(entry, std::move(result),
                         static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()));
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return entry->state == EntryState::READY ? entry->code : std::shared_ptr<const JitCode>{};
}

void PointProgramJitManager::WorkerLoop() {
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
            if (entry->state != EntryState::QUEUED) {
                continue;
            }
            entry->state = EntryState::COMPILING;
            ++impl_->compiling_count;
        }
        const auto started = std::chrono::steady_clock::now();
        auto result = Compile(entry->program_template);
        const auto elapsed = std::chrono::steady_clock::now() - started;
        PublishCompileResult(
            entry, std::move(result),
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()));
    }
}

void PointProgramJitManager::PublishCompileResult(const std::shared_ptr<Entry>& entry, JitCompileResult result,
                                                  uint64_t compile_ns) {
    const bool identity_current = IdentityCurrent(entry->program_template->identity());
    std::vector<std::shared_ptr<const JitCode>> released_code;
    std::shared_ptr<const JitCode> registered_code;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        ++impl_->compile_attempts;
        impl_->compile_ns += compile_ns;
        --impl_->compiling_count;
        const bool current = impl_->accepting && identity_current;
        auto position = impl_->entries.find(entry->key);
        const bool still_cached = position != impl_->entries.end() && position->second == entry;
        const bool fits = result && result.code && result.code.code_size() <= config_.max_code_bytes;
        if (current && still_cached && fits) {
            auto code = std::make_shared<JitCode>(std::move(result.code));
            entry->code_bytes = code->code_size();
            entry->code = std::move(code);
            entry->state = EntryState::READY;
            impl_->code_bytes += entry->code_bytes;
            registered_code = entry->code;
            EvictLocked(entry, &released_code);
        } else if (current && still_cached) {
            if (result.code) {
                released_code.push_back(std::make_shared<JitCode>(std::move(result.code)));
            }
            entry->state = EntryState::FAILED_COOLDOWN;
            entry->cooldown_until = std::chrono::steady_clock::now() + config_.failure_cooldown;
            ++impl_->compile_failures;
            EvictLocked(entry, &released_code);
        } else {
            if (result.code) {
                released_code.push_back(std::make_shared<JitCode>(std::move(result.code)));
            }
            entry->state = EntryState::EVICTED;
            if (still_cached) {
                impl_->entries.erase(position);
            }
        }
        impl_->idle_cv.notify_all();
    }
    if (registered_code != nullptr) {
        RegisterPerfMapSymbol(*registered_code, entry->key);
    }
    released_code.clear();
}

void PointProgramJitManager::EvictLocked(const std::shared_ptr<Entry>& protected_entry,
                                         std::vector<std::shared_ptr<const JitCode>>* released_code) {
    while (impl_->entries.size() > config_.max_entries || impl_->code_bytes > config_.max_code_bytes) {
        auto victim = impl_->entries.end();
        for (auto candidate = impl_->entries.begin(); candidate != impl_->entries.end(); ++candidate) {
            const auto& entry = candidate->second;
            if (entry == protected_entry || entry->state == EntryState::QUEUED ||
                entry->state == EntryState::COMPILING) {
                continue;
            }
            if (victim == impl_->entries.end() || entry->last_use < victim->second->last_use) {
                victim = candidate;
            }
        }
        if (victim == impl_->entries.end()) {
            return;
        }
        auto entry = victim->second;
        entry->state = EntryState::EVICTED;
        if (entry->code != nullptr) {
            released_code->push_back(std::move(entry->code));
            impl_->code_bytes -= entry->code_bytes;
            entry->code_bytes = 0;
        }
        impl_->entries.erase(victim);
        ++impl_->evictions;
        impl_->cache_epoch.fetch_add(1, std::memory_order_release);
    }
}

bool PointProgramJitManager::WaitUntilIdle(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(impl_->mutex);
    return impl_->idle_cv.wait_for(lock, timeout, [&] { return impl_->queue.empty() && impl_->compiling_count == 0; });
}

void PointProgramJitManager::ShutdownAndDrain() {
    std::thread worker;
    std::vector<std::shared_ptr<const JitCode>> released_code;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        if (!impl_->accepting && impl_->stop_worker) {
            return;
        }
        impl_->accepting = false;
        impl_->stop_worker = true;
        impl_->cache_epoch.fetch_add(1, std::memory_order_release);
        for (const auto& entry : impl_->queue) {
            entry->state = EntryState::EVICTED;
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
        impl_->idle_cv.wait(lock, [&] { return impl_->compiling_count == 0 && impl_->active_force_calls == 0; });
        for (auto& [_, entry] : impl_->entries) {
            entry->state = EntryState::EVICTED;
            if (entry->code != nullptr) {
                released_code.push_back(std::move(entry->code));
            }
        }
        impl_->entries.clear();
        impl_->code_bytes = 0;
        impl_->idle_cv.notify_all();
    }
    released_code.clear();
}

PointProgramJitStats PointProgramJitManager::Stats() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return {impl_->entries.size(),
            impl_->queue.size(),
            impl_->code_bytes,
            impl_->interpreter_executions,
            impl_->native_executions.load(std::memory_order_relaxed),
            impl_->native_cache_hits.load(std::memory_order_relaxed),
            impl_->compile_attempts,
            impl_->compile_failures,
            impl_->compile_ns,
            impl_->evictions};
}

} // namespace jit
