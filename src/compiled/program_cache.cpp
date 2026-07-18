/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "compiled/program_cache.h"

#include <algorithm>

#include "compiled/program_verifier.h"

namespace compiled {

size_t ProgramCacheKeyHash::operator()(const ProgramCacheKey& key) const noexcept {
    size_t hash = parser::TokenShapeKeyHash{}(key.shape);
    const auto mix = [&](uint64_t value) {
        hash ^= static_cast<size_t>(value) + 0x9e3779b97f4a7c15ULL + (hash << 6U) + (hash >> 2U);
    };
    mix(key.statement_generation);
    mix(key.planner_generation);
    mix(key.catalog_generation);
    mix(static_cast<uint64_t>(key.kind));
    return hash;
}

std::shared_ptr<const CompiledProgram> ProgramCache::Lookup(const ProgramCacheKey& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = entries_.find(key);
    if (found == entries_.end() || found->second.program == nullptr ||
        found->second.program->catalog_generation() != key.catalog_generation ||
        found->second.program->kind() != key.kind) {
        misses_.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    hits_.fetch_add(1, std::memory_order_relaxed);
    found->second.last_use = ++clock_;
    return found->second.program;
}

std::shared_ptr<const CompiledProgram> ProgramCache::Publish(const ProgramCacheKey& key,
                                                             std::shared_ptr<const CompiledProgram> program) {
    if (program == nullptr || program->catalog_generation() != key.catalog_generation || program->kind() != key.kind ||
        capacity_ == 0 || !VerifyProgram(*program)) {
        return program;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto [position, inserted] = entries_.emplace(key, Entry{program, ++clock_});
    if (!inserted) {
        position->second.last_use = ++clock_;
        return position->second.program;
    }
    while (entries_.size() > capacity_) {
        auto oldest = std::min_element(entries_.begin(), entries_.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.second.last_use < rhs.second.last_use;
        });
        entries_.erase(oldest);
    }
    return program;
}

void ProgramCache::RecordFallback() noexcept {
    fallbacks_.fetch_add(1, std::memory_order_relaxed);
}

void ProgramCache::RecordHandled() noexcept {
    handled_.fetch_add(1, std::memory_order_relaxed);
}

ProgramCacheStats ProgramCache::Stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return {hits_.load(std::memory_order_relaxed), misses_.load(std::memory_order_relaxed),
            fallbacks_.load(std::memory_order_relaxed), handled_.load(std::memory_order_relaxed), entries_.size()};
}

void ProgramCache::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.clear();
}

ProgramTemplatePtr ProgramTemplateCache::Lookup(const ProgramCacheKey& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = entries_.find(key);
    if (found == entries_.end() || found->second.program_template == nullptr ||
        !found->second.program_template->Matches(key.shape, key.catalog_generation, key.statement_generation,
                                                 key.planner_generation, key.kind)) {
        misses_.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    found->second.last_use = ++clock_;
    hits_.fetch_add(1, std::memory_order_relaxed);
    return found->second.program_template;
}

ProgramTemplatePtr ProgramTemplateCache::Publish(const ProgramCacheKey& key, ProgramTemplatePtr program_template) {
    if (program_template == nullptr || capacity_ == 0 ||
        !program_template->Matches(key.shape, key.catalog_generation, key.statement_generation,
                                   key.planner_generation, key.kind)) {
        return program_template;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto [position, inserted] = entries_.emplace(key, Entry{program_template, ++clock_});
    if (!inserted) {
        position->second.last_use = ++clock_;
        return position->second.program_template;
    }
    while (entries_.size() > capacity_) {
        auto oldest = std::min_element(entries_.begin(), entries_.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.second.last_use < rhs.second.last_use;
        });
        entries_.erase(oldest);
    }
    return program_template;
}

void ProgramTemplateCache::RecordFallback() noexcept {
    fallbacks_.fetch_add(1, std::memory_order_relaxed);
}

void ProgramTemplateCache::RecordHandled() noexcept {
    handled_.fetch_add(1, std::memory_order_relaxed);
}

ProgramCacheStats ProgramTemplateCache::Stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return {hits_.load(std::memory_order_relaxed), misses_.load(std::memory_order_relaxed),
            fallbacks_.load(std::memory_order_relaxed), handled_.load(std::memory_order_relaxed), entries_.size()};
}

void ProgramTemplateCache::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.clear();
}

} // namespace compiled
