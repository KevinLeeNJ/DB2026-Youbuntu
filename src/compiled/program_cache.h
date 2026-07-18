/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "compiled/compiled_program.h"
#include "compiled/program_template.h"
#include "parser/token_stream.h"

namespace compiled {

struct ProgramCacheKey {
    parser::TokenShapeKey shape;
    uint64_t statement_generation{0};
    uint64_t planner_generation{0};
    uint64_t catalog_generation{0};
    ProgramKind kind{ProgramKind::POINT_SELECT};

    friend bool operator==(const ProgramCacheKey& lhs, const ProgramCacheKey& rhs) {
        return lhs.shape == rhs.shape && lhs.statement_generation == rhs.statement_generation &&
               lhs.planner_generation == rhs.planner_generation &&
               lhs.catalog_generation == rhs.catalog_generation && lhs.kind == rhs.kind;
    }
};

struct ProgramCacheKeyHash {
    size_t operator()(const ProgramCacheKey& key) const noexcept;
};

struct ProgramCacheStats {
    uint64_t hits{0};
    uint64_t misses{0};
    uint64_t fallbacks{0};
    uint64_t handled{0};
    size_t entries{0};
};

class ProgramTemplateCache {
public:
    explicit ProgramTemplateCache(size_t capacity = 256) : capacity_(capacity) {}

    ProgramTemplatePtr Lookup(const ProgramCacheKey& key);
    ProgramTemplatePtr LookupAny(const parser::TokenShapeKey& shape, uint64_t statement_generation,
                                 uint64_t planner_generation, uint64_t catalog_generation);
    ProgramTemplatePtr Publish(const ProgramCacheKey& key, ProgramTemplatePtr program_template);
    void RecordFallback() noexcept;
    void RecordHandled() noexcept;
    ProgramCacheStats Stats() const;
    void Clear();

private:
    struct Entry {
        ProgramTemplatePtr program_template;
        uint64_t last_use{0};
    };

    const size_t capacity_;
    mutable std::mutex mutex_;
    std::unordered_map<ProgramCacheKey, Entry, ProgramCacheKeyHash> entries_;
    uint64_t clock_{0};
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
    std::atomic<uint64_t> fallbacks_{0};
    std::atomic<uint64_t> handled_{0};
};

} // namespace compiled
