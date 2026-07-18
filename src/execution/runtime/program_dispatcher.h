/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstdint>

#include "compiled/program_cache.h"
#include "common/context.h"
#include "parser/token_stream.h"

class SmManager;
namespace jit {
class PointProgramJitManager;
}

enum class ProgramDispatchStatus { MISS, HANDLED, FALLBACK };

struct ProgramDispatchRequest {
    compiled::ProgramTemplateCache* cache{nullptr};
    const parser::OwnedTokenStream* lexical{nullptr};
    uint64_t statement_generation{0};
    uint64_t planner_generation{0};
    SmManager* sm_manager{nullptr};
    Context* context{nullptr};
    compiled::ProgramTemplatePtr program_template;
    jit::PointProgramJitManager* point_jit_manager{nullptr};
};

ProgramDispatchStatus DispatchCachedPointProgram(const ProgramDispatchRequest& request);
