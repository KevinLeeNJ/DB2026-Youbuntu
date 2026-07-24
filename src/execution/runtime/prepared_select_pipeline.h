/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include "execution/prepared_select_descriptor.h"

class Context;
class SmManager;

namespace prepared_select_pipeline {

enum class RunStatus { FALLBACK, HANDLED };

// The pipeline is deliberately opt-in while its result and performance
// characteristics are being validated against the executor-tree fallback.
bool Enabled() noexcept;

// Phase 5A only covers IndexRange + Filter + column Projection.
bool IsEligible(const PreparedSelectDescriptor& descriptor) noexcept;

// FALLBACK is returned only before any output is staged. Once execution starts,
// errors propagate as exceptions and cannot request a second executor path.
RunStatus Run(const PreparedSelectDescriptor& descriptor, const parser::OwnedTokenStream& lexical,
              SmManager* sm_manager, Context* context);

} // namespace prepared_select_pipeline
