/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <optional>
#include <vector>

#include "compiled/parameter_frame.h"
#include "common/common.h"
#include "execution_defs.h"
#include "parser/token_stream.h"

// Immutable description of the lexical parameters referenced by a prepared
// execution descriptor. The bound ParameterFrame is request-local.
class PreparedParameterLayout {
public:
    bool Register(const Value& value, uint32_t max_length = 0);
    std::optional<compiled::ParameterFrame> Bind(const parser::OwnedTokenStream& lexical) const;
    bool Apply(const compiled::ParameterFrame& frame, Value* value) const;

    size_t size() const noexcept {
        return descriptors_.size();
    }

private:
    std::vector<compiled::ParameterDesc> descriptors_;
    std::vector<bool> registered_;
};
