/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "compiled/instruction.h"

namespace compiled {

inline constexpr uint32_t COMPILED_IR_VERSION = 2;
inline constexpr uint32_t COMPILED_ABI_VERSION = 1;

enum class ProgramKind : uint8_t { POINT_SELECT, POINT_UPDATE, POINT_DELETE, POINT_INSERT };

struct ParameterDesc {
    ValueType type{ValueType::INT32};
    uint32_t max_length{0};
    int32_t lexical_slot{-1};
};

struct ColumnDesc {
    ValueType type{ValueType::INT32};
    uint32_t offset{0};
    uint32_t width{0};
};

struct TupleLayout {
    uint32_t byte_size{0};
    std::vector<ColumnDesc> columns;
};

struct RegisterDesc {
    ValueType type{ValueType::INT32};
    uint32_t tuple_layout{kNoOperand};
    uint32_t max_length{0};
};

struct OutputColumnDesc {
    ValueType type{ValueType::INT32};
    uint32_t width{0};
};

struct OutputDesc {
    std::vector<OutputColumnDesc> columns;
};

class CompiledProgram {
public:
    CompiledProgram(uint32_t ir_version, uint32_t abi_version, ProgramKind kind, uint64_t catalog_generation,
                    std::vector<ParameterDesc> parameters, std::vector<RegisterDesc> registers,
                    std::vector<TupleLayout> tuple_layouts, std::vector<Instruction> instructions,
                    OutputDesc output = {})
        : ir_version_(ir_version), abi_version_(abi_version), kind_(kind), catalog_generation_(catalog_generation),
          parameters_(std::move(parameters)), registers_(std::move(registers)),
          tuple_layouts_(std::move(tuple_layouts)), instructions_(std::move(instructions)), output_(std::move(output)) {
    }

    uint32_t ir_version() const {
        return ir_version_;
    }
    uint32_t abi_version() const {
        return abi_version_;
    }
    ProgramKind kind() const {
        return kind_;
    }
    uint64_t catalog_generation() const {
        return catalog_generation_;
    }
    const std::vector<ParameterDesc>& parameters() const {
        return parameters_;
    }
    const std::vector<RegisterDesc>& registers() const {
        return registers_;
    }
    const std::vector<TupleLayout>& tuple_layouts() const {
        return tuple_layouts_;
    }
    const std::vector<Instruction>& instructions() const {
        return instructions_;
    }
    const OutputDesc& output() const {
        return output_;
    }

private:
    uint32_t ir_version_;
    uint32_t abi_version_;
    ProgramKind kind_;
    uint64_t catalog_generation_;
    std::vector<ParameterDesc> parameters_;
    std::vector<RegisterDesc> registers_;
    std::vector<TupleLayout> tuple_layouts_;
    std::vector<Instruction> instructions_;
    OutputDesc output_;
};

} // namespace compiled
