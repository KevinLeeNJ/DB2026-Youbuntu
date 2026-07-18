/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "compiled/compiled_program.h"
#include "parser/token_stream.h"

namespace compiled {

struct ProgramTemplateIdentity {
    parser::TokenShapeKey token_shape;
    uint64_t catalog_generation{0};
    uint64_t statement_generation{0};
    uint64_t planner_generation{0};
    ProgramKind kind{ProgramKind::POINT_SELECT};
};

// program_parameter is kNoOperand for literals used only by runtime
// revalidation metadata and not loaded by the bytecode itself.
struct LexicalParameterDesc {
    uint32_t program_parameter{kNoOperand};
    int32_t lexical_slot{-1};
    ValueType type{ValueType::INT32};
    uint32_t max_length{0};
};

struct TemplateColumnRef {
    std::string table_name;
    std::string column_name;
};

struct TemplateColumnDesc {
    std::string table_name;
    std::string column_name;
    ValueType type{ValueType::INT32};
    uint32_t offset{0};
    uint32_t width{0};
};

struct TemplateTableDesc {
    std::string table_name;
    uint32_t tuple_width{0};
    std::vector<TemplateColumnDesc> columns;
};

struct TemplateIndexDesc {
    std::string table_name;
    std::string index_name;
    std::vector<std::string> column_names;
    std::vector<uint32_t> tuple_offsets;
};

struct TemplateOutputColumnDesc {
    TemplateColumnDesc source;
    std::string caption;
};

struct TemplateConditionDesc {
    TemplateColumnRef lhs;
    CompareOp op{CompareOp::EQ};
    bool rhs_is_parameter{true};
    TemplateColumnRef rhs_column;
    int32_t rhs_lexical_slot{-1};
};

enum class TemplateSetOp : uint8_t { ASSIGNMENT, ADD, SUB, MUL, DIV };

struct TemplateSetDesc {
    TemplateColumnRef lhs;
    bool rhs_is_column{false};
    TemplateColumnRef rhs_column;
    TemplateSetOp op{TemplateSetOp::ASSIGNMENT};
    int32_t rhs_lexical_slot{-1};
};

struct ProgramBindingTemplate {
    TemplateTableDesc table;
    // Ordered by the index id used by MAKE_POINT_KEY.
    std::vector<TemplateIndexDesc> point_indexes;
    // Ordered like the live table indexes for row-mutation runtime metadata.
    std::vector<TemplateIndexDesc> mutation_indexes;
    std::vector<TemplateOutputColumnDesc> output_columns;
    std::vector<TemplateConditionDesc> conditions;
    std::vector<TemplateSetDesc> set_clauses;
    std::vector<int32_t> insert_value_slots;
    std::vector<bool> affected_mutation_indexes;
};

// ProgramTemplate owns only immutable, catalog-generation-scoped values. Live
// Context, transaction, file/index handles, ResultSink, and catalog pointers
// are deliberately reconstructed by the caller for every execution.
class ProgramTemplate final {
public:
    static std::shared_ptr<const ProgramTemplate> Create(ProgramTemplateIdentity identity,
                                                         std::shared_ptr<const CompiledProgram> program,
                                                         std::vector<LexicalParameterDesc> lexical_parameters,
                                                         ProgramBindingTemplate bindings, std::string* error = nullptr);

    const ProgramTemplateIdentity& identity() const {
        return identity_;
    }
    const CompiledProgram& program() const {
        return *program_;
    }
    const std::shared_ptr<const CompiledProgram>& program_ptr() const {
        return program_;
    }
    const std::vector<LexicalParameterDesc>& lexical_parameters() const {
        return lexical_parameters_;
    }
    const ProgramBindingTemplate& bindings() const {
        return bindings_;
    }

    bool Matches(const parser::TokenShapeKey& token_shape, uint64_t catalog_generation, uint64_t statement_generation,
                 uint64_t planner_generation, ProgramKind kind) const;
    const LexicalParameterDesc* FindLexicalParameter(int32_t lexical_slot) const noexcept;

private:
    ProgramTemplate(ProgramTemplateIdentity identity, std::shared_ptr<const CompiledProgram> program,
                    std::vector<LexicalParameterDesc> lexical_parameters, ProgramBindingTemplate bindings)
        : identity_(std::move(identity)), program_(std::move(program)),
          lexical_parameters_(std::move(lexical_parameters)), bindings_(std::move(bindings)) {}

    ProgramTemplateIdentity identity_;
    std::shared_ptr<const CompiledProgram> program_;
    std::vector<LexicalParameterDesc> lexical_parameters_;
    ProgramBindingTemplate bindings_;
};

using ProgramTemplatePtr = std::shared_ptr<const ProgramTemplate>;

} // namespace compiled
