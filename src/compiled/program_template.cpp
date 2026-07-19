/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "compiled/program_template.h"

#include <algorithm>
#include <unordered_set>

#include "compiled/program_verifier.h"

namespace compiled {
namespace {

bool IsScalar(ValueType type) {
    return type == ValueType::INT32 || type == ValueType::FLOAT64 || type == ValueType::BOOL ||
           type == ValueType::BYTES;
}

bool ValidParameterShape(ValueType type, uint32_t max_length) {
    return IsScalar(type) && ((type == ValueType::BYTES) == (max_length != 0));
}

const TemplateColumnDesc* FindColumn(const TemplateTableDesc& table, const TemplateColumnRef& ref) {
    if (ref.table_name != table.table_name) {
        return nullptr;
    }
    auto found = std::find_if(table.columns.begin(), table.columns.end(),
                              [&](const TemplateColumnDesc& column) { return column.column_name == ref.column_name; });
    return found == table.columns.end() ? nullptr : &*found;
}

bool SameColumn(const TemplateColumnDesc& lhs, const TemplateColumnDesc& rhs) {
    return lhs.table_name == rhs.table_name && lhs.column_name == rhs.column_name && lhs.type == rhs.type &&
           lhs.offset == rhs.offset && lhs.width == rhs.width;
}

std::shared_ptr<const ProgramTemplate> Fail(std::string message, std::string* error) {
    if (error != nullptr) {
        *error = std::move(message);
    }
    return nullptr;
}

} // namespace

std::shared_ptr<const ProgramTemplate> ProgramTemplate::Create(ProgramTemplateIdentity identity,
                                                               std::shared_ptr<const CompiledProgram> program,
                                                               std::vector<LexicalParameterDesc> lexical_parameters,
                                                               ProgramBindingTemplate bindings, std::string* error) {
    if (program == nullptr) {
        return Fail("compiled program template has no program", error);
    }
    if (identity.token_shape.canonical_bytes.empty()) {
        return Fail("compiled program template has no canonical token shape", error);
    }
    if (identity.kind != program->kind() || identity.catalog_generation != program->catalog_generation()) {
        return Fail("compiled program template identity does not match its program", error);
    }
    const VerifyResult verification = VerifyProgram(*program);
    if (!verification) {
        return Fail("invalid compiled program: " + verification.error, error);
    }

    std::unordered_set<int32_t> lexical_slots;
    std::vector<bool> mapped_program_parameters(program->parameters().size(), false);
    for (const auto& parameter : lexical_parameters) {
        if (parameter.lexical_slot < 0 || static_cast<size_t>(parameter.lexical_slot) >= lexical_parameters.size() ||
            !lexical_slots.insert(parameter.lexical_slot).second ||
            !ValidParameterShape(parameter.type, parameter.max_length)) {
            return Fail("invalid or duplicate lexical parameter descriptor", error);
        }
        if (parameter.program_parameter == kNoOperand) {
            continue;
        }
        if (parameter.program_parameter >= program->parameters().size() ||
            mapped_program_parameters[parameter.program_parameter]) {
            return Fail("invalid or duplicate program parameter mapping", error);
        }
        const auto& compiled_parameter = program->parameters()[parameter.program_parameter];
        if (compiled_parameter.lexical_slot != parameter.lexical_slot || compiled_parameter.type != parameter.type ||
            compiled_parameter.max_length != parameter.max_length) {
            return Fail("lexical parameter descriptor does not match its program parameter", error);
        }
        mapped_program_parameters[parameter.program_parameter] = true;
    }
    if (std::find(mapped_program_parameters.begin(), mapped_program_parameters.end(), false) !=
        mapped_program_parameters.end()) {
        return Fail("compiled program parameter has no lexical slot mapping", error);
    }

    const auto has_slot = [&](int32_t slot) { return lexical_slots.find(slot) != lexical_slots.end(); };
    const auto& table = bindings.table;
    if (table.table_name.empty() || table.tuple_width == 0 || table.columns.empty()) {
        return Fail("compiled program template has an invalid table descriptor", error);
    }
    std::unordered_set<std::string> column_names;
    for (const auto& column : table.columns) {
        if (column.table_name != table.table_name || column.column_name.empty() || !IsScalar(column.type) ||
            column.width == 0 || column.offset > table.tuple_width ||
            column.width > table.tuple_width - column.offset || !column_names.insert(column.column_name).second) {
            return Fail("compiled program template has an invalid table column", error);
        }
    }

    const auto validate_indexes = [&](const std::vector<TemplateIndexDesc>& indexes) {
        for (const auto& index : indexes) {
            if (index.table_name != table.table_name || index.index_name.empty() || index.column_names.empty() ||
                index.column_names.size() != index.tuple_offsets.size()) {
                return false;
            }
            for (size_t i = 0; i < index.column_names.size(); ++i) {
                TemplateColumnRef ref{table.table_name, index.column_names[i]};
                const auto* column = FindColumn(table, ref);
                if (column == nullptr || column->offset != index.tuple_offsets[i]) {
                    return false;
                }
            }
        }
        return true;
    };
    if (!validate_indexes(bindings.point_indexes) || !validate_indexes(bindings.mutation_indexes)) {
        return Fail("compiled program template has an invalid index descriptor", error);
    }

    for (const auto& output : bindings.output_columns) {
        TemplateColumnRef ref{output.source.table_name, output.source.column_name};
        const auto* source = FindColumn(table, ref);
        if (source == nullptr || !SameColumn(*source, output.source) || output.caption.empty()) {
            return Fail("compiled program template has an invalid output descriptor", error);
        }
    }
    for (const auto& condition : bindings.conditions) {
        if (FindColumn(table, condition.lhs) == nullptr) {
            return Fail("compiled program template condition has an invalid lhs column", error);
        }
        if (condition.rhs_is_parameter) {
            if (!has_slot(condition.rhs_lexical_slot) || !condition.rhs_column.table_name.empty() ||
                !condition.rhs_column.column_name.empty()) {
                return Fail("compiled program template condition has an invalid rhs parameter", error);
            }
        } else if (condition.rhs_lexical_slot >= 0 || FindColumn(table, condition.rhs_column) == nullptr) {
            return Fail("compiled program template condition has an invalid rhs column", error);
        }
    }
    for (const auto& set_clause : bindings.set_clauses) {
        if (FindColumn(table, set_clause.lhs) == nullptr) {
            return Fail("compiled program template SET clause has an invalid lhs column", error);
        }
        if (set_clause.rhs_is_column) {
            if (FindColumn(table, set_clause.rhs_column) == nullptr ||
                (set_clause.op != TemplateSetOp::ASSIGNMENT && !has_slot(set_clause.rhs_lexical_slot)) ||
                (set_clause.op == TemplateSetOp::ASSIGNMENT && set_clause.rhs_lexical_slot >= 0)) {
                return Fail("compiled program template SET clause has invalid column metadata", error);
            }
        } else if (set_clause.op != TemplateSetOp::ASSIGNMENT || !has_slot(set_clause.rhs_lexical_slot) ||
                   !set_clause.rhs_column.table_name.empty() || !set_clause.rhs_column.column_name.empty()) {
            return Fail("compiled program template SET clause has invalid parameter metadata", error);
        }
    }
    if (std::any_of(bindings.insert_value_slots.begin(), bindings.insert_value_slots.end(),
                    [&](int32_t slot) { return !has_slot(slot); })) {
        return Fail("compiled program template INSERT value has no lexical descriptor", error);
    }

    switch (identity.kind) {
    case ProgramKind::POINT_SELECT:
        if (bindings.point_indexes.empty() || bindings.output_columns.empty() || bindings.conditions.empty() ||
            !bindings.mutation_indexes.empty() || !bindings.set_clauses.empty() ||
            !bindings.insert_value_slots.empty() || !bindings.affected_mutation_indexes.empty()) {
            return Fail("point SELECT template has incompatible binding metadata", error);
        }
        break;
    case ProgramKind::POINT_UPDATE:
        if (bindings.point_indexes.empty() || bindings.conditions.empty() || bindings.set_clauses.empty() ||
            !bindings.output_columns.empty() || !bindings.insert_value_slots.empty() ||
            bindings.affected_mutation_indexes.size() != bindings.mutation_indexes.size()) {
            return Fail("point UPDATE template has incompatible binding metadata", error);
        }
        break;
    case ProgramKind::POINT_DELETE:
        if (bindings.point_indexes.empty() || bindings.conditions.empty() || !bindings.output_columns.empty() ||
            !bindings.set_clauses.empty() || !bindings.insert_value_slots.empty() ||
            !bindings.affected_mutation_indexes.empty()) {
            return Fail("point DELETE template has incompatible binding metadata", error);
        }
        break;
    case ProgramKind::POINT_INSERT:
        if (!bindings.point_indexes.empty() || !bindings.output_columns.empty() || !bindings.conditions.empty() ||
            !bindings.set_clauses.empty() || !bindings.affected_mutation_indexes.empty() ||
            bindings.insert_value_slots.size() != table.columns.size()) {
            return Fail("point INSERT template has incompatible binding metadata", error);
        }
        break;
    }

    if (error != nullptr) {
        error->clear();
    }
    return std::shared_ptr<const ProgramTemplate>(new ProgramTemplate(
        std::move(identity), std::move(program), std::move(lexical_parameters), std::move(bindings)));
}

bool ProgramTemplate::Matches(const parser::TokenShapeKey& token_shape, uint64_t catalog_generation,
                              uint64_t statement_generation, uint64_t planner_generation, ProgramKind kind) const {
    return identity_.token_shape == token_shape && identity_.catalog_generation == catalog_generation &&
           identity_.statement_generation == statement_generation &&
           identity_.planner_generation == planner_generation && identity_.kind == kind;
}

const LexicalParameterDesc* ProgramTemplate::FindLexicalParameter(int32_t lexical_slot) const noexcept {
    if (lexical_slot < 0 || static_cast<size_t>(lexical_slot) >= lexical_slot_descriptors_.size()) {
        return nullptr;
    }
    const int32_t descriptor = lexical_slot_descriptors_[static_cast<size_t>(lexical_slot)];
    return descriptor < 0 ? nullptr : &lexical_parameters_[static_cast<size_t>(descriptor)];
}

const LexicalParameterDesc* ProgramTemplate::FindProgramParameter(uint32_t program_parameter) const noexcept {
    if (program_parameter >= program_parameter_descriptors_.size()) {
        return nullptr;
    }
    const int32_t descriptor = program_parameter_descriptors_[program_parameter];
    return descriptor < 0 ? nullptr : &lexical_parameters_[static_cast<size_t>(descriptor)];
}

} // namespace compiled
