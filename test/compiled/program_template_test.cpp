/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "compiled/program_template.h"

namespace {

using compiled::ProgramKind;
using compiled::ValueType;

std::shared_ptr<const compiled::CompiledProgram> Program(ProgramKind kind, uint64_t generation,
                                                         std::vector<compiled::ParameterDesc> parameters) {
    return std::make_shared<const compiled::CompiledProgram>(
        compiled::COMPILED_IR_VERSION, compiled::COMPILED_ABI_VERSION, kind, generation, std::move(parameters),
        std::vector<compiled::RegisterDesc>{}, std::vector<compiled::TupleLayout>{},
        std::vector<compiled::Instruction>{{compiled::Opcode::HALT}});
}

compiled::ProgramTemplateIdentity Identity(ProgramKind kind, uint64_t generation = 7) {
    return {parser::TokenShapeKey{11, 12, "point ?"}, generation, 17, 19, kind};
}

compiled::TemplateTableDesc Table() {
    return {"accounts",
            16,
            {{"accounts", "id", ValueType::INT32, 0, 4},
             {"accounts", "balance", ValueType::INT32, 4, 4},
             {"accounts", "name", ValueType::BYTES, 8, 8}}};
}

compiled::TemplateIndexDesc IdIndex() {
    return {"accounts", "accounts_id", {"id"}, {0}};
}

compiled::TemplateConditionDesc ParameterCondition(std::string column, int32_t lexical_slot) {
    return {{"accounts", std::move(column)}, compiled::CompareOp::EQ, true, {}, lexical_slot};
}

compiled::ProgramBindingTemplate SelectBindings() {
    compiled::ProgramBindingTemplate bindings;
    bindings.table = Table();
    bindings.point_indexes.push_back(IdIndex());
    bindings.output_columns.push_back({bindings.table.columns[0], "id"});
    bindings.conditions.push_back(ParameterCondition("id", 0));
    return bindings;
}

TEST(ProgramTemplateTest, OwnsImmutableSelectMetadataAndMatchesEveryGeneration) {
    static_assert(std::is_same_v<decltype(std::declval<const compiled::ProgramTemplate&>().program()),
                                 const compiled::CompiledProgram&>);
    static_assert(std::is_same_v<decltype(std::declval<const compiled::ProgramTemplate&>().bindings()),
                                 const compiled::ProgramBindingTemplate&>);

    auto bindings = SelectBindings();
    std::string error;
    auto result = compiled::ProgramTemplate::Create(Identity(ProgramKind::POINT_SELECT),
                                                    Program(ProgramKind::POINT_SELECT, 7, {{ValueType::INT32, 0, 0}}),
                                                    {{0, 0, ValueType::INT32, 0}}, bindings, &error);
    ASSERT_NE(result, nullptr) << error;

    bindings.table.table_name = "mutated";
    bindings.output_columns[0].caption = "mutated";
    EXPECT_EQ(result->bindings().table.table_name, "accounts");
    EXPECT_EQ(result->bindings().output_columns[0].caption, "id");
    EXPECT_EQ(result->FindLexicalParameter(0)->program_parameter, 0U);
    EXPECT_EQ(result->FindLexicalParameter(99), nullptr);
    EXPECT_TRUE(result->Matches({11, 12, "point ?"}, 7, 17, 19, ProgramKind::POINT_SELECT));
    EXPECT_FALSE(result->Matches({11, 12, "point ?"}, 7, 17, 20, ProgramKind::POINT_SELECT));
    EXPECT_FALSE(result->Matches({11, 12, "point ?"}, 8, 17, 19, ProgramKind::POINT_SELECT));
}

TEST(ProgramTemplateTest, RetainsRuntimeOnlyResidualSlotsForUpdateRebinding) {
    compiled::ProgramBindingTemplate bindings;
    bindings.table = Table();
    bindings.point_indexes.push_back(IdIndex());
    bindings.mutation_indexes.push_back(IdIndex());
    bindings.conditions.push_back(ParameterCondition("id", 0));
    bindings.conditions.push_back(ParameterCondition("name", 2));
    bindings.set_clauses.push_back({{"accounts", "balance"}, false, {}, compiled::TemplateSetOp::ASSIGNMENT, 1});
    bindings.affected_mutation_indexes = {false};

    std::string error;
    auto result = compiled::ProgramTemplate::Create(
        Identity(ProgramKind::POINT_UPDATE),
        Program(ProgramKind::POINT_UPDATE, 7, {{ValueType::INT32, 0, 0}, {ValueType::INT32, 0, 1}}),
        {{0, 0, ValueType::INT32, 0}, {1, 1, ValueType::INT32, 0}, {compiled::kNoOperand, 2, ValueType::BYTES, 8}},
        std::move(bindings), &error);
    ASSERT_NE(result, nullptr) << error;
    const auto* residual = result->FindLexicalParameter(2);
    ASSERT_NE(residual, nullptr);
    EXPECT_EQ(residual->program_parameter, compiled::kNoOperand);
    EXPECT_EQ(result->bindings().conditions[1].rhs_lexical_slot, 2);
    EXPECT_EQ(result->bindings().set_clauses[0].rhs_lexical_slot, 1);
}

TEST(ProgramTemplateTest, CoversDeleteAndInsertRuntimeBindingMetadata) {
    compiled::ProgramBindingTemplate deletion;
    deletion.table = Table();
    deletion.point_indexes.push_back(IdIndex());
    deletion.mutation_indexes.push_back(IdIndex());
    deletion.conditions.push_back(ParameterCondition("id", 0));
    std::string error;
    auto delete_template = compiled::ProgramTemplate::Create(
        Identity(ProgramKind::POINT_DELETE), Program(ProgramKind::POINT_DELETE, 7, {{ValueType::INT32, 0, 0}}),
        {{0, 0, ValueType::INT32, 0}}, std::move(deletion), &error);
    ASSERT_NE(delete_template, nullptr) << error;
    EXPECT_EQ(delete_template->bindings().mutation_indexes[0].index_name, "accounts_id");

    compiled::ProgramBindingTemplate insertion;
    insertion.table = Table();
    insertion.mutation_indexes.push_back(IdIndex());
    insertion.insert_value_slots = {0, 1, 2};
    auto insert_template = compiled::ProgramTemplate::Create(
        Identity(ProgramKind::POINT_INSERT),
        Program(ProgramKind::POINT_INSERT, 7,
                {{ValueType::INT32, 0, 0}, {ValueType::INT32, 0, 1}, {ValueType::BYTES, 8, 2}}),
        {{0, 0, ValueType::INT32, 0}, {1, 1, ValueType::INT32, 0}, {2, 2, ValueType::BYTES, 8}}, std::move(insertion),
        &error);
    ASSERT_NE(insert_template, nullptr) << error;
    EXPECT_EQ(insert_template->bindings().insert_value_slots, (std::vector<int32_t>{0, 1, 2}));
}

TEST(ProgramTemplateTest, RejectsStaleIdentityAndMalformedSlotOrBindingMetadata) {
    std::string error;
    EXPECT_EQ(compiled::ProgramTemplate::Create(Identity(ProgramKind::POINT_SELECT, 8),
                                                Program(ProgramKind::POINT_SELECT, 7, {{ValueType::INT32, 0, 0}}),
                                                {{0, 0, ValueType::INT32, 0}}, SelectBindings(), &error),
              nullptr);
    EXPECT_NE(error.find("identity"), std::string::npos);

    EXPECT_EQ(compiled::ProgramTemplate::Create(Identity(ProgramKind::POINT_SELECT),
                                                Program(ProgramKind::POINT_SELECT, 7, {{ValueType::INT32, 0, 0}}),
                                                {{0, 1, ValueType::INT32, 0}}, SelectBindings(), &error),
              nullptr);
    EXPECT_NE(error.find("does not match"), std::string::npos);

    auto bad_bindings = SelectBindings();
    bad_bindings.point_indexes[0].tuple_offsets[0] = 4;
    EXPECT_EQ(compiled::ProgramTemplate::Create(Identity(ProgramKind::POINT_SELECT),
                                                Program(ProgramKind::POINT_SELECT, 7, {{ValueType::INT32, 0, 0}}),
                                                {{0, 0, ValueType::INT32, 0}}, std::move(bad_bindings), &error),
              nullptr);
    EXPECT_NE(error.find("index"), std::string::npos);
}

} // namespace
