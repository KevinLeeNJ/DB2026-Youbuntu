/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include <gtest/gtest.h>

#include "compiled/program_cache.h"

namespace {

std::shared_ptr<const compiled::CompiledProgram> Program(compiled::ProgramKind kind, uint64_t generation) {
    return std::make_shared<const compiled::CompiledProgram>(
        compiled::COMPILED_IR_VERSION, compiled::COMPILED_ABI_VERSION, kind, generation,
        std::vector<compiled::ParameterDesc>{}, std::vector<compiled::RegisterDesc>{},
        std::vector<compiled::TupleLayout>{},
        std::vector<compiled::Instruction>{{compiled::Opcode::HALT}});
}

compiled::ProgramCacheKey Key(std::string canonical, uint64_t statement_generation = 11,
                              uint64_t catalog_generation = 7,
                              compiled::ProgramKind kind = compiled::ProgramKind::POINT_SELECT) {
    return {parser::TokenShapeKey{1, 2, std::move(canonical)}, statement_generation, 3, catalog_generation, kind};
}

TEST(ProgramCacheTest, ReusesOnlyExactShapeAndGeneration) {
    compiled::ProgramCache cache;
    const auto key = Key("select ?");
    EXPECT_EQ(cache.Lookup(key), nullptr);

    auto program = Program(compiled::ProgramKind::POINT_SELECT, 7);
    EXPECT_EQ(cache.Publish(key, program), program);
    EXPECT_EQ(cache.Lookup(key), program);

    EXPECT_EQ(cache.Lookup(Key("select collision")), nullptr);
    EXPECT_EQ(cache.Lookup(Key("select ?", 12)), nullptr);
    EXPECT_EQ(cache.Lookup(Key("select ?", 11, 8)), nullptr);
    EXPECT_EQ(cache.Lookup(Key("select ?", 11, 7, compiled::ProgramKind::POINT_UPDATE)), nullptr);

    const auto stats = cache.Stats();
    EXPECT_EQ(stats.hits, 1U);
    EXPECT_EQ(stats.misses, 5U);
    EXPECT_EQ(stats.entries, 1U);
}

TEST(ProgramCacheTest, RejectsMismatchedProgramsAndTracksExecutionOutcomes) {
    compiled::ProgramCache cache(1);
    auto wrong_generation = Program(compiled::ProgramKind::POINT_INSERT, 9);
    EXPECT_EQ(cache.Publish(Key("insert ?", 11, 7, compiled::ProgramKind::POINT_INSERT), wrong_generation),
              wrong_generation);
    EXPECT_EQ(cache.Stats().entries, 0U);

    cache.Publish(Key("insert ?", 11, 7, compiled::ProgramKind::POINT_INSERT),
                  Program(compiled::ProgramKind::POINT_INSERT, 7));
    cache.Publish(Key("delete ?", 11, 7, compiled::ProgramKind::POINT_DELETE),
                  Program(compiled::ProgramKind::POINT_DELETE, 7));
    EXPECT_EQ(cache.Stats().entries, 1U);

    cache.RecordHandled();
    cache.RecordFallback();
    const auto stats = cache.Stats();
    EXPECT_EQ(stats.handled, 1U);
    EXPECT_EQ(stats.fallbacks, 1U);
}

TEST(ProgramCacheTest, EvictsLeastRecentlyUsedEntry) {
    compiled::ProgramCache cache(2);
    const auto first = Key("first");
    const auto second = Key("second");
    const auto third = Key("third");
    cache.Publish(first, Program(compiled::ProgramKind::POINT_SELECT, 7));
    cache.Publish(second, Program(compiled::ProgramKind::POINT_SELECT, 7));
    ASSERT_NE(cache.Lookup(first), nullptr);
    cache.Publish(third, Program(compiled::ProgramKind::POINT_SELECT, 7));

    EXPECT_EQ(cache.Lookup(second), nullptr);
    EXPECT_NE(cache.Lookup(first), nullptr);
    EXPECT_NE(cache.Lookup(third), nullptr);
}

} // namespace
