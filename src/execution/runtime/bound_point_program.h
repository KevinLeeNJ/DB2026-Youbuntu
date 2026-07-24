/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "execution/row_mutation.h"

struct PointIndexRuntimeBinding {
    std::string table_name;
    std::vector<std::string> index_col_names;
    std::vector<uint32_t> tuple_offsets;
    // Set by cached-program dispatch after catalog generation and index shape
    // validation. Empty keeps direct runtime callers on the compatibility path.
    std::string index_name{};
    const IndexMeta* meta{nullptr};
};

// Catalog-scoped metadata shared by cached point-program executions.  It may
// contain pointers into the live catalog, but only for the catalog generation
// recorded below.  Transaction, output-sink, and parameter-dependent state
// remains request-local.
struct BoundPointProgram {
    SmManager* owner{nullptr};
    uint64_t catalog_generation{0};
    std::string table_name;
    TabMeta* table{nullptr};
    RmFileHandle* fh{nullptr};

    std::vector<PointIndexRuntimeBinding> point_indexes;
    std::vector<RowMutationIndex> mutation_indexes;
    std::vector<ColMeta> output_cols;
    std::vector<std::string> captions;
    std::vector<bool> affected_indexes;
};

using BoundPointProgramPtr = std::shared_ptr<const BoundPointProgram>;
