#pragma once

#include "epoch_si_engine.h"

#include <algorithm>
#include <cstdint>
#include <string>

namespace epoch_si_poc::test_row {

inline RowImage Make(TableId table_id, std::string key, int64_t value) {
    RowImage row;
    row.bytes.assign(key.begin(), key.end());
    row.bytes.push_back(0);
    for (size_t i = 0; i < sizeof(value); ++i)
        row.bytes.push_back(static_cast<uint8_t>(static_cast<uint64_t>(value) >> (8 * i)));
    row.claims = {{1000U + table_id, std::vector<uint8_t>(key.begin(), key.end())}};
    return row;
}

inline std::string Key(const RowImage& row) {
    if (!row.claims.empty())
        return {row.claims.front().bytes.begin(), row.claims.front().bytes.end()};
    const auto end = std::find(row.bytes.begin(), row.bytes.end(), 0);
    return {row.bytes.begin(), end};
}

inline RowImage Random(TableId table_id, std::string claim_key, uint64_t value, uint64_t salt) {
    RowImage row;
    row.bytes.resize(13);
    for (size_t i = 0; i < row.bytes.size(); ++i)
        row.bytes[i] = static_cast<uint8_t>((value >> ((i % 8) * 8)) ^ (salt >> ((7 - i % 8) * 8)) ^ i);
    row.bytes[3] = 0;
    row.claims = {{1000U + table_id, std::vector<uint8_t>(claim_key.begin(), claim_key.end())}};
    return row;
}

inline int64_t Value(const RowImage& row) {
    const std::string key = Key(row);
    if (row.bytes.size() != key.size() + 9)
        throw std::runtime_error("unexpected test row image");
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(value); ++i)
        value |= static_cast<uint64_t>(row.bytes[key.size() + 1 + i]) << (8 * i);
    return static_cast<int64_t>(value);
}

} // namespace epoch_si_poc::test_row
