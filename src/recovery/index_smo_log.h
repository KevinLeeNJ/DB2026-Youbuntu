/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "recovery/log_manager.h"
#include "recovery/wal_reader.h"
#include "storage/index_smo_wal.h"

static constexpr uint32_t INDEX_SMO_MAGIC = 0x4958534dU;  // "IXSM"
static constexpr uint32_t INDEX_BIND_MAGIC = 0x49584244U; // "IXBD"
static constexpr uint16_t INDEX_SMO_VERSION_V1 = 1;
static constexpr uint16_t INDEX_SMO_VERSION_V2 = 2;
static constexpr uint16_t INDEX_SMO_VERSION_V3 = 3;
static constexpr uint16_t INDEX_BIND_VERSION_V1 = 1;
static constexpr uint16_t INDEX_BIND_VERSION_V2 = 2;
// Kept as an alias for code that emits the original renew-binding format.
static constexpr uint16_t INDEX_BIND_VERSION = INDEX_BIND_VERSION_V1;
static constexpr uint16_t INDEX_SMO_FLAG_HEADER_IMAGE = 1U;

uint32_t IndexSmoCrc32(const char* bytes, size_t length);

class IndexSmoLogRecord final : public LogRecord {
public:
    explicit IndexSmoLogRecord(const IndexSmoWalData& data);

    void serialize(char* dest) const override;

private:
    std::vector<char> payload_;
};

class IndexBindLogRecord final : public LogRecord {
public:
    explicit IndexBindLogRecord(std::string_view index_file_name);
    IndexBindLogRecord(std::string_view index_file_name, uint64_t existing_generation);
    void serialize(char* dest) const override;

private:
    std::string index_file_name_;
    uint16_t version_{INDEX_BIND_VERSION_V1};
    uint64_t explicit_generation_{0};
};

struct IndexSmoWalView {
    struct Page {
        page_id_t page_no{INVALID_PAGE_ID};
        const char* image{nullptr};
    };

    IndexSmoWalView() = default;
    IndexSmoWalView(const IndexSmoWalView&) = delete;
    IndexSmoWalView& operator=(const IndexSmoWalView&) = delete;
    IndexSmoWalView(IndexSmoWalView&&) noexcept = default;
    IndexSmoWalView& operator=(IndexSmoWalView&&) noexcept = default;

    std::string_view index_file_name;
    uint64_t index_generation{0};
    uint32_t page_count{0};
    const char* header_image{nullptr};

    page_id_t page_no(uint32_t index) const;
    const char* page_image(uint32_t index) const;

private:
    friend bool ParseIndexSmoWal(const WalRecordView& record, IndexSmoWalView* out);

    std::vector<Page> pages_;
    std::vector<std::array<char, PAGE_SIZE>> decoded_pages_;
};

struct IndexSmoWalDecodeStorage;
struct IndexSmoWalDecodedView;

// An allocation-free, read-only inspection result. Its private fields are an
// opaque capability: callers may use its counts for arena budgeting but cannot
// forge or alter the offsets trusted by Parse's same-lifetime fast path.
class IndexSmoWalLayout {
public:
    uint16_t version() const noexcept { return version_; }
    uint32_t total_len() const noexcept { return total_len_; }
    uint32_t page_count() const noexcept { return page_count_; }
    uint32_t decoded_count() const noexcept { return decoded_count_; }
    uint32_t transformed_count() const noexcept { return transformed_count_; }
    uint64_t generation() const noexcept { return generation_; }

private:
    friend bool InspectIndexSmoWal(const WalRecordView& record, IndexSmoWalLayout* out) noexcept;
    friend bool DecodeIndexSmoWal(const WalRecordView& record, const IndexSmoWalDecodeStorage& storage,
                                  IndexSmoWalDecodedView* out) noexcept;
    friend bool CopyIndexSmoPageCatalog(const WalRecordView& record, IndexSmoWalView::Page* pages,
                                        size_t page_capacity) noexcept;
    friend bool ParseIndexSmoWal(const WalRecordView& record, IndexSmoWalView* out);

    uint16_t version_{0};
    uint32_t total_len_{0};
    uint32_t payload_limit_{0};
    uint32_t name_offset_{0};
    uint32_t name_bytes_{0};
    uint32_t page_stream_offset_{0};
    uint32_t header_offset_{0};
    uint32_t page_count_{0};
    uint32_t decoded_count_{0};
    uint32_t transformed_count_{0};
    uint64_t generation_{0};
    int32_t col_tot_len_{0};
    int32_t btree_order_{0};
    int32_t keys_size_{0};

    // The caller is a friend that has just established byte immutability.
    // This avoids exposing a forgeable trusted-layout API.
    bool decode_materialized(const WalRecordView& record, const IndexSmoWalDecodeStorage& storage,
                             IndexSmoWalDecodedView* out) const noexcept;
    bool copy_catalog(const WalRecordView& record, IndexSmoWalView::Page* pages, size_t page_capacity) const noexcept;
};

struct IndexSmoWalDecodeStorage {
    IndexSmoWalView::Page* pages{nullptr};
    size_t page_capacity{0};
    std::array<char, PAGE_SIZE>* decoded_pages{nullptr};
    size_t decoded_capacity{0};
};

struct IndexSmoWalDecodedView {
    std::string_view index_file_name;
    uint64_t index_generation{0};
    uint32_t page_count{0};
    const char* header_image{nullptr};
    const IndexSmoWalView::Page* pages{nullptr};
};

// Inspect performs all validation (including every compressed image) with no
// heap allocation. It leaves |out| unchanged on failure. Its result is solely
// for caller-owned arena budgeting; safe Decode/Copy always inspect again.
bool InspectIndexSmoWal(const WalRecordView& record, IndexSmoWalLayout* out) noexcept;
// The bytes in |record| must remain immutable for the duration of each call.
// Decode/Copy fresh-inspect and only then publish to caller arenas; capacity
// failure and malformed input leave those arenas and |out| untouched.
bool DecodeIndexSmoWal(const WalRecordView& record, const IndexSmoWalDecodeStorage& storage,
                       IndexSmoWalDecodedView* out) noexcept;
bool CopyIndexSmoPageCatalog(const WalRecordView& record, IndexSmoWalView::Page* pages,
                             size_t page_capacity) noexcept;

uint64_t IndexSmoCrc32CallCountForTest() noexcept;
void ResetIndexSmoCrc32CallCountForTest() noexcept;
uint64_t IndexSmoImageDecodeCallCountForTest() noexcept;
void ResetIndexSmoImageDecodeCallCountForTest() noexcept;

// Validates magic, version, flags, bounded length arithmetic, sorted unique
// page identities, mandatory full header image and the record checksum.
bool ParseIndexSmoWal(const WalRecordView& record, IndexSmoWalView* out);
bool ParseIndexBindWal(const WalRecordView& record, std::string_view* index_file_name, uint64_t* generation);
