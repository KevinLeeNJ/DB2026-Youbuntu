/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "recovery/index_smo_log.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace {

constexpr uint32_t kFixedPayloadBytes =
    sizeof(uint32_t) + sizeof(uint16_t) * 2 + sizeof(uint32_t) * 4 + sizeof(uint64_t);
constexpr uint32_t kBindFixedPayloadBytes =
    sizeof(uint32_t) + sizeof(uint16_t) * 2 + sizeof(uint32_t) + sizeof(uint64_t);
constexpr uint32_t kV1PageEntryBytes = sizeof(page_id_t) + PAGE_SIZE;
constexpr uint32_t kChecksumBytes = sizeof(uint32_t);
constexpr uint32_t kImageEnvelopeBytes = sizeof(uint8_t) * 2 + sizeof(uint16_t) + sizeof(uint32_t) * 2;

constexpr std::array<uint32_t, 256> MakeCrc32Table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t value = 0; value < table.size(); ++value) {
        uint32_t crc = value;
        for (int bit = 0; bit < 8; ++bit) {
            const uint32_t mask = 0U - (crc & 1U);
            crc = (crc >> 1U) ^ (0xedb88320U & mask);
        }
        table[value] = crc;
    }
    return table;
}

constexpr std::array<uint32_t, 256> kCrc32Table = MakeCrc32Table();

enum class ImageCodec : uint8_t {
    RAW = 0,
    ZERO_LITERAL_RLE = 1,
};

template <typename T> void AppendScalar(char* dest, uint32_t* offset, T value) {
    std::memcpy(dest + *offset, &value, sizeof(T));
    *offset += sizeof(T);
}

template <typename T> void AppendScalar(std::vector<char>* dest, T value) {
    const size_t offset = dest->size();
    dest->resize(offset + sizeof(T));
    std::memcpy(dest->data() + offset, &value, sizeof(T));
}

void AppendBytes(std::vector<char>* dest, const char* bytes, size_t length) {
    dest->insert(dest->end(), bytes, bytes + length);
}

bool CheckedExpectedV1Length(uint32_t name_bytes, uint32_t page_count, uint32_t* result) {
    if (name_bytes == 0 || name_bytes > MAX_INDEX_SMO_FILE_NAME_BYTES || page_count == 0 ||
        page_count > MAX_INDEX_SMO_PAGE_COUNT) {
        return false;
    }
    uint64_t total = LOG_HEADER_SIZE;
    total += kFixedPayloadBytes;
    total += name_bytes;
    total += static_cast<uint64_t>(page_count) * kV1PageEntryBytes;
    total += PAGE_SIZE;
    total += kChecksumBytes;
    if (total > MAX_INDEX_SMO_RECORD_BYTES || total > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    *result = static_cast<uint32_t>(total);
    return true;
}

std::vector<char> EncodeZeroLiteralRle(const char* raw) {
    std::vector<char> encoded;
    encoded.reserve(PAGE_SIZE / 2);
    size_t offset = 0;
    while (offset < PAGE_SIZE) {
        size_t zero_run = 0;
        while (offset + zero_run < PAGE_SIZE && raw[offset + zero_run] == 0 && zero_run < 128) {
            ++zero_run;
        }
        if (zero_run >= 2) {
            encoded.push_back(static_cast<char>(0x80U | static_cast<uint8_t>(zero_run - 1)));
            offset += zero_run;
            continue;
        }

        const size_t literal_begin = offset;
        ++offset;
        while (offset < PAGE_SIZE && offset - literal_begin < 128) {
            zero_run = 0;
            while (offset + zero_run < PAGE_SIZE && raw[offset + zero_run] == 0 && zero_run < 128) {
                ++zero_run;
            }
            if (zero_run >= 2) {
                break;
            }
            ++offset;
        }
        const size_t literal_length = offset - literal_begin;
        encoded.push_back(static_cast<char>(literal_length - 1));
        AppendBytes(&encoded, raw + literal_begin, literal_length);
    }
    return encoded;
}

void AppendImage(std::vector<char>* dest, const char* raw) {
    const std::vector<char> encoded = EncodeZeroLiteralRle(raw);
    const bool use_rle = encoded.size() < PAGE_SIZE;
    AppendScalar(dest, static_cast<uint8_t>(use_rle ? ImageCodec::ZERO_LITERAL_RLE : ImageCodec::RAW));
    AppendScalar(dest, static_cast<uint8_t>(0));
    AppendScalar(dest, static_cast<uint16_t>(0));
    AppendScalar(dest, static_cast<uint32_t>(PAGE_SIZE));
    AppendScalar(dest, static_cast<uint32_t>(use_rle ? encoded.size() : PAGE_SIZE));
    if (use_rle) {
        AppendBytes(dest, encoded.data(), encoded.size());
    } else {
        AppendBytes(dest, raw, PAGE_SIZE);
    }
}

bool ReadBounded(const char* bytes, uint32_t limit, uint32_t* offset, void* out, uint32_t length) {
    if (*offset > limit || length > limit - *offset) {
        return false;
    }
    std::memcpy(out, bytes + *offset, length);
    *offset += length;
    return true;
}

template <typename T> bool ReadScalar(const char* bytes, uint32_t limit, uint32_t* offset, T* out) {
    return ReadBounded(bytes, limit, offset, out, sizeof(T));
}

bool DecodeZeroLiteralRle(const char* encoded, uint32_t encoded_length, std::array<char, PAGE_SIZE>* decoded) {
    uint32_t input = 0;
    uint32_t output = 0;
    while (input < encoded_length) {
        const uint8_t control = static_cast<uint8_t>(encoded[input++]);
        const uint32_t length = static_cast<uint32_t>(control & 0x7fU) + 1U;
        if (length > PAGE_SIZE - output) {
            return false;
        }
        if ((control & 0x80U) != 0) {
            std::memset(decoded->data() + output, 0, length);
        } else {
            if (length > encoded_length - input) {
                return false;
            }
            std::memcpy(decoded->data() + output, encoded + input, length);
            input += length;
        }
        output += length;
    }
    return output == PAGE_SIZE;
}

bool ParseV2Image(const char* bytes, uint32_t limit, uint32_t* offset,
                  std::vector<std::array<char, PAGE_SIZE>>* decoded_images, const char** image) {
    uint8_t codec_value = 0;
    uint8_t flags = 0;
    uint16_t reserved = 0;
    uint32_t raw_length = 0;
    uint32_t encoded_length = 0;
    if (!ReadScalar(bytes, limit, offset, &codec_value) || !ReadScalar(bytes, limit, offset, &flags) ||
        !ReadScalar(bytes, limit, offset, &reserved) || !ReadScalar(bytes, limit, offset, &raw_length) ||
        !ReadScalar(bytes, limit, offset, &encoded_length) || flags != 0 || reserved != 0 || raw_length != PAGE_SIZE ||
        encoded_length == 0 || encoded_length > limit - *offset) {
        return false;
    }

    const auto codec = static_cast<ImageCodec>(codec_value);
    if (codec == ImageCodec::RAW) {
        if (encoded_length != PAGE_SIZE) {
            return false;
        }
        *image = bytes + *offset;
    } else if (codec == ImageCodec::ZERO_LITERAL_RLE) {
        if (encoded_length >= PAGE_SIZE) {
            return false;
        }
        decoded_images->emplace_back();
        if (!DecodeZeroLiteralRle(bytes + *offset, encoded_length, &decoded_images->back())) {
            decoded_images->pop_back();
            return false;
        }
        *image = decoded_images->back().data();
    } else {
        return false;
    }
    *offset += encoded_length;
    return true;
}

} // namespace

uint32_t IndexSmoCrc32(const char* bytes, size_t length) {
    uint32_t crc = 0xffffffffU;
    for (size_t i = 0; i < length; ++i) {
        const uint8_t table_index = static_cast<uint8_t>(crc ^ static_cast<uint8_t>(bytes[i]));
        crc = (crc >> 8U) ^ kCrc32Table[table_index];
    }
    return ~crc;
}

IndexSmoLogRecord::IndexSmoLogRecord(const IndexSmoWalData& data) {
    if (data.index_file_name.empty() || data.index_file_name.size() > MAX_INDEX_SMO_FILE_NAME_BYTES ||
        data.pages.empty() || data.pages.size() > MAX_INDEX_SMO_PAGE_COUNT) {
        throw std::length_error("INDEX_SMO record exceeds its bounded format");
    }
    if (data.index_generation == 0) {
        throw std::invalid_argument("INDEX_SMO requires a nonzero index generation");
    }
    if (data.index_file_name.find('\0') != std::string::npos || data.index_file_name.find('/') != std::string::npos ||
        data.index_file_name == "." || data.index_file_name == "..") {
        throw std::invalid_argument("INDEX_SMO requires a safe database-local index filename");
    }
    page_id_t previous = INVALID_PAGE_ID;
    for (const auto& page : data.pages) {
        if (page.page_no <= 0 || (previous != INVALID_PAGE_ID && page.page_no <= previous)) {
            throw std::invalid_argument("INDEX_SMO page images must be sorted, unique and positive");
        }
        previous = page.page_no;
    }

    payload_.reserve(kFixedPayloadBytes + data.index_file_name.size() +
                     data.pages.size() * (sizeof(page_id_t) + kImageEnvelopeBytes + PAGE_SIZE) + kImageEnvelopeBytes +
                     PAGE_SIZE);
    AppendScalar(&payload_, INDEX_SMO_MAGIC);
    AppendScalar(&payload_, INDEX_SMO_VERSION_V2);
    AppendScalar(&payload_, INDEX_SMO_FLAG_HEADER_IMAGE);
    AppendScalar(&payload_, static_cast<uint32_t>(data.index_file_name.size()));
    AppendScalar(&payload_, static_cast<uint32_t>(data.pages.size()));
    AppendScalar(&payload_, static_cast<uint32_t>(PAGE_SIZE));
    AppendScalar(&payload_, static_cast<uint32_t>(PAGE_SIZE));
    AppendScalar(&payload_, data.index_generation);
    AppendBytes(&payload_, data.index_file_name.data(), data.index_file_name.size());
    for (const auto& page : data.pages) {
        AppendScalar(&payload_, page.page_no);
        AppendImage(&payload_, page.bytes.data());
    }
    AppendImage(&payload_, data.header.data());

    const uint64_t total_len = static_cast<uint64_t>(LOG_HEADER_SIZE) + payload_.size() + kChecksumBytes;
    if (total_len > MAX_INDEX_SMO_RECORD_BYTES || total_len > std::numeric_limits<uint32_t>::max()) {
        throw std::length_error("INDEX_SMO record exceeds its bounded format");
    }
    log_type_ = LogType::INDEX_SMO;
    lsn_ = INVALID_LSN;
    log_tot_len_ = static_cast<uint32_t>(total_len);
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
}

void IndexSmoLogRecord::serialize(char* dest) const {
    LogRecord::serialize(dest);
    uint32_t offset = OFFSET_LOG_DATA;
    std::memcpy(dest + offset, payload_.data(), payload_.size());
    offset += static_cast<uint32_t>(payload_.size());
    const uint32_t checksum = IndexSmoCrc32(dest, offset);
    AppendScalar(dest, &offset, checksum);
    assert(offset == log_tot_len_);
}

page_id_t IndexSmoWalView::page_no(uint32_t index) const {
    return pages_[index].page_no;
}

const char* IndexSmoWalView::page_image(uint32_t index) const {
    return pages_[index].image;
}

bool ParseIndexSmoWal(const WalRecordView& record, IndexSmoWalView* out) {
    if (out == nullptr || record.log_type != LogType::INDEX_SMO || record.bytes == nullptr ||
        record.total_len > MAX_INDEX_SMO_RECORD_BYTES ||
        record.total_len < LOG_HEADER_SIZE + kFixedPayloadBytes + 1 + kChecksumBytes) {
        return false;
    }
    const uint32_t stored_checksum =
        read_unaligned<uint32_t>(record.bytes + record.total_len - static_cast<uint32_t>(kChecksumBytes));
    if (stored_checksum != IndexSmoCrc32(record.bytes, record.total_len - kChecksumBytes)) {
        return false;
    }

    uint32_t offset = OFFSET_LOG_DATA;
    const uint32_t magic = read_unaligned<uint32_t>(record.bytes + offset);
    offset += sizeof(uint32_t);
    const uint16_t version = read_unaligned<uint16_t>(record.bytes + offset);
    offset += sizeof(uint16_t);
    const uint16_t flags = read_unaligned<uint16_t>(record.bytes + offset);
    offset += sizeof(uint16_t);
    const uint32_t name_bytes = read_unaligned<uint32_t>(record.bytes + offset);
    offset += sizeof(uint32_t);
    const uint32_t page_count = read_unaligned<uint32_t>(record.bytes + offset);
    offset += sizeof(uint32_t);
    const uint32_t page_size = read_unaligned<uint32_t>(record.bytes + offset);
    offset += sizeof(uint32_t);
    const uint32_t header_size = read_unaligned<uint32_t>(record.bytes + offset);
    offset += sizeof(uint32_t);
    const uint64_t generation = read_unaligned<uint64_t>(record.bytes + offset);
    offset += sizeof(uint64_t);

    if (magic != INDEX_SMO_MAGIC || (version != INDEX_SMO_VERSION_V1 && version != INDEX_SMO_VERSION_V2) ||
        flags != INDEX_SMO_FLAG_HEADER_IMAGE || name_bytes == 0 || name_bytes > MAX_INDEX_SMO_FILE_NAME_BYTES ||
        page_count == 0 || page_count > MAX_INDEX_SMO_PAGE_COUNT || page_size != PAGE_SIZE ||
        header_size != PAGE_SIZE || generation == 0) {
        return false;
    }
    if (version == INDEX_SMO_VERSION_V1) {
        uint32_t expected_len = 0;
        if (!CheckedExpectedV1Length(name_bytes, page_count, &expected_len) || expected_len != record.total_len) {
            return false;
        }
    }
    const uint32_t payload_limit = record.total_len - kChecksumBytes;
    if (offset > payload_limit || name_bytes > payload_limit - offset) {
        return false;
    }
    const std::string_view file_name(record.bytes + offset, name_bytes);
    if (file_name.find('\0') != std::string_view::npos || file_name.find('/') != std::string_view::npos ||
        file_name == "." || file_name == "..") {
        return false;
    }
    offset += name_bytes;

    IndexSmoWalView parsed;
    parsed.index_file_name = file_name;
    parsed.index_generation = generation;
    parsed.page_count = page_count;
    parsed.pages_.reserve(page_count);
    parsed.decoded_pages_.reserve(static_cast<size_t>(page_count) + 1);

    page_id_t previous = INVALID_PAGE_ID;
    if (version == INDEX_SMO_VERSION_V1) {
        for (uint32_t index = 0; index < page_count; ++index) {
            const page_id_t current = read_unaligned<page_id_t>(record.bytes + offset);
            if (current <= 0 || (previous != INVALID_PAGE_ID && current <= previous)) {
                return false;
            }
            previous = current;
            parsed.pages_.push_back({current, record.bytes + offset + sizeof(page_id_t)});
            offset += kV1PageEntryBytes;
        }
        parsed.header_image = record.bytes + offset;
        offset += PAGE_SIZE;
    } else {
        for (uint32_t index = 0; index < page_count; ++index) {
            page_id_t current = INVALID_PAGE_ID;
            if (!ReadScalar(record.bytes, payload_limit, &offset, &current) || current <= 0 ||
                (previous != INVALID_PAGE_ID && current <= previous)) {
                return false;
            }
            previous = current;
            const char* image = nullptr;
            if (!ParseV2Image(record.bytes, payload_limit, &offset, &parsed.decoded_pages_, &image)) {
                return false;
            }
            parsed.pages_.push_back({current, image});
        }
        if (!ParseV2Image(record.bytes, payload_limit, &offset, &parsed.decoded_pages_, &parsed.header_image)) {
            return false;
        }
    }
    if (offset != payload_limit) {
        return false;
    }
    *out = std::move(parsed);
    return true;
}

IndexBindLogRecord::IndexBindLogRecord(std::string_view index_file_name) : index_file_name_(index_file_name) {
    if (index_file_name_.empty() || index_file_name_.size() > MAX_INDEX_SMO_FILE_NAME_BYTES ||
        index_file_name_.find('\0') != std::string::npos || index_file_name_.find('/') != std::string::npos ||
        index_file_name_ == "." || index_file_name_ == "..") {
        throw std::invalid_argument("INDEX_BIND requires a safe database-local index filename");
    }
    log_type_ = LogType::INDEX_BIND;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE + kBindFixedPayloadBytes + index_file_name_.size() + kChecksumBytes;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
}

void IndexBindLogRecord::serialize(char* dest) const {
    LogRecord::serialize(dest);
    uint32_t offset = OFFSET_LOG_DATA;
    AppendScalar(dest, &offset, INDEX_BIND_MAGIC);
    AppendScalar(dest, &offset, INDEX_BIND_VERSION);
    AppendScalar(dest, &offset, static_cast<uint16_t>(0));
    AppendScalar(dest, &offset, static_cast<uint32_t>(index_file_name_.size()));
    AppendScalar(dest, &offset, static_cast<uint64_t>(lsn_) + 1U);
    std::memcpy(dest + offset, index_file_name_.data(), index_file_name_.size());
    offset += index_file_name_.size();
    AppendScalar(dest, &offset, IndexSmoCrc32(dest, offset));
    assert(offset == log_tot_len_);
}

bool ParseIndexBindWal(const WalRecordView& record, std::string_view* index_file_name, uint64_t* generation) {
    if (record.log_type != LogType::INDEX_BIND || record.bytes == nullptr || index_file_name == nullptr ||
        generation == nullptr || record.total_len < LOG_HEADER_SIZE + kBindFixedPayloadBytes + 1 + kChecksumBytes ||
        record.total_len > LOG_HEADER_SIZE + kBindFixedPayloadBytes + MAX_INDEX_SMO_FILE_NAME_BYTES + kChecksumBytes) {
        return false;
    }
    const uint32_t stored = read_unaligned<uint32_t>(record.bytes + record.total_len - kChecksumBytes);
    if (stored != IndexSmoCrc32(record.bytes, record.total_len - kChecksumBytes)) {
        return false;
    }
    uint32_t offset = OFFSET_LOG_DATA;
    const uint32_t magic = read_unaligned<uint32_t>(record.bytes + offset);
    offset += sizeof(uint32_t);
    const uint16_t version = read_unaligned<uint16_t>(record.bytes + offset);
    offset += sizeof(uint16_t);
    const uint16_t reserved = read_unaligned<uint16_t>(record.bytes + offset);
    offset += sizeof(uint16_t);
    const uint32_t name_bytes = read_unaligned<uint32_t>(record.bytes + offset);
    offset += sizeof(uint32_t);
    const uint64_t parsed_generation = read_unaligned<uint64_t>(record.bytes + offset);
    offset += sizeof(uint64_t);
    if (magic != INDEX_BIND_MAGIC || version != INDEX_BIND_VERSION || reserved != 0 || name_bytes == 0 ||
        name_bytes > MAX_INDEX_SMO_FILE_NAME_BYTES || parsed_generation != static_cast<uint64_t>(record.lsn) + 1U ||
        offset + name_bytes + kChecksumBytes != record.total_len) {
        return false;
    }
    std::string_view name(record.bytes + offset, name_bytes);
    if (name.find('\0') != std::string_view::npos || name.find('/') != std::string_view::npos || name == "." ||
        name == "..") {
        return false;
    }
    *index_file_name = name;
    *generation = parsed_generation;
    return true;
}
