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
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <limits>
#include <stdexcept>

#include "index/ix_defs.h"
#include "minilog.h"

namespace {

constexpr uint32_t kFixedPayloadBytes =
    sizeof(uint32_t) + sizeof(uint16_t) * 2 + sizeof(uint32_t) * 4 + sizeof(uint64_t);
constexpr uint32_t kBindFixedPayloadBytes =
    sizeof(uint32_t) + sizeof(uint16_t) * 2 + sizeof(uint32_t) + sizeof(uint64_t);
constexpr uint32_t kV1PageEntryBytes = sizeof(page_id_t) + PAGE_SIZE;
constexpr uint32_t kChecksumBytes = sizeof(uint32_t);
constexpr uint32_t kImageEnvelopeBytes = sizeof(uint8_t) * 2 + sizeof(uint16_t) + sizeof(uint32_t) * 2;
constexpr uint32_t kV3LayoutBytes = sizeof(int32_t) * 3;
constexpr size_t kMaxRleBytes = PAGE_SIZE + (PAGE_SIZE + 127) / 128;
constexpr size_t kBitmapBytes = PAGE_SIZE / 8;
constexpr size_t kMaxBitmapBytes = kBitmapBytes + PAGE_SIZE;
constexpr size_t kWordSize = sizeof(uint32_t);
constexpr size_t kWordCount = PAGE_SIZE / kWordSize;
constexpr size_t kWordBitmapBytes = kWordCount / 8;
constexpr size_t kMaxWordBitmapBytes = kWordBitmapBytes + PAGE_SIZE;

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
std::atomic<uint64_t> g_index_smo_crc32_calls{0};
std::atomic<uint64_t> g_index_smo_image_decode_calls{0};

enum class ImageCodec : uint8_t {
    RAW = 0,
    ZERO_LITERAL_RLE = 1,
    STRUCTURED_XOR_RLE = 2,
    STRUCTURED_XOR_BITMAP = 3,
    STRUCTURED_XOR_WORD_BITMAP = 4,
};

struct IndexLayout {
    int col_tot_len{0};
    int btree_order{0};
    int keys_size{0};
};

struct V3Config {
    bool enabled;
    bool metrics_enabled;
};

const V3Config& GetV3Config() {
    static const V3Config config = [] {
        const char* enabled = std::getenv("RMDB_INDEX_SMO_V3");
        if (enabled == nullptr) {
            enabled = std::getenv("INDEX_SMO_V3");
        }
        const char* metrics = std::getenv("RMDB_INDEX_SMO_V3_METRICS");
        return V3Config{enabled == nullptr || std::strcmp(enabled, "0") != 0,
                        metrics != nullptr && std::strcmp(metrics, "1") == 0};
    }();
    return config;
}

struct V3Metrics {
    std::atomic<uint64_t> old_bytes{0}, new_bytes{0}, raw{0}, rle{0}, xor_rle{0}, bitmap{0}, word_bitmap{0},
        encode_ns{0}, max_ns{0};
    // Monotonic timestamp of the last aggregate report. This is diagnostic-only
    // state and deliberately never participates in WAL construction.
    std::atomic<int64_t> last_report_ns{0};
};

V3Metrics& GetV3Metrics() {
    static V3Metrics metrics;
    return metrics;
}

bool ValidateLayout(const IndexLayout& layout) {
    if (layout.col_tot_len <= 0 || layout.btree_order <= 0 || layout.keys_size < 0) {
        return false;
    }
    const uint64_t capacity = static_cast<uint64_t>(layout.btree_order) + 1;
    const uint64_t raw_key_end = sizeof(IxPageHdr) + capacity * static_cast<uint64_t>(layout.col_tot_len);
    if (raw_key_end > PAGE_SIZE) {
        return false;
    }
    const uint64_t rid_begin = (raw_key_end + alignof(Rid) - 1) / alignof(Rid) * alignof(Rid);
    return layout.keys_size == static_cast<int>(rid_begin - sizeof(IxPageHdr)) &&
           rid_begin + capacity * sizeof(Rid) <= PAGE_SIZE;
}

bool ReadHeaderLayout(const char* header, IndexLayout* layout) {
    if (header == nullptr || layout == nullptr) {
        return false;
    }
    const int total_length = read_unaligned<int>(header);
    const page_id_t first_free = read_unaligned<page_id_t>(header + sizeof(int));
    const int num_pages = read_unaligned<int>(header + sizeof(int) * 2);
    const page_id_t root_page = read_unaligned<page_id_t>(header + sizeof(int) * 3);
    const int col_num = read_unaligned<int>(header + sizeof(int) * 4);
    if (col_num <= 0 || col_num > 256 || total_length != 40 + col_num * static_cast<int>(sizeof(ColType) + sizeof(int)) ||
        total_length > PAGE_SIZE || num_pages < IX_INIT_NUM_PAGES || root_page < IX_INIT_ROOT_PAGE ||
        root_page >= num_pages || first_free < IX_NO_PAGE || first_free >= num_pages) {
        return false;
    }
    const uint64_t types_offset = sizeof(int) * 5;
    const uint64_t lens_offset = types_offset + static_cast<uint64_t>(col_num) * sizeof(ColType);
    int64_t column_sum = 0;
    for (int index = 0; index < col_num; ++index) {
        const int type = read_unaligned<int>(header + types_offset + static_cast<uint64_t>(index) * sizeof(ColType));
        const int length = read_unaligned<int>(header + lens_offset + static_cast<uint64_t>(index) * sizeof(int));
        if (type < TYPE_INT || type > TYPE_DATETIME || length <= 0 ||
            ((type == TYPE_INT || type == TYPE_FLOAT) && length != static_cast<int>(sizeof(int)))) {
            return false;
        }
        column_sum += length;
        if (column_sum > PAGE_SIZE) {
            return false;
        }
    }
    const uint64_t layout_offset = lens_offset + static_cast<uint64_t>(col_num) * sizeof(int);
    if (layout_offset + sizeof(int) * 3 > PAGE_SIZE) {
        return false;
    }
    layout->col_tot_len = read_unaligned<int>(header + layout_offset);
    layout->btree_order = read_unaligned<int>(header + layout_offset + sizeof(int));
    layout->keys_size = read_unaligned<int>(header + layout_offset + sizeof(int) * 2);
    const page_id_t first_leaf = read_unaligned<page_id_t>(header + layout_offset + sizeof(int) * 3);
    const page_id_t last_leaf = read_unaligned<page_id_t>(header + layout_offset + sizeof(int) * 4);
    return first_leaf >= IX_LEAF_HEADER_PAGE && first_leaf < num_pages && last_leaf >= IX_LEAF_HEADER_PAGE &&
           last_leaf < num_pages && column_sum == layout->col_tot_len && ValidateLayout(*layout);
}

bool SameLayout(const IndexLayout& left, const IndexLayout& right) {
    return left.col_tot_len == right.col_tot_len && left.btree_order == right.btree_order &&
           left.keys_size == right.keys_size;
}

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

uint32_t EncodeZeroLiteralRleInto(const char* raw, std::array<char, kMaxRleBytes>* encoded) {
    uint32_t output = 0;
    size_t offset = 0;
    while (offset < PAGE_SIZE) {
        size_t zero_run = 0;
        while (offset + zero_run < PAGE_SIZE && raw[offset + zero_run] == 0 && zero_run < 128) {
            ++zero_run;
        }
        if (zero_run >= 2) {
            (*encoded)[output++] = static_cast<char>(0x80U | static_cast<uint8_t>(zero_run - 1));
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
        const uint32_t length = static_cast<uint32_t>(offset - literal_begin);
        (*encoded)[output++] = static_cast<char>(length - 1);
        std::memcpy(encoded->data() + output, raw + literal_begin, length);
        output += length;
    }
    return output;
}

uint32_t EncodeBitmapInto(const char* raw, std::array<char, kMaxBitmapBytes>* encoded) {
    encoded->fill(0);
    uint32_t output = kBitmapBytes;
    for (size_t index = 0; index < PAGE_SIZE; ++index) {
        if (raw[index] == 0) {
            continue;
        }
        (*encoded)[index / 8] = static_cast<char>(static_cast<uint8_t>((*encoded)[index / 8]) | (1U << (index % 8)));
        (*encoded)[output++] = raw[index];
    }
    return output;
}

uint32_t EncodeWordBitmapInto(const char* raw, std::array<char, kMaxWordBitmapBytes>* encoded) {
    encoded->fill(0);
    uint32_t output = kWordBitmapBytes;
    for (size_t word = 0; word < kWordCount; ++word) {
        uint32_t value = 0;
        std::memcpy(&value, raw + word * kWordSize, kWordSize);
        if (value == 0) {
            continue;
        }
        (*encoded)[word / 8] = static_cast<char>(static_cast<uint8_t>((*encoded)[word / 8]) | (1U << (word % 8)));
        std::memcpy(encoded->data() + output, raw + word * kWordSize, kWordSize);
        output += kWordSize;
    }
    return output;
}

void AppendImage(std::vector<char>* dest, const char* raw) {
    std::array<char, kMaxRleBytes> encoded{};
    const uint32_t encoded_length = EncodeZeroLiteralRleInto(raw, &encoded);
    const bool use_rle = encoded_length < PAGE_SIZE;
    AppendScalar(dest, static_cast<uint8_t>(use_rle ? ImageCodec::ZERO_LITERAL_RLE : ImageCodec::RAW));
    AppendScalar(dest, static_cast<uint8_t>(0));
    AppendScalar(dest, static_cast<uint16_t>(0));
    AppendScalar(dest, static_cast<uint32_t>(PAGE_SIZE));
    AppendScalar(dest, static_cast<uint32_t>(use_rle ? encoded_length : PAGE_SIZE));
    if (use_rle) {
        AppendBytes(dest, encoded.data(), encoded_length);
    } else {
        AppendBytes(dest, raw, PAGE_SIZE);
    }
}

bool ValidateIxPageImage(const IndexLayout& layout, const char* raw) {
    if (!ValidateLayout(layout) || raw == nullptr) {
        return false;
    }
    int num_key = 0;
    std::memcpy(&num_key, raw + offsetof(IxPageHdr, num_key), sizeof(num_key));
    const uint8_t is_leaf = static_cast<uint8_t>(raw[offsetof(IxPageHdr, is_leaf)]);
    const uint64_t capacity = static_cast<uint64_t>(layout.btree_order) + 1;
    return num_key >= 0 && static_cast<uint64_t>(num_key) <= capacity && is_leaf <= 1;
}

bool TransformIxPage(const IndexLayout& layout, const char* raw, std::array<char, PAGE_SIZE>* transformed) {
    if (transformed == nullptr || !ValidateIxPageImage(layout, raw)) {
        return false;
    }
    int num_key = 0;
    std::memcpy(&num_key, raw + offsetof(IxPageHdr, num_key), sizeof(num_key));
    std::memcpy(transformed->data(), raw, PAGE_SIZE);
    const size_t key_begin = sizeof(IxPageHdr);
    const size_t rid_begin = key_begin + static_cast<size_t>(layout.keys_size);
    for (int key = 0; key < num_key; ++key) {
        const char* source = raw + key_begin + static_cast<size_t>(key) * layout.col_tot_len;
        char* destination = transformed->data() + key_begin + static_cast<size_t>(key) * layout.col_tot_len;
        for (int byte = 0; byte < layout.col_tot_len; ++byte) {
            destination[byte] = key == 0 ? source[byte] : static_cast<char>(source[byte] ^ source[byte - layout.col_tot_len]);
        }
        source = raw + rid_begin + static_cast<size_t>(key) * sizeof(Rid);
        destination = transformed->data() + rid_begin + static_cast<size_t>(key) * sizeof(Rid);
        for (size_t byte = 0; byte < sizeof(Rid); ++byte) {
            destination[byte] = key == 0 ? source[byte] : static_cast<char>(source[byte] ^ source[byte - sizeof(Rid)]);
        }
    }
    return true;
}

bool UntransformIxPage(const IndexLayout& layout, std::array<char, PAGE_SIZE>* image) {
    if (image == nullptr || !ValidateIxPageImage(layout, image->data())) {
        return false;
    }
    int num_key = 0;
    std::memcpy(&num_key, image->data() + offsetof(IxPageHdr, num_key), sizeof(num_key));
    const size_t key_begin = sizeof(IxPageHdr);
    const size_t rid_begin = key_begin + static_cast<size_t>(layout.keys_size);
    for (int key = 1; key < num_key; ++key) {
        char* key_bytes = image->data() + key_begin + static_cast<size_t>(key) * layout.col_tot_len;
        const char* previous = key_bytes - layout.col_tot_len;
        for (int byte = 0; byte < layout.col_tot_len; ++byte) {
            key_bytes[byte] = static_cast<char>(key_bytes[byte] ^ previous[byte]);
        }
        char* rid_bytes = image->data() + rid_begin + static_cast<size_t>(key) * sizeof(Rid);
        previous = rid_bytes - sizeof(Rid);
        for (size_t byte = 0; byte < sizeof(Rid); ++byte) {
            rid_bytes[byte] = static_cast<char>(rid_bytes[byte] ^ previous[byte]);
        }
    }
    return true;
}

ImageCodec AppendV3PageImage(std::vector<char>* dest, const char* raw, const IndexLayout& layout) {
    std::array<char, kMaxRleBytes> rle{};
    const uint32_t rle_length = EncodeZeroLiteralRleInto(raw, &rle);
    std::array<char, PAGE_SIZE> transformed{};
    std::array<char, kMaxRleBytes> transformed_rle{};
    std::array<char, kMaxBitmapBytes> bitmap{};
    std::array<char, kMaxWordBitmapBytes> word_bitmap{};
    uint32_t transformed_length = PAGE_SIZE;
    uint32_t bitmap_length = PAGE_SIZE;
    uint32_t word_bitmap_length = PAGE_SIZE;
    const bool can_transform = TransformIxPage(layout, raw, &transformed);
    if (can_transform) {
        transformed_length = EncodeZeroLiteralRleInto(transformed.data(), &transformed_rle);
        bitmap_length = EncodeBitmapInto(transformed.data(), &bitmap);
        word_bitmap_length = EncodeWordBitmapInto(transformed.data(), &word_bitmap);
    }
    const bool use_word_bitmap = can_transform && word_bitmap_length < PAGE_SIZE && word_bitmap_length < bitmap_length &&
                                 word_bitmap_length < transformed_length && word_bitmap_length < rle_length;
    const bool use_bitmap = !use_word_bitmap && can_transform && bitmap_length < PAGE_SIZE && bitmap_length < transformed_length &&
                            bitmap_length < rle_length;
    const bool use_transformed = !use_word_bitmap && !use_bitmap && can_transform && transformed_length < PAGE_SIZE &&
                                 transformed_length < rle_length;
    const bool use_rle = !use_word_bitmap && !use_bitmap && !use_transformed && rle_length < PAGE_SIZE;
    AppendScalar(dest, static_cast<uint8_t>(use_word_bitmap ? ImageCodec::STRUCTURED_XOR_WORD_BITMAP
                                         : use_bitmap       ? ImageCodec::STRUCTURED_XOR_BITMAP
                                         : use_transformed ? ImageCodec::STRUCTURED_XOR_RLE
                                         : use_rle         ? ImageCodec::ZERO_LITERAL_RLE
                                                           : ImageCodec::RAW));
    AppendScalar(dest, static_cast<uint8_t>(0));
    AppendScalar(dest, static_cast<uint16_t>(0));
    AppendScalar(dest, static_cast<uint32_t>(PAGE_SIZE));
    AppendScalar(dest, static_cast<uint32_t>(use_word_bitmap ? word_bitmap_length : use_bitmap ? bitmap_length : use_transformed ? transformed_length
                                                                    : use_rle ? rle_length : PAGE_SIZE));
    if (use_word_bitmap) {
        AppendBytes(dest, word_bitmap.data(), word_bitmap_length);
    } else if (use_bitmap) {
        AppendBytes(dest, bitmap.data(), bitmap_length);
    } else if (use_transformed) {
        AppendBytes(dest, transformed_rle.data(), transformed_length);
    } else if (use_rle) {
        AppendBytes(dest, rle.data(), rle_length);
    } else {
        AppendBytes(dest, raw, PAGE_SIZE);
    }
    return use_word_bitmap ? ImageCodec::STRUCTURED_XOR_WORD_BITMAP
                      : use_bitmap ? ImageCodec::STRUCTURED_XOR_BITMAP
                      : use_transformed ? ImageCodec::STRUCTURED_XOR_RLE
                                        : use_rle ? ImageCodec::ZERO_LITERAL_RLE : ImageCodec::RAW;
}

void RecordV3Metrics(uint64_t old_bytes, uint64_t new_bytes, uint64_t encode_ns, ImageCodec codec) {
    V3Metrics& metrics = GetV3Metrics();
    metrics.old_bytes.fetch_add(old_bytes, std::memory_order_relaxed);
    metrics.new_bytes.fetch_add(new_bytes, std::memory_order_relaxed);
    (codec == ImageCodec::RAW   ? metrics.raw
     : codec == ImageCodec::ZERO_LITERAL_RLE ? metrics.rle
     : codec == ImageCodec::STRUCTURED_XOR_RLE ? metrics.xor_rle
     : codec == ImageCodec::STRUCTURED_XOR_BITMAP ? metrics.bitmap
                                                    : metrics.word_bitmap)
        .fetch_add(1, std::memory_order_relaxed);
    metrics.encode_ns.fetch_add(encode_ns, std::memory_order_relaxed);
    uint64_t previous = metrics.max_ns.load(std::memory_order_relaxed);
    while (previous < encode_ns &&
           !metrics.max_ns.compare_exchange_weak(previous, encode_ns, std::memory_order_relaxed)) {
    }

    constexpr int64_t kReportIntervalNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::seconds(5)).count();
    const int64_t now_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
    int64_t last_report_ns = metrics.last_report_ns.load(std::memory_order_relaxed);
    while (now_ns - last_report_ns >= kReportIntervalNs) {
        if (metrics.last_report_ns.compare_exchange_weak(last_report_ns, now_ns, std::memory_order_relaxed)) {
            // This is a best-effort aggregate snapshot: concurrent encodes can
            // land on either side of it, but no individual page is logged.
            LOG_WARN(
                "index-smo-v3-metrics old_bytes=%llu new_bytes=%llu codec_raw=%llu codec_rle=%llu "
                "codec_xor_rle=%llu codec_bitmap=%llu codec_word_bitmap=%llu encode_total_ns=%llu encode_max_ns=%llu",
                static_cast<unsigned long long>(metrics.old_bytes.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(metrics.new_bytes.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(metrics.raw.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(metrics.rle.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(metrics.xor_rle.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(metrics.bitmap.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(metrics.word_bitmap.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(metrics.encode_ns.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(metrics.max_ns.load(std::memory_order_relaxed)));
            return;
        }
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

bool DecodeBitmap(const char* encoded, uint32_t encoded_length, std::array<char, PAGE_SIZE>* decoded) {
    if (encoded_length < kBitmapBytes || encoded_length >= PAGE_SIZE) {
        return false;
    }
    uint32_t nonzero = 0;
    for (size_t index = 0; index < kBitmapBytes; ++index) {
        uint8_t bits = static_cast<uint8_t>(encoded[index]);
        while (bits != 0) {
            nonzero += bits & 1U;
            bits >>= 1U;
        }
    }
    if (nonzero != encoded_length - kBitmapBytes) {
        return false;
    }
    decoded->fill(0);
    uint32_t input = kBitmapBytes;
    for (size_t index = 0; index < PAGE_SIZE; ++index) {
        if ((static_cast<uint8_t>(encoded[index / 8]) & (1U << (index % 8))) != 0) {
            if (input == encoded_length) {
                return false;
            }
            (*decoded)[index] = encoded[input++];
        }
    }
    return input == encoded_length;
}

bool DecodeWordBitmap(const char* encoded, uint32_t encoded_length, std::array<char, PAGE_SIZE>* decoded) {
    if (encoded_length < kWordBitmapBytes || encoded_length >= PAGE_SIZE ||
        (encoded_length - kWordBitmapBytes) % kWordSize != 0) {
        return false;
    }
    uint32_t words = 0;
    for (size_t index = 0; index < kWordBitmapBytes; ++index) {
        uint8_t bits = static_cast<uint8_t>(encoded[index]);
        while (bits != 0) {
            words += bits & 1U;
            bits >>= 1U;
        }
    }
    if (words != (encoded_length - kWordBitmapBytes) / kWordSize) {
        return false;
    }
    decoded->fill(0);
    uint32_t input = kWordBitmapBytes;
    for (size_t word = 0; word < kWordCount; ++word) {
        if ((static_cast<uint8_t>(encoded[word / 8]) & (1U << (word % 8))) != 0) {
            if (input + kWordSize > encoded_length) {
                return false;
            }
            std::memcpy(decoded->data() + word * kWordSize, encoded + input, kWordSize);
            input += kWordSize;
        }
    }
    return input == encoded_length;
}

struct ImageToken {
    ImageCodec codec{ImageCodec::RAW};
    const char* encoded{nullptr};
    uint32_t encoded_length{0};
    bool transformed{false};
};

class IndexSmoLayoutCursor {
public:
    IndexSmoLayoutCursor(const char* bytes, uint32_t limit, uint32_t offset, uint16_t version) noexcept
        : bytes_(bytes), limit_(limit), offset_(offset), version_(version) {}

    bool NextPage(page_id_t* page_no, ImageToken* image) noexcept {
        return ReadScalar(bytes_, limit_, &offset_, page_no) && *page_no > 0 && NextImage(image, false);
    }

    bool NextHeader(ImageToken* image) noexcept {
        return NextImage(image, true);
    }
    uint32_t offset() const noexcept {
        return offset_;
    }

private:
    bool NextImage(ImageToken* image, bool header) noexcept {
        if (version_ == INDEX_SMO_VERSION_V1) {
            if (offset_ > limit_ || PAGE_SIZE > limit_ - offset_) {
                return false;
            }
            *image = {ImageCodec::RAW, bytes_ + offset_, PAGE_SIZE, false};
            offset_ += PAGE_SIZE;
            return true;
        }
        uint8_t codec_value = 0, flags = 0;
        uint16_t reserved = 0;
        uint32_t raw_length = 0, encoded_length = 0;
        if (!ReadScalar(bytes_, limit_, &offset_, &codec_value) || !ReadScalar(bytes_, limit_, &offset_, &flags) ||
            !ReadScalar(bytes_, limit_, &offset_, &reserved) || !ReadScalar(bytes_, limit_, &offset_, &raw_length) ||
            !ReadScalar(bytes_, limit_, &offset_, &encoded_length) || flags != 0 || reserved != 0 ||
            raw_length != PAGE_SIZE || encoded_length == 0 || encoded_length > limit_ - offset_) {
            return false;
        }
        const ImageCodec codec = static_cast<ImageCodec>(codec_value);
        const bool transformed = codec == ImageCodec::STRUCTURED_XOR_RLE ||
                                 codec == ImageCodec::STRUCTURED_XOR_BITMAP ||
                                 codec == ImageCodec::STRUCTURED_XOR_WORD_BITMAP;
        const bool v2_codec = codec == ImageCodec::RAW || codec == ImageCodec::ZERO_LITERAL_RLE;
        if (!(v2_codec || (version_ == INDEX_SMO_VERSION_V3 && !header && transformed)) ||
            (codec == ImageCodec::RAW && encoded_length != PAGE_SIZE) ||
            (codec != ImageCodec::RAW && encoded_length >= PAGE_SIZE)) {
            return false;
        }
        *image = {codec, bytes_ + offset_, encoded_length, transformed};
        offset_ += encoded_length;
        return true;
    }

    const char* bytes_;
    uint32_t limit_;
    uint32_t offset_;
    uint16_t version_;
};

bool DecodeImageToken(const ImageToken& token, std::array<char, PAGE_SIZE>* decoded) noexcept {
    g_index_smo_image_decode_calls.fetch_add(1, std::memory_order_relaxed);
    if (decoded == nullptr || token.codec == ImageCodec::RAW) {
        return false;
    }
    return token.codec == ImageCodec::ZERO_LITERAL_RLE || token.codec == ImageCodec::STRUCTURED_XOR_RLE
               ? DecodeZeroLiteralRle(token.encoded, token.encoded_length, decoded)
           : token.codec == ImageCodec::STRUCTURED_XOR_BITMAP
               ? DecodeBitmap(token.encoded, token.encoded_length, decoded)
               : DecodeWordBitmap(token.encoded, token.encoded_length, decoded);
}

bool ImagePointer(const ImageToken& token, std::array<char, PAGE_SIZE>* decoded, const char** image) noexcept {
    if (token.codec == ImageCodec::RAW) {
        *image = token.encoded;
        return true;
    }
    if (!DecodeImageToken(token, decoded)) {
        return false;
    }
    *image = decoded->data();
    return true;
}

} // namespace

uint32_t IndexSmoCrc32(const char* bytes, size_t length) {
    g_index_smo_crc32_calls.fetch_add(1, std::memory_order_relaxed);
    uint32_t crc = 0xffffffffU;
    for (size_t i = 0; i < length; ++i) {
        const uint8_t table_index = static_cast<uint8_t>(crc ^ static_cast<uint8_t>(bytes[i]));
        crc = (crc >> 8U) ^ kCrc32Table[table_index];
    }
    return ~crc;
}

uint64_t IndexSmoCrc32CallCountForTest() noexcept {
    return g_index_smo_crc32_calls.load(std::memory_order_relaxed);
}

void ResetIndexSmoCrc32CallCountForTest() noexcept {
    g_index_smo_crc32_calls.store(0, std::memory_order_relaxed);
}

uint64_t IndexSmoImageDecodeCallCountForTest() noexcept {
    return g_index_smo_image_decode_calls.load(std::memory_order_relaxed);
}

void ResetIndexSmoImageDecodeCallCountForTest() noexcept {
    g_index_smo_image_decode_calls.store(0, std::memory_order_relaxed);
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

    IndexLayout layout;
    const V3Config& config = GetV3Config();
    const bool use_v3 = config.enabled && ReadHeaderLayout(data.header.data(), &layout);
    payload_.reserve(kFixedPayloadBytes + (use_v3 ? kV3LayoutBytes : 0) + data.index_file_name.size() +
                     data.pages.size() * (sizeof(page_id_t) + kImageEnvelopeBytes + PAGE_SIZE) + kImageEnvelopeBytes +
                     PAGE_SIZE);
    AppendScalar(&payload_, INDEX_SMO_MAGIC);
    AppendScalar(&payload_, use_v3 ? INDEX_SMO_VERSION_V3 : INDEX_SMO_VERSION_V2);
    AppendScalar(&payload_, INDEX_SMO_FLAG_HEADER_IMAGE);
    AppendScalar(&payload_, static_cast<uint32_t>(data.index_file_name.size()));
    AppendScalar(&payload_, static_cast<uint32_t>(data.pages.size()));
    AppendScalar(&payload_, static_cast<uint32_t>(PAGE_SIZE));
    AppendScalar(&payload_, static_cast<uint32_t>(PAGE_SIZE));
    AppendScalar(&payload_, data.index_generation);
    if (use_v3) {
        AppendScalar(&payload_, static_cast<int32_t>(layout.col_tot_len));
        AppendScalar(&payload_, static_cast<int32_t>(layout.btree_order));
        AppendScalar(&payload_, static_cast<int32_t>(layout.keys_size));
    }
    AppendBytes(&payload_, data.index_file_name.data(), data.index_file_name.size());
    for (const auto& page : data.pages) {
        AppendScalar(&payload_, page.page_no);
        if (use_v3) {
            const size_t before = payload_.size();
            const auto image_start =
                config.metrics_enabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
            const ImageCodec codec = AppendV3PageImage(&payload_, page.bytes.data(), layout);
            if (config.metrics_enabled) {
                const uint64_t elapsed = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - image_start)
                        .count());
                RecordV3Metrics(PAGE_SIZE + kImageEnvelopeBytes, payload_.size() - before, elapsed, codec);
            }
        } else {
            AppendImage(&payload_, page.bytes.data());
        }
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

bool InspectIndexSmoWal(const WalRecordView& record, IndexSmoWalLayout* out) noexcept {
    if (out == nullptr || record.log_type != LogType::INDEX_SMO || record.bytes == nullptr ||
        record.total_len > MAX_INDEX_SMO_RECORD_BYTES ||
        record.total_len < LOG_HEADER_SIZE + kFixedPayloadBytes + 1 + kChecksumBytes)
        return false;
    const uint32_t payload_limit = record.total_len - kChecksumBytes;
    const uint32_t stored_crc = read_unaligned<uint32_t>(record.bytes + payload_limit);
    const uint32_t prefix_crc = IndexSmoCrc32(record.bytes, payload_limit);
    if (stored_crc != prefix_crc)
        return false;
    uint32_t offset = OFFSET_LOG_DATA, magic = 0, name_bytes = 0, page_count = 0, page_size = 0, header_size = 0;
    uint16_t version = 0, flags = 0;
    uint64_t generation = 0;
    if (!ReadScalar(record.bytes, payload_limit, &offset, &magic) ||
        !ReadScalar(record.bytes, payload_limit, &offset, &version) ||
        !ReadScalar(record.bytes, payload_limit, &offset, &flags) ||
        !ReadScalar(record.bytes, payload_limit, &offset, &name_bytes) ||
        !ReadScalar(record.bytes, payload_limit, &offset, &page_count) ||
        !ReadScalar(record.bytes, payload_limit, &offset, &page_size) ||
        !ReadScalar(record.bytes, payload_limit, &offset, &header_size) ||
        !ReadScalar(record.bytes, payload_limit, &offset, &generation) || magic != INDEX_SMO_MAGIC ||
        (version != 1 && version != 2 && version != 3) || flags != INDEX_SMO_FLAG_HEADER_IMAGE || name_bytes == 0 ||
        name_bytes > MAX_INDEX_SMO_FILE_NAME_BYTES || page_count == 0 || page_count > MAX_INDEX_SMO_PAGE_COUNT ||
        page_size != PAGE_SIZE || header_size != PAGE_SIZE || generation == 0)
        return false;
    if (version == 1) {
        uint32_t expected = 0;
        if (!CheckedExpectedV1Length(name_bytes, page_count, &expected) || expected != record.total_len)
            return false;
    }
    IndexLayout v3_layout;
    if (version == 3 &&
        (!ReadScalar(record.bytes, payload_limit, &offset, &v3_layout.col_tot_len) ||
         !ReadScalar(record.bytes, payload_limit, &offset, &v3_layout.btree_order) ||
         !ReadScalar(record.bytes, payload_limit, &offset, &v3_layout.keys_size) || !ValidateLayout(v3_layout)))
        return false;
    if (offset > payload_limit || name_bytes > payload_limit - offset)
        return false;
    const uint32_t name_offset = offset;
    const std::string_view name(record.bytes + offset, name_bytes);
    if (name.find('\0') != std::string_view::npos || name.find('/') != std::string_view::npos || name == "." ||
        name == "..")
        return false;
    offset += name_bytes;
    const uint32_t page_stream_offset = offset;
    IndexSmoLayoutCursor cursor(record.bytes, payload_limit, offset, version);
    page_id_t previous = INVALID_PAGE_ID;
    uint32_t decoded_count = 0, transformed_count = 0;
    std::array<char, PAGE_SIZE> scratch{};
    for (uint32_t index = 0; index < page_count; ++index) {
        page_id_t page = INVALID_PAGE_ID;
        ImageToken token;
        if (!cursor.NextPage(&page, &token) || (previous != INVALID_PAGE_ID && page <= previous))
            return false;
        previous = page;
        if (token.codec != ImageCodec::RAW)
            ++decoded_count;
        if (token.transformed)
            ++transformed_count;
        const char* image = nullptr;
        if (!ImagePointer(token, &scratch, &image) ||
            (version == 3 && token.transformed && !UntransformIxPage(v3_layout, &scratch)) ||
            (version == 3 && !ValidateIxPageImage(v3_layout, image)))
            return false;
    }
    ImageToken header;
    if (!cursor.NextHeader(&header) || cursor.offset() != payload_limit)
        return false;
    const uint32_t header_offset = header.encoded - record.bytes;
    if (header.codec != ImageCodec::RAW)
        ++decoded_count;
    const char* header_image = nullptr;
    if (!ImagePointer(header, &scratch, &header_image))
        return false;
    if (version == 3) {
        IndexLayout header_layout;
        if (!ReadHeaderLayout(header_image, &header_layout) || !SameLayout(v3_layout, header_layout))
            return false;
    }
    IndexSmoWalLayout layout;
    layout.version_ = version;
    layout.total_len_ = record.total_len;
    layout.payload_limit_ = payload_limit;
    layout.name_offset_ = name_offset;
    layout.name_bytes_ = name_bytes;
    layout.page_stream_offset_ = page_stream_offset;
    layout.header_offset_ = header_offset;
    layout.page_count_ = page_count;
    layout.decoded_count_ = decoded_count;
    layout.transformed_count_ = transformed_count;
    layout.generation_ = generation;
    layout.col_tot_len_ = v3_layout.col_tot_len;
    layout.btree_order_ = v3_layout.btree_order;
    layout.keys_size_ = v3_layout.keys_size;
    *out = layout;
    return true;
}

bool IndexSmoWalLayout::copy_catalog(const WalRecordView& record, IndexSmoWalView::Page* pages,
                                     size_t page_capacity) const noexcept {
    if (record.bytes == nullptr || record.total_len != total_len_ || pages == nullptr || page_capacity < page_count_ ||
        payload_limit_ > record.total_len || page_stream_offset_ > payload_limit_)
        return false;
    IndexSmoLayoutCursor cursor(record.bytes, payload_limit_, page_stream_offset_, version_);
    // First pass makes the no-partial-publish guarantee independent of an
    // arena's previous contents. The second pass is safe under the immutable
    // binding above and only publishes fully validated identities.
    for (uint32_t i = 0; i < page_count_; ++i) {
        page_id_t page = INVALID_PAGE_ID;
        ImageToken token;
        if (!cursor.NextPage(&page, &token))
            return false;
    }
    ImageToken header;
    if (!cursor.NextHeader(&header) || cursor.offset() != payload_limit_)
        return false;
    cursor = IndexSmoLayoutCursor(record.bytes, payload_limit_, page_stream_offset_, version_);
    for (uint32_t i = 0; i < page_count_; ++i) {
        ImageToken token;
        if (!cursor.NextPage(&pages[i].page_no, &token))
            return false;
        pages[i].image = nullptr;
    }
    return true;
}

bool CopyIndexSmoPageCatalog(const WalRecordView& record, IndexSmoWalView::Page* pages, size_t page_capacity) noexcept {
    IndexSmoWalLayout fresh;
    return InspectIndexSmoWal(record, &fresh) && fresh.copy_catalog(record, pages, page_capacity);
}

bool IndexSmoWalLayout::decode_materialized(const WalRecordView& record, const IndexSmoWalDecodeStorage& storage,
                                            IndexSmoWalDecodedView* out) const noexcept {
    if (out == nullptr || record.bytes == nullptr || record.total_len != total_len_ || storage.pages == nullptr ||
        storage.page_capacity < page_count_ ||
        (decoded_count_ != 0 && (storage.decoded_pages == nullptr || storage.decoded_capacity < decoded_count_)))
        return false;
    // Inspect just decoded and validated these immutable bytes. Materializing
    // each compressed image below is therefore its only additional codec pass.
    IndexSmoLayoutCursor cursor(record.bytes, payload_limit_, page_stream_offset_, version_);
    size_t decoded = 0;
    for (uint32_t i = 0; i < page_count_; ++i) {
        ImageToken token;
        if (!cursor.NextPage(&storage.pages[i].page_no, &token))
            return false;
        if (token.codec == ImageCodec::RAW)
            storage.pages[i].image = token.encoded;
        else {
            auto* image = &storage.decoded_pages[decoded++];
            if (!DecodeImageToken(token, image) ||
                (token.transformed && !UntransformIxPage({col_tot_len_, btree_order_, keys_size_}, image)))
                return false;
            storage.pages[i].image = image->data();
        }
    }
    ImageToken header;
    if (!cursor.NextHeader(&header))
        return false;
    const char* header_image = header.encoded;
    if (header.codec != ImageCodec::RAW) {
        auto* image = &storage.decoded_pages[decoded++];
        if (!DecodeImageToken(header, image))
            return false;
        header_image = image->data();
    }
    if (decoded != decoded_count_ || cursor.offset() != payload_limit_)
        return false;
    IndexSmoWalDecodedView decoded_view{std::string_view(record.bytes + name_offset_, name_bytes_), generation_, page_count_,
                                        header_image, storage.pages};
    *out = decoded_view;
    return true;
}

bool DecodeIndexSmoWal(const WalRecordView& record, const IndexSmoWalDecodeStorage& storage,
                       IndexSmoWalDecodedView* out) noexcept {
    IndexSmoWalLayout fresh;
    return InspectIndexSmoWal(record, &fresh) && fresh.decode_materialized(record, storage, out);
}

bool ParseIndexSmoWal(const WalRecordView& record, IndexSmoWalView* out) {
    if (out == nullptr)
        return false;
    IndexSmoWalLayout layout;
    if (!InspectIndexSmoWal(record, &layout))
        return false;
    IndexSmoWalView parsed;
    parsed.pages_.resize(layout.page_count_);
    parsed.decoded_pages_.resize(layout.decoded_count_);
    IndexSmoWalDecodeStorage storage{parsed.pages_.data(), parsed.pages_.size(), parsed.decoded_pages_.data(),
                                     parsed.decoded_pages_.size()};
    IndexSmoWalDecodedView view;
    if (!layout.decode_materialized(record, storage, &view))
        return false;
    parsed.index_file_name = view.index_file_name;
    parsed.index_generation = view.index_generation;
    parsed.page_count = view.page_count;
    parsed.header_image = view.header_image;
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

IndexBindLogRecord::IndexBindLogRecord(std::string_view index_file_name, uint64_t existing_generation)
    : IndexBindLogRecord(index_file_name) {
    if (existing_generation == 0) {
        throw std::invalid_argument("INDEX_BIND V2 requires a nonzero existing generation");
    }
    version_ = INDEX_BIND_VERSION_V2;
    explicit_generation_ = existing_generation;
}

void IndexBindLogRecord::serialize(char* dest) const {
    LogRecord::serialize(dest);
    uint32_t offset = OFFSET_LOG_DATA;
    AppendScalar(dest, &offset, INDEX_BIND_MAGIC);
    AppendScalar(dest, &offset, version_);
    AppendScalar(dest, &offset, static_cast<uint16_t>(0));
    AppendScalar(dest, &offset, static_cast<uint32_t>(index_file_name_.size()));
    const uint64_t generation =
        version_ == INDEX_BIND_VERSION_V1 ? static_cast<uint64_t>(lsn_) + 1U : explicit_generation_;
    AppendScalar(dest, &offset, generation);
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
    if (magic != INDEX_BIND_MAGIC || (version != INDEX_BIND_VERSION_V1 && version != INDEX_BIND_VERSION_V2) ||
        reserved != 0 || name_bytes == 0 || name_bytes > MAX_INDEX_SMO_FILE_NAME_BYTES || parsed_generation == 0 ||
        record.lsn == INVALID_LSN ||
        (version == INDEX_BIND_VERSION_V1 && parsed_generation != static_cast<uint64_t>(record.lsn) + 1U) ||
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
