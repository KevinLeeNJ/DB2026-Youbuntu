#include "deltakernel/delta_database.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <set>
#include <sstream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/csv.h"
#include "system/sm_meta.h"

namespace deltakernel {
namespace {
thread_local const DeltaParameterFrame* active_prepared_parameters = nullptr;

struct PreparedParameterScope {
    const DeltaParameterFrame* previous;
    explicit PreparedParameterScope(const DeltaParameterFrame& parameters) : previous(active_prepared_parameters) {
        active_prepared_parameters = &parameters;
    }
    ~PreparedParameterScope() {
        active_prepared_parameters = previous;
    }
};

constexpr const char* kCatalog = "DELTA_CATALOG";
constexpr const char* kCatalogMagic = "DELTAKERNEL";
constexpr size_t kMaxRowBytes = 1U << 20;
constexpr size_t kMaxColumns = 256;
constexpr size_t kMaxTables = 65536;
constexpr uintmax_t kMaxCatalogBytes = 16U << 20;
constexpr size_t kMaxJoinMaterializedBytes = 64U << 20;
constexpr size_t kMaxJoinMaterializedRows = 1U << 20;
constexpr uint64_t kSidecarMagic = 0x58444941544c4544ULL; // DELTAIDX
constexpr uint32_t kSidecarFormatVersion = 3;
constexpr uint64_t kMaxSidecarBytes = 1ULL << 34;
constexpr uint32_t kSidecarHeaderBytes = 96;
constexpr uint32_t kSidecarEntryBytes = 16;
constexpr uint32_t kSidecarRowOrderEntryBytes = 4;
constexpr uint32_t kSidecarHeaderCrcOffset = 92;
constexpr size_t kCommitBatchSize = 32;
constexpr size_t kCommitQueueLimit = 128;

enum class DiagnosticOperation : size_t { TxnControl, PointDml, ScanJoinAggregate, Other, Count };

DiagnosticOperation ClassifyDiagnosticOperation(ast::AstType type) {
    switch (type) {
    case ast::AstType::TxnBegin:
    case ast::AstType::TxnCommit:
    case ast::AstType::TxnAbort:
    case ast::AstType::TxnRollback:
    case ast::AstType::SetTransaction:
        return DiagnosticOperation::TxnControl;
    case ast::AstType::InsertStmt:
    case ast::AstType::UpdateStmt:
    case ast::AstType::DeleteStmt:
        return DiagnosticOperation::PointDml;
    case ast::AstType::SelectStmt:
        return DiagnosticOperation::ScanJoinAggregate;
    default:
        return DiagnosticOperation::Other;
    }
}

uint64_t NsSince(std::chrono::steady_clock::time_point start) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count());
}

void RecordMax(std::atomic<uint64_t>& maximum, uint64_t value) noexcept {
    uint64_t observed = maximum.load(std::memory_order_relaxed);
    while (observed < value &&
           !maximum.compare_exchange_weak(observed, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

struct SidecarHeader {
    uint64_t magic;
    uint32_t format_version;
    uint32_t header_bytes;
    uint32_t entry_bytes;
    uint32_t table_id;
    uint32_t constraint_id;
    uint64_t generation;
    uint64_t snapshot_epoch;
    uint64_t count;
    uint64_t total_bytes;
    uint64_t key_bytes;
    uint64_t row_order_offset;
    uint32_t entries_crc;
    uint32_t keys_crc;
    uint32_t row_order_crc;
    uint32_t header_crc;
};

constexpr std::array<uint32_t, 256> MakeCrc32Table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t value = 0; value < table.size(); ++value) {
        uint32_t crc = value;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1U) ^ (0xedb88320U & (0U - (crc & 1U)));
        table[value] = crc;
    }
    return table;
}
constexpr auto kCrc32Table = MakeCrc32Table();

uint32_t UpdateCrc32(uint32_t crc, const uint8_t* data, size_t size) {
    for (size_t n = 0; n < size; ++n)
        crc = (crc >> 8U) ^ kCrc32Table[(crc ^ data[n]) & 0xffU];
    return crc;
}

uint32_t Crc32(const void* data, size_t size) {
    return ~UpdateCrc32(0xffffffffU, static_cast<const uint8_t*>(data), size);
}

uint32_t Crc32(const std::string& bytes) {
    return Crc32(bytes.data(), bytes.size());
}

template <typename T> void PutLe(std::vector<uint8_t>& out, T value) {
    using U = typename std::make_unsigned<T>::type;
    U bits = static_cast<U>(value);
    for (size_t i = 0; i < sizeof(T); ++i)
        out.push_back(static_cast<uint8_t>(bits >> (i * 8)));
}

template <typename T> T GetLeAt(const uint8_t* bytes) {
    using U = typename std::make_unsigned<T>::type;
    U value = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
        value |= static_cast<U>(bytes[i]) << (i * 8);
    return static_cast<T>(value);
}

template <typename T> void PutLeAt(std::array<uint8_t, kSidecarHeaderBytes>& bytes, size_t offset, T value) {
    using U = typename std::make_unsigned<T>::type;
    const U bits = static_cast<U>(value);
    for (size_t i = 0; i < sizeof(T); ++i)
        bytes[offset + i] = static_cast<uint8_t>(bits >> (i * 8));
}

uint64_t AlignUp(uint64_t value, uint64_t alignment) {
    const uint64_t remainder = value % alignment;
    if (remainder == 0)
        return value;
    if (value > std::numeric_limits<uint64_t>::max() - (alignment - remainder))
        throw std::overflow_error("sidecar offset overflow");
    return value + alignment - remainder;
}

bool CheckedAdd(uint64_t left, uint64_t right, uint64_t* result) {
    if (left > std::numeric_limits<uint64_t>::max() - right)
        return false;
    *result = left + right;
    return true;
}

bool CheckedMultiply(uint64_t left, uint64_t right, uint64_t* result) {
    if (right != 0 && left > std::numeric_limits<uint64_t>::max() / right)
        return false;
    *result = left * right;
    return true;
}

std::array<uint8_t, kSidecarHeaderBytes> EncodeSidecarHeader(const SidecarHeader& header) {
    std::array<uint8_t, kSidecarHeaderBytes> bytes{};
    PutLeAt<uint64_t>(bytes, 0, header.magic);
    PutLeAt<uint32_t>(bytes, 8, header.format_version);
    PutLeAt<uint32_t>(bytes, 12, header.header_bytes);
    PutLeAt<uint32_t>(bytes, 16, header.entry_bytes);
    PutLeAt<uint32_t>(bytes, 20, header.table_id);
    PutLeAt<uint32_t>(bytes, 24, header.constraint_id);
    PutLeAt<uint32_t>(bytes, 28, 0);
    PutLeAt<uint64_t>(bytes, 32, header.generation);
    PutLeAt<uint64_t>(bytes, 40, header.snapshot_epoch);
    PutLeAt<uint64_t>(bytes, 48, header.count);
    PutLeAt<uint64_t>(bytes, 56, header.total_bytes);
    PutLeAt<uint64_t>(bytes, 64, header.key_bytes);
    PutLeAt<uint64_t>(bytes, 72, header.row_order_offset);
    PutLeAt<uint32_t>(bytes, 80, header.entries_crc);
    PutLeAt<uint32_t>(bytes, 84, header.keys_crc);
    PutLeAt<uint32_t>(bytes, 88, header.row_order_crc);
    PutLeAt<uint32_t>(bytes, 92, header.header_crc);
    return bytes;
}

SidecarHeader DecodeSidecarHeader(const uint8_t* bytes) {
    return {GetLeAt<uint64_t>(bytes),      GetLeAt<uint32_t>(bytes + 8),  GetLeAt<uint32_t>(bytes + 12),
            GetLeAt<uint32_t>(bytes + 16), GetLeAt<uint32_t>(bytes + 20), GetLeAt<uint32_t>(bytes + 24),
            GetLeAt<uint64_t>(bytes + 32), GetLeAt<uint64_t>(bytes + 40), GetLeAt<uint64_t>(bytes + 48),
            GetLeAt<uint64_t>(bytes + 56), GetLeAt<uint64_t>(bytes + 64), GetLeAt<uint64_t>(bytes + 72),
            GetLeAt<uint32_t>(bytes + 80), GetLeAt<uint32_t>(bytes + 84), GetLeAt<uint32_t>(bytes + 88),
            GetLeAt<uint32_t>(bytes + 92)};
}

template <typename T> void PutBe(std::vector<uint8_t>& out, T value) {
    using U = typename std::make_unsigned<T>::type;
    const U bits = static_cast<U>(value);
    for (size_t i = sizeof(T); i-- > 0;)
        out.push_back(static_cast<uint8_t>(bits >> (i * 8)));
}

std::vector<uint8_t> PrefixSuccessor(std::vector<uint8_t> key) {
    while (!key.empty() && key.back() == 0xff)
        key.pop_back();
    if (key.empty())
        return {};
    ++key.back();
    return key;
}

template <typename T> T GetLe(const std::vector<uint8_t>& bytes, size_t& offset) {
    using U = typename std::make_unsigned<T>::type;
    if (offset > bytes.size() || bytes.size() - offset < sizeof(T))
        throw std::runtime_error("truncated Delta row");
    U value = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
        value |= static_cast<U>(bytes[offset++]) << (i * 8);
    return static_cast<T>(value);
}

template <typename... T> bool ParseLine(const std::string& line, T&... values) {
    std::istringstream input(line);
    std::string extra;
    if (!(input >> ... >> values))
        return false;
    return !(input >> extra);
}

bool CompareResult(int comparison, ast::SvCompOp op) {
    switch (op) {
    case ast::SV_OP_EQ:
        return comparison == 0;
    case ast::SV_OP_NE:
        return comparison != 0;
    case ast::SV_OP_LT:
        return comparison < 0;
    case ast::SV_OP_GT:
        return comparison > 0;
    case ast::SV_OP_LE:
        return comparison <= 0;
    case ast::SV_OP_GE:
        return comparison >= 0;
    default:
        return false;
    }
}

void AppendOverlay(DeltaOverlay& overlay, DeltaOverlayKey key, uint64_t local_id) {
    auto& ids = overlay[std::move(key)];
    const auto position = std::lower_bound(ids.begin(), ids.end(), local_id);
    if (position == ids.end() || *position != local_id)
        ids.insert(position, local_id);
}

void AppendOverlay(CommittedOverlay& overlay, DeltaOverlayKey key, uint64_t local_id) {
    overlay.emplace(std::move(key), std::vector<uint64_t>{local_id});
}

// Per-node RowId lists are sorted and unique. The common one-node case advances in O(1); multiple committed tickets
// with the same key are merged without query-time allocation by probing one sorted position per node.
template <typename Overlay> class OverlayRangeCursor {
public:
    OverlayRangeCursor(const Overlay& overlay, epoch_si_poc::TableId table_id, epoch_si_poc::ConstraintId constraint_id,
                       const EncodedKey& first, const EncodedKey& last, bool reverse, size_t& nodes, size_t& refs,
                       size_t& refs_examined)
        : table_id_(table_id), reverse_(reverse), begin_(overlay.lower_bound({table_id, constraint_id, first})),
          end_(last.empty() ? ScopeEnd(overlay, table_id, constraint_id)
                            : overlay.lower_bound({table_id, constraint_id, last})),
          current_(reverse ? end_ : begin_), nodes_(nodes), refs_(refs), refs_examined_(refs_examined) {
        if (!last.empty() && !(first < last)) {
            begin_ = end_;
            current_ = end_;
            return;
        }
        if (reverse)
            LoadReverseKey();
        else
            LoadForwardKey();
    }

    bool valid() const {
        return valid_;
    }
    const EncodedKey& key() const {
        return *key_;
    }
    epoch_si_poc::RowId id() const {
        return {table_id_, id_};
    }
    void Advance() {
        if (!valid_)
            return;
        if (!SelectId(false)) {
            if (!reverse_)
                LoadForwardKey();
            else
                LoadReverseKey();
        }
    }

private:
    using Iterator = typename Overlay::const_iterator;

    static Iterator ScopeEnd(const Overlay& overlay, epoch_si_poc::TableId table_id,
                             epoch_si_poc::ConstraintId constraint_id) {
        if (constraint_id != std::numeric_limits<epoch_si_poc::ConstraintId>::max())
            return overlay.lower_bound({table_id, constraint_id + 1, {}});
        if (table_id != std::numeric_limits<epoch_si_poc::TableId>::max())
            return overlay.lower_bound({table_id + 1, 0, {}});
        return overlay.end();
    }
    size_t UpperBound(const std::vector<uint64_t>& ids, uint64_t value) {
        size_t first = 0, last = ids.size();
        while (first < last) {
            const size_t middle = first + (last - first) / 2;
            ++refs_examined_;
            if (ids[middle] <= value)
                first = middle + 1;
            else
                last = middle;
        }
        return first;
    }
    size_t LowerBound(const std::vector<uint64_t>& ids, uint64_t value) {
        size_t first = 0, last = ids.size();
        while (first < last) {
            const size_t middle = first + (last - first) / 2;
            ++refs_examined_;
            if (ids[middle] < value)
                first = middle + 1;
            else
                last = middle;
        }
        return first;
    }
    bool SelectId(bool first) {
        Iterator after_first = group_begin_;
        if (after_first != group_end_ && ++after_first == group_end_) {
            const auto& ids = group_begin_->second;
            if (ids.empty()) {
                valid_ = false;
                return false;
            }
            if (first)
                single_position_ = reverse_ ? ids.size() - 1 : 0;
            else if (!reverse_) {
                if (++single_position_ == ids.size()) {
                    valid_ = false;
                    return false;
                }
            } else {
                if (single_position_ == 0) {
                    valid_ = false;
                    return false;
                }
                --single_position_;
            }
            ++refs_examined_;
            id_ = ids[single_position_];
            ++refs_;
            valid_ = true;
            return true;
        }
        bool found = false;
        uint64_t selected = 0;
        for (Iterator node = group_begin_; node != group_end_; ++node) {
            const auto& ids = node->second;
            if (ids.empty())
                continue;
            size_t position = 0;
            if (first) {
                position = reverse_ ? ids.size() - 1 : 0;
            } else if (!reverse_) {
                position = UpperBound(ids, id_);
                if (position == ids.size())
                    continue;
            } else {
                position = LowerBound(ids, id_);
                if (position == 0)
                    continue;
                --position;
            }
            ++refs_examined_;
            const uint64_t candidate = ids[position];
            if (!found || (reverse_ ? candidate > selected : candidate < selected)) {
                selected = candidate;
                found = true;
            }
        }
        valid_ = found;
        if (found) {
            id_ = selected;
            ++refs_;
        }
        return found;
    }
    void LoadForwardKey() {
        if (current_ == end_) {
            valid_ = false;
            return;
        }
        group_begin_ = current_;
        key_ = &std::get<2>(current_->first);
        do {
            ++nodes_;
            ++current_;
        } while (current_ != end_ && std::get<2>(current_->first) == *key_);
        group_end_ = current_;
        if (!SelectId(true))
            LoadForwardKey();
    }
    void LoadReverseKey() {
        if (current_ == begin_) {
            valid_ = false;
            return;
        }
        Iterator node = current_;
        --node;
        group_end_ = current_;
        key_ = &std::get<2>(node->first);
        for (;;) {
            ++nodes_;
            current_ = node;
            if (node == begin_)
                break;
            Iterator previous = node;
            --previous;
            if (std::get<2>(previous->first) != *key_)
                break;
            node = previous;
        }
        group_begin_ = current_;
        if (!SelectId(true))
            LoadReverseKey();
    }

    epoch_si_poc::TableId table_id_;
    bool reverse_;
    Iterator begin_;
    Iterator end_;
    Iterator current_;
    Iterator group_begin_;
    Iterator group_end_;
    const EncodedKey* key_ = nullptr;
    size_t& nodes_;
    size_t& refs_;
    size_t& refs_examined_;
    uint64_t id_ = 0;
    size_t single_position_ = 0;
    bool valid_ = false;
};

} // namespace

DeltaDatabase::SidecarDescriptor::SidecarDescriptor(epoch_si_poc::TableId table_id,
                                                    epoch_si_poc::ConstraintId constraint_id, uint64_t generation,
                                                    epoch_si_poc::Epoch snapshot_epoch, uint64_t count,
                                                    uint64_t key_bytes, uint64_t row_order_offset, size_t mapped_bytes,
                                                    void* mapping)
    : table_id(table_id), constraint_id(constraint_id), generation(generation), snapshot_epoch(snapshot_epoch),
      count(count), key_bytes(key_bytes), row_order_offset(row_order_offset), mapped_bytes(mapped_bytes),
      mapping(mapping), live_bitmap(static_cast<size_t>((count + 63) / 64), ~uint64_t{0}) {
    if (mapping && count != 0) {
        const auto* mapped = static_cast<const uint8_t*>(mapping);
        const uint32_t ordinal =
            GetLeAt<uint32_t>(mapped + row_order_offset + (count - 1) * kSidecarRowOrderEntryBytes);
        max_local_id =
            GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes + static_cast<uint64_t>(ordinal) * kSidecarEntryBytes + 8);
    }
    if (!live_bitmap.empty() && count % 64 != 0)
        live_bitmap.back() = (uint64_t{1} << (count % 64)) - 1;
    for (size_t words = live_bitmap.size(); words > 1; words = (words + 63) / 64)
        live_summary.emplace_back((words + 63) / 64, ~uint64_t{0});
    for (size_t level = 0; level < live_summary.size(); ++level) {
        const size_t child_words = level == 0 ? live_bitmap.size() : live_summary[level - 1].size();
        auto& summary = live_summary[level];
        for (size_t word = 0; word < summary.size(); ++word) {
            const size_t remaining = child_words > word * 64 ? child_words - word * 64 : 0;
            summary[word] = remaining >= 64 ? ~uint64_t{0} : (remaining == 0 ? 0 : (uint64_t{1} << remaining) - 1);
        }
    }
}

DeltaDatabase::SidecarDescriptor::~SidecarDescriptor() {
    if (mapping)
        munmap(mapping, mapped_bytes);
}

DeltaDatabase::SidecarDescriptor::SidecarDescriptor(SidecarDescriptor&& other) noexcept
    : table_id(other.table_id), constraint_id(other.constraint_id), generation(other.generation),
      snapshot_epoch(other.snapshot_epoch), count(other.count), key_bytes(other.key_bytes),
      row_order_offset(other.row_order_offset), max_local_id(other.max_local_id), mapped_bytes(other.mapped_bytes),
      mapping(other.mapping), live_bitmap(std::move(other.live_bitmap)), live_summary(std::move(other.live_summary)) {
    other.mapping = nullptr;
    other.mapped_bytes = 0;
}

DeltaDatabase::SidecarDescriptor& DeltaDatabase::SidecarDescriptor::operator=(SidecarDescriptor&& other) noexcept {
    if (this == &other)
        return *this;
    if (mapping)
        munmap(mapping, mapped_bytes);
    table_id = other.table_id;
    constraint_id = other.constraint_id;
    generation = other.generation;
    snapshot_epoch = other.snapshot_epoch;
    count = other.count;
    key_bytes = other.key_bytes;
    row_order_offset = other.row_order_offset;
    max_local_id = other.max_local_id;
    mapped_bytes = other.mapped_bytes;
    mapping = other.mapping;
    live_bitmap = std::move(other.live_bitmap);
    live_summary = std::move(other.live_summary);
    other.mapping = nullptr;
    other.mapped_bytes = 0;
    return *this;
}

DeltaDatabase::DeltaDatabase(epoch_si_poc::CheckpointDb db) : db_(std::move(db)) {
    const char* enabled = std::getenv("RMDB_DELTA_DIAGNOSTICS");
    if (enabled != nullptr && *enabled != '\0' && std::strcmp(enabled, "0") != 0) {
        diagnostics_ = std::make_shared<epoch_si_poc::DeltaDiagnostics>();
        report_diagnostics_ = true;
        db_.engine().SetDiagnostics(diagnostics_);
        // Opportunistic snapshots: emitted by a completed Execute at most once per interval.
        // This deliberately avoids a background diagnostics thread.
        const char* period = std::getenv("RMDB_DELTA_DIAGNOSTICS_MIN_REPORT_INTERVAL_MS");
        if (period != nullptr) {
            char* end = nullptr;
            const unsigned long long milliseconds = std::strtoull(period, &end, 10);
            if (end != period && *end == '\0' && milliseconds != 0 &&
                milliseconds <= std::numeric_limits<uint64_t>::max() / 1000000ULL) {
                diagnostics_period_ns_ = milliseconds * 1000000ULL;
            }
        }
    }
}

DeltaDatabase::~DeltaDatabase() {
    ReportDiagnostics();
}

std::shared_lock<std::shared_mutex> DeltaDatabase::LockExecutionShared() {
    std::unique_lock<std::mutex> turnstile(execution_turnstile_);
    std::shared_lock<std::shared_mutex> lock(execution_gate_);
    return lock;
}

std::unique_lock<std::shared_mutex> DeltaDatabase::LockExecutionUnique() {
    std::unique_lock<std::mutex> turnstile(execution_turnstile_);
    std::unique_lock<std::shared_mutex> lock(execution_gate_, std::defer_lock);
    if (!lock.try_lock()) {
        if (execution_writer_wait_hook_for_test_)
            execution_writer_wait_hook_for_test_();
        lock.lock();
    }
    return lock;
}

std::shared_lock<std::shared_mutex> DeltaDatabase::LockStateShared(bool report_blocked) const {
    std::unique_lock<std::mutex> turnstile(state_turnstile_, std::defer_lock);
    bool reported = false;
    if (!turnstile.try_lock()) {
        if (report_blocked && execute_blocked_hook_for_test_) {
            execute_blocked_hook_for_test_();
            reported = true;
        }
        turnstile.lock();
    }
    std::shared_lock<std::shared_mutex> lock(state_mutex_, std::defer_lock);
    if (!lock.try_lock()) {
        if (report_blocked && !reported && execute_blocked_hook_for_test_)
            execute_blocked_hook_for_test_();
        lock.lock();
    }
    return lock;
}

std::unique_lock<std::shared_mutex> DeltaDatabase::LockStateUnique() const {
    const auto started = diagnostics_ ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    std::unique_lock<std::mutex> turnstile(state_turnstile_);
    if (diagnostics_)
        writer_turnstile_wait_ns_.fetch_add(NsSince(started), std::memory_order_relaxed);
    std::unique_lock<std::shared_mutex> lock(state_mutex_, std::defer_lock);
    if (!lock.try_lock()) {
        if (state_writer_wait_hook_for_test_)
            state_writer_wait_hook_for_test_();
        lock.lock();
    }
    return lock;
}

void DeltaDatabase::RecordPreparedClone(uint64_t elapsed_ns) noexcept {
    if (!diagnostics_)
        return;
    prepared_clone_count_.fetch_add(1, std::memory_order_relaxed);
    prepared_clone_ns_.fetch_add(elapsed_ns, std::memory_order_relaxed);
}

void DeltaDatabase::RecordPreparedNative() noexcept {
    if (diagnostics_)
        prepared_native_count_.fetch_add(1, std::memory_order_relaxed);
}

void DeltaDatabase::RecordPreparedFallback() noexcept {
    if (diagnostics_)
        prepared_fallback_count_.fetch_add(1, std::memory_order_relaxed);
}

void DeltaDatabase::EnableDiagnosticsForTest() {
    auto state_lock = LockStateUnique();
    if (!diagnostics_) {
        diagnostics_ = std::make_shared<epoch_si_poc::DeltaDiagnostics>();
        db_.engine().SetDiagnostics(diagnostics_);
    }
}

std::array<uint64_t, 6> DeltaDatabase::QueryDiagnosticsForTest() const {
    return {sidecar_base_entries_.load(std::memory_order_relaxed),
            join_parameterized_probes_.load(std::memory_order_relaxed),
            join_parameterized_.load(std::memory_order_relaxed), join_fallback_.load(std::memory_order_relaxed),
            ordered_stream_.load(std::memory_order_relaxed), ordered_materialize_.load(std::memory_order_relaxed)};
}

std::array<uint64_t, 5> DeltaDatabase::RouteDiagnosticsForTest() const {
    if (!diagnostics_)
        return {};
    return {diagnostics_->current_index_latest_routes.load(std::memory_order_relaxed),
            diagnostics_->current_index_historical_routes.load(std::memory_order_relaxed),
            diagnostics_->current_live_base_candidates.load(std::memory_order_relaxed),
            diagnostics_->current_live_summary_skips.load(std::memory_order_relaxed),
            diagnostics_->current_live_summary_words.load(std::memory_order_relaxed)};
}

std::array<uint64_t, 8> DeltaDatabase::TailDiagnosticsForTest() const {
    if (!diagnostics_)
        return {};
    return {execute_shared_wait_max_ns_.load(std::memory_order_relaxed),
            commit_ready_wait_max_ns_.load(std::memory_order_relaxed),
            commit_initial_unique_wait_max_ns_.load(std::memory_order_relaxed),
            commit_publish_unique_wait_max_ns_.load(std::memory_order_relaxed),
            commit_prepare_unique_max_ns_.load(std::memory_order_relaxed),
            commit_sync_unlocked_max_ns_.load(std::memory_order_relaxed),
            commit_publish_unique_max_ns_.load(std::memory_order_relaxed),
            diagnostics_->wal_fdatasync_max_ns.load(std::memory_order_relaxed)};
}

std::array<uint64_t, 5> DeltaDatabase::InflightDiagnosticsForTest() const {
    return {execute_inflight_[0].load(std::memory_order_relaxed), execute_inflight_[1].load(std::memory_order_relaxed),
            execute_inflight_[2].load(std::memory_order_relaxed), execute_inflight_[3].load(std::memory_order_relaxed),
            commit_phase_.load(std::memory_order_relaxed)};
}

void DeltaDatabase::MaybeReportDiagnostics() const noexcept {
    if (diagnostics_period_ns_ == 0)
        return;
    const uint64_t now = NsSince(std::chrono::steady_clock::time_point{});
    uint64_t previous = diagnostics_last_report_ns_.load(std::memory_order_relaxed);
    while (now - previous >= diagnostics_period_ns_) {
        if (diagnostics_last_report_ns_.compare_exchange_weak(previous, now, std::memory_order_relaxed,
                                                              std::memory_order_relaxed)) {
            ReportDiagnostics(true);
            return;
        }
    }
}

void DeltaDatabase::ReportDiagnostics(bool periodic) const noexcept {
    if (!report_diagnostics_)
        return;
    struct rusage usage {};
    getrusage(RUSAGE_SELF, &usage);
    char histogram[512]{};
    size_t used = 0;
    for (size_t n = 0; n < diagnostics_->commit_batch_hist.size(); ++n) {
        const uint64_t count = diagnostics_->commit_batch_hist[n].load(std::memory_order_relaxed);
        if (count == 0)
            continue;
        const int written = std::snprintf(histogram + used, sizeof(histogram) - used, "%s%zu:%llu",
                                          used == 0 ? "" : ",", n, static_cast<unsigned long long>(count));
        if (written < 0 || static_cast<size_t>(written) >= sizeof(histogram) - used)
            break;
        used += static_cast<size_t>(written);
    }
    const auto load = [](const std::atomic<uint64_t>& value) { return value.load(std::memory_order_relaxed); };
    const uint64_t phase_code = load(commit_phase_);
    constexpr std::array<const char*, 6> kCommitPhaseNames = {"idle", "initial_wait", "prepare", "sync",
                                                               "publish_wait", "publish"};
    const char* phase_name = phase_code < kCommitPhaseNames.size() ? kCommitPhaseNames[phase_code] : "unknown";
    std::fprintf(stderr, "DELTA_PREPARED_DIAGNOSTICS prepared_native_count=%llu prepared_fallback_count=%llu\n",
                 static_cast<unsigned long long>(load(prepared_native_count_)),
                 static_cast<unsigned long long>(load(prepared_fallback_count_)));
    std::fprintf(stderr,
                 "DELTA_DIAGNOSTICS report=%s prepared_clone_count=%llu prepared_clone_ns=%llu "
                 "execute_txn_calls=%llu execute_point_dml_calls=%llu execute_scan_join_aggregate_calls=%llu "
                 "execute_other_calls=%llu execute_txn_wall_ns=%llu execute_point_dml_wall_ns=%llu "
                 "execute_scan_join_aggregate_wall_ns=%llu execute_other_wall_ns=%llu execute_shared_wait_ns=%llu "
                 "execute_shared_calls=%llu state_unique_commit_wait_ns=%llu state_unique_commit_batches=%llu "
                 "peak_inflight_execute=%llu writer_turnstile_wait_ns=%llu "
                 "immutable_reads=%llu immutable_bytes=%llu immutable_pread_ns=%llu immutable_decode_ns=%llu "
                 "private_hits=%llu version_hits=%llu base_hits=%llu sidecar_base_entries=%llu sidecar_overlay_refs=%llu "
                 "join_parameterized_queries=%llu join_fallback_queries=%llu ordered_early_stop=%llu "
                 "join_outer_indexed_queries=%llu join_outer_candidates=%llu join_outer_full_scan_rows=%llu "
                 "join_inferred_literal_components=%llu join_inferred_literal_probes=%llu "
                 "ordered_stream_queries=%llu ordered_materialize_queries=%llu "
                 "commit_queue_wait_ns=%llu commit_ready_wait_ns=%llu commit_leader_reacquire_wait_ns=%llu "
                 "prepare_unique_ns=%llu sync_unlocked_ns=%llu reacquire_ns=%llu publish_unique_ns=%llu "
                 "commit_frames=%llu commit_tickets=%llu commit_batch_hist=%s commit_encode_ns=%llu "
                 "commit_prepare_ns=%llu wal_pwrite_calls=%llu wal_pwrite_bytes=%llu wal_pwrite_ns=%llu "
                 "wal_fdatasync_calls=%llu wal_fdatasync_ns=%llu commit_install_ns=%llu commit_publish_ns=%llu "
                 "execute_txn_max_ns=%llu execute_point_dml_max_ns=%llu execute_scan_join_aggregate_max_ns=%llu "
                 "execute_other_max_ns=%llu execute_shared_wait_max_ns=%llu commit_ready_wait_max_ns=%llu "
                 "initial_unique_wait_max_ns=%llu publish_unique_wait_max_ns=%llu prepare_unique_max_ns=%llu "
                 "sync_unlocked_max_ns=%llu publish_unique_max_ns=%llu wal_fdatasync_max_ns=%llu "
                 "current_index_latest_routes=%llu current_index_historical_routes=%llu "
                 "current_live_base_candidates=%llu current_live_summary_skips=%llu current_live_summary_words=%llu "
                 "current_overlay_addition_ids=%llu current_overlay_removal_ids=%llu current_base_bit_flips=%llu "
                 "current_base_row_order_comparisons=%llu "
                 "execute_txn_inflight=%llu execute_point_dml_inflight=%llu "
                 "execute_scan_join_aggregate_inflight=%llu execute_other_inflight=%llu "
                 "commit_phase_code=%llu commit_phase=%s "
                 "minor_faults=%ld major_faults=%ld max_rss_kb=%ld\n",
                 periodic ? "periodic" : "final",
                 static_cast<unsigned long long>(load(prepared_clone_count_)),
                 static_cast<unsigned long long>(load(prepared_clone_ns_)),
                 static_cast<unsigned long long>(load(execute_calls_[0])),
                 static_cast<unsigned long long>(load(execute_calls_[1])),
                 static_cast<unsigned long long>(load(execute_calls_[2])),
                 static_cast<unsigned long long>(load(execute_calls_[3])),
                 static_cast<unsigned long long>(load(execute_ns_[0])),
                 static_cast<unsigned long long>(load(execute_ns_[1])),
                 static_cast<unsigned long long>(load(execute_ns_[2])),
                 static_cast<unsigned long long>(load(execute_ns_[3])),
                 static_cast<unsigned long long>(load(execute_shared_wait_ns_)),
                 static_cast<unsigned long long>(load(execute_shared_calls_)),
                 static_cast<unsigned long long>(load(state_unique_commit_wait_ns_)),
                 static_cast<unsigned long long>(load(state_unique_commit_batches_)),
                 static_cast<unsigned long long>(load(peak_inflight_execute_)),
                 static_cast<unsigned long long>(load(writer_turnstile_wait_ns_)),
                 static_cast<unsigned long long>(load(diagnostics_->immutable_reads)),
                 static_cast<unsigned long long>(load(diagnostics_->immutable_bytes)),
                 static_cast<unsigned long long>(load(diagnostics_->immutable_pread_ns)),
                 static_cast<unsigned long long>(load(diagnostics_->immutable_decode_ns)),
                 static_cast<unsigned long long>(load(diagnostics_->private_hits)),
                 static_cast<unsigned long long>(load(diagnostics_->version_hits)),
                 static_cast<unsigned long long>(load(diagnostics_->base_hits)),
                 static_cast<unsigned long long>(load(sidecar_base_entries_)),
                 static_cast<unsigned long long>(load(sidecar_overlay_refs_)),
                 static_cast<unsigned long long>(load(join_parameterized_)),
                 static_cast<unsigned long long>(load(join_fallback_)),
                 static_cast<unsigned long long>(load(ordered_early_stop_)),
                 static_cast<unsigned long long>(load(join_outer_indexed_)),
                 static_cast<unsigned long long>(load(join_outer_candidates_)),
                 static_cast<unsigned long long>(load(join_outer_full_scan_rows_)),
                 static_cast<unsigned long long>(load(join_inferred_literal_components_)),
                 static_cast<unsigned long long>(load(join_inferred_literal_probes_)),
                 static_cast<unsigned long long>(load(ordered_stream_)),
                 static_cast<unsigned long long>(load(ordered_materialize_)),
                 static_cast<unsigned long long>(load(commit_queue_wait_ns_)),
                 static_cast<unsigned long long>(load(commit_ready_wait_ns_)),
                 static_cast<unsigned long long>(load(commit_leader_reacquire_wait_ns_)),
                 static_cast<unsigned long long>(load(commit_prepare_unique_ns_)),
                 static_cast<unsigned long long>(load(commit_sync_unlocked_ns_)),
                 static_cast<unsigned long long>(load(commit_reacquire_ns_)),
                 static_cast<unsigned long long>(load(commit_publish_unique_ns_)),
                 static_cast<unsigned long long>(load(diagnostics_->commit_frames)),
                 static_cast<unsigned long long>(load(diagnostics_->commit_tickets)), histogram,
                 static_cast<unsigned long long>(load(diagnostics_->commit_encode_ns)),
                 static_cast<unsigned long long>(load(diagnostics_->commit_prepare_ns)),
                 static_cast<unsigned long long>(load(diagnostics_->wal_pwrite_calls)),
                 static_cast<unsigned long long>(load(diagnostics_->wal_pwrite_bytes)),
                 static_cast<unsigned long long>(load(diagnostics_->wal_pwrite_ns)),
                 static_cast<unsigned long long>(load(diagnostics_->wal_fdatasync_calls)),
                 static_cast<unsigned long long>(load(diagnostics_->wal_fdatasync_ns)),
                 static_cast<unsigned long long>(load(diagnostics_->commit_install_ns)),
                 static_cast<unsigned long long>(load(diagnostics_->commit_publish_ns)),
                 static_cast<unsigned long long>(load(execute_max_ns_[0])),
                 static_cast<unsigned long long>(load(execute_max_ns_[1])),
                 static_cast<unsigned long long>(load(execute_max_ns_[2])),
                 static_cast<unsigned long long>(load(execute_max_ns_[3])),
                 static_cast<unsigned long long>(load(execute_shared_wait_max_ns_)),
                 static_cast<unsigned long long>(load(commit_ready_wait_max_ns_)),
                 static_cast<unsigned long long>(load(commit_initial_unique_wait_max_ns_)),
                 static_cast<unsigned long long>(load(commit_publish_unique_wait_max_ns_)),
                 static_cast<unsigned long long>(load(commit_prepare_unique_max_ns_)),
                 static_cast<unsigned long long>(load(commit_sync_unlocked_max_ns_)),
                 static_cast<unsigned long long>(load(commit_publish_unique_max_ns_)),
                 static_cast<unsigned long long>(load(diagnostics_->wal_fdatasync_max_ns)),
                 static_cast<unsigned long long>(load(diagnostics_->current_index_latest_routes)),
                 static_cast<unsigned long long>(load(diagnostics_->current_index_historical_routes)),
                 static_cast<unsigned long long>(load(diagnostics_->current_live_base_candidates)),
                 static_cast<unsigned long long>(load(diagnostics_->current_live_summary_skips)),
                 static_cast<unsigned long long>(load(diagnostics_->current_live_summary_words)),
                 static_cast<unsigned long long>(load(diagnostics_->current_overlay_addition_ids)),
                 static_cast<unsigned long long>(load(diagnostics_->current_overlay_removal_ids)),
                 static_cast<unsigned long long>(load(diagnostics_->current_base_bit_flips)),
                 static_cast<unsigned long long>(load(current_base_row_order_comparisons_)),
                 static_cast<unsigned long long>(load(execute_inflight_[0])),
                 static_cast<unsigned long long>(load(execute_inflight_[1])),
                 static_cast<unsigned long long>(load(execute_inflight_[2])),
                 static_cast<unsigned long long>(load(execute_inflight_[3])),
                 static_cast<unsigned long long>(phase_code), phase_name, usage.ru_minflt, usage.ru_majflt,
                 usage.ru_maxrss);
}

std::array<size_t, 3> DeltaDatabase::IndexProbeCensusForTest(const DeltaSession& session) const {
    std::lock_guard<std::mutex> operation_lock(session.operation_mutex);
    return {session.census.last_overlay_nodes_probed, session.census.last_row_ids_probed,
            session.census.last_row_reads_probed};
}

std::array<uint64_t, 6> DeltaDatabase::ConcurrencyDiagnosticsForTest() const {
    return {execute_shared_calls_.load(std::memory_order_relaxed),
            execute_shared_wait_ns_.load(std::memory_order_relaxed),
            state_unique_commit_batches_.load(std::memory_order_relaxed),
            state_unique_commit_wait_ns_.load(std::memory_order_relaxed),
            peak_inflight_execute_.load(std::memory_order_relaxed),
            writer_turnstile_wait_ns_.load(std::memory_order_relaxed)};
}

std::array<size_t, 8> DeltaDatabase::JoinProbeCensusForTest(const DeltaSession& session) const {
    std::lock_guard<std::mutex> operation_lock(session.operation_mutex);
    return {session.census.last_parameterized_join_probes,     session.census.last_join_inner_rows_resolved,
            session.census.last_join_pairs_rechecked,          session.census.last_join_full_scan_rows,
            session.census.last_join_right_rows_visited,       session.census.last_join_base_entries_examined,
            session.census.last_join_overlay_entries_examined, session.census.last_join_overlay_refs_examined};
}

std::array<size_t, 5> DeltaDatabase::JoinOuterProbeCensusForTest(const DeltaSession& session) const {
    std::lock_guard<std::mutex> operation_lock(session.operation_mutex);
    return {session.census.last_join_outer_indexed_queries, session.census.last_join_outer_candidates,
            session.census.last_join_outer_full_scan_rows, session.census.last_join_inferred_literal_components,
            session.census.last_join_inferred_literal_probes};
}

std::array<uint64_t, 5> DeltaDatabase::JoinOuterDiagnosticsForTest() const {
    return {join_outer_indexed_.load(std::memory_order_relaxed), join_outer_candidates_.load(std::memory_order_relaxed),
            join_outer_full_scan_rows_.load(std::memory_order_relaxed),
            join_inferred_literal_components_.load(std::memory_order_relaxed),
            join_inferred_literal_probes_.load(std::memory_order_relaxed)};
}

std::array<size_t, 8> DeltaDatabase::OrderedProbeCensusForTest(const DeltaSession& session) const {
    std::lock_guard<std::mutex> operation_lock(session.operation_mutex);
    return {session.census.last_ordered_candidates_examined, session.census.last_ordered_rows_decoded,
            session.census.last_ordered_sort_input_rows,     session.census.last_ordered_early_stops,
            session.census.last_overlay_nodes_probed,        session.census.last_overlay_refs_examined,
            session.census.last_overlay_refs_copied,         session.census.last_overlay_order_ops};
}

size_t DeltaDatabase::LiveSummaryProbeCensusForTest(const DeltaSession& session) const {
    std::lock_guard<std::mutex> operation_lock(session.operation_mutex);
    return session.census.last_live_summary_words_probed;
}

std::array<size_t, 4> DeltaDatabase::SidecarIoCensusForTest(const DeltaSession& session) const {
    std::lock_guard<std::mutex> operation_lock(session.operation_mutex);
    auto state_lock = LockStateShared();
    size_t mapped_bytes = 0;
    for (const auto& [constraint_id, descriptor] : sidecars_)
        mapped_bytes += descriptor.mapped_bytes;
    return {session.census.last_sidecar_query_opens, session.census.last_sidecar_query_preads,
            session.census.last_sidecar_binary_comparisons, mapped_bytes};
}

std::array<size_t, 3> DeltaDatabase::CurrentIndexCensusForTest() const {
    auto state_lock = LockStateShared();
    const auto refs = [](const CommittedOverlay& overlay) {
        size_t count = 0;
        for (const auto& [key, ids] : overlay)
            count += ids.size();
        return count;
    };
    return {current_base_row_order_comparisons_.load(std::memory_order_relaxed), refs(overlay_), refs(current_overlay_)};
}

size_t DeltaDatabase::SidecarValidationCountForTest() const {
    auto state_lock = LockStateShared();
    return sidecar_validation_count_;
}

std::optional<uint64_t> DeltaDatabase::SidecarLiveTailForTest(epoch_si_poc::ConstraintId constraint_id) const {
    auto state_lock = LockStateShared();
    const auto found = sidecars_.find(constraint_id);
    if (found == sidecars_.end() || found->second.live_bitmap.empty())
        return std::nullopt;
    return found->second.live_bitmap.back();
}

void DeltaDatabase::RequireUsable() const {
    if (poisoned_.load(std::memory_order_acquire))
        throw std::logic_error("Delta database is poisoned; reopen required");
}

void DeltaDatabase::CloseWalForTest() {
    auto state_lock = LockStateUnique();
    db_.engine().CloseFileForTest();
}

bool DeltaDatabase::IsDeltaDirectory(const std::string& directory) {
    std::error_code error;
    if (!std::filesystem::is_directory(directory, error))
        return false;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        const std::string name = entry.path().filename().string();
        if (name == std::string(kCatalog) + ".tmp" || name == "MANIFEST" || name == "MANIFEST.tmp" ||
            name.rfind("base.", 0) == 0 || name.rfind("db.log.", 0) == 0 || name.rfind("wal.", 0) == 0)
            return true;
    }
    return false;
}

std::unique_ptr<DeltaDatabase> DeltaDatabase::Create(const std::string& directory) {
    auto result = std::unique_ptr<DeltaDatabase>(new DeltaDatabase(epoch_si_poc::CheckpointDb::Create(directory, {})));
    result->directory_ = directory;
    result->SaveCatalog({}, 1, 1, 1);
    return result;
}

std::unique_ptr<DeltaDatabase> DeltaDatabase::Open(const std::string& directory) {
    auto result = std::unique_ptr<DeltaDatabase>(new DeltaDatabase(epoch_si_poc::CheckpointDb::Open(directory)));
    result->directory_ = directory;
    result->LoadCatalog();
    for (const auto& [name, schema] : result->tables_) {
        bool rebuild = false;
        for (const auto& index : schema.indexes)
            rebuild = !result->ValidateSidecar(schema, index) || rebuild;
        if (rebuild)
            result->RebuildSidecars(schema);
    }
    result->db_.engine().VisitLatestVersions([&](epoch_si_poc::RowId id, const epoch_si_poc::RowImage& image) {
        if (const TableSchema* schema = result->TableById(id.table_id))
            result->AddOverlay(result->overlay_, *schema, result->DecodeRow(*schema, image), id);
    });
    result->db_.engine().VisitLatestVersionHeads(
        [&](epoch_si_poc::RowId id, epoch_si_poc::Epoch, const epoch_si_poc::RowImage& image) {
            const TableSchema* schema = result->TableById(id.table_id);
            if (!schema)
                return;
            result->ClearCurrentBaseRow(*schema, id.local_id);
            if (image.deleted)
                return;
            const auto cells = result->DecodeRow(*schema, image);
            for (const auto& index : schema->indexes) {
                const auto key = result->EncodeKey(*schema, index, cells);
                result->ApplyCurrentBaseState(*schema, index.constraint_id, key, id.local_id, true);
                auto sidecar = result->sidecars_.find(index.constraint_id);
                bool is_base = false;
                if (sidecar != result->sidecars_.end()) {
                    const auto* mapped = static_cast<const uint8_t*>(sidecar->second.mapping);
                    uint64_t ordinal = 0;
                    uint64_t first = 0, last = sidecar->second.count;
                    while (first < last) {
                        const uint64_t middle = first + (last - first) / 2;
                        const uint32_t candidate = GetLeAt<uint32_t>(
                            mapped + sidecar->second.row_order_offset +
                            middle * kSidecarRowOrderEntryBytes);
                        const uint64_t candidate_id = GetLeAt<uint64_t>(
                            mapped + kSidecarHeaderBytes + static_cast<uint64_t>(candidate) * kSidecarEntryBytes + 8);
                        if (candidate_id < id.local_id)
                            first = middle + 1;
                        else
                            last = middle;
                    }
                    if (first < sidecar->second.count) {
                        ordinal = GetLeAt<uint32_t>(mapped + sidecar->second.row_order_offset +
                                                   first * kSidecarRowOrderEntryBytes);
                        const uint64_t begin = GetLeAt<uint64_t>(
                            mapped + kSidecarHeaderBytes + ordinal * kSidecarEntryBytes);
                        const uint64_t end = ordinal + 1 == sidecar->second.count
                                                 ? sidecar->second.key_bytes
                                                 : GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes +
                                                                     (ordinal + 1) * kSidecarEntryBytes);
                        const uint8_t* keys = mapped + kSidecarHeaderBytes + sidecar->second.count * kSidecarEntryBytes;
                        is_base = end - begin == key.size() && std::memcmp(keys + begin, key.data(), key.size()) == 0;
                    }
                }
                if (!is_base)
                    AppendOverlay(result->current_overlay_, {schema->id, index.constraint_id, key}, id.local_id);
            }
        });
    return result;
}

void DeltaDatabase::SaveCatalog(const Catalog& tables, epoch_si_poc::TableId next_table_id,
                                epoch_si_poc::ConstraintId next_constraint_id, uint64_t catalog_generation) {
    RequireUsable();
    if (fail_catalog_save_for_test_)
        throw std::runtime_error("injected Delta catalog save failure");
    const std::string temp = directory_ + "/" + kCatalog + ".tmp";
    std::ostringstream body;
    body << kCatalogMagic << '\n'
         << catalog_generation << ' ' << next_table_id << ' ' << next_constraint_id << ' ' << tables.size() << '\n';
    for (const auto& [name, table] : tables) {
        body << "TABLE " << table.id << ' ' << table.version << ' ' << table.name << ' ' << table.columns.size() << ' '
             << table.indexes.size() << '\n';
        for (const Column& column : table.columns)
            body << "COLUMN " << static_cast<unsigned>(column.type) << ' ' << column.length << ' ' << column.nullable
                 << ' ' << column.name << '\n';
        for (const Index& index : table.indexes) {
            body << "INDEX " << index.constraint_id << ' ' << index.unique << ' ' << index.columns.size();
            for (uint32_t column : index.columns)
                body << ' ' << column;
            body << '\n';
        }
    }
    const std::string bytes = body.str();
    const std::string footer = "CRC32 " + std::to_string(Crc32(bytes)) + '\n';
    if (bytes.size() + footer.size() > kMaxCatalogBytes)
        throw std::runtime_error("Delta catalog exceeds limit");
    std::ofstream output(temp, std::ios::binary | std::ios::trunc);
    if (!output)
        throw std::runtime_error("open Delta catalog");
    output << bytes << footer;
    output.flush();
    if (!output)
        throw std::runtime_error("write Delta catalog");
    output.close();
    const int catalog_fd = ::open(temp.c_str(), O_RDONLY | O_CLOEXEC);
    if (catalog_fd < 0 || ::fdatasync(catalog_fd) != 0) {
        if (catalog_fd >= 0)
            ::close(catalog_fd);
        throw std::runtime_error("sync Delta catalog");
    }
    ::close(catalog_fd);
    bool renamed = false;
    try {
        std::filesystem::rename(temp, directory_ + "/" + kCatalog);
        renamed = true;
        if (fail_catalog_post_rename_for_test_)
            throw std::runtime_error("injected post-rename Delta catalog failure");
        const int directory_fd = ::open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (directory_fd < 0 || ::fsync(directory_fd) != 0) {
            if (directory_fd >= 0)
                ::close(directory_fd);
            throw std::runtime_error("sync Delta catalog directory");
        }
        ::close(directory_fd);
    } catch (...) {
        if (renamed)
            poisoned_.store(true, std::memory_order_release);
        throw;
    }
}

void DeltaDatabase::LoadCatalog() {
    std::error_code size_error;
    const uintmax_t catalog_size = std::filesystem::file_size(directory_ + "/" + kCatalog, size_error);
    if (size_error || catalog_size > kMaxCatalogBytes)
        throw std::runtime_error("invalid Delta catalog size");
    std::ifstream file(directory_ + "/" + kCatalog, std::ios::binary);
    if (!file)
        throw std::runtime_error("open Delta catalog");
    const std::string bytes{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
    const size_t footer = bytes.rfind("\nCRC32 ");
    if (footer == std::string::npos || bytes.empty() || bytes.back() != '\n')
        throw std::runtime_error("invalid Delta catalog checksum footer");
    const std::string body = bytes.substr(0, footer + 1);
    const std::string footer_line = bytes.substr(footer + 1, bytes.size() - footer - 2);
    std::string checksum_tag;
    uint32_t checksum = 0;
    if (!ParseLine(footer_line, checksum_tag, checksum) || checksum_tag != "CRC32" || checksum != Crc32(body))
        throw std::runtime_error("invalid Delta catalog checksum");
    std::istringstream input(body);
    std::string line;
    if (!std::getline(input, line) || line != kCatalogMagic || !std::getline(input, line))
        throw std::runtime_error("invalid Delta catalog");
    uint64_t generation = 0;
    epoch_si_poc::TableId next_table = 0;
    epoch_si_poc::ConstraintId next_constraint = 0;
    size_t table_count = 0;
    if (!ParseLine(line, generation, next_table, next_constraint, table_count) || generation == 0 || next_table == 0 ||
        next_constraint == 0 || table_count > kMaxTables)
        throw std::runtime_error("invalid Delta catalog header");
    Catalog candidate;
    std::set<epoch_si_poc::TableId> table_ids;
    std::set<epoch_si_poc::ConstraintId> constraint_ids;
    epoch_si_poc::TableId max_table = 0;
    epoch_si_poc::ConstraintId max_constraint = 0;
    for (size_t t = 0; t < table_count; ++t) {
        std::string tag;
        TableSchema table{};
        size_t columns = 0;
        size_t indexes = 0;
        if (!std::getline(input, line) ||
            !ParseLine(line, tag, table.id, table.version, table.name, columns, indexes) || tag != "TABLE" ||
            table.id == 0 || table.version == 0 || columns == 0 || columns > kMaxColumns ||
            !table_ids.insert(table.id).second)
            throw std::runtime_error("invalid Delta table catalog entry");
        std::set<std::string> column_names;
        size_t maximum_row = sizeof(uint32_t) + columns;
        for (size_t c = 0; c < columns; ++c) {
            unsigned type = 0;
            unsigned nullable = 0;
            Column column{};
            if (!std::getline(input, line) || !ParseLine(line, tag, type, column.length, nullable, column.name) ||
                tag != "COLUMN" || type > static_cast<unsigned>(ColumnType::Char) || nullable > 1 ||
                !column_names.insert(column.name).second)
                throw std::runtime_error("invalid Delta column catalog entry");
            column.type = static_cast<ColumnType>(type);
            column.nullable = nullable != 0;
            if ((column.type == ColumnType::Int || column.type == ColumnType::Float) && column.length != 4)
                throw std::runtime_error("invalid fixed Delta column length");
            if (column.type == ColumnType::Char && (column.length == 0 || column.length > kMaxRowBytes))
                throw std::runtime_error("invalid Delta CHAR length");
            maximum_row += column.type == ColumnType::Char ? sizeof(uint32_t) + column.length : 4;
            if (maximum_row > kMaxRowBytes)
                throw std::runtime_error("Delta row schema exceeds limit");
            table.columns.push_back(std::move(column));
        }
        for (size_t i = 0; i < indexes; ++i) {
            std::istringstream row;
            if (!std::getline(input, line))
                throw std::runtime_error("truncated Delta index catalog");
            row.str(line);
            unsigned unique = 0;
            size_t count = 0;
            Index index{};
            if (!(row >> tag >> index.constraint_id >> unique >> count) || tag != "INDEX" || index.constraint_id == 0 ||
                unique > 1 || count == 0 || count > table.columns.size() ||
                !constraint_ids.insert(index.constraint_id).second)
                throw std::runtime_error("invalid Delta index catalog entry");
            index.unique = unique != 0;
            std::set<uint32_t> distinct;
            for (size_t c = 0; c < count; ++c) {
                uint32_t column = 0;
                if (!(row >> column) || column >= table.columns.size() || !distinct.insert(column).second)
                    throw std::runtime_error("invalid Delta index column");
                index.columns.push_back(column);
            }
            std::string extra;
            if (row >> extra)
                throw std::runtime_error("invalid Delta index trailing data");
            max_constraint = std::max(max_constraint, index.constraint_id);
            table.indexes.push_back(std::move(index));
        }
        max_table = std::max(max_table, table.id);
        if (!candidate.emplace(table.name, std::move(table)).second)
            throw std::runtime_error("duplicate Delta table name");
    }
    if (std::getline(input, line) || !input.eof() || next_table <= max_table || next_constraint <= max_constraint)
        throw std::runtime_error("invalid Delta catalog tail");
    tables_.swap(candidate);
    table_by_id_.clear();
    for (const auto& [name, table] : tables_)
        table_by_id_.emplace(table.id, &table);
    next_table_id_ = next_table;
    next_constraint_id_ = next_constraint;
    catalog_generation_ = generation;
}

const DeltaDatabase::TableSchema& DeltaDatabase::Table(const std::string& name) const {
    const auto found = tables_.find(name);
    if (found == tables_.end())
        throw std::runtime_error("DeltaKernel table not found: " + name);
    return found->second;
}

const DeltaDatabase::TableSchema* DeltaDatabase::TableById(epoch_si_poc::TableId id) const {
    const auto found = table_by_id_.find(id);
    return found == table_by_id_.end() ? nullptr : found->second;
}

epoch_si_poc::EpochSiEngine::Txn& DeltaDatabase::Txn(DeltaSession& session) {
    if (!session.txn)
        session.txn.emplace(db_.engine().Begin());
    return *session.txn;
}

CommittedOverlay DeltaDatabase::PrepareCommittedOverlay(const DeltaOverlay& overlay) {
    CommittedOverlay prepared;
    // AppendOverlay maintains each DeltaOverlay bucket sorted and unique.
    for (const auto& [key, ids] : overlay)
        prepared.emplace(key, ids);
    return prepared;
}

void DeltaDatabase::SetCurrentBaseBit(SidecarDescriptor& descriptor, uint64_t local_id, bool live) noexcept {
    if (!descriptor.mapping || descriptor.count == 0)
        return;
    const auto* mapped = static_cast<const uint8_t*>(descriptor.mapping);
    uint64_t first = 0;
    uint64_t last = descriptor.count;
    while (first < last) {
        const uint64_t middle = first + (last - first) / 2;
        const uint32_t candidate =
            GetLeAt<uint32_t>(mapped + descriptor.row_order_offset + middle * kSidecarRowOrderEntryBytes);
        const uint64_t candidate_id = GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes +
                                                         static_cast<uint64_t>(candidate) * kSidecarEntryBytes + 8);
        if (candidate_id < local_id)
            first = middle + 1;
        else
            last = middle;
    }
    if (first == descriptor.count)
        return;
    const uint32_t ordinal = GetLeAt<uint32_t>(mapped + descriptor.row_order_offset + first * kSidecarRowOrderEntryBytes);
    const uint64_t found_id = GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes +
                                                static_cast<uint64_t>(ordinal) * kSidecarEntryBytes + 8);
    if (found_id != local_id)
        return;
    const size_t word = ordinal / 64;
    const uint64_t mask = uint64_t{1} << (ordinal % 64);
    const bool was_live = (descriptor.live_bitmap[word] & mask) != 0;
    if (was_live == live)
        return;
    if (live)
        descriptor.live_bitmap[word] |= mask;
    else
        descriptor.live_bitmap[word] &= ~mask;
    if (diagnostics_)
        diagnostics_->current_base_bit_flips.fetch_add(1, std::memory_order_relaxed);
    for (size_t level = 0, child = word; level < descriptor.live_summary.size(); ++level) {
        const size_t parent = child / 64;
        const uint64_t child_mask = uint64_t{1} << (child % 64);
        const auto& source = level == 0 ? descriptor.live_bitmap : descriptor.live_summary[level - 1];
        const bool child_nonzero = child < source.size() && source[child] != 0;
        auto& summary_word = descriptor.live_summary[level][parent];
        if (child_nonzero)
            summary_word |= child_mask;
        else
            summary_word &= ~child_mask;
        child = parent;
    }
}

void DeltaDatabase::ApplyCurrentBaseState(const TableSchema& schema, epoch_si_poc::ConstraintId constraint_id,
                                          const EncodedKey& key, uint64_t local_id, bool live) noexcept {
    const auto found = sidecars_.find(constraint_id);
    if (found == sidecars_.end())
        return;
    auto& descriptor = found->second;
    if (descriptor.table_id != schema.id || descriptor.constraint_id != constraint_id || !descriptor.mapping)
        return;
    if (descriptor.count == 0 || local_id > descriptor.max_local_id)
        return;
    const auto* mapped = static_cast<const uint8_t*>(descriptor.mapping);
    uint64_t first = 0, last = descriptor.count;
    while (first < last) {
        const uint64_t middle = first + (last - first) / 2;
        const uint32_t ordinal =
            GetLeAt<uint32_t>(mapped + descriptor.row_order_offset + middle * kSidecarRowOrderEntryBytes);
        const uint64_t id = GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes +
                                              static_cast<uint64_t>(ordinal) * kSidecarEntryBytes + 8);
        if (diagnostics_)
            ++current_base_row_order_comparisons_;
        if (id < local_id)
            first = middle + 1;
        else
            last = middle;
    }
    if (first == descriptor.count)
        return;
    const uint32_t ordinal = GetLeAt<uint32_t>(mapped + descriptor.row_order_offset + first * kSidecarRowOrderEntryBytes);
    const uint64_t id = GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes +
                                          static_cast<uint64_t>(ordinal) * kSidecarEntryBytes + 8);
    if (id != local_id)
        return;
    const uint64_t begin = GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes + ordinal * kSidecarEntryBytes);
    const uint64_t end = ordinal + 1 == descriptor.count
                             ? descriptor.key_bytes
                             : GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes + (ordinal + 1) * kSidecarEntryBytes);
    const uint8_t* keys = mapped + kSidecarHeaderBytes + descriptor.count * kSidecarEntryBytes;
    if (end - begin == key.size() && std::memcmp(keys + begin, key.data(), key.size()) == 0)
        SetCurrentBaseBit(descriptor, local_id, live);
}

void DeltaDatabase::ClearCurrentBaseRow(const TableSchema& schema, uint64_t local_id) noexcept {
    for (const auto& index : schema.indexes) {
        auto found = sidecars_.find(index.constraint_id);
        if (found != sidecars_.end())
            SetCurrentBaseBit(found->second, local_id, false);
    }
}

void DeltaDatabase::InstallCurrentOverlay(CommittedOverlay& additions, CommittedOverlay& removals) noexcept {
    try {
        if (diagnostics_) {
            for (const auto& [key, ids] : additions)
                diagnostics_->current_overlay_addition_ids.fetch_add(ids.size(), std::memory_order_relaxed);
            for (const auto& [key, ids] : removals)
                diagnostics_->current_overlay_removal_ids.fetch_add(ids.size(), std::memory_order_relaxed);
        }
        for (auto it = removals.begin(); it != removals.end(); ++it) {
            auto range = current_overlay_.equal_range(it->first);
            for (auto current = range.first; current != range.second;) {
                auto& ids = current->second;
                for (uint64_t id : it->second)
                    ids.erase(std::remove(ids.begin(), ids.end(), id), ids.end());
                if (ids.empty())
                    current = current_overlay_.erase(current);
                else
                    ++current;
            }
        }
        current_overlay_.merge(additions);
    } catch (...) {
        std::terminate();
    }
}

void DeltaDatabase::RunCommitInstallHookForTest() noexcept {
    try {
        if (commit_install_hook_for_test_)
            commit_install_hook_for_test_();
    } catch (...) {
        std::terminate();
    }
}

void DeltaDatabase::InstallCommittedOverlay(CommittedOverlay& overlay) noexcept {
    try {
        overlay_.merge(overlay);
    } catch (...) {
        std::terminate();
    }
}

void DeltaDatabase::CaptureQueryCensus(const DeltaSession& session, ExecutionCensus& census) const {
    if (!diagnostics_ || census.captured)
        return;
    census.captured = true;
    census.base_entries = session.census.last_join_base_entries_examined != 0
                              ? session.census.last_join_base_entries_examined
                              : session.census.last_base_entries_probed;
    census.overlay_refs = session.census.last_join_overlay_refs_examined != 0
                              ? session.census.last_join_overlay_refs_examined
                              : session.census.last_overlay_refs_examined;
    census.parameterized_join_probes = session.census.last_parameterized_join_probes;
    census.join_outer_candidates = session.census.last_join_outer_candidates;
    census.join_outer_full_scan_rows = session.census.last_join_outer_full_scan_rows;
    census.join_inferred_literal_components = session.census.last_join_inferred_literal_components;
    census.join_inferred_literal_probes = session.census.last_join_inferred_literal_probes;
    census.join_outer_indexed = session.census.last_join_outer_indexed_queries != 0;
    census.ordered_early_stops = session.census.last_ordered_early_stops;
}

void DeltaDatabase::Commit(DeltaSession& session, std::shared_lock<std::shared_mutex>& state_lock,
                           ExecutionCensus* census) {
    if (!session.txn)
        return;
    if (census != nullptr)
        CaptureQueryCensus(session, *census);
    auto ticket = std::make_shared<CommitTicket>();
    ticket->active_additions = PrepareCommittedOverlay(session.overlay);
    ticket->overlay.merge(session.overlay);
    ticket->active_removals.merge(session.removed_overlay);
    ticket->txn.emplace(std::move(*session.txn));
    state_lock.unlock();
    bool leader = false;
    try {
        std::unique_lock<std::mutex> queue_lock(commit_mutex_);
        const auto queue_wait_started = diagnostics_ ? std::chrono::steady_clock::now()
                                                     : std::chrono::steady_clock::time_point{};
        commit_slot_available_.wait(queue_lock, [&] { return commit_queue_.size() < kCommitQueueLimit; });
        if (diagnostics_)
            commit_queue_wait_ns_.fetch_add(NsSince(queue_wait_started), std::memory_order_relaxed);
        if (fail_commit_enqueue_for_test_)
            throw std::runtime_error("test commit enqueue failure");
        commit_queue_.push_back(ticket);
        if (!commit_leader_) {
            commit_leader_ = true;
            leader = true;
        }
    } catch (...) {
        ticket->txn.reset();
        session.txn.reset();
        session.overlay.clear();
        session.removed_overlay.clear();
        session.private_insert_ids.clear();
        session.explicit_txn = false;
        session.admission.reset();
        throw;
    }
    if (leader)
        DrainCommitQueue();
    {
        std::unique_lock<std::mutex> queue_lock(commit_mutex_);
        const auto ready_wait_started = diagnostics_ ? std::chrono::steady_clock::now()
                                                     : std::chrono::steady_clock::time_point{};
        ticket->ready.wait(queue_lock, [&] { return ticket->done; });
        if (diagnostics_) {
            const uint64_t elapsed = NsSince(ready_wait_started);
            commit_ready_wait_ns_.fetch_add(elapsed, std::memory_order_relaxed);
            RecordMax(commit_ready_wait_max_ns_, elapsed);
        }
    }
    session.txn.reset();
    session.overlay.clear();
    session.removed_overlay.clear();
    session.private_insert_ids.clear();
    session.explicit_txn = false;
    session.admission.reset();
    if (ticket->error)
        std::rethrow_exception(ticket->error);
    if (ticket->result.status != epoch_si_poc::CommitStatus::kCommitted) {
        throw DeltaTransactionAbort(ticket->result.status == epoch_si_poc::CommitStatus::kWriteConflict
                                        ? "SI write conflict"
                                        : "unique key conflict");
    }
}

void DeltaDatabase::DrainCommitQueue() {
    for (;;) {
        std::array<std::shared_ptr<CommitTicket>, kCommitBatchSize> batch;
        size_t batch_count = 0;
        std::exception_ptr error;
        bool engine_called = false;
        {
            try {
                if (commit_batch_hook_for_test_)
                    commit_batch_hook_for_test_();
                std::this_thread::sleep_for(std::chrono::microseconds(200));
                std::lock_guard<std::mutex> queue_lock(commit_mutex_);
                const size_t count = std::min(kCommitBatchSize, commit_queue_.size());
                for (size_t n = 0; n < count; ++n) {
                    batch[batch_count++] = std::move(commit_queue_.front());
                    commit_queue_.pop_front();
                }
                commit_slot_available_.notify_all();
            } catch (...) {
                error = std::current_exception();
                std::lock_guard<std::mutex> queue_lock(commit_mutex_);
                while (batch_count < batch.size() && !commit_queue_.empty()) {
                    batch[batch_count++] = std::move(commit_queue_.front());
                    commit_queue_.pop_front();
                }
                commit_slot_available_.notify_all();
            }
        }
        std::vector<epoch_si_poc::CommitResult> results;
        if (!error && batch_count != 0) {
            try {
                std::unique_lock<std::mutex> commit_epoch_lock(commit_epoch_gate_);
                std::vector<epoch_si_poc::EpochSiEngine::Txn*> txns;
                txns.reserve(batch_count);
                for (size_t n = 0; n < batch_count; ++n)
                    txns.push_back(&*batch[n]->txn);
                const auto initial_unique_wait_started =
                    diagnostics_ ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
                if (diagnostics_)
                    commit_phase_.store(1, std::memory_order_relaxed);
                auto prepare_lock = LockStateUnique();
                if (diagnostics_)
                    RecordMax(commit_initial_unique_wait_max_ns_, NsSince(initial_unique_wait_started));
                if (diagnostics_)
                    commit_phase_.store(2, std::memory_order_relaxed);
                const auto prepare_started =
                    diagnostics_ ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
                engine_called = true;
                auto prepared = db_.engine().PrepareCommitBatch(txns);
                prepare_lock.unlock();
                if (diagnostics_) {
                    const uint64_t elapsed = NsSince(prepare_started);
                    commit_prepare_unique_ns_.fetch_add(elapsed, std::memory_order_relaxed);
                    RecordMax(commit_prepare_unique_max_ns_, elapsed);
                }
                results = prepared.results(); // All ticket-result allocation stays before WAL durability.
                if (commit_sync_hook_for_test_)
                    commit_sync_hook_for_test_();
                if (prepared.needs_sync()) {
                    if (diagnostics_)
                        commit_phase_.store(3, std::memory_order_relaxed);
                    const auto sync_started =
                        diagnostics_ ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
                    try {
                        db_.engine().SyncPreparedCommit(prepared);
                    } catch (...) {
                        poisoned_.store(true, std::memory_order_release);
                        throw;
                    }
                    if (diagnostics_) {
                        const uint64_t elapsed = NsSince(sync_started);
                        commit_sync_unlocked_ns_.fetch_add(elapsed, std::memory_order_relaxed);
                        RecordMax(commit_sync_unlocked_max_ns_, elapsed);
                    }
                }
                if (commit_reacquire_hook_for_test_)
                    commit_reacquire_hook_for_test_();
                const auto reacquire_started =
                    diagnostics_ ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
                if (diagnostics_)
                    commit_phase_.store(4, std::memory_order_relaxed);
                auto state_lock = LockStateUnique();
                if (diagnostics_)
                    commit_phase_.store(5, std::memory_order_relaxed);
                if (diagnostics_) {
                    const uint64_t waited = NsSince(reacquire_started);
                    commit_leader_reacquire_wait_ns_.fetch_add(waited, std::memory_order_relaxed);
                    commit_reacquire_ns_.fetch_add(waited, std::memory_order_relaxed);
                    state_unique_commit_wait_ns_.fetch_add(waited, std::memory_order_relaxed);
                    state_unique_commit_batches_.fetch_add(1, std::memory_order_relaxed);
                    RecordMax(commit_publish_unique_wait_max_ns_, waited);
                }
                try {
                    const auto publish_started =
                        diagnostics_ ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
                    if (prepared.needs_publish())
                        db_.engine().PublishPreparedCommit(prepared);
                    if (results.size() != batch_count)
                        std::terminate();
                    RunCommitInstallHookForTest();
                    for (size_t n = 0; n < batch_count; ++n)
                        if (results[n].status == epoch_si_poc::CommitStatus::kCommitted) {
                            for (const auto& [key, ids] : batch[n]->active_removals) {
                                const auto* schema = TableById(std::get<0>(key));
                                if (!schema)
                                    continue;
                                for (uint64_t id : ids)
                                    ApplyCurrentBaseState(*schema, std::get<1>(key), std::get<2>(key), id, false);
                            }
                            for (const auto& [key, ids] : batch[n]->active_additions) {
                                const auto* schema = TableById(std::get<0>(key));
                                if (!schema)
                                    continue;
                                for (uint64_t id : ids)
                                    ApplyCurrentBaseState(*schema, std::get<1>(key), std::get<2>(key), id, true);
                            }
                            InstallCurrentOverlay(batch[n]->active_additions, batch[n]->active_removals);
                            InstallCommittedOverlay(batch[n]->overlay);
                        }
                    if (diagnostics_) {
                        const uint64_t elapsed = NsSince(publish_started);
                        commit_publish_unique_ns_.fetch_add(elapsed, std::memory_order_relaxed);
                        RecordMax(commit_publish_unique_max_ns_, elapsed);
                    }
                } catch (...) {
                    if (engine_called)
                        poisoned_.store(true, std::memory_order_release);
                    throw;
                }
            } catch (...) {
                error = std::current_exception();
            }
            if (diagnostics_)
                commit_phase_.store(0, std::memory_order_relaxed);
        }
        {
            std::lock_guard<std::mutex> queue_lock(commit_mutex_);
            for (size_t n = 0; n < batch_count; ++n) {
                batch[n]->result = error ? epoch_si_poc::CommitResult{} : results[n];
                batch[n]->error = error;
                batch[n]->done = true;
                batch[n]->ready.notify_one();
            }
            commit_slot_available_.notify_all();
            if (commit_queue_.empty()) {
                commit_leader_ = false;
                return;
            }
        }
    }
}

void DeltaDatabase::Abort(DeltaSession& session) noexcept {
    if (abort_lock_attempt_hook_for_test_)
        abort_lock_attempt_hook_for_test_();
    std::lock_guard<std::mutex> operation_lock(session.operation_mutex);
    std::shared_lock<std::shared_mutex> admission;
    if (!session.admission)
        admission = LockExecutionShared();
    auto state_lock = LockStateShared();
    AbortLocked(session);
}

void DeltaDatabase::AbortLocked(DeltaSession& session) noexcept {
    if (session.txn) {
        try {
            db_.engine().Abort(*session.txn);
        } catch (...) {
        }
    }
    session.txn.reset();
    session.explicit_txn = false;
    session.overlay.clear();
    session.removed_overlay.clear();
    session.private_insert_ids.clear();
    session.admission.reset();
}

void DeltaDatabase::Checkpoint() {
    auto admission = LockExecutionUnique();
    std::unique_lock<std::mutex> commit_epoch_lock(commit_epoch_gate_);
    auto state_lock = LockStateUnique();
    RequireUsable();
    CheckpointSidecars();
}

std::array<uint64_t, 4> DeltaDatabase::RotateWalForTest() {
    auto admission = LockExecutionUnique();
    std::unique_lock<std::mutex> commit_epoch_lock(commit_epoch_gate_);
    auto state_lock = LockStateUnique();
    RequireUsable();
    const auto boundary = db_.RotateWalAtGate(rotation_crash_point_for_test_);
    return {boundary.epoch, boundary.next_commit_seq, boundary.wal_lineage, boundary.new_active_segment_id};
}

uint64_t DeltaDatabase::CatalogGeneration() const {
    auto state_lock = LockStateShared();
    RequireUsable();
    return catalog_generation_;
}

size_t DeltaDatabase::WalFrameCountForTest() const {
    auto state_lock = LockStateShared();
    return db_.engine().wal_frame_count();
}

size_t DeltaDatabase::CommitQueueDepthForTest() const {
    std::lock_guard<std::mutex> lock(commit_mutex_);
    return commit_queue_.size();
}

DeltaDatabase::Cell DeltaDatabase::Literal(const Column& column, const ast::Value& value) const {
    if (value.type == ast::AstType::Parameter) {
        if (active_prepared_parameters == nullptr)
            throw std::runtime_error("Delta parameter outside prepared execution");
        const auto ordinal = static_cast<const ast::Parameter&>(value).ordinal;
        if (ordinal == 0 || ordinal > active_prepared_parameters->size())
            throw std::runtime_error("Delta prepared parameter out of range");
        const DeltaParameter& parameter = (*active_prepared_parameters)[ordinal - 1];
        if (!parameter.present) {
            if (!column.nullable)
                throw std::runtime_error("NULL in non-nullable Delta column");
            return {};
        }
        Cell cell;
        cell.is_null = false;
        if (column.type == ColumnType::Int && parameter.type == DeltaValueType::Int) {
            cell.integer = parameter.integer;
        } else if (column.type == ColumnType::Float && parameter.type == DeltaValueType::Float) {
            std::memcpy(&cell.floating, &parameter.float_bits, sizeof(cell.floating));
        } else if (column.type == ColumnType::Float && parameter.type == DeltaValueType::Int) {
            cell.floating = static_cast<float>(parameter.integer);
        } else if (column.type == ColumnType::Char && parameter.type == DeltaValueType::Char) {
            if (parameter.text.size() > column.length)
                throw std::runtime_error("Delta CHAR value too long");
            cell.text = parameter.text;
        } else {
            throw std::runtime_error("Delta value type mismatch");
        }
        return cell;
    }
    Cell cell;
    if (value.type == ast::AstType::NullLit) {
        if (!column.nullable)
            throw std::runtime_error("NULL in non-nullable Delta column");
        return cell;
    }
    cell.is_null = false;
    if (column.type == ColumnType::Int && value.type == ast::AstType::IntLit)
        cell.integer = static_cast<const ast::IntLit&>(value).val;
    else if (column.type == ColumnType::Float && value.type == ast::AstType::FloatLit)
        cell.floating = static_cast<const ast::FloatLit&>(value).val;
    else if (column.type == ColumnType::Float && value.type == ast::AstType::IntLit)
        cell.floating = static_cast<float>(static_cast<const ast::IntLit&>(value).val);
    else if (column.type == ColumnType::Char && value.type == ast::AstType::StringLit) {
        cell.text = static_cast<const ast::StringLit&>(value).val;
        if (cell.text.size() > column.length)
            throw std::runtime_error("Delta CHAR value too long");
    } else {
        throw std::runtime_error("Delta value type mismatch");
    }
    return cell;
}

std::vector<uint8_t> DeltaDatabase::EncodeKey(const TableSchema& schema, const Index& index,
                                              const std::vector<Cell>& cells, size_t columns) const {
    std::vector<uint8_t> key;
    for (size_t i = 0; i < std::min(columns, index.columns.size()); ++i) {
        const uint32_t position = index.columns[i];
        const Column& column = schema.columns[position];
        const Cell& cell = cells[position];
        key.push_back(cell.is_null ? 0 : 1);
        if (cell.is_null)
            continue;
        if (column.type == ColumnType::Int) {
            PutBe<uint32_t>(key, static_cast<uint32_t>(cell.integer) ^ 0x80000000U);
        } else if (column.type == ColumnType::Float) {
            uint32_t bits;
            std::memcpy(&bits, &cell.floating, sizeof(bits));
            if ((bits & 0x7fffffffU) == 0)
                bits = 0;
            PutBe<uint32_t>(key, bits & 0x80000000U ? ~bits : bits ^ 0x80000000U);
        } else {
            for (unsigned char byte : cell.text) {
                key.push_back(byte);
                if (byte == 0)
                    key.push_back(0xff);
            }
            key.push_back(0);
            key.push_back(0);
        }
    }
    return key;
}

std::string DeltaDatabase::DistinctKey(const Column& column, const Cell& cell) const {
    std::string key(1, static_cast<char>(column.type));
    const auto append = [&](uint32_t value) {
        for (size_t i = 0; i < sizeof(value); ++i)
            key.push_back(static_cast<char>(value >> (i * 8)));
    };
    if (column.type == ColumnType::Int)
        append(static_cast<uint32_t>(cell.integer));
    else if (column.type == ColumnType::Float) {
        uint32_t bits;
        std::memcpy(&bits, &cell.floating, sizeof(bits));
        if ((bits & 0x7fffffffU) == 0)
            bits = 0;
        append(bits);
    } else
        key += cell.text;
    return key;
}

void DeltaDatabase::BuildSidecars(const TableSchema& schema, std::vector<std::vector<SidecarBuildEntry>> entries,
                                  uint64_t generation) {
    for (size_t n = 0; n < schema.indexes.size(); ++n) {
        const Index& index = schema.indexes[n];
        sidecars_.erase(index.constraint_id);
        std::vector<SidecarBuildEntry>& sorted = entries[n];
        std::sort(sorted.begin(), sorted.end(), [](const auto& left, const auto& right) {
            return left.key != right.key ? left.key < right.key : left.local_id < right.local_id;
        });
        uint64_t key_bytes = 0;
        for (const auto& entry : sorted) {
            if (entry.key.size() > kMaxSidecarBytes - key_bytes) {
                key_bytes = kMaxSidecarBytes;
                break;
            }
            key_bytes += entry.key.size();
        }
        if (sorted.size() > std::numeric_limits<uint32_t>::max() ||
            sorted.size() >
                (kMaxSidecarBytes - kSidecarHeaderBytes - 7) / (kSidecarEntryBytes + kSidecarRowOrderEntryBytes) ||
            key_bytes > kMaxSidecarBytes - kSidecarHeaderBytes - 7 -
                            sorted.size() * (kSidecarEntryBytes + kSidecarRowOrderEntryBytes))
            continue;
        std::vector<uint8_t> disk;
        disk.reserve(sorted.size() * kSidecarEntryBytes);
        std::vector<std::pair<uint64_t, uint32_t>> row_order;
        row_order.reserve(sorted.size());
        uint64_t key_offset = 0;
        for (size_t ordinal = 0; ordinal < sorted.size(); ++ordinal) {
            const auto& entry = sorted[ordinal];
            PutLe<uint64_t>(disk, key_offset);
            PutLe<uint64_t>(disk, entry.local_id);
            row_order.emplace_back(entry.local_id, static_cast<uint32_t>(ordinal));
            key_offset += entry.key.size();
        }
        std::sort(row_order.begin(), row_order.end(),
                  [](const auto& left, const auto& right) { return left.first < right.first; });
        if (std::adjacent_find(row_order.begin(), row_order.end(), [](const auto& left, const auto& right) {
                return left.first == right.first;
            }) != row_order.end()) {
            continue;
        }
        std::vector<uint8_t> row_order_bytes;
        row_order_bytes.reserve(row_order.size() * kSidecarRowOrderEntryBytes);
        for (const auto& entry : row_order)
            PutLe<uint32_t>(row_order_bytes, entry.second);
        const uint64_t keys_offset = kSidecarHeaderBytes + disk.size();
        const uint64_t row_order_offset = AlignUp(keys_offset + key_bytes, alignof(uint32_t));
        SidecarHeader header{kSidecarMagic,
                             kSidecarFormatVersion,
                             kSidecarHeaderBytes,
                             kSidecarEntryBytes,
                             schema.id,
                             index.constraint_id,
                             generation,
                             db_.engine().published_epoch(),
                             sorted.size(),
                             row_order_offset + row_order_bytes.size(),
                             key_bytes,
                             row_order_offset,
                             0,
                             0,
                             0,
                             0};
        header.entries_crc = Crc32(disk.data(), disk.size());
        uint32_t keys_crc = 0xffffffffU;
        for (const auto& entry : sorted)
            keys_crc = UpdateCrc32(keys_crc, entry.key.data(), entry.key.size());
        header.keys_crc = ~keys_crc;
        header.row_order_crc = Crc32(row_order_bytes.data(), row_order_bytes.size());
        auto encoded_header = EncodeSidecarHeader(header);
        header.header_crc = Crc32(encoded_header.data(), kSidecarHeaderCrcOffset);
        encoded_header = EncodeSidecarHeader(header);
        const std::string path =
            directory_ + "/deltaidx." + std::to_string(schema.id) + "." + std::to_string(index.constraint_id);
        const std::string temp = path + ".tmp";
        const int fd = open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0644);
        if (fd < 0)
            continue;
        const auto write_all = [&](const void* data, size_t bytes) {
            const auto* p = static_cast<const uint8_t*>(data);
            while (bytes) {
                const ssize_t wrote = write(fd, p, bytes);
                if (wrote <= 0)
                    return false;
                p += wrote;
                bytes -= static_cast<size_t>(wrote);
            }
            return true;
        };
        bool ok = write_all(encoded_header.data(), encoded_header.size()) && write_all(disk.data(), disk.size());
        for (const auto& entry : sorted)
            ok = ok && write_all(entry.key.data(), entry.key.size());
        std::array<uint8_t, 3> padding{};
        ok = ok && write_all(padding.data(), static_cast<size_t>(row_order_offset - keys_offset - key_bytes));
        ok = ok && write_all(row_order_bytes.data(), row_order_bytes.size());
        if (ok)
            ok = fsync(fd) == 0;
        if (close(fd) != 0)
            ok = false;
        if (ok)
            ok = rename(temp.c_str(), path.c_str()) == 0;
        if (!ok)
            unlink(temp.c_str());
        else if (!ValidateSidecar(schema, index))
            sidecars_.erase(index.constraint_id);
    }
}

bool DeltaDatabase::ValidateSidecar(const TableSchema& schema, const Index& index) {
    const std::string path =
        directory_ + "/deltaidx." + std::to_string(schema.id) + "." + std::to_string(index.constraint_id);
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        sidecars_.erase(index.constraint_id);
        return false;
    }
    ++sidecar_validation_count_;
    const auto read_exact = [&](void* output, size_t bytes, uint64_t offset) {
        auto* out = static_cast<uint8_t*>(output);
        size_t done = 0;
        while (done < bytes) {
            const ssize_t read_bytes = pread(fd, out + done, bytes - done, static_cast<off_t>(offset + done));
            if (read_bytes < 0 && errno == EINTR)
                continue;
            if (read_bytes <= 0)
                return false;
            done += static_cast<size_t>(read_bytes);
        }
        return true;
    };
    std::array<uint8_t, kSidecarHeaderBytes> header_bytes{};
    struct stat st {};
    const auto generation = db_.TableGeneration(schema.id);
    const auto visible_from = db_.TableVisibleFrom(schema.id);
    const bool read_header = fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size >= 0 &&
                             static_cast<uint64_t>(st.st_size) >= header_bytes.size() &&
                             read_exact(header_bytes.data(), header_bytes.size(), 0);
    const SidecarHeader header = read_header ? DecodeSidecarHeader(header_bytes.data()) : SidecarHeader{};
    const bool header_valid =
        generation && fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size >= 0 && visible_from && read_header &&
        header.magic == kSidecarMagic && header.format_version == kSidecarFormatVersion &&
        header.header_bytes == kSidecarHeaderBytes && header.entry_bytes == kSidecarEntryBytes &&
        GetLeAt<uint32_t>(header_bytes.data() + 28) == 0 && header.table_id == schema.id &&
        header.constraint_id == index.constraint_id && header.generation == *generation &&
        *visible_from <= header.snapshot_epoch && header.snapshot_epoch <= db_.engine().published_epoch() &&
        header.total_bytes == static_cast<uint64_t>(st.st_size) && header.total_bytes <= kMaxSidecarBytes &&
        header.count <= std::numeric_limits<uint32_t>::max() &&
        header.total_bytes <= std::numeric_limits<size_t>::max() &&
        header.header_crc == Crc32(header_bytes.data(), kSidecarHeaderCrcOffset);
    uint64_t entry_bytes = 0;
    uint64_t keys_offset = 0;
    uint64_t keys_end = 0;
    uint64_t row_order_bytes = 0;
    uint64_t expected_total_bytes = 0;
    const bool layout_valid =
        header.count <= std::numeric_limits<uint64_t>::max() / kSidecarEntryBytes &&
        CheckedMultiply(header.count, kSidecarEntryBytes, &entry_bytes) &&
        CheckedAdd(kSidecarHeaderBytes, entry_bytes, &keys_offset) &&
        CheckedAdd(keys_offset, header.key_bytes, &keys_end) &&
        header.count <= std::numeric_limits<uint64_t>::max() / kSidecarRowOrderEntryBytes &&
        CheckedMultiply(header.count, kSidecarRowOrderEntryBytes, &row_order_bytes) &&
        keys_end <= kMaxSidecarBytes && row_order_bytes <= kMaxSidecarBytes &&
        header.row_order_offset >= keys_end && header.row_order_offset - keys_end < alignof(uint32_t) &&
        header.row_order_offset % alignof(uint32_t) == 0 &&
        header.row_order_offset <= kMaxSidecarBytes - row_order_bytes &&
        CheckedAdd(header.row_order_offset, row_order_bytes, &expected_total_bytes) &&
        expected_total_bytes == header.total_bytes;
    bool valid = header_valid && layout_valid;
    if (valid) {
        const size_t padding_bytes = static_cast<size_t>(header.row_order_offset - keys_end);
        std::array<uint8_t, alignof(uint32_t) - 1> padding{};
        valid = read_exact(padding.data(), padding_bytes, keys_end) &&
                std::all_of(padding.begin(), padding.begin() + padding_bytes,
                            [](uint8_t byte) { return byte == 0; });
    }
    std::array<uint8_t, 4096 * kSidecarEntryBytes> chunk{};
    uint64_t done = 0;
    uint32_t crc = 0xffffffffU;
    uint64_t previous_offset = 0;
    bool have_previous = false;
    while (valid && done < header.count) {
        const size_t count = static_cast<size_t>(std::min<uint64_t>(4096, header.count - done));
        valid = read_exact(chunk.data(), count * kSidecarEntryBytes, kSidecarHeaderBytes + done * kSidecarEntryBytes);
        crc = UpdateCrc32(crc, chunk.data(), count * kSidecarEntryBytes);
        for (size_t i = 0; valid && i < count; ++i) {
            const uint64_t offset = GetLeAt<uint64_t>(chunk.data() + i * kSidecarEntryBytes);
            valid = offset <= header.key_bytes && (!have_previous ? offset == 0 : offset > previous_offset) &&
                    (!have_previous || offset - previous_offset <= kMaxRowBytes);
            previous_offset = offset;
            have_previous = true;
        }
        done += count;
    }
    valid = valid &&
            (have_previous ? header.key_bytes >= previous_offset && header.key_bytes - previous_offset <= kMaxRowBytes
                           : header.key_bytes == 0);
    std::array<uint8_t, 1U << 16> key_chunk{};
    uint64_t key_done = 0;
    uint32_t keys_crc = 0xffffffffU;
    while (valid && key_done < header.key_bytes) {
        const size_t bytes = static_cast<size_t>(std::min<uint64_t>(key_chunk.size(), header.key_bytes - key_done));
        valid = read_exact(key_chunk.data(), bytes, keys_offset + key_done);
        keys_crc = UpdateCrc32(keys_crc, key_chunk.data(), bytes);
        key_done += bytes;
    }
    valid = valid && ~crc == header.entries_crc && ~keys_crc == header.keys_crc;
    std::array<uint8_t, 4096 * kSidecarRowOrderEntryBytes> row_order_chunk{};
    uint64_t row_order_done = 0;
    uint32_t row_order_crc = 0xffffffffU;
    while (valid && row_order_done < header.count) {
        const size_t count = static_cast<size_t>(std::min<uint64_t>(4096, header.count - row_order_done));
        valid = read_exact(row_order_chunk.data(), count * kSidecarRowOrderEntryBytes,
                           header.row_order_offset + row_order_done * kSidecarRowOrderEntryBytes);
        row_order_crc = UpdateCrc32(row_order_crc, row_order_chunk.data(), count * kSidecarRowOrderEntryBytes);
        for (size_t i = 0; valid && i < count; ++i) {
            const uint8_t* entry = row_order_chunk.data() + i * kSidecarRowOrderEntryBytes;
            valid = GetLeAt<uint32_t>(entry) < header.count;
        }
        row_order_done += count;
    }
    valid = valid && ~row_order_crc == header.row_order_crc;
    void* mapping = MAP_FAILED;
    if (valid)
        mapping = mmap(nullptr, static_cast<size_t>(header.total_bytes), PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (valid && mapping != MAP_FAILED) {
        const auto* mapped = static_cast<const uint8_t*>(mapping);
        uint64_t previous_local_id = 0;
        bool have_previous_local_id = false;
        for (uint64_t n = 0; valid && n < header.count; ++n) {
            const uint8_t* row_order = mapped + header.row_order_offset + n * kSidecarRowOrderEntryBytes;
            const uint64_t ordinal = GetLeAt<uint32_t>(row_order);
            const uint64_t local_id =
                GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes + ordinal * kSidecarEntryBytes + 8);
            valid = !have_previous_local_id || local_id > previous_local_id;
            previous_local_id = local_id;
            have_previous_local_id = true;
        }
    }
    if (!valid || mapping == MAP_FAILED) {
        if (mapping != MAP_FAILED)
            munmap(mapping, static_cast<size_t>(header.total_bytes));
        sidecars_.erase(index.constraint_id);
        return false;
    }
    sidecars_.insert_or_assign(index.constraint_id,
                               SidecarDescriptor{schema.id, index.constraint_id, *generation, header.snapshot_epoch,
                                                 header.count, header.key_bytes, header.row_order_offset,
                                                 static_cast<size_t>(header.total_bytes), mapping});
    return true;
}

void DeltaDatabase::RebuildSidecars(const TableSchema& schema) {
    for (const auto& index : schema.indexes)
        sidecars_.erase(index.constraint_id);
    std::vector<std::vector<SidecarBuildEntry>> entries(schema.indexes.size());
    auto txn = db_.engine().Begin();
    try {
        db_.engine().VisitScan(txn, schema.id, [&](epoch_si_poc::RowId id, const epoch_si_poc::RowImage& image) {
            const auto cells = DecodeRow(schema, image);
            for (size_t n = 0; n < schema.indexes.size(); ++n) {
                entries[n].push_back({EncodeKey(schema, schema.indexes[n], cells), id.local_id});
            }
        });
        db_.engine().Abort(txn);
        if (const auto generation = db_.TableGeneration(schema.id))
            BuildSidecars(schema, std::move(entries), *generation);
    } catch (...) {
        for (const auto& index : schema.indexes)
            sidecars_.erase(index.constraint_id);
    }
}

void DeltaDatabase::CheckpointSidecars() {
    const auto dirty = db_.engine().DirtyTableIds();
    db_.OfflineCheckpoint();
    for (epoch_si_poc::TableId id : dirty) {
        const TableSchema* schema = TableById(id);
        if (!schema)
            continue;
        RebuildSidecars(*schema);
        for (auto it = overlay_.begin(); it != overlay_.end();) {
            if (std::get<0>(it->first) == id)
                it = overlay_.erase(it);
            else
                ++it;
        }
        for (auto it = current_overlay_.begin(); it != current_overlay_.end();) {
            const auto sidecar = sidecars_.find(std::get<1>(it->first));
            if (std::get<0>(it->first) == id && sidecar != sidecars_.end() && sidecar->second.table_id == id)
                it = current_overlay_.erase(it);
            else
                ++it;
        }
    }
}

std::vector<epoch_si_poc::RowId>
DeltaDatabase::IndexedCandidates(DeltaSession& session, const TableSchema& schema,
                                 const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions, bool* usable,
                                 const IndexEqualities* equalities, size_t* inferred_used) const {
    *usable = false;
    if (inferred_used)
        *inferred_used = 0;
    const Index* selected = nullptr;
    std::vector<Cell> cells;
    std::vector<Cell> selected_cells;
    size_t prefix_columns = 0;
    const ast::BinaryExpr* lower = nullptr;
    const ast::BinaryExpr* upper = nullptr;
    size_t selected_inferred = 0;
    for (const Index& index : schema.indexes) {
        cells.assign(schema.columns.size(), Cell{});
        size_t equal = 0;
        size_t index_inferred = 0;
        for (; equal < index.columns.size(); ++equal) {
            const uint32_t column = index.columns[equal];
            if (equalities && column < equalities->values.size() && equalities->values[column]) {
                cells[column] = *equalities->values[column];
                index_inferred += column < equalities->inferred.size() && equalities->inferred[column];
                continue;
            }
            const auto found = std::find_if(conditions.begin(), conditions.end(), [&](const auto& condition) {
                if (!condition || condition->op != ast::SV_OP_EQ || !condition->lhs || !condition->rhs ||
                    condition->lhs->type != ast::AstType::Col || condition->rhs->type == ast::AstType::Col)
                    return false;
                const auto& lhs = static_cast<const ast::Col&>(*condition->lhs);
                return (lhs.tab_name.empty() || lhs.tab_name == schema.name) &&
                       lhs.col_name == schema.columns[column].name;
            });
            if (found == conditions.end())
                break;
            Cell value = Literal(schema.columns[column], static_cast<const ast::Value&>(*(*found)->rhs));
            if (value.is_null)
                break;
            cells[column] = std::move(value);
        }
        const bool range =
            equal < index.columns.size() &&
            std::any_of(conditions.begin(), conditions.end(), [&](const auto& condition) {
                if (!condition || !condition->lhs || !condition->rhs || condition->lhs->type != ast::AstType::Col ||
                    condition->rhs->type == ast::AstType::Col)
                    return false;
                const auto& lhs = static_cast<const ast::Col&>(*condition->lhs);
                return (lhs.tab_name.empty() || lhs.tab_name == schema.name) &&
                       lhs.col_name == schema.columns[index.columns[equal]].name &&
                       (condition->op == ast::SV_OP_GT || condition->op == ast::SV_OP_GE ||
                        condition->op == ast::SV_OP_LT || condition->op == ast::SV_OP_LE);
            });
        if ((equal || range) && (!selected || equal > prefix_columns)) {
            selected = &index;
            prefix_columns = equal;
            selected_cells = cells;
            selected_inferred = index_inferred;
        }
    }
    if (!selected)
        return {};
    if (inferred_used)
        *inferred_used = selected_inferred;
    cells = std::move(selected_cells);
    for (const auto& condition : conditions) {
        if (!condition || !condition->lhs || !condition->rhs || condition->lhs->type != ast::AstType::Col ||
            condition->rhs->type == ast::AstType::Col)
            continue;
        const auto& lhs = static_cast<const ast::Col&>(*condition->lhs);
        if ((!lhs.tab_name.empty() && lhs.tab_name != schema.name) || prefix_columns == selected->columns.size() ||
            schema.columns[selected->columns[prefix_columns]].name != lhs.col_name)
            continue;
        if (condition->op == ast::SV_OP_GT || condition->op == ast::SV_OP_GE)
            lower = condition.get();
        else if (condition->op == ast::SV_OP_LT || condition->op == ast::SV_OP_LE)
            upper = condition.get();
    }
    std::vector<epoch_si_poc::RowId> result;
    EncodedKey first_key = EncodeKey(schema, *selected, cells, prefix_columns);
    if (lower) {
        cells[selected->columns[prefix_columns]] =
            Literal(schema.columns[selected->columns[prefix_columns]], static_cast<const ast::Value&>(*lower->rhs));
        first_key = EncodeKey(schema, *selected, cells, prefix_columns + 1);
        if (lower->op == ast::SV_OP_GT)
            first_key = PrefixSuccessor(std::move(first_key));
    }
    EncodedKey last_key = PrefixSuccessor(EncodeKey(schema, *selected, cells, prefix_columns));
    if (upper) {
        cells[selected->columns[prefix_columns]] =
            Literal(schema.columns[selected->columns[prefix_columns]], static_cast<const ast::Value&>(*upper->rhs));
        last_key = EncodeKey(schema, *selected, cells, prefix_columns + 1);
        if (upper->op == ast::SV_OP_LE)
            last_key = PrefixSuccessor(std::move(last_key));
    }
    VisitIndexInterval(
        session, schema, *selected, first_key, last_key,
        [&](const EncodedKey&, epoch_si_poc::RowId id) { result.push_back(id); }, usable);
    if (!*usable)
        return {};
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

std::optional<DeltaDatabase::OrderedIndexAccess>
DeltaDatabase::FindOrderedIndexAccess(const TableSchema& schema, const std::string& alias,
                                      const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions,
                                      const std::vector<size_t>& ordered_columns) const {
    if (ordered_columns.empty())
        return std::nullopt;
    const auto matches_column = [&](const ast::BinaryExpr& condition, size_t position) {
        const auto* column = condition.lhs ? dynamic_cast<const ast::Col*>(condition.lhs.get()) : nullptr;
        return column && (column->tab_name.empty() || column->tab_name == schema.name || column->tab_name == alias) &&
               column->col_name == schema.columns[position].name;
    };
    for (const Index& index : schema.indexes) {
        std::vector<Cell> cells(schema.columns.size());
        size_t prefix = 0;
        for (; prefix < index.columns.size(); ++prefix) {
            const auto found = std::find_if(conditions.begin(), conditions.end(), [&](const auto& condition) {
                return condition && condition->op == ast::SV_OP_EQ && condition->rhs &&
                       dynamic_cast<const ast::Value*>(condition->rhs.get()) &&
                       matches_column(*condition, index.columns[prefix]);
            });
            if (found == conditions.end())
                break;
            Cell value = Literal(schema.columns[index.columns[prefix]], static_cast<const ast::Value&>(*(*found)->rhs));
            if (value.is_null)
                break;
            cells[index.columns[prefix]] = std::move(value);
        }
        if (prefix + ordered_columns.size() > index.columns.size() ||
            !std::equal(ordered_columns.begin(), ordered_columns.end(), index.columns.begin() + prefix) ||
            !sidecars_.count(index.constraint_id))
            continue;
        EncodedKey first = EncodeKey(schema, index, cells, prefix);
        EncodedKey last = PrefixSuccessor(first);
        const ast::BinaryExpr* lower = nullptr;
        const ast::BinaryExpr* upper = nullptr;
        for (const auto& condition : conditions) {
            if (!condition || !condition->rhs || !dynamic_cast<const ast::Value*>(condition->rhs.get()) ||
                !matches_column(*condition, ordered_columns.front()))
                continue;
            if (condition->op == ast::SV_OP_GT || condition->op == ast::SV_OP_GE)
                lower = condition.get();
            else if (condition->op == ast::SV_OP_LT || condition->op == ast::SV_OP_LE)
                upper = condition.get();
        }
        if (lower) {
            cells[ordered_columns.front()] =
                Literal(schema.columns[ordered_columns.front()], static_cast<const ast::Value&>(*lower->rhs));
            if (!cells[ordered_columns.front()].is_null) {
                first = EncodeKey(schema, index, cells, prefix + 1);
                if (lower->op == ast::SV_OP_GT)
                    first = PrefixSuccessor(std::move(first));
            }
        }
        if (upper) {
            cells[ordered_columns.front()] =
                Literal(schema.columns[ordered_columns.front()], static_cast<const ast::Value&>(*upper->rhs));
            if (!cells[ordered_columns.front()].is_null) {
                last = EncodeKey(schema, index, cells, prefix + 1);
                if (upper->op == ast::SV_OP_LE)
                    last = PrefixSuccessor(std::move(last));
            }
        }
        if (!last.empty() && first > last)
            first = last;
        return OrderedIndexAccess{&index, std::move(first), std::move(last)};
    }
    return std::nullopt;
}

void DeltaDatabase::VisitIndexInterval(DeltaSession& session, const TableSchema& schema, const Index& index,
                                       const EncodedKey& first_key, const EncodedKey& last_key,
                                       const std::function<void(const EncodedKey&, epoch_si_poc::RowId)>& visitor,
                                       bool* usable) const {
    std::vector<std::pair<EncodedKey, epoch_si_poc::RowId>> keyed;
    VisitOrderedIndexInterval(
        session, schema, index, first_key, last_key, false,
        [&](const EncodedKey& key, epoch_si_poc::RowId id) {
            keyed.push_back({key, id});
            return true;
        },
        usable);
    if (!*usable)
        return;
    for (const auto& [key, id] : keyed)
        visitor(key, id);
}

void DeltaDatabase::VisitOrderedIndexInterval(
    DeltaSession& session, const TableSchema& schema, const Index& index, const EncodedKey& first_key,
    const EncodedKey& last_key, bool reverse,
    const std::function<bool(const EncodedKey&, epoch_si_poc::RowId)>& visitor, bool* usable) const {
    *usable = false;
    session.census.last_base_entries_probed = 0;
    session.census.last_live_summary_words_probed = 0;
    session.census.last_overlay_nodes_probed = 0;
    session.census.last_row_ids_probed = 0;
    session.census.last_overlay_refs_examined = 0;
    session.census.last_overlay_refs_copied = 0;
    session.census.last_overlay_order_ops = 0;
    const auto descriptor = sidecars_.find(index.constraint_id);
    if (descriptor == sidecars_.end())
        return;
    const SidecarDescriptor& sidecar = descriptor->second;
    if (sidecar.table_id != schema.id || sidecar.constraint_id != index.constraint_id || !sidecar.mapping)
        return;
    const auto* mapped = static_cast<const uint8_t*>(sidecar.mapping);
    const uint8_t* keys = mapped + kSidecarHeaderBytes + sidecar.count * kSidecarEntryBytes;
    struct KeySpan {
        const uint8_t* data;
        size_t size;
    };
    const auto key_at = [&](uint64_t position) {
        const uint64_t begin = GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes + position * kSidecarEntryBytes);
        const uint64_t end =
            position + 1 == sidecar.count
                ? sidecar.key_bytes
                : GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes + (position + 1) * kSidecarEntryBytes);
        return KeySpan{keys + begin, static_cast<size_t>(end - begin)};
    };
    const auto compare = [&](KeySpan current, const EncodedKey& key) {
        ++session.census.last_sidecar_binary_comparisons;
        const size_t shared = std::min(current.size, key.size());
        const int compared = shared == 0 ? 0 : std::memcmp(current.data, key.data(), shared);
        if (compared != 0)
            return compared;
        if (current.size < key.size())
            return -1;
        return current.size == key.size() ? 0 : 1;
    };
    const auto lower_bound = [&](const EncodedKey& key) {
        uint64_t first = 0, last = sidecar.count;
        while (first < last) {
            const uint64_t middle = first + (last - first) / 2;
            if (compare(key_at(middle), key) < 0)
                first = middle + 1;
            else
                last = middle;
        }
        return first;
    };
    uint64_t base_first = lower_bound(first_key);
    const uint64_t base_last = last_key.empty() ? sidecar.count : lower_bound(last_key);
    if (base_first > base_last)
        base_first = base_last;
    uint64_t base_position = reverse ? base_last : base_first;
    bool base_valid = false;
    std::pair<EncodedKey, epoch_si_poc::RowId> base_current;
    const bool latest_snapshot = session.txn && db_.engine().IsLatestSnapshot(*session.txn);
    if (diagnostics_) {
        (latest_snapshot ? diagnostics_->current_index_latest_routes : diagnostics_->current_index_historical_routes)
            .fetch_add(1, std::memory_order_relaxed);
    }
    const auto find_next_set = [&](auto&& self, size_t level, size_t start) -> std::optional<size_t> {
        const auto& words = sidecar.live_summary[level];
        if (start >= words.size() * 64)
            return std::nullopt;
        size_t word = start / 64;
        ++session.census.last_live_summary_words_probed;
        if (diagnostics_)
            diagnostics_->current_live_summary_words.fetch_add(1, std::memory_order_relaxed);
        uint64_t bits = words[word] & (~uint64_t{0} << (start % 64));
        while (bits == 0) {
            if (level + 1 >= sidecar.live_summary.size())
                return std::nullopt;
            const auto parent = self(self, level + 1, word + 1);
            if (!parent)
                return std::nullopt;
            word = *parent;
            ++session.census.last_live_summary_words_probed;
            if (diagnostics_)
                diagnostics_->current_live_summary_words.fetch_add(1, std::memory_order_relaxed);
            bits = words[word];
        }
        return word * 64 + static_cast<size_t>(__builtin_ctzll(bits));
    };
    const auto find_previous_set = [&](auto&& self, size_t level, size_t start) -> std::optional<size_t> {
        const auto& words = sidecar.live_summary[level];
        if (words.empty())
            return std::nullopt;
        start = std::min(start, words.size() * 64 - 1);
        size_t word = start / 64;
        ++session.census.last_live_summary_words_probed;
        if (diagnostics_)
            diagnostics_->current_live_summary_words.fetch_add(1, std::memory_order_relaxed);
        uint64_t bits = words[word] & (start % 64 == 63 ? ~uint64_t{0} : (uint64_t{1} << ((start % 64) + 1)) - 1);
        while (bits == 0) {
            if (level + 1 >= sidecar.live_summary.size() || word == 0)
                return std::nullopt;
            const auto parent = self(self, level + 1, word - 1);
            if (!parent)
                return std::nullopt;
            word = *parent;
            ++session.census.last_live_summary_words_probed;
            if (diagnostics_)
                diagnostics_->current_live_summary_words.fetch_add(1, std::memory_order_relaxed);
            bits = words[word];
        }
        return word * 64 + static_cast<size_t>(63 - __builtin_clzll(bits));
    };
    const auto next_live = [&](uint64_t position, bool backwards) -> std::optional<uint64_t> {
        if (!latest_snapshot)
            return backwards ? (position > base_first ? std::optional<uint64_t>{position - 1} : std::nullopt)
                             : (position < base_last ? std::optional<uint64_t>{position} : std::nullopt);
        if (!backwards) {
            while (position < base_last) {
                const size_t word = static_cast<size_t>(position / 64);
                uint64_t bits = sidecar.live_bitmap[word] & (~uint64_t{0} << (position % 64));
                if (word == (base_last - 1) / 64)
                    bits &= base_last % 64 == 0 ? ~uint64_t{0} : ((uint64_t{1} << (base_last % 64)) - 1);
                if (bits != 0)
                    return static_cast<uint64_t>(word * 64 + __builtin_ctzll(bits));
                if (sidecar.live_summary.empty())
                    return std::nullopt;
                if (diagnostics_)
                    diagnostics_->current_live_summary_skips.fetch_add(1, std::memory_order_relaxed);
                const auto next_word = find_next_set(find_next_set, 0, word + 1);
                if (!next_word)
                    return std::nullopt;
                position = static_cast<uint64_t>(*next_word * 64);
            }
            return std::nullopt;
        }
        while (position > base_first) {
            const uint64_t candidate = position - 1;
            const size_t word = static_cast<size_t>(candidate / 64);
            const uint64_t mask = candidate % 64 == 63 ? ~uint64_t{0} : ((uint64_t{1} << ((candidate % 64) + 1)) - 1);
            uint64_t bits = sidecar.live_bitmap[word] & mask;
            if (word == base_first / 64)
                bits &= ~uint64_t{0} << (base_first % 64);
            if (bits != 0)
                return static_cast<uint64_t>(word * 64 + 63 - __builtin_clzll(bits));
            if (sidecar.live_summary.empty() || word == 0 || word == base_first / 64)
                return std::nullopt;
            if (diagnostics_)
                diagnostics_->current_live_summary_skips.fetch_add(1, std::memory_order_relaxed);
            const auto previous_word = find_previous_set(find_previous_set, 0, word - 1);
            if (!previous_word)
                return std::nullopt;
            if (*previous_word > (std::numeric_limits<uint64_t>::max() - 64) / 64)
                return std::nullopt;
            position = static_cast<uint64_t>(*previous_word * 64 + 64);
            if (position > sidecar.count)
                return std::nullopt;
        }
        return std::nullopt;
    };
    const auto advance_base = [&]() {
        if (base_first == base_last) {
            base_valid = false;
            return true;
        }
        const auto next = next_live(base_position, reverse);
        if (!next) {
            base_valid = false;
            return true;
        }
        const uint64_t position = *next;
        base_position = reverse ? position : position + 1;
        ++session.census.last_base_entries_probed;
        if (diagnostics_ && latest_snapshot)
            diagnostics_->current_live_base_candidates.fetch_add(1, std::memory_order_relaxed);
        const KeySpan span = key_at(position);
        base_current = {
            EncodedKey(span.data, span.data + span.size),
            {schema.id, GetLeAt<uint64_t>(mapped + kSidecarHeaderBytes + position * kSidecarEntryBytes + 8)}};
        base_valid = true;
        return true;
    };
    advance_base();
    const auto& committed_overlay = latest_snapshot ? current_overlay_ : overlay_;
    OverlayRangeCursor<CommittedOverlay> committed(committed_overlay, schema.id, index.constraint_id, first_key, last_key,
                                                   reverse, session.census.last_overlay_nodes_probed,
                                                   session.census.last_row_ids_probed,
                                                   session.census.last_overlay_refs_examined);
    OverlayRangeCursor<DeltaOverlay> local(session.overlay, schema.id, index.constraint_id, first_key, last_key,
                                           reverse, session.census.last_overlay_nodes_probed,
                                           session.census.last_row_ids_probed,
                                           session.census.last_overlay_refs_examined);
    std::optional<std::pair<EncodedKey, epoch_si_poc::RowId>> emitted;
    for (;;) {
        std::optional<std::pair<EncodedKey, epoch_si_poc::RowId>> selected;
        const auto consider = [&](std::pair<EncodedKey, epoch_si_poc::RowId> candidate) {
            if (!selected || (reverse ? *selected < candidate : candidate < *selected))
                selected = std::move(candidate);
        };
        if (base_valid)
            consider(base_current);
        if (committed.valid())
            consider({committed.key(), committed.id()});
        if (local.valid())
            consider({local.key(), local.id()});
        if (!selected)
            break;
        enum class Source { Base, Committed, Local } source;
        if (base_valid && base_current == *selected) {
            source = Source::Base;
        } else if (committed.valid() && std::pair{committed.key(), committed.id()} == *selected) {
            source = Source::Committed;
        } else {
            source = Source::Local;
        }
        if ((!emitted || *emitted != *selected) && !visitor(selected->first, selected->second)) {
            *usable = true;
            return;
        }
        emitted = std::move(selected);
        if (source == Source::Base) {
            advance_base();
        } else if (source == Source::Committed)
            committed.Advance();
        else
            local.Advance();
    }
    *usable = true;
}

template <typename Overlay>
void DeltaDatabase::AddOverlay(Overlay& overlay, const TableSchema& schema, const std::vector<Cell>& cells,
                               epoch_si_poc::RowId id, const std::vector<Cell>* previous) {
    for (const Index& index : schema.indexes) {
        EncodedKey key = EncodeKey(schema, index, cells);
        if (!previous || key != EncodeKey(schema, index, *previous))
            AppendOverlay(overlay, {schema.id, index.constraint_id, std::move(key)}, id.local_id);
    }
}

void DeltaDatabase::RemoveOverlay(DeltaOverlay& overlay, const TableSchema& schema, const std::vector<Cell>& cells,
                                  epoch_si_poc::RowId id) {
    for (const Index& index : schema.indexes) {
        RemoveOverlay(overlay, schema, index, cells, id);
    }
}

void DeltaDatabase::RemoveOverlay(DeltaOverlay& overlay, const TableSchema& schema, const Index& index,
                                  const std::vector<Cell>& cells, epoch_si_poc::RowId id) {
    auto key = EncodeKey(schema, index, cells);
    auto found = overlay.find({schema.id, index.constraint_id, std::move(key)});
    if (found == overlay.end())
        return;
    auto& ids = found->second;
    ids.erase(std::remove(ids.begin(), ids.end(), id.local_id), ids.end());
    if (ids.empty())
        overlay.erase(found);
}

void DeltaDatabase::VisitRows(DeltaSession& session, const TableSchema& schema,
                              const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions,
                              const std::function<void(epoch_si_poc::RowId, const epoch_si_poc::RowImage&)>& visitor,
                              bool* used_index, const IndexEqualities* equalities, size_t* inferred_used) {
    auto& txn = Txn(session);
    bool usable = false;
    const auto candidates = IndexedCandidates(session, schema, conditions, &usable, equalities, inferred_used);
    if (used_index)
        *used_index = usable;
    session.census.last_row_reads_probed = 0;
    if (!usable) {
        db_.engine().VisitScan(txn, schema.id, visitor);
        return;
    }
    for (epoch_si_poc::RowId id : candidates) {
        ++session.census.last_row_reads_probed;
        if (auto row = db_.engine().Read(txn, id))
            visitor(id, *row);
    }
}

epoch_si_poc::RowImage DeltaDatabase::EncodeRow(const TableSchema& schema, const std::vector<Cell>& cells) const {
    if (cells.size() != schema.columns.size())
        throw std::runtime_error("Delta row column count mismatch");
    epoch_si_poc::RowImage image;
    PutLe<uint32_t>(image.bytes, schema.version);
    for (size_t i = 0; i < cells.size(); ++i) {
        const Column& column = schema.columns[i];
        const Cell& cell = cells[i];
        if (cell.is_null && !column.nullable)
            throw std::runtime_error("NULL in non-nullable Delta column");
        image.bytes.push_back(cell.is_null ? 0 : 1);
        if (cell.is_null)
            continue;
        if (column.type == ColumnType::Int)
            PutLe<int32_t>(image.bytes, cell.integer);
        else if (column.type == ColumnType::Float) {
            uint32_t bits;
            std::memcpy(&bits, &cell.floating, sizeof(bits));
            PutLe<uint32_t>(image.bytes, bits);
        } else {
            if (cell.text.size() > column.length)
                throw std::runtime_error("Delta CHAR value too long");
            PutLe<uint32_t>(image.bytes, static_cast<uint32_t>(cell.text.size()));
            image.bytes.insert(image.bytes.end(), cell.text.begin(), cell.text.end());
        }
        if (image.bytes.size() > kMaxRowBytes)
            throw std::runtime_error("Delta row exceeds limit");
    }
    for (const Index& index : schema.indexes) {
        if (!index.unique)
            continue;
        bool has_null = false;
        for (uint32_t column : index.columns)
            has_null = has_null || cells[column].is_null;
        if (!has_null)
            image.claims.push_back({index.constraint_id, EncodeKey(schema, index, cells)});
    }
    std::sort(image.claims.begin(), image.claims.end());
    image.claims.erase(std::unique(image.claims.begin(), image.claims.end()), image.claims.end());
    return image;
}

std::vector<DeltaDatabase::Cell> DeltaDatabase::DecodeRow(const TableSchema& schema,
                                                          const epoch_si_poc::RowImage& image) const {
    if (image.deleted || image.bytes.size() > kMaxRowBytes)
        throw std::runtime_error("invalid Delta row image");
    size_t offset = 0;
    if (GetLe<uint32_t>(image.bytes, offset) != schema.version)
        throw std::runtime_error("Delta schema version mismatch");
    std::vector<Cell> cells(schema.columns.size());
    for (size_t i = 0; i < cells.size(); ++i) {
        if (offset >= image.bytes.size())
            throw std::runtime_error("truncated Delta row null tag");
        const uint8_t present = image.bytes[offset++];
        if (present > 1 || (!present && !schema.columns[i].nullable))
            throw std::runtime_error("invalid Delta row null tag");
        if (!present)
            continue;
        cells[i].is_null = false;
        if (schema.columns[i].type == ColumnType::Int)
            cells[i].integer = GetLe<int32_t>(image.bytes, offset);
        else if (schema.columns[i].type == ColumnType::Float) {
            const uint32_t bits = GetLe<uint32_t>(image.bytes, offset);
            std::memcpy(&cells[i].floating, &bits, sizeof(bits));
        } else {
            const uint32_t length = GetLe<uint32_t>(image.bytes, offset);
            if (length > schema.columns[i].length || offset > image.bytes.size() ||
                image.bytes.size() - offset < length)
                throw std::runtime_error("invalid Delta CHAR payload");
            cells[i].text.assign(reinterpret_cast<const char*>(image.bytes.data() + offset), length);
            offset += length;
        }
    }
    if (offset != image.bytes.size())
        throw std::runtime_error("Delta row has trailing bytes");
    return cells;
}

bool DeltaDatabase::Matches(const TableSchema& schema, const std::vector<Cell>& cells,
                            const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions) const {
    const auto position = [&](const ast::Col& column) {
        if (!column.tab_name.empty() && column.tab_name != schema.name)
            throw std::runtime_error("unknown Delta qualifier");
        for (size_t i = 0; i < schema.columns.size(); ++i)
            if (schema.columns[i].name == column.col_name)
                return i;
        throw std::runtime_error("Delta column not found: " + column.col_name);
    };
    for (const auto& condition : conditions) {
        if (condition->lhs->type != ast::AstType::Col)
            throw std::runtime_error("Delta predicate lhs must be a column");
        const size_t lhs_pos = position(static_cast<const ast::Col&>(*condition->lhs));
        const Cell& lhs = cells[lhs_pos];
        if (condition->op == ast::SV_OP_IS_NULL || condition->op == ast::SV_OP_IS_NOT_NULL) {
            if ((condition->op == ast::SV_OP_IS_NULL) != lhs.is_null)
                return false;
            continue;
        }
        Cell rhs;
        if (condition->rhs->type == ast::AstType::Col) {
            const size_t rhs_pos = position(static_cast<const ast::Col&>(*condition->rhs));
            if (schema.columns[rhs_pos].type != schema.columns[lhs_pos].type)
                throw std::runtime_error("Delta predicate type mismatch");
            rhs = cells[rhs_pos];
        } else {
            rhs = Literal(schema.columns[lhs_pos], static_cast<const ast::Value&>(*condition->rhs));
        }
        if (lhs.is_null || rhs.is_null)
            return false;
        int comparison = 0;
        if (schema.columns[lhs_pos].type == ColumnType::Int)
            comparison = lhs.integer < rhs.integer ? -1 : lhs.integer > rhs.integer;
        else if (schema.columns[lhs_pos].type == ColumnType::Float)
            comparison = lhs.floating < rhs.floating ? -1 : lhs.floating > rhs.floating;
        else
            comparison = lhs.text.compare(rhs.text);
        if (!CompareResult(comparison, condition->op))
            return false;
    }
    return true;
}

void DeltaDatabase::EmitCells(const std::vector<Column>& columns, const std::vector<std::vector<Cell>>& rows,
                              QueryResultSink* sink) const {
    if (!sink)
        return;
    std::vector<ColMeta> metadata;
    std::vector<std::string> names;
    int size = 0;
    for (const auto& column : columns) {
        metadata.push_back({"", column.name,
                            column.type == ColumnType::Int     ? TYPE_INT
                            : column.type == ColumnType::Float ? TYPE_FLOAT
                                                               : TYPE_STRING,
                            static_cast<int>(column.length), size});
        names.push_back(column.name);
        size += static_cast<int>(column.length);
    }
    bind_null_positions(metadata, size);
    sink->begin_query(metadata, names);
    for (const auto& row : rows) {
        std::vector<char> tuple(static_cast<size_t>(size + null_bitmap_bytes(metadata.size())), 0);
        auto output = metadata;
        for (size_t i = 0; i < row.size(); ++i) {
            if (row[i].is_null) {
                set_null(tuple.data(), output[i]);
                continue;
            }
            if (output[i].type == TYPE_INT)
                write_unaligned<int32_t>(tuple.data() + output[i].offset, row[i].integer);
            else if (output[i].type == TYPE_FLOAT)
                write_float(tuple.data() + output[i].offset, row[i].floating);
            else {
                std::memcpy(tuple.data() + output[i].offset, row[i].text.data(), row[i].text.size());
                output[i].value_length_is_exact = true;
                output[i].len = static_cast<int>(row[i].text.size());
            }
        }
        sink->append_row(output, tuple.data(), tuple.size());
    }
}

void DeltaDatabase::EmitRows(const TableSchema& schema, const ast::SelectStmt& select,
                             const std::vector<std::vector<Cell>>& rows, QueryResultSink* sink, bool aggregate_values,
                             bool query_started) const {
    if (!sink)
        return;
    const bool aggregate = std::any_of(select.select_items.begin(), select.select_items.end(),
                                       [](const auto& item) { return item->expr->type == ast::AstType::AggExpr; });
    if (aggregate) {
        if (select.has_select_star ||
            std::any_of(select.select_items.begin(), select.select_items.end(),
                        [](const auto& item) { return item->expr->type != ast::AstType::AggExpr; }))
            throw std::runtime_error("mixed Delta aggregate projection is unsupported");
        struct State {
            int64_t count = 0;
            bool seen = false;
            double sum = 0;
            Cell min;
            std::set<std::string> distinct;
        };
        std::vector<State> states(select.select_items.size());
        std::vector<size_t> positions(select.select_items.size());
        std::vector<Column> outputs;
        for (size_t i = 0; i < select.select_items.size(); ++i) {
            const auto& agg = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
            Column output{select.select_items[i]->alias.empty() ? "?column?" : select.select_items[i]->alias,
                          ColumnType::Int, 4, false};
            if (!agg.is_star) {
                if (!agg.col)
                    throw std::runtime_error("invalid Delta aggregate");
                positions[i] = 0;
                while (positions[i] < schema.columns.size() && schema.columns[positions[i]].name != agg.col->col_name)
                    ++positions[i];
                if (positions[i] == schema.columns.size())
                    throw std::runtime_error("Delta aggregate column not found");
                if (agg.func != ast::AGG_COUNT) {
                    output.type = schema.columns[positions[i]].type;
                    output.length = output.type == ColumnType::Char ? schema.columns[positions[i]].length : 4;
                }
            } else if (agg.func != ast::AGG_COUNT)
                throw std::runtime_error("unsupported Delta aggregate");
            outputs.push_back(std::move(output));
        }
        for (const auto& row : rows)
            for (size_t i = 0; !aggregate_values && i < outputs.size(); ++i) {
                const auto& agg = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
                State& state = states[i];
                const Cell value = agg.is_star ? Cell{} : row[positions[i]];
                if (agg.func == ast::AGG_COUNT) {
                    if (agg.is_star) {
                        ++state.count;
                        continue;
                    }
                    if (value.is_null)
                        continue;
                    if (agg.is_distinct) {
                        if (!state.distinct.insert(DistinctKey(schema.columns[positions[i]], value)).second)
                            continue;
                    }
                    ++state.count;
                } else if (!value.is_null && agg.func == ast::AGG_SUM) {
                    if (outputs[i].type == ColumnType::Int)
                        state.sum += value.integer;
                    else if (outputs[i].type == ColumnType::Float)
                        state.sum += value.floating;
                    else
                        throw std::runtime_error("Delta SUM requires numeric column");
                    state.seen = true;
                } else if (!value.is_null && agg.func == ast::AGG_MIN) {
                    if (!state.seen || (outputs[i].type == ColumnType::Int     ? value.integer < state.min.integer
                                        : outputs[i].type == ColumnType::Float ? value.floating < state.min.floating
                                                                               : value.text < state.min.text))
                        state.min = value;
                    state.seen = true;
                } else if (agg.func != ast::AGG_MIN)
                    throw std::runtime_error("unsupported Delta aggregate");
            }
        std::vector<ColMeta> metadata;
        std::vector<std::string> names;
        int size = 0;
        for (const auto& output : outputs) {
            metadata.push_back({schema.name, output.name,
                                output.type == ColumnType::Int     ? TYPE_INT
                                : output.type == ColumnType::Float ? TYPE_FLOAT
                                                                   : TYPE_STRING,
                                static_cast<int>(output.length), size});
            names.push_back(output.name);
            size += static_cast<int>(output.length);
        }
        bind_null_positions(metadata, size);
        sink->begin_query(metadata, names);
        std::vector<char> tuple(static_cast<size_t>(size + null_bitmap_bytes(metadata.size())), 0);
        for (size_t i = 0; i < outputs.size(); ++i) {
            const auto& agg = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
            Cell value;
            if (aggregate_values)
                value = rows[0][i];
            else if (agg.func == ast::AGG_COUNT) {
                value.is_null = false;
                value.integer = static_cast<int32_t>(states[i].count);
            } else if (states[i].seen) {
                value = states[i].min;
                if (agg.func == ast::AGG_SUM) {
                    value.is_null = false;
                    if (outputs[i].type == ColumnType::Int)
                        value.integer = static_cast<int32_t>(states[i].sum);
                    else
                        value.floating = static_cast<float>(states[i].sum);
                }
            }
            if (value.is_null) {
                set_null(tuple.data(), metadata[i]);
                continue;
            }
            if (metadata[i].type == TYPE_INT)
                write_unaligned<int32_t>(tuple.data() + metadata[i].offset, value.integer);
            else if (metadata[i].type == TYPE_FLOAT)
                write_float(tuple.data() + metadata[i].offset, value.floating);
            else {
                std::memcpy(tuple.data() + metadata[i].offset, value.text.data(), value.text.size());
                metadata[i].value_length_is_exact = true;
                metadata[i].len = static_cast<int>(value.text.size());
            }
        }
        sink->append_row(metadata, tuple.data(), tuple.size());
        return;
    }
    struct Projection {
        Column column;
        std::optional<size_t> source;
        Cell literal;
    };
    std::vector<Projection> projection;
    if (select.has_select_star) {
        for (size_t i = 0; i < schema.columns.size(); ++i)
            projection.push_back({schema.columns[i], i, {}});
    } else {
        for (const auto& item : select.select_items) {
            if (item->expr->type == ast::AstType::Col) {
                const auto& col = static_cast<const ast::Col&>(*item->expr);
                size_t found = schema.columns.size();
                for (size_t i = 0; i < schema.columns.size(); ++i)
                    if (schema.columns[i].name == col.col_name)
                        found = i;
                if (found == schema.columns.size())
                    throw std::runtime_error("Delta projection column not found");
                Column output = schema.columns[found];
                if (!item->alias.empty())
                    output.name = item->alias;
                projection.push_back({std::move(output), found, {}});
            } else if (item->expr->type == ast::AstType::IntLit) {
                Column output{item->alias.empty() ? "?column?" : item->alias, ColumnType::Int, 4, false};
                projection.push_back(
                    {output, std::nullopt, Literal(output, static_cast<const ast::Value&>(*item->expr))});
            } else if (item->expr->type == ast::AstType::FloatLit) {
                Column output{item->alias.empty() ? "?column?" : item->alias, ColumnType::Float, 4, false};
                projection.push_back(
                    {output, std::nullopt, Literal(output, static_cast<const ast::Value&>(*item->expr))});
            } else if (item->expr->type == ast::AstType::StringLit) {
                const auto& literal = static_cast<const ast::StringLit&>(*item->expr);
                Column output{item->alias.empty() ? "?column?" : item->alias, ColumnType::Char,
                              static_cast<uint32_t>(std::max<size_t>(1, literal.val.size())), false};
                projection.push_back({output, std::nullopt, Literal(output, literal)});
            } else
                throw std::runtime_error("unsupported Delta projection");
        }
    }
    std::vector<ColMeta> metadata;
    std::vector<std::string> names;
    int data_size = 0;
    for (const Projection& item : projection) {
        ColMeta column;
        column.tab_name = schema.name;
        column.name = item.column.name;
        column.type = item.column.type == ColumnType::Int     ? TYPE_INT
                      : item.column.type == ColumnType::Float ? TYPE_FLOAT
                                                              : TYPE_STRING;
        column.len = static_cast<int>(item.column.length);
        column.offset = data_size;
        data_size += column.len;
        metadata.push_back(column);
        names.push_back(column.name);
    }
    bind_null_positions(metadata, data_size);
    if (!query_started)
        sink->begin_query(metadata, names);
    for (const auto& source_row : rows) {
        std::vector<char> tuple(static_cast<size_t>(data_size + null_bitmap_bytes(metadata.size())), 0);
        std::vector<ColMeta> row_columns = metadata;
        for (size_t i = 0; i < projection.size(); ++i) {
            const Cell& cell = projection[i].source ? source_row[*projection[i].source] : projection[i].literal;
            if (metadata[i].type == TYPE_STRING) {
                row_columns[i].value_length_is_exact = true;
                row_columns[i].len = cell.is_null ? 0 : static_cast<int>(cell.text.size());
            }
            if (cell.is_null) {
                set_null(tuple.data(), metadata[i]);
                continue;
            }
            char* target = tuple.data() + metadata[i].offset;
            if (metadata[i].type == TYPE_INT)
                write_unaligned<int32_t>(target, cell.integer);
            else if (metadata[i].type == TYPE_FLOAT)
                write_float(target, cell.floating);
            else
                std::memcpy(target, cell.text.data(), cell.text.size());
        }
        sink->append_row(row_columns, tuple.data(), tuple.size());
    }
}

void DeltaDatabase::EmitTables(QueryResultSink* sink) const {
    if (!sink)
        return;
    ColMeta column;
    column.name = "Tables";
    column.type = TYPE_STRING;
    column.len = 256;
    column.offset = 0;
    std::vector<ColMeta> metadata{column};
    bind_null_positions(metadata, column.len);
    sink->begin_query(metadata, {column.name});
    for (const auto& [name, table] : tables_) {
        std::vector<char> tuple(static_cast<size_t>(column.len + 1), 0);
        std::memcpy(tuple.data(), name.data(), std::min(name.size(), static_cast<size_t>(column.len)));
        auto row_column = metadata;
        row_column[0].len = static_cast<int>(std::min(name.size(), static_cast<size_t>(column.len)));
        row_column[0].value_length_is_exact = true;
        sink->append_row(row_column, tuple.data(), tuple.size());
    }
}

void DeltaDatabase::LoadCsv(const ast::LoadStmt& load, DeltaSession& session) {
    if (session.txn || db_.engine().active_transaction_count() != 0)
        throw std::runtime_error("Delta LOAD requires no active transaction");
    const TableSchema schema = Table(load.tab_name_);
    std::filesystem::path csv_path(load.file_name_);
    if (csv_path.is_relative())
        csv_path = std::filesystem::path(directory_) / csv_path;
    std::ifstream input(csv_path);
    if (!input)
        throw std::runtime_error("open Delta CSV");

    if (std::any_of(schema.indexes.begin(), schema.indexes.end(), [](const Index& index) { return index.unique; }))
        throw std::runtime_error("Delta LOAD into unique-indexed table is unsupported");
    auto writer = db_.BeginTableBase(schema.id);
    std::vector<std::vector<SidecarBuildEntry>> sidecars(schema.indexes.size());

    std::vector<size_t> source(schema.columns.size());
    for (size_t i = 0; i < source.size(); ++i)
        source[i] = i;
    std::vector<const char*> fields;
    fields.reserve(schema.columns.size() * 2);
    bool first_line = true;
    std::string line;
    while (std::getline(input, line)) {
        rmdb_csv::StripCr(line);
        if (!first_line && line.empty())
            continue;
        rmdb_csv::SplitLineInPlace(line, fields);
        if (fields.size() == schema.columns.size() + 1 && *fields.back() == '\0')
            fields.pop_back();

        if (first_line) {
            first_line = false;
            std::vector<size_t> header_source(schema.columns.size(), schema.columns.size());
            std::set<size_t> seen;
            bool header = fields.size() == schema.columns.size();
            for (size_t input_column = 0; input_column < fields.size(); ++input_column) {
                const char* begin = fields[input_column];
                const char* end = begin + std::strlen(begin);
                while (begin < end && std::isspace(static_cast<unsigned char>(*begin)))
                    ++begin;
                while (end > begin && std::isspace(static_cast<unsigned char>(end[-1])))
                    --end;
                const std::string header_name(begin, static_cast<size_t>(end - begin));
                auto found = std::find_if(schema.columns.begin(), schema.columns.end(),
                                          [&](const Column& column) { return column.name == header_name; });
                if (found == schema.columns.end() || !seen.insert(found - schema.columns.begin()).second) {
                    header = false;
                    break;
                }
                header_source[found - schema.columns.begin()] = input_column;
            }
            if (header) {
                source = std::move(header_source);
                continue;
            }
        }
        if (fields.size() < schema.columns.size())
            throw std::runtime_error("Delta CSV column count mismatch");

        std::vector<Cell> cells;
        cells.reserve(schema.columns.size());
        for (size_t i = 0; i < schema.columns.size(); ++i) {
            const char* field = fields[source[i]];
            if (*field == '\0' && schema.columns[i].type != ColumnType::Char) {
                if (!schema.columns[i].nullable)
                    throw std::runtime_error("empty numeric CSV field for non-nullable Delta column");
                cells.emplace_back();
                continue;
            }
            if (schema.columns[i].type == ColumnType::Int) {
                errno = 0;
                char* end = nullptr;
                const long value = std::strtol(field, &end, 10);
                if (errno != 0 || end == field || *end != '\0' || value < INT32_MIN || value > INT32_MAX)
                    throw std::runtime_error("invalid Delta CSV INT");
                Cell cell;
                cell.is_null = false;
                cell.integer = static_cast<int32_t>(value);
                cells.push_back(std::move(cell));
            } else if (schema.columns[i].type == ColumnType::Float) {
                errno = 0;
                char* end = nullptr;
                Cell cell;
                cell.is_null = false;
                cell.floating = std::strtof(field, &end);
                if (errno != 0 || end == field || *end != '\0' || !std::isfinite(cell.floating))
                    throw std::runtime_error("invalid Delta CSV FLOAT");
                cells.push_back(std::move(cell));
            } else {
                const size_t length = std::strlen(field);
                if (length > schema.columns[i].length)
                    throw std::runtime_error("Delta CSV CHAR too long");
                Cell cell;
                cell.is_null = false;
                cell.text.assign(field, length);
                cells.push_back(std::move(cell));
            }
        }
        writer.Append(EncodeRow(schema, cells));
        const uint64_t local_id = writer.row_count() - 1;
        for (size_t i = 0; i < schema.indexes.size(); ++i) {
            sidecars[i].push_back({EncodeKey(schema, schema.indexes[i], cells), local_id});
        }
    }
    if (!input.eof() || first_line)
        throw std::runtime_error("read Delta CSV");
    if (load_before_publish_hook_for_test_)
        load_before_publish_hook_for_test_();
    db_.PublishTableBase(std::move(writer));
    try {
        BuildSidecars(schema, std::move(sidecars), *db_.TableGeneration(schema.id));
    } catch (...) {
        // Sidecars are disposable acceleration artifacts; tablebase is already authoritative.
    }
}

PreparedDescription DeltaDatabase::DescribePrepared(const ast::TreeNode& tree,
                                                    const std::vector<DeltaValueType>& declared_parameters) const {
    auto state_lock = LockStateShared();
    RequireUsable();
    PreparedDescription result;
    result.catalog_generation = catalog_generation_;
    const auto value_type = [](ColumnType type) { return static_cast<DeltaValueType>(type); };
    const auto validate_value = [&](const Column& column, const ast::Value& value) {
        if (value.type != ast::AstType::Parameter) {
            (void)Literal(column, value);
            return;
        }
        const auto& parameter = static_cast<const ast::Parameter&>(value);
        if (parameter.ordinal == 0 || parameter.ordinal > declared_parameters.size())
            throw std::runtime_error("prepared Delta parameter is out of range");
        const DeltaValueType actual = declared_parameters[parameter.ordinal - 1];
        if (actual != value_type(column.type) && !(column.type == ColumnType::Float && actual == DeltaValueType::Int))
            throw std::runtime_error("prepared Delta parameter type mismatch");
    };
    const auto column = [&](const TableSchema& schema, const ast::Col& name) -> const Column& {
        if (!name.tab_name.empty() && name.tab_name != schema.name)
            throw std::runtime_error("unknown prepared Delta qualifier");
        auto found = std::find_if(schema.columns.begin(), schema.columns.end(),
                                  [&](const Column& candidate) { return candidate.name == name.col_name; });
        if (found == schema.columns.end())
            throw std::runtime_error("prepared Delta column not found: " + name.col_name);
        return *found;
    };
    const auto conditions = [&](const auto& resolve, const std::vector<std::unique_ptr<ast::BinaryExpr>>& predicates) {
        for (const auto& predicate : predicates) {
            if (!predicate || !predicate->lhs || !predicate->rhs || predicate->lhs->type != ast::AstType::Col)
                throw std::runtime_error("invalid prepared Delta predicate");
            const Column& lhs = resolve(static_cast<const ast::Col&>(*predicate->lhs));
            if (predicate->op == ast::SV_OP_IS_NULL || predicate->op == ast::SV_OP_IS_NOT_NULL)
                continue;
            if (predicate->rhs->type == ast::AstType::Col) {
                if (resolve(static_cast<const ast::Col&>(*predicate->rhs)).type != lhs.type)
                    throw std::runtime_error("prepared Delta predicate type mismatch");
            } else if (predicate->rhs->type == ast::AstType::Parameter ||
                       predicate->rhs->type == ast::AstType::IntLit || predicate->rhs->type == ast::AstType::FloatLit ||
                       predicate->rhs->type == ast::AstType::StringLit ||
                       predicate->rhs->type == ast::AstType::NullLit) {
                validate_value(lhs, static_cast<const ast::Value&>(*predicate->rhs));
            } else {
                throw std::runtime_error("unsupported prepared Delta predicate rhs");
            }
        }
    };

    if (tree.type == ast::AstType::SelectStmt) {
        const auto& select = static_cast<const ast::SelectStmt&>(tree);
        if (select.tabs.size() == 2 && select.jointree.empty() && !select.has_sort && !select.has_limit &&
            select.group_by_cols.empty() && select.having_conds.empty()) {
            if (select.has_select_star || select.select_items.empty())
                throw std::runtime_error("unsupported prepared Delta multi-table projection");
            const TableSchema& first = Table(select.tabs[0].table_name);
            const TableSchema& second = Table(select.tabs[1].table_name);
            const auto multi_column = [&](const ast::Col& name) -> const Column& {
                const auto find = [&](const TableSchema& schema, const ast::TableRef& table) -> const Column* {
                    if (!name.tab_name.empty() && name.tab_name != table.table_name && name.tab_name != table.alias)
                        return nullptr;
                    auto found = std::find_if(schema.columns.begin(), schema.columns.end(),
                                              [&](const Column& candidate) { return candidate.name == name.col_name; });
                    return found == schema.columns.end() ? nullptr : &*found;
                };
                const Column* first_found = find(first, select.tabs[0]);
                const Column* second_found = find(second, select.tabs[1]);
                if (first_found && second_found)
                    throw std::runtime_error("ambiguous prepared Delta multi-table column: " + name.col_name);
                if (!first_found && !second_found)
                    throw std::runtime_error("prepared Delta column not found: " + name.col_name);
                return first_found ? *first_found : *second_found;
            };
            result.query = true;
            if (select.select_items.size() == 1 && select.select_items[0] &&
                select.select_items[0]->expr->type == ast::AstType::AggExpr &&
                static_cast<const ast::AggExpr&>(*select.select_items[0]->expr).func == ast::AGG_COUNT) {
                const auto& aggregate = static_cast<const ast::AggExpr&>(*select.select_items[0]->expr);
                if (aggregate.is_star || !aggregate.col)
                    throw std::runtime_error("unsupported prepared Delta multi-table aggregate");
                (void)multi_column(*aggregate.col);
                conditions(multi_column, select.conds);
                result.names.push_back(select.select_items[0]->alias.empty() ? "?column?"
                                                                             : select.select_items[0]->alias);
                result.types.push_back(DeltaValueType::Int);
                return result;
            }
            for (const auto& item : select.select_items) {
                if (!item || item->expr->type != ast::AstType::Col)
                    throw std::runtime_error("unsupported prepared Delta multi-table projection");
                const auto& source = static_cast<const ast::Col&>(*item->expr);
                const Column& found = multi_column(source);
                result.names.push_back(item->alias.empty() ? source.col_name : item->alias);
                result.types.push_back(value_type(found.type));
            }
            conditions(multi_column, select.conds);
            return result;
        }
        if (select.tabs.size() != 1 || !select.jointree.empty() || !select.having_conds.empty())
            throw std::runtime_error("unsupported prepared Delta SELECT shape");
        const TableSchema& schema = Table(select.tabs[0].table_name);
        conditions([&](const ast::Col& name) -> const Column& { return column(schema, name); }, select.conds);
        result.query = true;
        if (select.has_select_star) {
            if (!select.select_items.empty())
                throw std::runtime_error("invalid prepared Delta star projection");
            for (const Column& item : schema.columns) {
                result.names.push_back(item.name);
                result.types.push_back(value_type(item.type));
            }
        } else {
            if (select.select_items.empty())
                throw std::runtime_error("empty prepared Delta projection");
            for (const auto& item : select.select_items) {
                if (!item || !item->expr)
                    throw std::runtime_error("invalid prepared Delta projection");
                DeltaValueType type;
                std::string name = item->alias.empty() ? "?column?" : item->alias;
                if (item->expr->type == ast::AstType::Col) {
                    const auto& source = static_cast<const ast::Col&>(*item->expr);
                    type = value_type(column(schema, source).type);
                    if (item->alias.empty())
                        name = source.col_name;
                } else if (item->expr->type == ast::AstType::AggExpr) {
                    const auto& aggregate = static_cast<const ast::AggExpr&>(*item->expr);
                    if (aggregate.func == ast::AGG_COUNT)
                        type = DeltaValueType::Int;
                    else if (aggregate.col && (aggregate.func == ast::AGG_MIN || aggregate.func == ast::AGG_MAX ||
                                               aggregate.func == ast::AGG_SUM))
                        type = value_type(column(schema, *aggregate.col).type);
                    else
                        throw std::runtime_error("unsupported prepared Delta aggregate");
                } else if (item->expr->type == ast::AstType::Parameter) {
                    throw std::runtime_error("prepared Delta parameter projection is unsupported");
                } else if (item->expr->type == ast::AstType::IntLit) {
                    type = DeltaValueType::Int;
                } else if (item->expr->type == ast::AstType::FloatLit) {
                    type = DeltaValueType::Float;
                } else if (item->expr->type == ast::AstType::StringLit) {
                    type = DeltaValueType::Char;
                } else {
                    throw std::runtime_error("unsupported prepared Delta projection");
                }
                result.names.push_back(std::move(name));
                result.types.push_back(type);
            }
        }
    } else if (tree.type == ast::AstType::InsertStmt) {
        const auto& insert = static_cast<const ast::InsertStmt&>(tree);
        const TableSchema& schema = Table(insert.tab_name);
        if (insert.vals.size() != schema.columns.size())
            throw std::runtime_error("prepared Delta INSERT column count mismatch");
        for (size_t i = 0; i < insert.vals.size(); ++i) {
            if (!insert.vals[i])
                throw std::runtime_error("invalid prepared Delta INSERT value");
            validate_value(schema.columns[i], *insert.vals[i]);
        }
    } else if (tree.type == ast::AstType::DeleteStmt || tree.type == ast::AstType::UpdateStmt) {
        const bool deleting = tree.type == ast::AstType::DeleteStmt;
        const auto& name = deleting ? static_cast<const ast::DeleteStmt&>(tree).tab_name
                                    : static_cast<const ast::UpdateStmt&>(tree).tab_name;
        const TableSchema& schema = Table(name);
        conditions([&](const ast::Col& name) -> const Column& { return column(schema, name); },
                   deleting ? static_cast<const ast::DeleteStmt&>(tree).conds
                            : static_cast<const ast::UpdateStmt&>(tree).conds);
        if (!deleting) {
            const auto& update = static_cast<const ast::UpdateStmt&>(tree);
            if (update.set_clauses.empty())
                throw std::runtime_error("empty prepared Delta UPDATE");
            for (const auto& clause : update.set_clauses) {
                if (!clause)
                    throw std::runtime_error("invalid prepared Delta UPDATE clause");
                auto target = std::find_if(schema.columns.begin(), schema.columns.end(),
                                           [&](const Column& item) { return item.name == clause->col_name; });
                if (target == schema.columns.end())
                    throw std::runtime_error("prepared Delta UPDATE column not found");
                if (!clause->is_self_ref) {
                    if (!clause->val)
                        throw std::runtime_error("missing prepared Delta UPDATE value");
                    validate_value(*target, *clause->val);
                    continue;
                }
                if (!clause->rhs_col || column(schema, *clause->rhs_col).type != target->type)
                    throw std::runtime_error("prepared Delta UPDATE source mismatch");
                if (clause->op != ast::SetOp::ASSIGNMENT) {
                    if (!clause->val || (target->type != ColumnType::Int && target->type != ColumnType::Float))
                        throw std::runtime_error("invalid prepared Delta arithmetic");
                    validate_value(*target, *clause->val);
                }
                for (const auto& term : clause->additional_terms) {
                    if (!term.val || (target->type != ColumnType::Int && target->type != ColumnType::Float))
                        throw std::runtime_error("invalid prepared Delta arithmetic term");
                    validate_value(*target, *term.val);
                }
            }
        }
    } else if (tree.type != ast::AstType::TxnBegin && tree.type != ast::AstType::TxnCommit &&
               tree.type != ast::AstType::TxnAbort && tree.type != ast::AstType::TxnRollback) {
        throw std::runtime_error("unsupported prepared Delta statement");
    }
    return result;
}

std::unique_ptr<DeltaPreparedProgram>
DeltaDatabase::CompilePrepared(std::unique_ptr<ast::TreeNode> tree,
                               const std::vector<DeltaValueType>& declared_parameters) const {
    if (!tree)
        throw std::invalid_argument("null Delta prepared tree");
    const auto description = DescribePrepared(*tree, declared_parameters);
    const auto* select = dynamic_cast<const ast::SelectStmt*>(tree.get());
    if (select != nullptr && (select->tabs.size() > 1 || select->limit_is_parameter || select->offset_is_parameter))
        return nullptr;
    return std::make_unique<DeltaPreparedProgram>(std::move(tree), description.query, description.catalog_generation);
}

bool DeltaDatabase::Execute(std::unique_ptr<ast::TreeNode> tree, DeltaSession& session, QueryResultSink* sink) {
    return ExecuteImpl(tree.get(), session, sink);
}

bool DeltaDatabase::ExecutePrepared(const DeltaPreparedProgram& program, const DeltaParameterFrame& parameters,
                                    DeltaSession& session, QueryResultSink* sink) {
    if (!program.tree_ || program.catalog_generation_ != CatalogGeneration())
        throw std::runtime_error("stale Delta prepared statement");
    PreparedParameterScope parameter_scope(parameters);
    return ExecuteImpl(program.tree_.get(), session, sink, program.catalog_generation_);
}

bool DeltaDatabase::ExecuteImpl(const ast::TreeNode* tree, DeltaSession& session, QueryResultSink* sink,
                                uint64_t expected_catalog_generation) {
    if (!tree)
        return false;
    std::lock_guard<std::mutex> operation_lock(session.operation_mutex);
    if (expected_catalog_generation != 0 && expected_catalog_generation != CatalogGeneration())
        throw std::runtime_error("stale Delta prepared statement");
    const DiagnosticOperation diagnostic_operation = ClassifyDiagnosticOperation(tree->type);
    const bool counted_execution = diagnostics_ != nullptr;
    const auto execute_started =
        counted_execution ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    struct ExecutionInflightScope {
        DeltaDatabase* database;
        size_t index;
        bool enabled;
        ~ExecutionInflightScope() {
            if (!enabled)
                return;
            database->execute_inflight_[index].fetch_sub(1, std::memory_order_relaxed);
            database->inflight_execute_.fetch_sub(1, std::memory_order_relaxed);
            database->MaybeReportDiagnostics();
        }
    } inflight_scope{this, static_cast<size_t>(diagnostic_operation), counted_execution};
    if (counted_execution) {
        const uint64_t active = inflight_execute_.fetch_add(1, std::memory_order_relaxed) + 1;
        execute_inflight_[static_cast<size_t>(diagnostic_operation)].fetch_add(1, std::memory_order_relaxed);
        uint64_t peak = peak_inflight_execute_.load(std::memory_order_relaxed);
        while (peak < active && !peak_inflight_execute_.compare_exchange_weak(peak, active, std::memory_order_relaxed,
                                                                              std::memory_order_relaxed)) {
        }
    }
    const bool exclusive = tree->type == ast::AstType::StaticCheckpoint || tree->type == ast::AstType::CreateTable ||
                           tree->type == ast::AstType::CreateIndex || tree->type == ast::AstType::LoadStmt;
    if (exclusive && session.admission)
        throw std::runtime_error("Delta schema/checkpoint operation inside transaction");
    std::unique_lock<std::shared_mutex> exclusive_admission;
    std::shared_lock<std::shared_mutex> statement_admission;
    std::unique_lock<std::mutex> exclusive_commit_epoch_gate;
    if (exclusive)
        exclusive_admission = LockExecutionUnique();
    else if (!session.admission)
        statement_admission = LockExecutionShared();
    if (exclusive)
        exclusive_commit_epoch_gate = std::unique_lock<std::mutex>(commit_epoch_gate_);
    std::unique_lock<std::shared_mutex> unique_state_lock;
    std::shared_lock<std::shared_mutex> lock;
    const auto shared_wait_started =
        !exclusive && diagnostics_ ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    if (exclusive)
        unique_state_lock = LockStateUnique();
    else {
        lock = LockStateShared(true);
        if (diagnostics_) {
            const uint64_t elapsed = NsSince(shared_wait_started);
            execute_shared_wait_ns_.fetch_add(elapsed, std::memory_order_relaxed);
            RecordMax(execute_shared_wait_max_ns_, elapsed);
            execute_shared_calls_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    ExecutionCensus census;
    struct ExecutionDiagnosticsScope {
        DeltaDatabase* database;
        const DeltaSession* session;
        DiagnosticOperation operation;
        ExecutionCensus* census;
        std::chrono::steady_clock::time_point started;
        bool counted_execution;
        ~ExecutionDiagnosticsScope() {
            if (!counted_execution)
                return;
            database->CaptureQueryCensus(*session, *census);
            const size_t index = static_cast<size_t>(operation);
            const uint64_t elapsed = NsSince(started);
            database->execute_calls_[index].fetch_add(1, std::memory_order_relaxed);
            database->execute_ns_[index].fetch_add(elapsed, std::memory_order_relaxed);
            RecordMax(database->execute_max_ns_[index], elapsed);
            database->sidecar_base_entries_.fetch_add(census->base_entries, std::memory_order_relaxed);
            database->sidecar_overlay_refs_.fetch_add(census->overlay_refs, std::memory_order_relaxed);
            database->join_parameterized_probes_.fetch_add(census->parameterized_join_probes,
                                                            std::memory_order_relaxed);
            if (census->join_parameterized)
                database->join_parameterized_.fetch_add(1, std::memory_order_relaxed);
            if (census->join_fallback)
                database->join_fallback_.fetch_add(1, std::memory_order_relaxed);
            if (census->join_outer_indexed)
                database->join_outer_indexed_.fetch_add(1, std::memory_order_relaxed);
            database->join_outer_candidates_.fetch_add(census->join_outer_candidates, std::memory_order_relaxed);
            database->join_outer_full_scan_rows_.fetch_add(census->join_outer_full_scan_rows,
                                                           std::memory_order_relaxed);
            database->join_inferred_literal_components_.fetch_add(census->join_inferred_literal_components,
                                                                  std::memory_order_relaxed);
            database->join_inferred_literal_probes_.fetch_add(census->join_inferred_literal_probes,
                                                              std::memory_order_relaxed);
            database->ordered_early_stop_.fetch_add(census->ordered_early_stops, std::memory_order_relaxed);
            if (census->ordered_stream)
                database->ordered_stream_.fetch_add(1, std::memory_order_relaxed);
            if (census->ordered_materialize)
                database->ordered_materialize_.fetch_add(1, std::memory_order_relaxed);
        }
    } diagnostics_scope{this,
                        &session,
                        diagnostic_operation,
                        &census,
                        execute_started,
                        counted_execution};
    session.census = {};
    RequireUsable();
    if (execute_lock_hook_for_test_)
        execute_lock_hook_for_test_();
    if (tree->type == ast::AstType::ShowTables) {
        EmitTables(sink);
        return true;
    }
    if (tree->type == ast::AstType::SetTransaction) {
        if (static_cast<const ast::SetTransaction&>(*tree).isolation_level_ !=
            ast::IsolationLevelType::SNAPSHOT_ISOLATION)
            throw std::runtime_error("DeltaKernel supports SNAPSHOT ISOLATION only");
        return false;
    }
    if (tree->type == ast::AstType::TxnBegin) {
        if (session.txn)
            throw std::runtime_error("transaction already active");
        session.txn.emplace(db_.engine().Begin());
        session.explicit_txn = true;
        session.admission.emplace(std::move(statement_admission));
        return false;
    }
    if (tree->type == ast::AstType::TxnCommit) {
        Commit(session, lock, &census);
        return false;
    }
    if (tree->type == ast::AstType::TxnAbort || tree->type == ast::AstType::TxnRollback) {
        AbortLocked(session);
        return false;
    }
    if (tree->type == ast::AstType::StaticCheckpoint) {
        CheckpointSidecars();
        return false;
    }
    if (tree->type == ast::AstType::CreateTable) {
        const auto& create = static_cast<const ast::CreateTable&>(*tree);
        if (session.txn || create.fields.empty() || create.fields.size() > kMaxColumns ||
            next_table_id_ == std::numeric_limits<epoch_si_poc::TableId>::max() ||
            catalog_generation_ == std::numeric_limits<uint64_t>::max() || tables_.count(create.tab_name))
            throw std::runtime_error("invalid Delta CREATE TABLE");
        TableSchema table{next_table_id_, 1, create.tab_name, {}, {}};
        std::set<std::string> names;
        size_t maximum_row = sizeof(uint32_t) + create.fields.size();
        for (const auto& field : create.fields) {
            const auto* column = dynamic_cast<const ast::ColDef*>(field.get());
            if (!column || !names.insert(column->col_name).second)
                throw std::runtime_error("invalid Delta column");
            ColumnType type;
            uint32_t length;
            if (column->type_len->type == ast::SV_TYPE_INT) {
                type = ColumnType::Int;
                length = 4;
            } else if (column->type_len->type == ast::SV_TYPE_FLOAT) {
                type = ColumnType::Float;
                length = 4;
            } else if (column->type_len->type == ast::SV_TYPE_STRING && column->type_len->len > 0) {
                type = ColumnType::Char;
                length = static_cast<uint32_t>(column->type_len->len);
            } else
                throw std::runtime_error("DeltaKernel supports INT, FLOAT, and CHAR only");
            maximum_row += type == ColumnType::Char ? sizeof(uint32_t) + length : 4;
            if (maximum_row > kMaxRowBytes)
                throw std::runtime_error("Delta row schema exceeds limit");
            table.columns.push_back({column->col_name, type, length, true});
        }
        Catalog candidate = tables_;
        candidate.emplace(table.name, table);
        const auto next_table = static_cast<epoch_si_poc::TableId>(next_table_id_ + 1);
        const uint64_t generation = catalog_generation_ + 1;
        SaveCatalog(candidate, next_table, next_constraint_id_, generation);
        tables_.swap(candidate);
        table_by_id_.clear();
        for (const auto& [name, schema] : tables_)
            table_by_id_.emplace(schema.id, &schema);
        next_table_id_ = next_table;
        catalog_generation_ = generation;
        return false;
    }
    if (tree->type == ast::AstType::CreateIndex) {
        const auto& create = static_cast<const ast::CreateIndex&>(*tree);
        if (session.txn || create.col_names.empty() ||
            next_constraint_id_ == std::numeric_limits<epoch_si_poc::ConstraintId>::max() ||
            catalog_generation_ == std::numeric_limits<uint64_t>::max())
            throw std::runtime_error("invalid Delta CREATE INDEX");
        Catalog candidate = tables_;
        TableSchema& table = candidate.at(create.tab_name);
        Index index{next_constraint_id_, {}, false};
        for (const std::string& name : create.col_names) {
            auto found = std::find_if(table.columns.begin(), table.columns.end(),
                                      [&](const Column& c) { return c.name == name; });
            if (found == table.columns.end())
                throw std::runtime_error("Delta index column not found");
            const uint32_t position = static_cast<uint32_t>(found - table.columns.begin());
            if (std::find(index.columns.begin(), index.columns.end(), position) != index.columns.end())
                throw std::runtime_error("duplicate Delta index column");
            index.columns.push_back(position);
        }
        table.indexes.push_back(index);
        const auto next_constraint = static_cast<epoch_si_poc::ConstraintId>(next_constraint_id_ + 1);
        const uint64_t generation = catalog_generation_ + 1;
        SaveCatalog(candidate, next_table_id_, next_constraint, generation);
        tables_.swap(candidate);
        table_by_id_.clear();
        for (const auto& [name, schema] : tables_)
            table_by_id_.emplace(schema.id, &schema);
        next_constraint_id_ = next_constraint;
        catalog_generation_ = generation;
        try {
            // A declared index over WAL-only rows needs a tablebase generation before it can persist.
            CheckpointSidecars();
        } catch (...) {
            // The catalog is authoritative; a later checkpoint/Open retries acceleration construction.
        }
        RebuildSidecars(Table(create.tab_name));
        return false;
    }
    if (tree->type == ast::AstType::LoadStmt) {
        LoadCsv(static_cast<const ast::LoadStmt&>(*tree), session);
        return false;
    }
    const bool implicit = !session.txn;
    try {
        if (tree->type == ast::AstType::InsertStmt) {
            const auto& insert = static_cast<const ast::InsertStmt&>(*tree);
            const TableSchema& schema = Table(insert.tab_name);
            if (insert.vals.size() != schema.columns.size())
                throw std::runtime_error("Delta INSERT column count mismatch");
            std::vector<Cell> cells;
            for (size_t i = 0; i < insert.vals.size(); ++i)
                cells.push_back(Literal(schema.columns[i], *insert.vals[i]));
            const auto id = db_.engine().InsertImage(Txn(session), schema.id, EncodeRow(schema, cells));
            AddOverlay(session.overlay, schema, cells, id);
            session.private_insert_ids.insert(id);
        } else if (tree->type == ast::AstType::SelectStmt) {
            const auto& select = static_cast<const ast::SelectStmt&>(*tree);
            if (select.tabs.empty() || !select.jointree.empty() || !select.having_conds.empty())
                throw std::runtime_error("unsupported Delta SELECT shape");
            if (select.tabs.size() > 2)
                throw std::runtime_error("unsupported Delta SELECT shape");
            if (select.tabs.size() == 2) {
                const bool projection_query =
                    !select.select_items.empty() &&
                    std::all_of(select.select_items.begin(), select.select_items.end(),
                                [](const auto& item) { return item->expr->type == ast::AstType::Col; });
                if ((!projection_query &&
                     (!std::all_of(select.select_items.begin(), select.select_items.end(),
                                   [](const auto& item) { return item->expr->type == ast::AstType::AggExpr; }) ||
                      select.select_items.size() != 1)) ||
                    select.has_sort || select.has_limit)
                    throw std::runtime_error("unsupported Delta multi-table SELECT shape");
                const ast::AggExpr* aggregate_ptr =
                    projection_query ? nullptr : static_cast<const ast::AggExpr*>(select.select_items[0]->expr.get());
                if (!projection_query &&
                    ((aggregate_ptr->func != ast::AGG_COUNT && aggregate_ptr->func != ast::AGG_SUM &&
                      aggregate_ptr->func != ast::AGG_MIN && aggregate_ptr->func != ast::AGG_MAX) ||
                     aggregate_ptr->is_star || !aggregate_ptr->col))
                    throw std::runtime_error("unsupported Delta multi-table aggregate");
                const TableSchema& left_schema = Table(select.tabs[0].table_name);
                const TableSchema& right_schema = Table(select.tabs[1].table_name);
                session.census.last_parameterized_join_probes = 0;
                session.census.last_join_inner_rows_resolved = 0;
                session.census.last_join_pairs_rechecked = 0;
                session.census.last_join_full_scan_rows = 0;
                session.census.last_join_right_rows_visited = 0;
                session.census.last_join_base_entries_examined = 0;
                session.census.last_join_overlay_entries_examined = 0;
                session.census.last_join_overlay_refs_examined = 0;
                session.census.last_join_outer_indexed_queries = 0;
                session.census.last_join_outer_candidates = 0;
                session.census.last_join_outer_full_scan_rows = 0;
                session.census.last_join_inferred_literal_components = 0;
                session.census.last_join_inferred_literal_probes = 0;
                const auto pos = [](const TableSchema& schema, const std::string& name) {
                    for (size_t i = 0; i < schema.columns.size(); ++i)
                        if (schema.columns[i].name == name)
                            return i;
                    throw std::runtime_error("Delta multi-table column not found: " + name);
                };
                const auto resolve = [&](const ast::Col& column, const std::vector<Cell>& left,
                                         const std::vector<Cell>& right, const Column*& out_column,
                                         const Cell*& out_cell) {
                    const bool use_left = column.tab_name.empty() || column.tab_name == select.tabs[0].table_name ||
                                          column.tab_name == select.tabs[0].alias;
                    const bool use_right = column.tab_name.empty() || column.tab_name == select.tabs[1].table_name ||
                                           column.tab_name == select.tabs[1].alias;
                    const bool left_has =
                        use_left && std::any_of(left_schema.columns.begin(), left_schema.columns.end(),
                                                [&](const Column& c) { return c.name == column.col_name; });
                    const bool right_has =
                        use_right && std::any_of(right_schema.columns.begin(), right_schema.columns.end(),
                                                 [&](const Column& c) { return c.name == column.col_name; });
                    if (left_has == right_has)
                        throw std::runtime_error("ambiguous Delta multi-table column: " + column.col_name);
                    if (left_has) {
                        const size_t i = pos(left_schema, column.col_name);
                        out_column = &left_schema.columns[i];
                        out_cell = &left[i];
                    } else {
                        const size_t i = pos(right_schema, column.col_name);
                        out_column = &right_schema.columns[i];
                        out_cell = &right[i];
                    }
                };
                const Column* aggregate_column = nullptr;
                Column aggregate_output;
                if (!projection_query) {
                    const Cell* ignored;
                    const std::vector<Cell> left(left_schema.columns.size());
                    const std::vector<Cell> right(right_schema.columns.size());
                    resolve(*aggregate_ptr->col, left, right, aggregate_column, ignored);
                    aggregate_output = *aggregate_column;
                    aggregate_output.name =
                        select.select_items[0]->alias.empty() ? "?column?" : select.select_items[0]->alias;
                    if (aggregate_ptr->func == ast::AGG_COUNT)
                        aggregate_output = {aggregate_output.name, ColumnType::Int, 4, false};
                    else if (aggregate_ptr->func == ast::AGG_SUM && aggregate_column->type == ColumnType::Char)
                        throw std::runtime_error("Delta SUM requires numeric column");
                }
                std::vector<ColMeta> projection_meta;
                std::vector<std::string> projection_names;
                int projection_size = 0;
                if (projection_query && sink) {
                    for (const auto& item : select.select_items) {
                        const auto& col = static_cast<const ast::Col&>(*item->expr);
                        const Column* source;
                        const Cell* ignored;
                        resolve(col, std::vector<Cell>(left_schema.columns.size()),
                                std::vector<Cell>(right_schema.columns.size()), source, ignored);
                        ColMeta meta;
                        meta.tab_name = "";
                        meta.name = item->alias.empty() ? col.col_name : item->alias;
                        meta.type = source->type == ColumnType::Int     ? TYPE_INT
                                    : source->type == ColumnType::Float ? TYPE_FLOAT
                                                                        : TYPE_STRING;
                        meta.len = static_cast<int>(source->type == ColumnType::Char ? source->length : 4);
                        meta.offset = projection_size;
                        projection_size += meta.len;
                        projection_meta.push_back(meta);
                        projection_names.push_back(meta.name);
                    }
                    bind_null_positions(projection_meta, projection_size);
                }
                struct BoundPredicate {
                    const Column* lhs_column;
                    size_t lhs_pos;
                    int lhs_side;
                    const Column* rhs_column = nullptr;
                    size_t rhs_pos = 0;
                    int rhs_side = -1;
                    Cell literal;
                    ast::SvCompOp op;
                };
                const auto locate = [&](const ast::Col& column) {
                    const bool use_left = column.tab_name.empty() || column.tab_name == select.tabs[0].table_name ||
                                          column.tab_name == select.tabs[0].alias;
                    const bool use_right = column.tab_name.empty() || column.tab_name == select.tabs[1].table_name ||
                                           column.tab_name == select.tabs[1].alias;
                    const bool left_has =
                        use_left && std::any_of(left_schema.columns.begin(), left_schema.columns.end(),
                                                [&](const Column& c) { return c.name == column.col_name; });
                    const bool right_has =
                        use_right && std::any_of(right_schema.columns.begin(), right_schema.columns.end(),
                                                 [&](const Column& c) { return c.name == column.col_name; });
                    if (left_has == right_has)
                        throw std::runtime_error("ambiguous Delta multi-table column: " + column.col_name);
                    return std::tuple<int, size_t, const Column*>{
                        left_has ? 0 : 1,
                        left_has ? pos(left_schema, column.col_name) : pos(right_schema, column.col_name),
                        left_has ? &left_schema.columns[pos(left_schema, column.col_name)]
                                 : &right_schema.columns[pos(right_schema, column.col_name)]};
                };
                std::vector<BoundPredicate> left_predicates, right_predicates, cross_predicates;
                for (const auto& condition : select.conds) {
                    const auto* lhs =
                        condition && condition->lhs ? dynamic_cast<const ast::Col*>(condition->lhs.get()) : nullptr;
                    if (!lhs)
                        throw std::runtime_error("Delta predicate lhs must be a column");
                    auto [lhs_side, lhs_pos, lhs_column] = locate(*lhs);
                    BoundPredicate predicate{lhs_column, lhs_pos, lhs_side, nullptr, 0, -1, {}, condition->op};
                    if (condition->op != ast::SV_OP_IS_NULL && condition->op != ast::SV_OP_IS_NOT_NULL) {
                        if (const auto* rhs = dynamic_cast<const ast::Col*>(condition->rhs.get())) {
                            std::tie(predicate.rhs_side, predicate.rhs_pos, predicate.rhs_column) = locate(*rhs);
                            if (lhs_column->type != predicate.rhs_column->type)
                                throw std::runtime_error("Delta multi-table predicate type mismatch");
                        } else if (const auto* rhs = dynamic_cast<const ast::Value*>(condition->rhs.get()))
                            predicate.literal = Literal(*lhs_column, *rhs);
                        else
                            throw std::runtime_error("unsupported Delta multi-table predicate");
                    }
                    auto& destination = predicate.rhs_side < 0 || predicate.rhs_side == lhs_side
                                            ? (lhs_side == 0 ? left_predicates : right_predicates)
                                            : cross_predicates;
                    destination.push_back(std::move(predicate));
                }
                const size_t equality_columns = left_schema.columns.size() + right_schema.columns.size();
                std::vector<size_t> equality_parent(equality_columns);
                for (size_t i = 0; i < equality_parent.size(); ++i)
                    equality_parent[i] = i;
                const auto equality_node = [&](int side, size_t position) {
                    return side == 0 ? position : left_schema.columns.size() + position;
                };
                const auto equality_column = [&](size_t node) -> const Column& {
                    return node < left_schema.columns.size() ? left_schema.columns[node]
                                                             : right_schema.columns[node - left_schema.columns.size()];
                };
                const auto find_root = [&](size_t node) {
                    while (equality_parent[node] != node)
                        node = equality_parent[node];
                    return node;
                };
                const auto each_predicate = [&](const auto& visitor) {
                    for (const auto& predicate : left_predicates)
                        visitor(predicate);
                    for (const auto& predicate : right_predicates)
                        visitor(predicate);
                    for (const auto& predicate : cross_predicates)
                        visitor(predicate);
                };
                each_predicate([&](const BoundPredicate& predicate) {
                    if (predicate.op != ast::SV_OP_EQ || predicate.rhs_side < 0)
                        return;
                    const size_t left = find_root(equality_node(predicate.lhs_side, predicate.lhs_pos));
                    const size_t right = find_root(equality_node(predicate.rhs_side, predicate.rhs_pos));
                    if (left != right)
                        equality_parent[right] = left;
                });
                std::vector<std::optional<Cell>> component_literals(equality_columns);
                std::vector<bool> direct_literals(equality_columns);
                bool equality_unsatisfiable = false;
                const auto same_literal = [&](const Column& column, const Cell& left, const Cell& right) {
                    if (left.is_null || right.is_null)
                        return left.is_null == right.is_null;
                    if (column.type == ColumnType::Int)
                        return left.integer == right.integer;
                    if (column.type == ColumnType::Float)
                        return left.floating == right.floating;
                    return left.text == right.text;
                };
                each_predicate([&](const BoundPredicate& predicate) {
                    if (predicate.op != ast::SV_OP_EQ || predicate.rhs_side >= 0)
                        return;
                    const size_t node = equality_node(predicate.lhs_side, predicate.lhs_pos);
                    const size_t root = find_root(node);
                    direct_literals[node] = true;
                    if (predicate.literal.is_null ||
                        (component_literals[root] &&
                         !same_literal(equality_column(root), *component_literals[root], predicate.literal))) {
                        equality_unsatisfiable = true;
                        return;
                    }
                    component_literals[root] = predicate.literal;
                });
                IndexEqualities left_equalities;
                left_equalities.values.resize(left_schema.columns.size());
                left_equalities.inferred.resize(left_schema.columns.size());
                IndexEqualities right_equalities;
                right_equalities.values.resize(right_schema.columns.size());
                right_equalities.inferred.resize(right_schema.columns.size());
                std::vector<bool> inferred_components(equality_columns);
                for (size_t node = 0; node < equality_columns; ++node) {
                    const size_t root = find_root(node);
                    if (!component_literals[root])
                        continue;
                    const Cell& value = *component_literals[root];
                    if (equality_column(node).type == ColumnType::Char &&
                        value.text.size() > equality_column(node).length) {
                        equality_unsatisfiable = true;
                        continue;
                    }
                    const bool inferred = !direct_literals[node];
                    inferred_components[root] = inferred_components[root] || inferred;
                    if (node < left_schema.columns.size()) {
                        left_equalities.values[node] = value;
                        left_equalities.inferred[node] = inferred;
                    } else {
                        const size_t right = node - left_schema.columns.size();
                        right_equalities.values[right] = value;
                        right_equalities.inferred[right] = inferred;
                    }
                }
                session.census.last_join_inferred_literal_components =
                    static_cast<size_t>(std::count(inferred_components.begin(), inferred_components.end(), true));
                const auto matches = [](const std::vector<BoundPredicate>& predicates, const std::vector<Cell>& left,
                                        const std::vector<Cell>& right) {
                    for (const auto& predicate : predicates) {
                        const Cell& lhs = predicate.lhs_side == 0 ? left[predicate.lhs_pos] : right[predicate.lhs_pos];
                        if (predicate.op == ast::SV_OP_IS_NULL || predicate.op == ast::SV_OP_IS_NOT_NULL) {
                            if ((predicate.op == ast::SV_OP_IS_NULL) != lhs.is_null)
                                return false;
                            continue;
                        }
                        const Cell& rhs = predicate.rhs_side < 0 ? predicate.literal
                                                                 : (predicate.rhs_side == 0 ? left[predicate.rhs_pos]
                                                                                            : right[predicate.rhs_pos]);
                        if (lhs.is_null || rhs.is_null)
                            return false;
                        const int cmp = predicate.lhs_column->type == ColumnType::Int
                                            ? (lhs.integer < rhs.integer ? -1 : lhs.integer > rhs.integer)
                                        : predicate.lhs_column->type == ColumnType::Float
                                            ? (lhs.floating < rhs.floating ? -1 : lhs.floating > rhs.floating)
                                            : lhs.text.compare(rhs.text);
                        if (!CompareResult(cmp, predicate.op))
                            return false;
                    }
                    return true;
                };
                // ponytail: logical RowImage object/payload bound, not exact RSS; add a spill/hash join when too big.
                std::vector<epoch_si_poc::RowImage> left_rows;
                size_t left_bytes = 0;
                bool left_used_index = false;
                size_t outer_inferred_used = 0;
                if (!equality_unsatisfiable) {
                    VisitRows(
                        session, left_schema, select.conds,
                        [&](epoch_si_poc::RowId, const epoch_si_poc::RowImage& image) {
                            if (!left_used_index) {
                                ++session.census.last_join_full_scan_rows;
                                ++session.census.last_join_outer_full_scan_rows;
                            }
                            auto left = DecodeRow(left_schema, image);
                            if (!matches(left_predicates, left, {}))
                                return;
                            size_t image_bytes = sizeof(epoch_si_poc::RowImage);
                            const auto add_image_bytes = [&](size_t bytes) {
                                if (bytes > kMaxJoinMaterializedBytes - image_bytes)
                                    throw std::runtime_error("unsupported Delta join exceeds memory limit");
                                image_bytes += bytes;
                            };
                            add_image_bytes(image.bytes.size());
                            if (image.claims.size() >
                                (kMaxJoinMaterializedBytes - image_bytes) / sizeof(epoch_si_poc::ConstraintClaim))
                                throw std::runtime_error("unsupported Delta join exceeds memory limit");
                            image_bytes += image.claims.size() * sizeof(epoch_si_poc::ConstraintClaim);
                            for (const auto& claim : image.claims)
                                add_image_bytes(claim.bytes.size());
                            if (left_rows.size() == kMaxJoinMaterializedRows ||
                                image_bytes > kMaxJoinMaterializedBytes - left_bytes)
                                throw std::runtime_error("unsupported Delta join exceeds memory limit");
                            left_bytes += image_bytes;
                            left_rows.push_back(image);
                        },
                        &left_used_index, &left_equalities, &outer_inferred_used);
                    if (left_used_index) {
                        session.census.last_join_outer_indexed_queries = 1;
                        session.census.last_join_outer_candidates = session.census.last_row_reads_probed;
                        session.census.last_join_inferred_literal_probes = outer_inferred_used != 0;
                    }
                }
                int64_t count = 0;
                bool aggregate_seen = false;
                double sum = 0;
                Cell extreme;
                std::set<std::string> distinct;
                const auto emit_pair = [&](const std::vector<Cell>& left, const std::vector<Cell>& right) {
                    if (projection_query) {
                        if (!sink)
                            return;
                        std::vector<char> tuple(
                            static_cast<size_t>(projection_size + null_bitmap_bytes(projection_meta.size())), 0);
                        auto columns = projection_meta;
                        for (size_t i = 0; i < select.select_items.size(); ++i) {
                            const Column* source;
                            const Cell* value;
                            resolve(static_cast<const ast::Col&>(*select.select_items[i]->expr), left, right, source,
                                    value);
                            if (value->is_null) {
                                set_null(tuple.data(), columns[i]);
                                continue;
                            }
                            if (columns[i].type == TYPE_INT)
                                write_unaligned<int32_t>(tuple.data() + columns[i].offset, value->integer);
                            else if (columns[i].type == TYPE_FLOAT)
                                write_float(tuple.data() + columns[i].offset, value->floating);
                            else {
                                std::memcpy(tuple.data() + columns[i].offset, value->text.data(), value->text.size());
                                columns[i].value_length_is_exact = true;
                                columns[i].len = static_cast<int>(value->text.size());
                            }
                        }
                        sink->append_row(columns, tuple.data(), tuple.size());
                        return;
                    }
                    const Column* value_column;
                    const Cell* value;
                    resolve(*aggregate_ptr->col, left, right, value_column, value);
                    if (value->is_null)
                        return;
                    if (aggregate_ptr->is_distinct && !distinct.insert(DistinctKey(*value_column, *value)).second)
                        return;
                    if (aggregate_ptr->func == ast::AGG_COUNT)
                        ++count;
                    else if (aggregate_ptr->func == ast::AGG_SUM) {
                        sum += value_column->type == ColumnType::Int ? value->integer : value->floating;
                        aggregate_seen = true;
                    } else {
                        const int comparison =
                            value_column->type == ColumnType::Int
                                ? (value->integer < extreme.integer ? -1 : value->integer > extreme.integer)
                            : value_column->type == ColumnType::Float
                                ? (value->floating < extreme.floating ? -1 : value->floating > extreme.floating)
                                : value->text.compare(extreme.text);
                        if (!aggregate_seen || (aggregate_ptr->func == ast::AGG_MIN ? comparison < 0 : comparison > 0))
                            extreme = *value;
                        aggregate_seen = true;
                    }
                };
                struct ProbeKeySource {
                    bool from_literal = false;
                    size_t left_pos = 0;
                    Cell literal;
                };
                const Index* probe_index = nullptr;
                std::vector<ProbeKeySource> probe_sources;
                for (const Index& index : right_schema.indexes) {
                    std::vector<ProbeKeySource> sources;
                    for (uint32_t right_pos : index.columns) {
                        ProbeKeySource source;
                        bool found = false;
                        if (right_equalities.values[right_pos]) {
                            source.from_literal = true;
                            source.literal = *right_equalities.values[right_pos];
                            found = true;
                        }
                        for (const BoundPredicate& predicate : right_predicates) {
                            if (found)
                                break;
                            if (predicate.lhs_pos == right_pos && predicate.op == ast::SV_OP_EQ &&
                                predicate.rhs_side < 0 && !predicate.literal.is_null) {
                                source.from_literal = true;
                                source.literal = predicate.literal;
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            for (const BoundPredicate& predicate : cross_predicates) {
                                if (predicate.op != ast::SV_OP_EQ)
                                    continue;
                                if (predicate.lhs_side == 1 && predicate.lhs_pos == right_pos &&
                                    predicate.rhs_side == 0) {
                                    source.left_pos = predicate.rhs_pos;
                                    found = true;
                                    break;
                                }
                                if (predicate.rhs_side == 1 && predicate.rhs_pos == right_pos &&
                                    predicate.lhs_side == 0) {
                                    source.left_pos = predicate.lhs_pos;
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if (!found) {
                            sources.clear();
                            break;
                        }
                        sources.push_back(std::move(source));
                    }
                    if (sources.size() == index.columns.size() && sidecars_.count(index.constraint_id)) {
                        probe_index = &index;
                        probe_sources = std::move(sources);
                        break;
                    }
                }
                const auto make_probe_key = [&](const std::vector<Cell>& left) -> std::optional<EncodedKey> {
                    if (!probe_index)
                        return std::nullopt;
                    std::vector<Cell> right(right_schema.columns.size());
                    for (size_t i = 0; i < probe_sources.size(); ++i) {
                        const Cell& value =
                            probe_sources[i].from_literal ? probe_sources[i].literal : left[probe_sources[i].left_pos];
                        if (value.is_null)
                            return std::nullopt;
                        right[probe_index->columns[i]] = value;
                    }
                    return EncodeKey(right_schema, *probe_index, right);
                };
                const bool parameterized = probe_index != nullptr;
                census.join_parameterized = parameterized;
                census.join_fallback = !parameterized;
                if (projection_query && sink)
                    sink->begin_query(projection_meta, projection_names);
                if (parameterized) {
                    auto& txn = Txn(session);
                    for (const auto& left_image : left_rows) {
                        const auto left = DecodeRow(left_schema, left_image);
                        const auto key = make_probe_key(left);
                        if (!key)
                            continue;
                        ++session.census.last_parameterized_join_probes;
                        bool usable = false;
                        VisitIndexInterval(
                            session, right_schema, *probe_index, *key, PrefixSuccessor(*key),
                            [&](const EncodedKey& candidate_key, epoch_si_poc::RowId id) {
                                ++session.census.last_join_inner_rows_resolved;
                                const auto image = db_.engine().Read(txn, id);
                                if (!image)
                                    return;
                                const auto right = DecodeRow(right_schema, *image);
                                if (EncodeKey(right_schema, *probe_index, right) != candidate_key ||
                                    !matches(right_predicates, {}, right))
                                    return;
                                ++session.census.last_join_pairs_rechecked;
                                if (matches(cross_predicates, left, right))
                                    emit_pair(left, right);
                            },
                            &usable);
                        session.census.last_join_base_entries_examined += session.census.last_base_entries_probed;
                        session.census.last_join_overlay_entries_examined += session.census.last_overlay_nodes_probed;
                        session.census.last_join_overlay_refs_examined += session.census.last_row_ids_probed;
                        if (!usable)
                            throw std::runtime_error("Delta parameterized index became unavailable");
                    }
                } else {
                    bool right_used_index = false;
                    VisitRows(
                        session, right_schema, select.conds,
                        [&](epoch_si_poc::RowId, const epoch_si_poc::RowImage& image) {
                            ++session.census.last_join_right_rows_visited;
                            if (!right_used_index)
                                ++session.census.last_join_full_scan_rows;
                            const auto right = DecodeRow(right_schema, image);
                            if (!matches(right_predicates, {}, right))
                                return;
                            for (const auto& left_image : left_rows) {
                                const auto left = DecodeRow(left_schema, left_image);
                                ++session.census.last_join_pairs_rechecked;
                                if (matches(cross_predicates, left, right))
                                    emit_pair(left, right);
                            }
                        },
                        &right_used_index);
                }
                if (!projection_query) {
                    Cell value;
                    if (aggregate_ptr->func == ast::AGG_COUNT) {
                        value.is_null = false;
                        value.integer = static_cast<int32_t>(count);
                    } else if (aggregate_seen) {
                        if (aggregate_ptr->func == ast::AGG_SUM) {
                            value.is_null = false;
                            if (aggregate_column->type == ColumnType::Int)
                                value.integer = static_cast<int32_t>(sum);
                            else
                                value.floating = static_cast<float>(sum);
                        } else
                            value = extreme;
                    }
                    EmitCells({aggregate_output}, {{value}}, sink);
                }
                if (implicit)
                    Commit(session, lock, &census);
                return true;
            }
            const TableSchema& schema = Table(select.tabs[0].table_name);
            census.ordered_materialize = select.has_sort;
            session.census.last_ordered_candidates_examined = 0;
            session.census.last_ordered_rows_decoded = 0;
            session.census.last_ordered_sort_input_rows = 0;
            session.census.last_ordered_early_stops = 0;
            if (!select.group_by_cols.empty()) {
                if (select.has_sort || select.has_limit)
                    throw std::runtime_error("unsupported Delta GROUP BY shape");
                std::vector<size_t> keys;
                for (const auto& column : select.group_by_cols) {
                    size_t pos = 0;
                    while (pos < schema.columns.size() && schema.columns[pos].name != column->col_name)
                        ++pos;
                    if (pos == schema.columns.size())
                        throw std::runtime_error("Delta GROUP BY column not found");
                    keys.push_back(pos);
                }
                struct State {
                    int64_t count = 0;
                    double sum = 0;
                    Cell value;
                    bool seen = false;
                };
                struct Group {
                    std::vector<Cell> values;
                    std::vector<State> states;
                };
                std::map<std::string, Group> groups;
                VisitRows(session, schema, select.conds, [&](epoch_si_poc::RowId, const epoch_si_poc::RowImage& image) {
                    const auto cells = DecodeRow(schema, image);
                    if (!Matches(schema, cells, select.conds))
                        return;
                    std::string key;
                    for (size_t pos : keys) {
                        key += std::to_string(static_cast<unsigned>(schema.columns[pos].type)) + ":";
                        if (cells[pos].is_null)
                            key += "N";
                        else if (schema.columns[pos].type == ColumnType::Int)
                            key += std::to_string(cells[pos].integer);
                        else if (schema.columns[pos].type == ColumnType::Float) {
                            uint32_t bits;
                            std::memcpy(&bits, &cells[pos].floating, sizeof(bits));
                            key += std::to_string(bits);
                        } else
                            key += cells[pos].text;
                        key += ":";
                    }
                    auto [it, inserted] = groups.emplace(key, Group{});
                    auto& group = it->second;
                    if (inserted) {
                        for (size_t pos : keys)
                            group.values.push_back(cells[pos]);
                        group.states.resize(select.select_items.size());
                    }
                    for (size_t item_index = 0; item_index < select.select_items.size(); ++item_index) {
                        const auto& item = select.select_items[item_index];
                        if (item->expr->type == ast::AstType::AggExpr) {
                            const auto& agg = static_cast<const ast::AggExpr&>(*item->expr);
                            State& state = group.states[item_index];
                            if (agg.func == ast::AGG_COUNT && agg.is_star) {
                                ++state.count;
                                continue;
                            }
                            if (agg.col) {
                                size_t pos = 0;
                                while (pos < schema.columns.size() && schema.columns[pos].name != agg.col->col_name)
                                    ++pos;
                                if (pos == schema.columns.size())
                                    throw std::runtime_error("Delta aggregate column not found");
                                const Cell& value = cells[pos];
                                if (value.is_null)
                                    continue;
                                if (agg.func == ast::AGG_COUNT)
                                    ++state.count;
                                else if (agg.func == ast::AGG_SUM) {
                                    if (schema.columns[pos].type == ColumnType::Int)
                                        state.sum += value.integer;
                                    else if (schema.columns[pos].type == ColumnType::Float)
                                        state.sum += value.floating;
                                    else
                                        throw std::runtime_error("Delta SUM requires numeric column");
                                    state.seen = true;
                                } else if (agg.func == ast::AGG_MIN || agg.func == ast::AGG_MAX) {
                                    const bool smaller = schema.columns[pos].type == ColumnType::Int
                                                             ? value.integer < state.value.integer
                                                         : schema.columns[pos].type == ColumnType::Float
                                                             ? value.floating < state.value.floating
                                                             : value.text < state.value.text;
                                    if (!state.seen || (agg.func == ast::AGG_MIN ? smaller : !smaller))
                                        state.value = value;
                                    state.seen = true;
                                } else
                                    throw std::runtime_error("unsupported Delta GROUP BY aggregate");
                            }
                        }
                    }
                });
                std::vector<Column> columns;
                std::vector<std::vector<Cell>> rows;
                for (const auto& item : select.select_items) {
                    if (const auto* col = dynamic_cast<const ast::Col*>(item->expr.get())) {
                        size_t pos = 0;
                        while (pos < schema.columns.size() && schema.columns[pos].name != col->col_name)
                            ++pos;
                        if (std::find(keys.begin(), keys.end(), pos) == keys.end())
                            throw std::runtime_error("Delta SELECT column must be grouped");
                        Column output = schema.columns[pos];
                        if (!item->alias.empty())
                            output.name = item->alias;
                        columns.push_back(std::move(output));
                    } else if (const auto* agg = dynamic_cast<const ast::AggExpr*>(item->expr.get())) {
                        if (agg->func == ast::AGG_COUNT)
                            columns.push_back(
                                {item->alias.empty() ? "?column?" : item->alias, ColumnType::Int, 4, false});
                        else if ((agg->func == ast::AGG_MAX || agg->func == ast::AGG_MIN ||
                                  agg->func == ast::AGG_SUM) &&
                                 agg->col) {
                            size_t pos = 0;
                            while (pos < schema.columns.size() && schema.columns[pos].name != agg->col->col_name)
                                ++pos;
                            Column output = schema.columns[pos];
                            if (!item->alias.empty())
                                output.name = item->alias;
                            columns.push_back(std::move(output));
                        } else
                            throw std::runtime_error("unsupported Delta GROUP BY aggregate");
                    } else
                        throw std::runtime_error("unsupported Delta GROUP BY projection");
                }
                for (const auto& [name, group] : groups) {
                    std::vector<Cell> row;
                    for (size_t i = 0; i < select.select_items.size(); ++i) {
                        const auto& item = select.select_items[i];
                        if (const auto* col = dynamic_cast<const ast::Col*>(item->expr.get())) {
                            size_t pos = 0;
                            while (pos < schema.columns.size() && schema.columns[pos].name != col->col_name)
                                ++pos;
                            row.push_back(group.values[static_cast<size_t>(std::find(keys.begin(), keys.end(), pos) -
                                                                           keys.begin())]);
                        } else {
                            const auto& agg = static_cast<const ast::AggExpr&>(*item->expr);
                            const State& state = group.states[i];
                            if (agg.func == ast::AGG_COUNT) {
                                Cell value;
                                value.is_null = false;
                                value.integer = static_cast<int32_t>(state.count);
                                row.push_back(value);
                            } else if (state.seen) {
                                Cell value = state.value;
                                if (agg.func == ast::AGG_SUM) {
                                    value.is_null = false;
                                    if (columns[i].type == ColumnType::Int)
                                        value.integer = static_cast<int32_t>(state.sum);
                                    else
                                        value.floating = static_cast<float>(state.sum);
                                }
                                row.push_back(std::move(value));
                            } else
                                row.push_back({});
                        }
                    }
                    rows.push_back(std::move(row));
                }
                EmitCells(columns, rows, sink);
                if (implicit)
                    Commit(session, lock, &census);
                return true;
            }
            const bool aggregate =
                std::any_of(select.select_items.begin(), select.select_items.end(),
                            [](const auto& item) { return item->expr->type == ast::AstType::AggExpr; });
            if (aggregate) {
                if (select.has_sort || select.has_limit || select.has_select_star ||
                    std::any_of(select.select_items.begin(), select.select_items.end(),
                                [](const auto& item) { return item->expr->type != ast::AstType::AggExpr; }))
                    throw std::runtime_error("unsupported Delta aggregate SELECT shape");
                struct State {
                    int64_t count = 0;
                    bool seen = false;
                    double sum = 0;
                    Cell min;
                    std::set<std::string> distinct;
                };
                std::vector<State> states(select.select_items.size());
                std::vector<size_t> positions(select.select_items.size());
                for (size_t i = 0; i < positions.size(); ++i) {
                    const auto& expr = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
                    if (expr.is_star) {
                        if (expr.func != ast::AGG_COUNT)
                            throw std::runtime_error("unsupported Delta aggregate");
                        continue;
                    }
                    if (!expr.col)
                        throw std::runtime_error("invalid Delta aggregate");
                    while (positions[i] < schema.columns.size() &&
                           schema.columns[positions[i]].name != expr.col->col_name)
                        ++positions[i];
                    if (positions[i] == schema.columns.size())
                        throw std::runtime_error("Delta aggregate column not found");
                }
                if (select.select_items.size() == 1) {
                    const auto& expr = static_cast<const ast::AggExpr&>(*select.select_items[0]->expr);
                    if (!expr.is_distinct && !expr.is_star &&
                        (expr.func == ast::AGG_MIN || expr.func == ast::AGG_MAX)) {
                        const auto access =
                            FindOrderedIndexAccess(schema, select.tabs[0].alias, select.conds, {positions[0]});
                        if (access) {
                            bool usable = false;
                            bool seen = false;
                            Cell value;
                            auto& txn = Txn(session);
                            VisitOrderedIndexInterval(
                                session, schema, *access->index, access->first, access->last, expr.func == ast::AGG_MAX,
                                [&](const EncodedKey& candidate_key, epoch_si_poc::RowId id) {
                                    ++session.census.last_ordered_candidates_examined;
                                    const auto image = db_.engine().Read(txn, id);
                                    if (!image)
                                        return true;
                                    auto cells = DecodeRow(schema, *image);
                                    ++session.census.last_ordered_rows_decoded;
                                    if (EncodeKey(schema, *access->index, cells) != candidate_key ||
                                        !Matches(schema, cells, select.conds) || cells[positions[0]].is_null)
                                        return true;
                                    value = std::move(cells[positions[0]]);
                                    seen = true;
                                    ++session.census.last_ordered_early_stops;
                                    return false;
                                },
                                &usable);
                            if (usable) {
                                census.ordered_stream = true;
                                census.ordered_materialize = false;
                                if (!seen)
                                    value = {};
                                EmitRows(schema, select, {{std::move(value)}}, sink, true);
                                if (implicit)
                                    Commit(session, lock, &census);
                                return true;
                            }
                        }
                    }
                }
                VisitRows(session, schema, select.conds, [&](epoch_si_poc::RowId, const epoch_si_poc::RowImage& image) {
                    const auto cells = DecodeRow(schema, image);
                    if (!Matches(schema, cells, select.conds))
                        return;
                    for (size_t i = 0; i < states.size(); ++i) {
                        const auto& expr = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
                        State& state = states[i];
                        if (expr.func == ast::AGG_COUNT && expr.is_star) {
                            ++state.count;
                            continue;
                        }
                        const Cell& value = cells[positions[i]];
                        if (value.is_null)
                            continue;
                        if (expr.func == ast::AGG_COUNT) {
                            if (expr.is_distinct) {
                                if (!state.distinct.insert(DistinctKey(schema.columns[positions[i]], value)).second)
                                    continue;
                            }
                            ++state.count;
                        } else if (expr.func == ast::AGG_SUM) {
                            if (schema.columns[positions[i]].type == ColumnType::Int)
                                state.sum += value.integer;
                            else if (schema.columns[positions[i]].type == ColumnType::Float)
                                state.sum += value.floating;
                            else
                                throw std::runtime_error("Delta SUM requires numeric column");
                            state.seen = true;
                        } else if (expr.func == ast::AGG_MIN || expr.func == ast::AGG_MAX) {
                            const bool smaller = schema.columns[positions[i]].type == ColumnType::Int
                                                     ? value.integer < state.min.integer
                                                 : schema.columns[positions[i]].type == ColumnType::Float
                                                     ? value.floating < state.min.floating
                                                     : value.text < state.min.text;
                            if (!state.seen || (expr.func == ast::AGG_MIN ? smaller : !smaller))
                                state.min = value;
                            state.seen = true;
                        } else
                            throw std::runtime_error("unsupported Delta aggregate");
                    }
                });
                std::vector<Cell> values;
                for (size_t i = 0; i < states.size(); ++i) {
                    const auto& expr = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
                    Cell value;
                    if (expr.func == ast::AGG_COUNT) {
                        value.is_null = false;
                        value.integer = static_cast<int32_t>(states[i].count);
                    } else if (states[i].seen) {
                        value = states[i].min;
                        if (expr.func == ast::AGG_SUM) {
                            value.is_null = false;
                            if (schema.columns[positions[i]].type == ColumnType::Int)
                                value.integer = static_cast<int32_t>(states[i].sum);
                            else
                                value.floating = static_cast<float>(states[i].sum);
                        }
                    }
                    values.push_back(std::move(value));
                }
                EmitRows(schema, select, {std::move(values)}, sink, true);
                if (implicit)
                    Commit(session, lock, &census);
                return true;
            }
            if (select.has_sort && select.has_limit && !select.limit_is_parameter && !select.offset_is_parameter &&
                select.limit >= 0 && select.limit <= 4096 && select.offset >= 0) {
                std::vector<size_t> order_columns;
                std::optional<bool> reverse;
                bool eligible = true;
                for (const auto& item : select.order_by_items) {
                    const auto* column = dynamic_cast<const ast::Col*>(item->expr.get());
                    const bool item_reverse = item->orderby_dir == ast::OrderBy_DESC;
                    if (!column ||
                        (!column->tab_name.empty() && column->tab_name != schema.name &&
                         column->tab_name != select.tabs[0].alias) ||
                        (reverse && *reverse != item_reverse)) {
                        eligible = false;
                        break;
                    }
                    reverse = item_reverse;
                    size_t position = 0;
                    while (position < schema.columns.size() && schema.columns[position].name != column->col_name)
                        ++position;
                    if (position == schema.columns.size())
                        throw std::runtime_error("Delta ORDER BY column not found");
                    order_columns.push_back(position);
                }
                const auto access =
                    eligible ? FindOrderedIndexAccess(schema, select.tabs[0].alias, select.conds, order_columns)
                             : std::nullopt;
                if (access) {
                    const size_t offset = static_cast<size_t>(select.offset);
                    const size_t limit = static_cast<size_t>(select.limit);
                    const size_t target = offset + limit;
                    std::vector<std::vector<Cell>> ordered_rows;
                    size_t valid_rows = 0;
                    bool usable = limit == 0;
                    auto& txn = Txn(session);
                    if (limit != 0) {
                        VisitOrderedIndexInterval(
                            session, schema, *access->index, access->first, access->last, *reverse,
                            [&](const EncodedKey& candidate_key, epoch_si_poc::RowId id) {
                                ++session.census.last_ordered_candidates_examined;
                                const auto image = db_.engine().Read(txn, id);
                                if (!image)
                                    return true;
                                auto cells = DecodeRow(schema, *image);
                                ++session.census.last_ordered_rows_decoded;
                                if (EncodeKey(schema, *access->index, cells) != candidate_key ||
                                    !Matches(schema, cells, select.conds))
                                    return true;
                                if (valid_rows++ >= offset)
                                    ordered_rows.push_back(std::move(cells));
                                if (valid_rows < target)
                                    return true;
                                ++session.census.last_ordered_early_stops;
                                return false;
                            },
                            &usable);
                    }
                    if (usable) {
                        census.ordered_stream = true;
                        census.ordered_materialize = false;
                        EmitRows(schema, select, ordered_rows, sink);
                        if (implicit)
                            Commit(session, lock, &census);
                        return true;
                    }
                }
            }
            std::vector<std::vector<Cell>> rows;
            bool query_started = false;
            VisitRows(session, schema, select.conds, [&](epoch_si_poc::RowId, const epoch_si_poc::RowImage& image) {
                auto cells = DecodeRow(schema, image);
                if (!Matches(schema, cells, select.conds))
                    return;
                if (!select.has_sort) {
                    EmitRows(schema, select, {std::move(cells)}, sink, false, query_started);
                    query_started = true;
                } else {
                    if (rows.size() == 4096)
                        throw std::runtime_error("Delta ORDER BY result exceeds 4096 rows");
                    ++session.census.last_ordered_sort_input_rows;
                    rows.push_back(std::move(cells));
                }
            });
            if (select.has_sort) {
                const auto position = [&](const ast::Col& column) {
                    if (!column.tab_name.empty() && column.tab_name != schema.name &&
                        column.tab_name != select.tabs[0].alias)
                        throw std::runtime_error("unknown Delta qualifier");
                    for (size_t i = 0; i < schema.columns.size(); ++i)
                        if (schema.columns[i].name == column.col_name)
                            return i;
                    throw std::runtime_error("Delta ORDER BY column not found");
                };
                std::sort(rows.begin(), rows.end(), [&](const auto& left, const auto& right) {
                    for (const auto& item : select.order_by_items) {
                        const auto* column = dynamic_cast<const ast::Col*>(item->expr.get());
                        if (!column)
                            throw std::runtime_error("Delta ORDER BY expression unsupported");
                        const size_t pos = position(*column);
                        const int comparison =
                            left[pos].is_null || right[pos].is_null ? (left[pos].is_null == right[pos].is_null ? 0
                                                                       : left[pos].is_null                     ? -1
                                                                                                               : 1)
                            : schema.columns[pos].type == ColumnType::Int
                                ? (left[pos].integer < right[pos].integer ? -1 : left[pos].integer > right[pos].integer)
                            : schema.columns[pos].type == ColumnType::Float
                                ? (left[pos].floating < right[pos].floating ? -1
                                                                            : left[pos].floating > right[pos].floating)
                                : left[pos].text.compare(right[pos].text);
                        if (comparison != 0)
                            return item->orderby_dir == ast::OrderBy_DESC ? comparison > 0 : comparison < 0;
                    }
                    return false;
                });
            }
            if (select.has_limit && select.has_sort) {
                const size_t begin = std::min(rows.size(), static_cast<size_t>(std::max(0, select.offset)));
                const size_t end = std::min(rows.size(), begin + static_cast<size_t>(std::max(0, select.limit)));
                rows = std::vector<std::vector<Cell>>(rows.begin() + begin, rows.begin() + end);
            }
            if (select.has_sort)
                EmitRows(schema, select, rows, sink);
            else if (!query_started)
                EmitRows(schema, select, rows, sink);
            if (implicit)
                Commit(session, lock, &census);
            return true;
        } else if (tree->type == ast::AstType::DeleteStmt || tree->type == ast::AstType::UpdateStmt) {
            const bool deleting = tree->type == ast::AstType::DeleteStmt;
            const std::string& name = deleting ? static_cast<const ast::DeleteStmt&>(*tree).tab_name
                                               : static_cast<const ast::UpdateStmt&>(*tree).tab_name;
            const auto& conditions = deleting ? static_cast<const ast::DeleteStmt&>(*tree).conds
                                              : static_cast<const ast::UpdateStmt&>(*tree).conds;
            const TableSchema& schema = Table(name);
            auto& txn = Txn(session);
            std::vector<std::pair<epoch_si_poc::RowId, epoch_si_poc::RowImage>> targets;
            VisitRows(session, schema, conditions, [&](epoch_si_poc::RowId id, const epoch_si_poc::RowImage& image) {
                targets.emplace_back(id, image);
            });
            for (const auto& [row_id, image] : targets) {
                auto cells = DecodeRow(schema, image);
                if (!Matches(schema, cells, conditions))
                    continue;
                if (deleting) {
                    const bool private_insert = session.private_insert_ids.erase(row_id) != 0;
                    RemoveOverlay(session.overlay, schema, cells, row_id);
                    if (!private_insert) {
                        for (const auto& index : schema.indexes)
                            AppendOverlay(session.removed_overlay,
                                          {schema.id, index.constraint_id, EncodeKey(schema, index, cells)}, row_id.local_id);
                    }
                    db_.engine().Erase(txn, row_id);
                    continue;
                }
                const auto& update = static_cast<const ast::UpdateStmt&>(*tree);
                auto next = cells;
                for (const auto& clause : update.set_clauses) {
                    auto target = std::find_if(schema.columns.begin(), schema.columns.end(),
                                               [&](const Column& c) { return c.name == clause->col_name; });
                    if (target == schema.columns.end())
                        throw std::runtime_error("Delta UPDATE column not found");
                    const size_t target_pos = static_cast<size_t>(target - schema.columns.begin());
                    if (!clause->is_self_ref) {
                        next[target_pos] = Literal(*target, *clause->val);
                        continue;
                    }
                    auto source = std::find_if(schema.columns.begin(), schema.columns.end(),
                                               [&](const Column& c) { return c.name == clause->rhs_col->col_name; });
                    if (source == schema.columns.end() || source->type != target->type)
                        throw std::runtime_error("Delta UPDATE source mismatch");
                    Cell value = cells[static_cast<size_t>(source - schema.columns.begin())];
                    const auto apply = [&](ast::SetOp op, const ast::Value& operand) {
                        if (value.is_null)
                            return;
                        Cell rhs = Literal(*target, operand);
                        if (rhs.is_null || (target->type != ColumnType::Int && target->type != ColumnType::Float))
                            throw std::runtime_error("invalid Delta arithmetic");
                        if (target->type == ColumnType::Int) {
                            int64_t result = value.integer;
                            if (op == ast::SetOp::SELF_ADD)
                                result += rhs.integer;
                            else if (op == ast::SetOp::SELF_SUB)
                                result -= rhs.integer;
                            else if (op == ast::SetOp::SELF_MUL)
                                result *= rhs.integer;
                            else if (op == ast::SetOp::SELF_DIV) {
                                if (rhs.integer == 0)
                                    throw std::runtime_error("division by zero");
                                result /= rhs.integer;
                            }
                            if (result < INT32_MIN || result > INT32_MAX)
                                throw std::runtime_error("Delta INT arithmetic overflow");
                            value.integer = static_cast<int32_t>(result);
                        } else {
                            if (op == ast::SetOp::SELF_ADD)
                                value.floating = static_cast<float>(value.floating + rhs.floating);
                            else if (op == ast::SetOp::SELF_SUB)
                                value.floating = static_cast<float>(value.floating - rhs.floating);
                            else if (op == ast::SetOp::SELF_MUL)
                                value.floating = static_cast<float>(value.floating * rhs.floating);
                            else if (op == ast::SetOp::SELF_DIV) {
                                if (rhs.floating == 0)
                                    throw std::runtime_error("division by zero");
                                value.floating = static_cast<float>(value.floating / rhs.floating);
                            }
                        }
                    };
                    if (clause->op == ast::SetOp::ASSIGNMENT)
                        value = cells[static_cast<size_t>(source - schema.columns.begin())];
                    else
                        apply(clause->op, *clause->val);
                    for (const auto& term : clause->additional_terms)
                        apply(term.op, *term.val);
                    next[target_pos] = std::move(value);
                }
                const bool private_insert = session.private_insert_ids.count(row_id) != 0;
                for (const auto& index : schema.indexes) {
                    const auto old_key = EncodeKey(schema, index, cells);
                    const auto new_key = EncodeKey(schema, index, next);
                    if (old_key == new_key)
                        continue;
                    RemoveOverlay(session.overlay, schema, index, cells, row_id);
                    if (!private_insert)
                        AppendOverlay(session.removed_overlay,
                                      {schema.id, index.constraint_id, old_key}, row_id.local_id);
                    AppendOverlay(session.overlay, {schema.id, index.constraint_id, new_key}, row_id.local_id);
                }
                db_.engine().PutImage(txn, row_id, EncodeRow(schema, next));
            }
        } else
            throw std::runtime_error("unsupported SQL for DeltaKernel");
        if (implicit)
            Commit(session, lock, &census);
        return false;
    } catch (...) {
        if (implicit && session.txn)
            AbortLocked(session);
        throw;
    }
}

} // namespace deltakernel
