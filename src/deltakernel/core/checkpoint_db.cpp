#include "checkpoint_db.h"

#include "file_wal.h"

#include <algorithm>
#include <chrono>
#include <array>
#include <charconv>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <set>
#include <string_view>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <vector>
#include <linux/fs.h>
#include <dirent.h>

namespace epoch_si_poc {
namespace {

constexpr uint64_t kManifestMagic = 0x54534546494e414dULL; // MANIFEST
constexpr uint64_t kTableMagic = 0x45534142454c4254ULL;    // TBLBASE
constexpr uint32_t kTableFooterMagic = 0x454e4454;
constexpr uint32_t kTableHeaderBytes = 80;
constexpr uint32_t kTableFooterBytes = 24;
constexpr uint32_t kManifestHeaderBytes = 44;
constexpr uint32_t kManifestRefBytes = 40;
constexpr uint32_t kLegacyManifestFormat = 0;
constexpr uint32_t kSegmentedManifestFormat = 4;
constexpr uint32_t kLegacyManifestFixedBytes = 48;
constexpr uint32_t kSegmentedManifestFixedBytes = 112;
constexpr uint32_t kSegmentedManifestTableBytes = 56;
constexpr uint64_t kMaxManifestBytes = 16ULL << 20;
constexpr uint64_t kMaxTableBytes = 64ULL << 30;
constexpr uint64_t kRowsPerBlock = 32;
constexpr size_t kTableWriteBufferBytes = 1U << 20;
constexpr size_t kPointReadScratchBytes = 1U << 20;

struct ManifestState {
    uint32_t manifest_format = kLegacyManifestFormat;
    uint64_t generation = 0;
    uint64_t wal_generation = 0;
    Epoch wal_base_epoch = 0;
    uint64_t base_next_commit_seq = 1;
    bool has_legacy_prefix = true;
    uint64_t legacy_prefix_generation = 0;
    uint64_t wal_lineage_generation = 0;
    uint64_t first_segment_id = 0;
    uint64_t active_segment_id = 0;
    uint64_t segment_count = 0;
    std::map<TableId, CheckpointDb::TableRef> tables;
};

void ThrowSystemError(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

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
    for (size_t i = 0; i < size; ++i)
        crc = (crc >> 8U) ^ kCrc32Table[(crc ^ data[i]) & 0xffU];
    return crc;
}

uint32_t Crc32(const uint8_t* data, size_t size) {
    return ~UpdateCrc32(0xffffffffU, data, size);
}

template <typename T> void PutLe(std::vector<uint8_t>& out, T value) {
    for (size_t i = 0; i < sizeof(T); ++i)
        out.push_back(static_cast<uint8_t>(value >> (8 * i)));
}

template <typename T> T GetLe(const std::vector<uint8_t>& in, size_t& pos, size_t end) {
    if (pos > end || end > in.size() || end - pos < sizeof(T))
        throw std::runtime_error("truncated checkpoint metadata");
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
        value |= static_cast<T>(in[pos++]) << (8 * i);
    return value;
}

void EncodeRow(std::vector<uint8_t>& out, const RowImage& row) {
    if (row.deleted || row.bytes.size() > std::numeric_limits<uint32_t>::max() ||
        row.claims.size() > std::numeric_limits<uint16_t>::max())
        throw std::invalid_argument("invalid immutable row image");
    PutLe<uint32_t>(out, static_cast<uint32_t>(row.bytes.size()));
    PutLe<uint16_t>(out, static_cast<uint16_t>(row.claims.size()));
    out.insert(out.end(), row.bytes.begin(), row.bytes.end());
    for (size_t i = 0; i < row.claims.size(); ++i) {
        const auto& claim = row.claims[i];
        if (claim.bytes.size() > std::numeric_limits<uint32_t>::max() || (i && !(row.claims[i - 1] < claim)))
            throw std::invalid_argument("invalid immutable row claims");
        PutLe<uint32_t>(out, claim.constraint_id);
        PutLe<uint32_t>(out, static_cast<uint32_t>(claim.bytes.size()));
        out.insert(out.end(), claim.bytes.begin(), claim.bytes.end());
    }
}

RowImage DecodeRow(const uint8_t* bytes, size_t size) {
    size_t pos = 0;
    const auto get = [&](auto* value) {
        using T = std::remove_pointer_t<decltype(value)>;
        if (pos > size || size - pos < sizeof(T))
            throw std::runtime_error("truncated immutable row image");
        *value = 0;
        for (size_t i = 0; i < sizeof(T); ++i)
            *value |= static_cast<T>(bytes[pos++]) << (8 * i);
    };
    uint32_t image_bytes = 0;
    uint16_t claim_count = 0;
    get(&image_bytes);
    get(&claim_count);
    if (image_bytes > size - pos || claim_count > (size - pos - image_bytes) / 8)
        throw std::runtime_error("invalid immutable row image");
    RowImage row;
    row.bytes.assign(bytes + pos, bytes + pos + image_bytes);
    pos += image_bytes;
    for (uint16_t i = 0; i < claim_count; ++i) {
        ConstraintClaim claim;
        uint32_t claim_bytes = 0;
        get(&claim.constraint_id);
        get(&claim_bytes);
        if (claim_bytes > size - pos)
            throw std::runtime_error("invalid immutable row claim");
        claim.bytes.assign(bytes + pos, bytes + pos + claim_bytes);
        pos += claim_bytes;
        if (!row.claims.empty() && !(row.claims.back() < claim))
            throw std::runtime_error("invalid immutable row claim ordering");
        row.claims.push_back(std::move(claim));
    }
    if (pos != size)
        throw std::runtime_error("immutable row trailing bytes");
    return row;
}

RowImage DecodeRow(const std::vector<uint8_t>& bytes) {
    return DecodeRow(bytes.data(), bytes.size());
}

std::string Join(const std::string& directory, const std::string& name) {
    return directory + "/" + name;
}
std::string WalName(uint64_t generation) {
    return "db.log." + std::to_string(generation);
}
std::string LegacyWalName(uint64_t generation) {
    return "wal." + std::to_string(generation);
}
std::string SegmentName(uint64_t lineage, uint64_t segment_id) {
    return "db.log.s." + std::to_string(lineage) + "." + std::to_string(segment_id);
}

bool ParseCanonicalUnsigned(std::string_view text, uint64_t* value) {
    if (text.empty())
        return false;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), *value);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size() && std::to_string(*value) == text;
}

bool ParseSegmentName(std::string_view name, uint64_t* lineage, uint64_t* segment_id) {
    constexpr std::string_view prefix = "db.log.s.";
    if (name.substr(0, prefix.size()) != prefix)
        return false;
    name.remove_prefix(prefix.size());
    const size_t separator = name.find('.');
    return separator != std::string_view::npos && name.find('.', separator + 1) == std::string_view::npos &&
           ParseCanonicalUnsigned(name.substr(0, separator), lineage) &&
           ParseCanonicalUnsigned(name.substr(separator + 1), segment_id);
}

bool ParseWalName(std::string_view name, std::string_view prefix) {
    if (name.substr(0, prefix.size()) != prefix)
        return false;
    uint64_t generation = 0;
    return ParseCanonicalUnsigned(name.substr(prefix.size()), &generation);
}
std::string TableName(TableId table_id, uint64_t generation) {
    return "tablebase." + std::to_string(table_id) + "." + std::to_string(generation);
}

bool ParseTableName(std::string_view name, TableId* table_id, uint64_t* generation) {
    constexpr std::string_view prefix = "tablebase.";
    if (name.substr(0, prefix.size()) != prefix)
        return false;
    name.remove_prefix(prefix.size());
    const size_t separator = name.find('.');
    uint64_t parsed_table = 0;
    if (separator == std::string_view::npos || name.find('.', separator + 1) != std::string_view::npos ||
        !ParseCanonicalUnsigned(name.substr(0, separator), &parsed_table) ||
        parsed_table > std::numeric_limits<TableId>::max() ||
        !ParseCanonicalUnsigned(name.substr(separator + 1), generation))
        return false;
    *table_id = static_cast<TableId>(parsed_table);
    return true;
}

void WriteLoop(int fd, const uint8_t* bytes, size_t size) {
    size_t done = 0;
    while (done < size) {
        const ssize_t written = write(fd, bytes + done, size - done);
        if (written < 0 && errno == EINTR)
            continue;
        if (written < 0)
            ThrowSystemError("write checkpoint file");
        if (written == 0)
            throw std::runtime_error("zero-length checkpoint write");
        done += static_cast<size_t>(written);
    }
}

void PreadLoop(int fd, uint8_t* bytes, size_t size, uint64_t offset) {
    size_t done = 0;
    while (done < size) {
        const ssize_t got = pread(fd, bytes + done, size - done, static_cast<off_t>(offset + done));
        if (got < 0 && errno == EINTR)
            continue;
        if (got < 0)
            ThrowSystemError("read checkpoint file");
        if (got == 0)
            throw std::runtime_error("checkpoint file shrank during read");
        done += static_cast<size_t>(got);
    }
}

void PwriteLoop(int fd, const uint8_t* bytes, size_t size, uint64_t offset) {
    size_t done = 0;
    while (done < size) {
        const ssize_t written = pwrite(fd, bytes + done, size - done, static_cast<off_t>(offset + done));
        if (written < 0 && errno == EINTR)
            continue;
        if (written < 0)
            ThrowSystemError("rewrite checkpoint header");
        if (written == 0)
            throw std::runtime_error("zero-length checkpoint rewrite");
        done += static_cast<size_t>(written);
    }
}

void SyncFd(int fd) {
    while (fdatasync(fd) != 0)
        if (errno != EINTR)
            ThrowSystemError("sync checkpoint file");
}

void SyncDirectory(const std::string& directory) {
    const int fd = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        ThrowSystemError("open database directory");
    while (fsync(fd) != 0) {
        if (errno != EINTR) {
            close(fd);
            ThrowSystemError("sync database directory");
        }
    }
    if (close(fd) != 0)
        ThrowSystemError("close database directory");
}

class DirectoryFd {
public:
    explicit DirectoryFd(const std::string& directory)
        : fd_(open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC)) {
        if (fd_ < 0)
            ThrowSystemError("open database directory");
    }
    ~DirectoryFd() {
        if (fd_ >= 0)
            close(fd_);
    }
    int get() const {
        return fd_;
    }
    void Sync() const {
        while (fsync(fd_) != 0)
            if (errno != EINTR)
                ThrowSystemError("sync database directory");
    }

private:
    int fd_;
};

bool WalEntryExists(int directory_fd, const std::string& name) {
    struct stat state {};
    if (fstatat(directory_fd, name.c_str(), &state, AT_SYMLINK_NOFOLLOW) == 0) {
        if (!S_ISREG(state.st_mode))
            throw std::runtime_error("WAL authority must be a regular file");
        return true;
    }
    if (errno == ENOENT)
        return false;
    ThrowSystemError("stat WAL entry");
    return false;
}

void RemoveExpectedFutureWal(const DirectoryFd& directory, const std::string& name) {
    const int fd = openat(directory.get(), name.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        if (errno == ENOENT)
            return;
        ThrowSystemError("open future WAL entry");
    }
    struct stat opened {};
    struct stat current {};
    if (fstat(fd, &opened) != 0 || !S_ISREG(opened.st_mode) ||
        fstatat(directory.get(), name.c_str(), &current, AT_SYMLINK_NOFOLLOW) != 0 || !S_ISREG(current.st_mode) ||
        opened.st_dev != current.st_dev || opened.st_ino != current.st_ino) {
        close(fd);
        throw std::runtime_error("future WAL residue must be the opened regular file");
    }
    if (close(fd) != 0)
        ThrowSystemError("close future WAL entry");
    if (unlinkat(directory.get(), name.c_str(), 0) != 0)
        ThrowSystemError("remove future WAL residue");
    directory.Sync();
}

std::string ResolveAndMigrateWal(const DirectoryFd& directory, uint64_t generation) {
    const std::string current = WalName(generation);
    const std::string legacy = LegacyWalName(generation);
    const bool current_exists = WalEntryExists(directory.get(), current);
    const bool legacy_exists = WalEntryExists(directory.get(), legacy);
    if (current_exists && legacy_exists)
        throw std::runtime_error("ambiguous WAL authority");
    if (!current_exists && !legacy_exists)
        return current;
    if (!current_exists) {
        if (renameat(directory.get(), legacy.c_str(), directory.get(), current.c_str()) != 0)
            ThrowSystemError("migrate legacy WAL");
    }
    // This also completes a prior rename whose directory sync was interrupted.
    directory.Sync();
    return current;
}

void RenameFile(const std::string& from, const std::string& to) {
    if (rename(from.c_str(), to.c_str()) != 0)
        ThrowSystemError("rename checkpoint file");
}

void WriteFile(const std::string& path, const std::vector<uint8_t>& bytes, bool partial) {
    const int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0)
        ThrowSystemError("open checkpoint file");
    try {
        WriteLoop(fd, bytes.data(), partial ? std::max<size_t>(1, bytes.size() / 2) : bytes.size());
        if (partial)
            throw SimulatedCrash();
        SyncFd(fd);
    } catch (...) {
        close(fd);
        throw;
    }
    if (close(fd) != 0)
        ThrowSystemError("close checkpoint file");
}

std::vector<uint8_t> ReadFile(const std::string& path, uint64_t max_bytes) {
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        ThrowSystemError("open checkpoint file for read");
    struct stat st {};
    if (fstat(fd, &st) != 0 || st.st_size < 0 || static_cast<uint64_t>(st.st_size) > max_bytes) {
        close(fd);
        throw std::runtime_error("checkpoint file has invalid size");
    }
    std::vector<uint8_t> bytes(static_cast<size_t>(st.st_size));
    try {
        PreadLoop(fd, bytes.data(), bytes.size(), 0);
    } catch (...) {
        close(fd);
        throw;
    }
    if (close(fd) != 0)
        ThrowSystemError("close checkpoint file");
    return bytes;
}

std::vector<uint8_t> EncodeManifest(const ManifestState& state) {
    const uint64_t table_count = state.tables.size();
    if (state.manifest_format != kSegmentedManifestFormat || table_count > std::numeric_limits<uint32_t>::max() ||
        table_count > (kMaxManifestBytes - kSegmentedManifestFixedBytes) / kSegmentedManifestTableBytes ||
        state.segment_count == 0 || state.segment_count > 4096 || state.active_segment_id < state.first_segment_id ||
        state.active_segment_id - state.first_segment_id + 1 != state.segment_count ||
        state.base_next_commit_seq == 0 ||
        (state.has_legacy_prefix && state.legacy_prefix_generation == state.wal_lineage_generation))
        throw std::overflow_error("manifest table count exceeds limit");
    std::vector<uint8_t> out;
    const uint64_t total_bytes = kSegmentedManifestFixedBytes + table_count * kSegmentedManifestTableBytes;
    if (total_bytes > std::numeric_limits<uint32_t>::max() || total_bytes > out.max_size())
        throw std::overflow_error("manifest size exceeds limit");
    out.reserve(static_cast<size_t>(total_bytes));
    PutLe<uint64_t>(out, kManifestMagic);
    PutLe<uint32_t>(out, 0);
    PutLe<uint64_t>(out, state.generation);
    PutLe<uint64_t>(out, state.wal_lineage_generation);
    PutLe<uint64_t>(out, state.wal_base_epoch);
    PutLe<uint32_t>(out, static_cast<uint32_t>(state.tables.size()));
    PutLe<uint32_t>(out, kSegmentedManifestFormat);
    for (const auto& [id, ref] : state.tables) {
        PutLe<uint32_t>(out, id);
        PutLe<uint32_t>(out, 0);
        PutLe<uint64_t>(out, ref.file_generation);
        PutLe<uint64_t>(out, ref.visible_from);
        PutLe<uint64_t>(out, ref.row_count);
        PutLe<uint64_t>(out, ref.file_bytes);
    }
    PutLe<uint64_t>(out, state.base_next_commit_seq);
    PutLe<uint32_t>(out, state.has_legacy_prefix ? 1U : 0U);
    PutLe<uint32_t>(out, 0);
    PutLe<uint64_t>(out, state.legacy_prefix_generation);
    PutLe<uint64_t>(out, state.wal_lineage_generation);
    PutLe<uint64_t>(out, state.first_segment_id);
    PutLe<uint64_t>(out, state.active_segment_id);
    PutLe<uint64_t>(out, state.segment_count);
    PutLe<uint64_t>(out, 0);
    for (const auto& [id, ref] : state.tables) {
        PutLe<uint32_t>(out, id);
        PutLe<uint32_t>(out, 0);
        PutLe<uint64_t>(out, ref.next_local_id);
    }
    if (out.size() + 4 != total_bytes)
        throw std::logic_error("manifest size mismatch");
    const uint32_t total = static_cast<uint32_t>(total_bytes);
    for (size_t i = 0; i < 4; ++i)
        out[8 + i] = static_cast<uint8_t>(total >> (8 * i));
    PutLe<uint32_t>(out, Crc32(out.data(), out.size()));
    return out;
}

ManifestState DecodeManifest(const std::vector<uint8_t>& bytes) {
    if (bytes.size() < kManifestHeaderBytes + 4 || bytes.size() > kMaxManifestBytes ||
        Crc32(bytes.data(), bytes.size() - 4) != [&] {
            size_t p = bytes.size() - 4;
            return GetLe<uint32_t>(bytes, p, bytes.size());
        }())
        throw std::runtime_error("invalid manifest size or CRC");
    size_t pos = 0;
    if (GetLe<uint64_t>(bytes, pos, bytes.size()) != kManifestMagic ||
        GetLe<uint32_t>(bytes, pos, bytes.size()) != bytes.size())
        throw std::runtime_error("invalid manifest header");
    ManifestState state;
    state.generation = GetLe<uint64_t>(bytes, pos, bytes.size());
    const uint64_t wal_generation = GetLe<uint64_t>(bytes, pos, bytes.size());
    state.wal_base_epoch = GetLe<uint64_t>(bytes, pos, bytes.size());
    const uint32_t count = GetLe<uint32_t>(bytes, pos, bytes.size());
    const uint32_t format = GetLe<uint32_t>(bytes, pos, bytes.size());
    if (format != kLegacyManifestFormat && format != kSegmentedManifestFormat)
        throw std::runtime_error("invalid manifest format");
    const uint64_t expected_bytes =
        format == kLegacyManifestFormat
            ? kLegacyManifestFixedBytes + static_cast<uint64_t>(count) * kManifestRefBytes
            : kSegmentedManifestFixedBytes + static_cast<uint64_t>(count) * kSegmentedManifestTableBytes;
    if (expected_bytes > kMaxManifestBytes || expected_bytes > std::numeric_limits<uint32_t>::max() ||
        expected_bytes > bytes.max_size() || bytes.size() != expected_bytes)
        throw std::runtime_error("invalid manifest table count");
    state.manifest_format = format;
    state.wal_generation = wal_generation;
    state.wal_lineage_generation = wal_generation;
    for (uint32_t i = 0; i < count; ++i) {
        CheckpointDb::TableRef ref;
        ref.table_id = GetLe<uint32_t>(bytes, pos, bytes.size());
        if (GetLe<uint32_t>(bytes, pos, bytes.size()) != 0)
            throw std::runtime_error("invalid manifest reserved field");
        ref.file_generation = GetLe<uint64_t>(bytes, pos, bytes.size());
        ref.visible_from = GetLe<uint64_t>(bytes, pos, bytes.size());
        ref.row_count = GetLe<uint64_t>(bytes, pos, bytes.size());
        ref.file_bytes = GetLe<uint64_t>(bytes, pos, bytes.size());
        if (ref.table_id == 0 || ref.file_bytes < kTableHeaderBytes + kTableFooterBytes ||
            !state.tables.emplace(ref.table_id, ref).second)
            throw std::runtime_error("invalid manifest table reference");
    }
    if (format == kLegacyManifestFormat) {
        state.has_legacy_prefix = true;
        state.legacy_prefix_generation = wal_generation;
        state.base_next_commit_seq = 1;
        state.first_segment_id = state.active_segment_id = state.segment_count = 0;
        return state;
    }
    state.base_next_commit_seq = GetLe<uint64_t>(bytes, pos, bytes.size());
    const uint32_t legacy = GetLe<uint32_t>(bytes, pos, bytes.size());
    if (GetLe<uint32_t>(bytes, pos, bytes.size()) != 0 || legacy > 1)
        throw std::runtime_error("invalid manifest WAL prefix");
    state.has_legacy_prefix = legacy != 0;
    state.legacy_prefix_generation = GetLe<uint64_t>(bytes, pos, bytes.size());
    state.wal_lineage_generation = GetLe<uint64_t>(bytes, pos, bytes.size());
    if (state.wal_lineage_generation != wal_generation)
        throw std::runtime_error("manifest WAL lineage authorities disagree");
    state.first_segment_id = GetLe<uint64_t>(bytes, pos, bytes.size());
    state.active_segment_id = GetLe<uint64_t>(bytes, pos, bytes.size());
    state.segment_count = GetLe<uint64_t>(bytes, pos, bytes.size());
    if (GetLe<uint64_t>(bytes, pos, bytes.size()) != 0)
        throw std::runtime_error("invalid manifest WAL reserved field");
    if (state.base_next_commit_seq == 0 || state.segment_count == 0 || state.segment_count > 4096 ||
        state.active_segment_id < state.first_segment_id ||
        state.active_segment_id - state.first_segment_id + 1 != state.segment_count ||
        (state.wal_lineage_generation == state.legacy_prefix_generation && state.has_legacy_prefix))
        throw std::runtime_error("invalid manifest WAL chain");
    std::set<TableId> frontier_ids;
    for (uint32_t i = 0; i < count; ++i) {
        const TableId id = GetLe<uint32_t>(bytes, pos, bytes.size());
        if (GetLe<uint32_t>(bytes, pos, bytes.size()) != 0)
            throw std::runtime_error("invalid manifest frontier reserved field");
        const uint64_t frontier = GetLe<uint64_t>(bytes, pos, bytes.size());
        auto found = state.tables.find(id);
        if (found == state.tables.end() || !frontier_ids.insert(id).second)
            throw std::runtime_error("invalid manifest frontier table");
        found->second.next_local_id = frontier;
    }
    if (pos + 4 != bytes.size())
        throw std::runtime_error("invalid manifest extension size");
    return state;
}

void PublishManifest(const std::string& directory, const ManifestState& state, CheckpointCrashPoint crash) {
    const std::string temp = Join(directory, "MANIFEST.tmp");
    WriteFile(temp, EncodeManifest(state),
              crash == CheckpointCrashPoint::kDuringManifestTemp ||
                  crash == CheckpointCrashPoint::kRotationDuringManifestTemp);
    RenameFile(temp, Join(directory, "MANIFEST"));
    if (crash == CheckpointCrashPoint::kAfterManifestRenameBeforeDirSync ||
        crash == CheckpointCrashPoint::kRotationAfterManifestRenameBeforeDirSync)
        throw SimulatedCrash();
    SyncDirectory(directory);
}

void PublishManifestBytes(const std::string& directory, const std::vector<uint8_t>& bytes, CheckpointCrashPoint crash,
                          bool& renamed) {
    renamed = false;
    const std::string temp = Join(directory, "MANIFEST.tmp");
    WriteFile(temp, bytes, crash == CheckpointCrashPoint::kDuringManifestTemp);
    RenameFile(temp, Join(directory, "MANIFEST"));
    renamed = true;
    if (crash == CheckpointCrashPoint::kAfterManifestRenameBeforeDirSync)
        throw SimulatedCrash();
    SyncDirectory(directory);
}

std::shared_ptr<const ImmutableTable> OpenTable(const std::string& directory, const CheckpointDb::TableRef& ref);

} // namespace

struct CheckpointDb::WalChain {
    bool has_legacy_prefix = false;
    bool has_segment_chain = false;
    uint64_t legacy_prefix_generation = 0;
    uint64_t lineage_generation = 0;
    uint64_t first_segment_id = 0;
    uint64_t active_segment_id = 0;
    Epoch base_epoch = 0;
    uint64_t base_next_commit_seq = 1;
};

struct TableBaseWriter::Impl {
    std::string directory;
    std::string temp_name;
    std::string final_name;
    std::string temp;
    std::string final;
    int directory_fd = -1;
    int fd = -1;
    dev_t device = 0;
    ino_t inode = 0;
    TableId table_id = 0;
    uint64_t generation = 0;
    Epoch visible_from = 0;
    uint64_t rows = 0;
    uint64_t next_local_id = 0;
    uint64_t payload_bytes = 0;
    uint32_t payload_crc = 0xffffffffU;
    std::vector<uint8_t> output;
    std::vector<uint64_t> offsets;
    std::vector<uint64_t> first_local_ids;
    bool renamed = false;
    bool keep_final = false;

    void RemoveOwned(const std::string& name) noexcept {
        if (directory_fd < 0)
            return;
        struct stat state {};
        if (fstatat(directory_fd, name.c_str(), &state, AT_SYMLINK_NOFOLLOW) == 0 && S_ISREG(state.st_mode) &&
            state.st_dev == device && state.st_ino == inode)
            unlinkat(directory_fd, name.c_str(), 0);
    }

    ~Impl() {
        if (fd >= 0)
            close(fd);
        if (!renamed)
            RemoveOwned(temp_name);
        else if (!keep_final)
            RemoveOwned(final_name);
        if (directory_fd >= 0)
            close(directory_fd);
    }

    Impl(std::string directory_, TableId table_id_, uint64_t generation_, Epoch visible_from_)
        : directory(std::move(directory_)), temp_name(TableName(table_id_, generation_) + ".tmp"),
          final_name(TableName(table_id_, generation_)), temp(Join(directory, temp_name)),
          final(Join(directory, final_name)), table_id(table_id_), generation(generation_),
          visible_from(visible_from_) {
        directory_fd = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (directory_fd < 0)
            ThrowSystemError("open immutable table directory");
        try {
            fd = openat(directory_fd, temp_name.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
            if (fd < 0)
                ThrowSystemError("create immutable table temp");
            struct stat state {};
            if (fstat(fd, &state) != 0 || !S_ISREG(state.st_mode))
                throw std::runtime_error("immutable table temp is not a regular file");
            device = state.st_dev;
            inode = state.st_ino;
            std::vector<uint8_t> header(kTableHeaderBytes, 0);
            WriteLoop(fd, header.data(), header.size());
            output.reserve(kTableWriteBufferBytes);
        } catch (...) {
            if (fd >= 0) {
                close(fd);
                fd = -1;
            }
            RemoveOwned(temp_name);
            close(directory_fd);
            directory_fd = -1;
            throw;
        }
    }

    void FlushOutput() {
        WriteLoop(fd, output.data(), output.size());
        output.clear();
    }

    void AppendAt(uint64_t local_id, RowImage row) {
        if (local_id == std::numeric_limits<uint64_t>::max() || (rows && local_id < next_local_id))
            throw std::invalid_argument("immutable table RowIds must increase");
        std::vector<uint8_t> encoded;
        EncodeRow(encoded, row);
        if (encoded.size() > std::numeric_limits<uint32_t>::max() ||
            payload_bytes > kMaxTableBytes - kTableHeaderBytes - kTableFooterBytes - encoded.size() - 4)
            throw std::overflow_error("immutable table exceeds limit");
        if (rows % kRowsPerBlock == 0) {
            offsets.push_back(payload_bytes);
            first_local_ids.push_back(local_id);
        }
        std::vector<uint8_t> record;
        PutLe<uint64_t>(record, local_id);
        PutLe<uint32_t>(record, static_cast<uint32_t>(encoded.size()));
        record.insert(record.end(), encoded.begin(), encoded.end());
        if (output.size() + record.size() > kTableWriteBufferBytes)
            FlushOutput();
        if (record.size() >= kTableWriteBufferBytes)
            WriteLoop(fd, record.data(), record.size());
        else
            output.insert(output.end(), record.begin(), record.end());
        payload_crc = UpdateCrc32(payload_crc, record.data(), record.size());
        payload_bytes += record.size();
        ++rows;
        next_local_id = local_id + 1;
    }

    void Append(RowImage row) {
        AppendAt(next_local_id, std::move(row));
    }

    CheckpointDb::TableRef Finish(CheckpointCrashPoint crash) {
        if (crash == CheckpointCrashPoint::kDuringBaseTemp)
            throw SimulatedCrash();
        FlushOutput();
        std::vector<uint8_t> index;
        for (size_t i = 0; i < offsets.size(); ++i) {
            PutLe<uint64_t>(index, first_local_ids[i]);
            const uint64_t offset = offsets[i];
            PutLe<uint64_t>(index, offset);
        }
        WriteLoop(fd, index.data(), index.size());
        const uint64_t index_offset = kTableHeaderBytes + payload_bytes;
        const uint64_t total = index_offset + index.size() + kTableFooterBytes;
        std::vector<uint8_t> footer;
        PutLe<uint32_t>(footer, kTableFooterMagic);
        PutLe<uint64_t>(footer, total);
        PutLe<uint32_t>(footer, ~payload_crc);
        PutLe<uint32_t>(footer, Crc32(index.data(), index.size()));
        PutLe<uint32_t>(footer, Crc32(footer.data(), footer.size()));
        WriteLoop(fd, footer.data(), footer.size());
        std::vector<uint8_t> header;
        PutLe<uint64_t>(header, kTableMagic);
        PutLe<uint32_t>(header, kTableHeaderBytes);
        PutLe<uint32_t>(header, table_id);
        PutLe<uint32_t>(header, 0);
        PutLe<uint64_t>(header, generation);
        PutLe<uint64_t>(header, visible_from);
        PutLe<uint64_t>(header, rows);
        PutLe<uint64_t>(header, payload_bytes);
        PutLe<uint64_t>(header, index_offset);
        PutLe<uint64_t>(header, offsets.size());
        PutLe<uint64_t>(header, total);
        PutLe<uint32_t>(header, Crc32(header.data(), header.size()));
        if (header.size() != kTableHeaderBytes)
            throw std::logic_error("immutable table header size mismatch");
        PwriteLoop(fd, header.data(), header.size(), 0);
        SyncFd(fd);
        if (close(fd) != 0) {
            fd = -1;
            ThrowSystemError("close immutable table temp");
        }
        fd = -1;
        if (syscall(SYS_renameat2, directory_fd, temp_name.c_str(), directory_fd, final_name.c_str(),
                    RENAME_NOREPLACE) != 0)
            ThrowSystemError("rename immutable table");
        renamed = true;
        if (crash == CheckpointCrashPoint::kAfterBaseRename)
            throw SimulatedCrash();
        while (fsync(directory_fd) != 0)
            if (errno != EINTR)
                ThrowSystemError("sync immutable table directory");
        return {table_id, generation, visible_from, rows, total};
    }
};

CheckpointDb::SnapshotArtifact::~SnapshotArtifact() {}

CheckpointDb::SnapshotArtifact::SnapshotArtifact(SnapshotArtifact&& other) noexcept
    : tables_(std::move(other.tables_)), writers_(std::move(other.writers_)) {}

CheckpointDb::SnapshotArtifact& CheckpointDb::SnapshotArtifact::operator=(SnapshotArtifact&& other) noexcept {
    if (this == &other)
        return *this;
    tables_ = std::move(other.tables_);
    writers_ = std::move(other.writers_);
    return *this;
}

CheckpointDb::PreparedSnapshotArtifacts::PreparedSnapshotArtifacts(PreparedSnapshotArtifacts&& other) noexcept
    : tables_(std::move(other.tables_)), writers_(std::move(other.writers_)), readers_(std::move(other.readers_)) {}

CheckpointDb::PreparedSnapshotArtifacts&
CheckpointDb::PreparedSnapshotArtifacts::operator=(PreparedSnapshotArtifacts&& other) noexcept {
    if (this == &other)
        return *this;
    tables_ = std::move(other.tables_);
    writers_ = std::move(other.writers_);
    readers_ = std::move(other.readers_);
    return *this;
}

CheckpointDb::PreparedSnapshotPublication::PreparedSnapshotPublication(PreparedSnapshotPublication&& other) noexcept
    : artifacts_(std::move(other.artifacts_)), source_(std::move(other.source_)), tables_(std::move(other.tables_)),
      manifest_bytes_(std::move(other.manifest_bytes_)), generation_(other.generation_), base_epoch_(other.base_epoch_),
      base_next_commit_seq_(other.base_next_commit_seq_), wal_lineage_(other.wal_lineage_),
      first_segment_id_(other.first_segment_id_), active_segment_id_(other.active_segment_id_) {}

CheckpointDb::PreparedSnapshotPublication&
CheckpointDb::PreparedSnapshotPublication::operator=(PreparedSnapshotPublication&& other) noexcept {
    if (this == &other)
        return *this;
    artifacts_ = std::move(other.artifacts_);
    source_ = std::move(other.source_);
    tables_ = std::move(other.tables_);
    manifest_bytes_ = std::move(other.manifest_bytes_);
    generation_ = other.generation_;
    base_epoch_ = other.base_epoch_;
    base_next_commit_seq_ = other.base_next_commit_seq_;
    wal_lineage_ = other.wal_lineage_;
    first_segment_id_ = other.first_segment_id_;
    active_segment_id_ = other.active_segment_id_;
    return *this;
}

void CheckpointDb::PreparedSnapshotArtifacts::KeepPublishedFiles() noexcept {
    for (auto& writer : writers_) {
        if (writer.impl_)
            writer.impl_->keep_final = true;
    }
}

void CheckpointDb::PreparedSnapshotArtifacts::UnkeepPublishedFiles() noexcept {
    for (auto& writer : writers_) {
        if (writer.impl_)
            writer.impl_->keep_final = false;
    }
}

void CheckpointDb::PreparedSnapshotArtifacts::MarkPublished() noexcept {
    for (auto& writer : writers_) {
        if (writer.impl_) {
            writer.impl_->keep_final = true;
            writer.impl_.reset();
        }
    }
    writers_.clear();
}

TableBaseWriter::TableBaseWriter(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
TableBaseWriter::~TableBaseWriter() = default;
TableBaseWriter::TableBaseWriter(TableBaseWriter&&) noexcept = default;
TableBaseWriter& TableBaseWriter::operator=(TableBaseWriter&&) noexcept = default;
void TableBaseWriter::Append(RowImage row) {
    if (!impl_)
        throw std::logic_error("inactive immutable table writer");
    impl_->Append(std::move(row));
}
uint64_t TableBaseWriter::row_count() const {
    return impl_ ? impl_->rows : 0;
}

ImmutableTable::ImmutableTable(std::string path, int fd, TableId table_id, uint64_t generation, Epoch visible_from,
                               uint64_t row_count, uint64_t payload_bytes, uint64_t file_bytes,
                               std::vector<uint64_t> first_ids, std::vector<uint64_t> offsets)
    : path_(std::move(path)), fd_(fd), table_id_(table_id), generation_(generation), visible_from_(visible_from),
      row_count_(row_count), payload_bytes_(payload_bytes), file_bytes_(file_bytes),
      block_first_ids_(std::move(first_ids)), block_offsets_(std::move(offsets)) {}

ImmutableTable::~ImmutableTable() {
    if (fd_ >= 0)
        close(fd_);
}

std::optional<bool> ImmutableTable::RecoveryContains(uint64_t local_id) const {
    if (!recovery_membership_complete_)
        return std::nullopt;
    const uint64_t word = local_id / 64;
    return word < recovery_membership_.size() && (recovery_membership_[word] & (uint64_t{1} << (local_id % 64))) != 0;
}

std::optional<Row> ImmutableTable::Read(uint64_t local_id, std::vector<uint8_t>* scratch) const {
    if (local_id >= next_local_id_)
        return std::nullopt;
    const auto diagnostics = diagnostics_;
    auto upper = std::upper_bound(block_first_ids_.begin(), block_first_ids_.end(), local_id);
    if (upper == block_first_ids_.begin())
        return std::nullopt;
    const size_t block = static_cast<size_t>(upper - block_first_ids_.begin() - 1);
    const uint64_t offset = kTableHeaderBytes + block_offsets_[block];
    const uint64_t stop = block + 1 < block_offsets_.size() ? kTableHeaderBytes + block_offsets_[block + 1]
                                                            : kTableHeaderBytes + payload_bytes_;
    if (offset >= stop || stop > kTableHeaderBytes + payload_bytes_ ||
        stop - offset > std::vector<uint8_t>().max_size())
        throw std::runtime_error("invalid immutable table block range");
    const size_t block_bytes = static_cast<size_t>(stop - offset);
    std::vector<uint8_t> local;
    std::vector<uint8_t>& bytes = scratch != nullptr && block_bytes <= kPointReadScratchBytes ? *scratch : local;
    bytes.resize(block_bytes);
    const auto pread_started = diagnostics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    PreadLoop(fd_, bytes.data(), bytes.size(), offset);
    if (diagnostics) {
        const auto pread_done = std::chrono::steady_clock::now();
        diagnostics->immutable_reads.fetch_add(1, std::memory_order_relaxed);
        diagnostics->immutable_bytes.fetch_add(bytes.size(), std::memory_order_relaxed);
        diagnostics->immutable_pread_ns.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(pread_done - pread_started).count()),
            std::memory_order_relaxed);
    }
    size_t pos = 0;
    while (pos < bytes.size()) {
        const uint64_t id = GetLe<uint64_t>(bytes, pos, bytes.size());
        const uint32_t length = GetLe<uint32_t>(bytes, pos, bytes.size());
        if (length > bytes.size() - pos)
            throw std::runtime_error("invalid immutable row length");
        if (id > local_id)
            return std::nullopt;
        if (id == local_id) {
            const auto decode_started =
                diagnostics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
            const auto decoded = DecodeRow(bytes.data() + pos, length);
            if (diagnostics) {
                diagnostics->immutable_decode_ns.fetch_add(
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now() - decode_started)
                                              .count()),
                    std::memory_order_relaxed);
            }
            return decoded;
        }
        pos += length;
    }
    return std::nullopt;
}

std::optional<std::pair<uint64_t, Row>> ImmutableTable::ReadAtOrAfter(uint64_t local_id) const {
    if (local_id >= next_local_id_)
        return std::nullopt;
    const auto diagnostics = diagnostics_;
    auto upper = std::upper_bound(block_first_ids_.begin(), block_first_ids_.end(), local_id);
    size_t block = upper == block_first_ids_.begin() ? 0 : static_cast<size_t>(upper - block_first_ids_.begin() - 1);
    for (; block < block_offsets_.size(); ++block) {
        const uint64_t offset = kTableHeaderBytes + block_offsets_[block];
        const uint64_t stop = block + 1 < block_offsets_.size() ? kTableHeaderBytes + block_offsets_[block + 1]
                                                                : kTableHeaderBytes + payload_bytes_;
        if (offset >= stop || stop > kTableHeaderBytes + payload_bytes_)
            throw std::runtime_error("invalid immutable table block range");
        std::vector<uint8_t> bytes(static_cast<size_t>(stop - offset));
        PreadLoop(fd_, bytes.data(), bytes.size(), offset);
        size_t pos = 0;
        while (pos < bytes.size()) {
            const uint64_t id = GetLe<uint64_t>(bytes, pos, bytes.size());
            const uint32_t length = GetLe<uint32_t>(bytes, pos, bytes.size());
            if (length > bytes.size() - pos)
                throw std::runtime_error("invalid immutable row length");
            if (id >= local_id)
                return std::make_pair(id, DecodeRow(bytes.data() + pos, length));
            pos += length;
        }
    }
    return std::nullopt;
}

void ImmutableTable::SetDiagnostics(std::shared_ptr<DeltaDiagnostics> diagnostics) const {
    diagnostics_ = std::move(diagnostics);
}

void ImmutableTable::Visit(const std::function<void(uint64_t, Row&&)>& visitor) const {
    const auto diagnostics = diagnostics_;
    if (diagnostics)
        diagnostics->immutable_scan_calls.fetch_add(1, std::memory_order_relaxed);
    constexpr size_t kBufferBytes = 1U << 20;
    const uint64_t end = kTableHeaderBytes + payload_bytes_;
    std::vector<uint8_t> buffer(kBufferBytes);
    size_t buffer_pos = 0;
    size_t buffer_size = 0;
    uint64_t next_read = kTableHeaderBytes;
    uint64_t consumed = kTableHeaderBytes;
    uint64_t previous_id = 0;
    bool have_previous_id = false;
    const auto read_bytes = [&](uint8_t* out, size_t bytes) {
        while (bytes != 0) {
            if (buffer_pos == buffer_size) {
                const size_t available = static_cast<size_t>(std::min<uint64_t>(buffer.size(), end - next_read));
                if (available == 0)
                    throw std::runtime_error("immutable table row count mismatch");
                const auto pread_started =
                    diagnostics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
                PreadLoop(fd_, buffer.data(), available, next_read);
                if (diagnostics) {
                    diagnostics->immutable_scan_pread_calls.fetch_add(1, std::memory_order_relaxed);
                    diagnostics->immutable_scan_bytes.fetch_add(available, std::memory_order_relaxed);
                    diagnostics->immutable_scan_pread_ns.fetch_add(
                        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                  std::chrono::steady_clock::now() - pread_started)
                                                  .count()),
                        std::memory_order_relaxed);
                }
                next_read += available;
                buffer_pos = 0;
                buffer_size = available;
            }
            const size_t copied = std::min(bytes, buffer_size - buffer_pos);
            std::memcpy(out, buffer.data() + buffer_pos, copied);
            out += copied;
            bytes -= copied;
            buffer_pos += copied;
            consumed += copied;
        }
    };
    for (uint64_t ordinal = 0; ordinal < row_count_; ++ordinal) {
        const uint64_t record_offset = consumed - kTableHeaderBytes;
        std::vector<uint8_t> header(12);
        read_bytes(header.data(), header.size());
        size_t pos = 0;
        const uint64_t id = GetLe<uint64_t>(header, pos, header.size());
        const uint32_t length = GetLe<uint32_t>(header, pos, header.size());
        if (have_previous_id && id <= previous_id)
            throw std::runtime_error("immutable table row ids are not ordered");
        if (ordinal % kRowsPerBlock == 0) {
            const size_t block = static_cast<size_t>(ordinal / kRowsPerBlock);
            if (block >= block_offsets_.size() || block >= block_first_ids_.size() ||
                record_offset != block_offsets_[block] || id != block_first_ids_[block])
                throw std::runtime_error("immutable table sparse index mismatch");
        }
        if (length > end - consumed)
            throw std::runtime_error("invalid immutable row length");
        std::vector<uint8_t> encoded(length);
        read_bytes(encoded.data(), encoded.size());
        const auto decode_started =
            diagnostics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
        Row decoded = DecodeRow(encoded);
        if (diagnostics) {
            diagnostics->immutable_scan_rows.fetch_add(1, std::memory_order_relaxed);
            diagnostics->immutable_scan_decode_calls.fetch_add(1, std::memory_order_relaxed);
            diagnostics->immutable_scan_decode_ns.fetch_add(
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::steady_clock::now() - decode_started)
                                          .count()),
                std::memory_order_relaxed);
        }
        visitor(id, std::move(decoded));
        previous_id = id;
        have_previous_id = true;
    }
    if (consumed != end)
        throw std::runtime_error("immutable table row count mismatch");
}

void ImmutableTable::ValidateRowsForInstall() {
    next_local_id_ = 0;
    recovery_membership_.clear();
    recovery_membership_complete_ = row_count_ <= recovery_membership_.max_size();
    Visit([&](uint64_t local_id, Row&& row) {
        if (row.bytes.size() > 16U * 1024U * 1024U || !row.claims.empty() ||
            local_id == std::numeric_limits<uint64_t>::max())
            throw std::invalid_argument("invalid immutable table row");
        next_local_id_ = local_id + 1;
        if (recovery_membership_complete_) {
            const uint64_t word = local_id / 64;
            // Keep the recovery accelerator bounded by physical rows. Very sparse tables use the point-read fallback.
            if (word >= row_count_) {
                std::vector<uint64_t>().swap(recovery_membership_);
                recovery_membership_complete_ = false;
            } else {
                recovery_membership_.resize(static_cast<size_t>(word + 1));
                recovery_membership_[word] |= uint64_t{1} << (local_id % 64);
            }
        }
    });
}

namespace {

std::shared_ptr<const ImmutableTable> OpenTable(const std::string& directory, const CheckpointDb::TableRef& ref) {
    const std::string path = Join(directory, TableName(ref.table_id, ref.file_generation));
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        ThrowSystemError("open immutable table");
    try {
        struct stat st {};
        if (fstat(fd, &st) != 0 || st.st_size < 0 || static_cast<uint64_t>(st.st_size) != ref.file_bytes ||
            ref.file_bytes > kMaxTableBytes)
            throw std::runtime_error("immutable table size mismatch");
        std::vector<uint8_t> header(kTableHeaderBytes);
        PreadLoop(fd, header.data(), header.size(), 0);
        size_t p = 0;
        const uint64_t magic = GetLe<uint64_t>(header, p, header.size());
        const uint32_t header_bytes = GetLe<uint32_t>(header, p, header.size());
        const TableId table_id = GetLe<uint32_t>(header, p, header.size());
        const uint32_t reserved = GetLe<uint32_t>(header, p, header.size());
        const uint64_t generation = GetLe<uint64_t>(header, p, header.size());
        const Epoch visible = GetLe<uint64_t>(header, p, header.size());
        const uint64_t rows = GetLe<uint64_t>(header, p, header.size());
        const uint64_t payload = GetLe<uint64_t>(header, p, header.size());
        const uint64_t index_offset = GetLe<uint64_t>(header, p, header.size());
        const uint64_t index_count = GetLe<uint64_t>(header, p, header.size());
        const uint64_t total = GetLe<uint64_t>(header, p, header.size());
        const uint32_t header_crc = GetLe<uint32_t>(header, p, header.size());
        if (magic != kTableMagic || header_bytes != kTableHeaderBytes || reserved != 0 || table_id != ref.table_id ||
            generation != ref.file_generation || visible != ref.visible_from || rows != ref.row_count ||
            total != ref.file_bytes || index_offset != kTableHeaderBytes + payload ||
            index_count != (rows + kRowsPerBlock - 1) / kRowsPerBlock ||
            index_count > (kMaxTableBytes / (2 * sizeof(uint64_t))) ||
            index_offset + index_count * 2 * sizeof(uint64_t) + kTableFooterBytes != total ||
            header_crc != Crc32(header.data(), header.size() - 4))
            throw std::runtime_error("invalid immutable table header");
        std::vector<uint8_t> index_bytes(static_cast<size_t>(index_count * 2 * sizeof(uint64_t)));
        PreadLoop(fd, index_bytes.data(), index_bytes.size(), index_offset);
        std::vector<uint64_t> first_ids;
        std::vector<uint64_t> offsets;
        size_t ip = 0;
        for (uint64_t i = 0; i < index_count; ++i) {
            const uint64_t first_id = GetLe<uint64_t>(index_bytes, ip, index_bytes.size());
            const uint64_t offset = GetLe<uint64_t>(index_bytes, ip, index_bytes.size());
            if (offset >= payload || (i == 0 && offset != 0) || (!offsets.empty() && offset <= offsets.back()))
                throw std::runtime_error("invalid immutable table sparse index");
            if ((!first_ids.empty() && first_id <= first_ids.back()))
                throw std::runtime_error("invalid immutable table sparse ids");
            first_ids.push_back(first_id);
            offsets.push_back(offset);
        }
        std::vector<uint8_t> footer(kTableFooterBytes);
        PreadLoop(fd, footer.data(), footer.size(), total - footer.size());
        size_t fp = 0;
        if (GetLe<uint32_t>(footer, fp, footer.size()) != kTableFooterMagic ||
            GetLe<uint64_t>(footer, fp, footer.size()) != total)
            throw std::runtime_error("invalid immutable table footer");
        const uint32_t payload_crc = GetLe<uint32_t>(footer, fp, footer.size());
        const uint32_t index_crc = GetLe<uint32_t>(footer, fp, footer.size());
        const uint32_t footer_crc = GetLe<uint32_t>(footer, fp, footer.size());
        if (index_crc != Crc32(index_bytes.data(), index_bytes.size()) ||
            footer_crc != Crc32(footer.data(), footer.size() - 4))
            throw std::runtime_error("invalid immutable table index/footer CRC");
        std::vector<uint8_t> chunk(1U << 20);
        uint64_t done = 0;
        uint32_t crc = 0xffffffffU;
        while (done < payload) {
            const size_t bytes = static_cast<size_t>(std::min<uint64_t>(chunk.size(), payload - done));
            PreadLoop(fd, chunk.data(), bytes, kTableHeaderBytes + done);
            crc = UpdateCrc32(crc, chunk.data(), bytes);
            done += bytes;
        }
        if (~crc != payload_crc)
            throw std::runtime_error("invalid immutable table payload CRC");
        auto table = std::make_shared<ImmutableTable>(path, fd, table_id, generation, visible, rows, payload, total,
                                                      std::move(first_ids), std::move(offsets));
        table->ValidateRowsForInstall(); // Structural validation is bounded and fail-closed.
        return table;
    } catch (...) {
        close(fd);
        throw;
    }
}

void GarbageCollectTables(const std::string& directory_name,
                          const std::map<TableId, CheckpointDb::TableRef>& refs) noexcept {
    try {
        std::set<std::string> keep;
        for (const auto& [id, ref] : refs)
            keep.insert(TableName(id, ref.file_generation));
        DirectoryFd directory(directory_name);
        const int scan_fd = dup(directory.get());
        if (scan_fd < 0)
            return;
        DIR* entries = fdopendir(scan_fd);
        if (!entries) {
            close(scan_fd);
            return;
        }
        bool removed = false;
        while (dirent* entry = readdir(entries)) {
            const std::string_view name(entry->d_name);
            TableId table_id = 0;
            uint64_t generation = 0;
            if (!ParseTableName(name, &table_id, &generation) || keep.count(std::string(name)))
                continue;
            struct stat before {};
            if (fstatat(directory.get(), entry->d_name, &before, AT_SYMLINK_NOFOLLOW) != 0 || !S_ISREG(before.st_mode))
                continue;
            const int fd = openat(directory.get(), entry->d_name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
            if (fd < 0)
                continue;
            struct stat opened {};
            struct stat current {};
            const bool owned = fstat(fd, &opened) == 0 && S_ISREG(opened.st_mode) &&
                               fstatat(directory.get(), entry->d_name, &current, AT_SYMLINK_NOFOLLOW) == 0 &&
                               S_ISREG(current.st_mode) && opened.st_dev == current.st_dev &&
                               opened.st_ino == current.st_ino && opened.st_dev == before.st_dev &&
                               opened.st_ino == before.st_ino;
            close(fd);
            if (owned && unlinkat(directory.get(), entry->d_name, 0) == 0)
                removed = true;
        }
        closedir(entries);
        if (removed)
            directory.Sync();
    } catch (...) {
    }
}

void RefreshMigrationFrontiersAtReplayBoundary(std::map<TableId, CheckpointDb::TableRef>& tables,
                                               const EpochSiEngine& engine) {
    for (auto& [id, ref] : tables)
        ref.next_local_id = std::max(ref.next_local_id, engine.next_local_id(id));
}

} // namespace

CheckpointDb::CheckpointDb(std::string directory, uint64_t generation, uint64_t wal_generation, Epoch base_epoch,
                           std::map<TableId, TableRef> tables, EpochSiEngine engine, size_t wal_open_directory_syncs)
    : directory_(std::move(directory)), generation_(generation), wal_generation_(wal_generation),
      wal_chain_(std::make_unique<WalChain>()), base_epoch_(base_epoch), tables_(std::move(tables)),
      engine_(std::move(engine)), wal_open_directory_syncs_(wal_open_directory_syncs) {}

CheckpointDb::~CheckpointDb() = default;

CheckpointDb::CheckpointDb(CheckpointDb&& other) noexcept
    : directory_(std::move(other.directory_)), generation_(other.generation_), wal_generation_(other.wal_generation_),
      wal_chain_(std::move(other.wal_chain_)), base_epoch_(other.base_epoch_), tables_(std::move(other.tables_)),
      engine_(std::move(other.engine_)), wal_open_directory_syncs_(other.wal_open_directory_syncs_),
      sealed_wal_bytes_(other.sealed_wal_bytes_), crash_point_(other.crash_point_), poisoned_(other.poisoned_) {}

CheckpointDb& CheckpointDb::operator=(CheckpointDb&& other) noexcept {
    if (this == &other)
        return *this;
    directory_ = std::move(other.directory_);
    generation_ = other.generation_;
    wal_generation_ = other.wal_generation_;
    base_epoch_ = other.base_epoch_;
    tables_ = std::move(other.tables_);
    engine_ = std::move(other.engine_);
    wal_open_directory_syncs_ = other.wal_open_directory_syncs_;
    crash_point_ = other.crash_point_;
    poisoned_ = other.poisoned_;
    sealed_wal_bytes_ = other.sealed_wal_bytes_;
    wal_chain_ = std::move(other.wal_chain_);
    return *this;
}

void CheckpointDb::RequireUsable() const {
    if (poisoned_)
        throw std::logic_error("checkpoint database is poisoned");
}

CheckpointDb CheckpointDb::Create(const std::string& directory, BaseImage initial_rows) {
    if (!std::filesystem::is_directory(directory) || !std::filesystem::is_empty(directory))
        throw std::invalid_argument("fresh database directory required");
    std::map<TableId, TableRef> tables;
    for (auto it = initial_rows.begin(); it != initial_rows.end();) {
        const TableId table_id = it->first.table_id;
        TableBaseWriter writer(std::make_unique<TableBaseWriter::Impl>(directory, table_id, 0, 0));
        do {
            writer.impl_->AppendAt(it->first.local_id, std::move(it->second));
            ++it;
        } while (it != initial_rows.end() && it->first.table_id == table_id);
        auto ref = writer.impl_->Finish(CheckpointCrashPoint::kNone);
        ref.next_local_id = writer.impl_->next_local_id;
        tables.emplace(table_id, ref);
        writer.impl_->keep_final = true;
        writer.impl_.reset();
    }
    DirectoryFd directory_fd(directory);
    auto segment = FileWal::CreateSegmentAt(directory_fd.get(), SegmentName(0, 0),
                                            {0, 0, std::numeric_limits<uint64_t>::max(), 1, 1});
    segment.Sync();
    directory_fd.Sync();
    ManifestState manifest;
    manifest.manifest_format = kSegmentedManifestFormat;
    manifest.generation = 0;
    manifest.wal_base_epoch = 0;
    manifest.base_next_commit_seq = 1;
    manifest.has_legacy_prefix = false;
    manifest.wal_lineage_generation = 0;
    manifest.first_segment_id = manifest.active_segment_id = 0;
    manifest.segment_count = 1;
    manifest.tables = std::move(tables);
    PublishManifest(directory, manifest, CheckpointCrashPoint::kNone);
    return Open(directory);
}

CheckpointDb CheckpointDb::Open(const std::string& directory) {
    ManifestState manifest = DecodeManifest(ReadFile(Join(directory, "MANIFEST"), kMaxManifestBytes));
    ImmutableTables tables;
    for (const auto& [id, ref] : manifest.tables)
        tables.emplace(id, OpenTable(directory, ref));
    DirectoryFd directory_fd(directory);
    std::optional<EpochSiEngine> opened;
    std::unique_ptr<FileWal> legacy;
    std::vector<std::unique_ptr<FileWal>> segments;
    size_t sealed_wal_bytes = 0;
    if (manifest.manifest_format == kLegacyManifestFormat) {
        const std::string wal_name = ResolveAndMigrateWal(directory_fd, manifest.wal_generation);
        opened.emplace(
            EpochSiEngine::OpenFileAt({}, std::move(tables), directory_fd.get(), wal_name, manifest.wal_base_epoch));
    } else {
        if (manifest.has_legacy_prefix) {
            const std::string wal_name = ResolveAndMigrateWal(directory_fd, manifest.legacy_prefix_generation);
            legacy = std::make_unique<FileWal>(directory_fd.get(), wal_name, FileWal::OpenMode::kExisting);
            const size_t size = legacy->size();
            if (size > std::numeric_limits<size_t>::max() - sealed_wal_bytes)
                throw std::overflow_error("sealed WAL byte count exhausted");
            sealed_wal_bytes += size;
        }
        segments.reserve(static_cast<size_t>(manifest.segment_count));
        for (uint64_t offset = 0; offset < manifest.segment_count; ++offset) {
            const uint64_t id = manifest.first_segment_id + offset;
            segments.push_back(std::make_unique<FileWal>(FileWal::OpenSegmentAt(
                directory_fd.get(), SegmentName(manifest.wal_lineage_generation, id),
                {manifest.wal_lineage_generation, id, id == 0 ? std::numeric_limits<uint64_t>::max() : id - 1, 0, 0})));
            if (id < manifest.active_segment_id) {
                const size_t size = segments.back()->size();
                if (size > std::numeric_limits<size_t>::max() - sealed_wal_bytes)
                    throw std::overflow_error("sealed WAL byte count exhausted");
                sealed_wal_bytes += size;
            }
        }
        std::map<TableId, uint64_t> frontiers;
        for (const auto& [id, ref] : manifest.tables)
            frontiers.emplace(id, ref.next_local_id);
        opened.emplace(EpochSiEngine::OpenWalChain({}, std::move(tables), std::move(legacy), std::move(segments),
                                                   manifest.wal_base_epoch, manifest.base_next_commit_seq, frontiers));
    }
    EpochSiEngine engine = std::move(*opened);
    if (manifest.manifest_format == kLegacyManifestFormat) {
        for (auto& [id, ref] : manifest.tables)
            ref.next_local_id = engine.next_local_id(id);
    }
    GarbageCollectTables(directory, manifest.tables);
    CheckpointDb result(directory, manifest.generation, manifest.wal_generation, manifest.wal_base_epoch,
                        std::move(manifest.tables), std::move(engine), 1);
    result.wal_chain_->has_legacy_prefix =
        manifest.manifest_format == kLegacyManifestFormat ? true : manifest.has_legacy_prefix;
    result.wal_chain_->has_segment_chain = manifest.manifest_format == kSegmentedManifestFormat;
    result.wal_chain_->legacy_prefix_generation =
        manifest.manifest_format == kLegacyManifestFormat ? manifest.wal_generation : manifest.legacy_prefix_generation;
    result.wal_chain_->lineage_generation =
        manifest.manifest_format == kLegacyManifestFormat ? manifest.wal_generation : manifest.wal_lineage_generation;
    result.wal_chain_->first_segment_id =
        manifest.manifest_format == kLegacyManifestFormat ? 0 : manifest.first_segment_id;
    result.wal_chain_->active_segment_id = manifest.manifest_format == kLegacyManifestFormat
                                               ? std::numeric_limits<uint64_t>::max()
                                               : manifest.active_segment_id;
    result.wal_chain_->base_epoch = manifest.wal_base_epoch;
    result.wal_chain_->base_next_commit_seq =
        manifest.manifest_format == kLegacyManifestFormat ? 1 : manifest.base_next_commit_seq;
    result.sealed_wal_bytes_ = sealed_wal_bytes;
    result.GarbageCollectExcludedWal();
    return result;
}

size_t CheckpointDb::durable_wal_bytes() const {
    RequireUsable();
    const size_t active = engine_.durable_wal_bytes();
    if (active > std::numeric_limits<size_t>::max() - sealed_wal_bytes_)
        throw std::overflow_error("WAL byte count exhausted");
    return sealed_wal_bytes_ + active;
}

TableBaseWriter CheckpointDb::BeginTableBase(TableId table_id) {
    RequireUsable();
    if (engine_.active_transaction_count() != 0 || !engine_.CanInstallPristineTable(table_id))
        throw std::logic_error("LOAD requires a pristine table and no active transaction");
    if (generation_ == std::numeric_limits<uint64_t>::max())
        throw std::overflow_error("manifest generation exhausted");
    return TableBaseWriter(
        std::make_unique<TableBaseWriter::Impl>(directory_, table_id, generation_ + 1, engine_.published_epoch()));
}

void CheckpointDb::PublishTableBase(TableBaseWriter&& writer) {
    RequireUsable();
    if (!writer.impl_ || writer.impl_->directory != directory_ || engine_.active_transaction_count() != 0 ||
        !engine_.CanInstallPristineTable(writer.impl_->table_id))
        throw std::logic_error("invalid immutable table publish");
    const TableRef ref = writer.impl_->Finish(crash_point_);
    TableRef persisted_ref = ref;
    persisted_ref.next_local_id = writer.impl_->next_local_id;
    auto table = OpenTable(directory_, persisted_ref);
    auto prepared = engine_.PrepareTableInstall(table);
    ManifestState next;
    next.manifest_format = kSegmentedManifestFormat;
    next.generation = generation_ + 1;
    next.wal_generation = wal_generation_;
    next.wal_base_epoch = base_epoch_;
    next.base_next_commit_seq = wal_chain_->base_next_commit_seq;
    next.has_legacy_prefix = wal_chain_->has_legacy_prefix;
    next.legacy_prefix_generation = wal_chain_->legacy_prefix_generation;
    next.wal_lineage_generation = wal_chain_->lineage_generation;
    if (wal_chain_->has_segment_chain) {
        next.first_segment_id = wal_chain_->first_segment_id;
        next.active_segment_id = wal_chain_->active_segment_id;
        next.segment_count = next.active_segment_id - next.first_segment_id + 1;
    }
    next.tables = tables_;
    next.tables[persisted_ref.table_id] = persisted_ref;
    std::unique_ptr<FileWal> migration_candidate;
    size_t migrated_active_wal = 0;
    if (!wal_chain_->has_segment_chain) {
        RefreshMigrationFrontiersAtReplayBoundary(next.tables, engine_);
        if (wal_chain_->legacy_prefix_generation == std::numeric_limits<uint64_t>::max())
            throw std::overflow_error("WAL lineage exhausted");
        migrated_active_wal = engine_.durable_wal_bytes();
        if (migrated_active_wal > std::numeric_limits<size_t>::max() - sealed_wal_bytes_)
            throw std::overflow_error("sealed WAL byte count exhausted");
        const uint64_t lineage = wal_chain_->legacy_prefix_generation + 1;
        DirectoryFd directory_fd(directory_);
        const std::string segment_name = SegmentName(lineage, 0);
        RemoveExpectedFutureWal(directory_fd, segment_name);
        migration_candidate = std::make_unique<FileWal>(
            FileWal::CreateSegmentAt(directory_fd.get(), segment_name,
                                     {lineage, 0, std::numeric_limits<uint64_t>::max(), engine_.published_epoch() + 1,
                                      engine_.next_commit_seq()}));
        migration_candidate->SetDiagnostics(engine_.diagnostics_);
        migration_candidate->Sync();
        directory_fd.Sync();
        next.wal_lineage_generation = lineage;
        next.base_next_commit_seq = wal_chain_->base_next_commit_seq;
        next.first_segment_id = next.active_segment_id = 0;
        next.segment_count = 1;
    }
    try {
        PublishManifest(directory_, next, crash_point_);
        generation_ = next.generation;
        tables_.swap(next.tables);
        engine_.InstallTablePrepared(std::move(prepared));
        if (migration_candidate) {
            engine_.InstallFileWalForRotation(std::move(migration_candidate));
            sealed_wal_bytes_ += migrated_active_wal;
            wal_chain_->has_legacy_prefix = true;
            wal_chain_->has_segment_chain = true;
            wal_chain_->legacy_prefix_generation = next.legacy_prefix_generation;
            wal_chain_->lineage_generation = next.wal_lineage_generation;
            wal_chain_->first_segment_id = wal_chain_->active_segment_id = 0;
            wal_chain_->base_next_commit_seq = next.base_next_commit_seq;
            wal_generation_ = next.wal_lineage_generation;
        }
        writer.impl_->keep_final = true;
        writer.impl_.reset();
        if (crash_point_ == CheckpointCrashPoint::kAfterSuccess)
            throw SimulatedCrash();
    } catch (...) {
        try {
            if (DecodeManifest(ReadFile(Join(directory_, "MANIFEST"), kMaxManifestBytes)).generation ==
                next.generation) {
                poisoned_ = true;
                engine_.Poison();
            }
        } catch (...) {
            poisoned_ = true;
            engine_.Poison();
        }
        throw;
    }
}

CheckpointDb::SnapshotCutBoundary CheckpointDb::RotateWalAtGate(CheckpointCrashPoint point) {
    RequireUsable();
    if (!wal_chain_ || generation_ == std::numeric_limits<uint64_t>::max())
        throw std::logic_error("WAL chain is unavailable");
    const bool migrating_legacy_prefix = !wal_chain_->has_segment_chain;
    engine_.SyncFileWalForRotation();
    const SnapshotCutBoundary boundary{engine_.published_epoch(), engine_.next_commit_seq(), 0, 0};
    uint64_t next_id = 0;
    if (wal_chain_->has_segment_chain) {
        if (wal_chain_->active_segment_id == std::numeric_limits<uint64_t>::max() ||
            wal_chain_->active_segment_id < wal_chain_->first_segment_id ||
            wal_chain_->active_segment_id - wal_chain_->first_segment_id + 1 >= 4096)
            throw std::overflow_error("WAL segment chain exhausted");
        next_id = wal_chain_->active_segment_id + 1;
    }
    uint64_t lineage = wal_chain_->lineage_generation;
    if (wal_chain_->has_legacy_prefix && wal_chain_->lineage_generation == wal_chain_->legacy_prefix_generation) {
        if (lineage == std::numeric_limits<uint64_t>::max())
            throw std::overflow_error("WAL lineage exhausted");
        lineage++;
    }
    const uint64_t first_id = wal_chain_->has_segment_chain ? wal_chain_->first_segment_id : 0;
    const std::string name = SegmentName(lineage, next_id);
    DirectoryFd directory_fd(directory_);
    RemoveExpectedFutureWal(directory_fd, name);
    auto candidate = std::make_unique<FileWal>(
        FileWal::CreateSegmentAt(directory_fd.get(), name,
                                 {lineage, next_id, next_id == 0 ? std::numeric_limits<uint64_t>::max() : next_id - 1,
                                  boundary.epoch + 1, boundary.next_commit_seq}));
    candidate->SetDiagnostics(engine_.diagnostics_);
    const size_t prior_active_wal = engine_.durable_wal_bytes();
    if (prior_active_wal > std::numeric_limits<size_t>::max() - sealed_wal_bytes_)
        throw std::overflow_error("sealed WAL byte count exhausted");
    if (point == CheckpointCrashPoint::kRotationAfterHeaderWrite)
        throw SimulatedCrash();
    candidate->Sync();
    if (point == CheckpointCrashPoint::kRotationAfterHeaderSync)
        throw SimulatedCrash();
    directory_fd.Sync();
    if (point == CheckpointCrashPoint::kRotationAfterDirSync)
        throw SimulatedCrash();

    ManifestState next;
    next.manifest_format = kSegmentedManifestFormat;
    next.generation = generation_ + 1;
    next.wal_base_epoch = base_epoch_;
    next.base_next_commit_seq = wal_chain_->base_next_commit_seq;
    next.has_legacy_prefix = wal_chain_->has_legacy_prefix;
    next.legacy_prefix_generation = wal_chain_->legacy_prefix_generation;
    next.wal_lineage_generation = lineage;
    next.first_segment_id = first_id;
    next.active_segment_id = next_id;
    next.segment_count = next_id - first_id + 1;
    next.wal_generation = lineage;
    next.tables = tables_;
    if (migrating_legacy_prefix)
        RefreshMigrationFrontiersAtReplayBoundary(next.tables, engine_);
    try {
        PublishManifest(directory_, next, point);
        engine_.InstallFileWalForRotation(std::move(candidate));
        sealed_wal_bytes_ += prior_active_wal;
        generation_ = next.generation;
        wal_generation_ = next.wal_lineage_generation;
        wal_chain_->has_legacy_prefix = next.has_legacy_prefix;
        wal_chain_->has_segment_chain = true;
        wal_chain_->legacy_prefix_generation = next.legacy_prefix_generation;
        wal_chain_->lineage_generation = lineage;
        wal_chain_->first_segment_id = first_id;
        wal_chain_->active_segment_id = next_id;
        wal_chain_->base_epoch = base_epoch_;
        wal_chain_->base_next_commit_seq = next.base_next_commit_seq;
        if (point == CheckpointCrashPoint::kRotationAfterSwitch)
            throw SimulatedCrash();
    } catch (...) {
        poisoned_ = true;
        engine_.Poison();
        throw;
    }
    return {boundary.epoch, boundary.next_commit_seq, lineage, next_id};
}

TableBaseWriter CheckpointDb::BeginSnapshotTableBase(TableId table_id, Epoch visible_from) {
    RequireUsable();
    if (table_id == 0 || generation_ == std::numeric_limits<uint64_t>::max())
        throw std::logic_error("invalid snapshot table base");
    return TableBaseWriter(
        std::make_unique<TableBaseWriter::Impl>(directory_, table_id, generation_ + 1, visible_from));
}

void CheckpointDb::AppendSnapshotRows(TableBaseWriter& writer, std::vector<std::pair<RowId, Row>> rows) {
    RequireUsable();
    if (!writer.impl_)
        throw std::logic_error("snapshot table writer is unavailable");
    for (auto& [id, row] : rows) {
        if (id.table_id != writer.impl_->table_id)
            throw std::logic_error("snapshot row belongs to another table");
        writer.impl_->AppendAt(id.local_id, std::move(row));
    }
}

void CheckpointDb::FinishSnapshotTableBase(TableBaseWriter& writer, uint64_t next_local_id,
                                           SnapshotArtifact& artifact) {
    RequireUsable();
    if (!writer.impl_)
        throw std::logic_error("snapshot table writer is unavailable");
    TableRef ref = writer.impl_->Finish(crash_point_);
    ref.next_local_id = std::max(writer.impl_->next_local_id, next_local_id);
    artifact.tables_.push_back(ref);
    artifact.writers_.push_back(std::move(writer));
}

CheckpointDb::PreparedSnapshotArtifacts CheckpointDb::AdoptForPublication(SnapshotArtifact&& artifact) {
    RequireUsable();
    PreparedSnapshotArtifacts prepared;
    prepared.tables_ = std::move(artifact.tables_);
    prepared.writers_ = std::move(artifact.writers_);
    try {
        for (const TableRef& ref : prepared.tables_)
            prepared.readers_.emplace(ref.table_id, OpenTable(directory_, ref));
    } catch (...) {
        prepared.readers_.clear();
        throw;
    }
    return prepared;
}

CheckpointDb::PreparedSnapshotPublication
CheckpointDb::PrepareSnapshotPublication(const SnapshotCutBoundary& boundary, uint64_t expected_source_identity,
                                         PreparedSnapshotArtifacts&& artifacts) {
    RequireUsable();
    if (!wal_chain_ || !wal_chain_->has_segment_chain || boundary.wal_lineage != wal_chain_->lineage_generation ||
        boundary.new_active_segment_id < wal_chain_->first_segment_id ||
        boundary.new_active_segment_id > wal_chain_->active_segment_id ||
        generation_ == std::numeric_limits<uint64_t>::max())
        throw std::logic_error("snapshot publication token is stale");
    if (!engine_.current_source_ || engine_.current_source_->identity != expected_source_identity)
        throw std::logic_error("snapshot source publication is stale");
    ManifestState next;
    next.manifest_format = kSegmentedManifestFormat;
    next.generation = generation_ + 1;
    next.wal_generation = wal_generation_;
    next.wal_base_epoch = boundary.epoch;
    next.base_next_commit_seq = boundary.next_commit_seq;
    next.has_legacy_prefix = false;
    next.legacy_prefix_generation = 0;
    next.wal_lineage_generation = wal_chain_->lineage_generation;
    next.first_segment_id = boundary.new_active_segment_id;
    next.active_segment_id = wal_chain_->active_segment_id;
    next.segment_count = next.active_segment_id - next.first_segment_id + 1;
    next.tables = tables_;
    ImmutableTables replacements;
    for (const auto& ref : artifacts.tables_) {
        if (ref.table_id == 0)
            throw std::logic_error("snapshot publication table set changed");
        next.tables[ref.table_id] = ref;
        auto table = artifacts.readers_.find(ref.table_id);
        if (table == artifacts.readers_.end() || !table->second)
            throw std::logic_error("snapshot publication reader missing");
        replacements.emplace(ref.table_id, table->second);
    }
    auto prepared_source =
        engine_.PrepareSourcePublication(expected_source_identity, std::move(replacements), boundary.epoch);
    PreparedSnapshotPublication prepared;
    prepared.artifacts_ = std::move(artifacts);
    prepared.source_ = std::move(prepared_source);
    prepared.manifest_bytes_ = EncodeManifest(next);
    prepared.tables_ = std::move(next.tables);
    prepared.generation_ = next.generation;
    prepared.base_epoch_ = next.wal_base_epoch;
    prepared.base_next_commit_seq_ = next.base_next_commit_seq;
    prepared.wal_lineage_ = next.wal_lineage_generation;
    prepared.first_segment_id_ = next.first_segment_id;
    prepared.active_segment_id_ = next.active_segment_id;
    return prepared;
}

void CheckpointDb::PublishSnapshotPublication(PreparedSnapshotPublication&& publication) {
    RequireUsable();
    if (publication.manifest_bytes_.empty())
        throw std::logic_error("invalid snapshot publication");
    engine_.ValidatePreparedSourcePublication(publication.source_);
    publication.artifacts_.KeepPublishedFiles();
    bool manifest_renamed = false;
    try {
        PublishManifestBytes(directory_, publication.manifest_bytes_, crash_point_, manifest_renamed);
    } catch (...) {
        if (manifest_renamed) {
            poisoned_ = true;
            engine_.Poison();
            std::terminate();
        }
        publication.artifacts_.UnkeepPublishedFiles();
        throw;
    }
    if (crash_point_ == CheckpointCrashPoint::kSnapshotAfterManifestDurable) {
        poisoned_ = true;
        engine_.Poison();
        std::terminate();
    }
    if (crash_point_ == CheckpointCrashPoint::kSnapshotBeforeSourceInstall) {
        poisoned_ = true;
        engine_.Poison();
        std::terminate();
    }
    publication.artifacts_.MarkPublished();
    generation_ = publication.generation_;
    base_epoch_ = publication.base_epoch_;
    wal_generation_ = publication.wal_lineage_;
    tables_ = std::move(publication.tables_);
    wal_chain_->base_epoch = publication.base_epoch_;
    wal_chain_->base_next_commit_seq = publication.base_next_commit_seq_;
    wal_chain_->has_legacy_prefix = false;
    wal_chain_->legacy_prefix_generation = 0;
    wal_chain_->lineage_generation = publication.wal_lineage_;
    wal_chain_->first_segment_id = publication.first_segment_id_;
    wal_chain_->active_segment_id = publication.active_segment_id_;
    sealed_wal_bytes_ = 0;
    engine_.InstallPreparedSource(publication.source_);
    if (crash_point_ == CheckpointCrashPoint::kSnapshotAfterSourceInstall) {
        poisoned_ = true;
        engine_.Poison();
        std::terminate();
    }
    GarbageCollectExcludedWal();
}

void CheckpointDb::GarbageCollectExcludedWal() noexcept {
    if (!wal_chain_ || !wal_chain_->has_segment_chain || wal_chain_->has_legacy_prefix)
        return;
    try {
        DirectoryFd directory(directory_);
        const int scan_fd = dup(directory.get());
        if (scan_fd < 0)
            return;
        DIR* entries = fdopendir(scan_fd);
        if (!entries) {
            close(scan_fd);
            return;
        }
        bool removed = false;
        while (dirent* entry = readdir(entries)) {
            const std::string_view name(entry->d_name);
            bool excluded = false;
            uint64_t lineage = 0;
            uint64_t segment_id = 0;
            if (ParseSegmentName(name, &lineage, &segment_id)) {
                excluded = lineage != wal_chain_->lineage_generation || segment_id < wal_chain_->first_segment_id ||
                           segment_id > wal_chain_->active_segment_id;
            } else if (ParseWalName(name, "db.log.") || ParseWalName(name, "wal.")) {
                excluded = true;
            }
            if (!excluded)
                continue;
            struct stat before {};
            if (fstatat(directory.get(), entry->d_name, &before, AT_SYMLINK_NOFOLLOW) != 0 || !S_ISREG(before.st_mode))
                continue;
            const int fd = openat(directory.get(), entry->d_name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
            if (fd < 0)
                continue;
            struct stat opened {};
            struct stat current {};
            const bool owned = fstat(fd, &opened) == 0 && S_ISREG(opened.st_mode) &&
                               fstatat(directory.get(), entry->d_name, &current, AT_SYMLINK_NOFOLLOW) == 0 &&
                               S_ISREG(current.st_mode) && opened.st_dev == current.st_dev &&
                               opened.st_ino == current.st_ino && opened.st_dev == before.st_dev &&
                               opened.st_ino == before.st_ino;
            close(fd);
            if (owned && unlinkat(directory.get(), entry->d_name, 0) == 0)
                removed = true;
        }
        closedir(entries);
        if (removed)
            directory.Sync();
    } catch (...) {
    }
}

void CheckpointDb::GarbageCollectExcludedTables() noexcept {
    GarbageCollectTables(directory_, tables_);
}

void CheckpointDb::OfflineCheckpoint() {
    RequireUsable();
    if (engine_.active_transaction_count() != 0)
        throw std::logic_error("checkpoint requires no active transaction");
    if (generation_ == std::numeric_limits<uint64_t>::max() || wal_generation_ == std::numeric_limits<uint64_t>::max())
        throw std::overflow_error("checkpoint generation exhausted");
    const uint64_t next_generation = generation_ + 1;
    const uint64_t next_wal = wal_generation_ + 1;
    const Epoch cut = engine_.published_epoch();
    auto next_refs = tables_;
    for (TableId table_id : engine_.dirty_tables()) {
        TableBaseWriter writer(std::make_unique<TableBaseWriter::Impl>(directory_, table_id, next_generation, cut));
        engine_.VisitPublished(table_id, [&](RowId id, const Row& row) { writer.impl_->AppendAt(id.local_id, row); });
        TableRef ref = writer.impl_->Finish(crash_point_);
        ref.next_local_id = std::max(writer.impl_->next_local_id, engine_.next_local_id(table_id));
        next_refs[table_id] = ref;
        writer.impl_->keep_final = true;
        writer.impl_.reset();
    }
    const std::string next_wal_name = SegmentName(next_wal, 0);
    DirectoryFd directory_fd(directory_);
    RemoveExpectedFutureWal(directory_fd, next_wal_name);
    const uint64_t next_seq = engine_.next_commit_seq();
    auto next_segment = std::make_unique<FileWal>(FileWal::CreateSegmentAt(
        directory_fd.get(), next_wal_name, {next_wal, 0, std::numeric_limits<uint64_t>::max(), cut + 1, next_seq}));
    next_segment->Sync();
    directory_fd.Sync();
    if (crash_point_ == CheckpointCrashPoint::kAfterWalCreate)
        throw SimulatedCrash();
    ImmutableTables readers;
    for (const auto& [id, ref] : next_refs)
        readers.emplace(id, OpenTable(directory_, ref));
    if (crash_point_ == CheckpointCrashPoint::kBeforeNextEngineOpen)
        throw SimulatedCrash();
    std::vector<std::unique_ptr<FileWal>> next_segments;
    next_segments.push_back(std::move(next_segment));
    std::map<TableId, uint64_t> frontiers;
    for (const auto& [id, ref] : next_refs)
        frontiers.emplace(id, ref.next_local_id);
    EpochSiEngine next_engine = EpochSiEngine::OpenWalChain({}, std::move(readers), nullptr, std::move(next_segments),
                                                            cut, next_seq, frontiers);
    ManifestState next;
    next.manifest_format = kSegmentedManifestFormat;
    next.generation = next_generation;
    next.wal_generation = next_wal;
    next.wal_base_epoch = cut;
    next.base_next_commit_seq = next_seq;
    next.has_legacy_prefix = false;
    next.wal_lineage_generation = next_wal;
    next.first_segment_id = next.active_segment_id = 0;
    next.segment_count = 1;
    next.tables = next_refs;
    try {
        PublishManifest(directory_, next, crash_point_);
        generation_ = next_generation;
        wal_generation_ = next_wal;
        base_epoch_ = cut;
        wal_chain_->has_legacy_prefix = false;
        wal_chain_->has_segment_chain = true;
        wal_chain_->lineage_generation = next_wal;
        wal_chain_->first_segment_id = wal_chain_->active_segment_id = 0;
        wal_chain_->base_epoch = cut;
        wal_chain_->base_next_commit_seq = next_seq;
        tables_.swap(next_refs);
        engine_ = std::move(next_engine);
        sealed_wal_bytes_ = 0;
        GarbageCollectExcludedTables();
        if (crash_point_ == CheckpointCrashPoint::kAfterSuccess)
            throw SimulatedCrash();
    } catch (...) {
        try {
            if (DecodeManifest(ReadFile(Join(directory_, "MANIFEST"), kMaxManifestBytes)).generation ==
                next_generation) {
                poisoned_ = true;
                engine_.Poison();
            }
        } catch (...) {
            poisoned_ = true;
            engine_.Poison();
        }
        throw;
    }
}

} // namespace epoch_si_poc
