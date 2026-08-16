#include "checkpoint_db.h"

#include "file_wal.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <set>
#include <stdexcept>
#include <system_error>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace epoch_si_poc {
namespace {

constexpr uint64_t kManifestMagic = 0x54534546494e414dULL; // MANIFEST
constexpr uint64_t kTableMagic = 0x45534142454c4254ULL;    // TBLBASE
constexpr uint32_t kTableFooterMagic = 0x454e4454;
constexpr uint32_t kTableHeaderBytes = 80;
constexpr uint32_t kTableFooterBytes = 24;
constexpr uint32_t kManifestHeaderBytes = 44;
constexpr uint32_t kManifestRefBytes = 40;
constexpr uint64_t kMaxManifestBytes = 16ULL << 20;
constexpr uint64_t kMaxTableBytes = 64ULL << 30;
constexpr uint64_t kRowsPerBlock = 32;
constexpr size_t kTableWriteBufferBytes = 1U << 20;

struct ManifestState {
    uint64_t generation = 0;
    uint64_t wal_generation = 0;
    Epoch wal_base_epoch = 0;
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

RowImage DecodeRow(const std::vector<uint8_t>& bytes) {
    size_t pos = 0;
    const uint32_t image_bytes = GetLe<uint32_t>(bytes, pos, bytes.size());
    const uint16_t claim_count = GetLe<uint16_t>(bytes, pos, bytes.size());
    if (image_bytes > bytes.size() - pos || claim_count > (bytes.size() - pos - image_bytes) / 8)
        throw std::runtime_error("invalid immutable row image");
    RowImage row;
    row.bytes.assign(bytes.begin() + static_cast<std::ptrdiff_t>(pos),
                     bytes.begin() + static_cast<std::ptrdiff_t>(pos + image_bytes));
    pos += image_bytes;
    for (uint16_t i = 0; i < claim_count; ++i) {
        ConstraintClaim claim;
        claim.constraint_id = GetLe<uint32_t>(bytes, pos, bytes.size());
        const uint32_t claim_bytes = GetLe<uint32_t>(bytes, pos, bytes.size());
        if (claim_bytes > bytes.size() - pos)
            throw std::runtime_error("invalid immutable row claim");
        claim.bytes.assign(bytes.begin() + static_cast<std::ptrdiff_t>(pos),
                           bytes.begin() + static_cast<std::ptrdiff_t>(pos + claim_bytes));
        pos += claim_bytes;
        if (!row.claims.empty() && !(row.claims.back() < claim))
            throw std::runtime_error("invalid immutable row claim ordering");
        row.claims.push_back(std::move(claim));
    }
    if (pos != bytes.size())
        throw std::runtime_error("immutable row trailing bytes");
    return row;
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
std::string TableName(TableId table_id, uint64_t generation) {
    return "tablebase." + std::to_string(table_id) + "." + std::to_string(generation);
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
    const int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
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
    if (state.tables.size() > (kMaxManifestBytes - kManifestHeaderBytes - 4) / kManifestRefBytes)
        throw std::overflow_error("manifest table count exceeds limit");
    std::vector<uint8_t> out;
    PutLe<uint64_t>(out, kManifestMagic);
    PutLe<uint32_t>(out, 0);
    PutLe<uint64_t>(out, state.generation);
    PutLe<uint64_t>(out, state.wal_generation);
    PutLe<uint64_t>(out, state.wal_base_epoch);
    PutLe<uint32_t>(out, static_cast<uint32_t>(state.tables.size()));
    PutLe<uint32_t>(out, 0);
    for (const auto& [id, ref] : state.tables) {
        PutLe<uint32_t>(out, id);
        PutLe<uint32_t>(out, 0);
        PutLe<uint64_t>(out, ref.file_generation);
        PutLe<uint64_t>(out, ref.visible_from);
        PutLe<uint64_t>(out, ref.row_count);
        PutLe<uint64_t>(out, ref.file_bytes);
    }
    const uint32_t total = static_cast<uint32_t>(out.size() + 4);
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
    state.wal_generation = GetLe<uint64_t>(bytes, pos, bytes.size());
    state.wal_base_epoch = GetLe<uint64_t>(bytes, pos, bytes.size());
    const uint32_t count = GetLe<uint32_t>(bytes, pos, bytes.size());
    if (GetLe<uint32_t>(bytes, pos, bytes.size()) != 0 ||
        bytes.size() != kManifestHeaderBytes + static_cast<uint64_t>(count) * kManifestRefBytes + 4)
        throw std::runtime_error("invalid manifest table count");
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
    return state;
}

void PublishManifest(const std::string& directory, const ManifestState& state, CheckpointCrashPoint crash) {
    const std::string temp = Join(directory, "MANIFEST.tmp");
    WriteFile(temp, EncodeManifest(state), crash == CheckpointCrashPoint::kDuringManifestTemp);
    RenameFile(temp, Join(directory, "MANIFEST"));
    if (crash == CheckpointCrashPoint::kAfterManifestRenameBeforeDirSync)
        throw SimulatedCrash();
    SyncDirectory(directory);
}

void CreateEmptyWal(int directory_fd, const std::string& name) {
    FileWal wal(directory_fd, name, FileWal::OpenMode::kCreateNew);
    wal.Sync();
}

std::shared_ptr<const ImmutableTable> OpenTable(const std::string& directory, const CheckpointDb::TableRef& ref);

} // namespace

struct TableBaseWriter::Impl {
    std::string directory;
    std::string temp;
    std::string final;
    int fd = -1;
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

    Impl(std::string directory_, TableId table_id_, uint64_t generation_, Epoch visible_from_)
        : directory(std::move(directory_)), temp(Join(directory, TableName(table_id_, generation_) + ".tmp")),
          final(Join(directory, TableName(table_id_, generation_))), table_id(table_id_), generation(generation_),
          visible_from(visible_from_) {
        fd = open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
        if (fd < 0)
            ThrowSystemError("open immutable table temp");
        std::vector<uint8_t> header(kTableHeaderBytes, 0);
        WriteLoop(fd, header.data(), header.size());
        output.reserve(kTableWriteBufferBytes);
    }

    ~Impl() {
        if (fd >= 0)
            close(fd);
        if (!renamed)
            unlink(temp.c_str());
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
        RenameFile(temp, final);
        renamed = true;
        if (crash == CheckpointCrashPoint::kAfterBaseRename)
            throw SimulatedCrash();
        SyncDirectory(directory);
        return {table_id, generation, visible_from, rows, total};
    }
};

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

std::optional<Row> ImmutableTable::Read(uint64_t local_id) const {
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
    std::vector<uint8_t> bytes(static_cast<size_t>(stop - offset));
    PreadLoop(fd_, bytes.data(), bytes.size(), offset);
    size_t pos = 0;
    while (pos < bytes.size()) {
        const uint64_t id = GetLe<uint64_t>(bytes, pos, bytes.size());
        const uint32_t length = GetLe<uint32_t>(bytes, pos, bytes.size());
        if (length > bytes.size() - pos)
            throw std::runtime_error("invalid immutable row length");
        if (id > local_id)
            return std::nullopt;
        if (id == local_id)
            return DecodeRow(std::vector<uint8_t>(bytes.begin() + pos, bytes.begin() + pos + length));
        pos += length;
    }
    return std::nullopt;
}

bool ImmutableTable::Contains(uint64_t local_id) const {
    return Read(local_id).has_value();
}

void ImmutableTable::Visit(const std::function<void(uint64_t, Row&&)>& visitor) const {
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
                PreadLoop(fd_, buffer.data(), available, next_read);
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
        visitor(id, DecodeRow(encoded));
        previous_id = id;
        have_previous_id = true;
    }
    if (consumed != end)
        throw std::runtime_error("immutable table row count mismatch");
}

void ImmutableTable::ValidateRowsForInstall() {
    next_local_id_ = 0;
    Visit([&](uint64_t local_id, Row&& row) {
        if (row.bytes.size() > 16U * 1024U * 1024U || !row.claims.empty() ||
            local_id == std::numeric_limits<uint64_t>::max())
            throw std::invalid_argument("invalid immutable table row");
        next_local_id_ = local_id + 1;
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

void GarbageCollectTables(const std::string& directory, const std::map<TableId, CheckpointDb::TableRef>& refs) {
    std::set<std::string> keep;
    for (const auto& [id, ref] : refs)
        keep.insert(TableName(id, ref.file_generation));
    bool removed = false;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        const std::string name = entry.path().filename().string();
        if (name.rfind("tablebase.", 0) == 0 && !keep.count(name)) {
            std::error_code error;
            removed = std::filesystem::remove(entry.path(), error) || removed;
        }
    }
    if (removed)
        SyncDirectory(directory);
}

} // namespace

CheckpointDb::CheckpointDb(std::string directory, uint64_t generation, uint64_t wal_generation, Epoch base_epoch,
                           std::map<TableId, TableRef> tables, EpochSiEngine engine, size_t wal_open_directory_syncs)
    : directory_(std::move(directory)), generation_(generation), wal_generation_(wal_generation),
      base_epoch_(base_epoch), tables_(std::move(tables)), engine_(std::move(engine)),
      wal_open_directory_syncs_(wal_open_directory_syncs) {}

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
        tables.emplace(table_id, writer.impl_->Finish(CheckpointCrashPoint::kNone));
        writer.impl_.reset();
    }
    DirectoryFd directory_fd(directory);
    CreateEmptyWal(directory_fd.get(), WalName(0));
    directory_fd.Sync();
    ManifestState manifest;
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
    const std::string wal_name = ResolveAndMigrateWal(directory_fd, manifest.wal_generation);
    EpochSiEngine engine =
        EpochSiEngine::OpenFileAt({}, std::move(tables), directory_fd.get(), wal_name, manifest.wal_base_epoch);
    GarbageCollectTables(directory, manifest.tables);
    return CheckpointDb(directory, manifest.generation, manifest.wal_generation, manifest.wal_base_epoch,
                        std::move(manifest.tables), std::move(engine), 1);
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
    auto table = OpenTable(directory_, ref);
    auto prepared = engine_.PrepareTableInstall(table);
    ManifestState next{generation_ + 1, wal_generation_, base_epoch_, tables_};
    next.tables[ref.table_id] = ref;
    try {
        PublishManifest(directory_, next, crash_point_);
        generation_ = next.generation;
        tables_.swap(next.tables);
        engine_.InstallTablePrepared(std::move(prepared));
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
    std::vector<std::string> replaced;
    for (TableId table_id : engine_.dirty_tables()) {
        TableBaseWriter writer(std::make_unique<TableBaseWriter::Impl>(directory_, table_id, next_generation, cut));
        engine_.VisitPublished(table_id, [&](RowId id, const Row& row) { writer.impl_->AppendAt(id.local_id, row); });
        const TableRef ref = writer.impl_->Finish(crash_point_);
        if (auto old = next_refs.find(table_id); old != next_refs.end())
            replaced.push_back(Join(directory_, TableName(table_id, old->second.file_generation)));
        next_refs[table_id] = ref;
        writer.impl_.reset();
    }
    const std::string next_wal_name = WalName(next_wal);
    const std::string next_legacy_wal_name = LegacyWalName(next_wal);
    DirectoryFd directory_fd(directory_);
    const bool current_exists = WalEntryExists(directory_fd.get(), next_wal_name);
    const bool legacy_exists = WalEntryExists(directory_fd.get(), next_legacy_wal_name);
    if (current_exists && unlinkat(directory_fd.get(), next_wal_name.c_str(), 0) != 0)
        ThrowSystemError("remove abandoned checkpoint WAL");
    if (legacy_exists && unlinkat(directory_fd.get(), next_legacy_wal_name.c_str(), 0) != 0)
        ThrowSystemError("remove abandoned checkpoint WAL");
    if (current_exists || legacy_exists)
        directory_fd.Sync();
    CreateEmptyWal(directory_fd.get(), next_wal_name);
    directory_fd.Sync();
    if (crash_point_ == CheckpointCrashPoint::kAfterWalCreate)
        throw SimulatedCrash();
    ImmutableTables readers;
    for (const auto& [id, ref] : next_refs)
        readers.emplace(id, OpenTable(directory_, ref));
    if (crash_point_ == CheckpointCrashPoint::kBeforeNextEngineOpen)
        throw SimulatedCrash();
    EpochSiEngine next_engine =
        EpochSiEngine::OpenFileAt({}, std::move(readers), directory_fd.get(), next_wal_name, cut);
    ManifestState next{next_generation, next_wal, cut, next_refs};
    try {
        PublishManifest(directory_, next, crash_point_);
        const std::string old_wal = Join(directory_, WalName(wal_generation_));
        generation_ = next_generation;
        wal_generation_ = next_wal;
        base_epoch_ = cut;
        tables_.swap(next_refs);
        engine_ = std::move(next_engine);
        unlink(old_wal.c_str());
        for (const std::string& path : replaced)
            unlink(path.c_str());
        SyncDirectory(directory_);
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
