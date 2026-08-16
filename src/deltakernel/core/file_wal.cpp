#include "file_wal.h"

#include "epoch_si_engine.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>

namespace epoch_si_poc {
namespace {

constexpr uint64_t kSegmentMagic = 0x544E454D47455344ULL; // DSGMENT
constexpr uint32_t kSegmentVersion = 1;

void RecordMax(std::atomic<uint64_t>& maximum, uint64_t value) noexcept {
    uint64_t observed = maximum.load(std::memory_order_relaxed);
    while (observed < value &&
           !maximum.compare_exchange_weak(observed, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

void ThrowSystemError(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

template <typename T> void PutLe(uint8_t* out, size_t& pos, T value) {
    for (size_t i = 0; i < sizeof(T); ++i)
        out[pos++] = static_cast<uint8_t>(value >> (8 * i));
}

template <typename T> T GetLe(const uint8_t* in, size_t& pos) {
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
        value |= static_cast<T>(in[pos++]) << (8 * i);
    return value;
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
uint32_t Crc32(const uint8_t* data, size_t size) {
    uint32_t crc = 0xffffffffU;
    for (size_t i = 0; i < size; ++i)
        crc = (crc >> 8U) ^ kCrc32Table[(crc ^ data[i]) & 0xffU];
    return ~crc;
}

} // namespace

FileWal::FileWal(const std::string& path, OpenMode mode) {
    const int flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW | (mode == OpenMode::kCreateNew ? O_CREAT | O_EXCL : 0);
    fd_ = open(path.c_str(), flags, 0600);
    if (fd_ < 0) {
        ThrowSystemError("open WAL");
    }
    struct stat stat_buf {};
    if (fstat(fd_, &stat_buf) != 0) {
        const int saved_errno = errno;
        close(fd_);
        fd_ = -1;
        errno = saved_errno;
        ThrowSystemError("stat WAL");
    }
    if (!S_ISREG(stat_buf.st_mode) || stat_buf.st_size < 0 ||
        static_cast<uintmax_t>(stat_buf.st_size) > std::numeric_limits<size_t>::max()) {
        close(fd_);
        fd_ = -1;
        throw std::runtime_error("WAL must be a representable regular file");
    }
    end_offset_ = static_cast<size_t>(stat_buf.st_size);
}

FileWal::FileWal(int directory_fd, const std::string& name, OpenMode mode) {
    if (name.empty() || name == "." || name == ".." || name.find('/') != std::string::npos)
        throw std::invalid_argument("WAL name must be one path component");
    const int flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW | (mode == OpenMode::kCreateNew ? O_CREAT | O_EXCL : 0);
    fd_ = openat(directory_fd, name.c_str(), flags, 0600);
    if (fd_ < 0) {
        ThrowSystemError("open WAL");
    }
    struct stat stat_buf {};
    if (fstat(fd_, &stat_buf) != 0) {
        const int saved_errno = errno;
        close(fd_);
        fd_ = -1;
        errno = saved_errno;
        ThrowSystemError("stat WAL");
    }
    if (!S_ISREG(stat_buf.st_mode) || stat_buf.st_size < 0 ||
        static_cast<uintmax_t>(stat_buf.st_size) > std::numeric_limits<size_t>::max()) {
        close(fd_);
        fd_ = -1;
        throw std::runtime_error("WAL must be a representable regular file");
    }
    end_offset_ = static_cast<size_t>(stat_buf.st_size);
}

FileWal FileWal::OpenSegmentAt(int directory_fd, const std::string& name, const SegmentInfo& expected) {
    FileWal wal(directory_fd, name, OpenMode::kExisting);
    wal.LoadSegmentHeader(expected);
    return wal;
}

FileWal FileWal::CreateSegmentAt(int directory_fd, const std::string& name, const SegmentInfo& info) {
    FileWal wal(directory_fd, name, OpenMode::kCreateNew);
    wal.WriteSegmentHeader(info);
    return wal;
}

void FileWal::WriteSegmentHeader(const SegmentInfo& info) {
    if (fd_ < 0 || end_offset_ != 0 || data_offset_ != 0 || info.first_epoch == 0 || info.first_commit_seq == 0)
        throw std::invalid_argument("invalid WAL segment header state");
    std::array<uint8_t, kSegmentHeaderBytes> header{};
    size_t pos = 0;
    PutLe<uint64_t>(header.data(), pos, kSegmentMagic);
    PutLe<uint32_t>(header.data(), pos, kSegmentVersion);
    PutLe<uint32_t>(header.data(), pos, kSegmentHeaderBytes);
    PutLe<uint64_t>(header.data(), pos, info.wal_generation);
    PutLe<uint64_t>(header.data(), pos, info.segment_id);
    PutLe<uint64_t>(header.data(), pos, info.previous_segment_id);
    PutLe<uint64_t>(header.data(), pos, info.first_epoch);
    PutLe<uint64_t>(header.data(), pos, info.first_commit_seq);
    PutLe<uint32_t>(header.data(), pos, 0);
    pos += 8;
    PutLe<uint32_t>(header.data(), pos, Crc32(header.data(), kSegmentHeaderBytes - sizeof(uint32_t)));
    if (pos != kSegmentHeaderBytes)
        throw std::logic_error("WAL segment header size mismatch");
    size_t done = 0;
    while (done < header.size()) {
        const ssize_t n = pwrite(fd_, header.data() + done, header.size() - done, static_cast<off_t>(done));
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            ThrowSystemError("write WAL segment header");
        done += static_cast<size_t>(n);
    }
    data_offset_ = kSegmentHeaderBytes;
    segment_info_ = info;
}

void FileWal::LoadSegmentHeader(const SegmentInfo& expected) {
    struct stat stat_buf {};
    if (fstat(fd_, &stat_buf) != 0 || stat_buf.st_size < kSegmentHeaderBytes)
        throw std::runtime_error("WAL segment header is truncated");
    std::array<uint8_t, kSegmentHeaderBytes> header{};
    size_t done = 0;
    while (done < header.size()) {
        const ssize_t n = pread(fd_, header.data() + done, header.size() - done, static_cast<off_t>(done));
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            ThrowSystemError("read WAL segment header");
        done += static_cast<size_t>(n);
    }
    size_t pos = 0;
    const uint64_t magic = GetLe<uint64_t>(header.data(), pos);
    const uint32_t version = GetLe<uint32_t>(header.data(), pos);
    const uint32_t bytes = GetLe<uint32_t>(header.data(), pos);
    SegmentInfo actual;
    actual.wal_generation = GetLe<uint64_t>(header.data(), pos);
    actual.segment_id = GetLe<uint64_t>(header.data(), pos);
    actual.previous_segment_id = GetLe<uint64_t>(header.data(), pos);
    actual.first_epoch = GetLe<uint64_t>(header.data(), pos);
    actual.first_commit_seq = GetLe<uint64_t>(header.data(), pos);
    const uint32_t flags = GetLe<uint32_t>(header.data(), pos);
    for (int i = 0; i < 8; ++i)
        if (header[pos++] != 0)
            throw std::runtime_error("WAL segment reserved header is nonzero");
    const uint32_t crc = GetLe<uint32_t>(header.data(), pos);
    if (magic != kSegmentMagic || version != kSegmentVersion || bytes != kSegmentHeaderBytes || flags != 0 ||
        crc != Crc32(header.data(), kSegmentHeaderBytes - sizeof(uint32_t)) ||
        actual.wal_generation != expected.wal_generation || actual.segment_id != expected.segment_id ||
        actual.previous_segment_id != expected.previous_segment_id ||
        (expected.first_epoch != 0 && actual.first_epoch != expected.first_epoch) ||
        (expected.first_commit_seq != 0 && actual.first_commit_seq != expected.first_commit_seq))
        throw std::runtime_error("invalid WAL segment header");
    data_offset_ = kSegmentHeaderBytes;
    end_offset_ = static_cast<size_t>(stat_buf.st_size - kSegmentHeaderBytes);
    segment_info_ = actual;
}

FileWal::~FileWal() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

FileWal::FileWal(FileWal&& other) noexcept
    : fd_(other.fd_), end_offset_(other.end_offset_), data_offset_(other.data_offset_),
      max_write_chunk_(other.max_write_chunk_), write_calls_(other.write_calls_), sync_calls_(other.sync_calls_),
      diagnostics_(std::move(other.diagnostics_)), segment_info_(other.segment_info_) {
    other.fd_ = -1;
    other.end_offset_ = 0;
    other.data_offset_ = 0;
}

FileWal& FileWal::operator=(FileWal&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    if (fd_ >= 0) {
        close(fd_);
    }
    fd_ = other.fd_;
    end_offset_ = other.end_offset_;
    data_offset_ = other.data_offset_;
    max_write_chunk_ = other.max_write_chunk_;
    write_calls_ = other.write_calls_;
    sync_calls_ = other.sync_calls_;
    diagnostics_ = std::move(other.diagnostics_);
    segment_info_ = other.segment_info_;
    other.fd_ = -1;
    other.end_offset_ = 0;
    other.data_offset_ = 0;
    return *this;
}

void FileWal::ReadAt(size_t offset, uint8_t* output, size_t bytes) const {
    if (offset > end_offset_ || bytes > end_offset_ - offset ||
        offset > static_cast<size_t>(std::numeric_limits<off_t>::max()) - data_offset_ ||
        bytes > static_cast<size_t>(std::numeric_limits<off_t>::max()) - data_offset_ - offset) {
        throw std::out_of_range("WAL read exceeds captured file size");
    }
    size_t done = 0;
    while (done < bytes) {
        const size_t chunk = std::min(bytes - done, static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
        const ssize_t read_bytes = pread(fd_, output + done, chunk, static_cast<off_t>(data_offset_ + offset + done));
        if (read_bytes < 0 && errno == EINTR) {
            continue;
        }
        if (read_bytes < 0) {
            ThrowSystemError("read WAL");
        }
        if (read_bytes == 0) {
            throw std::runtime_error("WAL shrank during read");
        }
        done += static_cast<size_t>(read_bytes);
    }
}

void FileWal::RequireUnchangedSize() const {
    struct stat stat_buf {};
    if (fstat(fd_, &stat_buf) != 0) {
        ThrowSystemError("stat WAL after read");
    }
    if (!S_ISREG(stat_buf.st_mode) || stat_buf.st_size < 0 ||
        static_cast<uintmax_t>(stat_buf.st_size) != data_offset_ + end_offset_) {
        throw std::runtime_error("WAL size changed during recovery");
    }
}

void FileWal::Append(const std::vector<uint8_t>& bytes, size_t limit) {
    const size_t target = std::min(bytes.size(), limit);
    if (end_offset_ > static_cast<size_t>(std::numeric_limits<off_t>::max()) - data_offset_ - target) {
        throw std::overflow_error("WAL append offset overflow");
    }
    size_t done = 0;
    while (done < target) {
        const size_t chunk =
            std::min({target - done, max_write_chunk_, static_cast<size_t>(std::numeric_limits<ssize_t>::max())});
        const auto started = diagnostics_ ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
        const ssize_t written = pwrite(fd_, bytes.data() + done, chunk, static_cast<off_t>(data_offset_ + end_offset_));
        if (diagnostics_) {
            diagnostics_->wal_pwrite_calls.fetch_add(1, std::memory_order_relaxed);
            diagnostics_->wal_pwrite_ns.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started)
                        .count()),
                std::memory_order_relaxed);
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written < 0) {
            ThrowSystemError("append WAL");
        }
        if (written == 0) {
            throw std::runtime_error("zero-length WAL write");
        }
        done += static_cast<size_t>(written);
        end_offset_ += static_cast<size_t>(written);
        if (diagnostics_)
            diagnostics_->wal_pwrite_bytes.fetch_add(static_cast<uint64_t>(written), std::memory_order_relaxed);
        ++write_calls_;
    }
}

void FileWal::Sync() {
    const auto started = diagnostics_ ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    while (fdatasync(fd_) != 0) {
        if (errno != EINTR) {
            ThrowSystemError("sync WAL");
        }
    }
    if (diagnostics_) {
        const uint64_t elapsed = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started).count());
        diagnostics_->wal_fdatasync_calls.fetch_add(1, std::memory_order_relaxed);
        diagnostics_->wal_fdatasync_ns.fetch_add(elapsed, std::memory_order_relaxed);
        RecordMax(diagnostics_->wal_fdatasync_max_ns, elapsed);
    }
    ++sync_calls_;
}

void FileWal::TruncateAndSync(size_t bytes) {
    if (bytes > static_cast<size_t>(std::numeric_limits<off_t>::max()) - data_offset_) {
        throw std::overflow_error("WAL truncate offset overflow");
    }
    if (ftruncate(fd_, static_cast<off_t>(data_offset_ + bytes)) != 0) {
        ThrowSystemError("truncate WAL");
    }
    end_offset_ = bytes;
    Sync();
}

const FileWal::SegmentInfo& FileWal::segment_info() const {
    if (!segment_info_)
        throw std::logic_error("legacy WAL has no segment header");
    return *segment_info_;
}

void FileWal::CloseForTest() {
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

} // namespace epoch_si_poc
