#include "file_wal.h"

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <sys/stat.h>
#include <unistd.h>

namespace epoch_si_poc {
namespace {

void ThrowSystemError(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
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

FileWal::~FileWal() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

FileWal::FileWal(FileWal&& other) noexcept
    : fd_(other.fd_), end_offset_(other.end_offset_), max_write_chunk_(other.max_write_chunk_),
      write_calls_(other.write_calls_), sync_calls_(other.sync_calls_) {
    other.fd_ = -1;
    other.end_offset_ = 0;
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
    max_write_chunk_ = other.max_write_chunk_;
    write_calls_ = other.write_calls_;
    sync_calls_ = other.sync_calls_;
    other.fd_ = -1;
    other.end_offset_ = 0;
    return *this;
}

std::vector<uint8_t> FileWal::ReadAll() const {
    std::vector<uint8_t> bytes(end_offset_);
    size_t done = 0;
    while (done < bytes.size()) {
        const ssize_t read_bytes = pread(fd_, bytes.data() + done, bytes.size() - done, static_cast<off_t>(done));
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
    return bytes;
}

void FileWal::Append(const std::vector<uint8_t>& bytes, size_t limit) {
    const size_t target = std::min(bytes.size(), limit);
    if (end_offset_ > static_cast<size_t>(std::numeric_limits<off_t>::max()) - target) {
        throw std::overflow_error("WAL append offset overflow");
    }
    size_t done = 0;
    while (done < target) {
        const size_t chunk =
            std::min({target - done, max_write_chunk_, static_cast<size_t>(std::numeric_limits<ssize_t>::max())});
        const ssize_t written = pwrite(fd_, bytes.data() + done, chunk, static_cast<off_t>(end_offset_));
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
        ++write_calls_;
    }
}

void FileWal::Sync() {
    while (fdatasync(fd_) != 0) {
        if (errno != EINTR) {
            ThrowSystemError("sync WAL");
        }
    }
    ++sync_calls_;
}

void FileWal::TruncateAndSync(size_t bytes) {
    if (bytes > static_cast<size_t>(std::numeric_limits<off_t>::max())) {
        throw std::overflow_error("WAL truncate offset overflow");
    }
    if (ftruncate(fd_, static_cast<off_t>(bytes)) != 0) {
        ThrowSystemError("truncate WAL");
    }
    end_offset_ = bytes;
    Sync();
}

void FileWal::CloseForTest() {
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

} // namespace epoch_si_poc
