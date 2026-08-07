/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/disk_manager.h"

#include <assert.h>   // for assert
#include <algorithm>
#include <cerrno>     // for errno
#include <dirent.h>
#include <fcntl.h>
#include <functional>
#include <limits>
#include <set>
#include <sys/stat.h> // for stat
#include <unistd.h>   // for lseek

#include "defs.h"
#include "common/fault_injection.h"

namespace {

std::string SegmentedWalName(uint64_t generation, uint64_t segment) {
    return "db.log." + std::to_string(generation) + "." + std::to_string(segment);
}

bool IsRegularSegment(const std::string& name) {
    struct stat st {};
    if (lstat(name.c_str(), &st) != 0) {
        if (errno == ENOENT) return false;
        throw UnixError();
    }
    if (!S_ISREG(st.st_mode)) throw InternalError("segmented WAL entry is not a regular file: " + name);
    return true;
}

int OpenRegularSegment(const std::string& name, bool create, bool* created = nullptr) {
    if (created != nullptr) *created = false;
    constexpr int kFlags = O_RDWR | O_NOFOLLOW | O_CLOEXEC;
    int fd = open(name.c_str(), kFlags);
    if (fd < 0 && errno == ENOENT && create) {
        fd = open(name.c_str(), kFlags | O_CREAT | O_EXCL, 0644);
        if (fd >= 0 && created != nullptr) *created = true;
        if (fd < 0 && errno == EEXIST) fd = open(name.c_str(), kFlags);
    }
    if (fd < 0) throw UnixError();
    struct stat st {};
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
        const int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        throw InternalError("segmented WAL descriptor is not a regular file: " + name);
    }
    return fd;
}

void WritePageAt(int fd, page_id_t page_no, const char* data, int num_bytes) {
    int written = 0;
    const off_t offset = static_cast<off_t>(page_no) * PAGE_SIZE;
    while (written < num_bytes) {
        const ssize_t count = pwrite(fd, data + written, static_cast<size_t>(num_bytes - written), offset + written);
        if (count <= 0) {
            throw InternalError("DiskManager::write_page Error");
        }
        written += static_cast<int>(count);
    }
}

void ReadPageAt(int fd, page_id_t page_no, char* data, int num_bytes) {
    int read_bytes = 0;
    const off_t offset = static_cast<off_t>(page_no) * PAGE_SIZE;
    while (read_bytes < num_bytes) {
        const ssize_t count =
            pread(fd, data + read_bytes, static_cast<size_t>(num_bytes - read_bytes), offset + read_bytes);
        if (count <= 0) {
            throw InternalError("DiskManager::read_page Error");
        }
        read_bytes += static_cast<int>(count);
    }
}

} // namespace

DiskManager::DiskManager() = default;

std::string DiskManager::wal_segment_name(uint64_t segment) const {
    return SegmentedWalName(wal_generation_, segment);
}

void DiskManager::configure_legacy_wal() {
    std::lock_guard<std::mutex> lock(wal_segment_latch_);
    sync_pending_segment_directory_locked();
    wal_segmented_ = false;
    dirty_wal_segments_.clear();
}

void DiskManager::sync_pending_segment_directory_locked() {
    if (pending_segment_directory_sync_.empty()) return;
    FaultInjector::Point("before_wal_segment_directory_sync");
    sync_directory(".");
    pending_segment_directory_sync_.clear();
}

void DiskManager::sync_segmented_wal_directory() {
    std::lock_guard<std::mutex> lock(wal_segment_latch_);
    if (!wal_segmented_) return;
    // Startup has no in-memory evidence about a prior process's O_CREAT, so
    // sync the directory once after validating a v2 root.
    FaultInjector::Point("before_wal_segment_directory_sync");
    sync_directory(".");
    pending_segment_directory_sync_.clear();
}

size_t DiskManager::pending_segment_directory_sync_for_test() const {
    std::lock_guard<std::mutex> lock(wal_segment_latch_);
    return pending_segment_directory_sync_.size();
}

int DiskManager::open_wal_segment_locked(uint64_t segment, bool create) {
    return OpenRegularSegment(wal_segment_name(segment), create);
}

void DiskManager::validate_segmented_layout_locked() const {
    const std::string prefix = "db.log." + std::to_string(wal_generation_) + ".";
    DIR* dir = opendir(".");
    if (dir == nullptr) throw UnixError();
    std::set<uint64_t> segments;
    try {
        while (dirent* entry = readdir(dir)) {
            const std::string name(entry->d_name);
            if (name.rfind(prefix, 0) != 0) continue;
            const std::string suffix = name.substr(prefix.size());
            if (suffix.empty() || suffix.find_first_not_of("0123456789") != std::string::npos) {
                throw InternalError("malformed segmented WAL filename: " + name);
            }
            const uint64_t number = std::stoull(suffix);
            if (wal_segment_name(number) != name || !segments.insert(number).second) {
                throw InternalError("ambiguous segmented WAL filename: " + name);
            }
        }
        if (closedir(dir) != 0) throw UnixError();
        dir = nullptr;
    } catch (...) {
        if (dir != nullptr) closedir(dir);
        throw;
    }
    if (segments.empty()) return;
    const uint64_t first = *segments.begin();
    if (first > wal_restart_segment_) {
        throw InternalError("segmented WAL is missing its manifest restart prefix");
    }
    uint64_t expected = first;
    for (uint64_t segment : segments) {
        if (segment != expected++) throw InternalError("segmented WAL has a missing segment");
        struct stat st {};
        const std::string name = wal_segment_name(segment);
        if (lstat(name.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) throw InternalError("invalid segmented WAL entry");
        const bool last = segment == *segments.rbegin();
        if (st.st_size < 0 || (!last && st.st_size != wal_segment_bytes_) ||
            (last && st.st_size > wal_segment_bytes_)) {
            throw InternalError("segmented WAL has an invalid segment length");
        }
    }
}

int64_t DiskManager::segmented_log_size_locked() const {
    validate_segmented_layout_locked();
    uint64_t highest = wal_restart_segment_;
    bool found = false;
    DIR* dir = opendir(".");
    if (dir == nullptr) throw UnixError();
    const std::string prefix = "db.log." + std::to_string(wal_generation_) + ".";
    try {
        while (dirent* entry = readdir(dir)) {
            const std::string name(entry->d_name);
            if (name.rfind(prefix, 0) != 0) continue;
            const std::string suffix = name.substr(prefix.size());
            if (suffix.empty() || suffix.find_first_not_of("0123456789") != std::string::npos) continue;
            const uint64_t segment = static_cast<uint64_t>(std::stoull(suffix));
            highest = std::max(highest, segment);
            found = true;
        }
        if (closedir(dir) != 0) throw UnixError();
        dir = nullptr;
    } catch (...) {
        if (dir != nullptr) closedir(dir);
        throw;
    }
    if (!found) return static_cast<int64_t>(wal_restart_segment_) * wal_segment_bytes_;
    const std::string name = wal_segment_name(highest);
    struct stat st {};
    if (lstat(name.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) throw InternalError("invalid segmented WAL entry");
    if (highest > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) /
                      static_cast<uint64_t>(wal_segment_bytes_) ||
        st.st_size > std::numeric_limits<int64_t>::max() -
                         static_cast<int64_t>(highest * static_cast<uint64_t>(wal_segment_bytes_))) {
        throw InternalError("segmented WAL logical offset overflow");
    }
    return static_cast<int64_t>(highest * static_cast<uint64_t>(wal_segment_bytes_)) + st.st_size;
}

void DiskManager::configure_segmented_wal(uint64_t generation, uint64_t restart_segment, int64_t segment_bytes) {
    if (segment_bytes <= 0 || segment_bytes > kWalSegmentBytes) {
        throw InternalError("invalid segmented WAL size");
    }
    if (restart_segment > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) /
                              static_cast<uint64_t>(segment_bytes)) {
        throw InternalError("segmented WAL restart offset overflow");
    }
    std::lock_guard<std::mutex> lock(wal_segment_latch_);
    wal_segmented_ = true;
    wal_generation_ = generation;
    wal_restart_segment_ = restart_segment;
    wal_segment_bytes_ = segment_bytes;
    dirty_wal_segments_.clear();
    sync_pending_segment_directory_locked();
    log_offset_ = segmented_log_size_locked();
}

void DiskManager::ensure_segmented_wal_root() {
    std::lock_guard<std::mutex> lock(wal_segment_latch_);
    if (!wal_segmented_) throw InternalError("cannot create a legacy WAL segment");
    const std::string name = wal_segment_name(wal_restart_segment_);
    if (IsRegularSegment(name)) {
        sync_pending_segment_directory_locked();
        return;
    }
    bool created = false;
    const int fd = OpenRegularSegment(name, true, &created);
    if (created) pending_segment_directory_sync_.insert(wal_restart_segment_);
    if (close(fd) != 0) throw UnixError();
    sync_pending_segment_directory_locked();
}

/**
 * @description: 将数据写入文件的指定磁盘页面中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 写入目标页面的page_id
 * @param {char} *offset 要写入磁盘的数据
 * @param {int} num_bytes 要写入磁盘的数据大小
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char* offset, int num_bytes) {
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用write()函数
    // 注意write返回值与num_bytes不等时 throw InternalError("DiskManager::write_page Error");
    if (fd2path_.count(fd) == 0) {
        throw FileNotOpenError(fd);
    }
    WritePageAt(fd, page_no, offset, num_bytes);
}

/**
 * @description: 读取文件中指定编号的页面中的部分数据到内存中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 指定的页面编号
 * @param {char} *offset 读取的内容写入到offset中
 * @param {int} num_bytes 读取的数据量大小
 */
void DiskManager::read_page(int fd, page_id_t page_no, char* offset, int num_bytes) {
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用read()函数
    // 注意read返回值与num_bytes不等时，throw InternalError("DiskManager::read_page Error");
    if (fd2path_.count(fd) == 0) {
        throw FileNotOpenError(fd);
    }
    ReadPageAt(fd, page_no, offset, num_bytes);
}

/**
 * @description: 分配一个新的页号
 * @return {page_id_t} 分配的新页号
 * @param {int} fd 指定文件的文件句柄
 */
page_id_t DiskManager::allocate_page(int fd) {
    // 简单的自增分配策略，指定文件的页面编号加1
    assert(fd >= 0 && fd < MAX_FD);
    return fd2pageno_[fd]++;
}

void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager::create_dir(const std::string& path) {
    // Create a subdirectory
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) { // 创建一个名为path的目录
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string& path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 判断指定路径文件是否存在
 * @return {bool} 若指定路径文件存在则返回true
 * @param {string} &path 指定路径文件
 */
bool DiskManager::is_file(const std::string& path) {
    // 用struct stat获取文件信息
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

/**
 * @description: 用于创建指定路径文件
 * @return {*}
 * @param {string} &path
 */
void DiskManager::create_file(const std::string& path) {
    // Todo:
    // 调用open()函数，使用O_CREAT模式
    // 注意不能重复创建相同文件
    if (is_file(path)) {
        throw FileExistsError(path);
    }
    int fd = open(path.c_str(), O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        throw UnixError();
    }
    if (fd >= MAX_FD) {
        close(fd);
        unlink(path.c_str());
        throw InternalError("DiskManager::create_file Error");
    }
    close(fd);
}

/**
 * @description: 删除指定路径的文件
 * @param {string} &path 文件所在路径
 */
void DiskManager::destroy_file(const std::string& path) {
    // Todo:
    // 调用unlink()函数
    // 注意不能删除未关闭的文件
    if (path2fd_.count(path)) {
        throw FileNotClosedError(path);
    }
    if (is_file(path) == 0) {
        throw FileNotFoundError(path);
    }
    if (unlink(path.c_str()) < 0) {
        throw UnixError();
    }
    int fd = path2fd_[path];
    path2fd_.erase(path);
    fd2path_.erase(fd);
    fd2pageno_[fd] = 0;
}

/**
 * @description: 打开指定路径文件
 * @return {int} 返回打开的文件的文件句柄
 * @param {string} &path 文件所在路径
 */
int DiskManager::open_file(const std::string& path) {
    // Todo:
    // 调用open()函数，使用O_RDWR模式
    // 注意不能重复打开相同文件，并且需要更新文件打开列表
    if (is_file(path) == 0) {
        throw FileNotFoundError(path);
    }
    int fd = -1;
    if (path2fd_.find(path) == path2fd_.end()) {
        fd = open(path.c_str(), O_RDWR);
        path2fd_[path] = fd;
        fd2path_[fd] = path;
    } else {
        return path2fd_[path];
    }
    return fd;
}

/**
 * @description:用于关闭指定路径文件
 * @param {int} fd 打开的文件的文件句柄
 */
void DiskManager::close_file(int fd) {
    // Todo:
    // 调用close()函数
    // 注意不能关闭未打开的文件，并且需要更新文件打开列表
    if (fd2path_.find(fd) != fd2path_.end()) {
        close(fd);
        const std::string path = fd2path_[fd];
        path2fd_.erase(path);
        fd2path_.erase(fd);
    } else {
        throw FileNotOpenError(fd);
    }
}

/**
 * @description: 获得文件的大小
 * @return {int64_t} 文件的大小
 * @param {string} &file_name 文件名
 */
int64_t DiskManager::get_file_size(const std::string& file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? static_cast<int64_t>(stat_buf.st_size) : -1;
}

int64_t DiskManager::get_file_size(int fd) {
    struct stat stat_buf;
    while (fstat(fd, &stat_buf) != 0) {
        const int fstat_errno = errno;
        if (fstat_errno == EINTR) {
            continue;
        }
        throw InternalError("DiskManager::get_file_size(fstat) failed for fd " + std::to_string(fd) + ": " +
                            std::strerror(fstat_errno));
    }
    if (stat_buf.st_size < 0 ||
        static_cast<uintmax_t>(stat_buf.st_size) > static_cast<uintmax_t>(std::numeric_limits<int64_t>::max())) {
        throw InternalError("DiskManager::get_file_size(fstat) returned an invalid size for fd " + std::to_string(fd));
    }
    return static_cast<int64_t>(stat_buf.st_size);
}

/**
 * @description: 根据文件句柄获得文件名
 * @return {string} 文件句柄对应文件的文件名
 * @param {int} fd 文件句柄
 */
std::string DiskManager::get_file_name(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}

/**
 * @description:  获得文件名对应的文件句柄
 * @return {int} 文件句柄
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_fd(const std::string& file_name) {
    if (!path2fd_.count(file_name)) {
        return open_file(file_name);
    }
    return path2fd_[file_name];
}

void DiskManager::open_log_fd() {
    if (log_fd_ != -1) {
        return;
    }

    int local_fd = -1;
    bool opened_here = false;
    bool path_registered = false;
    bool fd_registered = false;
    try {
        const auto existing = path2fd_.find(LOG_FILE_NAME);
        if (existing != path2fd_.end()) {
            local_fd = existing->second;
        } else {
            if (!is_file(LOG_FILE_NAME)) {
                throw FileNotFoundError(LOG_FILE_NAME);
            }
            local_fd = open(LOG_FILE_NAME.c_str(), O_RDWR);
            if (local_fd < 0) {
                throw UnixError();
            }
            opened_here = true;
        }

        // Keep both published WAL fields and the fd bookkeeping untouched until
        // the descriptor has yielded a valid append offset.
        const int64_t local_offset = get_file_size(local_fd);
        if (opened_here) {
            if (!path2fd_.emplace(LOG_FILE_NAME, local_fd).second) {
                throw InternalError("DiskManager::open_log_fd found duplicate WAL path bookkeeping");
            }
            path_registered = true;
            if (!fd2path_.emplace(local_fd, LOG_FILE_NAME).second) {
                throw InternalError("DiskManager::open_log_fd found duplicate WAL descriptor bookkeeping");
            }
            fd_registered = true;
        }
        log_fd_ = local_fd;
        log_offset_ = local_offset;
    } catch (...) {
        if (fd_registered) {
            fd2path_.erase(local_fd);
        }
        if (path_registered) {
            path2fd_.erase(LOG_FILE_NAME);
        }
        if (opened_here) {
            const int original_errno = errno;
            (void)close(local_fd);
            errno = original_errno;
        }
        throw;
    }
}

int64_t DiskManager::get_log_file_size() {
    std::lock_guard<std::mutex> segmented_lock(wal_segment_latch_);
    if (wal_segmented_) {
        return log_offset_;
    }
    open_log_fd();
    if (log_fd_ < 0) {
        throw InternalError("DiskManager::get_log_file_size has an invalid WAL descriptor: " +
                            std::to_string(log_fd_));
    }
    return get_file_size(log_fd_);
}

/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int64_t} offset 读取的内容在文件中的位置
 */
int DiskManager::read_log(char* log_data, int size, int64_t offset) {
    if (wal_is_segmented()) {
        return read_log_chunk(log_data, size, offset);
    }
    // read log file from the previous end
    open_log_fd();
    const int64_t file_size = get_log_file_size();
    if (offset > file_size) {
        return -1;
    }

    int64_t readable = file_size - offset;
    size = static_cast<int>(std::min<int64_t>(size, readable));
    if (size == 0) {
        return 0;
    }
    // A short pread must never be reported as a short log: callers treat that
    // as end-of-log and would silently drop committed records. The requested
    // range was just measured against the file size, so it is fully readable.
    int bytes_read = 0;
    while (bytes_read < size) {
        const ssize_t count =
            pread(log_fd_, log_data + bytes_read, static_cast<size_t>(size - bytes_read), offset + bytes_read);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw UnixError();
        }
        if (count == 0) {
            throw InternalError("DiskManager::read_log truncated read");
        }
        bytes_read += static_cast<int>(count);
    }
    log_read_count_.fetch_add(1, std::memory_order_relaxed);
    log_read_bytes_.fetch_add(static_cast<uint64_t>(bytes_read), std::memory_order_relaxed);
    return bytes_read;
}

int DiskManager::read_log_chunk(char* log_data, int size, int64_t offset) {
    if (size <= 0 || offset < 0) {
        return 0;
    }
    uint64_t generation = 0;
    int64_t segment_bytes = 0;
    int64_t file_size = 0;
    {
        std::lock_guard<std::mutex> segmented_lock(wal_segment_latch_);
        if (wal_segmented_) {
            generation = wal_generation_;
            segment_bytes = wal_segment_bytes_;
            file_size = log_offset_;
        }
    }
    if (segment_bytes != 0) {
        if (offset >= file_size) return 0;
        const int wanted = static_cast<int>(std::min<int64_t>(size, file_size - offset));
        int bytes_read = 0;
        while (bytes_read < wanted) {
            const int64_t logical = offset + bytes_read;
            const uint64_t segment = static_cast<uint64_t>(logical / segment_bytes);
            const off_t segment_offset = static_cast<off_t>(logical % segment_bytes);
            const int chunk = static_cast<int>(std::min<int64_t>(wanted - bytes_read, segment_bytes - segment_offset));
            const int fd = OpenRegularSegment(SegmentedWalName(generation, segment), false);
            int chunk_read = 0;
            while (chunk_read < chunk) {
                const ssize_t got = pread(fd, log_data + bytes_read + chunk_read,
                                          static_cast<size_t>(chunk - chunk_read), segment_offset + chunk_read);
                if (got < 0 && errno == EINTR) continue;
                if (got < 0) {
                    const int saved_errno = errno;
                    close(fd);
                    errno = saved_errno;
                    throw UnixError();
                }
                if (got == 0) {
                    close(fd);
                    throw InternalError("unexpected EOF inside segmented WAL");
                }
                chunk_read += static_cast<int>(got);
            }
            if (close(fd) != 0) throw UnixError();
            bytes_read += chunk;
        }
        log_read_count_.fetch_add(1, std::memory_order_relaxed);
        log_read_bytes_.fetch_add(static_cast<uint64_t>(bytes_read), std::memory_order_relaxed);
        return bytes_read;
    }
    open_log_fd();
    int bytes_read = 0;
    while (bytes_read < size) {
        const ssize_t count =
            pread(log_fd_, log_data + bytes_read, static_cast<size_t>(size - bytes_read), offset + bytes_read);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw UnixError();
        }
        if (count == 0) {
            // Genuine end of file. The caller bounds its own scan, so a short
            // result here is information, not a lost record.
            break;
        }
        bytes_read += static_cast<int>(count);
    }
    log_read_count_.fetch_add(1, std::memory_order_relaxed);
    log_read_bytes_.fetch_add(static_cast<uint64_t>(bytes_read), std::memory_order_relaxed);
    return bytes_read;
}

/**
 * @description: 写日志内容
 * @param {char} *log_data 要写入的日志内容
 * @param {int} size 要写入的内容大小
 */
void DiskManager::write_log(char* log_data, int size) {
    std::unique_lock<std::mutex> segmented_lock(wal_segment_latch_);
    if (wal_segmented_) {
        if (size < 0) throw InternalError("negative WAL write size");
        FaultInjector::Point("during_wal_pwrite");
        const int64_t begin_offset = log_offset_;
        if (size > std::numeric_limits<int64_t>::max() - begin_offset) {
            throw InternalError("segmented WAL append offset overflow");
        }
        int written = 0;
        while (written < size) {
            const int64_t logical = begin_offset + written;
            const uint64_t segment = static_cast<uint64_t>(logical / wal_segment_bytes_);
            const off_t segment_offset = static_cast<off_t>(logical % wal_segment_bytes_);
            const int chunk = static_cast<int>(std::min<int64_t>(size - written, wal_segment_bytes_ - segment_offset));
            const std::string name = wal_segment_name(segment);
            bool created = false;
            const int fd = OpenRegularSegment(name, true, &created);
            if (created) pending_segment_directory_sync_.insert(segment);
            if (created) {
                try {
                    FaultInjector::Point("after_wal_segment_create");
                } catch (...) {
                    close(fd);
                    throw;
                }
            }
            int chunk_written = 0;
            while (chunk_written < chunk) {
                const ssize_t count = pwrite(fd, log_data + written + chunk_written,
                                             static_cast<size_t>(chunk - chunk_written), segment_offset + chunk_written);
                if (count < 0 && errno == EINTR) continue;
                if (count <= 0) {
                    const int saved_errno = errno;
                    close(fd);
                    errno = saved_errno;
                    throw UnixError();
                }
                chunk_written += static_cast<int>(count);
            }
            if (close(fd) != 0) throw UnixError();
            // A segment's directory entry must be durable before any COMMIT
            // can rely on bytes written to it. If a previous attempt created
            // it but failed later, the pending obligation survives and is
            // discharged here even though this retry did not create it.
            sync_pending_segment_directory_locked();
            dirty_wal_segments_.insert(segment);
            written += chunk;
        }
        log_offset_ = begin_offset + size;
        return;
    }
    segmented_lock.unlock();
    open_log_fd();

    // write from the file_end
    FaultInjector::Point("during_wal_pwrite");
    const int64_t begin_offset = log_offset_;
    int bytes_write = 0;
    while (bytes_write < size) {
        const ssize_t count = pwrite(log_fd_, log_data + bytes_write, static_cast<size_t>(size - bytes_write),
                                     static_cast<off_t>(begin_offset + bytes_write));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            // log_offset_ still holds the entry value, so a retry rewrites this
            // buffer from the same offset instead of leaving a hole. Nothing in
            // this range has been fsynced yet, so overwriting is safe.
            throw UnixError();
        }
        bytes_write += static_cast<int>(count);
    }
    log_offset_ = begin_offset + size;
}

void DiskManager::fsync_log() {
    std::unique_lock<std::mutex> segmented_lock(wal_segment_latch_);
    if (wal_segmented_) {
        FaultInjector::Point("before_wal_fsync");
        for (uint64_t segment : dirty_wal_segments_) {
            const int fd = open_wal_segment_locked(segment, false);
            const int result = fdatasync(fd);
            const int saved_errno = errno;
            close(fd);
            errno = saved_errno;
            if (result != 0) throw UnixError();
        }
        dirty_wal_segments_.clear();
        FaultInjector::Point("after_wal_fsync");
        return;
    }
    segmented_lock.unlock();
    FaultInjector::Point("before_wal_fsync");
    if (log_fd_ != -1 && fdatasync(log_fd_) != 0) {
        throw UnixError();
    }
    FaultInjector::Point("after_wal_fsync");
}

void DiskManager::sync_file(int fd) {
    FaultInjector::Point("before_data_fsync");
    if (fd < 0) {
        errno = EBADF;
        throw UnixError();
    }
    for (;;) {
        const int result = fdatasync(fd);
        if (result == 0) {
            return;
        }
        if (errno != EINTR) {
            throw UnixError();
        }
    }
}

void DiskManager::sync_path(const std::string& path) {
    int fd = open(path.c_str(), O_RDWR);
    if (fd < 0) {
        throw UnixError();
    }
    try {
        sync_file(fd);
    } catch (...) {
        close(fd);
        throw;
    }
    if (close(fd) != 0) {
        throw UnixError();
    }
}

void DiskManager::sync_directory(const std::string& path) {
    FaultInjector::Point("before_directory_fsync");
    int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        throw UnixError();
    }
    if (fsync(fd) != 0) {
        close(fd);
        throw UnixError();
    }
    if (close(fd) != 0) {
        throw UnixError();
    }
}

void DiskManager::truncate_log() {
    if (wal_is_segmented()) {
        // Batch 1 deliberately does not reclaim or switch generations. A
        // segmented clean-reset must publish a new v2 manifest before any old
        // segment can be removed, which is Batch 2's responsibility.
        throw InternalError("segmented WAL reset requires manifest-backed generation switch");
    }
    open_log_fd();
    if (log_fd_ != -1) {
        if (ftruncate(log_fd_, 0) != 0) {
            throw UnixError();
        }
        FaultInjector::Point("after_wal_ftruncate");
        // final.md tests same-machine SIGKILL, which preserves the in-kernel
        // zero length. The first post-checkpoint WAL fdatasync also persists
        // this size change before acknowledging its COMMIT.
        if (lseek(log_fd_, 0, SEEK_SET) < 0) {
            throw UnixError();
        }
    }
    log_offset_ = 0;
}

void DiskManager::truncate_log_to(int64_t offset) {
    if (offset < 0) {
        throw InternalError("negative WAL truncation offset");
    }
    if (wal_is_segmented()) {
        std::lock_guard<std::mutex> lock(wal_segment_latch_);
        const int64_t end = log_offset_;
        if (offset > end) throw InternalError("segmented WAL truncation past end");
        if (offset == end) return;
        // Validate the complete pre-crash layout before deleting or truncating
        // anything. This path is only for discarding a torn tail discovered by
        // startup, never for checkpoint reclaim.
        validate_segmented_layout_locked();
        const uint64_t segment = static_cast<uint64_t>(offset / wal_segment_bytes_);
        const off_t segment_offset = static_cast<off_t>(offset % wal_segment_bytes_);
        // At an exact boundary the next segment belongs entirely to the torn
        // tail. Delete it (and successors), retain the preceding full segment,
        // and let the next append create a fresh boundary segment.
        const bool boundary_is_manifest_root = segment_offset == 0 && segment == wal_restart_segment_;
        const uint64_t first_deleted = segment_offset == 0 && !boundary_is_manifest_root ? segment : segment + 1;
        std::vector<uint64_t> later_segments;
        const std::string prefix = "db.log." + std::to_string(wal_generation_) + ".";
        DIR* dir = opendir(".");
        if (dir == nullptr) throw UnixError();
        try {
            while (dirent* entry = readdir(dir)) {
                const std::string name(entry->d_name);
                if (name.rfind(prefix, 0) != 0) continue;
                const std::string suffix = name.substr(prefix.size());
                if (suffix.empty() || suffix.find_first_not_of("0123456789") != std::string::npos) {
                    throw InternalError("malformed segmented WAL filename: " + name);
                }
                const uint64_t candidate = static_cast<uint64_t>(std::stoull(suffix));
                if (candidate >= first_deleted) later_segments.push_back(candidate);
            }
            if (closedir(dir) != 0) throw UnixError();
            dir = nullptr;
        } catch (...) {
            if (dir != nullptr) closedir(dir);
            throw;
        }
        std::sort(later_segments.begin(), later_segments.end(), std::greater<uint64_t>());
        for (uint64_t later : later_segments) {
            if (unlink(wal_segment_name(later).c_str()) != 0) throw UnixError();
            dirty_wal_segments_.erase(later);
        }
        if (!later_segments.empty()) sync_directory(".");
        if (segment_offset != 0 || boundary_is_manifest_root) {
            const int fd = open_wal_segment_locked(segment, false);
            if (ftruncate(fd, segment_offset) != 0 || fdatasync(fd) != 0) {
                const int saved_errno = errno;
                close(fd);
                errno = saved_errno;
                throw UnixError();
            }
            if (close(fd) != 0) throw UnixError();
            dirty_wal_segments_.erase(segment);
            FaultInjector::Point("after_wal_ftruncate");
        }
        log_offset_ = offset;
        return;
    }
    open_log_fd();
    if (ftruncate(log_fd_, static_cast<off_t>(offset)) != 0) {
        throw UnixError();
    }
    if (fdatasync(log_fd_) != 0) {
        throw UnixError();
    }
    if (lseek(log_fd_, static_cast<off_t>(offset), SEEK_SET) < 0) {
        throw UnixError();
    }
    log_offset_ = offset;
}
