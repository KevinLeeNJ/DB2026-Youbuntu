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
#include <cerrno>     // for errno
#include <sys/stat.h> // for stat
#include <unistd.h>   // for lseek

#include "defs.h"
#include "common/fault_injection.h"

namespace {

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
    page_write_count_.fetch_add(1, std::memory_order_relaxed);
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
    page_read_count_.fetch_add(1, std::memory_order_relaxed);
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
    log_fd_ = open_file(LOG_FILE_NAME);
    log_offset_ = get_file_size(LOG_FILE_NAME);
    if (log_offset_ < 0) {
        log_offset_ = 0;
    }
}

/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int64_t} offset 读取的内容在文件中的位置
 */
int DiskManager::read_log(char* log_data, int size, int64_t offset) {
    // read log file from the previous end
    open_log_fd();
    int64_t file_size = get_file_size(LOG_FILE_NAME);
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
    FaultInjector::Point("before_wal_fsync");
    if (log_fd_ != -1 && fdatasync(log_fd_) != 0) {
        throw UnixError();
    }
    FaultInjector::Point("after_wal_fsync");
}

void DiskManager::sync_file(int fd) {
    FaultInjector::Point("before_data_fsync");
    if (fd < 0 || fdatasync(fd) != 0) {
        throw UnixError();
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
    open_log_fd();
    if (log_fd_ != -1) {
        if (ftruncate(log_fd_, 0) != 0) {
            throw UnixError();
        }
        if (fdatasync(log_fd_) != 0) {
            throw UnixError();
        }
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
