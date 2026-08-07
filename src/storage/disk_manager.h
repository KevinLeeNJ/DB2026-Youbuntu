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

#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "errors.h"

class DiskManager;

struct WalSnapshotAccess {
    bool copied{false};
    uint32_t copied_bytes{0};
};

// Recovery-lifetime immutable view of one finalized logical WAL prefix. The
// mappings are shared read-only; only records crossing physical segments copy
// into caller-owned scratch space.
class WalReadSnapshot {
public:
    ~WalReadSnapshot();
    WalReadSnapshot(const WalReadSnapshot&) = delete;
    WalReadSnapshot& operator=(const WalReadSnapshot&) = delete;

    const char* record_bytes(int64_t offset, uint32_t length, std::vector<char>* scratch,
                             WalSnapshotAccess* access = nullptr) const;
    int64_t begin_offset() const noexcept { return begin_offset_; }
    int64_t end_offset() const noexcept { return end_offset_; }

private:
    friend class DiskManager;
    struct Span {
        int fd{-1};
        void* mapping{nullptr};
        size_t mapping_length{0};
        int64_t mapping_logical_begin{0};
        int64_t range_begin{0};
        int64_t range_end{0};
    };

    WalReadSnapshot(int64_t begin_offset, int64_t end_offset)
        : begin_offset_(begin_offset), end_offset_(end_offset) {}
    void add_span(Span span) { spans_.push_back(span); }

    int64_t begin_offset_{0};
    int64_t end_offset_{0};
    std::vector<Span> spans_;
};

/**
 * @description: DiskManager的作用主要是根据上层的需要对磁盘文件进行操作
 */
class DiskManager {
public:
    static constexpr int64_t kWalSegmentBytes = 64LL * 1024 * 1024;
    explicit DiskManager();

    ~DiskManager() = default;

    void write_page(int fd, page_id_t page_no, const char* offset, int num_bytes);

    void read_page(int fd, page_id_t page_no, char* offset, int num_bytes);

    page_id_t allocate_page(int fd);

    void deallocate_page(page_id_t page_id);

    /*目录操作*/
    bool is_dir(const std::string& path);

    void create_dir(const std::string& path);

    void destroy_dir(const std::string& path);

    /*文件操作*/
    bool is_file(const std::string& path);

    void create_file(const std::string& path);

    void destroy_file(const std::string& path);

    int open_file(const std::string& path);

    void close_file(int fd);

    int64_t get_file_size(const std::string& file_name);

    // Query the size of an already-open descriptor without resolving its path
    // again. This is used by recovery so a transient pathname failure cannot be
    // misclassified as a malformed record-file tail.
    int64_t get_file_size(int fd);

    // Return the size of the established WAL descriptor. Durable WAL callers
    // must use this rather than resolving LOG_FILE_NAME again.
    int64_t get_log_file_size();

    // The segmented layout exposes one continuous logical WAL byte stream.
    // Segment names are generation-scoped (`db.log.<generation>.<segment>`),
    // while callers continue to use logical offsets exactly as they did for
    // legacy db.log.  The test-only size parameter must divide neither record
    // semantics nor production layout; production always uses kWalSegmentBytes.
    void configure_segmented_wal(uint64_t generation, uint64_t restart_segment = 0,
                                 int64_t segment_bytes = kWalSegmentBytes);
    void configure_legacy_wal();
    bool wal_is_segmented() const noexcept { return wal_segmented_; }
    uint64_t wal_generation() const noexcept { return wal_generation_; }
    int64_t wal_segment_bytes() const noexcept { return wal_segment_bytes_; }
    std::string wal_segment_name(uint64_t segment) const;
    void ensure_segmented_wal_root();
    void sync_segmented_wal_directory();
    size_t pending_segment_directory_sync_for_test() const;

    std::string get_file_name(int fd);

    int get_file_fd(const std::string& file_name);

    /*日志操作*/
    int read_log(char* log_data, int size, int64_t offset);

    // Bulk WAL read for the streaming recovery scan. Unlike read_log it does
    // not stat the file per call and does not touch the append offset: the
    // caller owns the scan bound. Returns the number of bytes read, which is
    // short only at end of file.
    int read_log_chunk(char* log_data, int size, int64_t offset);

    // Must be created after recovery analyze succeeded and startup finalize
    // truncated any physical tail. The returned object pins read-only inode
    // mappings until every consumer has joined.
    std::unique_ptr<WalReadSnapshot> create_wal_read_snapshot(int64_t begin_offset, int64_t end_offset);

    void write_log(char* log_data, int size);

    void fsync_log();

    // Flush a data or metadata file so a checkpoint can establish a durable
    // WAL -> data -> metadata ordering.
    void sync_file(int fd);
    void sync_path(const std::string& path);
    void sync_directory(const std::string& path);

    // 将日志文件截断为空，并把追加偏移归零。recovery/checkpoint 完成后调用，
    // 消除已处理完毕的 loser 日志，避免跨轮 recovery 重复 undo 同 RID 上的数据。
    void truncate_log();
    // Drop an incomplete WAL tail while preserving the complete prefix.
    void truncate_log_to(int64_t offset);

    void SetLogOffset(int64_t log_offset) {
        log_offset_ = log_offset;
    }

    void SetLogFd(int log_fd) {
        log_fd_ = log_fd;
    }

    int GetLogFd() {
        return log_fd_;
    }

    /**
     * @description: 设置文件已经分配的页面个数
     * @param {int} fd 文件对应的文件句柄
     * @param {int} start_page_no 已经分配的页面个数，即文件接下来从start_page_no开始分配页面编号
     */
    void set_fd2pageno(int fd, int start_page_no) {
        fd2pageno_[fd] = start_page_no;
    }

    uint64_t get_log_read_count() const {
        return log_read_count_.load(std::memory_order_relaxed);
    }
    uint64_t get_log_read_bytes() const {
        return log_read_bytes_.load(std::memory_order_relaxed);
    }

    /**
     * @description: 获得文件目前已分配的页面个数，即如果文件要分配一个新页面，需要从fd2pagenp_[fd]开始分配
     * @return {page_id_t} 已分配的页面个数
     * @param {int} fd 文件对应的句柄
     */
    page_id_t get_fd2pageno(int fd) {
        return fd2pageno_[fd];
    }

    static constexpr int MAX_FD = 8192;

private:
    // Opens the WAL lazily. The append offset is initialized here, together
    // with the descriptor, so that "log_fd_ != -1" always implies "log_offset_
    // is the real append offset". read_log used to reassign log_offset_ on
    // every call, which left correctness depending on the order in which
    // readers and writers happened to run.
    void open_log_fd();
    int64_t segmented_log_size_locked() const;
    int open_wal_segment_locked(uint64_t segment, bool create);
    void validate_segmented_layout_locked() const;
    void sync_pending_segment_directory_locked();

    // 文件打开列表，用于记录文件是否被打开
    std::unordered_map<std::string, int> path2fd_; //<Page文件磁盘路径,Page fd>哈希表
    std::unordered_map<int, std::string> fd2path_; //<Page fd,Page文件磁盘路径>哈希表

    int log_fd_ = -1;        // WAL日志文件的文件句柄，默认为-1，代表未打开日志文件
    int64_t log_offset_ = 0; // 日志文件追加偏移
    mutable std::mutex wal_segment_latch_;
    bool wal_segmented_{false};
    uint64_t wal_generation_{0};
    uint64_t wal_restart_segment_{0};
    int64_t wal_segment_bytes_{kWalSegmentBytes};
    std::unordered_set<uint64_t> dirty_wal_segments_;
    // A successful O_CREAT is not durable until its parent directory is
    // synced. Keep this obligation across write/close/fsync failures so a
    // retry cannot mistake an existing pathname for a durable one.
    std::unordered_set<uint64_t> pending_segment_directory_sync_;
    std::atomic<uint64_t> log_read_count_{0};
    std::atomic<uint64_t> log_read_bytes_{0};
    std::atomic<page_id_t> fd2pageno_[MAX_FD]{}; // 文件中已经分配的页面个数，初始值为0
};
