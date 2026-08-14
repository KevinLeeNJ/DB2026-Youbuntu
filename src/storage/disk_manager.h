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
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
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
    // Drops file-backed PTEs without changing the immutable mapping. Future
    // record_bytes() calls fault the same bytes back from the page cache.
    bool discard_resident_pages() const noexcept;
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
    enum class FileOperationForTest { Read, Write, Sync, Claim, CloseAfterSyscall };
    struct FileIdentityForTest { int fd; uint64_t generation; };
    class FileLease;
    class FileWriteClaim;
    static constexpr int64_t kWalSegmentBytes = 64LL * 1024 * 1024;
    explicit DiskManager();

    ~DiskManager();

    FileWriteClaim acquire_file_write_claim(int fd);
    // Test-only lifecycle barriers are stored in RegistryState, so a token
    // released after its manager has gone away cannot dereference the manager.
    void set_file_lifecycle_test_hooks(std::function<void()> before_open, std::function<void()> before_close);
    void set_file_operation_test_hook(std::function<void(FileOperationForTest)> hook);
    void set_file_operation_entry_test_hook(std::function<void(FileOperationForTest)> hook);
    // Test-only segmented-WAL read barrier. The caller must install or clear
    // it only while this manager has no concurrent operations.
    using SegmentedReadTestHook = void (*)(void*);
    void set_segmented_read_test_hook(SegmentedReadTestHook hook, void* context);
    void set_wal_lease_test_hook(std::function<void()> hook);
    FileIdentityForTest capture_file_identity_for_test(int fd);
    bool stale_file_identity_rejected_for_test(FileIdentityForTest identity);

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

    void SetLogOffset(int64_t log_offset);

    void SetLogFd(int log_fd);
    int GetLogFd();

    /**
     * @description: 设置文件已经分配的页面个数
     * @param {int} fd 文件对应的文件句柄
     * @param {int} start_page_no 已经分配的页面个数，即文件接下来从start_page_no开始分配页面编号
     */
    void set_fd2pageno(int fd, int start_page_no);

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
    page_id_t get_fd2pageno(int fd);

    static constexpr int MAX_FD = 8192;

private:
    enum class FileStateKind { Opening, Open, Closing, Deleting, Closed };
    struct FileState {
        int fd{-1};
        uint64_t generation{0};
        std::string path;
        FileStateKind state{FileStateKind::Opening};
        page_id_t next_page_no{0};
        size_t claims{0};
        size_t leases{0};
    };
    struct RegistryState {
        std::mutex mutex;
        std::condition_variable cv;
        bool shutting_down{false};
        size_t active_operations{0};
        uint64_t next_generation{1};
        std::unordered_map<std::string, std::shared_ptr<FileState>> paths;
        std::unordered_map<int, std::shared_ptr<FileState>> fds;
        std::function<void()> before_open;
        std::function<void()> before_close;
        std::function<void(FileOperationForTest)> before_operation_entry;
        std::function<void(FileOperationForTest)> before_operation;
        std::function<void()> before_wal_lease;
        std::atomic<SegmentedReadTestHook> before_segmented_read{nullptr};
        std::atomic<void*> before_segmented_read_context{nullptr};
    };
    struct RegistryIdentity {
        int fd{-1};
        uint64_t generation{0};
    };
    class RegistryOperation {
    public:
        RegistryOperation() = default;
        explicit RegistryOperation(std::shared_ptr<RegistryState> registry);
        RegistryOperation(std::shared_ptr<RegistryState> registry, FileOperationForTest entry_operation);
        ~RegistryOperation();
        RegistryOperation(RegistryOperation&& other) noexcept;
        RegistryOperation& operator=(RegistryOperation&& other) noexcept;
        RegistryOperation(const RegistryOperation&) = delete;
        RegistryOperation& operator=(const RegistryOperation&) = delete;

        const std::shared_ptr<RegistryState>& registry() const noexcept { return registry_; }

    private:
        std::shared_ptr<RegistryState> registry_;
    };
    struct LogHandle {
        RegistryIdentity identity;
        int raw_fd{-1};
        bool raw_override{false};
    };
    FileLease acquire_file_lease(int fd);
    static FileLease acquire_file_lease(const std::shared_ptr<RegistryState>& registry, int fd,
                                        bool admitted_before_shutdown = false);
    FileLease acquire_file_lease(RegistryIdentity identity, bool admitted_before_shutdown = false);
    static FileLease claim_to_lease(const std::shared_ptr<RegistryState>& registry,
                                    const std::shared_ptr<FileState>& state, uint64_t generation);
    static void release_claim(const std::shared_ptr<RegistryState>& registry,
                              const std::shared_ptr<FileState>& state, uint64_t generation) noexcept;
    static void release_lease(const std::shared_ptr<RegistryState>& registry,
                              const std::shared_ptr<FileState>& state, uint64_t generation) noexcept;
    static std::shared_ptr<FileState> find_open_file_locked(const std::shared_ptr<RegistryState>& registry, int fd);
    static void remove_state_locked(const std::shared_ptr<RegistryState>& registry,
                                    const std::shared_ptr<FileState>& state) noexcept;
    // Opens the WAL lazily. The append offset is initialized here, together
    // with the descriptor, so that "log_fd_ != -1" always implies "log_offset_
    // is the real append offset". read_log used to reassign log_offset_ on
    // every call, which left correctness depending on the order in which
    // readers and writers happened to run.
    int open_file_admitted(const std::string& path, const RegistryOperation& operation);
    void open_log_fd(const RegistryOperation& operation);
    int read_log_chunk_admitted(char* log_data, int size, int64_t offset, const RegistryOperation& operation);
    void sync_file_admitted(int fd, const RegistryOperation& operation);
    LogHandle snapshot_log_handle_locked() const;
    int64_t segmented_log_size_locked() const;
    int open_wal_segment_locked(uint64_t segment, bool create);
    void validate_segmented_layout_locked() const;
    void sync_pending_segment_directory_locked();

    std::shared_ptr<RegistryState> registry_{std::make_shared<RegistryState>()};

    mutable std::mutex legacy_wal_latch_;
    LogHandle log_handle_;
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
};

class DiskManager::FileLease {
public:
    FileLease() = default;
    ~FileLease();
    FileLease(FileLease&&) noexcept;
    FileLease& operator=(FileLease&&) noexcept;
    FileLease(const FileLease&) = delete;
    FileLease& operator=(const FileLease&) = delete;
    int fd() const noexcept { return state_ ? state_->fd : -1; }
private:
    friend class DiskManager;
    FileLease(std::shared_ptr<RegistryState> registry, std::shared_ptr<FileState> state, uint64_t generation)
        : registry_(std::move(registry)), state_(std::move(state)), generation_(generation) {}
    void reset() noexcept;
    std::shared_ptr<RegistryState> registry_;
    std::shared_ptr<FileState> state_;
    uint64_t generation_{0};
};

class DiskManager::FileWriteClaim {
public:
    FileWriteClaim() = default;
    ~FileWriteClaim();
    FileWriteClaim(FileWriteClaim&&) noexcept;
    FileWriteClaim& operator=(FileWriteClaim&&) noexcept;
    FileWriteClaim(const FileWriteClaim&) = delete;
    FileWriteClaim& operator=(const FileWriteClaim&) = delete;
    FileLease acquire_lease();
private:
    friend class DiskManager;
    FileWriteClaim(std::shared_ptr<RegistryState> registry, std::shared_ptr<FileState> state, uint64_t generation)
        : registry_(std::move(registry)), state_(std::move(state)), generation_(generation) {}
    void reset() noexcept;
    std::shared_ptr<RegistryState> registry_;
    std::shared_ptr<FileState> state_;
    uint64_t generation_{0};
};
