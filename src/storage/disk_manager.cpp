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
#include <cstring>
#include <dirent.h>
#include <exception>
#include <fcntl.h>
#include <functional>
#include <limits>
#include <set>
#include <sys/mman.h>
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

WalReadSnapshot::~WalReadSnapshot() {
    for (Span& span : spans_) {
        if (span.mapping != nullptr && span.mapping != MAP_FAILED) munmap(span.mapping, span.mapping_length);
        if (span.fd >= 0) close(span.fd);
    }
}

bool WalReadSnapshot::discard_resident_pages() const noexcept {
    bool discarded = true;
    for (const Span& span : spans_) {
        if (span.mapping != nullptr && span.mapping != MAP_FAILED &&
            madvise(span.mapping, span.mapping_length, MADV_DONTNEED) != 0) {
            discarded = false;
        }
    }
    return discarded;
}

const char* WalReadSnapshot::record_bytes(int64_t offset, uint32_t length, std::vector<char>* scratch,
                                          WalSnapshotAccess* access) const {
    if (access != nullptr) *access = WalSnapshotAccess{};
    const int64_t bytes = static_cast<int64_t>(length);
    if (scratch == nullptr || length == 0 || offset < begin_offset_ || offset > end_offset_ - bytes) {
        throw InternalError("WAL snapshot access leaves the accepted recovery prefix; WAL retained");
    }
    const int64_t record_end = offset + bytes;
    // Spans are installed in logical offset order. Recovery can have many WAL
    // segments, so a descriptor lookup must not become a linear scan over all
    // segments. lower_bound selects the first span after offset; its predecessor
    // is the only span that can contain offset.
    const auto after = std::upper_bound(spans_.begin(), spans_.end(), offset,
                                        [](int64_t value, const Span& span) { return value < span.range_begin; });
    if (after != spans_.begin()) {
        const Span& span = *std::prev(after);
        if (offset >= span.range_begin && record_end <= span.range_end) {
            return static_cast<const char*>(span.mapping) + (offset - span.mapping_logical_begin);
        }
    }

    scratch->resize(length);
    int64_t cursor = offset;
    size_t copied = 0;
    auto span = after == spans_.begin() ? after : std::prev(after);
    for (; span != spans_.end() && cursor < record_end; ++span) {
        if (cursor < span->range_begin) {
            break;
        }
        if (cursor >= span->range_end) continue;
        const int64_t chunk_end = std::min(record_end, span->range_end);
        const size_t chunk = static_cast<size_t>(chunk_end - cursor);
        std::memcpy(scratch->data() + copied,
                    static_cast<const char*>(span->mapping) + (cursor - span->mapping_logical_begin), chunk);
        copied += chunk;
        cursor = chunk_end;
        if (cursor == record_end) {
            if (access != nullptr) {
                access->copied = true;
                access->copied_bytes = length;
            }
            return scratch->data();
        }
    }
    throw InternalError("WAL snapshot spans do not continuously cover a recovery record; WAL retained");
}

DiskManager::DiskManager() = default;

DiskManager::~DiskManager() {
    std::unique_lock<std::mutex> lock(registry_->mutex);
    registry_->shutting_down = true;
    registry_->cv.wait(lock, [&] {
        if (registry_->active_operations != 0) return false;
        for (const auto& entry : registry_->fds) {
            if (entry.second->claims != 0 || entry.second->leases != 0) return false;
        }
        return true;
    });
    std::vector<std::shared_ptr<FileState>> files;
    for (const auto& entry : registry_->fds) {
        entry.second->state = FileStateKind::Closing;
        files.push_back(entry.second);
    }
    lock.unlock();
    for (const auto& state : files) (void)close(state->fd);
    lock.lock();
    for (const auto& state : files) {
        remove_state_locked(registry_, state);
        state->state = FileStateKind::Closed;
    }
    registry_->cv.notify_all();
}

DiskManager::RegistryOperation::RegistryOperation(std::shared_ptr<RegistryState> registry)
    : registry_(std::move(registry)) {
    std::lock_guard<std::mutex> lock(registry_->mutex);
    if (registry_->shutting_down) throw FileNotOpenError(-1);
    if (registry_->active_operations == std::numeric_limits<size_t>::max()) {
        throw InternalError("file registry operation counter overflow");
    }
    ++registry_->active_operations;
}
DiskManager::RegistryOperation::RegistryOperation(std::shared_ptr<RegistryState> registry,
                                                  FileOperationForTest entry_operation)
    : registry_(std::move(registry)) {
    std::function<void(FileOperationForTest)> hook;
    bool admitted = false;
    try {
        std::lock_guard<std::mutex> lock(registry_->mutex);
        if (registry_->shutting_down) throw FileNotOpenError(-1);
        if (registry_->active_operations == std::numeric_limits<size_t>::max()) {
            throw InternalError("file registry operation counter overflow");
        }
        ++registry_->active_operations;
        admitted = true;
        hook = registry_->before_operation_entry;
    } catch (...) {
        if (admitted) {
            std::lock_guard<std::mutex> lock(registry_->mutex);
            if (registry_->active_operations != 0) {
                --registry_->active_operations;
                registry_->cv.notify_all();
            }
        }
        throw;
    }
    try {
        if (hook) hook(entry_operation);
    } catch (...) {
        std::lock_guard<std::mutex> lock(registry_->mutex);
        if (registry_->active_operations == 0) std::terminate();
        --registry_->active_operations;
        registry_->cv.notify_all();
        throw;
    }
}
DiskManager::RegistryOperation::~RegistryOperation() {
    if (!registry_) return;
    std::lock_guard<std::mutex> lock(registry_->mutex);
    if (registry_->active_operations == 0) std::terminate();
    --registry_->active_operations;
    registry_->cv.notify_all();
}
DiskManager::RegistryOperation::RegistryOperation(RegistryOperation&& other) noexcept = default;
DiskManager::RegistryOperation& DiskManager::RegistryOperation::operator=(RegistryOperation&& other) noexcept {
    if (this != &other) {
        if (registry_) {
            std::lock_guard<std::mutex> lock(registry_->mutex);
            if (registry_->active_operations == 0) std::terminate();
            --registry_->active_operations;
            registry_->cv.notify_all();
        }
        registry_ = std::move(other.registry_);
    }
    return *this;
}

DiskManager::FileLease::~FileLease() { reset(); }
DiskManager::FileLease::FileLease(FileLease&& other) noexcept = default;
DiskManager::FileLease& DiskManager::FileLease::operator=(FileLease&& other) noexcept {
    if (this != &other) {
        reset();
        registry_ = std::move(other.registry_);
        state_ = std::move(other.state_);
        generation_ = other.generation_;
    }
    return *this;
}
void DiskManager::FileLease::reset() noexcept {
    if (registry_ && state_) DiskManager::release_lease(registry_, state_, generation_);
    registry_.reset(); state_.reset();
}
DiskManager::FileWriteClaim::~FileWriteClaim() { reset(); }
DiskManager::FileWriteClaim::FileWriteClaim(FileWriteClaim&& other) noexcept = default;
DiskManager::FileWriteClaim& DiskManager::FileWriteClaim::operator=(FileWriteClaim&& other) noexcept {
    if (this != &other) {
        reset();
        registry_ = std::move(other.registry_);
        state_ = std::move(other.state_);
        generation_ = other.generation_;
    }
    return *this;
}
void DiskManager::FileWriteClaim::reset() noexcept {
    if (registry_ && state_) DiskManager::release_claim(registry_, state_, generation_);
    registry_.reset(); state_.reset();
}
DiskManager::FileLease DiskManager::FileWriteClaim::acquire_lease() {
    if (!registry_ || !state_) throw FileNotOpenError(-1);
    FileLease result = DiskManager::claim_to_lease(registry_, state_, generation_);
    registry_.reset(); state_.reset();
    return result;
}

std::shared_ptr<DiskManager::FileState> DiskManager::find_open_file_locked(
    const std::shared_ptr<RegistryState>& registry, int fd) {
    const auto it = registry->fds.find(fd);
    if (it == registry->fds.end() || it->second->state != FileStateKind::Open) throw FileNotOpenError(fd);
    return it->second;
}
DiskManager::FileLease DiskManager::acquire_file_lease(int fd) {
    return acquire_file_lease(registry_, fd);
}
DiskManager::FileLease DiskManager::acquire_file_lease(const std::shared_ptr<RegistryState>& registry, int fd,
                                                        bool admitted_before_shutdown) {
    std::lock_guard<std::mutex> lock(registry->mutex);
    if (registry->shutting_down && !admitted_before_shutdown) throw FileNotOpenError(fd);
    auto state = find_open_file_locked(registry, fd);
    if (state->leases == std::numeric_limits<size_t>::max()) throw InternalError("file lease counter overflow");
    ++state->leases;
    const uint64_t generation = state->generation;
    return FileLease(registry, std::move(state), generation);
}
DiskManager::FileLease DiskManager::acquire_file_lease(RegistryIdentity identity, bool admitted_before_shutdown) {
    std::lock_guard<std::mutex> lock(registry_->mutex);
    if (registry_->shutting_down && !admitted_before_shutdown) throw FileNotOpenError(identity.fd);
    const auto it = registry_->fds.find(identity.fd);
    if (it == registry_->fds.end() || it->second->generation != identity.generation ||
        it->second->state != FileStateKind::Open) {
        throw FileNotOpenError(identity.fd);
    }
    auto state = it->second;
    if (state->leases == std::numeric_limits<size_t>::max()) throw InternalError("file lease counter overflow");
    ++state->leases;
    return FileLease(registry_, std::move(state), identity.generation);
}
DiskManager::FileWriteClaim DiskManager::acquire_file_write_claim(int fd) {
    RegistryOperation operation(registry_, FileOperationForTest::Claim);
    std::lock_guard<std::mutex> lock(registry_->mutex);
    auto state = find_open_file_locked(registry_, fd);
    if (state->claims == std::numeric_limits<size_t>::max()) throw InternalError("file claim counter overflow");
    ++state->claims;
    const uint64_t generation = state->generation;
    return FileWriteClaim(registry_, std::move(state), generation);
}
void DiskManager::set_file_lifecycle_test_hooks(std::function<void()> before_open, std::function<void()> before_close) {
    std::lock_guard<std::mutex> lock(registry_->mutex);
    registry_->before_open = std::move(before_open);
    registry_->before_close = std::move(before_close);
}
void DiskManager::set_file_operation_test_hook(std::function<void(FileOperationForTest)> hook) {
    std::lock_guard<std::mutex> lock(registry_->mutex);
    registry_->before_operation = std::move(hook);
}
void DiskManager::set_file_operation_entry_test_hook(std::function<void(FileOperationForTest)> hook) {
    std::lock_guard<std::mutex> lock(registry_->mutex);
    registry_->before_operation_entry = std::move(hook);
}
void DiskManager::set_segmented_read_test_hook(SegmentedReadTestHook hook, void* context) {
    // This is a quiescent-manager test seam: publishing the context before the
    // callback keeps the read path to one cold atomic callback check.
    registry_->before_segmented_read_context.store(context, std::memory_order_relaxed);
    registry_->before_segmented_read.store(hook, std::memory_order_release);
}
void DiskManager::set_wal_lease_test_hook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(registry_->mutex);
    registry_->before_wal_lease = std::move(hook);
}
DiskManager::FileIdentityForTest DiskManager::capture_file_identity_for_test(int fd) {
    RegistryOperation operation(registry_);
    std::lock_guard<std::mutex> lock(registry_->mutex);
    const auto state = find_open_file_locked(registry_, fd);
    return FileIdentityForTest{fd, state->generation};
}
bool DiskManager::stale_file_identity_rejected_for_test(FileIdentityForTest identity) {
    try {
        auto lease = acquire_file_lease(RegistryIdentity{identity.fd, identity.generation});
        return false;
    } catch (const FileNotOpenError&) {
        return true;
    }
}
DiskManager::FileLease DiskManager::claim_to_lease(const std::shared_ptr<RegistryState>& registry,
                                                   const std::shared_ptr<FileState>& state, uint64_t generation) {
    std::lock_guard<std::mutex> lock(registry->mutex);
    const auto it = registry->fds.find(state->fd);
    if (it == registry->fds.end() || it->second != state || state->generation != generation ||
        (state->state != FileStateKind::Open && state->state != FileStateKind::Closing) || state->claims == 0 ||
        state->leases == std::numeric_limits<size_t>::max())
        throw FileNotOpenError(state->fd);
    --state->claims; ++state->leases;
    return FileLease(registry, state, generation);
}
void DiskManager::release_claim(const std::shared_ptr<RegistryState>& registry, const std::shared_ptr<FileState>& state,
                                uint64_t generation) noexcept {
    std::lock_guard<std::mutex> lock(registry->mutex);
    if (state->generation != generation || state->claims == 0) std::terminate();
    --state->claims; registry->cv.notify_all();
}
void DiskManager::release_lease(const std::shared_ptr<RegistryState>& registry, const std::shared_ptr<FileState>& state,
                                uint64_t generation) noexcept {
    std::lock_guard<std::mutex> lock(registry->mutex);
    if (state->generation != generation || state->leases == 0) std::terminate();
    --state->leases; registry->cv.notify_all();
}

void DiskManager::remove_state_locked(const std::shared_ptr<RegistryState>& registry,
                                      const std::shared_ptr<FileState>& state) noexcept {
    const auto fd_it = registry->fds.find(state->fd);
    if (fd_it != registry->fds.end() && fd_it->second == state) registry->fds.erase(fd_it);
    const auto path_it = registry->paths.find(state->path);
    if (path_it != registry->paths.end() && path_it->second == state) registry->paths.erase(path_it);
}

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
    RegistryOperation operation(registry_, FileOperationForTest::Write);
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用write()函数
    // 注意write返回值与num_bytes不等时 throw InternalError("DiskManager::write_page Error");
    const FileLease lease = acquire_file_lease(operation.registry(), fd, true);
    std::function<void(FileOperationForTest)> hook;
    { std::lock_guard<std::mutex> lock(operation.registry()->mutex); hook = operation.registry()->before_operation; }
    if (hook) hook(FileOperationForTest::Write);
    FaultInjector::Point("during_data_page_pwrite");
    WritePageAt(lease.fd(), page_no, offset, num_bytes);
}

/**
 * @description: 读取文件中指定编号的页面中的部分数据到内存中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 指定的页面编号
 * @param {char} *offset 读取的内容写入到offset中
 * @param {int} num_bytes 读取的数据量大小
 */
void DiskManager::read_page(int fd, page_id_t page_no, char* offset, int num_bytes) {
    RegistryOperation operation(registry_, FileOperationForTest::Read);
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用read()函数
    // 注意read返回值与num_bytes不等时，throw InternalError("DiskManager::read_page Error");
    const FileLease lease = acquire_file_lease(operation.registry(), fd, true);
    std::function<void(FileOperationForTest)> hook;
    { std::lock_guard<std::mutex> lock(operation.registry()->mutex); hook = operation.registry()->before_operation; }
    if (hook) hook(FileOperationForTest::Read);
    FaultInjector::Point("during_data_page_pread");
    ReadPageAt(lease.fd(), page_no, offset, num_bytes);
}

/**
 * @description: 分配一个新的页号
 * @return {page_id_t} 分配的新页号
 * @param {int} fd 指定文件的文件句柄
 */
page_id_t DiskManager::allocate_page(int fd) {
    RegistryOperation operation(registry_);
    // 简单的自增分配策略，指定文件的页面编号加1
    std::lock_guard<std::mutex> lock(registry_->mutex);
    return find_open_file_locked(registry_, fd)->next_page_no++;
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
    RegistryOperation operation(registry_);
    std::shared_ptr<FileState> state;
    {
        std::lock_guard<std::mutex> lock(registry_->mutex);
        if (registry_->paths.count(path)) throw FileNotClosedError(path);
        state = std::make_shared<FileState>();
        state->path = path; state->state = FileStateKind::Deleting;
        if (registry_->next_generation == std::numeric_limits<uint64_t>::max())
            throw InternalError("file generation overflow");
        state->generation = registry_->next_generation++;
        registry_->paths.emplace(path, state);
    }
    if (is_file(path) == 0) {
        std::lock_guard<std::mutex> lock(registry_->mutex);
        remove_state_locked(registry_, state);
        throw FileNotFoundError(path);
    }
    if (unlink(path.c_str()) < 0) {
        std::lock_guard<std::mutex> lock(registry_->mutex);
        remove_state_locked(registry_, state);
        throw UnixError();
    }
    std::lock_guard<std::mutex> lock(registry_->mutex);
    remove_state_locked(registry_, state);
    registry_->cv.notify_all();
}

/**
 * @description: 打开指定路径文件
 * @return {int} 返回打开的文件的文件句柄
 * @param {string} &path 文件所在路径
 */
int DiskManager::open_file(const std::string& path) {
    RegistryOperation operation(registry_);
    return open_file_admitted(path, operation);
}

int DiskManager::open_file_admitted(const std::string& path, const RegistryOperation& operation) {
    const auto& registry = operation.registry();
    std::shared_ptr<FileState> state;
    {
        std::lock_guard<std::mutex> lock(registry->mutex);
        const auto it = registry->paths.find(path);
        if (it != registry->paths.end()) {
            if (it->second->state == FileStateKind::Open) return it->second->fd;
            throw FileNotOpenError(it->second->fd);
        }
        if (registry->next_generation == std::numeric_limits<uint64_t>::max()) {
            throw InternalError("file generation overflow");
        }
        state = std::make_shared<FileState>();
        state->path = path; state->generation = registry->next_generation++;
        registry->paths.emplace(path, state);
    }
    auto rollback = [&]() noexcept {
        std::lock_guard<std::mutex> lock(registry->mutex);
        remove_state_locked(registry, state);
        registry->cv.notify_all();
    };
    if (!is_file(path)) { rollback(); throw FileNotFoundError(path); }
    std::function<void()> before_open;
    try { std::lock_guard<std::mutex> lock(registry->mutex); before_open = registry->before_open; }
    catch (...) { rollback(); throw; }
    try { if (before_open) before_open(); }
    catch (...) { rollback(); throw; }
    const int fd = open(path.c_str(), O_RDWR);
    if (fd < 0 || fd >= MAX_FD) {
        if (fd >= MAX_FD) close(fd);
        rollback();
        if (fd >= MAX_FD) throw InternalError("DiskManager::open_file fd exceeds MAX_FD");
        throw UnixError();
    }
    bool abandon = false;
    try {
        std::unique_lock<std::mutex> lock(registry->mutex);
        while (registry->fds.find(fd) != registry->fds.end()) {
            registry->cv.wait(lock, [&] {
                return registry->fds.find(fd) == registry->fds.end();
            });
        }
        abandon = registry->paths.find(path) == registry->paths.end() || registry->paths.find(path)->second != state;
        if (!abandon) { state->fd = fd; state->state = FileStateKind::Open; registry->fds.emplace(fd, state); }
    } catch (...) { const int saved_errno = errno; close(fd); errno = saved_errno; rollback(); throw; }
    if (abandon) {
        const int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        rollback();
        throw FileNotOpenError(fd);
    }
    return fd;
}

/**
 * @description:用于关闭指定路径文件
 * @param {int} fd 打开的文件的文件句柄
 */
void DiskManager::close_file(int fd) {
    RegistryOperation operation(registry_);
    std::shared_ptr<FileState> state;
    { std::unique_lock<std::mutex> lock(registry_->mutex); state = find_open_file_locked(registry_, fd);
      state->state = FileStateKind::Closing;
      registry_->cv.wait(lock, [&] { return state->claims == 0 && state->leases == 0; }); }
    std::function<void()> before_close;
    try { std::lock_guard<std::mutex> lock(registry_->mutex); before_close = registry_->before_close; }
    catch (...) {
        std::lock_guard<std::mutex> lock(registry_->mutex);
        state->state = FileStateKind::Open;
        registry_->cv.notify_all();
        throw;
    }
    try { if (before_close) before_close(); }
    catch (...) {
        std::lock_guard<std::mutex> lock(registry_->mutex);
        state->state = FileStateKind::Open;
        registry_->cv.notify_all();
        throw;
    }
    const int result = close(fd); const int saved_errno = errno;
    std::exception_ptr hook_failure;
    try {
        std::function<void(FileOperationForTest)> hook;
        { std::lock_guard<std::mutex> lock(registry_->mutex); hook = registry_->before_operation; }
        if (hook) hook(FileOperationForTest::CloseAfterSyscall);
    } catch (...) { hook_failure = std::current_exception(); }
    { std::lock_guard<std::mutex> lock(registry_->mutex);
      remove_state_locked(registry_, state); state->state = FileStateKind::Closed; registry_->cv.notify_all(); }
    if (hook_failure) std::rethrow_exception(hook_failure);
    if (result != 0) { errno = saved_errno; throw UnixError(); }
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
    RegistryOperation operation(registry_);
    std::lock_guard<std::mutex> lock(registry_->mutex);
    return find_open_file_locked(registry_, fd)->path;
}

/**
 * @description:  获得文件名对应的文件句柄
 * @return {int} 文件句柄
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_fd(const std::string& file_name) {
    RegistryOperation operation(registry_);
    { std::lock_guard<std::mutex> lock(registry_->mutex);
      const auto it = registry_->paths.find(file_name);
      if (it != registry_->paths.end() && it->second->state == FileStateKind::Open) return it->second->fd; }
    return open_file_admitted(file_name, operation);
}

void DiskManager::open_log_fd(const RegistryOperation& operation) {
    std::lock_guard<std::mutex> log_lock(legacy_wal_latch_);
    if (log_handle_.raw_override && log_handle_.raw_fd != -1) {
        return;
    }
    if (!log_handle_.raw_override && log_handle_.identity.fd >= 0) return;
    const int local_fd = open_file_admitted(LOG_FILE_NAME, operation);
    FileLease lease = acquire_file_lease(operation.registry(), local_fd, true);
    const int64_t local_offset = get_file_size(lease.fd());
    {
        std::lock_guard<std::mutex> registry_lock(operation.registry()->mutex);
        const auto state = find_open_file_locked(operation.registry(), local_fd);
        log_handle_.identity = RegistryIdentity{local_fd, state->generation};
    }
    log_handle_.raw_override = false;
    log_handle_.raw_fd = -1;
    log_offset_ = local_offset;
}

DiskManager::LogHandle DiskManager::snapshot_log_handle_locked() const { return log_handle_; }

void DiskManager::SetLogOffset(int64_t log_offset) {
    std::lock_guard<std::mutex> log_lock(legacy_wal_latch_);
    log_offset_ = log_offset;
}

void DiskManager::SetLogFd(int log_fd) {
    RegistryOperation operation(registry_);
    std::lock_guard<std::mutex> log_lock(legacy_wal_latch_);
    if (log_fd >= 0) {
        std::lock_guard<std::mutex> registry_lock(registry_->mutex);
        const auto state = find_open_file_locked(registry_, log_fd);
        log_handle_.identity = RegistryIdentity{log_fd, state->generation};
        log_handle_.raw_override = false;
        log_handle_.raw_fd = -1;
        return;
    }
    log_handle_ = LogHandle{};
    log_handle_.raw_override = true;
    log_handle_.raw_fd = log_fd;
}
int DiskManager::GetLogFd() {
    std::lock_guard<std::mutex> log_lock(legacy_wal_latch_);
    return log_handle_.raw_override ? log_handle_.raw_fd : log_handle_.identity.fd;
}
void DiskManager::set_fd2pageno(int fd, int start_page_no) {
    RegistryOperation operation(registry_);
    std::lock_guard<std::mutex> lock(registry_->mutex);
    find_open_file_locked(registry_, fd)->next_page_no = start_page_no;
}
page_id_t DiskManager::get_fd2pageno(int fd) {
    RegistryOperation operation(registry_);
    std::lock_guard<std::mutex> lock(registry_->mutex);
    return find_open_file_locked(registry_, fd)->next_page_no;
}

int64_t DiskManager::get_log_file_size() {
    RegistryOperation operation(registry_);
    std::lock_guard<std::mutex> segmented_lock(wal_segment_latch_);
    if (wal_segmented_) {
        return log_offset_;
    }
    open_log_fd(operation);
    std::lock_guard<std::mutex> log_lock(legacy_wal_latch_);
    const LogHandle handle = snapshot_log_handle_locked();
    std::function<void()> before_wal_lease;
    { std::lock_guard<std::mutex> registry_lock(registry_->mutex); before_wal_lease = registry_->before_wal_lease; }
    if (before_wal_lease) before_wal_lease();
    FileLease lease;
    int fd = handle.raw_override ? handle.raw_fd : -1;
    if (!handle.raw_override) {
        try {
            lease = acquire_file_lease(handle.identity, true);
            fd = lease.fd();
        } catch (const FileNotOpenError&) {
            throw InternalError("DiskManager::get_file_size(fstat) failed for closed WAL descriptor " +
                                std::to_string(handle.identity.fd));
        }
    }
    if (fd < 0) {
        throw InternalError("DiskManager::get_log_file_size has an invalid WAL descriptor: " +
                            std::to_string(fd));
    }
    return get_file_size(fd);
}

/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int64_t} offset 读取的内容在文件中的位置
 */
int DiskManager::read_log(char* log_data, int size, int64_t offset) {
    RegistryOperation operation(registry_);
    if (wal_is_segmented()) {
        return read_log_chunk_admitted(log_data, size, offset, operation);
    }
    // read log file from the previous end
    open_log_fd(operation);
    std::unique_lock<std::mutex> log_lock(legacy_wal_latch_);
    const LogHandle handle = snapshot_log_handle_locked();
    FileLease lease;
    const int fd = handle.raw_override ? handle.raw_fd : (lease = acquire_file_lease(handle.identity, true)).fd();
    const int64_t file_size = get_file_size(fd);
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
            pread(fd, log_data + bytes_read, static_cast<size_t>(size - bytes_read), offset + bytes_read);
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
    RegistryOperation operation(registry_);
    return read_log_chunk_admitted(log_data, size, offset, operation);
}

int DiskManager::read_log_chunk_admitted(char* log_data, int size, int64_t offset,
                                         const RegistryOperation& operation) {
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
        if (const auto hook = operation.registry()->before_segmented_read.load(std::memory_order_acquire)) {
            hook(operation.registry()->before_segmented_read_context.load(std::memory_order_relaxed));
        }
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
    open_log_fd(operation);
    std::unique_lock<std::mutex> log_lock(legacy_wal_latch_);
    const LogHandle handle = snapshot_log_handle_locked();
    FileLease lease;
    const int fd = handle.raw_override ? handle.raw_fd : (lease = acquire_file_lease(handle.identity, true)).fd();
    int bytes_read = 0;
    while (bytes_read < size) {
        const ssize_t count =
            pread(fd, log_data + bytes_read, static_cast<size_t>(size - bytes_read), offset + bytes_read);
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

std::unique_ptr<WalReadSnapshot> DiskManager::create_wal_read_snapshot(int64_t begin_offset, int64_t end_offset) {
    if (begin_offset < 0 || end_offset <= begin_offset) {
        throw InternalError("invalid WAL snapshot range; WAL retained");
    }
    std::lock_guard<std::mutex> lock(wal_segment_latch_);
    auto snapshot = std::unique_ptr<WalReadSnapshot>(new WalReadSnapshot(begin_offset, end_offset));
    const long page_size_long = sysconf(_SC_PAGESIZE);
    if (page_size_long <= 0) throw InternalError("could not determine mmap page size; WAL retained");
    const int64_t page_size = page_size_long;

    auto map_span = [&](int fd, int64_t physical_begin, int64_t logical_base, int64_t range_begin,
                        int64_t range_end) {
        const int64_t aligned_physical = physical_begin - physical_begin % page_size;
        const int64_t mapping_logical_begin = logical_base + aligned_physical;
        const int64_t mapping_bytes = range_end - mapping_logical_begin;
        if (mapping_bytes <= 0 || static_cast<uint64_t>(mapping_bytes) > std::numeric_limits<size_t>::max()) {
            close(fd);
            throw InternalError("WAL snapshot mapping length overflow; WAL retained");
        }
        void* mapping = mmap(nullptr, static_cast<size_t>(mapping_bytes), PROT_READ, MAP_SHARED, fd, aligned_physical);
        if (mapping == MAP_FAILED) {
            const int saved_errno = errno;
            close(fd);
            errno = saved_errno;
            throw UnixError();
        }
        // Recovery reads page-sorted records at scattered WAL offsets. Disable
        // mmap readahead so an accessed record does not make unrelated WAL pages
        // resident before the bounded residency trim can discard them.
        (void)madvise(mapping, static_cast<size_t>(mapping_bytes), MADV_RANDOM);
        snapshot->add_span(WalReadSnapshot::Span{fd, mapping, static_cast<size_t>(mapping_bytes),
                                                 mapping_logical_begin, range_begin, range_end});
    };

    if (!wal_segmented_) {
        const int fd = open(LOG_FILE_NAME.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
        if (fd < 0) throw UnixError();
        struct stat st {};
        if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < end_offset) {
            const int saved_errno = errno;
            close(fd);
            errno = saved_errno;
            throw InternalError("legacy WAL snapshot is not a regular complete file; WAL retained");
        }
        map_span(fd, begin_offset, 0, begin_offset, end_offset);
        return snapshot;
    }

    validate_segmented_layout_locked();
    if (end_offset > log_offset_ || wal_segment_bytes_ <= 0) {
        throw InternalError("segmented WAL snapshot exceeds the finalized logical range; WAL retained");
    }
    const uint64_t generation = wal_generation_;
    const int64_t segment_bytes = wal_segment_bytes_;
    const uint64_t first_segment = static_cast<uint64_t>(begin_offset / segment_bytes);
    const uint64_t last_segment = static_cast<uint64_t>((end_offset - 1) / segment_bytes);
    for (uint64_t segment = first_segment;; ++segment) {
        if (segment > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) /
                          static_cast<uint64_t>(segment_bytes)) {
            throw InternalError("segmented WAL snapshot logical offset overflow; WAL retained");
        }
        const int64_t logical_base = static_cast<int64_t>(segment * static_cast<uint64_t>(segment_bytes));
        const int64_t range_begin = std::max(begin_offset, logical_base);
        const int64_t range_end = std::min(end_offset, logical_base + segment_bytes);
        const std::string name = SegmentedWalName(generation, segment);
        const int fd = open(name.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
        if (fd < 0) throw UnixError();
        struct stat st {};
        const int64_t required_bytes = range_end - logical_base;
        if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < required_bytes || st.st_size > segment_bytes ||
            (segment != last_segment && st.st_size != segment_bytes)) {
            const int saved_errno = errno;
            close(fd);
            errno = saved_errno;
            throw InternalError("segmented WAL snapshot has a missing, short, or invalid span; WAL retained");
        }
        map_span(fd, range_begin - logical_base, logical_base, range_begin, range_end);
        if (segment == last_segment) break;
    }
    return snapshot;
}

/**
 * @description: 写日志内容
 * @param {char} *log_data 要写入的日志内容
 * @param {int} size 要写入的内容大小
 */
void DiskManager::write_log(char* log_data, int size) {
    RegistryOperation operation(registry_);
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
    open_log_fd(operation);
    std::unique_lock<std::mutex> log_lock(legacy_wal_latch_);
    const LogHandle handle = snapshot_log_handle_locked();
    FileLease lease;
    const int fd = handle.raw_override ? handle.raw_fd : (lease = acquire_file_lease(handle.identity, true)).fd();

    // write from the file_end
    FaultInjector::Point("during_wal_pwrite");
    const int64_t begin_offset = log_offset_;
    int bytes_write = 0;
    while (bytes_write < size) {
        const ssize_t count = pwrite(fd, log_data + bytes_write, static_cast<size_t>(size - bytes_write),
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
    RegistryOperation operation(registry_);
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
    open_log_fd(operation);
    std::unique_lock<std::mutex> log_lock(legacy_wal_latch_);
    const LogHandle handle = snapshot_log_handle_locked();
    FileLease lease;
    const int fd = handle.raw_override ? handle.raw_fd : (lease = acquire_file_lease(handle.identity, true)).fd();
    if (fd != -1 && fdatasync(fd) != 0) {
        throw UnixError();
    }
    FaultInjector::Point("after_wal_fsync");
}

void DiskManager::sync_file(int fd) {
    RegistryOperation operation(registry_, FileOperationForTest::Sync);
    sync_file_admitted(fd, operation);
}

void DiskManager::sync_file_admitted(int fd, const RegistryOperation& operation) {
    FaultInjector::Point("before_data_fsync");
    if (fd < 0) {
        errno = EBADF;
        throw UnixError();
    }
    FileLease lease;
    try {
        lease = acquire_file_lease(operation.registry(), fd, true);
        fd = lease.fd();
    } catch (const FileNotOpenError&) {
        // Raw descriptors are accepted only when the registry has no entry:
        // a Closing entry may have released its OS number already.
        std::lock_guard<std::mutex> lock(operation.registry()->mutex);
        if (operation.registry()->fds.find(fd) != operation.registry()->fds.end()) throw;
    }
    std::function<void(FileOperationForTest)> hook;
    { std::lock_guard<std::mutex> lock(operation.registry()->mutex); hook = operation.registry()->before_operation; }
    if (hook) hook(FileOperationForTest::Sync);
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
    RegistryOperation operation(registry_);
    int fd = open(path.c_str(), O_RDWR);
    if (fd < 0) {
        throw UnixError();
    }
    try {
        sync_file_admitted(fd, operation);
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
    RegistryOperation operation(registry_);
    if (wal_is_segmented()) {
        // Batch 1 deliberately does not reclaim or switch generations. A
        // segmented clean-reset must publish a new v2 manifest before any old
        // segment can be removed, which is Batch 2's responsibility.
        throw InternalError("segmented WAL reset requires manifest-backed generation switch");
    }
    open_log_fd(operation);
    std::unique_lock<std::mutex> log_lock(legacy_wal_latch_);
    const LogHandle handle = snapshot_log_handle_locked();
    FileLease lease;
    const int fd = handle.raw_override ? handle.raw_fd : (lease = acquire_file_lease(handle.identity, true)).fd();
    if (fd != -1) {
        if (ftruncate(fd, 0) != 0) {
            throw UnixError();
        }
        FaultInjector::Point("after_wal_ftruncate");
        // final.md tests same-machine SIGKILL, which preserves the in-kernel
        // zero length. The first post-checkpoint WAL fdatasync also persists
        // this size change before acknowledging its COMMIT.
        if (lseek(fd, 0, SEEK_SET) < 0) {
            throw UnixError();
        }
    }
    log_offset_ = 0;
}

void DiskManager::truncate_log_to(int64_t offset) {
    RegistryOperation operation(registry_);
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
    open_log_fd(operation);
    std::unique_lock<std::mutex> log_lock(legacy_wal_latch_);
    const LogHandle handle = snapshot_log_handle_locked();
    FileLease lease;
    const int fd = handle.raw_override ? handle.raw_fd : (lease = acquire_file_lease(handle.identity, true)).fd();
    if (ftruncate(fd, static_cast<off_t>(offset)) != 0) {
        throw UnixError();
    }
    if (fdatasync(fd) != 0) {
        throw UnixError();
    }
    if (lseek(fd, static_cast<off_t>(offset), SEEK_SET) < 0) {
        throw UnixError();
    }
    log_offset_ = offset;
}
