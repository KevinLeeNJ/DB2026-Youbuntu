#include "sidecar_sorter.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <queue>
#include <stdexcept>
#include <system_error>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace deltakernel::detail {
namespace {

class OwnedFd {
public:
    explicit OwnedFd(int fd = -1) : fd_(fd) {}
    ~OwnedFd() {
        reset();
    }
    OwnedFd(const OwnedFd&) = delete;
    OwnedFd& operator=(const OwnedFd&) = delete;
    OwnedFd(OwnedFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    int get() const {
        return fd_;
    }
    void reset(int fd = -1) {
        if (fd_ >= 0)
            close(fd_);
        fd_ = fd;
    }

private:
    int fd_ = -1;
};

constexpr uint64_t kMagic = 0x524F5254534B4445ULL; // EDKSTKOR
constexpr uint32_t kFormat = 1;
constexpr size_t kHeaderBytes = 48;
constexpr size_t kHeaderCrcOffset = 36;

constexpr std::array<uint32_t, 256> MakeCrcTable() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < table.size(); ++i) {
        uint32_t crc = i;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1U) ^ (0xedb88320U & (0U - (crc & 1U)));
        table[i] = crc;
    }
    return table;
}
constexpr auto kCrcTable = MakeCrcTable();

uint32_t Crc32(const uint8_t* data, size_t size, uint32_t crc = 0xffffffffU) {
    for (size_t i = 0; i < size; ++i)
        crc = (crc >> 8U) ^ kCrcTable[(crc ^ data[i]) & 0xffU];
    return crc;
}

template <typename T> void PutLe(uint8_t* data, size_t& offset, T value) {
    for (size_t i = 0; i < sizeof(T); ++i)
        data[offset++] = static_cast<uint8_t>(value >> (i * 8));
}

template <typename T> T GetLe(const uint8_t* data, size_t& offset) {
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
        value |= static_cast<T>(data[offset++]) << (i * 8);
    return value;
}

void ThrowErrno(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

uint64_t AddChecked(uint64_t left, uint64_t right) {
    if (right > std::numeric_limits<uint64_t>::max() - left)
        throw std::runtime_error("sorter size overflow");
    return left + right;
}

void WriteAll(int fd, const uint8_t* data, size_t size) {
    while (size != 0) {
        const ssize_t written = write(fd, data, size);
        if (written < 0 && errno == EINTR)
            continue;
        if (written <= 0)
            ThrowErrno("write sorter run");
        data += written;
        size -= static_cast<size_t>(written);
    }
}

void ReadAll(int fd, uint8_t* data, size_t size) {
    while (size != 0) {
        const ssize_t read_bytes = read(fd, data, size);
        if (read_bytes < 0 && errno == EINTR)
            continue;
        if (read_bytes == 0)
            throw std::runtime_error("truncated sorter run");
        if (read_bytes < 0)
            ThrowErrno("read sorter run");
        data += read_bytes;
        size -= static_cast<size_t>(read_bytes);
    }
}

void WriteAtAll(int fd, const uint8_t* data, size_t size, off_t offset) {
    while (size != 0) {
        const ssize_t written = pwrite(fd, data, size, offset);
        if (written < 0 && errno == EINTR)
            continue;
        if (written <= 0)
            ThrowErrno("write sorter header");
        data += written;
        size -= static_cast<size_t>(written);
        offset += written;
    }
}

bool Less(const SortRecord& left, const SortRecord& right) {
    return left.key < right.key || (left.key == right.key && left.value < right.value);
}

struct Run {
    std::string name;
    dev_t device = 0;
    ino_t inode = 0;
};

struct Reader {
    int fd = -1;
    uint64_t expected_count = 0;
    uint64_t expected_payload = 0;
    uint32_t expected_crc = 0;
    uint64_t count = 0;
    uint64_t payload = 0;
    uint32_t crc = 0xffffffffU;
    std::string previous;
    uint64_t previous_value = 0;
    bool has_previous = false;

    size_t max_record_bytes = 0;
    explicit Reader(int file, size_t max_record) : fd(file), max_record_bytes(max_record) {
        std::array<uint8_t, kHeaderBytes> header{};
        ReadAll(fd, header.data(), header.size());
        size_t offset = 0;
        if (GetLe<uint64_t>(header.data(), offset) != kMagic || GetLe<uint32_t>(header.data(), offset) != kFormat ||
            GetLe<uint32_t>(header.data(), offset) != kHeaderBytes)
            throw std::runtime_error("invalid sorter run header");
        expected_count = GetLe<uint64_t>(header.data(), offset);
        expected_payload = GetLe<uint64_t>(header.data(), offset);
        expected_crc = GetLe<uint32_t>(header.data(), offset);
        const uint32_t header_crc = GetLe<uint32_t>(header.data(), offset);
        const uint64_t total = GetLe<uint64_t>(header.data(), offset);
        std::array<uint8_t, kHeaderBytes> check = header;
        size_t crc_offset = kHeaderCrcOffset;
        std::fill(check.begin() + crc_offset, check.begin() + crc_offset + 4, 0);
        if (expected_payload > std::numeric_limits<uint64_t>::max() - kHeaderBytes ||
            total != kHeaderBytes + expected_payload || (Crc32(check.data(), check.size()) ^ 0xffffffffU) != header_crc)
            throw std::runtime_error("invalid sorter run header checksum");
    }

    bool Next(SortRecord& record, const std::function<bool()>& cancelled = {}) {
        if (cancelled && cancelled())
            throw SidecarSortCancelled();
        if (count == expected_count) {
            if (payload != expected_payload || (crc ^ 0xffffffffU) != expected_crc)
                throw std::runtime_error("invalid sorter run payload checksum");
            uint8_t extra = 0;
            const ssize_t result = read(fd, &extra, 1);
            if (result < 0 && errno == EINTR)
                return Next(record);
            if (result != 0)
                throw std::runtime_error("sorter run has trailing bytes");
            return false;
        }
        std::array<uint8_t, 12> fixed{};
        ReadAll(fd, fixed.data(), fixed.size());
        size_t offset = 0;
        const uint32_t key_bytes = GetLe<uint32_t>(fixed.data(), offset);
        const uint64_t value = GetLe<uint64_t>(fixed.data(), offset);
        if (expected_payload < payload || expected_payload - payload < 12 || key_bytes > max_record_bytes - 12 ||
            key_bytes > expected_payload - payload - 12)
            throw std::runtime_error("invalid sorter run record");
        record.key.resize(key_bytes);
        if (key_bytes != 0)
            ReadAll(fd, reinterpret_cast<uint8_t*>(record.key.data()), key_bytes);
        record.value = value;
        crc = Crc32(fixed.data(), fixed.size(), crc);
        if (key_bytes != 0)
            crc = Crc32(reinterpret_cast<const uint8_t*>(record.key.data()), key_bytes, crc);
        payload += 12 + key_bytes;
        ++count;
        if (has_previous && (record.key < previous || (record.key == previous && record.value < previous_value)))
            throw std::runtime_error("sorter run is not sorted");
        previous = record.key;
        previous_value = record.value;
        has_previous = true;
        return true;
    }
};

} // namespace

struct SidecarSorter::State {
    int directory_fd = -1;
    std::string token;
    SidecarSorter::Options options;
    std::vector<SortRecord> buffer;
    size_t buffer_payload = 0;
    std::vector<Run> runs;
    uint64_t serial = 0;
    bool finished = false;

    ~State() {
        for (const Run& run : runs) {
            struct stat status {};
            if (fstatat(directory_fd, run.name.c_str(), &status, AT_SYMLINK_NOFOLLOW) == 0 &&
                status.st_dev == run.device && status.st_ino == run.inode)
                unlinkat(directory_fd, run.name.c_str(), 0);
        }
        if (directory_fd >= 0)
            close(directory_fd);
    }
};

int OpenOwned(const SidecarSorter::State& state, const Run& run, int flags) {
    const int fd = openat(state.directory_fd, run.name.c_str(), flags | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0)
        ThrowErrno("open sorter run");
    struct stat status {};
    if (fstat(fd, &status) != 0 || !S_ISREG(status.st_mode) || status.st_dev != run.device ||
        status.st_ino != run.inode) {
        close(fd);
        throw std::runtime_error("sorter run ownership changed");
    }
    return fd;
}

Run CreateRunStream(SidecarSorter::State& state, uint64_t count, uint64_t payload_bytes,
                    const std::function<void(const std::function<void(const SortRecord&)>&)>& produce,
                    const std::function<bool()>& cancelled) {
    const std::string name = state.token + "-sort-run-" + std::to_string(++state.serial);
    const int fd = openat(state.directory_fd, name.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0)
        ThrowErrno("create sorter run");
    struct stat status {};
    if (fstat(fd, &status) != 0 || !S_ISREG(status.st_mode)) {
        close(fd);
        unlinkat(state.directory_fd, name.c_str(), 0);
        throw std::runtime_error("sorter run is not regular");
    }
    std::array<uint8_t, kHeaderBytes> header{};
    const uint64_t total_bytes = AddChecked(kHeaderBytes, payload_bytes);
    size_t offset = 0;
    PutLe<uint64_t>(header.data(), offset, kMagic);
    PutLe<uint32_t>(header.data(), offset, kFormat);
    PutLe<uint32_t>(header.data(), offset, kHeaderBytes);
    PutLe<uint64_t>(header.data(), offset, count);
    PutLe<uint64_t>(header.data(), offset, payload_bytes);
    PutLe<uint32_t>(header.data(), offset, 0);
    const size_t header_crc_offset = offset;
    PutLe<uint32_t>(header.data(), offset, 0);
    PutLe<uint64_t>(header.data(), offset, total_bytes);
    offset = header_crc_offset;
    PutLe<uint32_t>(header.data(), offset, Crc32(header.data(), header.size()) ^ 0xffffffffU);
    try {
        WriteAll(fd, header.data(), header.size());
        uint32_t payload_crc = 0xffffffffU;
        uint64_t written_payload = 0;
        produce([&](const SortRecord& record) {
            if (cancelled && cancelled())
                throw SidecarSortCancelled();
            std::array<uint8_t, 12> fixed{};
            size_t record_offset = 0;
            PutLe<uint32_t>(fixed.data(), record_offset, static_cast<uint32_t>(record.key.size()));
            PutLe<uint64_t>(fixed.data(), record_offset, record.value);
            WriteAll(fd, fixed.data(), fixed.size());
            payload_crc = Crc32(fixed.data(), fixed.size(), payload_crc);
            written_payload = AddChecked(written_payload, fixed.size());
            if (!record.key.empty())
                WriteAll(fd, reinterpret_cast<const uint8_t*>(record.key.data()), record.key.size());
            payload_crc = Crc32(reinterpret_cast<const uint8_t*>(record.key.data()), record.key.size(), payload_crc);
            written_payload = AddChecked(written_payload, record.key.size());
        });
        if (written_payload != payload_bytes)
            throw std::runtime_error("sorter producer count mismatch");
        offset = 0;
        PutLe<uint64_t>(header.data(), offset, kMagic);
        PutLe<uint32_t>(header.data(), offset, kFormat);
        PutLe<uint32_t>(header.data(), offset, kHeaderBytes);
        PutLe<uint64_t>(header.data(), offset, count);
        PutLe<uint64_t>(header.data(), offset, payload_bytes);
        PutLe<uint32_t>(header.data(), offset, payload_crc ^ 0xffffffffU);
        PutLe<uint32_t>(header.data(), offset, 0);
        PutLe<uint64_t>(header.data(), offset, total_bytes);
        const uint32_t header_crc = Crc32(header.data(), header.size()) ^ 0xffffffffU;
        offset = kHeaderCrcOffset;
        PutLe<uint32_t>(header.data(), offset, header_crc);
        WriteAtAll(fd, header.data(), header.size(), 0);
        close(fd);
    } catch (...) {
        close(fd);
        unlinkat(state.directory_fd, name.c_str(), 0);
        throw;
    }
    return {name, status.st_dev, status.st_ino};
}

Run CreateRun(SidecarSorter::State& state, const std::vector<SortRecord>& records,
              const std::function<bool()>& cancelled) {
    uint64_t payload_bytes = 0;
    for (const SortRecord& record : records)
        payload_bytes = AddChecked(payload_bytes, AddChecked(12, record.key.size()));
    return CreateRunStream(
        state, records.size(), payload_bytes,
        [&](const auto& emit) {
            for (const SortRecord& record : records)
                emit(record);
        },
        cancelled);
}

void FlushBuffer(SidecarSorter::State& state, SidecarSorter::Census& census, const std::function<bool()>& cancelled) {
    if (cancelled && cancelled())
        throw SidecarSortCancelled();
    std::sort(state.buffer.begin(), state.buffer.end(), Less);
    state.runs.push_back(CreateRun(state, state.buffer, cancelled));
    state.buffer.clear();
    state.buffer_payload = 0;
    census.run_count = std::max(census.run_count, state.runs.size());
}

Run MergeRuns(SidecarSorter::State& state, const std::vector<Run>& runs, size_t first, size_t last,
              SidecarSorter::Census& census, const std::function<bool()>& cancelled) {
    struct Cursor {
        OwnedFd fd;
        Reader reader;
        SortRecord record;
        Cursor(int file, const Run& run, size_t max_record) : fd(file), reader(fd.get(), max_record), run(run) {}
        Run run;
    };
    struct Entry {
        size_t cursor;
        SortRecord record;
    };
    auto compare = [](const Entry& left, const Entry& right) { return Less(right.record, left.record); };
    std::vector<std::unique_ptr<Cursor>> cursors;
    std::priority_queue<Entry, std::vector<Entry>, decltype(compare)> queue(compare);
    for (size_t i = first; i < last; ++i) {
        if (cancelled && cancelled())
            throw SidecarSortCancelled();
        const int fd = OpenOwned(state, runs[i], O_RDONLY);
        try {
            auto cursor = std::make_unique<Cursor>(fd, runs[i], state.options.max_record_bytes);
            if (cursor->reader.Next(cursor->record, cancelled))
                queue.push({cursors.size(), cursor->record});
            cursors.push_back(std::move(cursor));
        } catch (...) {
            throw;
        }
    }
    census.max_merge_cursors = std::max(census.max_merge_cursors, cursors.size());
    uint64_t merged_count = 0;
    uint64_t merged_payload = 0;
    for (const auto& cursor : cursors) {
        merged_count = AddChecked(merged_count, cursor->reader.expected_count);
        merged_payload = AddChecked(merged_payload, cursor->reader.expected_payload);
    }
    return CreateRunStream(
        state, merged_count, merged_payload,
        [&](const auto& emit) {
            while (!queue.empty()) {
                if (cancelled && cancelled())
                    throw SidecarSortCancelled();
                Entry entry = queue.top();
                queue.pop();
                emit(entry.record);
                Cursor& cursor = *cursors[entry.cursor];
                if (cursor.reader.Next(cursor.record, cancelled)) {
                    queue.push({entry.cursor, cursor.record});
                }
            }
        },
        cancelled);
}

void RemoveOwnedRun(SidecarSorter::State& state, const Run& run) {
    OwnedFd fd(OpenOwned(state, run, O_RDONLY));
    struct stat status {};
    if (fstatat(state.directory_fd, run.name.c_str(), &status, AT_SYMLINK_NOFOLLOW) != 0 ||
        status.st_dev != run.device || status.st_ino != run.inode)
        throw std::runtime_error("sorter run ownership changed");
    if (unlinkat(state.directory_fd, run.name.c_str(), 0) != 0)
        ThrowErrno("remove sorter run");
}

void RemoveRuns(SidecarSorter::State& state, const std::vector<Run>& old_runs, const std::vector<Run>& kept) {
    for (const Run& old : old_runs) {
        if (std::find_if(kept.begin(), kept.end(), [&](const Run& run) { return run.name == old.name; }) != kept.end())
            continue;
        RemoveOwnedRun(state, old);
    }
}

SidecarSorter::SidecarSorter(const std::filesystem::path& directory, std::string semantic_token)
    : SidecarSorter(directory, std::move(semantic_token), Options{}) {}

SidecarSorter::SidecarSorter(const std::filesystem::path& directory, std::string semantic_token, Options options)
    : state_(std::make_unique<State>()) {
    if (semantic_token.empty() || semantic_token == "." || semantic_token == ".." ||
        semantic_token.find('/') != std::string::npos)
        throw std::invalid_argument("sorter token must be one path component");
    if (options.buffer_payload_bytes == 0 || options.max_record_bytes < 12 ||
        options.max_record_bytes > options.buffer_payload_bytes || options.fanin < 2)
        throw std::invalid_argument("invalid sorter options");
    state_->directory_fd = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (state_->directory_fd < 0)
        ThrowErrno("open sorter directory");
    struct stat status {};
    if (fstat(state_->directory_fd, &status) != 0 || !S_ISDIR(status.st_mode))
        throw std::runtime_error("sorter directory is not a directory");
    state_->token = std::move(semantic_token);
    state_->options = options;
}

SidecarSorter::~SidecarSorter() {}

SidecarSorter::SidecarSorter(SidecarSorter&& other) noexcept
    : state_(std::move(other.state_)), census_(other.census_) {}

SidecarSorter& SidecarSorter::operator=(SidecarSorter&& other) noexcept {
    if (this != &other) {
        state_ = std::move(other.state_);
        census_ = other.census_;
    }
    return *this;
}

void SidecarSorter::Add(std::string_view key, uint64_t value) {
    Add(SortRecord{std::string(key), value});
}

void SidecarSorter::Add(SortRecord record) {
    if (state_ == nullptr || state_->finished)
        throw std::logic_error("sorter is finished");
    if (record.key.size() > std::numeric_limits<size_t>::max() - 12 || record.key.size() > UINT32_MAX - 12)
        throw std::invalid_argument("sorter record size overflow");
    const size_t record_bytes = 12 + record.key.size();
    if (record_bytes > state_->options.max_record_bytes)
        throw std::invalid_argument("sorter record exceeds limit");
    if (!state_->buffer.empty() && (record_bytes > state_->options.buffer_payload_bytes - state_->buffer_payload))
        FlushBuffer(*state_, census_, {});
    state_->buffer_payload = AddChecked(state_->buffer_payload, record_bytes);
    state_->buffer.push_back(std::move(record));
    census_.max_buffer_payload = std::max(census_.max_buffer_payload, state_->buffer_payload);
    ++census_.records;
}

void SidecarSorter::Finish(const std::function<void(std::string_view, uint64_t)>& emit,
                           const std::function<bool()>& cancelled) {
    if (state_ == nullptr || state_->finished)
        throw std::logic_error("sorter is finished");
    if (!state_->buffer.empty())
        FlushBuffer(*state_, census_, cancelled);
    while (state_->runs.size() > 1) {
        if (cancelled && cancelled())
            throw SidecarSortCancelled();
        std::vector<Run> next;
        for (size_t offset = 0; offset < state_->runs.size(); offset += state_->options.fanin) {
            const size_t end = std::min(state_->runs.size(), offset + state_->options.fanin);
            if (end - offset == 1) {
                next.push_back(state_->runs[offset]);
                continue;
            }
            next.push_back(MergeRuns(*state_, state_->runs, offset, end, census_, cancelled));
        }
        RemoveRuns(*state_, state_->runs, next);
        state_->runs = std::move(next);
        ++census_.merge_passes;
    }
    if (!state_->runs.empty()) {
        const Run run = state_->runs.front();
        OwnedFd fd(OpenOwned(*state_, run, O_RDONLY));
        try {
            Reader reader(fd.get(), state_->options.max_record_bytes);
            SortRecord record;
            while (reader.Next(record, cancelled))
                emit(record.key, record.value);
        } catch (...) {
            throw;
        }
    }
    for (const Run& run : state_->runs)
        RemoveOwnedRun(*state_, run);
    state_->runs.clear();
    state_->finished = true;
}

} // namespace deltakernel::detail
