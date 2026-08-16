#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace epoch_si_poc {

struct DeltaDiagnostics;

class FileWal {
public:
    static constexpr uint32_t kSegmentHeaderBytes = 72;
    struct SegmentInfo {
        uint64_t wal_generation = 0;
        uint64_t segment_id = 0;
        uint64_t previous_segment_id = 0;
        uint64_t first_epoch = 0;
        uint64_t first_commit_seq = 0;
    };
    enum class OpenMode { kExisting, kCreateNew };

    explicit FileWal(const std::string& path, OpenMode mode = OpenMode::kExisting);
    FileWal(int directory_fd, const std::string& name, OpenMode mode);
    static FileWal OpenSegmentAt(int directory_fd, const std::string& name, const SegmentInfo& expected);
    static FileWal CreateSegmentAt(int directory_fd, const std::string& name, const SegmentInfo& info);
    ~FileWal();

    FileWal(const FileWal&) = delete;
    FileWal& operator=(const FileWal&) = delete;
    FileWal(FileWal&& other) noexcept;
    FileWal& operator=(FileWal&& other) noexcept;

    void ReadAt(size_t offset, uint8_t* output, size_t bytes) const;
    void RequireUnchangedSize() const;
    void Append(const std::vector<uint8_t>& bytes, size_t limit = SIZE_MAX);
    void Sync();
    void TruncateAndSync(size_t bytes);

    size_t size() const {
        return end_offset_;
    }
    size_t write_calls_for_test() const {
        return write_calls_;
    }
    size_t sync_calls_for_test() const {
        return sync_calls_;
    }
    void SetMaxWriteChunkForTest(size_t bytes) {
        max_write_chunk_ = bytes == 0 ? 1 : bytes;
    }
    void CloseForTest();
    void SetDiagnostics(std::shared_ptr<DeltaDiagnostics> diagnostics) {
        diagnostics_ = std::move(diagnostics);
    }
    bool is_segment() const noexcept {
        return segment_info_.has_value();
    }
    const SegmentInfo& segment_info() const;

private:
    int fd_ = -1;
    size_t end_offset_ = 0;
    size_t data_offset_ = 0;
    size_t max_write_chunk_ = SIZE_MAX;
    size_t write_calls_ = 0;
    size_t sync_calls_ = 0;
    std::shared_ptr<DeltaDiagnostics> diagnostics_;
    std::optional<SegmentInfo> segment_info_;

    void LoadSegmentHeader(const SegmentInfo& expected);
    void WriteSegmentHeader(const SegmentInfo& info);
};

} // namespace epoch_si_poc
