#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace epoch_si_poc {

struct DeltaDiagnostics;

class FileWal {
public:
    enum class OpenMode { kExisting, kCreateNew };

    explicit FileWal(const std::string& path, OpenMode mode = OpenMode::kExisting);
    FileWal(int directory_fd, const std::string& name, OpenMode mode);
    ~FileWal();

    FileWal(const FileWal&) = delete;
    FileWal& operator=(const FileWal&) = delete;
    FileWal(FileWal&& other) noexcept;
    FileWal& operator=(FileWal&& other) noexcept;

    std::vector<uint8_t> ReadAll() const;
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

private:
    int fd_ = -1;
    size_t end_offset_ = 0;
    size_t max_write_chunk_ = SIZE_MAX;
    size_t write_calls_ = 0;
    size_t sync_calls_ = 0;
    std::shared_ptr<DeltaDiagnostics> diagnostics_;
};

} // namespace epoch_si_poc
