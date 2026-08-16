#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace epoch_si_poc {

class FileWal {
public:
    enum class OpenMode { kExisting, kCreateNew };

    explicit FileWal(const std::string& path, OpenMode mode = OpenMode::kExisting);
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
    void SetMaxWriteChunkForTest(size_t bytes) {
        max_write_chunk_ = bytes == 0 ? 1 : bytes;
    }
    void CloseForTest();

private:
    int fd_ = -1;
    size_t end_offset_ = 0;
    size_t max_write_chunk_ = SIZE_MAX;
};

} // namespace epoch_si_poc
