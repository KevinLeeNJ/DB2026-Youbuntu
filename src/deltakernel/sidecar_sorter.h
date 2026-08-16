#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace deltakernel::detail {

struct SortRecord {
    std::string key;
    uint64_t value = 0;
};

class SidecarSortCancelled final : public std::exception {
public:
    const char* what() const noexcept override {
        return "sidecar sort cancelled";
    }
};

class SidecarSorter {
public:
    struct Options {
        size_t buffer_payload_bytes = 4 * 1024 * 1024;
        size_t max_record_bytes = 1024 * 1024;
        size_t fanin = 8;
    };

    struct Census {
        size_t max_buffer_payload = 0;
        size_t max_merge_cursors = 0;
        size_t run_count = 0;
        size_t merge_passes = 0;
        uint64_t records = 0;
    };

    SidecarSorter(const std::filesystem::path& directory, std::string semantic_token);
    SidecarSorter(const std::filesystem::path& directory, std::string semantic_token, Options options);
    ~SidecarSorter();

    SidecarSorter(const SidecarSorter&) = delete;
    SidecarSorter& operator=(const SidecarSorter&) = delete;
    SidecarSorter(SidecarSorter&& other) noexcept;
    SidecarSorter& operator=(SidecarSorter&& other) noexcept;

    void Add(SortRecord record);
    void Add(std::string_view key, uint64_t value);
    void Finish(const std::function<void(std::string_view, uint64_t)>& emit,
                const std::function<bool()>& cancelled = {});

    const Census& census() const noexcept {
        return census_;
    }

    struct State;

private:
    std::unique_ptr<State> state_;
    Census census_;
};

} // namespace deltakernel::detail
