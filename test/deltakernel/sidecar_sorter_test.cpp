#include "deltakernel/sidecar_sorter.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {
class TempDirectory {
public:
    TempDirectory() {
        path_ = std::filesystem::temp_directory_path() / "rmdb-sorter-test";
        for (size_t i = 0; i < 100; ++i) {
            auto candidate = path_;
            candidate += std::to_string(i);
            if (std::filesystem::create_directory(candidate)) {
                path_ = std::move(candidate);
                return;
            }
        }
        throw std::runtime_error("cannot create test directory");
    }
    ~TempDirectory() {
        std::filesystem::remove_all(path_);
    }
    const std::filesystem::path& path() const {
        return path_;
    }

private:
    std::filesystem::path path_;
};
} // namespace

TEST(SidecarSorterTest, SortsBinaryKeysAndRemovesRuns) {
    TempDirectory directory;
    deltakernel::detail::SidecarSorter::Options options;
    options.buffer_payload_bytes = 20;
    options.max_record_bytes = 20;
    options.fanin = 2;
    deltakernel::detail::SidecarSorter sorter(directory.path(), "basic", options);
    sorter.Add(std::string("b\0", 2), 3);
    sorter.Add("a", 4);
    sorter.Add("a", 1);
    sorter.Add("", 7);
    std::vector<std::pair<std::string, uint64_t>> actual;
    sorter.Finish([&](std::string_view key, uint64_t value) { actual.emplace_back(key, value); });
    EXPECT_EQ(actual,
              (std::vector<std::pair<std::string, uint64_t>>{{"", 7}, {"a", 1}, {"a", 4}, {std::string("b\0", 2), 3}}));
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator(directory.path()), {}), 0);
    EXPECT_GT(sorter.census().merge_passes, 0U);
}

TEST(SidecarSorterTest, EmptyInputAndCallbackFailureCleanUp) {
    TempDirectory directory;
    deltakernel::detail::SidecarSorter sorter(directory.path(), "empty");
    sorter.Finish([](std::string_view, uint64_t) {});
    EXPECT_TRUE(std::filesystem::is_empty(directory.path()));

    deltakernel::detail::SidecarSorter failing(directory.path(), "failure");
    failing.Add("key", 1);
    EXPECT_THROW(failing.Finish([](std::string_view, uint64_t) { throw std::runtime_error("stop"); }),
                 std::runtime_error);
}

TEST(SidecarSorterTest, EnforcesRecordLimit) {
    TempDirectory directory;
    deltakernel::detail::SidecarSorter::Options options;
    options.max_record_bytes = 12;
    deltakernel::detail::SidecarSorter sorter(directory.path(), "limit", options);
    EXPECT_THROW(sorter.Add("x", 1), std::invalid_argument);
}

TEST(SidecarSorterTest, RejectsTruncatedAndCorruptRuns) {
    for (int corruption = 0; corruption < 3; ++corruption) {
        const bool truncate = corruption == 0;
        TempDirectory directory;
        deltakernel::detail::SidecarSorter::Options options;
        options.buffer_payload_bytes = 24;
        options.max_record_bytes = 24;
        deltakernel::detail::SidecarSorter sorter(directory.path(), truncate ? "truncated" : "corrupt", options);
        sorter.Add("first", 1);
        sorter.Add("second", 2);
        const auto run = *std::filesystem::directory_iterator(directory.path());
        if (truncate) {
            std::filesystem::resize_file(run.path(), 3);
        } else {
            std::fstream file(run.path(), std::ios::in | std::ios::out | std::ios::binary);
            file.seekp(corruption == 1 ? 36 : 48);
            char byte = static_cast<char>(0x7f);
            file.write(&byte, 1);
        }
        EXPECT_THROW(sorter.Finish([](std::string_view, uint64_t) {}), std::exception);
    }
}

TEST(SidecarSorterTest, CallbackFailureRemovesOwnedRuns) {
    TempDirectory directory;
    deltakernel::detail::SidecarSorter::Options options;
    options.buffer_payload_bytes = 24;
    options.max_record_bytes = 24;
    deltakernel::detail::SidecarSorter sorter(directory.path(), "callback", options);
    sorter.Add("first", 1);
    sorter.Add("second", 2);
    EXPECT_THROW(sorter.Finish([](std::string_view, uint64_t) { throw std::runtime_error("callback"); }),
                 std::runtime_error);
    sorter = deltakernel::detail::SidecarSorter(directory.path(), "after-callback", options);
    EXPECT_TRUE(std::filesystem::is_empty(directory.path()));
}

TEST(SidecarSorterTest, CancellationRemovesMidMergeRuns) {
    TempDirectory directory;
    deltakernel::detail::SidecarSorter::Options options;
    options.buffer_payload_bytes = 24;
    options.max_record_bytes = 24;
    options.fanin = 2;
    deltakernel::detail::SidecarSorter sorter(directory.path(), "cancel", options);
    for (uint64_t value = 0; value < 12; ++value)
        sorter.Add(std::to_string(value), value);
    bool cancelled = false;
    EXPECT_THROW(sorter.Finish([](std::string_view, uint64_t) {},
                               [&] {
                                   cancelled = true;
                                   return true;
                               }),
                 deltakernel::detail::SidecarSortCancelled);
    sorter = deltakernel::detail::SidecarSorter(directory.path(), "after-cancel", options);
    EXPECT_TRUE(cancelled);
    EXPECT_TRUE(std::filesystem::is_empty(directory.path()));
}

TEST(SidecarSorterTest, RejectsRunReplacementBySymlink) {
    TempDirectory directory;
    deltakernel::detail::SidecarSorter::Options options;
    options.buffer_payload_bytes = 24;
    options.max_record_bytes = 24;
    deltakernel::detail::SidecarSorter sorter(directory.path(), "symlink", options);
    sorter.Add("first", 1);
    sorter.Add("second", 2);
    const auto run = *std::filesystem::directory_iterator(directory.path());
    const auto saved = directory.path() / "saved-run";
    std::filesystem::rename(run.path(), saved);
    std::filesystem::create_symlink(saved.filename(), run.path());
    EXPECT_THROW(sorter.Finish([](std::string_view, uint64_t) {}), std::exception);
}
