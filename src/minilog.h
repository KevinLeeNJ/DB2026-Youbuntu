#ifndef MINILOG_H
#define MINILOG_H

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace minilog {

enum class LogLevel { DEBUG, INFO, WARN, ERROR };

class Logger {
public:
    static Logger& get() {
        static Logger instance;
        return instance;
    }

    void init(const std::string& file_path, bool async = true) {
        std::scoped_lock lifecycle_lock(lifecycle_mutex_);
        accepting_.store(false, std::memory_order_release);
        std::unique_lock admission_lock(admission_mutex_);
        stop_locked();

        FILE* file = std::fopen(file_path.c_str(), "a");
        if (!file) {
            throw std::runtime_error("failed to open file " + file_path);
        }
        std::setvbuf(file, nullptr, _IOFBF, kFileBufferSize);
        init_locked(file, async, true);
    }

    void init(FILE* file, bool async = true, bool own_file = false) {
        if (!file) {
            throw std::invalid_argument("file handle is null");
        }
        std::scoped_lock lifecycle_lock(lifecycle_mutex_);
        accepting_.store(false, std::memory_order_release);
        std::unique_lock admission_lock(admission_mutex_);
        stop_locked();
        init_locked(file, async, own_file);
    }

    void stop() {
        std::scoped_lock lifecycle_lock(lifecycle_mutex_);
        accepting_.store(false, std::memory_order_release);
        std::unique_lock admission_lock(admission_mutex_);
        stop_locked();
    }

    void flush() {
        std::scoped_lock lifecycle_lock(lifecycle_mutex_);
        flush_locked();
    }

    bool has_error() const noexcept {
        return write_error_count_.load(std::memory_order_acquire) != 0;
    }

    std::uint64_t write_error_count() const noexcept {
        return write_error_count_.load(std::memory_order_acquire);
    }

    std::uint64_t dropped_count() const noexcept {
        return dropped_count_.load(std::memory_order_acquire);
    }

    int last_error_code() const noexcept {
        return last_error_code_.load(std::memory_order_acquire);
    }

    void set_level(LogLevel level) noexcept {
        min_level_.store(level, std::memory_order_relaxed);
    }

    LogLevel get_level() const noexcept {
        return static_cast<LogLevel>(min_level_.load(std::memory_order_relaxed));
    }

    ~Logger() {
        stop();
    }

    void log(LogLevel level, const char* file, int line, const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
        __attribute__((format(printf, 5, 6)))
#endif
    {
        if (level < min_level_.load(std::memory_order_relaxed)) {
            return;
        }
        if (!accepting_.load(std::memory_order_acquire)) {
            return;
        }
        std::shared_lock admission_lock(admission_mutex_);
        if (!accepting_.load(std::memory_order_acquire)) {
            return;
        }

        std::array<char, kLineBufferSize> line_buffer;

        const auto header_len = format_header(line_buffer.data(), line_buffer.size(), level, file, line);
        if (header_len < 0 || static_cast<std::size_t>(header_len) >= line_buffer.size() - 1) {
            return;
        }

        va_list args;
        va_start(args, format);
        const auto body_len =
            std::vsnprintf(line_buffer.data() + header_len,
                           line_buffer.size() - static_cast<std::size_t>(header_len) - 1, format, args);
        va_end(args);

        if (body_len < 0) {
            return;
        }

        const auto body_cap = line_buffer.size() - static_cast<std::size_t>(header_len) - 1;
        auto total_len = static_cast<std::size_t>(header_len) + static_cast<std::size_t>(body_len);
        if (static_cast<std::size_t>(body_len) >= body_cap) {
            total_len = line_buffer.size() - 2;
            apply_truncation_marker(line_buffer.data(), static_cast<std::size_t>(header_len), total_len);
        }
        line_buffer[total_len++] = '\n';

        if (!async_.load(std::memory_order_acquire)) {
            std::scoped_lock file_lock(file_mutex_);
            if (!file_) {
                return;
            }
            write_bytes_locked(line_buffer.data(), total_len);
            return;
        }

        publish(line_buffer.data(), total_len);
    }

private:
    using Clock = std::chrono::system_clock;

    static constexpr auto kFileBufferSize = std::size_t{1024 * 1024};
    static constexpr auto kLineBufferSize = std::size_t{510};
    static constexpr auto kInitialQueueCapacity = std::size_t{1024};
    static constexpr auto kMaxQueueCapacity = std::size_t{2 * kInitialQueueCapacity};

    static constexpr std::uint16_t kBarrierBit = 0x8000;
    static constexpr std::uint16_t kSizeMask = 0x7FFF;

    struct FlushWaiter {
        std::mutex mutex;
        std::condition_variable cv;
        bool done = false;
        bool failed = false;
    };

    struct MessageSlot {
        std::uint16_t size_and_flag = 0;
        std::array<char, kLineBufferSize> bytes;

        bool is_barrier() const noexcept {
            return (size_and_flag & kBarrierBit) != 0;
        }

        std::uint16_t payload_size() const noexcept {
            return size_and_flag & kSizeMask;
        }

        void set_log_size(std::uint16_t size) noexcept {
            size_and_flag = static_cast<std::uint16_t>(size & kSizeMask);
        }

        void set_barrier() noexcept {
            size_and_flag = kBarrierBit;
        }
    };

    using ReadyQueue = std::vector<MessageSlot>;

    struct TimeCache {
        std::time_t second = {};
        std::array<char, 24> prefix{};
        int prefix_len = 0;
    };

    static void store_waiter(MessageSlot& slot, FlushWaiter* waiter) noexcept {
        std::memcpy(slot.bytes.data(), &waiter, sizeof(waiter));
    }

    static FlushWaiter* load_waiter(const MessageSlot& slot) noexcept {
        FlushWaiter* waiter;
        std::memcpy(&waiter, slot.bytes.data(), sizeof(waiter));
        return waiter;
    }

    static void notify_waiter(FlushWaiter* waiter, bool failed) noexcept {
        std::scoped_lock lock(waiter->mutex);
        waiter->done = true;
        waiter->failed = failed;
        waiter->cv.notify_one();
    }

    void init_locked(FILE* file, bool async, bool own_file) {
        {
            std::scoped_lock file_lock(file_mutex_);
            file_ = file;
            own_file_ = own_file;
        }
        {
            std::scoped_lock queue_lock(queue_mutex_);
            ready_.clear();
            draining_.clear();
            ready_.reserve(kInitialQueueCapacity);
            draining_.reserve(kInitialQueueCapacity);
        }
        async_.store(async, std::memory_order_release);
        running_.store(async, std::memory_order_release);
        write_error_count_.store(0, std::memory_order_release);
        last_error_code_.store(0, std::memory_order_release);
        dropped_count_.store(0, std::memory_order_release);
        if (!async) {
            accepting_.store(true, std::memory_order_release);
            return;
        }

        try {
            worker_ = std::thread(&Logger::worker_loop, this);
            accepting_.store(true, std::memory_order_release);
        } catch (...) {
            accepting_.store(false, std::memory_order_release);
            running_.store(false, std::memory_order_release);
            {
                std::scoped_lock queue_lock(queue_mutex_);
                ready_.clear();
                draining_.clear();
            }
            {
                std::scoped_lock file_lock(file_mutex_);
                close_file_locked();
                file_ = nullptr;
                own_file_ = false;
            }
            throw;
        }
    }

    void publish(const char* data, std::size_t size) {
        bool was_empty;
        {
            std::scoped_lock lock(queue_mutex_);
            if (ready_.size() >= kMaxQueueCapacity) {
                dropped_count_.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            was_empty = ready_.empty();
            ready_.emplace_back();
            ready_.back().set_log_size(static_cast<std::uint16_t>(size));
            std::memcpy(ready_.back().bytes.data(), data, size);
        }
        if (was_empty) {
            cv_.notify_one();
        }
    }

    void publish_barrier(FlushWaiter* waiter) {
        std::unique_lock lock(queue_mutex_);
        cv_.wait(lock,
                 [this] { return ready_.size() < kMaxQueueCapacity || !running_.load(std::memory_order_acquire); });
        if (ready_.size() >= kMaxQueueCapacity) {
            lock.unlock();
            notify_waiter(waiter, true);
            return;
        }
        const bool was_empty = ready_.empty();
        ready_.emplace_back();
        ready_.back().set_barrier();
        store_waiter(ready_.back(), waiter);
        lock.unlock();
        if (was_empty) {
            cv_.notify_one();
        }
    }

    void flush_locked() {
        if (!async_.load(std::memory_order_acquire) || !accepting_.load(std::memory_order_acquire)) {
            std::scoped_lock file_lock(file_mutex_);
            flush_file_locked();
            return;
        }

        FlushWaiter waiter;
        publish_barrier(&waiter);
        std::unique_lock wait_lock(waiter.mutex);
        waiter.cv.wait(wait_lock, [&waiter] { return waiter.done; });
    }

    void stop_locked() {
        accepting_.store(false, std::memory_order_release);
        running_.store(false, std::memory_order_release);
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
        {
            std::scoped_lock lock(queue_mutex_);
            ready_.swap(draining_);
        }
        write_batch(draining_);
        std::scoped_lock lock(file_mutex_);
        if (file_) {
            flush_file_locked();
            close_file_locked();
            file_ = nullptr;
            own_file_ = false;
        }
    }

    void worker_loop() {
        for (;;) {
            {
                std::unique_lock lock(queue_mutex_);
                cv_.wait(lock, [this] { return !ready_.empty() || !running_.load(std::memory_order_acquire); });
                if (ready_.empty()) {
                    return;
                }
                ready_.swap(draining_);
            }
            cv_.notify_all();
            write_batch(draining_);
        }
    }

    void write_batch(ReadyQueue& batch) {
        if (batch.empty()) {
            return;
        }

        std::scoped_lock file_lock(file_mutex_);
        bool failed = (file_ == nullptr);

        for (const auto& message : batch) {
            if (message.is_barrier()) {
                if (!failed && !flush_file_locked()) {
                    failed = true;
                }
                notify_waiter(load_waiter(message), failed);
                continue;
            }
            if (failed) {
                continue;
            }
            if (!write_bytes_locked(message.bytes.data(), message.payload_size())) {
                failed = true;
            }
        }

        if (!failed) {
            flush_file_locked();
        }
        batch.clear();
    }

    bool write_bytes_locked(const char* data, std::size_t size) {
        if (!file_) {
            return false;
        }
        errno = 0;
        const auto written = std::fwrite(data, 1, size, file_);
        if (written != size) {
            record_write_error_locked();
            return false;
        }
        return true;
    }

    bool flush_file_locked() {
        if (!file_) {
            return false;
        }
        errno = 0;
        if (std::fflush(file_) != 0) {
            record_write_error_locked();
            return false;
        }
        return true;
    }

    void close_file_locked() {
        if (!file_) {
            return;
        }
        if (!own_file_ || file_ == stdout || file_ == stderr) {
            return;
        }
        errno = 0;
        if (std::fclose(file_) != 0) {
            record_write_error_locked();
        }
    }

    void record_write_error_locked() {
        write_error_count_.fetch_add(1, std::memory_order_acq_rel);
        const int err = errno != 0 ? errno : EIO;
        last_error_code_.store(err, std::memory_order_release);
        if (file_) {
            std::clearerr(file_);
        }
    }

    static void apply_truncation_marker(char* buffer, std::size_t header_len, std::size_t total_len) {
        if (total_len <= header_len) {
            return;
        }
        static constexpr char kTruncatedSuffix[] = "...[truncated]";
        constexpr auto suffix_len = sizeof(kTruncatedSuffix) - 1;
        const auto available = total_len - header_len;
        const auto copy_len = suffix_len < available ? suffix_len : available;
        std::memcpy(buffer + total_len - copy_len, kTruncatedSuffix + suffix_len - copy_len, copy_len);
    }

    static int format_header(char* out, std::size_t cap, LogLevel level, const char* file, int line) {
        static constexpr std::array<const char*, 4> kLevelNames = {"DEBUG", "INFO", "WARN", "ERROR"};

        const auto now = Clock::now();
        const auto epoch = now.time_since_epoch();
        const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(epoch);
        const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(epoch - seconds);
        const auto current_second = static_cast<std::time_t>(seconds.count());

        thread_local TimeCache cache;

        if (cache.second != current_second || cache.prefix_len == 0) {
            struct tm tm_info;
            localtime_r(&current_second, &tm_info);
            const int prefix_len = std::snprintf(
                cache.prefix.data(), cache.prefix.size(), "%04d-%02d-%02d %02d:%02d:%02d.", tm_info.tm_year + 1900,
                tm_info.tm_mon + 1, tm_info.tm_mday, tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec);
            if (prefix_len < 0 || static_cast<std::size_t>(prefix_len) >= cache.prefix.size()) {
                cache.prefix_len = 0;
                return -1;
            }
            cache.prefix_len = prefix_len;
            cache.second = current_second;
        }

        const auto* level_name = kLevelNames[static_cast<std::size_t>(level)];
        const int body_len = std::snprintf(out, cap, "%s %.*s%03d [%s:%d] ", level_name, cache.prefix_len,
                                           cache.prefix.data(), static_cast<int>(millis.count()), file, line);
        if (body_len < 0) {
            return -1;
        }
        return body_len;
    }

    FILE* file_ = nullptr;
    bool own_file_ = false;
    std::atomic<bool> async_{true};
    std::atomic<bool> running_{false};
    std::atomic<bool> accepting_{false};
    std::atomic<LogLevel> min_level_{LogLevel::DEBUG};
    std::atomic<std::uint64_t> write_error_count_{0};
    std::atomic<std::uint64_t> dropped_count_{0};
    std::atomic<int> last_error_code_{0};
    std::mutex lifecycle_mutex_;
    std::shared_mutex admission_mutex_;

    std::mutex queue_mutex_;
    std::mutex file_mutex_;
    std::condition_variable cv_;
    std::thread worker_;
    ReadyQueue ready_;
    ReadyQueue draining_;
};

constexpr const char* basename(const char* path) noexcept {
    const char* file = path;
    while (*path) {
        if (*path == '/' || *path == '\\') {
            file = path + 1;
        }
        path++;
    }
    return file;
}
} // namespace minilog

#define MINILOG_IMPL(level, ...) \
    do { \
        auto& logger_ = minilog::Logger::get(); \
        if (logger_.get_level() <= (level)) { \
            constexpr const char* bname = minilog::basename(__FILE__); \
            logger_.log((level), bname, __LINE__, __VA_ARGS__); \
        } \
    } while (0)

#define LOG_DEBUG(...) MINILOG_IMPL(minilog::LogLevel::DEBUG, __VA_ARGS__)
#define LOG_INFO(...) MINILOG_IMPL(minilog::LogLevel::INFO, __VA_ARGS__)
#define LOG_WARN(...) MINILOG_IMPL(minilog::LogLevel::WARN, __VA_ARGS__)
#define LOG_ERROR(...) MINILOG_IMPL(minilog::LogLevel::ERROR, __VA_ARGS__)

#endif // MINILOG_H
