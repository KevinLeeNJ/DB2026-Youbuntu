// Standalone WAL stabilization microbenchmark.  It intentionally never opens
// database files: every tested file lives below one mkdtemp-created directory.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <thread>
#include <condition_variable>
#include <unistd.h>
#include <vector>

namespace {

constexpr size_t kSizes[] = {4 * 1024, 16 * 1024, 64 * 1024};
volatile uint64_t g_io_sink = 0;

enum class Mode { kAppendFdatasync, kPreallocOverwriteFdatasync, kPwritev2RwfDsync };

struct Options {
    std::string directory = "/tmp";
    std::string output = "wal_sync_microbench";
    size_t samples = 1000;
    size_t warmup = 100;
    std::string schedule = "interleaved";
    uint64_t seed = 20260808;
    bool self_check = false;
    bool group_sync = false;
    size_t group_samples = 256;
};

struct GroupResult {
    unsigned depth = 0;
    std::vector<uint64_t> wait_ns;
    uint64_t crossed_epochs = 0;
    unsigned max_syncs_in_flight = 0;
    int errors = 0;
};

struct Result {
    std::string mode;
    size_t bytes = 0;
    std::vector<uint64_t> samples_ns;
    int errors = 0;
    int first_errno = 0;
    bool skipped = false;
    std::string skip_reason;
};

struct BenchmarkCase {
    Mode mode;
    size_t bytes;
    std::string path;
    int fd = -1;
    std::vector<unsigned char> payload;
    Result result;
};

const char* ModeName(Mode mode) {
    switch (mode) {
    case Mode::kAppendFdatasync:
        return "append_pwrite_fdatasync";
    case Mode::kPreallocOverwriteFdatasync:
        return "prealloc_overwrite_pwrite_fdatasync";
    case Mode::kPwritev2RwfDsync:
        return "pwritev2_rwf_dsync";
    }
    return "unknown";
}

uint64_t NowNs() {
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) {
        std::perror("clock_gettime(CLOCK_MONOTONIC_RAW)");
        std::exit(2);
    }
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}

void Usage(const char* program) {
    std::cerr << "Usage: " << program
              << " [--dir DIRECTORY] [--samples N] [--warmup N] [--output PATH_PREFIX]\n"
                 "       [--schedule interleaved|serial] [--seed N]\n"
                 "       [--group-sync] [--group-samples N]\n"
                 "\n"
                 "Creates a private mkdtemp directory below --dir, benchmarks only files it creates,\n"
                 "then removes that directory.  PATH_PREFIX produces PATH_PREFIX.csv and PATH_PREFIX.json.\n"
                 "Defaults: --dir /tmp --samples 1000 --warmup 100 --schedule interleaved\n"
                 "          --seed 20260808 --output wal_sync_microbench\n"
                 "Use --self-check to verify statistics/argument plumbing without performing I/O.\n";
}

bool ParseSize(const char* text, size_t* value) {
    char* end = nullptr;
    errno = 0;
    unsigned long long parsed = std::strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || parsed == 0 || parsed > (1ULL << 31)) {
        return false;
    }
    *value = static_cast<size_t>(parsed);
    return true;
}

bool ParseSeed(const char* text, uint64_t* value) {
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return false;
    }
    *value = static_cast<uint64_t>(parsed);
    return true;
}

bool ParseArgs(int argc, char** argv, Options* options) {
    for (int index = 1; index < argc; ++index) {
        std::string argument(argv[index]);
        if (argument == "--help" || argument == "-h") {
            Usage(argv[0]);
            std::exit(0);
        }
        if (argument == "--self-check") {
            options->self_check = true;
            continue;
        }
        if ((argument == "--dir" || argument == "--samples" || argument == "--warmup" || argument == "--output" ||
             argument == "--schedule" || argument == "--seed" || argument == "--group-samples") &&
            index + 1 < argc) {
            const char* value = argv[++index];
            if (argument == "--dir") {
                options->directory = value;
            } else if (argument == "--output") {
                options->output = value;
            } else if (argument == "--schedule") {
                options->schedule = value;
                if (options->schedule != "interleaved" && options->schedule != "serial") {
                    std::cerr << "Invalid value for --schedule: " << value << '\n';
                    return false;
                }
            } else if (argument == "--seed") {
                if (!ParseSeed(value, &options->seed)) {
                    std::cerr << "Invalid value for --seed: " << value << '\n';
                    return false;
                }
            } else if (!ParseSize(value, argument == "--samples" ? &options->samples
                                                           : argument == "--warmup" ? &options->warmup
                                                                                    : &options->group_samples)) {
                std::cerr << "Invalid value for " << argument << ": " << value << '\n';
                return false;
            }
            continue;
        }
        if (argument == "--group-sync") {
            options->group_sync = true;
            continue;
        }
        std::cerr << "Unknown or incomplete argument: " << argument << '\n';
        return false;
    }
    return true;
}

bool IsUnsupportedRwfDsync(int error) {
    return error == EOPNOTSUPP || error == ENOTSUP || error == EINVAL || error == ENOSYS;
}

bool WriteAndStabilize(int fd, Mode mode, const void* data, size_t bytes, off_t offset, int* error) {
    ssize_t written = 0;
    if (mode == Mode::kPwritev2RwfDsync) {
        iovec iov{const_cast<void*>(data), bytes};
        written = pwritev2(fd, &iov, 1, offset, RWF_DSYNC);
    } else {
        written = pwrite(fd, data, bytes, offset);
    }
    if (written != static_cast<ssize_t>(bytes)) {
        *error = written < 0 ? errno : EIO;
        return false;
    }
    g_io_sink ^= static_cast<uint64_t>(written) + static_cast<uint64_t>(offset);
    if (mode != Mode::kPwritev2RwfDsync && fdatasync(fd) != 0) {
        *error = errno;
        return false;
    }
    return true;
}

BenchmarkCase CreateCase(const Options& options, const std::string& temp_directory, Mode mode, size_t bytes,
                         unsigned ordinal) {
    BenchmarkCase benchmark_case;
    benchmark_case.mode = mode;
    benchmark_case.bytes = bytes;
    benchmark_case.result.mode = ModeName(mode);
    benchmark_case.result.bytes = bytes;
    benchmark_case.path = temp_directory + "/" + benchmark_case.result.mode + "-" + std::to_string(bytes) + "-" +
                          std::to_string(ordinal) + ".wal";
    benchmark_case.fd = open(benchmark_case.path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
    if (benchmark_case.fd < 0) {
        benchmark_case.result.errors = 1;
        benchmark_case.result.first_errno = errno;
        return benchmark_case;
    }
    const size_t total_writes = options.warmup + options.samples;
    const off_t total_bytes = static_cast<off_t>(total_writes) * static_cast<off_t>(bytes);
    const int allocation_error =
        mode == Mode::kPreallocOverwriteFdatasync ? posix_fallocate(benchmark_case.fd, 0, total_bytes) : 0;
    if (allocation_error != 0) {
        benchmark_case.result.errors = 1;
        benchmark_case.result.first_errno = allocation_error;
        close(benchmark_case.fd);
        benchmark_case.fd = -1;
        unlink(benchmark_case.path.c_str());
        benchmark_case.path.clear();
        return benchmark_case;
    }
    benchmark_case.payload.resize(bytes);
    for (size_t index = 0; index < bytes; ++index) {
        benchmark_case.payload[index] = static_cast<unsigned char>((index * 131U + ordinal) & 0xffU);
    }
    benchmark_case.result.samples_ns.reserve(options.samples);
    return benchmark_case;
}

void RunSample(BenchmarkCase* benchmark_case, size_t round, size_t warmup) {
    if (benchmark_case->fd < 0 || benchmark_case->result.errors != 0 || benchmark_case->result.skipped) {
        return;
    }
    const off_t offset = static_cast<off_t>(round) * static_cast<off_t>(benchmark_case->bytes);
    const uint64_t begin_ns = NowNs();
    int error = 0;
    const bool ok = WriteAndStabilize(benchmark_case->fd, benchmark_case->mode, benchmark_case->payload.data(),
                                      benchmark_case->bytes, offset, &error);
    const uint64_t elapsed_ns = NowNs() - begin_ns;
    if (!ok) {
        if (benchmark_case->mode == Mode::kPwritev2RwfDsync && IsUnsupportedRwfDsync(error)) {
            benchmark_case->result.skipped = true;
            benchmark_case->result.skip_reason = std::strerror(error);
            return;
        }
        ++benchmark_case->result.errors;
        if (benchmark_case->result.first_errno == 0) {
            benchmark_case->result.first_errno = error;
        }
        return;
    }
    if (round >= warmup) {
        benchmark_case->result.samples_ns.push_back(elapsed_ns);
    }
}

std::vector<size_t> BuildInitialOrder(size_t count, uint64_t seed, bool interleaved) {
    std::vector<size_t> order(count);
    std::iota(order.begin(), order.end(), 0);
    if (interleaved) {
        std::mt19937_64 generator(seed);
        std::shuffle(order.begin(), order.end(), generator);
    }
    return order;
}

bool RotationIsLatin(const std::vector<size_t>& initial_order) {
    const size_t count = initial_order.size();
    std::vector<unsigned> positions(count * count, 0);
    for (size_t round = 0; round < count; ++round) {
        for (size_t position = 0; position < count; ++position) {
            const size_t case_index = initial_order[(position + round) % count];
            if (case_index >= count) {
                return false;
            }
            ++positions[case_index * count + position];
        }
    }
    return std::all_of(positions.begin(), positions.end(), [](unsigned appearances) { return appearances == 1; });
}

void RunSchedule(const Options& options, const std::vector<size_t>& initial_order,
                 std::vector<BenchmarkCase>* benchmark_cases) {
    const size_t total_rounds = options.warmup + options.samples;
    if (options.schedule == "serial") {
        for (size_t case_index : initial_order) {
            for (size_t round = 0; round < total_rounds; ++round) {
                RunSample(&(*benchmark_cases)[case_index], round, options.warmup);
            }
        }
        return;
    }
    // A seeded base permutation removes mode/size ordering bias. Rotating it
    // once per round makes every case occupy every temporal slot in each block
    // of nine rounds, while preserving one stabilization operation at a time.
    for (size_t round = 0; round < total_rounds; ++round) {
        for (size_t position = 0; position < initial_order.size(); ++position) {
            const size_t rotated_position = (position + round) % initial_order.size();
            RunSample(&(*benchmark_cases)[initial_order[rotated_position]], round, options.warmup);
        }
    }
}

void CloseAndUnlink(BenchmarkCase* benchmark_case) {
    if (benchmark_case->fd >= 0) {
        if (close(benchmark_case->fd) != 0 && benchmark_case->result.first_errno == 0) {
            ++benchmark_case->result.errors;
            benchmark_case->result.first_errno = errno;
        }
        benchmark_case->fd = -1;
    }
    if (!benchmark_case->path.empty()) {
        if (unlink(benchmark_case->path.c_str()) != 0 && benchmark_case->result.first_errno == 0) {
            ++benchmark_case->result.errors;
            benchmark_case->result.first_errno = errno;
        }
        benchmark_case->path.clear();
    }
}

uint64_t AdvanceCompletionFrontier(uint64_t frontier, uint64_t successful_coverage) {
    return std::max(frontier, successful_coverage);
}

// This is deliberately a diagnostic model, not LogManager code.  Writers obtain
// monotonically increasing pwrite epochs under one mutex.  A successful sync
// captures the highest fully-written epoch at issue time; completions may arrive
// out of order, so waiters use the maximum successful coverage frontier.
GroupResult RunGroupSync(int fd, unsigned depth, size_t rounds_per_producer) {
    constexpr size_t kProducers = 32;
    constexpr size_t kBytes = 4096;
    std::vector<unsigned char> payload(kBytes, 0x5a);
    std::mutex state_latch;
    std::mutex write_latch;
    std::mutex result_latch;
    std::condition_variable state_cv;
    uint64_t written_epoch = 0;
    uint64_t issued_epoch = 0;
    uint64_t completed_epoch = 0;
    size_t active = 0;
    size_t producers_left = kProducers;
    int sync_error = 0;
    GroupResult result;
    result.depth = depth;
    std::vector<std::thread> sync_threads;

    std::thread coordinator([&] {
        std::unique_lock<std::mutex> lock(state_latch);
        for (;;) {
            state_cv.wait(lock, [&] {
                return sync_error != 0 || (written_epoch > issued_epoch && active < depth) ||
                       (producers_left == 0 && active == 0 && completed_epoch >= written_epoch);
            });
            if (sync_error != 0 || (producers_left == 0 && active == 0 && completed_epoch >= written_epoch)) {
                return;
            }
            const uint64_t covered_epoch = written_epoch;
            issued_epoch = covered_epoch;
            ++active;
            result.max_syncs_in_flight = std::max(result.max_syncs_in_flight, static_cast<unsigned>(active));
            sync_threads.emplace_back([&, covered_epoch] {
                const int rc = fdatasync(fd);
                const int error = rc == 0 ? 0 : errno;
                std::lock_guard<std::mutex> completion_lock(state_latch);
                if (error != 0 && sync_error == 0) {
                    sync_error = error;
                }
                if (error == 0) {
                    completed_epoch = AdvanceCompletionFrontier(completed_epoch, covered_epoch);
                }
                --active;
                state_cv.notify_all();
            });
        }
    });

    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (size_t producer = 0; producer < kProducers; ++producer) {
        producers.emplace_back([&, producer] {
            for (size_t round = 0; round < rounds_per_producer; ++round) {
                uint64_t epoch;
                {
                    std::lock_guard<std::mutex> write_lock(write_latch);
                    std::lock_guard<std::mutex> lock(state_latch);
                    epoch = ++written_epoch;
                    const ssize_t written = pwrite(fd, payload.data(), payload.size(),
                                                   static_cast<off_t>(epoch - 1) * static_cast<off_t>(kBytes));
                    if (written != static_cast<ssize_t>(payload.size())) {
                        if (sync_error == 0) {
                            sync_error = written < 0 ? errno : EIO;
                        }
                        state_cv.notify_all();
                        break;
                    }
                    state_cv.notify_all();
                }
                const uint64_t begin = NowNs();
                std::unique_lock<std::mutex> lock(state_latch);
                state_cv.wait(lock, [&] { return sync_error != 0 || completed_epoch >= epoch; });
                if (sync_error != 0) {
                    break;
                }
                std::lock_guard<std::mutex> result_lock(result_latch);
                result.crossed_epochs += completed_epoch > epoch ? 1 : 0;
                result.wait_ns.push_back(NowNs() - begin);
            }
            std::lock_guard<std::mutex> lock(state_latch);
            --producers_left;
            state_cv.notify_all();
        });
    }
    for (std::thread& producer : producers) {
        producer.join();
    }
    coordinator.join();
    for (std::thread& sync_thread : sync_threads) {
        sync_thread.join();
    }
    result.errors = sync_error == 0 ? 0 : 1;
    return result;
}

// Virtual service hook: no wall-clock sleeps.  A 2ms service has a 50ms cliff
// every virtual 4 seconds, making the depth-two second wave measurable.
std::vector<uint64_t> SimulateGroupWait(unsigned depth, size_t waves) {
    constexpr uint64_t kSteadyNs = 2'000'000;
    constexpr uint64_t kCliffNs = 50'000'000;
    constexpr uint64_t kCliffEveryNs = 4'000'000'000;
    uint64_t virtual_now = 0;
    std::vector<uint64_t> waits;
    waits.reserve(waves * 32);
    for (size_t wave = 0; wave < waves; ++wave) {
        const uint64_t service = (virtual_now % kCliffEveryNs == 0 && wave != 0) ? kCliffNs : kSteadyNs;
        for (unsigned lane = 0; lane < depth; ++lane) {
            const uint64_t done = virtual_now + service;
            for (unsigned waiter = 0; waiter < 32; ++waiter) {
                waits.push_back(done - virtual_now);
            }
        }
        // Depth one needs a second wave after a cliff; depth two overlaps it.
        if (depth == 1) {
            for (unsigned waiter = 0; waiter < 32; ++waiter) {
                waits.push_back(service + kSteadyNs);
            }
        }
        virtual_now += service;
    }
    return waits;
}

double Quantile(std::vector<uint64_t> values, double fraction) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const double position = fraction * static_cast<double>(values.size() - 1);
    const size_t lower = static_cast<size_t>(std::floor(position));
    const size_t upper = static_cast<size_t>(std::ceil(position));
    return static_cast<double>(values[lower]) +
           (static_cast<double>(values[upper]) - values[lower]) * (position - lower);
}

double Mean(const std::vector<uint64_t>& values) {
    if (values.empty()) {
        return 0.0;
    }
    long double sum = 0;
    for (uint64_t value : values) {
        sum += value;
    }
    return static_cast<double>(sum / values.size());
}

std::string JsonEscape(const std::string& value) {
    std::string escaped;
    for (char c : value) {
        if (c == '"' || c == '\\') {
            escaped += '\\';
        }
        escaped += c;
    }
    return escaped;
}

bool WriteResults(const Options& options, const std::vector<Result>& results,
                  const std::vector<size_t>& initial_order) {
    const std::string csv_path = options.output + ".csv";
    const std::string json_path = options.output + ".json";
    FILE* csv = std::fopen(csv_path.c_str(), "w");
    if (csv == nullptr) {
        std::perror(csv_path.c_str());
        return false;
    }
    std::fprintf(csv, "mode,bytes,status,sample,latency_ns,errno\n");
    for (const Result& result : results) {
        if (result.skipped || result.errors != 0) {
            std::fprintf(csv, "%s,%zu,%s,,,%d\n", result.mode.c_str(), result.bytes, result.skipped ? "SKIP" : "ERROR",
                         result.first_errno);
        }
        for (size_t index = 0; index < result.samples_ns.size(); ++index) {
            std::fprintf(csv, "%s,%zu,OK,%zu,%llu,0\n", result.mode.c_str(), result.bytes, index,
                         static_cast<unsigned long long>(result.samples_ns[index]));
        }
    }
    if (std::fclose(csv) != 0) {
        std::perror(csv_path.c_str());
        return false;
    }
    std::ofstream json(json_path);
    if (!json) {
        std::perror(json_path.c_str());
        return false;
    }
    json << "{\n  \"samples_requested\": " << options.samples << ",\n  \"warmup\": " << options.warmup
         << ",\n  \"schedule\": \"" << options.schedule << "\",\n  \"seed\": " << options.seed
         << ",\n  \"order_policy\": \""
         << (options.schedule == "interleaved" ? "seeded_initial_order_with_per_round_latin_rotation"
                                               : "fixed_mode_size_order_serial")
         << "\",\n  \"initial_order\": [";
    for (size_t position = 0; position < initial_order.size(); ++position) {
        const Result& result = results[initial_order[position]];
        json << "{\"mode\":\"" << result.mode << "\",\"bytes\":" << result.bytes << "}";
        if (position + 1 != initial_order.size()) {
            json << ',';
        }
    }
    json << "],\n  \"results\": [\n";
    for (size_t index = 0; index < results.size(); ++index) {
        const Result& result = results[index];
        const uint64_t maximum =
            result.samples_ns.empty() ? 0 : *std::max_element(result.samples_ns.begin(), result.samples_ns.end());
        json << "    {\"mode\": \"" << result.mode << "\", \"bytes\": " << result.bytes << ", \"status\": \""
             << (result.skipped       ? "SKIP"
                 : result.errors == 0 ? "OK"
                                      : "ERROR")
             << "\", \"samples\": " << result.samples_ns.size() << ", \"errors\": " << result.errors
             << ", \"errno\": " << result.first_errno << ", \"skip_reason\": \"" << JsonEscape(result.skip_reason)
             << "\", \"mean_ns\": " << std::fixed << std::setprecision(1) << Mean(result.samples_ns)
             << ", \"p50_ns\": " << Quantile(result.samples_ns, 0.50)
             << ", \"p95_ns\": " << Quantile(result.samples_ns, 0.95)
             << ", \"p99_ns\": " << Quantile(result.samples_ns, 0.99) << ", \"max_ns\": " << maximum << "}";
        json << (index + 1 == results.size() ? "\n" : ",\n");
    }
    json << "  ]\n}\n";
    return true;
}

} // namespace

int main(int argc, char** argv) {
    Options options;
    if (!ParseArgs(argc, argv, &options)) {
        Usage(argv[0]);
        return 2;
    }
    if (options.self_check) {
        const std::vector<uint64_t> known{100, 200, 300, 400, 500};
        const std::vector<size_t> order = BuildInitialOrder(9, options.seed, true);
        std::vector<size_t> sorted_order = order;
        std::sort(sorted_order.begin(), sorted_order.end());
        const std::vector<size_t> expected_order{0, 1, 2, 3, 4, 5, 6, 7, 8};
        const std::vector<uint64_t> depth_one = SimulateGroupWait(1, 2002);
        const std::vector<uint64_t> depth_two = SimulateGroupWait(2, 2002);
        if (Quantile(known, 0.50) != 300.0 || Quantile(known, 0.95) != 480.0 || Mean(known) != 300.0 ||
            JsonEscape("a\\\"b") != "a\\\\\\\"b" || sorted_order != expected_order ||
            order != BuildInitialOrder(9, options.seed, true) || !RotationIsLatin(order) ||
            AdvanceCompletionFrontier(7, 3) != 7 || AdvanceCompletionFrontier(3, 7) != 7 ||
            *std::max_element(depth_two.begin(), depth_two.end()) >= *std::max_element(depth_one.begin(), depth_one.end())) {
            std::cerr << "self-check: FAIL\n";
            return 2;
        }
        const auto print_synthetic = [](unsigned depth, const std::vector<uint64_t>& waits) {
            const uint64_t maximum = *std::max_element(waits.begin(), waits.end());
            // Equal virtual completion times intentionally produce no crossed
            // epochs here; RunGroupSync records real out-of-order crossings.
            std::cout << "synthetic,depth=" << depth << ",p50_ms=" << std::fixed << std::setprecision(3)
                      << Quantile(waits, 0.50) / 1e6 << ",p99_ms=" << Quantile(waits, 0.99) / 1e6
                      << ",max_ms=" << static_cast<double>(maximum) / 1e6 << ",waiter_crossed_epochs=0\n";
        };
        print_synthetic(1, depth_one);
        print_synthetic(2, depth_two);
        std::cout << "self-check: PASS\n";
        return 0;
    }
    struct stat directory_stat {};
    if (stat(options.directory.c_str(), &directory_stat) != 0 || !S_ISDIR(directory_stat.st_mode)) {
        std::cerr << "--dir is not an accessible directory: " << options.directory << '\n';
        return 2;
    }
    std::string template_path = options.directory + "/wal_sync_microbench.XXXXXX";
    std::vector<char> mutable_template(template_path.begin(), template_path.end());
    mutable_template.push_back('\0');
    char* created_directory = mkdtemp(mutable_template.data());
    if (created_directory == nullptr) {
        std::perror("mkdtemp");
        return 2;
    }
    std::vector<BenchmarkCase> benchmark_cases;
    benchmark_cases.reserve(9);
    unsigned ordinal = 0;
    for (Mode mode : {Mode::kAppendFdatasync, Mode::kPreallocOverwriteFdatasync, Mode::kPwritev2RwfDsync}) {
        for (size_t bytes : kSizes) {
            benchmark_cases.push_back(CreateCase(options, created_directory, mode, bytes, ordinal++));
        }
    }
    const std::vector<size_t> initial_order =
        BuildInitialOrder(benchmark_cases.size(), options.seed, options.schedule == "interleaved");
    RunSchedule(options, initial_order, &benchmark_cases);
    std::vector<GroupResult> group_results;
    if (options.group_sync) {
        const std::string group_path = std::string(created_directory) + "/group_sync_same_wal.wal";
        const int group_fd = open(group_path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
        if (group_fd < 0) {
            std::perror("open group sync WAL");
        } else {
            group_results.push_back(RunGroupSync(group_fd, 1, options.group_samples));
            group_results.push_back(RunGroupSync(group_fd, 2, options.group_samples));
            if (close(group_fd) != 0 || unlink(group_path.c_str()) != 0) {
                std::perror("cleanup group sync WAL");
                for (GroupResult& result : group_results) {
                    ++result.errors;
                }
            }
        }
    }
    std::vector<Result> results;
    results.reserve(benchmark_cases.size());
    for (BenchmarkCase& benchmark_case : benchmark_cases) {
        CloseAndUnlink(&benchmark_case);
        results.push_back(std::move(benchmark_case.result));
    }
    if (rmdir(created_directory) != 0) {
        std::perror("rmdir temporary benchmark directory");
        return 2;
    }
    if (!WriteResults(options, results, initial_order)) {
        return 2;
    }
    std::cout << "WAL stabilization microbenchmark (ns; " << options.samples
              << " measured samples/mode-size; schedule=" << options.schedule << "; seed=" << options.seed << ")\n";
    std::cout << "mode,bytes,status,p50_ms,p95_ms,p99_ms,max_ms,errors\n";
    for (const Result& result : results) {
        const uint64_t maximum =
            result.samples_ns.empty() ? 0 : *std::max_element(result.samples_ns.begin(), result.samples_ns.end());
        std::cout << result.mode << ',' << result.bytes << ','
                  << (result.skipped       ? "SKIP"
                      : result.errors == 0 ? "OK"
                                           : "ERROR")
                  << ',' << std::fixed << std::setprecision(3) << Quantile(result.samples_ns, 0.50) / 1e6 << ','
                  << Quantile(result.samples_ns, 0.95) / 1e6 << ',' << Quantile(result.samples_ns, 0.99) / 1e6 << ','
                  << static_cast<double>(maximum) / 1e6 << ',' << result.errors;
        if (result.skipped) {
            std::cout << " (" << result.skip_reason << ')';
        } else if (result.first_errno != 0) {
            std::cout << " (errno=" << result.first_errno << ": " << std::strerror(result.first_errno) << ')';
        }
        std::cout << '\n';
    }
    if (!group_results.empty()) {
        std::cout << "same-WAL group durability (32 producers; strict pwrite epochs)\n";
        std::cout << "depth,status,p50_ms,p99_ms,max_ms,waiter_crossed_epochs,max_syncs_in_flight,errors\n";
        for (const GroupResult& result : group_results) {
            const uint64_t maximum = result.wait_ns.empty()
                                         ? 0
                                         : *std::max_element(result.wait_ns.begin(), result.wait_ns.end());
            std::cout << result.depth << ',' << (result.errors == 0 ? "OK" : "ERROR") << ',' << std::fixed
                      << std::setprecision(3) << Quantile(result.wait_ns, 0.50) / 1e6 << ','
                      << Quantile(result.wait_ns, 0.99) / 1e6 << ',' << static_cast<double>(maximum) / 1e6 << ','
                      << result.crossed_epochs << ',' << result.max_syncs_in_flight << ',' << result.errors << '\n';
        }
    }
    std::cout << "CSV: " << options.output << ".csv\nJSON: " << options.output << ".json\n";
    return 0;
}
