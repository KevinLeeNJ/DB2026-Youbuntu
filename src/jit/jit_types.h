/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace jit {

enum class JitStatus {
    OK,
    DIVISION_BY_ZERO,
    INVALID_INPUT,
    HELPER_ERROR,
    UNSUPPORTED_ARCHITECTURE,
    COMPILE_ERROR,
    ALLOCATION_ERROR,
};

struct JitRuntimeOptions {
    bool force_unsupported_architecture{false};
};

struct JitCompileOptions {
    bool force_compile_failure{false};
    bool force_allocation_failure{false};
};

struct JitRuntimeImpl;

class JitCode {
public:
    JitCode() = default;
    ~JitCode();

    JitCode(const JitCode&) = delete;
    JitCode& operator=(const JitCode&) = delete;
    JitCode(JitCode&& other) noexcept;
    JitCode& operator=(JitCode&& other) noexcept;

    explicit operator bool() const {
        return function_ != nullptr;
    }

    int32_t test_add_i32(int32_t lhs, int32_t rhs) const;
    size_t code_size() const {
        return code_size_;
    }

private:
    friend class JitRuntime;

    using TestAddI32Fn = int32_t (*)(int32_t, int32_t);

    JitCode(std::shared_ptr<JitRuntimeImpl> runtime, TestAddI32Fn function, size_t code_size);
    void reset();

    std::shared_ptr<JitRuntimeImpl> runtime_;
    TestAddI32Fn function_{nullptr};
    size_t code_size_{0};
};

struct JitCompileResult {
    JitStatus status{JitStatus::COMPILE_ERROR};
    JitCode code;
    std::string error;

    explicit operator bool() const {
        return status == JitStatus::OK;
    }
};

class JitRuntime {
public:
    explicit JitRuntime(JitRuntimeOptions options = {});
    ~JitRuntime() = default;

    JitRuntime(const JitRuntime&) = delete;
    JitRuntime& operator=(const JitRuntime&) = delete;

    bool is_supported() const;
    size_t active_code_count() const;
    JitCompileResult compile_test_add_i32(JitCompileOptions options = {});

private:
    std::shared_ptr<JitRuntimeImpl> runtime_;
    bool supported_{false};
};

} // namespace jit
