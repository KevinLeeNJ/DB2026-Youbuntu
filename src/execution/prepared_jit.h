/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

class Context;
class ParameterFrame;
class PreparedPlanDescriptor;
class QueryResultSink;
class SmManager;

enum class PreparedJitExecutionStatus { Handled, Fallback };

struct PreparedJitCompilerStats {
    std::uint64_t attempts{0};
    std::uint64_t compiled{0};
    std::uint64_t unsupported{0};
    std::uint64_t failures{0};
    std::uint64_t code_bytes{0};
};

class PreparedJitProgram final {
public:
    struct Impl;

    ~PreparedJitProgram();

    PreparedJitProgram(const PreparedJitProgram&) = delete;
    PreparedJitProgram& operator=(const PreparedJitProgram&) = delete;

    PreparedJitExecutionStatus Execute(const ParameterFrame& parameters, SmManager* sm_manager, Context* context,
                                       QueryResultSink* result_sink) const;

    bool is_query() const noexcept;
    std::size_t code_size() const noexcept;

private:
    friend class PreparedJitCompiler;
    explicit PreparedJitProgram(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;
};

class PreparedJitCompiler final {
public:
    PreparedJitCompiler();
    ~PreparedJitCompiler();

    PreparedJitCompiler(const PreparedJitCompiler&) = delete;
    PreparedJitCompiler& operator=(const PreparedJitCompiler&) = delete;

    std::unique_ptr<PreparedJitProgram> Compile(const PreparedPlanDescriptor& descriptor, SmManager* sm_manager,
                                                std::string* reason = nullptr);
    PreparedJitCompilerStats stats() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

PreparedJitCompiler& prepared_jit_compiler();
