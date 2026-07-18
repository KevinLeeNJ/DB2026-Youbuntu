/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
*/

#pragma once

#include <array>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "execution/executor_abstract.h"
#include "record_printer.h"
#include "system/sm.h"

class ResultSink {
public:
    ResultSink(SmManager* sm_manager, Context* context, const std::vector<ColMeta>& result_cols,
               std::vector<std::string> captions);
    ~ResultSink() = default;

    ResultSink(const ResultSink&) = delete;
    ResultSink& operator=(const ResultSink&) = delete;

    void Emit(TupleView tuple);
    void Finish();

private:
    SmManager* sm_manager_;
    Context* context_;
    std::vector<ColMeta> result_cols_;
    std::vector<std::string> captions_;
    std::unique_ptr<RecordPrinter> record_printer_;
    std::array<char, BUFFER_LENGTH> client_buffer_{};
    int output_start_{0};
    int staged_offset_{0};
    bool finished_{false};
    size_t row_count_{0};
    std::unique_ptr<Context> staged_context_;
    std::ostringstream output_file_stream_;
    std::vector<std::string> columns_;
};
