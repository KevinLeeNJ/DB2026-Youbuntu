/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
*/

#include "result_sink.h"

#include <cstring>
#include <fstream>

ResultSink::ResultSink(SmManager* sm_manager, Context* context, const std::vector<ColMeta>& result_cols,
                       std::vector<std::string> captions)
    : sm_manager_(sm_manager), context_(context), result_cols_(result_cols), captions_(std::move(captions)) {
    if (sm_manager_ == nullptr || context_ == nullptr || context_->data_send_ == nullptr ||
        context_->offset_ == nullptr) {
        throw InternalError("result sink requires an output context");
    }
    if (captions_.size() != result_cols_.size()) {
        captions_.clear();
        captions_.reserve(result_cols_.size());
        for (const auto& col : result_cols_) {
            captions_.push_back(col.name);
        }
    }
    if (captions_.empty()) {
        throw InternalError("result sink requires at least one column");
    }

    output_start_ = *context_->offset_;
    staged_offset_ = output_start_;
    if (output_start_ < 0 || static_cast<size_t>(output_start_) > client_buffer_.size()) {
        throw InternalError("result sink output offset is out of range");
    }
    if (output_start_ > 0) {
        memcpy(client_buffer_.data(), context_->data_send_, static_cast<size_t>(output_start_));
    }
    staged_context_ = std::make_unique<Context>(context_->lock_mgr_, context_->log_mgr_, context_->txn_,
                                                client_buffer_.data(), &staged_offset_, context_->txn_mgr_);
    staged_context_->ellipsis_ = context_->ellipsis_;
    record_printer_ = std::make_unique<RecordPrinter>(captions_.size());
    columns_.reserve(result_cols_.size());
    record_printer_->print_separator(staged_context_.get());
    record_printer_->print_record(captions_, staged_context_.get());
    record_printer_->print_separator(staged_context_.get());

    if (sm_manager_->output_file_enabled_) {
        output_file_stream_ << "|";
        for (const auto& caption : captions_) {
            output_file_stream_ << " " << caption << " |";
        }
        output_file_stream_ << "\n";
    }
}

void ResultSink::Emit(TupleView tuple) {
    if (finished_) {
        throw InternalError("cannot emit a row after result sink finish");
    }
    if (!tuple) {
        throw InternalError("cannot emit an empty tuple");
    }

    columns_.clear();
    for (const auto& col : result_cols_) {
        if (col.offset < 0 || col.len < 0) {
            throw InternalError("result column range is invalid");
        }
        const size_t offset = static_cast<size_t>(col.offset);
        const size_t len = static_cast<size_t>(col.len);
        if (offset > tuple.size || len > static_cast<size_t>(tuple.size) - offset) {
            throw InternalError("result tuple is too small for output schema");
        }
        size_t read_width = len;
        if (col.type == TYPE_INT) {
            read_width = sizeof(int);
        } else if (col.type == TYPE_FLOAT) {
            read_width = sizeof(double);
        } else if (col.type != TYPE_STRING && col.type != TYPE_DATETIME) {
            throw InternalError("result column type is invalid");
        }
        if (len < read_width || read_width > static_cast<size_t>(tuple.size) - offset) {
            throw InternalError("result column width is invalid");
        }
        std::string col_str;
        const char* rec_buf = tuple.data + offset;
        if (col.type == TYPE_INT) {
            col_str = std::to_string(read_unaligned<int>(rec_buf));
        } else if (col.type == TYPE_FLOAT) {
            col_str = std::to_string(read_unaligned<double>(rec_buf));
        } else if (col.type == TYPE_STRING || col.type == TYPE_DATETIME) {
            col_str.assign(rec_buf, strnlen(rec_buf, col.len));
        }
        columns_.push_back(std::move(col_str));
    }
    record_printer_->print_record(columns_, staged_context_.get());
    if (sm_manager_->output_file_enabled_) {
        output_file_stream_ << "|";
        for (const auto& column : columns_) {
            output_file_stream_ << " " << column << " |";
        }
        output_file_stream_ << "\n";
    }
    ++row_count_;
}

void ResultSink::Finish() {
    if (finished_) {
        return;
    }
    record_printer_->print_separator(staged_context_.get());
    RecordPrinter::print_record_count(row_count_, staged_context_.get());

    if (sm_manager_->output_file_enabled_ && staged_offset_ > output_start_) {
        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << output_file_stream_.str();
        outfile.close();
    }

    const size_t staged_size = static_cast<size_t>(staged_offset_ - output_start_);
    memcpy(context_->data_send_ + output_start_, client_buffer_.data() + output_start_, staged_size);
    *context_->offset_ = staged_offset_;
    context_->ellipsis_ = staged_context_->ellipsis_;
    finished_ = true;
}
