/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "server/wire_session_internal.h"

#include "index/ix_scan.h"

namespace wire_session_internal {
Type protocol_type(ColType type) {
    return type == TYPE_INT ? Type::INT32 : type == TYPE_FLOAT ? Type::FLOAT32 : Type::CHAR;
}

bool changes_catalog(ast::AstType type) {
    return type == ast::AstType::CreateTable || type == ast::AstType::DropTable || type == ast::AstType::CreateIndex ||
           type == ast::AstType::DropIndex || type == ast::AstType::LoadStmt;
}

bool descriptor_runtime_eligible(const PreparedPlanDescriptor* descriptor) {
    if (descriptor == nullptr || !descriptor->eligible()) {
        return false;
    }
    if (descriptor->statement_kind() == PreparedStatementKind::Update) {
        const DMLPlan* dml = descriptor->dml_plan();
        return dml != nullptr && dml->subplan_ != nullptr;
    }
    return true;
}

std::vector<Value> protocol_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) {
    std::vector<Value> row;
    row.reserve(columns.size());
    for (const auto& column : columns) {
        if (column.offset < 0 || static_cast<std::size_t>(column.offset) + column.len > size ||
            static_cast<std::size_t>(column.null_byte + 1) > size) {
            throw wire_protocol::ProtocolError("executor returned an invalid tuple");
        }
        Value value;
        value.type = protocol_type(column.type);
        const char* cell = data + column.offset;
        if (is_null(data, column)) {
            // present == 0 后不写任何值字节；NULL 不得编码为空字符串（final.md:761）
            value.present = false;
            row.push_back(std::move(value));
            continue;
        }
        if (column.type == TYPE_INT) {
            value.int32 = read_unaligned<int>(cell);
        } else if (column.type == TYPE_FLOAT) {
            float number = read_float(cell);
            std::memcpy(&value.float_bits, &number, sizeof(value.float_bits));
        } else {
            value.text.assign(cell, strnlen(cell, column.len));
        }
        row.push_back(std::move(value));
    }
    return row;
}

BatchResultBuilder::BatchResultBuilder() {
    write_success_header();
}

void BatchResultBuilder::begin_operation(std::uint16_t operation_index) {
    if (operation_active_) {
        throw wire_protocol::ProtocolError("batch result operation was not finished");
    }
    operation_active_ = true;
    query_active_ = false;
    operation_index_ = operation_index;
    row_count_ = 0;
}

void BatchResultBuilder::begin_query(const std::vector<ColMeta>& columns,
                                     const std::vector<std::string>& output_names) {
    if (!operation_active_ || query_active_ || columns.size() != output_names.size()) {
        throw wire_protocol::ProtocolError("invalid batch query schema");
    }
    query_active_ = true;
    writer_.u16(operation_index_);
    row_count_offset_ = writer_.size();
    writer_.u32(0);
    ++query_count_;
}

void BatchResultBuilder::append_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) {
    if (!query_active_ || data == nullptr) {
        throw wire_protocol::ProtocolError("batch query row emitted before schema");
    }
    for (const auto& column : columns) {
        if (column.offset < 0 || column.len < 0 || static_cast<std::size_t>(column.offset) > size ||
            static_cast<std::size_t>(column.len) > size - static_cast<std::size_t>(column.offset) ||
            (column.null_byte >= 0 && static_cast<std::size_t>(column.null_byte) >= size)) {
            throw wire_protocol::ProtocolError("executor returned an invalid tuple");
        }
        const bool present = !is_null(data, column);
        const char* cell = data + column.offset;
        const Type type = protocol_type(column.type);
        const std::size_t encoded_size = type == Type::CHAR && present
                                             ? strnlen(cell, static_cast<std::size_t>(column.len))
                                             : static_cast<std::size_t>(column.len);
        wire_protocol::encode_raw_value(writer_, type, present, cell, encoded_size);
    }
    if (row_count_ == UINT32_MAX) {
        throw wire_protocol::ProtocolError("batch query row count exceeds protocol limit");
    }
    ++row_count_;
}

void BatchResultBuilder::finish_operation(bool query) {
    if (!operation_active_ || query != query_active_) {
        throw wire_protocol::ProtocolError("batch query result kind mismatch");
    }
    if (query_active_) {
        writer_.patch_u32(row_count_offset_, row_count_);
    }
    operation_active_ = false;
    query_active_ = false;
}

std::vector<std::uint8_t> BatchResultBuilder::success(std::uint16_t executed) {
    if (operation_active_) {
        throw wire_protocol::ProtocolError("batch result operation was not finished");
    }
    writer_.patch_u16(kExecutedOffset, executed);
    writer_.patch_u16(kQueryCountOffset, query_count_);
    return writer_.take();
}

std::vector<std::uint8_t> BatchResultBuilder::failure(std::uint16_t executed, std::uint8_t status, std::uint16_t failed,
                                                      const std::string& diagnostic) {
    writer_.rewind(0);
    operation_active_ = false;
    query_active_ = false;
    query_count_ = 0;
    writer_.u16(executed);
    writer_.u8(status);
    writer_.u16(failed);
    writer_.u32(static_cast<std::uint32_t>(diagnostic.size()));
    writer_.bytes(diagnostic);
    writer_.u16(0);
    return writer_.take();
}

void BatchResultBuilder::write_success_header() {
    writer_.u16(0);
    writer_.u8(0);
    writer_.u16(0xffff);
    writer_.u32(0);
    writer_.u16(0);
}
std::string diagnostic(const std::exception& exception) {
    std::string text = exception.what();
    if (text.size() > wire_protocol::kMaxDiagnostic) {
        text.resize(wire_protocol::kMaxDiagnostic);
    }
    return text;
}

bool is_valid_utf8(const std::string& text) {
    for (std::size_t i = 0; i < text.size();) {
        const auto first = static_cast<unsigned char>(text[i]);
        if (first <= 0x7f) {
            ++i;
            continue;
        }
        std::size_t width = 0;
        std::uint32_t code_point = 0;
        if (first >= 0xc2 && first <= 0xdf) {
            width = 2;
            code_point = first & 0x1f;
        } else if (first >= 0xe0 && first <= 0xef) {
            width = 3;
            code_point = first & 0x0f;
        } else if (first >= 0xf0 && first <= 0xf4) {
            width = 4;
            code_point = first & 0x07;
        } else {
            return false;
        }
        if (i + width > text.size()) {
            return false;
        }
        for (std::size_t j = 1; j < width; ++j) {
            const auto next = static_cast<unsigned char>(text[i + j]);
            if ((next & 0xc0) != 0x80) {
                return false;
            }
            code_point = (code_point << 6) | (next & 0x3f);
        }
        if ((width == 3 && code_point < 0x800) || (width == 4 && code_point < 0x10000) ||
            (code_point >= 0xd800 && code_point <= 0xdfff) || code_point > 0x10ffff) {
            return false;
        }
        i += width;
    }
    return true;
}

void append_column_definition(Writer& writer, const std::string& name, Type type) {
    if (name.empty() || name.size() > UINT16_MAX || !is_valid_utf8(name)) {
        throw wire_protocol::ProtocolError("invalid column name");
    }
    writer.u16(static_cast<std::uint16_t>(name.size()));
    writer.bytes(name);
    writer.u8(static_cast<std::uint8_t>(type));
}

std::vector<std::uint8_t> make_row(const std::vector<Type>& types, const std::vector<Value>& row) {
    Writer writer;
    if (row.size() != types.size()) {
        throw wire_protocol::ProtocolError("row does not match query schema");
    }
    for (std::size_t i = 0; i < row.size(); ++i) {
        wire_protocol::encode_value(writer, row[i], types[i]);
    }
    return writer.take();
}

std::vector<std::uint8_t> make_error_payload(const std::string& text) {
    Writer writer;
    writer.bytes(text.substr(0, wire_protocol::kMaxDiagnostic));
    return writer.take();
}

struct ProtocolStreamSink : QueryResultSink {
    explicit ProtocolStreamSink(int socket_fd) : fd(socket_fd) {}

    void begin_query(const std::vector<ColMeta>& columns, const std::vector<std::string>& output_names) override {
        if (columns.empty() || columns.size() > UINT16_MAX || output_names.size() != columns.size()) {
            throw wire_protocol::ProtocolError("invalid query schema");
        }
        types.clear();
        Writer meta;
        meta.u16(static_cast<std::uint16_t>(columns.size()));
        for (std::size_t i = 0; i < columns.size(); ++i) {
            const Type type = protocol_type(columns[i].type);
            append_column_definition(meta, output_names[i], type);
            types.push_back(type);
        }
        wire_protocol::write_frame(fd, Tag::META, meta.take());
        query = true;
    }

    void append_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) override {
        if (!query) {
            throw wire_protocol::ProtocolError("query row emitted before META");
        }
        wire_protocol::write_frame(fd, Tag::ROW, make_row(types, protocol_row(columns, data, size)));
        ++row_count;
    }

    void finish() {
        Writer end;
        end.u64(row_count);
        wire_protocol::write_frame(fd, Tag::RESULT_END, end.take());
    }

    int fd;
    std::vector<Type> types;
    std::uint64_t row_count = 0;
    bool query = false;
};

void handle_exec_stream(DatabaseInstance& database, int fd, const wire_protocol::Frame& frame, SessionState& session,
                        std::unordered_map<std::uint16_t, PreparedStatement>& prepared) {
    Reader reader(frame.payload);
    if (frame.flags != 0 || frame.payload.empty()) {
        throw wire_protocol::ProtocolError("invalid EXEC_STREAM request");
    }
    const std::string sql = reader.bytes(reader.remaining());
    reader.require_end();
    if (sql.find('\0') != std::string::npos || !is_valid_utf8(sql)) {
        throw wire_protocol::ProtocolError("EXEC_STREAM SQL must be UTF-8 without NUL");
    }
    ProtocolStreamSink result(fd);
    const ExecutionOutcome outcome = execute_sql(database, sql, session, &result);
    if (outcome.query && result.query) {
        result.finish();
    } else {
        wire_protocol::write_frame(fd, Tag::COMMAND_OK, {});
    }
    if (outcome.catalog_changed) {
        prepared.clear();
    }
    return;
}

} // namespace wire_session_internal
