/* Copyright (c) 2026 Team Youbuntu */
#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace wire_protocol {

constexpr std::size_t kMaxPayload = 1u << 20;
constexpr std::size_t kMaxDiagnostic = 64u << 10;

enum class Tag : std::uint8_t {
    META = 0x01,
    ROW = 0x02,
    COMMAND_OK = 0x10,
    RESULT_END = 0x11,
    TRANSACTION_ABORT = 0x12,
    ERROR = 0x13,
    PREPARE_OK = 0x14,
    BATCH_RESULT = 0x15,
    EXEC_STREAM = 0x20,
    PREPARE_SET = 0x21,
    EXEC_BATCH = 0x22,
};

enum class Type : std::uint8_t { INT32 = 0x01, FLOAT32 = 0x02, CHAR = 0x03 };

struct Frame {
    Tag tag;
    std::uint8_t flags = 0;
    std::vector<std::uint8_t> payload;
};

struct Value {
    Type type = Type::INT32;
    bool present = true;
    std::int32_t int32 = 0;
    std::uint32_t float_bits = 0;
    std::string text;
};

struct ColumnDefinition {
    std::string name;
    Type type = Type::INT32;
};

struct PreparedSchema {
    std::uint16_t statement_id = 0;
    std::vector<ColumnDefinition> columns;
};

class ProtocolError : public std::runtime_error {
public:
    explicit ProtocolError(const std::string& message) : std::runtime_error(message) {}
};

class Reader {
public:
    explicit Reader(const std::vector<std::uint8_t>& bytes) : bytes_(bytes) {}
    Reader(std::vector<std::uint8_t>&&) = delete;

    std::uint8_t u8();
    std::uint16_t u16();
    std::uint32_t u32();
    std::uint64_t u64();
    std::string bytes(std::size_t count);
    std::size_t remaining() const {
        return bytes_.size() - offset_;
    }
    void require_end() const;

private:
    const std::vector<std::uint8_t>& bytes_;
    std::size_t offset_ = 0;
};

class Writer {
public:
    void u8(std::uint8_t value);
    void u16(std::uint16_t value);
    void u32(std::uint32_t value);
    void u64(std::uint64_t value);
    void bytes(const void* data, std::size_t count);
    void bytes(const std::string& value) {
        bytes(value.data(), value.size());
    }
    const std::vector<std::uint8_t>& data() const {
        return bytes_;
    }
    std::vector<std::uint8_t> take() {
        return std::move(bytes_);
    }

private:
    void ensure_capacity(std::size_t count) const;

    std::vector<std::uint8_t> bytes_;
};

bool read_exact(int fd, void* data, std::size_t count);
void write_all(int fd, const void* data, std::size_t count);
void server_handshake(int fd);
bool read_frame(int fd, Frame& frame);
void write_frame(int fd, Tag tag, const std::vector<std::uint8_t>& payload, std::uint8_t flags = 0);

std::vector<std::uint8_t> encode_prepare_ok(const std::vector<PreparedSchema>& schemas);

void encode_value(Writer& writer, const Value& value, Type expected);
Value decode_value(Reader& reader, Type expected);

} // namespace wire_protocol
