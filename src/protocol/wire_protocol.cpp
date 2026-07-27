/* Copyright (c) 2026 Team Youbuntu */
#include "wire_protocol.h"

#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <unordered_set>

namespace wire_protocol {

namespace {
void check_size(std::size_t value, std::size_t limit, const char* what) {
    if (value > limit) {
        throw ProtocolError(std::string(what) + " exceeds protocol limit");
    }
}

std::uint32_t load_u32(const std::uint8_t* data) {
    return (static_cast<std::uint32_t>(data[0]) << 24) | (static_cast<std::uint32_t>(data[1]) << 16) |
           (static_cast<std::uint32_t>(data[2]) << 8) | static_cast<std::uint32_t>(data[3]);
}

std::uint16_t load_u16(const std::uint8_t* data) {
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(data[0]) << 8) | data[1]);
}

bool is_supported_type(Type type) {
    switch (type) {
    case Type::INT32:
    case Type::FLOAT32:
    case Type::CHAR:
        return true;
    }
    return false;
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

bool is_known_tag(Tag tag) {
    switch (tag) {
    case Tag::META:
    case Tag::ROW:
    case Tag::COMMAND_OK:
    case Tag::RESULT_END:
    case Tag::TRANSACTION_ABORT:
    case Tag::ERROR:
    case Tag::PREPARE_OK:
    case Tag::BATCH_RESULT:
    case Tag::EXEC_STREAM:
    case Tag::PREPARE_SET:
    case Tag::EXEC_BATCH:
        return true;
    }
    return false;
}

std::uint8_t expected_flags(Tag tag) {
    if (!is_known_tag(tag)) {
        throw ProtocolError("unknown frame tag");
    }
    // This is the existing request convention used by rmdb.cpp. All other
    // currently defined frame types use zero flags.
    return tag == Tag::EXEC_BATCH ? 1 : 0;
}

void validate_frame_fields(Tag tag, std::uint8_t flags) {
    if (flags != expected_flags(tag)) {
        throw ProtocolError("invalid frame flags");
    }
}

std::int32_t int32_from_bits(std::uint32_t bits) {
    std::int32_t value;
    static_assert(sizeof(value) == sizeof(bits), "INT32 must be four bytes");
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}
} // namespace

std::uint8_t Reader::u8() {
    if (remaining() < 1) {
        throw ProtocolError("truncated payload");
    }
    return bytes_[offset_++];
}

std::uint16_t Reader::u16() {
    if (remaining() < 2) {
        throw ProtocolError("truncated payload");
    }
    const auto value = load_u16(bytes_.data() + offset_);
    offset_ += 2;
    return value;
}

std::uint32_t Reader::u32() {
    if (remaining() < 4) {
        throw ProtocolError("truncated payload");
    }
    const auto value = load_u32(bytes_.data() + offset_);
    offset_ += 4;
    return value;
}

std::uint64_t Reader::u64() {
    if (remaining() < 8) {
        throw ProtocolError("truncated payload");
    }
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value = (value << 8) | bytes_[offset_++];
    }
    return value;
}

std::string Reader::bytes(std::size_t count) {
    if (count > remaining()) {
        throw ProtocolError("truncated length-delimited value");
    }
    if (count == 0) {
        return {};
    }
    std::string result(reinterpret_cast<const char*>(bytes_.data() + offset_), count);
    offset_ += count;
    return result;
}

void Reader::require_end() const {
    if (remaining() != 0) {
        throw ProtocolError("payload contains trailing bytes");
    }
}

void Writer::ensure_capacity(std::size_t count) const {
    if (count > kMaxPayload || bytes_.size() > kMaxPayload - count) {
        throw ProtocolError("writer payload exceeds protocol limit");
    }
}

void Writer::u8(std::uint8_t value) {
    ensure_capacity(1);
    bytes_.push_back(value);
}

void Writer::u16(std::uint16_t value) {
    ensure_capacity(2);
    bytes_.push_back(static_cast<std::uint8_t>(value >> 8));
    bytes_.push_back(static_cast<std::uint8_t>(value));
}

void Writer::u32(std::uint32_t value) {
    ensure_capacity(4);
    for (int shift = 24; shift >= 0; shift -= 8) {
        bytes_.push_back(static_cast<std::uint8_t>(value >> shift));
    }
}

void Writer::u64(std::uint64_t value) {
    ensure_capacity(8);
    for (int shift = 56; shift >= 0; shift -= 8) {
        bytes_.push_back(static_cast<std::uint8_t>(value >> shift));
    }
}

void Writer::bytes(const void* data, std::size_t count) {
    if (count == 0) {
        return;
    }
    if (data == nullptr) {
        throw ProtocolError("cannot write bytes from a null pointer");
    }
    ensure_capacity(count);
    const auto* first = static_cast<const std::uint8_t*>(data);
    bytes_.insert(bytes_.end(), first, first + count);
}

bool read_exact(int fd, void* data, std::size_t count) {
    if (count == 0) {
        return true;
    }
    if (data == nullptr) {
        throw ProtocolError("cannot read bytes into a null pointer");
    }
    auto* output = static_cast<std::uint8_t*>(data);
    std::size_t read_bytes = 0;
    while (read_bytes < count) {
        const ssize_t result = ::read(fd, output + read_bytes, count - read_bytes);
        if (result == 0) {
            if (read_bytes == 0) {
                return false;
            }
            throw ProtocolError("connection closed in fixed-width field");
        }
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw ProtocolError(std::string("socket read failed: ") + std::strerror(errno));
        }
        read_bytes += static_cast<std::size_t>(result);
    }
    return true;
}

void write_all(int fd, const void* data, std::size_t count) {
    if (count == 0) {
        return;
    }
    if (data == nullptr) {
        throw ProtocolError("cannot write bytes from a null pointer");
    }
    const auto* input = static_cast<const std::uint8_t*>(data);
    std::size_t written = 0;
    while (written < count) {
        const ssize_t result = ::send(fd, input + written, count - written, MSG_NOSIGNAL);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw ProtocolError(std::string("socket write failed: ") + std::strerror(errno));
        }
        if (result == 0) {
            throw ProtocolError("socket write returned zero");
        }
        written += static_cast<std::size_t>(result);
    }
}

void server_handshake(int fd) {
    std::uint8_t handshake[8];
    if (!read_exact(fd, handshake, sizeof(handshake))) {
        throw ProtocolError("connection closed before handshake");
    }
    if (std::memcmp(handshake, "RMDB", 4) != 0 || handshake[4] != 0 || handshake[5] != 3 || handshake[6] != 0 ||
        handshake[7] != 0) {
        throw ProtocolError("unsupported RMDB protocol handshake");
    }
    write_all(fd, handshake, sizeof(handshake));
}

bool read_frame(int fd, Frame& frame) {
    std::uint8_t header[8];
    if (!read_exact(fd, header, sizeof(header))) {
        return false;
    }
    const std::uint32_t payload_size = load_u32(header);
    check_size(payload_size, kMaxPayload, "frame payload");
    if (header[6] != 0 || header[7] != 0) {
        throw ProtocolError("frame reserved field must be zero");
    }
    const auto tag = static_cast<Tag>(header[4]);
    validate_frame_fields(tag, header[5]);

    std::vector<std::uint8_t> payload(payload_size);
    if (payload_size != 0 && !read_exact(fd, payload.data(), payload_size)) {
        throw ProtocolError("connection closed in frame payload");
    }
    frame.tag = tag;
    frame.flags = header[5];
    frame.payload = std::move(payload);
    return true;
}

void write_frame(int fd, Tag tag, const std::vector<std::uint8_t>& payload, std::uint8_t flags) {
    check_size(payload.size(), kMaxPayload, "frame payload");
    validate_frame_fields(tag, flags);
    std::uint8_t header[8] = {static_cast<std::uint8_t>(payload.size() >> 24),
                              static_cast<std::uint8_t>(payload.size() >> 16),
                              static_cast<std::uint8_t>(payload.size() >> 8),
                              static_cast<std::uint8_t>(payload.size()),
                              static_cast<std::uint8_t>(tag),
                              flags,
                              0,
                              0};
    iovec vectors[2] = {
        {header, sizeof(header)},
        {const_cast<std::uint8_t*>(payload.data()), payload.size()},
    };
    std::size_t current = 0;
    const std::size_t vector_count = payload.empty() ? 1 : 2;
    while (current < vector_count) {
        msghdr message{};
        message.msg_iov = vectors + current;
        message.msg_iovlen = vector_count - current;
        const ssize_t result = ::sendmsg(fd, &message, MSG_NOSIGNAL);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw ProtocolError(std::string("socket write failed: ") + std::strerror(errno));
        }
        if (result == 0) {
            throw ProtocolError("socket write returned zero");
        }

        std::size_t written = static_cast<std::size_t>(result);
        while (written >= vectors[current].iov_len) {
            written -= vectors[current].iov_len;
            if (++current == vector_count) {
                break;
            }
        }
        if (current < vector_count && written != 0) {
            vectors[current].iov_base = static_cast<std::uint8_t*>(vectors[current].iov_base) + written;
            vectors[current].iov_len -= written;
        }
    }
}

std::vector<std::uint8_t> encode_prepare_ok(const std::vector<PreparedSchema>& schemas) {
    if (schemas.empty() || schemas.size() > 256) {
        throw ProtocolError("invalid prepared schema count");
    }
    Writer writer;
    writer.u16(static_cast<std::uint16_t>(schemas.size()));
    std::unordered_set<std::uint16_t> ids;
    for (const auto& schema : schemas) {
        if (schema.statement_id == 0 || !ids.insert(schema.statement_id).second || schema.columns.size() > UINT16_MAX) {
            throw ProtocolError("invalid prepared schema metadata");
        }
        writer.u16(schema.statement_id);
        writer.u16(static_cast<std::uint16_t>(schema.columns.size()));
        for (const auto& column : schema.columns) {
            if (column.name.empty() || column.name.size() > UINT16_MAX || !is_valid_utf8(column.name) ||
                !is_supported_type(column.type)) {
                throw ProtocolError("invalid prepared column definition");
            }
            writer.u16(static_cast<std::uint16_t>(column.name.size()));
            writer.bytes(column.name);
            writer.u8(static_cast<std::uint8_t>(column.type));
        }
    }
    return writer.take();
}

void encode_value(Writer& writer, const Value& value, Type expected) {
    if (!is_supported_type(expected)) {
        throw ProtocolError("unknown typed value type");
    }
    if (value.type != expected) {
        throw ProtocolError("typed value does not match schema");
    }
    writer.u8(value.present ? 1 : 0);
    if (!value.present) {
        return;
    }
    switch (expected) {
    case Type::INT32:
        writer.u32(static_cast<std::uint32_t>(value.int32));
        break;
    case Type::FLOAT32:
        writer.u32(value.float_bits);
        break;
    case Type::CHAR:
        check_size(value.text.size(), kMaxPayload - 5, "CHAR value");
        writer.u32(static_cast<std::uint32_t>(value.text.size()));
        writer.bytes(value.text);
        break;
    }
}

Value decode_value(Reader& reader, Type expected) {
    if (!is_supported_type(expected)) {
        throw ProtocolError("unknown typed value type");
    }
    Value value;
    value.type = expected;
    const std::uint8_t present = reader.u8();
    if (present > 1) {
        throw ProtocolError("invalid cell present flag");
    }
    value.present = present != 0;
    if (!value.present) {
        return value;
    }
    switch (expected) {
    case Type::INT32:
        value.int32 = int32_from_bits(reader.u32());
        break;
    case Type::FLOAT32:
        value.float_bits = reader.u32();
        break;
    case Type::CHAR: {
        const auto length = reader.u32();
        if (length > kMaxPayload - 5) {
            throw ProtocolError("CHAR value exceeds protocol limit");
        }
        value.text = reader.bytes(length);
        break;
    }
    }
    return value;
}

} // namespace wire_protocol
