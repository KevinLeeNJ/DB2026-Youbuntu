/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "wire_protocol.h"

#include <cerrno>
#include <sys/socket.h>
#include <unistd.h>

#include <exception>
#include <algorithm>
#include <thread>
#include <gtest/gtest.h>

namespace {

void send_bytes(int fd, const std::vector<std::uint8_t>& bytes) {
    wire_protocol::write_all(fd, bytes.data(), bytes.size());
}

std::vector<std::uint8_t> raw_frame(std::uint32_t length, std::uint8_t tag, std::uint8_t flags = 0,
                                    std::uint16_t reserved = 0) {
    return {static_cast<std::uint8_t>(length >> 24),
            static_cast<std::uint8_t>(length >> 16),
            static_cast<std::uint8_t>(length >> 8),
            static_cast<std::uint8_t>(length),
            tag,
            flags,
            static_cast<std::uint8_t>(reserved >> 8),
            static_cast<std::uint8_t>(reserved)};
}

void close_pair(int (&sockets)[2]) {
    close(sockets[0]);
    close(sockets[1]);
}

} // namespace

TEST(WireProtocolTest, UsesBigEndianPayloadFields) {
    wire_protocol::Writer writer;
    writer.u16(0x1234);
    writer.u32(0x89abcdef);
    writer.u64(0x0123456789abcdefULL);
    EXPECT_EQ(writer.data(), (std::vector<std::uint8_t>{0x12, 0x34, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67,
                                                        0x89, 0xab, 0xcd, 0xef}));
}

TEST(WireProtocolTest, PrepareOkUsesOnlyTheSpecifiedSchemaFields) {
    const std::vector<wire_protocol::PreparedSchema> schemas = {
        {7, {{"value", wire_protocol::Type::INT32}, {"price", wire_protocol::Type::FLOAT32}}},
        {9, {}},
    };
    EXPECT_EQ(wire_protocol::encode_prepare_ok(schemas),
              (std::vector<std::uint8_t>{0, 2, 0, 7,   0,   2,   0,   5,   'v', 'a', 'l', 'u', 'e',
                                         1, 0, 5, 'p', 'r', 'i', 'c', 'e', 2,   0,   9,   0,   0}));
}

TEST(WireProtocolTest, PrepareOkRejectsInvalidSchemaMetadata) {
    EXPECT_THROW(wire_protocol::encode_prepare_ok({{0, {}}}), wire_protocol::ProtocolError);
    EXPECT_THROW(wire_protocol::encode_prepare_ok({{1, {{"", wire_protocol::Type::INT32}}}}),
                 wire_protocol::ProtocolError);
    EXPECT_THROW(wire_protocol::encode_prepare_ok({{1, {{"value", static_cast<wire_protocol::Type>(0xff)}}}}),
                 wire_protocol::ProtocolError);
    EXPECT_THROW(wire_protocol::encode_prepare_ok({{1, {{std::string("\xc0\x80"), wire_protocol::Type::CHAR}}}}),
                 wire_protocol::ProtocolError);
}

TEST(WireProtocolTest, RejectsTrailingBytesAndPreservesFloatBits) {
    wire_protocol::Value value;
    value.type = wire_protocol::Type::FLOAT32;
    value.float_bits = 0x7fc01234;
    wire_protocol::Writer writer;
    wire_protocol::encode_value(writer, value, wire_protocol::Type::FLOAT32);
    auto payload = writer.take();
    payload.push_back(0);
    wire_protocol::Reader reader(payload);
    EXPECT_EQ(wire_protocol::decode_value(reader, wire_protocol::Type::FLOAT32).float_bits, 0x7fc01234U);
    EXPECT_THROW(reader.require_end(), wire_protocol::ProtocolError);
}

TEST(WireProtocolTest, EncodesNullAndIntegerBitsWithoutCoercion) {
    wire_protocol::Value integer;
    integer.type = wire_protocol::Type::INT32;
    integer.int32 = -1;
    wire_protocol::Writer integer_writer;
    wire_protocol::encode_value(integer_writer, integer, wire_protocol::Type::INT32);
    EXPECT_EQ(integer_writer.data(), (std::vector<std::uint8_t>{1, 0xff, 0xff, 0xff, 0xff}));

    wire_protocol::Reader integer_reader(integer_writer.data());
    EXPECT_EQ(wire_protocol::decode_value(integer_reader, wire_protocol::Type::INT32).int32, -1);
    EXPECT_NO_THROW(integer_reader.require_end());

    wire_protocol::Value null_value;
    null_value.type = wire_protocol::Type::CHAR;
    null_value.present = false;
    null_value.text = "must not be encoded";
    wire_protocol::Writer null_writer;
    wire_protocol::encode_value(null_writer, null_value, wire_protocol::Type::CHAR);
    EXPECT_EQ(null_writer.data(), (std::vector<std::uint8_t>{0}));

    wire_protocol::Reader null_reader(null_writer.data());
    const auto decoded = wire_protocol::decode_value(null_reader, wire_protocol::Type::CHAR);
    EXPECT_FALSE(decoded.present);
    EXPECT_TRUE(decoded.text.empty());
    EXPECT_NO_THROW(null_reader.require_end());
}

// 一行里 NULL 与非 NULL 混排是 protocol_row 的实际输出形状（例如
// `select ol_i_id, ol_amount, ol_delivery_d ...` 里 ol_delivery_d 为 NULL）。
// present == 0 的 cell 后面不能有任何值字节，NULL 也不得编码为空字符串
// （final.md:608 / :761）。
TEST(WireProtocolTest, MixedNullRowRoundTripsForEveryType) {
    const std::vector<wire_protocol::Type> types = {
        wire_protocol::Type::INT32, wire_protocol::Type::INT32,   wire_protocol::Type::FLOAT32,
        wire_protocol::Type::CHAR,  wire_protocol::Type::FLOAT32, wire_protocol::Type::CHAR,
    };

    std::vector<wire_protocol::Value> row(types.size());
    for (std::size_t i = 0; i < types.size(); ++i) {
        row[i].type = types[i];
    }
    row[0].int32 = 42;
    row[1].present = false; // NULL INT32
    row[2].float_bits = 0x3f800000U;
    row[3].present = false; // NULL CHAR
    row[3].text = "must not be encoded";
    row[4].present = false; // NULL FLOAT32
    row[4].float_bits = 0xdeadbeefU;
    row[5].text = "dist";

    wire_protocol::Writer writer;
    for (std::size_t i = 0; i < row.size(); ++i) {
        wire_protocol::encode_value(writer, row[i], types[i]);
    }
    // 3 个 NULL 各占 1 字节，INT32/FLOAT32 各 5 字节，CHAR 是 1 + 4 + 4 字节。
    EXPECT_EQ(writer.data().size(), 3U + 5U + 5U + 9U);

    wire_protocol::Reader reader(writer.data());
    std::vector<wire_protocol::Value> decoded;
    for (const auto type : types) {
        decoded.push_back(wire_protocol::decode_value(reader, type));
    }
    EXPECT_NO_THROW(reader.require_end());

    ASSERT_EQ(decoded.size(), types.size());
    EXPECT_TRUE(decoded[0].present);
    EXPECT_EQ(decoded[0].int32, 42);
    EXPECT_FALSE(decoded[1].present);
    EXPECT_TRUE(decoded[2].present);
    EXPECT_EQ(decoded[2].float_bits, 0x3f800000U);
    EXPECT_FALSE(decoded[3].present);
    EXPECT_TRUE(decoded[3].text.empty()); // NULL CHAR 不是空字符串，它没有值字节
    EXPECT_FALSE(decoded[4].present);
    EXPECT_TRUE(decoded[5].present);
    EXPECT_EQ(decoded[5].text, "dist");
}

TEST(WireProtocolTest, RejectsInvalidTypedValueBoundaries) {
    const auto invalid_type = static_cast<wire_protocol::Type>(0xff);
    wire_protocol::Value value;
    value.type = invalid_type;
    wire_protocol::Writer writer;
    EXPECT_THROW(wire_protocol::encode_value(writer, value, invalid_type), wire_protocol::ProtocolError);

    const std::vector<std::uint8_t> empty_payload;
    wire_protocol::Reader invalid_type_reader(empty_payload);
    EXPECT_THROW(wire_protocol::decode_value(invalid_type_reader, invalid_type), wire_protocol::ProtocolError);

    const std::vector<std::uint8_t> invalid_present = {2};
    wire_protocol::Reader invalid_present_reader(invalid_present);
    EXPECT_THROW(wire_protocol::decode_value(invalid_present_reader, wire_protocol::Type::INT32),
                 wire_protocol::ProtocolError);

    std::vector<std::uint8_t> oversized_char = {1, 0x00, 0x10, 0x00, 0x00};
    wire_protocol::Reader oversized_char_reader(oversized_char);
    EXPECT_THROW(wire_protocol::decode_value(oversized_char_reader, wire_protocol::Type::CHAR),
                 wire_protocol::ProtocolError);

    wire_protocol::Value mismatched;
    mismatched.type = wire_protocol::Type::INT32;
    EXPECT_THROW(wire_protocol::encode_value(writer, mismatched, wire_protocol::Type::FLOAT32),
                 wire_protocol::ProtocolError);
}

TEST(WireProtocolTest, WriterAndFrameRespectPayloadLimit) {
    std::vector<std::uint8_t> payload(wire_protocol::kMaxPayload + 1, 0);
    EXPECT_THROW(wire_protocol::write_frame(-1, wire_protocol::Tag::ROW, payload), wire_protocol::ProtocolError);

    wire_protocol::Writer writer;
    std::vector<std::uint8_t> max_payload(wire_protocol::kMaxPayload, 0);
    EXPECT_NO_THROW(writer.bytes(max_payload.data(), max_payload.size()));
    EXPECT_THROW(writer.u8(0), wire_protocol::ProtocolError);
}

TEST(WireProtocolTest, FrameRoundTripHandlesSocketStreams) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    const std::vector<std::uint8_t> payload = {0, 1, 2, 3, 4};
    wire_protocol::write_frame(sockets[0], wire_protocol::Tag::ROW, payload);
    wire_protocol::Frame frame;
    ASSERT_TRUE(wire_protocol::read_frame(sockets[1], frame));
    EXPECT_EQ(frame.tag, wire_protocol::Tag::ROW);
    EXPECT_EQ(frame.payload, payload);
    close_pair(sockets);
}

TEST(WireProtocolTest, WriteFrameHandlesPartialVectoredWrites) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    int send_buffer = 1024;
    ASSERT_EQ(setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &send_buffer, sizeof(send_buffer)), 0);

    const std::vector<std::uint8_t> payload(wire_protocol::kMaxPayload, 0xa5);
    std::exception_ptr writer_error;
    std::thread writer([&] {
        try {
            wire_protocol::write_frame(sockets[0], wire_protocol::Tag::ROW, payload);
        } catch (...) {
            writer_error = std::current_exception();
        }
    });

    const auto expected_header =
        raw_frame(static_cast<std::uint32_t>(payload.size()), static_cast<std::uint8_t>(wire_protocol::Tag::ROW));
    std::vector<std::uint8_t> received(expected_header.size() + payload.size());
    std::size_t offset = 0;
    while (offset < received.size()) {
        const auto result = ::read(sockets[1], received.data() + offset, received.size() - offset);
        ASSERT_GT(result, 0);
        offset += static_cast<std::size_t>(result);
    }
    writer.join();

    ASSERT_EQ(writer_error, nullptr);
    EXPECT_TRUE(std::equal(expected_header.begin(), expected_header.end(), received.begin()));
    EXPECT_TRUE(std::equal(payload.begin(), payload.end(), received.begin() + expected_header.size()));
    close_pair(sockets);
}
TEST(WireProtocolTest, HandshakeEchoesTheEstablishedVersion) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    const std::vector<std::uint8_t> handshake = {'R', 'M', 'D', 'B', 0, 3, 0, 0};
    send_bytes(sockets[0], handshake);
    EXPECT_NO_THROW(wire_protocol::server_handshake(sockets[1]));

    std::vector<std::uint8_t> response(handshake.size());
    ASSERT_TRUE(wire_protocol::read_exact(sockets[0], response.data(), response.size()));
    EXPECT_EQ(response, handshake);
    close_pair(sockets);
}

TEST(WireProtocolTest, HandshakeRejectsWrongMagicOrVersion) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    send_bytes(sockets[0], {'R', 'M', 'D', 'X', 0, 3, 0, 0});
    EXPECT_THROW(wire_protocol::server_handshake(sockets[1]), wire_protocol::ProtocolError);
    close_pair(sockets);
}

TEST(WireProtocolTest, HandshakeAndFrameReadHandleShortReads) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    const std::vector<std::uint8_t> handshake = {'R', 'M', 'D', 'B', 0, 3, 0, 0};
    std::exception_ptr sender_error;
    std::thread sender([&] {
        try {
            for (const auto byte : handshake) {
                if (::send(sockets[0], &byte, 1, MSG_NOSIGNAL) != 1) {
                    throw std::runtime_error("short handshake send failed");
                }
            }
        } catch (...) {
            sender_error = std::current_exception();
        }
    });
    EXPECT_NO_THROW(wire_protocol::server_handshake(sockets[1]));
    sender.join();
    EXPECT_EQ(sender_error, nullptr);

    std::vector<std::uint8_t> response(handshake.size());
    ASSERT_TRUE(wire_protocol::read_exact(sockets[0], response.data(), response.size()));
    EXPECT_EQ(response, handshake);

    const auto header = raw_frame(3, static_cast<std::uint8_t>(wire_protocol::Tag::ROW));
    const std::vector<std::uint8_t> payload = {7, 8, 9};
    std::vector<std::uint8_t> encoded = header;
    encoded.insert(encoded.end(), payload.begin(), payload.end());
    std::thread frame_sender([&] {
        for (const auto byte : encoded) {
            if (::send(sockets[0], &byte, 1, MSG_NOSIGNAL) != 1) {
                break;
            }
        }
    });
    wire_protocol::Frame frame;
    ASSERT_TRUE(wire_protocol::read_frame(sockets[1], frame));
    frame_sender.join();
    EXPECT_EQ(frame.payload, payload);
    close_pair(sockets);
}

TEST(WireProtocolTest, WriteAllHandlesShortWritesAndEpipe) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    int send_buffer = 1024;
    ASSERT_EQ(setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &send_buffer, sizeof(send_buffer)), 0);

    std::vector<std::uint8_t> data(2 * 1024 * 1024, 0x5a);
    std::exception_ptr writer_error;
    std::thread writer([&] {
        try {
            wire_protocol::write_all(sockets[0], data.data(), data.size());
        } catch (...) {
            writer_error = std::current_exception();
        }
    });

    std::vector<std::uint8_t> received(data.size());
    std::size_t offset = 0;
    while (offset < received.size()) {
        const auto result = ::read(sockets[1], received.data() + offset, received.size() - offset);
        EXPECT_GT(result, 0);
        if (result <= 0) {
            break;
        }
        offset += static_cast<std::size_t>(result);
    }
    writer.join();
    EXPECT_EQ(writer_error, nullptr);
    EXPECT_EQ(received, data);
    close_pair(sockets);

    int broken_pair[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, broken_pair), 0);
    close(broken_pair[1]);
    const std::uint8_t byte = 1;
    EXPECT_THROW(wire_protocol::write_all(broken_pair[0], &byte, 1), wire_protocol::ProtocolError);
    close(broken_pair[0]);
}

TEST(WireProtocolTest, ReadFrameRejectsUnknownTagsIllegalFlagsAndReservedBits) {
    const auto check_rejected = [](const std::vector<std::uint8_t>& bytes) {
        int sockets[2];
        EXPECT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
        send_bytes(sockets[0], bytes);
        wire_protocol::Frame frame;
        EXPECT_THROW(wire_protocol::read_frame(sockets[1], frame), wire_protocol::ProtocolError);
        close_pair(sockets);
    };

    check_rejected(raw_frame(0, 0x7f));
    check_rejected(raw_frame(0, static_cast<std::uint8_t>(wire_protocol::Tag::ROW), 1));
    check_rejected(raw_frame(0, static_cast<std::uint8_t>(wire_protocol::Tag::ROW), 0, 1));
    check_rejected(raw_frame(static_cast<std::uint32_t>(wire_protocol::kMaxPayload + 1),
                             static_cast<std::uint8_t>(wire_protocol::Tag::ROW)));
}

TEST(WireProtocolTest, ReadFrameDistinguishesCleanEofFromTruncation) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    close(sockets[0]);
    wire_protocol::Frame frame;
    EXPECT_FALSE(wire_protocol::read_frame(sockets[1], frame));
    close(sockets[1]);

    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    send_bytes(sockets[0], {0, 0, 0});
    close(sockets[0]);
    EXPECT_THROW(wire_protocol::read_frame(sockets[1], frame), wire_protocol::ProtocolError);
    close(sockets[1]);

    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    auto truncated_payload = raw_frame(3, static_cast<std::uint8_t>(wire_protocol::Tag::ROW));
    truncated_payload.push_back(1);
    truncated_payload.push_back(2);
    send_bytes(sockets[0], truncated_payload);
    close(sockets[0]);
    EXPECT_THROW(wire_protocol::read_frame(sockets[1], frame), wire_protocol::ProtocolError);
    close(sockets[1]);
}

TEST(WireProtocolTest, WriteFrameRejectsUnknownTagsAndIllegalFlags) {
    EXPECT_THROW(wire_protocol::write_frame(-1, static_cast<wire_protocol::Tag>(0x7f), {}),
                 wire_protocol::ProtocolError);
    EXPECT_THROW(wire_protocol::write_frame(-1, wire_protocol::Tag::ROW, {}, 1), wire_protocol::ProtocolError);

    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    EXPECT_NO_THROW(wire_protocol::write_frame(sockets[0], wire_protocol::Tag::EXEC_BATCH, {}, 1));
    wire_protocol::Frame frame;
    ASSERT_TRUE(wire_protocol::read_frame(sockets[1], frame));
    EXPECT_EQ(frame.tag, wire_protocol::Tag::EXEC_BATCH);
    EXPECT_EQ(frame.flags, 1);
    close_pair(sockets);
}
