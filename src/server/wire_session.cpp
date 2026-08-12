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

#include "server/wire_session.h"

#include "errors.h"
#include "minilog.h"
#include "server/wire_session_internal.h"

namespace wire_session_internal {

void handle_client_frame(DatabaseInstance& database, int fd, const wire_protocol::Frame& frame, SessionState& session,
                         std::unordered_map<std::uint16_t, PreparedStatement>& prepared) {
    Reader reader(frame.payload);
    if (frame.tag == Tag::EXEC_STREAM) {
        handle_exec_stream(database, fd, frame, session, prepared);
        return;
    }
    if (frame.tag == Tag::PREPARE_SET) {
        if (frame.flags != 0) {
            throw wire_protocol::ProtocolError("invalid PREPARE_SET flags");
        }
        handle_prepare_set(database, fd, reader, session, prepared);
        return;
    }
    if (frame.tag != Tag::EXEC_BATCH || frame.flags != 1) {
        throw wire_protocol::ProtocolError("unknown request tag or flags");
    }
    handle_batch(database, fd, reader, session, prepared);
}

} // namespace wire_session_internal

using namespace wire_session_internal;

void serve_wire_session(DatabaseInstance& database, int fd) {
    SessionState session;
    std::unordered_map<std::uint16_t, PreparedStatement> prepared;
    LOG_INFO("establish protocol connection, sockfd: %d", fd);
    try {
        wire_protocol::server_handshake(fd);
        wire_protocol::Frame frame;
        while (wire_protocol::read_frame(fd, frame)) {
            try {
                handle_client_frame(database, fd, frame, session, prepared);
            } catch (TransactionAbortException& exception) {
                abort_session(database, session, nullptr);
                wire_protocol::write_frame(fd, Tag::TRANSACTION_ABORT, make_error_payload(exception.GetInfo()));
            } catch (const std::exception& exception) {
                abort_session(database, session, nullptr);
                wire_protocol::write_frame(fd, Tag::ERROR, make_error_payload(diagnostic(exception)));
            }
        }
    } catch (const std::exception& exception) {
        LOG_WARN("protocol connection closed: %s", exception.what());
        abort_session(database, session, nullptr);
    }
    abort_session(database, session, nullptr);
}
