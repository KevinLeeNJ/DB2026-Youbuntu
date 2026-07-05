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

#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <thread>

#include "errors.h"
#include "instance/db_instance.h"
#include "minilog.h"
#include "recovery/checkpoint_manager.h"
#include "server/session.h"

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 8

using namespace rmdb;

static bool should_exit = false;

// 信号处理函数需要全局访问点；Phase 7 StatementRunner 重构后移除（见 migration-ledger）。
static rmdb::instance::DBInstance* g_instance = nullptr;

static jmp_buf jmpbuf;
void sigint_handler(int signo) {
    (void)signo;
    should_exit = true;
    if (g_instance != nullptr) {
        g_instance->log_manager().flush_log_to_disk_with_sync();
    }
    LOG_INFO("the server received Ctrl+C and will close");
    longjmp(jmpbuf, 1);
}

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(rmdb::server::Session& session, Context* context, rmdb::instance::DBInstance& instance) {
    auto& txn_manager = instance.transaction_manager();
    context->txn_ = txn_manager.get_transaction(session.txn_id());
    if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
        context->txn_->get_state() == TransactionState::ABORTED) {
        context->txn_ = txn_manager.begin(nullptr, context->log_mgr_, context->isolation_level_);
        session.set_txn_id(context->txn_->get_transaction_id());
        context->txn_->set_txn_mode(false);
        context->txn_->set_isolation_level(context->isolation_level_);
    }
    txn_manager.BeginStatement(context->txn_);
}

void client_handler(int fd, rmdb::instance::DBInstance& instance) {
    int i_recvBytes;
    // 接收客户端发送的请求
    char data_recv[BUFFER_LENGTH];
    // per-client 状态：事务 id、隔离级别、输出缓冲区
    rmdb::server::Session session;

    auto& lock_manager = instance.lock_manager();
    auto& log_manager = instance.log_manager();
    auto& txn_manager = instance.transaction_manager();
    auto& schema_manager = instance.schema_manager();
    auto& analyze = instance.analyze();
    auto& optimizer = instance.optimizer();
    auto& ql_manager = instance.ql_manager();
    auto& portal = instance.portal();

    LOG_INFO("establish client connection, sockfd: %d", fd);

    while (true) {
        LOG_DEBUG("waiting for request on sockfd: %d", fd);
        memset(data_recv, 0, BUFFER_LENGTH);

        i_recvBytes = read(fd, data_recv, BUFFER_LENGTH);

        if (i_recvBytes == 0) {
            LOG_WARN("client may have closed, sockfd: %d", fd);
            break;
        }
        if (i_recvBytes == -1) {
            LOG_ERROR("client read error on sockfd %d: %s", fd, strerror(errno));
            break;
        }

        LOG_DEBUG("received %d bytes from sockfd: %d", i_recvBytes, fd);

        if (strcmp(data_recv, "exit") == 0) {
            LOG_INFO("client exit, sockfd: %d", fd);
            break;
        }
        if (strcmp(data_recv, "crash") == 0) {
            LOG_ERROR("server crash command received from sockfd: %d", fd);
            minilog::Logger::get().flush();
            log_manager.flush_log_to_disk_with_sync();
            exit(1);
        }

        session.reset_buffer();
        char* data_send = session.data_send();
        int* offset_ptr = session.offset_ptr();

        // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
        auto _context =
            std::make_unique<Context>(&lock_manager, &log_manager, nullptr, data_send, offset_ptr, &txn_manager);
        Context* context = _context.get();
        context->isolation_level_ = session.isolation_level();

        std::unique_ptr<rmdb::parser::ast::TreeNode> parse_tree;
        try {
            parse_tree = rmdb::parser::ast::parse_sql(data_recv);
        } catch (const rmdb::parser::ast::ParseError& e) {
            LOG_ERROR("parse failed for SQL [%s]: %s", data_recv, e.what());
            const char* msg = e.what();
            int msg_len = strlen(msg);
            memcpy(data_send, msg, msg_len);
            data_send[msg_len] = '\n';
            data_send[msg_len + 1] = '\0';
            *offset_ptr = msg_len + 1;
        }

        if (parse_tree != nullptr) {
            try {
                auto parsed_type = parse_tree->type;
                bool is_checkpoint = parsed_type == rmdb::parser::ast::AstType::StaticCheckpoint;
                bool is_load = parsed_type == rmdb::parser::ast::AstType::LoadStmt;
                if (!is_checkpoint && !is_load) {
                    SetTransaction(session, context, instance);
                }
                // analyze and rewrite
                std::unique_ptr<Query> query = analyze.do_analyze(std::move(parse_tree));
                LOG_DEBUG("Parse successful for sockfd: %d, type: %d", fd, static_cast<int>(parsed_type));
                // 优化器
                std::unique_ptr<Plan> plan = optimizer.plan_query(std::move(query), context);
                // portal
                std::unique_ptr<PortalStmt> portalStmt = portal.start(std::move(plan), context);
                portal.run(std::move(portalStmt), &ql_manager, session.txn_id_ptr(), context);
                // Persist isolation level change (SET TRANSACTION ISOLATION LEVEL)
                session.set_isolation_level(context->isolation_level_);
                // Note: "set output_file on|off" is a database-global toggle stored
                // on SmManager (see execution_manager.cpp T_SetOutputFile), so it
                // persists across connections without per-session mirroring here.
                portal.drop();
                if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
                    context->txn_->get_state() != TransactionState::COMMITTED &&
                    context->txn_->get_state() != TransactionState::ABORTED) {
                    txn_manager.commit(context->txn_, context->log_mgr_);
                }
                context->txn_ = nullptr;
            } catch (TransactionAbortException& e) {
                // 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
                std::string str = "abort\n";
                memcpy(data_send, str.c_str(), str.length());
                data_send[str.length()] = '\0';
                *offset_ptr = str.length();

                // 回滚事务
                if (context->txn_ != nullptr && context->txn_->get_state() != TransactionState::ABORTED &&
                    context->txn_->get_state() != TransactionState::COMMITTED) {
                    txn_manager.abort(context->txn_, &log_manager);
                }
                context->txn_ = nullptr;
                LOG_WARN("transaction aborted: %s", e.GetInfo().c_str());

                if (schema_manager.output_file_enabled()) {
                    std::fstream outfile;
                    outfile.open("output.txt", std::ios::out | std::ios::app);
                    outfile << str;
                    outfile.close();
                }
            } catch (RMDBError& e) {
                // 遇到异常，需要打印failure到output.txt文件中，并发异常信息返回给客户端
                LOG_ERROR("RMDBError: %s", e.what());

                memcpy(data_send, e.what(), e.get_msg_len());
                data_send[e.get_msg_len()] = '\n';
                data_send[e.get_msg_len() + 1] = '\0';
                *offset_ptr = e.get_msg_len() + 1;

                if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
                    context->txn_->get_state() != TransactionState::COMMITTED &&
                    context->txn_->get_state() != TransactionState::ABORTED) {
                    txn_manager.abort(context->txn_, context->log_mgr_);
                }
                context->txn_ = nullptr;

                // 将报错信息写入output.txt
                if (schema_manager.output_file_enabled()) {
                    std::fstream outfile;
                    outfile.open("output.txt", std::ios::out | std::ios::app);
                    outfile << "failure\n";
                    outfile.close();
                }
            } catch (const std::exception& e) {
                LOG_ERROR("%s", e.what());

                if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
                    context->txn_->get_state() != TransactionState::COMMITTED &&
                    context->txn_->get_state() != TransactionState::ABORTED) {
                    txn_manager.abort(context->txn_, context->log_mgr_);
                }
                context->txn_ = nullptr;

                const char* msg = e.what();
                int msg_len = strlen(msg);
                memcpy(data_send, msg, msg_len);
                data_send[msg_len] = '\n';
                data_send[msg_len + 1] = '\0';
                *offset_ptr = msg_len + 1;
            }
        }
        // future TODO: 格式化 sql_handler.result, 传给客户端
        // send result with fixed format, use protobuf in the future
        if (write(fd, data_send, session.offset() + 1) == -1) {
            break;
        }
    }

    // Clear
    LOG_INFO("terminating client connection, sockfd: %d", fd);
    close(fd); // close a file descriptor.
    return;    // terminate calling thread!
}

void start_server(rmdb::instance::DBInstance& instance) {
    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in {};

    // 初始化连接
    sockfd_server = socket(AF_INET, SOCK_STREAM, 0); // ipv4,TCP
    assert(sockfd_server != -1);
    int val = 1;
    setsockopt(sockfd_server, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // before bind(), set the attr of structure sockaddr.
    memset(&s_addr_in, 0, sizeof(s_addr_in));
    s_addr_in.sin_family = AF_INET;
    s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
    s_addr_in.sin_port = htons(SOCK_PORT);
    fd_temp = bind(sockfd_server, (struct sockaddr*)(&s_addr_in), sizeof(s_addr_in));
    if (fd_temp == -1) {
        LOG_ERROR("bind failed: %s", strerror(errno));
        minilog::Logger::get().stop();
        exit(1);
    }

    fd_temp = listen(sockfd_server, MAX_CONN_LIMIT);
    if (fd_temp == -1) {
        LOG_ERROR("listen failed: %s", strerror(errno));
        minilog::Logger::get().stop();
        exit(1);
    }

    while (!should_exit) {
        LOG_DEBUG("waiting for new connection");
        struct sockaddr_in s_addr_client {};
        int client_length = sizeof(s_addr_client);

        if (setjmp(jmpbuf)) {
            LOG_INFO("break from server listen loop");
            break;
        }

        // Block here. Until server accepts a new connection.
        int sockfd = accept(sockfd_server, (struct sockaddr*)(&s_addr_client), (socklen_t*)(&client_length));
        if (sockfd == -1) {
            LOG_WARN("accept failed: %s", strerror(errno));
            continue; // ignore current socket ,continue while loop.
        }

        // 和客户端建立连接，并开启一个线程负责处理客户端请求
        std::thread(client_handler, sockfd, std::ref(instance)).detach();
    }

    // Clear
    LOG_INFO("try to close all client connections");
    int ret = shutdown(sockfd_server, SHUT_WR); // shut down the all or part of a full-duplex connection.
    if (ret == -1) {
        LOG_ERROR("shutdown server socket failed: %s", strerror(errno));
    }
    //    assert(ret != -1);
    LOG_INFO("server shuts down");
}

int main(int argc, char** argv) {
    minilog::Logger::get().init("rmdb.log");
    minilog::Logger::get().set_level(minilog::LogLevel::INFO);

    if (argc != 2) {
        // 需要指定数据库名称
        LOG_ERROR("usage: %s <database>", argv[0]);
        minilog::Logger::get().stop();
        exit(1);
    }

    signal(SIGINT, sigint_handler);
    signal(SIGPIPE, SIG_IGN);
    try {
        std::cout << "\n"
                     "  _____  __  __ _____  ____  \n"
                     " |  __ \\|  \\/  |  __ \\|  _ \\ \n"
                     " | |__) | \\  / | |  | | |_) |\n"
                     " |  _  /| |\\/| | |  | |  _ < \n"
                     " | | \\ \\| |  | | |__| | |_) |\n"
                     " |_|  \\_\\_|  |_|_____/|____/ \n"
                     "\n"
                     "Welcome to RMDB!\n"
                     "Type 'help;' for help.\n"
                     "\n";
        // Database name is passed by args
        std::string db_name = argv[1];
        LOG_INFO("RMDB server starting, database: %s", db_name.c_str());

        rmdb::instance::DBInstance instance;
        g_instance = &instance;
        instance.open_database(db_name);
        LOG_INFO("database opened: %s", db_name.c_str());

        // recovery database
        instance.run_recovery();
        LOG_INFO("database recovery finished");

        {
            std::atomic<bool> checkpoint_thread_stop{false};
            std::thread checkpoint_thread([&instance, &checkpoint_thread_stop] {
                CheckpointManager checkpoint_mgr(&instance.transaction_manager(), &instance.schema_manager(),
                                                 &instance.log_manager());
                while (!checkpoint_thread_stop.load()) {
                    std::this_thread::sleep_for(std::chrono::seconds(2));
                    if (checkpoint_thread_stop.load()) {
                        break;
                    }
                    checkpoint_mgr.RunIfNeeded();
                }
            });

            // 开启服务端，开始接受客户端连接
            start_server(instance);

            checkpoint_thread_stop.store(true);
            if (checkpoint_thread.joinable()) {
                checkpoint_thread.join();
            }
        }

        instance.close_database();
        g_instance = nullptr;
        LOG_INFO("database has been closed");
    } catch (RMDBError& e) {
        LOG_ERROR("RMDB error: %s", e.what());
        minilog::Logger::get().stop();
        exit(1);
    }
    minilog::Logger::get().stop();
    return 0;
}
