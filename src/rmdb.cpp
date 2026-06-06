/* Copyright (c) 2023 Renmin University of China
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

#include "errors.h"
#include "minilog.h"
#include "optimizer/optimizer.h"
#include "recovery/log_recovery.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "analyze/analyze.h"

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 8

static bool should_exit = false;

// 构建全局所需的管理器对象
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
auto ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
auto sm_manager =
    std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
auto lock_manager = std::make_unique<LockManager>();
auto txn_manager = std::make_unique<TransactionManager>(lock_manager.get(), sm_manager.get());
auto planner = std::make_unique<Planner>(sm_manager.get());
auto optimizer = std::make_unique<Optimizer>(sm_manager.get(), planner.get());
auto ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get(), nullptr);
auto log_manager = std::make_unique<LogManager>(disk_manager.get());
auto recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(), sm_manager.get());
auto portal = std::make_unique<Portal>(sm_manager.get());
auto analyze = std::make_unique<Analyze>(sm_manager.get());
pthread_mutex_t* buffer_mutex;
pthread_mutex_t* sockfd_mutex;

static jmp_buf jmpbuf;
void sigint_handler(int signo) {
    should_exit = true;
    log_manager->flush_log_to_disk();
    LOG_INFO("the server received Ctrl+C and will close");
    longjmp(jmpbuf, 1);
}

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(txn_id_t* txn_id, Context* context) {
    context->txn_ = txn_manager->get_transaction(*txn_id);
    if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
        context->txn_->get_state() == TransactionState::ABORTED) {
        context->txn_ = txn_manager->begin(nullptr, context->log_mgr_);
        *txn_id = context->txn_->get_transaction_id();
        context->txn_->set_txn_mode(false);
        context->txn_->set_isolation_level(context->isolation_level_);
    }
}

void* client_handler(void* sock_fd) {
    int fd = *((int*)sock_fd);
    pthread_mutex_unlock(sockfd_mutex);

    int i_recvBytes;
    // 接收客户端发送的请求
    char data_recv[BUFFER_LENGTH];
    // 需要返回给客户端的结果
    char* data_send = new char[BUFFER_LENGTH];
    // 需要返回给客户端的结果的长度
    int offset = 0;
    // 记录客户端当前正在执行的事务ID
    txn_id_t txn_id = INVALID_TXN_ID;
    // 记录客户端当前配置的隔离级别（默认 SERIALIZABLE）
    IsolationLevel session_isolation_level = IsolationLevel::SERIALIZABLE;

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
            exit(1);
        }

        memset(data_send, '\0', BUFFER_LENGTH);
        offset = 0;

        // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
        Context* context =
            new Context(lock_manager.get(), log_manager.get(), nullptr, data_send, &offset, txn_manager.get());
        context->isolation_level_ = session_isolation_level;

        // 用于判断是否已经调用了yy_delete_buffer来删除buf
        bool finish_analyze = false;
        pthread_mutex_lock(buffer_mutex);
        YY_BUFFER_STATE buf = yy_scan_string(data_recv);
        if (yyparse() == 0) {
            if (ast::parse_tree != nullptr) {
                try {
                    bool is_checkpoint = ast::parse_tree->type == ast::AstType::StaticCheckpoint;
                    if (!is_checkpoint) {
                        SetTransaction(&txn_id, context);
                    }
                    // analyze and rewrite
                    std::shared_ptr<Query> query = analyze->do_analyze(ast::parse_tree);
                    yy_delete_buffer(buf);
                    finish_analyze = true;
                    LOG_DEBUG("Parse successful for sockfd: %d, type: %d", fd, static_cast<int>(ast::parse_tree->type));
                    pthread_mutex_unlock(buffer_mutex);
                    // 优化器
                    std::shared_ptr<Plan> plan = optimizer->plan_query(query, context);
                    // portal
                    std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, context);
                    portal->run(portalStmt, ql_manager.get(), &txn_id, context);
                    // Persist isolation level change (SET TRANSACTION ISOLATION LEVEL)
                    session_isolation_level = context->isolation_level_;
                    portal->drop();
                    if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
                        context->txn_->get_state() != TransactionState::COMMITTED &&
                        context->txn_->get_state() != TransactionState::ABORTED) {
                        txn_manager->commit(context->txn_, context->log_mgr_);
                    }
                } catch (TransactionAbortException& e) {
                    // 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
                    std::string str = "abort\n";
                    memcpy(data_send, str.c_str(), str.length());
                    data_send[str.length()] = '\0';
                    offset = str.length();

                    // 回滚事务
                    if (context->txn_ != nullptr && context->txn_->get_state() != TransactionState::ABORTED &&
                        context->txn_->get_state() != TransactionState::COMMITTED) {
                        txn_manager->abort(context->txn_, log_manager.get());
                    }
                    LOG_WARN("transaction aborted: %s", e.GetInfo().c_str());

                    std::fstream outfile;
                    outfile.open("output.txt", std::ios::out | std::ios::app);
                    outfile << str;
                    outfile.close();
                } catch (RMDBError& e) {
                    // 遇到异常，需要打印failure到output.txt文件中，并发异常信息返回给客户端
                    LOG_ERROR("%s", e.what());

                    memcpy(data_send, e.what(), e.get_msg_len());
                    data_send[e.get_msg_len()] = '\n';
                    data_send[e.get_msg_len() + 1] = '\0';
                    offset = e.get_msg_len() + 1;

                    if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
                        context->txn_->get_state() != TransactionState::COMMITTED &&
                        context->txn_->get_state() != TransactionState::ABORTED) {
                        txn_manager->abort(context->txn_, context->log_mgr_);
                    }

                    // 将报错信息写入output.txt
                    std::fstream outfile;
                    outfile.open("output.txt", std::ios::out | std::ios::app);
                    outfile << "failure\n";
                    outfile.close();
                }
            }
        } else {
            // 解析失败，将 failure 信息写入 output.txt 并返回给客户端
            // where 中含有聚合函数时，会出现解析失败的情况，此处需要打印一个 failure
            LOG_ERROR("parse error");

            if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
                context->txn_->get_state() != TransactionState::COMMITTED &&
                context->txn_->get_state() != TransactionState::ABORTED) {
                txn_manager->abort(context->txn_, context->log_mgr_);
            }

            std::string str = "Parse error\n";
            memcpy(data_send, str.c_str(), str.length());
            data_send[str.length()] = '\0';
            offset = str.length();

            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << "failure\n";
            outfile.close();
        }
        if (finish_analyze == false) {
            yy_delete_buffer(buf);
            pthread_mutex_unlock(buffer_mutex);
        }
        // future TODO: 格式化 sql_handler.result, 传给客户端
        // send result with fixed format, use protobuf in the future
        if (write(fd, data_send, offset + 1) == -1) {
            break;
        }
    }

    // Clear
    LOG_INFO("terminating client connection, sockfd: %d", fd);
    close(fd);          // close a file descriptor.
    pthread_exit(NULL); // terminate calling thread!
}

void start_server() {
    // init mutex
    buffer_mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
    sockfd_mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(buffer_mutex, nullptr);
    pthread_mutex_init(sockfd_mutex, nullptr);

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
        pthread_t thread_id;
        struct sockaddr_in s_addr_client {};
        int client_length = sizeof(s_addr_client);

        if (setjmp(jmpbuf)) {
            LOG_INFO("break from server listen loop");
            break;
        }

        // Block here. Until server accepts a new connection.
        pthread_mutex_lock(sockfd_mutex);
        int sockfd = accept(sockfd_server, (struct sockaddr*)(&s_addr_client), (socklen_t*)(&client_length));
        if (sockfd == -1) {
            LOG_WARN("accept failed: %s", strerror(errno));
            continue; // ignore current socket ,continue while loop.
        }

        // 和客户端建立连接，并开启一个线程负责处理客户端请求
        if (pthread_create(&thread_id, nullptr, &client_handler, (void*)(&sockfd)) != 0) {
            LOG_ERROR("create client handler thread failed");
            break; // break while loop
        }
    }

    // Clear
    LOG_INFO("try to close all client connections");
    int ret = shutdown(sockfd_server, SHUT_WR); // shut down the all or part of a full-duplex connection.
    if (ret == -1) {
        LOG_ERROR("shutdown server socket failed: %s", strerror(errno));
    }
    //    assert(ret != -1);
    sm_manager->close_db();
    LOG_INFO("database has been closed");
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
        if (!sm_manager->is_dir(db_name)) {
            // Database not found, create a new one
            sm_manager->create_db(db_name);
            LOG_INFO("database created: %s", db_name.c_str());
        }
        // Open database
        sm_manager->open_db(db_name);
        LOG_INFO("database opened: %s", db_name.c_str());

        log_manager->initialize_from_existing_log();
        buffer_pool_manager->set_log_manager(log_manager.get());

        // recovery database
        recovery->analyze();
        recovery->redo();
        recovery->undo();
        LOG_INFO("database recovery finished");

        // 开启服务端，开始接受客户端连接
        start_server();
    } catch (RMDBError& e) {
        LOG_ERROR("RMDB error: %s", e.what());
        minilog::Logger::get().stop();
        exit(1);
    }
    minilog::Logger::get().stop();
    return 0;
}
