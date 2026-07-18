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
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <optional>
#include <string>
#include <thread>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "errors.h"
#include "cache/statement_template_cache.h"
#include "compiled/program_cache.h"
#include "execution/runtime/program_dispatcher.h"
#include "common/phase_metrics.h"
#include "index/ix_scan.h"
#include "minilog.h"
#include "optimizer/optimizer.h"
#include "recovery/checkpoint_manager.h"
#include "recovery/log_recovery.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "analyze/analyze.h"
#ifdef RMDB_ENABLE_JIT
#include "jit/jit_predicate.h"
#endif

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 8

static bool should_exit = false;

#ifdef RMDB_ENABLE_JIT
class ClientConnectionTracker {
public:
    bool enter(int fd) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return false;
        }
        active_fds_.insert(fd);
        return true;
    }

    void leave(int fd) {
        std::lock_guard<std::mutex> lock(mutex_);
        active_fds_.erase(fd);
        drained_.notify_all();
    }

    void stop_and_drain() {
        std::vector<int> fds;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
            fds.assign(active_fds_.begin(), active_fds_.end());
        }
        for (int fd : fds) {
            shutdown(fd, SHUT_RDWR);
        }
        std::unique_lock<std::mutex> lock(mutex_);
        drained_.wait(lock, [&] { return active_fds_.empty(); });
    }

private:
    std::mutex mutex_;
    std::condition_variable drained_;
    std::unordered_set<int> active_fds_;
    bool stopping_{false};
};

ClientConnectionTracker client_connections;
#endif

void write_phase_metrics(const std::string& path, const cache::StatementTemplateCache* template_cache,
                         const compiled::ProgramTemplateCache* program_cache) {
    const std::string temporary_path = path + ".tmp." + std::to_string(getpid());
    std::ofstream output(temporary_path, std::ios::out | std::ios::trunc);
    if (!output.is_open()) {
        return;
    }

    output << "{\n  \"format_version\": 2,\n  \"clock\": \"steady_clock_ns\",\n  \"phases\": {\n";
    constexpr size_t phase_count = static_cast<size_t>(phase_metrics::Phase::COUNT);
    for (size_t i = 0; i < phase_count; ++i) {
        const auto phase = static_cast<phase_metrics::Phase>(i);
        const auto snapshot = phase_metrics::Registry::instance().snapshot(phase);
        const auto rate = phase_metrics::sample_rate(phase);
        output << "    \"" << phase_metrics::phase_name(phase) << "\": {\"sample_rate\": " << rate
               << ", \"samples\": " << snapshot.samples << ", \"estimated_calls\": " << snapshot.samples * rate
               << ", \"sampled_ns\": " << snapshot.sampled_ns << ", \"estimated_ns\": " << snapshot.sampled_ns * rate
               << ", \"sampled_cycles\": " << snapshot.sampled_cycles
               << ", \"estimated_cycles\": " << snapshot.sampled_cycles * rate << "}";
        output << (i + 1 == phase_count ? "\n" : ",\n");
    }
    output << "  },\n  \"statement_template_cache\": ";
    if (template_cache != nullptr) {
        const auto stats = template_cache->stats();
        output << "{\"lookups\": " << stats.lookups << ", \"hits\": " << stats.hits
               << ", \"misses\": " << stats.misses << ", \"publishes\": " << stats.publishes
               << ", \"evictions\": " << stats.evictions << "}";
    } else {
        output << "null";
    }
    output << ",\n  \"program_template_cache\": ";
    if (program_cache != nullptr) {
        const auto stats = program_cache->Stats();
        output << "{\"hits\": " << stats.hits << ", \"misses\": " << stats.misses
               << ", \"fallbacks\": " << stats.fallbacks << ", \"handled\": " << stats.handled
               << ", \"entries\": " << stats.entries << "}";
    } else {
        output << "null";
    }
#ifdef RMDB_ENABLE_JIT
    const auto jit_stats = jit::predicate_jit_stats();
    output << ",\n  \"jit\": {\"entry_count\": " << jit_stats.entry_count
           << ", \"queued_count\": " << jit_stats.queued_count << ", \"code_bytes\": "
           << jit_stats.code_bytes << ", \"cache_hits\": " << jit_stats.cache_hits
           << ", \"fallbacks\": " << jit_stats.fallbacks << ", \"compile_attempts\": "
           << jit_stats.compile_attempts << ", \"compile_failures\": " << jit_stats.compile_failures
           << ", \"evictions\": " << jit_stats.evictions << "}";
#else
    output << ",\n  \"jit\": null";
#endif
    output << "\n}\n";
    output.close();
    if (output) {
        std::rename(temporary_path.c_str(), path.c_str());
    }
}

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
auto ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get(), planner.get());
auto log_manager = std::make_unique<LogManager>(disk_manager.get(),
                                                std::getenv("RMDB_DURABILITY_MODE") != nullptr &&
                                                        std::string(std::getenv("RMDB_DURABILITY_MODE")) == "strict"
                                                    ? DurabilityMode::STRICT
                                                    : DurabilityMode::PROCESS_CRASH);
auto recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(), sm_manager.get(),
                                                  log_manager.get());

auto point_program_template_cache = std::make_unique<compiled::ProgramTemplateCache>();
auto portal = std::make_unique<Portal>(sm_manager.get(), point_program_template_cache.get());
auto analyze = std::make_unique<Analyze>(sm_manager.get());
auto statement_template_cache = std::make_unique<cache::StatementTemplateCache>();
auto statement_template_generation = []() {
    return sm_manager->get_catalog_generation() ^ (planner->planner_knob_generation() * 0x9e3779b97f4a7c15ULL);
};
#ifdef RMDB_ENABLE_JIT
#endif

static jmp_buf jmpbuf;
void sigint_handler(int signo) {
    (void)signo;
    should_exit = true;
    log_manager->flush_log_to_disk_with_sync();
    LOG_INFO("the server received Ctrl+C and will close");
    longjmp(jmpbuf, 1);
}

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(txn_id_t* txn_id, Context* context) {
    context->txn_ = txn_manager->get_transaction(*txn_id);
    if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
        context->txn_->get_state() == TransactionState::ABORTED) {
        context->txn_ = txn_manager->begin(nullptr, context->log_mgr_, context->isolation_level_);
        *txn_id = context->txn_->get_transaction_id();
        context->txn_->set_txn_mode(false);
        context->txn_->set_isolation_level(context->isolation_level_);
    }
    txn_manager->BeginStatement(context->txn_);
}

void client_handler(int fd) {
#ifdef RMDB_ENABLE_JIT
    if (!client_connections.enter(fd)) {
        close(fd);
        return;
    }
    struct ClientConnectionScope {
        int fd;
        ~ClientConnectionScope() {
            client_connections.leave(fd);
        }
    } client_connection_scope{fd};
#endif

    int i_recvBytes;
    // 接收客户端发送的请求
    char data_recv[BUFFER_LENGTH];
    // 需要返回给客户端的结果
    char data_send[BUFFER_LENGTH];
    // 需要返回给客户端的结果的长度
    int offset = 0;
    // 记录客户端当前正在执行的事务ID
    txn_id_t txn_id = INVALID_TXN_ID;
    // 记录客户端当前配置的隔离级别
    IsolationLevel session_isolation_level = DEFAULT_ISOLATION_LEVEL;

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
            log_manager->flush_log_to_disk_with_sync();
            exit(1);
        }

#ifdef RMDB_ENABLE_JIT
        auto jit_execution_scope = jit::enter_predicate_jit_execution();
#endif

        memset(data_send, '\0', BUFFER_LENGTH);
        offset = 0;

        parser::OwnedTokenStream lexical_shape;
        const auto statement_cache_mode = rmdb_config::statement_cache_mode;
        std::unique_ptr<ast::TreeNode> cached_parse_tree;
        std::optional<ast::AstType> cached_statement_type;
        std::unique_ptr<Query> cached_query;
        std::unique_ptr<Plan> cached_plan;
        bool used_cached_parse = false;
        bool used_cached_query = false;
        bool used_cached_plan = false;
        bool cacheable_skeleton = false;
        bool cacheable_query = false;
        bool cacheable_plan = false;
        uint64_t current_statement_template_generation = 0;
        uint64_t current_planner_generation = 0;
        compiled::ProgramTemplatePtr cached_point_program;
        const bool point_program_cache_enabled = [] {
            const char* value = std::getenv("ENABLE_POINT_PROGRAM_CACHE");
            return value != nullptr && std::string(value) == "1";
        }();
        if (statement_cache_mode != cache::StatementCacheMode::OFF || point_program_cache_enabled) {
            phase_metrics::ScopedSample metrics_sample(phase_metrics::Phase::NORMALIZE,
                                                       phase_metrics::sample_rate(phase_metrics::Phase::NORMALIZE));
            lexical_shape = parser::normalize_sql(data_recv, false);
            cacheable_skeleton = static_cast<bool>(lexical_shape) && !lexical_shape.template_unsupported;
            cacheable_plan = cacheable_skeleton;
            cacheable_query = cacheable_skeleton;
            current_planner_generation = planner->planner_knob_generation();
            current_statement_template_generation = statement_template_generation();
            if (point_program_cache_enabled && lexical_shape && cacheable_plan) {
                phase_metrics::ScopedSample program_cache_sample(
                    phase_metrics::Phase::PROGRAM_TEMPLATE_CACHE,
                    phase_metrics::sample_rate(phase_metrics::Phase::PROGRAM_TEMPLATE_CACHE));
                cached_point_program = point_program_template_cache->LookupAny(
                    lexical_shape.key, current_statement_template_generation, current_planner_generation,
                    sm_manager->get_catalog_generation());
            }
            if (cached_point_program == nullptr && lexical_shape && cacheable_plan &&
                statement_cache_mode == cache::StatementCacheMode::FULL) {
                auto full = statement_template_cache->lookup_full(
                    lexical_shape.key, current_statement_template_generation, sm_manager.get(), &lexical_shape);
                if (full) {
                    cached_statement_type = full.statement_type;
                    cached_plan = std::move(full.plan);
                    used_cached_parse = true;
                    used_cached_plan = true;
                }
            }
            if (cached_point_program == nullptr && cached_plan == nullptr) {
                if (lexical_shape && cacheable_skeleton && cacheable_query &&
                    static_cast<int>(statement_cache_mode) >= static_cast<int>(cache::StatementCacheMode::ANALYZER)) {
                    cached_query = statement_template_cache->lookup_query(
                        lexical_shape.key, current_statement_template_generation, &lexical_shape);
                    if (cached_query != nullptr && cached_query->parse != nullptr) {
                        cached_parse_tree = ast::clone_tree(*cached_query->parse);
                        used_cached_parse = true;
                        used_cached_query = true;
                    }
                } else if (lexical_shape && cacheable_skeleton &&
                           static_cast<int>(statement_cache_mode) >=
                               static_cast<int>(cache::StatementCacheMode::PARSER)) {
                    cached_parse_tree = statement_template_cache->lookup_ast(
                        lexical_shape.key, current_statement_template_generation, &lexical_shape);
                    used_cached_parse = cached_parse_tree != nullptr;
                } else if (statement_cache_mode != cache::StatementCacheMode::OFF && lexical_shape &&
                           statement_template_cache->lookup(lexical_shape.key, current_statement_template_generation)) {
                    LOG_DEBUG("statement template shadow hit digest=%016lx%016lx", lexical_shape.key.high,
                              lexical_shape.key.low);
                }
            }
        }

        // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
        auto _context = std::make_unique<Context>(lock_manager.get(), log_manager.get(), nullptr, data_send, &offset,
                                                  txn_manager.get());
        Context* context = _context.get();
        context->isolation_level_ = session_isolation_level;
        if (lexical_shape && cacheable_plan) {
            context->has_statement_template_identity_ = true;
            context->statement_shape_high_ = lexical_shape.key.high;
            context->statement_shape_low_ = lexical_shape.key.low;
            context->statement_shape_canonical_ = lexical_shape.key.canonical_bytes;
            context->statement_template_generation_ = current_statement_template_generation;
            context->planner_generation_ = current_planner_generation;
        }

        auto abort_active_transaction = [&]() {
            Transaction* txn = context->txn_;
            if (txn == nullptr && txn_id != INVALID_TXN_ID) {
                txn = txn_manager->get_transaction(txn_id);
            }
            if (txn != nullptr && txn->get_state() != TransactionState::ABORTED &&
                txn->get_state() != TransactionState::COMMITTED) {
                txn_manager->abort(txn, log_manager.get());
            }
            txn_id = INVALID_TXN_ID;
            context->txn_ = nullptr;
        };

        std::unique_ptr<ast::TreeNode> parse_tree;
        try {
            if (!used_cached_plan && cached_point_program == nullptr) {
                if (cached_parse_tree != nullptr) {
                    parse_tree = std::move(cached_parse_tree);
                } else {
                    phase_metrics::ScopedSample metrics_sample(
                        phase_metrics::Phase::PARSER, phase_metrics::sample_rate(phase_metrics::Phase::PARSER));
                    parse_tree = ast::parse_sql(data_recv);
                }
            }
        } catch (const ast::ParseError& e) {
            abort_active_transaction();
            LOG_ERROR("parse failed for SQL [%s]: %s", data_recv, e.what());
            const char* msg = e.what();
            int msg_len = strlen(msg);
            memcpy(data_send, msg, msg_len);
            data_send[msg_len] = '\n';
            data_send[msg_len + 1] = '\0';
            offset = msg_len + 1;
        }

        if (cached_point_program != nullptr || used_cached_plan || parse_tree != nullptr) {
            try {
                bool cached_program_handled = false;
                bool statement_started = false;
                if (cached_point_program != nullptr) {
                    SetTransaction(&txn_id, context);
                    statement_started = true;
                    const auto dispatch = DispatchCachedPointProgram(
                        {point_program_template_cache.get(), &lexical_shape, current_statement_template_generation,
                         current_planner_generation, sm_manager.get(), context, cached_point_program});
                    cached_program_handled = dispatch == ProgramDispatchStatus::HANDLED;
                    if (!cached_program_handled) {
                        phase_metrics::ScopedSample metrics_sample(
                            phase_metrics::Phase::PARSER, phase_metrics::sample_rate(phase_metrics::Phase::PARSER));
                        parse_tree = ast::parse_sql(data_recv);
                    }
                }
                if (!cached_program_handled) {
                if (!used_cached_plan) {
                    ast::assign_literal_slots(*parse_tree);
                }
                std::shared_ptr<const ast::TreeNode> parsed_skeleton;
                if (!used_cached_parse && lexical_shape && cacheable_skeleton &&
                    statement_cache_mode != cache::StatementCacheMode::OFF) {
                    auto clone = ast::clone_tree(*parse_tree);
                    parsed_skeleton = std::shared_ptr<const ast::TreeNode>(std::move(clone));
                    statement_template_cache->publish(lexical_shape.key, current_statement_template_generation,
                                                      parsed_skeleton);
                }
                const auto parsed_type = used_cached_plan ? *cached_statement_type : parse_tree->type;
                bool is_checkpoint = parsed_type == ast::AstType::StaticCheckpoint;
                bool is_load = parsed_type == ast::AstType::LoadStmt;
                if (!is_checkpoint && !is_load && !statement_started) {
                    SetTransaction(&txn_id, context);
                }
                // analyze and rewrite
                std::unique_ptr<Query> query;
                if (!used_cached_plan) {
                    if (cached_query != nullptr) {
                        query = std::move(cached_query);
                    } else {
                        phase_metrics::ScopedSample metrics_sample(
                            phase_metrics::Phase::ANALYZER, phase_metrics::sample_rate(phase_metrics::Phase::ANALYZER));
                        query = analyze->do_analyze(std::move(parse_tree));
                    }
                }
                if (!used_cached_query && lexical_shape && cacheable_query &&
                    statement_cache_mode != cache::StatementCacheMode::OFF && query != nullptr) {
                    auto query_copy = clone_query(*query);
                    std::shared_ptr<const Query> semantic_skeleton(std::move(query_copy));
                    statement_template_cache->publish(lexical_shape.key, current_statement_template_generation, nullptr,
                                                      std::move(semantic_skeleton));
                }
                LOG_DEBUG("Parse successful for sockfd: %d, type: %d", fd, static_cast<int>(parsed_type));
                // 优化器
                std::unique_ptr<Plan> plan;
                {
                    if (cached_plan != nullptr) {
                        plan = std::move(cached_plan);
                    } else {
                        phase_metrics::ScopedSample metrics_sample(
                            phase_metrics::Phase::PLANNER, phase_metrics::sample_rate(phase_metrics::Phase::PLANNER));
                        plan = optimizer->plan_query(std::move(query), context);
                    }
                }
                if (!used_cached_plan && lexical_shape && cacheable_plan &&
                    statement_cache_mode != cache::StatementCacheMode::OFF && plan != nullptr) {
                    auto plan_copy = clone_plan(*plan, sm_manager.get());
                    std::shared_ptr<const Plan> physical_skeleton(std::move(plan_copy));
                    statement_template_cache->publish(lexical_shape.key, current_statement_template_generation, nullptr,
                                                      nullptr, std::move(physical_skeleton));
                }
                // portal
                std::unique_ptr<PortalStmt> portalStmt = portal->start(std::move(plan), context);
                {
                    phase_metrics::ScopedSample metrics_sample(
                        phase_metrics::Phase::EXECUTOR, phase_metrics::sample_rate(phase_metrics::Phase::EXECUTOR));
                    portal->run(std::move(portalStmt), ql_manager.get(), &txn_id, context);
                }
                // Persist isolation level change (SET TRANSACTION ISOLATION LEVEL)
                session_isolation_level = context->isolation_level_;
                // Note: "set output_file on|off" is a database-global toggle stored
                // on SmManager (see execution_manager.cpp T_SetOutputFile), so it
                // persists across connections without per-session mirroring here.
                portal->drop();
                }
                if (context->txn_ != nullptr && !context->txn_->get_txn_mode() &&
                    context->txn_->get_state() != TransactionState::COMMITTED &&
                    context->txn_->get_state() != TransactionState::ABORTED) {
                    txn_manager->commit(context->txn_, context->log_mgr_);
                    txn_id = INVALID_TXN_ID;
                }
                context->txn_ = nullptr;
            } catch (TransactionAbortException& e) {
                // 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
                std::string str = "abort\n";
                memcpy(data_send, str.c_str(), str.length());
                data_send[str.length()] = '\0';
                offset = str.length();

                // 回滚事务
                abort_active_transaction();
                LOG_INFO("transaction aborted: %s", e.GetInfo().c_str());

                if (sm_manager->output_file_enabled_) {
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
                offset = e.get_msg_len() + 1;

                abort_active_transaction();

                // 将报错信息写入output.txt
                if (sm_manager->output_file_enabled_) {
                    std::fstream outfile;
                    outfile.open("output.txt", std::ios::out | std::ios::app);
                    outfile << "failure\n";
                    outfile.close();
                }
            } catch (const std::exception& e) {
                LOG_ERROR("%s", e.what());

                abort_active_transaction();

                const char* msg = e.what();
                int msg_len = strlen(msg);
                memcpy(data_send, msg, msg_len);
                data_send[msg_len] = '\n';
                data_send[msg_len + 1] = '\0';
                offset = msg_len + 1;
            }
        }
        // future TODO: 格式化 sql_handler.result, 传给客户端
        // send result with fixed format, use protobuf in the future
        if (write(fd, data_send, offset + 1) == -1) {
            break;
        }
    }

    // An abruptly closed session may still own an explicit transaction and its locks.
    if (txn_id != INVALID_TXN_ID) {
        Transaction* txn = txn_manager->get_transaction(txn_id);
        if (txn != nullptr && txn->get_state() != TransactionState::COMMITTED &&
            txn->get_state() != TransactionState::ABORTED) {
            txn_manager->abort(txn, log_manager.get());
        }
    }

    // Clear
    LOG_INFO("terminating client connection, sockfd: %d", fd);
    close(fd); // close a file descriptor.
    return;    // terminate calling thread!
}

void start_server() {
    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in{};

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
        struct sockaddr_in s_addr_client{};
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
        std::thread(client_handler, sockfd).detach();
    }

    // Clear
    LOG_INFO("try to close all client connections");
#ifdef RMDB_ENABLE_JIT
    client_connections.stop_and_drain();
#endif
    int ret = shutdown(sockfd_server, SHUT_WR); // shut down the all or part of a full-duplex connection.
    if (ret == -1) {
        LOG_ERROR("shutdown server socket failed: %s", strerror(errno));
    }
    //    assert(ret != -1);
    LOG_INFO("server shuts down");
}

int main(int argc, char** argv) {
    const char* configured_log_path = std::getenv("RMDB_LOG_PATH");
    minilog::Logger::get().init(configured_log_path == nullptr ? "rmdb.log" : configured_log_path);
    minilog::Logger::get().set_level(minilog::LogLevel::WARN);

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
        if (!sm_manager->is_dir(db_name)) {
            // Database not found, create a new one
            sm_manager->create_db(db_name);
            LOG_INFO("database created: %s", db_name.c_str());
        }
        // Open database
        sm_manager->open_db(db_name);
        LOG_INFO("database opened: %s", db_name.c_str());

#ifdef RMDB_ENABLE_JIT
        jit::initialize_predicate_jit([] { return sm_manager->get_catalog_generation(); });
#endif

        log_manager->initialize_from_existing_log();
        buffer_pool_manager->set_log_manager(log_manager.get());

        // recovery database
        recovery->analyze();
        recovery->redo();
        recovery->undo();
        sm_manager->refresh_index_residency();
        LOG_INFO("database recovery finished");

        {
            std::atomic<bool> checkpoint_thread_stop{false};
            std::atomic<bool> metrics_thread_stop{false};
            std::thread metrics_thread;
            const char* metrics_path = std::getenv(rmdb_config::kPhaseMetricsPathEnv);
            if (metrics_path != nullptr) {
                const std::string path(metrics_path);
                auto* template_cache = statement_template_cache.get();
                auto* program_cache = point_program_template_cache.get();
                metrics_thread = std::thread([&metrics_thread_stop, path, template_cache, program_cache] {
                    while (!metrics_thread_stop.load()) {
                        if (std::remove((path + ".reset").c_str()) == 0) {
                            phase_metrics::Registry::instance().reset();
                        }
                        write_phase_metrics(path, template_cache, program_cache);
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                    }
                    if (std::remove((path + ".reset").c_str()) == 0) {
                        phase_metrics::Registry::instance().reset();
                    }
                    write_phase_metrics(path, template_cache, program_cache);
                });
            }
            std::thread checkpoint_thread([&checkpoint_thread_stop] {
                CheckpointManager checkpoint_mgr(txn_manager.get(), sm_manager.get(), log_manager.get());
                CheckpointOptions checkpoint_options;
                bool has_checkpoint_override = false;
                auto read_positive_int64 = [&](const char* name, int64_t* target) {
                    const char* value = std::getenv(name);
                    if (value == nullptr) {
                        return;
                    }
                    try {
                        const auto parsed = std::stoll(value);
                        if (parsed > 0) {
                            *target = parsed;
                            has_checkpoint_override = true;
                        }
                    } catch (const std::exception&) {
                        // Keep the default for malformed diagnostic overrides.
                    }
                };
                auto read_positive_size = [&](const char* name, size_t* target) {
                    const char* value = std::getenv(name);
                    if (value == nullptr) {
                        return;
                    }
                    try {
                        const auto parsed = std::stoull(value);
                        if (parsed > 0) {
                            *target = static_cast<size_t>(parsed);
                            has_checkpoint_override = true;
                        }
                    } catch (const std::exception&) {
                        // Keep the default for malformed diagnostic overrides.
                    }
                };
                read_positive_int64("RMDB_AUTO_CHECKPOINT_BYTES", &checkpoint_options.auto_checkpoint_bytes);
                read_positive_int64("RMDB_CHECKPOINT_PREFLUSH_BYTES", &checkpoint_options.preflush_trigger_bytes);
                read_positive_size("RMDB_CHECKPOINT_PREFLUSH_PAGES", &checkpoint_options.preflush_batch_pages);
                if (has_checkpoint_override) {
                    checkpoint_mgr.SetOptions(checkpoint_options);
                }
                while (!checkpoint_thread_stop.load()) {
                    std::this_thread::sleep_for(std::chrono::seconds(2));
                    if (checkpoint_thread_stop.load()) {
                        break;
                    }
                    checkpoint_mgr.RunIfNeeded();
                }
            });

            // 开启服务端，开始接受客户端连接
            start_server();

            checkpoint_thread_stop.store(true);
            metrics_thread_stop.store(true);
            if (checkpoint_thread.joinable()) {
                checkpoint_thread.join();
            }
            if (metrics_thread.joinable()) {
                metrics_thread.join();
            }
        }

#ifdef RMDB_ENABLE_JIT
        jit::shutdown_predicate_jit();
#endif
        sm_manager->close_db();
        LOG_INFO("database has been closed");
    } catch (RMDBError& e) {
        LOG_ERROR("RMDB error: %s", e.what());
        minilog::Logger::get().stop();
        exit(1);
    }
    minilog::Logger::get().stop();
    return 0;
}
