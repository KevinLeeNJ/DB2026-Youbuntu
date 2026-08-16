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

#include "server/server_runtime.h"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

#include "errors.h"
#include "minilog.h"
#include "recovery/checkpoint_manager.h"
#include "server/database_instance.h"
#include "server/wire_session.h"

#define MAX_CONN_LIMIT 128

ServerRuntime::~ServerRuntime() {
    stop();
}

void ServerRuntime::client_handler(int fd, std::uint64_t worker_id) {
    serve_wire_session(database_, fd);
    finish_client(fd, worker_id);
    ::close(fd);
}

void ServerRuntime::start_checkpoint() {
    checkpoint_stop_.store(false, std::memory_order_release);
    checkpoint_thread_ = std::thread([this] {
        if (database_.is_delta()) {
            auto retry_delay = std::chrono::milliseconds(100);
            while (!checkpoint_stop_.load(std::memory_order_acquire)) {
                std::unique_lock<std::mutex> wait_lock(checkpoint_mutex_);
                checkpoint_cv_.wait_for(wait_lock, std::chrono::seconds(1),
                                        [this] { return checkpoint_stop_.load(std::memory_order_acquire); });
                if (checkpoint_stop_.load(std::memory_order_acquire))
                    break;
                wait_lock.unlock();
                try {
                    database_.delta_database->MaintenanceTick();
                    retry_delay = std::chrono::milliseconds(100);
                } catch (const std::exception& error) {
                    LOG_WARN("delta maintenance tick failed: %s", error.what());
                    std::unique_lock<std::mutex> retry_lock(checkpoint_mutex_);
                    checkpoint_cv_.wait_for(retry_lock, retry_delay,
                                            [this] { return checkpoint_stop_.load(std::memory_order_acquire); });
                    retry_delay = std::min(retry_delay * 2, std::chrono::milliseconds(60000));
                } catch (...) {
                    LOG_WARN("delta maintenance tick failed with unknown exception");
                    std::unique_lock<std::mutex> retry_lock(checkpoint_mutex_);
                    checkpoint_cv_.wait_for(retry_lock, retry_delay,
                                            [this] { return checkpoint_stop_.load(std::memory_order_acquire); });
                    retry_delay = std::min(retry_delay * 2, std::chrono::milliseconds(60000));
                }
            }
            database_.delta_database->StopMaintenance();
            return;
        }
        auto& legacy = database_.legacy();
        CheckpointManager checkpoint_mgr(&legacy.txn_manager, &legacy.sm_manager, &legacy.log_manager);
        while (!checkpoint_stop_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (!checkpoint_stop_.load(std::memory_order_acquire)) {
                checkpoint_mgr.Tick();
            }
        }
    });
}

void ServerRuntime::finish_client(int fd, std::uint64_t worker_id) {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    auto it = std::find(clients_.begin(), clients_.end(), fd);
    if (it != clients_.end()) {
        clients_.erase(it);
    }
    auto worker = std::find_if(workers_.begin(), workers_.end(),
                               [worker_id](const Worker& item) { return item.id == worker_id; });
    if (worker != workers_.end()) {
        worker->complete = true;
    }
}

void ServerRuntime::reap_completed_workers() {
    std::vector<std::thread> completed;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        for (auto it = workers_.begin(); it != workers_.end();) {
            if (!it->complete) {
                ++it;
                continue;
            }
            completed.push_back(std::move(it->thread));
            it = workers_.erase(it);
        }
    }
    for (auto& worker : completed) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void ServerRuntime::run(std::uint16_t port) {
    struct sockaddr_in s_addr_in {};

    listener_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listener_ == -1) {
        throw RMDBError("socket failed: " + std::string(strerror(errno)));
    }
    int val = 1;
    setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // before bind(), set the attr of structure sockaddr.
    memset(&s_addr_in, 0, sizeof(s_addr_in));
    s_addr_in.sin_family = AF_INET;
    s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
    s_addr_in.sin_port = htons(port);
    if (bind(listener_, reinterpret_cast<struct sockaddr*>(&s_addr_in), sizeof(s_addr_in)) == -1) {
        throw RMDBError("bind failed: " + std::string(strerror(errno)));
    }

    if (listen(listener_, MAX_CONN_LIMIT) == -1) {
        throw RMDBError("listen failed: " + std::string(strerror(errno)));
    }
    start_checkpoint();

    while (!should_exit_) {
        reap_completed_workers();
        pollfd ready{listener_, POLLIN, 0};
        const int poll_result = poll(&ready, 1, 100);
        if (poll_result == 0 || (poll_result == -1 && errno == EINTR)) {
            continue;
        }
        if (poll_result < 0) {
            LOG_WARN("listener poll failed: %s", strerror(errno));
            break;
        }
        struct sockaddr_in s_addr_client {};
        socklen_t client_length = sizeof(s_addr_client);
        int sockfd = accept(listener_, reinterpret_cast<struct sockaddr*>(&s_addr_client), &client_length);
        if (sockfd == -1) {
            if (errno != EINTR) {
                LOG_WARN("accept failed: %s", strerror(errno));
            }
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            try {
                clients_.push_back(sockfd);
                const std::uint64_t worker_id = ++next_worker_id_;
                workers_.emplace_back(sockfd, worker_id);
                workers_.back().thread = std::thread(&ServerRuntime::client_handler, this, sockfd, worker_id);
            } catch (...) {
                if (!workers_.empty() && workers_.back().fd == sockfd && !workers_.back().thread.joinable()) {
                    workers_.pop_back();
                }
                clients_.erase(std::remove(clients_.begin(), clients_.end(), sockfd), clients_.end());
                ::close(sockfd);
                throw;
            }
        }
    }
    stop();
}

void ServerRuntime::stop() {
    if (database_.is_delta())
        database_.delta_database->StopMaintenance();
    if (listener_ != -1) {
        ::close(listener_);
        listener_ = -1;
    }
    std::vector<int> clients;
    std::vector<Worker> workers;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients = clients_;
        workers.swap(workers_);
    }
    for (int fd : clients) {
        shutdown(fd, SHUT_RDWR);
    }
    for (auto& worker : workers) {
        if (worker.thread.joinable()) {
            worker.thread.join();
        }
    }
    checkpoint_stop_.store(true, std::memory_order_release);
    checkpoint_cv_.notify_all();
    if (checkpoint_thread_.joinable()) {
        checkpoint_thread_.join();
    }
}
