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

#pragma once

#include <atomic>
#include <csignal>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

struct DatabaseInstance;

class ServerRuntime {
public:
    ServerRuntime(DatabaseInstance& database, volatile sig_atomic_t& should_exit)
        : database_(database), should_exit_(should_exit) {}
    ~ServerRuntime();

    void run(std::uint16_t port);

private:
    struct Worker {
        Worker(int socket_fd, std::uint64_t worker_id) : fd(socket_fd), id(worker_id) {}

        int fd;
        std::uint64_t id;
        std::thread thread;
        bool complete{false};
    };

    void client_handler(int fd, std::uint64_t worker_id);
    void finish_client(int fd, std::uint64_t worker_id);
    void reap_completed_workers();
    void start_checkpoint();
    void stop();

    DatabaseInstance& database_;
    volatile sig_atomic_t& should_exit_;
    int listener_{-1};
    std::mutex clients_mutex_;
    std::vector<int> clients_;
    std::vector<Worker> workers_;
    std::uint64_t next_worker_id_{0};
    std::atomic<bool> checkpoint_stop_{false};
    std::mutex checkpoint_mutex_;
    std::condition_variable checkpoint_cv_;
    std::thread checkpoint_thread_;
};
