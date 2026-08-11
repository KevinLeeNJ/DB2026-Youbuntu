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

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include "common/config.h"
#include "errors.h"

/**
 * @description: DiskManager的作用主要是根据上层的需要对磁盘文件进行操作
 */
class DiskManager {
public:
    explicit DiskManager();

    ~DiskManager() = default;

    void write_page(int fd, page_id_t page_no, const char* offset, int num_bytes);

    void read_page(int fd, page_id_t page_no, char* offset, int num_bytes);

    page_id_t allocate_page(int fd);

    void deallocate_page(page_id_t page_id);

    /*目录操作*/
    bool is_dir(const std::string& path);

    void create_dir(const std::string& path);

    void destroy_dir(const std::string& path);

    /*文件操作*/
    bool is_file(const std::string& path);

    void create_file(const std::string& path);

    void destroy_file(const std::string& path);

    int open_file(const std::string& path);

    void close_file(int fd);

    int64_t get_file_size(const std::string& file_name);

    std::string get_file_name(int fd);

    int get_file_fd(const std::string& file_name);

    /*日志操作*/
    int read_log(char* log_data, int size, int64_t offset);

    // Bulk WAL read for the streaming recovery scan. Unlike read_log it does
    // not stat the file per call and does not touch the append offset: the
    // caller owns the scan bound. Returns the number of bytes read, which is
    // short only at end of file.
    int read_log_chunk(char* log_data, int size, int64_t offset);

    void write_log(char* log_data, int size);

    void fsync_log();

    // Flush a data or metadata file so a checkpoint can establish a durable
    // WAL -> data -> metadata ordering.
    void sync_file(int fd);
    void sync_path(const std::string& path);
    void sync_directory(const std::string& path);

    // 将日志文件截断为空，并把追加偏移归零。recovery/checkpoint 完成后调用，
    // 消除已处理完毕的 loser 日志，避免跨轮 recovery 重复 undo 同 RID 上的数据。
    void truncate_log();
    // Drop an incomplete WAL tail while preserving the complete prefix.
    void truncate_log_to(int64_t offset);

    void SetLogOffset(int64_t log_offset) {
        log_offset_ = log_offset;
    }

    void SetLogFd(int log_fd) {
        log_fd_ = log_fd;
    }

    int GetLogFd() {
        return log_fd_;
    }

    /**
     * @description: 设置文件已经分配的页面个数
     * @param {int} fd 文件对应的文件句柄
     * @param {int} start_page_no 已经分配的页面个数，即文件接下来从start_page_no开始分配页面编号
     */
    void set_fd2pageno(int fd, int start_page_no) {
        fd2pageno_[fd] = start_page_no;
    }

    uint64_t get_log_read_count() const {
        return log_read_count_.load(std::memory_order_relaxed);
    }
    uint64_t get_log_read_bytes() const {
        return log_read_bytes_.load(std::memory_order_relaxed);
    }

    /**
     * @description: 获得文件目前已分配的页面个数，即如果文件要分配一个新页面，需要从fd2pagenp_[fd]开始分配
     * @return {page_id_t} 已分配的页面个数
     * @param {int} fd 文件对应的句柄
     */
    page_id_t get_fd2pageno(int fd) {
        return fd2pageno_[fd];
    }

    static constexpr int MAX_FD = 8192;

private:
    // Opens the WAL lazily. The append offset is initialized here, together
    // with the descriptor, so that "log_fd_ != -1" always implies "log_offset_
    // is the real append offset". read_log used to reassign log_offset_ on
    // every call, which left correctness depending on the order in which
    // readers and writers happened to run.
    void open_log_fd();

    // 文件打开列表，用于记录文件是否被打开
    std::unordered_map<std::string, int> path2fd_; //<Page文件磁盘路径,Page fd>哈希表
    std::unordered_map<int, std::string> fd2path_; //<Page fd,Page文件磁盘路径>哈希表

    int log_fd_ = -1;        // WAL日志文件的文件句柄，默认为-1，代表未打开日志文件
    int64_t log_offset_ = 0; // 日志文件追加偏移
    std::atomic<uint64_t> log_read_count_{0};
    std::atomic<uint64_t> log_read_bytes_{0};
    std::atomic<page_id_t> fd2pageno_[MAX_FD]{}; // 文件中已经分配的页面个数，初始值为0
};
