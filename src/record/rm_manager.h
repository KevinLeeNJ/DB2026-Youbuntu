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

#include <assert.h>

#include "bitmap.h"
#include "rm_defs.h"
#include "rm_file_handle.h"

/* 记录管理器，用于管理表的数据文件，进行文件的创建、打开、删除、关闭 */
class RmManager {
private:
    DiskManager* disk_manager_;
    BufferPoolManager* buffer_pool_manager_;

public:
    RmManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager) {}

    /**
     * @description: 创建表的数据文件并初始化相关信息
     * @param {string&} filename 要创建的文件名称
     * @param {int} record_size 表中记录的大小
     */
    void create_file(const std::string& filename, int record_size) {
        if (record_size < 1 || record_size > RM_MAX_RECORD_SIZE) {
            throw InvalidRecordSizeError(record_size);
        }
        disk_manager_->create_file(filename);
        int fd = disk_manager_->open_file(filename);

        // 初始化file header
        RmFileHdr file_hdr{};
        file_hdr.record_size = record_size;
        file_hdr.num_pages = 1;
        file_hdr.first_free_page_no = RM_NO_PAGE;
        // 页面容量的闭式解见 rm_defs.h 的 rm_num_records_per_page（那里也带着
        // "每页至少一条"的 static_assert，避免两处公式各写一遍而漂移）。
        static_assert(BITMAP_WIDTH == 8, "rm_num_records_per_page assumes 8 bits per bitmap byte");
        file_hdr.num_records_per_page = rm_num_records_per_page(record_size);
        file_hdr.bitmap_size = (file_hdr.num_records_per_page + BITMAP_WIDTH - 1) / BITMAP_WIDTH;

        // 将file header写入磁盘文件（名为file name，文件描述符为fd）中的第0页
        // head page直接写入磁盘，没有经过缓冲区的NewPage，那么也就不需要FlushPage
        disk_manager_->write_page(fd, RM_FILE_HDR_PAGE, (char*)&file_hdr, sizeof(file_hdr));
        disk_manager_->close_file(fd);
    }

    /**
     * @description: 删除表的数据文件
     * @param {string&} filename 要删除的文件名称
     */
    void destroy_file(const std::string& filename) {
        disk_manager_->destroy_file(filename);
    }

    // 注意这里打开文件，创建并返回了record file handle的指针
    /**
     * @description: 打开表的数据文件，并返回文件句柄
     * @param {string&} filename 要打开的文件名称
     * @return {unique_ptr<RmFileHandle>} 文件句柄的指针
     */
    std::unique_ptr<RmFileHandle> open_file(const std::string& filename) {
        int fd = disk_manager_->open_file(filename);
        return std::make_unique<RmFileHandle>(disk_manager_, buffer_pool_manager_, fd);
    }
    /**
     * @description: 关闭表的数据文件
     * @param {RmFileHandle*} file_handle 要关闭文件的句柄
     */
    void close_file(const RmFileHandle* file_handle) {
        // Publish data pages before the header that can make newly allocated
        // pages reachable. On failure keep both the handle and resident dirty
        // pages intact so the caller can fail closed or retry.
        if (!buffer_pool_manager_->flush_all_pages(file_handle->fd_)) {
            throw InternalError("failed to flush record pages while closing file");
        }
        disk_manager_->write_page(file_handle->fd_, RM_FILE_HDR_PAGE, (char*)&file_handle->file_hdr_,
                                  sizeof(file_handle->file_hdr_));
        buffer_pool_manager_->delete_all_pages(file_handle->fd_);
        disk_manager_->close_file(file_handle->fd_);
    }

    void flush_file_header(const RmFileHandle* file_handle) {
        disk_manager_->write_page(file_handle->fd_, RM_FILE_HDR_PAGE, (char*)&file_handle->file_hdr_,
                                  sizeof(file_handle->file_hdr_));
    }

    // Write a header captured at a fixed checkpoint cut without consulting the
    // live file handle. The catalog guard held by the caller keeps fd open.
    void write_detached_file_header(const RmFileHeaderSnapshot& snapshot) {
        if (snapshot.fd < 0) {
            throw InternalError("invalid detached record header snapshot");
        }
        disk_manager_->write_page(snapshot.fd, RM_FILE_HDR_PAGE, snapshot.bytes.data(),
                                  static_cast<int>(snapshot.bytes.size()));
    }
};
