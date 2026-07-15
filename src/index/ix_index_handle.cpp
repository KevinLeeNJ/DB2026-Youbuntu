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

#include "ix_index_handle.h"

#include "ix_scan.h"

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char* target) const {
    int left = 0;
    int right = page_hdr->num_key;
    while (left < right) {
        int mid = left + (right - left) / 2;
        if (ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) < 0) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char* target) const {
    int left = 0;
    int right = page_hdr->num_key;
    while (left < right) {
        int mid = left + (right - left) / 2;
        if (ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) <= 0) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::leaf_lookup(const char* key, Rid** value) {
    int pos = lower_bound(key);
    if (pos < get_size() && ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        *value = get_rid(pos);
        return true;
    }
    return false;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::internal_lookup(const char* key) {
    int child_idx = upper_bound(key) - 1;
    if (child_idx < 0) {
        child_idx = 0;
    }
    return value_at(child_idx);
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, const char* key, const Rid* rid, int n) {
    assert(pos >= 0 && pos <= page_hdr->num_key);
    assert(n >= 0 && page_hdr->num_key + n <= get_max_size());
    if (n == 0) {
        return;
    }
    int move_count = page_hdr->num_key - pos;
    memmove(get_key(pos + n), get_key(pos), move_count * file_hdr->col_tot_len_);
    memmove(get_rid(pos + n), get_rid(pos), move_count * sizeof(Rid));
    memcpy(get_key(pos), key, n * file_hdr->col_tot_len_);
    memcpy(get_rid(pos), rid, n * sizeof(Rid));
    page_hdr->num_key += n;
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxNodeHandle::insert(const char* key, const Rid& value) {
    int pos = lower_bound(key);
    if (pos < get_size() && ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        return get_size();
    }
    insert_pair(pos, key, value);
    return get_size();
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    assert(pos >= 0 && pos < page_hdr->num_key);
    int move_count = page_hdr->num_key - pos - 1;
    memmove(get_key(pos), get_key(pos + 1), move_count * file_hdr->col_tot_len_);
    memmove(get_rid(pos), get_rid(pos + 1), move_count * sizeof(Rid));
    page_hdr->num_key--;
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 */
int IxNodeHandle::remove(const char* key) {
    int pos = lower_bound(key);
    if (pos < get_size() && ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        erase_pair(pos);
    }
    return get_size();
}

IxIndexHandle::IxIndexHandle(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    std::vector<char> buf(PAGE_SIZE, 0);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf.data(), PAGE_SIZE);
    file_hdr_ = std::make_unique<IxFileHdr>();
    file_hdr_->deserialize(buf.data());

    // fd2pageno_ is process-local state and is reset when the database is
    // reopened. Reconstruct the allocation cursor from the persisted index
    // header instead of incrementing an unrelated value left on this fd.
    disk_manager_->set_fd2pageno(fd, file_hdr_->num_pages_);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 */
std::pair<IxNodeHandle*, bool> IxIndexHandle::find_leaf_page(const char* key, Operation operation,
                                                             Transaction* transaction, bool find_first) {
    (void)operation;
    (void)transaction;
    IxNodeHandle node;
    fetch_node_into(file_hdr_->root_page_, node);
    while (!node.is_leaf_page()) {
        page_id_t child_page_no = find_first ? node.value_at(0) : node.internal_lookup(key);
        buffer_pool_manager_->unpin_page(node.get_page_id(), false);
        fetch_node_into(child_page_no, node);
    }
    // Caller owns the returned leaf (must unpin + delete).
    IxNodeHandle* leaf = new IxNodeHandle(node.file_hdr, node.page);
    return std::make_pair(leaf, false);
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::get_value(const char* key, std::vector<Rid>* result, Transaction* transaction) {
    (void)transaction;
    auto guard = lock_shared();
    Iid lower = lower_bound(key);
    Iid upper = upper_bound(key);
    IxScan scan(this, lower, upper, buffer_pool_manager_, false);
    while (!scan.is_end()) {
        result->push_back(scan.rid());
        scan.next();
    }
    return !result->empty();
}

/**
 * @brief  将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note need to unpin the new node outside
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle* IxIndexHandle::split(IxNodeHandle* node) {
    IxNodeHandle* new_node = create_node();
    memcpy(new_node->page_hdr, node->page_hdr, sizeof(IxPageHdr));
    new_node->set_parent_page_no(node->get_parent_page_no());

    int old_size = node->get_size();
    int left_size = old_size / 2;
    int right_size = old_size - left_size;
    new_node->set_size(0);
    new_node->insert_pairs(0, node->get_key(left_size), node->get_rid(left_size), right_size);
    node->set_size(left_size);

    if (node->is_leaf_page()) {
        new_node->set_prev_leaf(node->get_page_no());
        new_node->set_next_leaf(node->get_next_leaf());
        if (node->get_next_leaf() != IX_LEAF_HEADER_PAGE) {
            IxNodeHandle next_leaf;
            fetch_node_into(node->get_next_leaf(), next_leaf);
            next_leaf.set_prev_leaf(new_node->get_page_no());
            buffer_pool_manager_->unpin_page(next_leaf.get_page_id(), true);
        }
        node->set_next_leaf(new_node->get_page_no());
    } else {
        for (int i = 0; i < new_node->get_size(); ++i) {
            maintain_child(new_node, i);
        }
    }

    return new_node;
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 */
void IxIndexHandle::insert_into_parent(IxNodeHandle* old_node, const char* key, IxNodeHandle* new_node,
                                       Transaction* transaction) {
    if (old_node->is_root_page()) {
        IxNodeHandle* new_root = create_node();
        new_root->page_hdr->is_leaf = false;
        new_root->set_size(0);
        new_root->set_parent_page_no(IX_NO_PAGE);
        new_root->insert_pair(0, old_node->get_key(0), Rid{old_node->get_page_no(), -1});
        new_root->insert_pair(1, new_node->get_key(0), Rid{new_node->get_page_no(), -1});

        old_node->set_parent_page_no(new_root->get_page_no());
        new_node->set_parent_page_no(new_root->get_page_no());
        update_root_page_no(new_root->get_page_no());
        buffer_pool_manager_->unpin_page(new_root->get_page_id(), true);
        delete new_root;
        return;
    }

    IxNodeHandle* parent = fetch_node(old_node->get_parent_page_no());
    int insert_pos = parent->find_child(old_node) + 1;
    parent->insert_pair(insert_pos, key, Rid{new_node->get_page_no(), -1});
    new_node->set_parent_page_no(parent->get_page_no());

    if (parent->get_size() >= parent->get_max_size()) {
        IxNodeHandle* new_parent = split(parent);
        insert_into_parent(parent, new_parent->get_key(0), new_parent, transaction);
        buffer_pool_manager_->unpin_page(new_parent->get_page_id(), true);
        delete new_parent;
    }
    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
    delete parent;
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
 */
page_id_t IxIndexHandle::insert_entry(const char* key, const Rid& value, Transaction* transaction,
                                      bool allow_duplicate) {
    // The common case only changes one existing leaf entry. Structure-shared
    // protects the root-to-leaf route while the leaf latch serializes writers
    // that target the same page. Splits, separator changes, and inserts at
    // position zero fall back to the stable tree-exclusive implementation.
    {
        auto structure_guard = lock_shared();
        IxNodeHandle leaf;
        fetch_node_into(file_hdr_->root_page_, leaf);
        while (!leaf.is_leaf_page()) {
            page_id_t child_page_no = leaf.internal_lookup(key);
            buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
            fetch_node_into(child_page_no, leaf);
        }

        std::unique_lock<std::shared_mutex> leaf_guard(leaf.page->latch());
        int pos = leaf.lower_bound(key);
        bool duplicate_key = pos < leaf.get_size() &&
                             ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0;
        if (duplicate_key && !allow_duplicate) {
            leaf_guard.unlock();
            buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
            throw IndexEntryExistsError();
        }
        if (allow_duplicate) {
            while (pos < leaf.get_size() &&
                   ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0) {
                ++pos;
            }
        }

        const bool needs_structure_change = pos == 0 || leaf.get_size() + 1 >= leaf.get_max_size();
        if (!needs_structure_change) {
            const page_id_t inserted_page_no = leaf.get_page_no();
            leaf.insert_pair(pos, key, value);
            leaf_guard.unlock();
            buffer_pool_manager_->unpin_page(leaf.get_page_id(), true);
            return inserted_page_no;
        }

        leaf_guard.unlock();
        buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
    }

    auto structure_guard = lock_exclusive();
    return insert_entry_unlocked(key, value, transaction, allow_duplicate);
}

page_id_t IxIndexHandle::insert_entry_unlocked(const char* key, const Rid& value, Transaction* transaction,
                                               bool allow_duplicate) {
    IxNodeHandle leaf;
    fetch_node_into(file_hdr_->root_page_, leaf);
    while (!leaf.is_leaf_page()) {
        page_id_t child_page_no = leaf.internal_lookup(key);
        buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
        fetch_node_into(child_page_no, leaf);
    }

    int pos = leaf.lower_bound(key);
    bool duplicate_key =
        pos < leaf.get_size() && ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0;
    if (duplicate_key && !allow_duplicate) {
        buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
        throw IndexEntryExistsError();
    }
    if (allow_duplicate) {
        while (pos < leaf.get_size() &&
               ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0) {
            ++pos;
        }
    }

    leaf.insert_pair(pos, key, value);
    page_id_t inserted_page_no = leaf.get_page_no();
    if (leaf.get_size() >= leaf.get_max_size()) {
        IxNodeHandle* new_leaf = split(&leaf);
        if (file_hdr_->last_leaf_ == leaf.get_page_no()) {
            file_hdr_->last_leaf_ = new_leaf->get_page_no();
        }
        insert_into_parent(&leaf, new_leaf->get_key(0), new_leaf, transaction);
        buffer_pool_manager_->unpin_page(new_leaf->get_page_id(), true);
        delete new_leaf;
    }

    if (pos == 0) {
        maintain_parent(&leaf);
    }
    buffer_pool_manager_->unpin_page(leaf.get_page_id(), true);
    return inserted_page_no;
}

IxIndexHandle::PinnedInserter::PinnedInserter(IxIndexHandle* h) : ih(h), latch(h->lock_exclusive()) {
    ih->fetch_node_into(ih->file_hdr_->root_page_, leaf);
    while (!leaf.is_leaf_page()) {
        page_id_t child_page_no = leaf.value_at(0);
        ih->buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
        ih->fetch_node_into(child_page_no, leaf);
    }
    active = true;
}

IxIndexHandle::PinnedInserter::~PinnedInserter() {
    if (active) {
        ih->buffer_pool_manager_->unpin_page(leaf.get_page_id(), true);
    }
}

void IxIndexHandle::PinnedInserter::insert(const char* key, const Rid& value, Transaction* txn, bool allow_duplicate) {
    // Skip root→leaf walk if key belongs in the pinned leaf (ascending bulk load).
    if (leaf.get_size() > 0) {
        int cmp_first = ix_compare(key, leaf.get_key(0), ih->file_hdr_->col_types_, ih->file_hdr_->col_lens_);
        int cmp_last =
            ix_compare(key, leaf.get_key(leaf.get_size() - 1), ih->file_hdr_->col_types_, ih->file_hdr_->col_lens_);
        bool before_leaf = cmp_first < 0;
        bool after_leaf = cmp_last > 0 && leaf.get_page_no() != ih->file_hdr_->last_leaf_;
        if (before_leaf || after_leaf) {
            // Key out of range — rewalk.
            ih->buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
            ih->fetch_node_into(ih->file_hdr_->root_page_, leaf);
            while (!leaf.is_leaf_page()) {
                page_id_t child_page_no = leaf.internal_lookup(key);
                ih->buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
                ih->fetch_node_into(child_page_no, leaf);
            }
        }
    }

    int pos = leaf.lower_bound(key);
    bool duplicate_key = pos < leaf.get_size() &&
                         ix_compare(leaf.get_key(pos), key, ih->file_hdr_->col_types_, ih->file_hdr_->col_lens_) == 0;
    if (duplicate_key && !allow_duplicate) {
        throw IndexEntryExistsError();
    }
    if (allow_duplicate) {
        while (pos < leaf.get_size() &&
               ix_compare(leaf.get_key(pos), key, ih->file_hdr_->col_types_, ih->file_hdr_->col_lens_) == 0) {
            ++pos;
        }
    }

    leaf.insert_pair(pos, key, value);

    if (leaf.get_size() >= leaf.get_max_size()) {
        IxNodeHandle* new_leaf = ih->split(&leaf);
        if (ih->file_hdr_->last_leaf_ == leaf.get_page_no()) {
            ih->file_hdr_->last_leaf_ = new_leaf->get_page_no();
        }
        ih->insert_into_parent(&leaf, new_leaf->get_key(0), new_leaf, txn);
        page_id_t new_leaf_page_no = new_leaf->get_page_no();
        ih->buffer_pool_manager_->unpin_page(new_leaf->get_page_id(), true);
        delete new_leaf;

        // Reposition to the right leaf after split.
        ih->buffer_pool_manager_->unpin_page(leaf.get_page_id(), true);
        ih->fetch_node_into(new_leaf_page_no, leaf);
    }

    if (pos == 0) {
        ih->maintain_parent(&leaf);
    }
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 */
bool IxIndexHandle::delete_entry(const char* key, Transaction* transaction) {
    {
        auto structure_guard = lock_shared();
        IxNodeHandle leaf;
        fetch_node_into(file_hdr_->root_page_, leaf);
        while (!leaf.is_leaf_page()) {
            page_id_t child_page_no = leaf.internal_lookup(key);
            buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
            fetch_node_into(child_page_no, leaf);
        }

        std::unique_lock<std::shared_mutex> leaf_guard(leaf.page->latch());
        int pos = leaf.lower_bound(key);
        if (pos >= leaf.get_size() ||
            ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) != 0) {
            leaf_guard.unlock();
            buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
            return false;
        }

        const bool needs_structure_change = pos == 0 || leaf.get_size() - 1 < leaf.get_min_size();
        if (!needs_structure_change) {
            leaf.erase_pair(pos);
            leaf_guard.unlock();
            buffer_pool_manager_->unpin_page(leaf.get_page_id(), true);
            return true;
        }

        leaf_guard.unlock();
        buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
    }

    auto structure_guard = lock_exclusive();
    return delete_entry_unlocked(key, transaction);
}

bool IxIndexHandle::delete_entry_unlocked(const char* key, Transaction* transaction) {
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::DELETE, transaction);
    int old_size = leaf->get_size();
    int pos = leaf->lower_bound(key);
    if (pos >= old_size || ix_compare(leaf->get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) != 0) {
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
        delete leaf;
        return false;
    }

    leaf->erase_pair(pos);
    if (leaf->get_size() > 0) {
        maintain_parent(leaf);
    }
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);
    delete leaf;
    return true;
}

bool IxIndexHandle::delete_entry(const char* key, const Rid& value, Transaction* transaction) {
    bool needs_structure_fallback = false;
    {
        auto structure_guard = lock_shared();
        IxNodeHandle leaf;
        fetch_node_into(file_hdr_->root_page_, leaf);
        while (!leaf.is_leaf_page()) {
            int child_idx = leaf.lower_bound(key);
            if (child_idx >= leaf.get_size()) {
                child_idx = leaf.get_size() - 1;
            } else if (child_idx > 0 &&
                       ix_compare(leaf.get_key(child_idx), key, file_hdr_->col_types_, file_hdr_->col_lens_) > 0) {
                --child_idx;
            }
            page_id_t child_page_no = leaf.value_at(child_idx);
            buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
            fetch_node_into(child_page_no, leaf);
        }

        for (;;) {
            std::unique_lock<std::shared_mutex> leaf_guard(leaf.page->latch());
            int pos = leaf.lower_bound(key);
            bool stop_at_leaf = false;
            while (pos < leaf.get_size()) {
                int cmp = ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_);
                if (cmp > 0) {
                    stop_at_leaf = true;
                    break;
                }
                if (*leaf.get_rid(pos) == value) {
                    needs_structure_fallback = pos == 0 || leaf.get_size() - 1 < leaf.get_min_size();
                    if (!needs_structure_fallback) {
                        leaf.erase_pair(pos);
                        leaf_guard.unlock();
                        buffer_pool_manager_->unpin_page(leaf.get_page_id(), true);
                        return true;
                    }
                    break;
                }
                ++pos;
            }

            const bool found_target =
                needs_structure_fallback ||
                (pos < leaf.get_size() && *leaf.get_rid(pos) == value &&
                 ix_compare(leaf.get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0);
            const bool at_last_leaf = leaf.get_page_no() == file_hdr_->last_leaf_;
            const page_id_t next_leaf = leaf.get_next_leaf();
            leaf_guard.unlock();
            buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
            if (found_target || stop_at_leaf || at_last_leaf) {
                break;
            }
            fetch_node_into(next_leaf, leaf);
        }
    }

    if (!needs_structure_fallback) {
        return false;
    }
    auto guard = lock_exclusive();
    return delete_entry_unlocked(key, value, transaction);
}

bool IxIndexHandle::delete_entry_unlocked(const char* key, const Rid& value, Transaction* transaction) {
    (void)transaction;
    Iid lower = lower_bound(key);
    Iid upper = upper_bound(key);
    IxScan scan(this, lower, upper, buffer_pool_manager_, false);
    while (!scan.is_end()) {
        Iid iid = scan.iid();
        if (scan.rid() == value) {
            IxNodeHandle* leaf = fetch_node(iid.page_no);
            leaf->erase_pair(iid.slot_no);
            if (leaf->get_size() > 0) {
                maintain_parent(leaf);
            }
            buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);
            delete leaf;
            return true;
        }
        scan.next();
    }
    return false;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then redistribute.
 * Otherwise, merge(Coalesce).
 */
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle* node, Transaction* transaction, bool* root_is_latched) {
    if (node->is_root_page()) {
        return adjust_root(node);
    }
    if (node->get_size() >= node->get_min_size()) {
        return false;
    }

    IxNodeHandle* parent = fetch_node(node->get_parent_page_no());
    int index = parent->find_child(node);
    int neighbor_index = index == 0 ? 1 : index - 1;
    IxNodeHandle* neighbor = fetch_node(parent->value_at(neighbor_index));

    bool deleted = false;
    if (node->get_size() + neighbor->get_size() >= node->get_min_size() * 2) {
        redistribute(neighbor, node, parent, index);
    } else {
        deleted = coalesce(&neighbor, &node, &parent, index, transaction, root_is_latched);
    }

    buffer_pool_manager_->unpin_page(neighbor->get_page_id(), true);
    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
    delete neighbor;
    delete parent;
    return deleted;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesce_or_redistribute()
 */
bool IxIndexHandle::adjust_root(IxNodeHandle* old_root_node) {
    if (!old_root_node->is_leaf_page() && old_root_node->get_size() == 1) {
        page_id_t child_page_no = old_root_node->value_at(0);
        IxNodeHandle* child = fetch_node(child_page_no);
        child->set_parent_page_no(IX_NO_PAGE);
        update_root_page_no(child_page_no);
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        delete child;
        return true;
    }
    if (old_root_node->is_leaf_page() && old_root_node->get_size() == 0) {
        return false;
    }
    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::redistribute(IxNodeHandle* neighbor_node, IxNodeHandle* node, IxNodeHandle* parent, int index) {
    (void)parent;
    if (index == 0) {
        node->insert_pair(node->get_size(), neighbor_node->get_key(0), *neighbor_node->get_rid(0));
        neighbor_node->erase_pair(0);
        maintain_child(node, node->get_size() - 1);
        maintain_parent(neighbor_node);
    } else {
        int move_pos = neighbor_node->get_size() - 1;
        node->insert_pair(0, neighbor_node->get_key(move_pos), *neighbor_node->get_rid(move_pos));
        neighbor_node->erase_pair(move_pos);
        maintain_child(node, 0);
        maintain_parent(node);
    }
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::coalesce(IxNodeHandle** neighbor_node, IxNodeHandle** node, IxNodeHandle** parent, int index,
                             Transaction* transaction, bool* root_is_latched) {
    if (index == 0) {
        std::swap(*neighbor_node, *node);
        index = 1;
    }

    IxNodeHandle* left = *neighbor_node;
    IxNodeHandle* right = *node;
    int left_size = left->get_size();
    left->insert_pairs(left_size, right->get_key(0), right->get_rid(0), right->get_size());
    for (int i = left_size; i < left->get_size(); ++i) {
        maintain_child(left, i);
    }

    if (right->is_leaf_page()) {
        if (file_hdr_->last_leaf_ == right->get_page_no()) {
            file_hdr_->last_leaf_ = left->get_page_no();
        }
        erase_leaf(right);
    }

    (*parent)->erase_pair(index);
    release_node_handle(*right);
    return coalesce_or_redistribute(*parent, transaction, root_is_latched);
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid& iid) const {
    IxNodeHandle* node = fetch_node(iid.page_no);
    if (iid.slot_no >= node->get_size()) {
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        delete node;
        throw IndexEntryNotFoundError();
    }
    buffer_pool_manager_->unpin_page(node->get_page_id(), false); // unpin it!
    Rid rid = *node->get_rid(iid.slot_no);
    delete node;
    return rid;
}

/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char* key) const {
    IxNodeHandle leaf;
    fetch_node_into(file_hdr_->root_page_, leaf);
    while (!leaf.is_leaf_page()) {
        int child_idx = leaf.lower_bound(key);
        if (child_idx >= leaf.get_size()) {
            child_idx = leaf.get_size() - 1;
        } else if (child_idx > 0 &&
                   ix_compare(leaf.get_key(child_idx), key, file_hdr_->col_types_, file_hdr_->col_lens_) > 0) {
            child_idx--;
        }
        page_id_t child_page_no = leaf.value_at(child_idx);
        buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
        fetch_node_into(child_page_no, leaf);
    }
    int slot_no = leaf.lower_bound(key);
    Iid iid{leaf.get_page_no(), slot_no};
    if (slot_no == leaf.get_size() && leaf.get_page_no() != file_hdr_->last_leaf_) {
        iid = Iid{leaf.get_next_leaf(), 0};
    }
    buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
    return iid;
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char* key) {
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::FIND, nullptr);
    int slot_no = leaf->upper_bound(key);
    Iid iid{leaf->get_page_no(), slot_no};
    if (slot_no == leaf->get_size() && leaf->get_page_no() != file_hdr_->last_leaf_) {
        iid = Iid{leaf->get_next_leaf(), 0};
    }
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    delete leaf;
    return iid;
}

/**
 * @brief Single root-to-leaf descent returning both lower and upper Iid for an
 * equality lookup. Avoids the double tree walk of lower_bound + upper_bound.
 */
std::pair<Iid, Iid> IxIndexHandle::equal_range(const char* key) {
    IxNodeHandle leaf;
    fetch_node_into(file_hdr_->root_page_, leaf);
    while (!leaf.is_leaf_page()) {
        int child_idx = leaf.lower_bound(key);
        if (child_idx >= leaf.get_size()) {
            child_idx = leaf.get_size() - 1;
        } else if (child_idx > 0 &&
                   ix_compare(leaf.get_key(child_idx), key, file_hdr_->col_types_, file_hdr_->col_lens_) > 0) {
            child_idx--;
        }
        page_id_t child_page_no = leaf.value_at(child_idx);
        buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
        fetch_node_into(child_page_no, leaf);
    }
    int lower_slot = leaf.lower_bound(key);
    int upper_slot = leaf.upper_bound(key);
    Iid lower{leaf.get_page_no(), lower_slot};
    if (lower_slot == leaf.get_size() && leaf.get_page_no() != file_hdr_->last_leaf_) {
        lower = Iid{leaf.get_next_leaf(), 0};
    }
    Iid upper{leaf.get_page_no(), upper_slot};
    if (upper_slot == leaf.get_size() && leaf.get_page_no() != file_hdr_->last_leaf_) {
        upper = Iid{leaf.get_next_leaf(), 0};
    }
    buffer_pool_manager_->unpin_page(leaf.get_page_id(), false);
    return {lower, upper};
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle* node = fetch_node(file_hdr_->last_leaf_);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false); // unpin it!
    delete node;
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle* IxIndexHandle::fetch_node(int page_no) const {
    Page* page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    IxNodeHandle* node = new IxNodeHandle(file_hdr_.get(), page);

    return node;
}

void IxIndexHandle::fetch_node_into(int page_no, IxNodeHandle& out) const {
    Page* page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    out.file_hdr = file_hdr_.get();
    out.page = page;
    out.page_hdr = reinterpret_cast<IxPageHdr*>(page->get_data());
    out.keys = page->get_data() + sizeof(IxPageHdr);
    out.rids = reinterpret_cast<Rid*>(out.keys + file_hdr_->keys_size_);
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle* IxIndexHandle::create_node() {
    IxNodeHandle* node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page* page = buffer_pool_manager_->new_page(&new_page_id);
    node = new IxNodeHandle(file_hdr_.get(), page);
    return node;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::maintain_parent(IxNodeHandle* node) {
    // Two alternating stack slots avoid heap alloc per parent fetch.
    IxNodeHandle slot_a;
    IxNodeHandle slot_b;
    slot_a.file_hdr = node->file_hdr;
    slot_a.page = node->page;
    slot_a.page_hdr = node->page_hdr;
    slot_a.keys = node->keys;
    slot_a.rids = node->rids;
    IxNodeHandle* curr = &slot_a;
    IxNodeHandle* next = &slot_b;
    while (curr->get_parent_page_no() != IX_NO_PAGE) {
        fetch_node_into(curr->get_parent_page_no(), *next);
        int rank = next->find_child(curr);
        char* parent_key = next->get_key(rank);
        char* child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
            assert(buffer_pool_manager_->unpin_page(next->get_page_id(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);
        assert(buffer_pool_manager_->unpin_page(next->get_page_id(), true));
        std::swap(curr, next);
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle* leaf) {
    assert(leaf->is_leaf_page());

    IxNodeHandle* prev = fetch_node(leaf->get_prev_leaf());
    prev->set_next_leaf(leaf->get_next_leaf());
    buffer_pool_manager_->unpin_page(prev->get_page_id(), true);
    delete prev;

    IxNodeHandle* next = fetch_node(leaf->get_next_leaf());
    next->set_prev_leaf(leaf->get_prev_leaf()); // 注意此处是SetPrevLeaf()
    buffer_pool_manager_->unpin_page(next->get_page_id(), true);
    delete next;
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle& node) {
    (void)node;
    file_hdr_->num_pages_--;
}

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle* node, int child_idx) {
    if (!node->is_leaf_page()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle* child = fetch_node(child_page_no);
        child->set_parent_page_no(node->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        delete child;
    }
}
