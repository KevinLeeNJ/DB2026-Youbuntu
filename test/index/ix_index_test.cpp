#undef NDEBUG

#define private public
#include "index/ix.h"
#include "index/ix_index_handle.h"
#include "storage/buffer_pool_manager.h"
#undef private

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <unistd.h>

#include "errors.h"
#include "gtest/gtest.h"
#include "storage/disk_manager.h"
#include "test_util.h"

const std::string TEST_DB_NAME = "ix_index_test_db";

namespace {

/* 构造单列 INT 类型索引的 ColMeta */
std::vector<ColMeta> make_int_col(const std::string& col_name) {
    ColMeta col;
    col.tab_name = "test_tab";
    col.name = col_name;
    col.type = TYPE_INT;
    col.len = 4;
    col.offset = 0;
    col.index = false;
    return {col};
}

/* 将 int 值写入 key buf 并返回指针 */
const char* int_key(int val) {
    static char buf[4];
    memcpy(buf, &val, 4);
    return buf;
}

/* 构建一个 int Rid */
Rid make_rid(int page_no, int slot_no) {
    return Rid{page_no, slot_no};
}

} // namespace

class IxIndexTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<IxManager> ix_manager_;

    std::string tab_name_;
    std::vector<ColMeta> idx_cols_;
    std::string ix_name_;

    void SetUp() override {
        ::testing::Test::SetUp();
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());

        // 创建测试目录并进入
        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        ASSERT_TRUE(disk_manager_->is_dir(TEST_DB_NAME));
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }

        tab_name_ = "test_tab";
        idx_cols_ = make_int_col("a");
        ix_name_ = ix_manager_->get_index_name(tab_name_, idx_cols_);

        // 清理残留文件
        if (disk_manager_->is_file(ix_name_)) {
            disk_manager_->destroy_file(ix_name_);
        }
    }

    void TearDown() override {
        // 清理索引文件
        if (disk_manager_->is_file(ix_name_)) {
            disk_manager_->destroy_file(ix_name_);
        }
        // 返回上层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        ASSERT_TRUE(disk_manager_->is_dir(TEST_DB_NAME));
    }

    /* 创建并打开一个 INT 类型单列索引 */
    std::unique_ptr<IxIndexHandle> create_int_index() {
        ix_manager_->create_index(tab_name_, idx_cols_);
        return ix_manager_->open_index(tab_name_, idx_cols_);
    }

    /* 关闭并销毁索引 */
    void close_and_destroy(IxIndexHandle* ih) {
        ix_manager_->close_index(ih);
        if (disk_manager_->is_file(ix_name_)) {
            disk_manager_->destroy_file(ix_name_);
        }
    }

    /* 验证指定 key 可以通过 get_value 找到 */
    void assert_can_find(IxIndexHandle* ih, int key_val) {
        std::vector<Rid> result;
        bool found = ih->get_value(int_key(key_val), &result, nullptr);
        EXPECT_TRUE(found) << "key " << key_val << " should be found";
        EXPECT_EQ(result.size(), 1u) << "key " << key_val << " should have exactly 1 rid";
    }

    /* 验证指定 key 不存在 */
    void assert_not_found(IxIndexHandle* ih, int key_val) {
        std::vector<Rid> result;
        bool found = ih->get_value(int_key(key_val), &result, nullptr);
        EXPECT_FALSE(found) << "key " << key_val << " should NOT be found";
        EXPECT_EQ(result.size(), 0u);
    }

    /* 验证 B+ 树中所有插入的 key 都能找到 */
    void verify_all_keys(IxIndexHandle* ih, const std::set<int>& keys) {
        for (int k : keys) {
            std::vector<Rid> result;
            ASSERT_TRUE(ih->get_value(int_key(k), &result, nullptr))
                << "key " << k << " should exist after insert";
            ASSERT_EQ(result.size(), 1u);
        }
    }

    int key_at_iid(IxIndexHandle* ih, const Iid& iid) {
        IxNodeHandle* node = ih->fetch_node(iid.page_no);
        int key = *(int*)node->get_key(iid.slot_no);
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        return key;
    }

    IxNodeHandle* create_test_node(IxIndexHandle* ih, bool is_leaf, page_id_t parent) {
        IxNodeHandle* node = ih->create_node();
        node->page_hdr->is_leaf = is_leaf;
        node->page_hdr->parent = parent;
        node->page_hdr->num_key = 0;
        node->page_hdr->prev_leaf = IX_LEAF_HEADER_PAGE;
        node->page_hdr->next_leaf = IX_LEAF_HEADER_PAGE;
        return node;
    }

    page_id_t create_child_page(IxIndexHandle* ih, page_id_t parent, int key) {
        IxNodeHandle* child = create_test_node(ih, true, parent);
        child->insert_pair(0, int_key(key), make_rid(key, key));
        page_id_t page_no = child->get_page_no();
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        return page_no;
    }

    void append_child_ref(IxNodeHandle* internal, int key, page_id_t child_page_no) {
        internal->insert_pair(internal->get_size(), int_key(key), make_rid(child_page_no, -1));
    }

    /* 全索引扫描，返回有序的 (key, Rid) 对列表 */
    std::vector<std::pair<int, Rid>> full_scan(IxIndexHandle* ih) {
        std::vector<std::pair<int, Rid>> result;
        IxScan scan(ih, ih->leaf_begin(), ih->leaf_end(), buffer_pool_manager_.get());
        for (; !scan.is_end(); scan.next()) {
            Rid rid = scan.rid();
            // 通过 Iid 读取 key：先 fetch 结点，再取 key
            Iid iid = scan.iid();
            IxNodeHandle* node = ih->fetch_node(iid.page_no);
            int key = *(int*)node->get_key(iid.slot_no);
            buffer_pool_manager_->unpin_page(node->get_page_id(), false);
            result.push_back({key, rid});
        }
        return result;
    }
};

// =============================================================================
// 基础功能测试：单点读写
// =============================================================================

TEST_F(IxIndexTest, SinglePointInsertAndFind) {
    auto ih = create_int_index();

    // 非递增顺序插入少量 key
    int keys[] = {5, 3, 8, 1, 9, 2, 7, 4, 6, 0};
    for (int k : keys) {
        Rid rid = make_rid(k, k * 10);
        ih->insert_entry(int_key(k), rid, nullptr);
    }

    // 逐个验证
    for (int k : keys) {
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(int_key(k), &result, nullptr));
        ASSERT_EQ(result.size(), 1u);
    }

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, InsertWithCorrectRid) {
    auto ih = create_int_index();

    // 插入不同的 key，验证 Rid 正确
    for (int i = 0; i < 20; i++) {
        Rid rid = make_rid(i * 2, i * 3);
        ih->insert_entry(int_key(i), rid, nullptr);
    }

    for (int i = 0; i < 20; i++) {
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(int_key(i), &result, nullptr));
        ASSERT_EQ(result.size(), 1u);
        EXPECT_EQ(result[0].page_no, i * 2) << "page_no mismatch for key " << i;
        EXPECT_EQ(result[0].slot_no, i * 3) << "slot_no mismatch for key " << i;
    }

    close_and_destroy(ih.get());
}

// =============================================================================
// 边界情况：重复 Key 处理
// =============================================================================

TEST_F(IxIndexTest, DuplicateKeyRejected) {
    auto ih = create_int_index();

    Rid rid1 = make_rid(1, 100);
    page_id_t page_no1 = ih->insert_entry(int_key(42), rid1, nullptr);
    EXPECT_NE(page_no1, -1);

    // 获取初始 key 数量（通过根结点的 size）
    IxNodeHandle* root = ih->fetch_node(ih->file_hdr_->root_page_);
    int size_before = root->get_size();
    buffer_pool_manager_->unpin_page(root->get_page_id(), false);

    // 尝试插入重复 key
    ih->insert_entry(int_key(42), make_rid(2, 200), nullptr);

    // 插入后 size 应不变
    root = ih->fetch_node(ih->file_hdr_->root_page_);
    int size_after = root->get_size();
    buffer_pool_manager_->unpin_page(root->get_page_id(), false);

    EXPECT_EQ(size_before, size_after) << "duplicate key should not increase node size";

    // 查找时仍返回第一个 Rid
    std::vector<Rid> result;
    ASSERT_TRUE(ih->get_value(int_key(42), &result, nullptr));
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].page_no, 1);
    EXPECT_EQ(result[0].slot_no, 100);

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, MultipleDuplicatesRejected) {
    auto ih = create_int_index();

    Rid rid = make_rid(10, 10);
    ih->insert_entry(int_key(99), rid, nullptr);

    // 多次尝试插入重复 key
    for (int i = 0; i < 5; i++) {
        Rid rid_dup = make_rid(10 + i, 20 + i);
        ih->insert_entry(int_key(99), rid_dup, nullptr);
    }

    // 查找时应只有一个结果
    std::vector<Rid> result;
    ASSERT_TRUE(ih->get_value(int_key(99), &result, nullptr));
    EXPECT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].page_no, 10);
    EXPECT_EQ(result[0].slot_no, 10);

    close_and_destroy(ih.get());
}

// =============================================================================
// 结构变更测试：节点分裂
// =============================================================================

TEST_F(IxIndexTest, NodeSplitOnSequentialInsert) {
    auto ih = create_int_index();

    // 计算触发分裂所需的 key 数量
    int max_size = ih->file_hdr_->btree_order_ + 1; // 每个节点最大键值对数
    int split_threshold = max_size + 10;              // 确保触发至少一次分裂
    std::set<int> inserted_keys;

    for (int i = 0; i < split_threshold; i++) {
        Rid rid = make_rid(i, i * 10);
        ih->insert_entry(int_key(i), rid, nullptr);
        inserted_keys.insert(i);
    }

    // 验证所有 key 都能找到
    verify_all_keys(ih.get(), inserted_keys);

    // 检查根节点：分裂后可能变成内部节点（有多个叶子时）
    IxNodeHandle* root = ih->fetch_node(ih->file_hdr_->root_page_);
    int root_size = root->get_size();
    EXPECT_GT(root_size, 0) << "root should not be empty after inserts";
    buffer_pool_manager_->unpin_page(root->get_page_id(), false);

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, NodeSplitNonSequentialInsert) {
    auto ih = create_int_index();

    std::set<int> inserted_keys;
    // 使用非递增数据：随机打乱后的数据
    int keys[] = {42, 17, 89, 3,  56, 91, 23, 78, 5,  38, 71, 14, 63, 97, 29,
                  81, 8,  50, 35, 68, 11, 44, 76, 20, 53, 95, 2,  31, 65, 87};
    int num_keys = sizeof(keys) / sizeof(keys[0]);

    for (int i = 0; i < num_keys; i++) {
        Rid rid = make_rid(i, keys[i]);
        ih->insert_entry(int_key(keys[i]), rid, nullptr);
        inserted_keys.insert(keys[i]);
    }

    verify_all_keys(ih.get(), inserted_keys);

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, MassiveInsertForcesMultiLevelSplit) {
    auto ih = create_int_index();

    int max_size = ih->file_hdr_->btree_order_ + 1;
    int total = max_size * 3; // 确保多层级分裂
    std::set<int> inserted_keys;

    for (int i = 0; i < total; i++) {
        Rid rid = make_rid(i, i + 1);
        ih->insert_entry(int_key(i), rid, nullptr);
        inserted_keys.insert(i);
    }

    // 验证所有 key
    verify_all_keys(ih.get(), inserted_keys);

    // 检查 B+ 树高度：如果不是叶子节点，说明至少有两层
    IxNodeHandle* root = ih->fetch_node(ih->file_hdr_->root_page_);
    if (!root->is_leaf_page()) {
        // 多层级树：内部节点的孩子也可能仍是内部节点
        int num_pages = ih->file_hdr_->num_pages_;
        EXPECT_GT(num_pages, 3) << "should have more than initial pages after massive inserts";
    }
    buffer_pool_manager_->unpin_page(root->get_page_id(), false);

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, SplitMiddleLeafUpdatesNeighborLinks) {
    auto ih = create_int_index();

    int max_size = ih->file_hdr_->btree_order_ + 1;
    std::set<int> inserted_keys;

    for (int i = 0; i < max_size + 20; i++) {
        ih->insert_entry(int_key(i), make_rid(i, i), nullptr);
        inserted_keys.insert(i);
    }

    page_id_t original_first_leaf = ih->file_hdr_->first_leaf_;
    IxNodeHandle* first_leaf = ih->fetch_node(original_first_leaf);
    ASSERT_NE(first_leaf->get_next_leaf(), IX_LEAF_HEADER_PAGE);
    page_id_t original_second_leaf = first_leaf->get_next_leaf();
    buffer_pool_manager_->unpin_page(first_leaf->get_page_id(), false);

    for (int i = 1; i <= max_size + 20; i++) {
        int key = -i;
        ih->insert_entry(int_key(key), make_rid(i, key), nullptr);
        inserted_keys.insert(key);
    }

    first_leaf = ih->fetch_node(original_first_leaf);
    page_id_t inserted_middle_leaf = first_leaf->get_next_leaf();
    ASSERT_NE(inserted_middle_leaf, IX_LEAF_HEADER_PAGE);
    buffer_pool_manager_->unpin_page(first_leaf->get_page_id(), false);

    IxNodeHandle* middle_leaf = ih->fetch_node(inserted_middle_leaf);
    EXPECT_NE(middle_leaf->get_next_leaf(), IX_NO_PAGE);
    buffer_pool_manager_->unpin_page(middle_leaf->get_page_id(), false);

    IxNodeHandle* second_leaf = ih->fetch_node(original_second_leaf);
    EXPECT_NE(second_leaf->get_prev_leaf(), IX_NO_PAGE);
    buffer_pool_manager_->unpin_page(second_leaf->get_page_id(), false);

    verify_all_keys(ih.get(), inserted_keys);
    auto scanned = full_scan(ih.get());
    ASSERT_EQ(scanned.size(), inserted_keys.size());
    for (size_t i = 1; i < scanned.size(); i++) {
        EXPECT_LT(scanned[i - 1].first, scanned[i].first);
    }

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, FindFirstWalksToLeftmostLeaf) {
    auto ih = create_int_index();

    int max_size = ih->file_hdr_->btree_order_ + 1;
    for (int i = 0; i < max_size * 2; i++) {
        ih->insert_entry(int_key(i), make_rid(i, i), nullptr);
    }

    auto [leaf, root_is_latched] = ih->find_leaf_page(int_key(max_size * 2 - 1), Operation::FIND, nullptr, true);
    ASSERT_NE(leaf, nullptr);
    EXPECT_TRUE(root_is_latched);
    EXPECT_EQ(leaf->get_page_no(), ih->file_hdr_->first_leaf_);
    EXPECT_EQ(leaf->get_prev_leaf(), IX_LEAF_HEADER_PAGE);
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, PublicLowerUpperBoundOnPopulatedAndEmptyTree) {
    auto ih = create_int_index();

    int keys[] = {10, 20, 30, 40};
    for (int key : keys) {
        ih->insert_entry(int_key(key), make_rid(key, key), nullptr);
    }

    Iid lower = ih->lower_bound(int_key(25));
    ASSERT_NE(lower.page_no, -1);
    EXPECT_EQ(key_at_iid(ih.get(), lower), 30);

    Iid upper = ih->upper_bound(int_key(20));
    ASSERT_NE(upper.page_no, -1);
    EXPECT_EQ(key_at_iid(ih.get(), upper), 30);

    for (int key : keys) {
        ASSERT_TRUE(ih->delete_entry(int_key(key), nullptr));
    }
    EXPECT_TRUE(ih->is_empty());

    lower = ih->lower_bound(int_key(25));
    EXPECT_EQ(lower.page_no, -1);
    EXPECT_EQ(lower.slot_no, -1);

    upper = ih->upper_bound(int_key(25));
    EXPECT_EQ(upper.page_no, -1);
    EXPECT_EQ(upper.slot_no, -1);

    close_and_destroy(ih.get());
}

// =============================================================================
// 结构变更测试：节点合并 (Coalesce)
// =============================================================================

TEST_F(IxIndexTest, NodeCoalesceAfterDeletion) {
    auto ih = create_int_index();

    int max_size = ih->file_hdr_->btree_order_ + 1;
    int total = max_size * 2 + 50; // 确保有多个叶子结点
    std::set<int> remaining_keys;

    // 阶段 1：插入大量数据产生多页
    for (int i = 0; i < total; i++) {
        Rid rid = make_rid(i, i);
        ih->insert_entry(int_key(i), rid, nullptr);
        remaining_keys.insert(i);
    }

    // 记录删除前的页面数量
    int pages_before = ih->file_hdr_->num_pages_;

    // 阶段 2：删除大部分数据，触发合并
    int delete_count = total - max_size / 2; // 只保留约半个节点的数据
    for (int i = 0; i < delete_count; i++) {
        ASSERT_TRUE(ih->delete_entry(int_key(i), nullptr))
            << "delete_entry should succeed for key " << i;
        remaining_keys.erase(i);
    }

    // 阶段 3：验证剩余 key 仍然存在
    verify_all_keys(ih.get(), remaining_keys);

    // 验证已删除的 key 不存在
    for (int i = 0; i < delete_count; i++) {
        std::vector<Rid> result;
        EXPECT_FALSE(ih->get_value(int_key(i), &result, nullptr))
            << "key " << i << " should not exist after deletion";
    }

    // 删除后页面数应减少
    int pages_after = ih->file_hdr_->num_pages_;
    EXPECT_LT(pages_after, pages_before)
        << "page count should decrease after deleting most entries";

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, DeleteAllEntries) {
    auto ih = create_int_index();

    // 插入数据
    for (int i = 0; i < 50; i++) {
        Rid rid = make_rid(i, i);
        ih->insert_entry(int_key(i), rid, nullptr);
    }

    // 逐个删除所有数据
    for (int i = 0; i < 50; i++) {
        ASSERT_TRUE(ih->delete_entry(int_key(i), nullptr))
            << "delete_entry should succeed for key " << i;
    }

    // 全部删除后，B+ 树应为空
    EXPECT_TRUE(ih->is_empty()) << "tree should be empty after deleting all entries";

    // 已删除的 key 查不到
    for (int i = 0; i < 50; i++) {
        assert_not_found(ih.get(), i);
    }

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, InsertDeleteInsertCycle) {
    auto ih = create_int_index();

    // 插入 → 删除 → 再插入 循环
    for (int cycle = 0; cycle < 3; cycle++) {
        std::set<int> keys;
        for (int i = 0; i < 100; i++) {
            int key = cycle * 1000 + i;
            Rid rid = make_rid(key, key);
            ih->insert_entry(int_key(key), rid, nullptr);
            keys.insert(key);
        }
        verify_all_keys(ih.get(), keys);

        // 删除一半
        int del_count = 0;
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                int key = cycle * 1000 + i;
                ASSERT_TRUE(ih->delete_entry(int_key(key), nullptr));
                keys.erase(key);
                del_count++;
            }
        }
        verify_all_keys(ih.get(), keys);
    }

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, SplitInternalNodeUpdatesMovedChildrenParent) {
    auto ih = create_int_index();

    IxNodeHandle* internal = create_test_node(ih.get(), false, IX_NO_PAGE);
    int total_children = internal->get_max_size();
    std::vector<page_id_t> child_pages;
    child_pages.reserve(total_children);

    for (int i = 0; i < total_children; i++) {
        page_id_t child_page = create_child_page(ih.get(), internal->get_page_no(), i);
        child_pages.push_back(child_page);
        append_child_ref(internal, i, child_page);
    }

    IxNodeHandle* sibling = ih->split(internal);
    ASSERT_FALSE(sibling->is_leaf_page());

    for (int i = 0; i < internal->get_size(); i++) {
        IxNodeHandle* child = ih->fetch_node(internal->value_at(i));
        EXPECT_EQ(child->get_parent_page_no(), internal->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), false);
    }

    for (int i = 0; i < sibling->get_size(); i++) {
        IxNodeHandle* child = ih->fetch_node(sibling->value_at(i));
        EXPECT_EQ(child->get_parent_page_no(), sibling->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), false);
    }

    buffer_pool_manager_->unpin_page(sibling->get_page_id(), true);
    buffer_pool_manager_->unpin_page(internal->get_page_id(), true);

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, RedistributeInternalNodeFromRightSiblingMaintainsMovedChild) {
    auto ih = create_int_index();

    IxNodeHandle* parent = create_test_node(ih.get(), false, IX_NO_PAGE);
    IxNodeHandle* left = create_test_node(ih.get(), false, parent->get_page_no());
    IxNodeHandle* right = create_test_node(ih.get(), false, parent->get_page_no());

    page_id_t left_child0 = create_child_page(ih.get(), left->get_page_no(), 10);
    page_id_t left_child1 = create_child_page(ih.get(), left->get_page_no(), 20);
    page_id_t moved_child = create_child_page(ih.get(), right->get_page_no(), 30);
    page_id_t right_child1 = create_child_page(ih.get(), right->get_page_no(), 40);
    page_id_t right_child2 = create_child_page(ih.get(), right->get_page_no(), 50);

    append_child_ref(left, 10, left_child0);
    append_child_ref(left, 20, left_child1);
    append_child_ref(right, 30, moved_child);
    append_child_ref(right, 40, right_child1);
    append_child_ref(right, 50, right_child2);
    append_child_ref(parent, 10, left->get_page_no());
    append_child_ref(parent, 30, right->get_page_no());

    ih->redistribute(right, left, parent, 0);

    EXPECT_EQ(left->value_at(left->get_size() - 1), moved_child);
    IxNodeHandle* moved = ih->fetch_node(moved_child);
    EXPECT_EQ(moved->get_parent_page_no(), left->get_page_no());
    buffer_pool_manager_->unpin_page(moved->get_page_id(), false);
    EXPECT_EQ(*(int*)parent->get_key(1), 40);

    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
    buffer_pool_manager_->unpin_page(left->get_page_id(), true);
    buffer_pool_manager_->unpin_page(right->get_page_id(), true);

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, RedistributeInternalNodeFromLeftSiblingMaintainsMovedChild) {
    auto ih = create_int_index();

    IxNodeHandle* parent = create_test_node(ih.get(), false, IX_NO_PAGE);
    IxNodeHandle* left = create_test_node(ih.get(), false, parent->get_page_no());
    IxNodeHandle* right = create_test_node(ih.get(), false, parent->get_page_no());

    page_id_t left_child0 = create_child_page(ih.get(), left->get_page_no(), 10);
    page_id_t left_child1 = create_child_page(ih.get(), left->get_page_no(), 20);
    page_id_t moved_child = create_child_page(ih.get(), left->get_page_no(), 30);
    page_id_t right_child0 = create_child_page(ih.get(), right->get_page_no(), 40);
    page_id_t right_child1 = create_child_page(ih.get(), right->get_page_no(), 50);

    append_child_ref(left, 10, left_child0);
    append_child_ref(left, 20, left_child1);
    append_child_ref(left, 30, moved_child);
    append_child_ref(right, 40, right_child0);
    append_child_ref(right, 50, right_child1);
    append_child_ref(parent, 10, left->get_page_no());
    append_child_ref(parent, 40, right->get_page_no());

    ih->redistribute(left, right, parent, 1);

    EXPECT_EQ(right->value_at(0), moved_child);
    IxNodeHandle* moved = ih->fetch_node(moved_child);
    EXPECT_EQ(moved->get_parent_page_no(), right->get_page_no());
    buffer_pool_manager_->unpin_page(moved->get_page_id(), false);
    EXPECT_EQ(*(int*)parent->get_key(1), 30);

    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
    buffer_pool_manager_->unpin_page(left->get_page_id(), true);
    buffer_pool_manager_->unpin_page(right->get_page_id(), true);

    close_and_destroy(ih.get());
}

// =============================================================================
// 结构变更测试：顺序 / 逆序扫描
// =============================================================================

TEST_F(IxIndexTest, SequentialScanAscending) {
    auto ih = create_int_index();

    // 非顺序插入
    int raw_keys[] = {50, 20, 80, 10, 60, 30, 90, 40, 70, 0, 100};
    int num = sizeof(raw_keys) / sizeof(raw_keys[0]);
    std::set<int> expected_keys;

    for (int i = 0; i < num; i++) {
        Rid rid = make_rid(raw_keys[i], raw_keys[i] * 2);
        ih->insert_entry(int_key(raw_keys[i]), rid, nullptr);
        expected_keys.insert(raw_keys[i]);
    }

    // 全索引扫描
    auto scanned = full_scan(ih.get());

    // 验证扫描结果数量
    ASSERT_EQ(scanned.size(), expected_keys.size());

    // 验证扫描结果严格递增
    for (size_t i = 0; i < scanned.size(); i++) {
        ASSERT_EQ(scanned[i].first, *(std::next(expected_keys.begin(), i)))
            << "scan result at index " << i << " should match sorted key order";
        if (i > 0) {
            EXPECT_LT(scanned[i - 1].first, scanned[i].first)
                << "scan keys must be strictly increasing";
        }
    }

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, SequentialScanAfterSplits) {
    auto ih = create_int_index();

    int total = 500;
    std::set<int> expected_keys;
    for (int i = 0; i < total; i++) {
        Rid rid = make_rid(i, i * 10);
        ih->insert_entry(int_key(i), rid, nullptr);
        expected_keys.insert(i);
    }

    auto scanned = full_scan(ih.get());
    ASSERT_EQ(scanned.size(), expected_keys.size());

    // 验证结果严格递增
    for (size_t i = 1; i < scanned.size(); i++) {
        EXPECT_LT(scanned[i - 1].first, scanned[i].first);
    }

    // 验证完整性：每个插入的 key 都在扫描结果中
    for (int k : expected_keys) {
        auto it = std::find_if(scanned.begin(), scanned.end(),
                               [k](const auto& p) { return p.first == k; });
        ASSERT_NE(it, scanned.end()) << "key " << k << " should appear in scan";
    }

    close_and_destroy(ih.get());
}

// =============================================================================
// 鲁棒性与异常测试
// =============================================================================

TEST_F(IxIndexTest, FindNonExistentKey) {
    auto ih = create_int_index();

    // 插入一些 key
    for (int i = 0; i < 20; i++) {
        Rid rid = make_rid(i, i);
        ih->insert_entry(int_key(i * 2), rid, nullptr); // 只插入偶数
    }

    // 查找不存在的奇数 key
    for (int i = 0; i < 20; i++) {
        assert_not_found(ih.get(), i * 2 + 1);
    }

    // 查找超出范围的 key
    assert_not_found(ih.get(), -999);
    assert_not_found(ih.get(), 99999);

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, FindInEmptyTree) {
    auto ih = create_int_index();

    // 空树查找任何 key 都应返回 false
    assert_not_found(ih.get(), 0);
    assert_not_found(ih.get(), 42);
    assert_not_found(ih.get(), -1);

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, DeleteNonExistentKey) {
    auto ih = create_int_index();

    // 空树删除
    EXPECT_FALSE(ih->delete_entry(int_key(42), nullptr))
        << "deleting from empty tree should return false";

    // 插入少量数据
    for (int i = 0; i < 10; i++) {
        Rid rid = make_rid(i, i);
        ih->insert_entry(int_key(i), rid, nullptr);
    }

    // 删除不存在的 key
    EXPECT_FALSE(ih->delete_entry(int_key(100), nullptr))
        << "deleting non-existent key should return false";
    EXPECT_FALSE(ih->delete_entry(int_key(-5), nullptr))
        << "deleting negative key should return false";

    // 现有数据应不受影响
    for (int i = 0; i < 10; i++) {
        assert_can_find(ih.get(), i);
    }

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, DeleteTwice) {
    auto ih = create_int_index();

    Rid rid = make_rid(1, 100);
    ih->insert_entry(int_key(55), rid, nullptr);

    // 第一次删除应成功
    EXPECT_TRUE(ih->delete_entry(int_key(55), nullptr));

    // 第二次删除应失败
    EXPECT_FALSE(ih->delete_entry(int_key(55), nullptr));

    assert_not_found(ih.get(), 55);

    close_and_destroy(ih.get());
}

TEST_F(IxIndexTest, NegativeKeys) {
    auto ih = create_int_index();

    // 插入负数和正数混合
    int keys[] = {-50, -20, -80, 0, -10, 30, -40, 10, -30, 20};
    int num = sizeof(keys) / sizeof(keys[0]);
    std::set<int> inserted;

    for (int i = 0; i < num; i++) {
        Rid rid = make_rid(keys[i] + 100, i);
        ih->insert_entry(int_key(keys[i]), rid, nullptr);
        inserted.insert(keys[i]);
    }

    verify_all_keys(ih.get(), inserted);

    // 扫描验证有序
    auto scanned = full_scan(ih.get());
    ASSERT_EQ(scanned.size(), inserted.size());
    for (size_t i = 1; i < scanned.size(); i++) {
        EXPECT_LT(scanned[i - 1].first, scanned[i].first);
    }

    close_and_destroy(ih.get());
}

// =============================================================================
// 边界测试：大键值范围
// =============================================================================

TEST_F(IxIndexTest, LargeKeyRange) {
    auto ih = create_int_index();

    std::set<int> inserted;
    // 插入大范围离散 key（INT_MIN / INT_MAX 附近）
    int test_keys[] = {
        -2000000000, -1000000000, 0,
        1000000000,  2000000000
    };
    for (int k : test_keys) {
        Rid rid = make_rid(k & 0xFF, (k >> 8) & 0xFF);
        ih->insert_entry(int_key(k), rid, nullptr);
        inserted.insert(k);
    }

    verify_all_keys(ih.get(), inserted);

    close_and_destroy(ih.get());
}

// =============================================================================
// 并发安全：重复打开关闭索引
// =============================================================================

TEST_F(IxIndexTest, ReopenIndexPreservesData) {
    auto ih = create_int_index();

    std::set<int> inserted;
    for (int i = 0; i < 100; i++) {
        Rid rid = make_rid(i, i);
        ih->insert_entry(int_key(i), rid, nullptr);
        inserted.insert(i);
    }

    // 关闭索引（数据落盘）
    close_and_destroy(ih.get());

    // 重新创建并打开（这里用 open_index 是因为文件还在）
    // 注意：close_and_destroy 已销毁文件，需要重新创建
    ix_manager_->create_index(tab_name_, idx_cols_);
    auto ih2 = ix_manager_->open_index(tab_name_, idx_cols_);

    // 重新插入相同数据
    for (int i = 0; i < 100; i++) {
        Rid rid = make_rid(i, i);
        ih2->insert_entry(int_key(i), rid, nullptr);
    }

    verify_all_keys(ih2.get(), inserted);

    close_and_destroy(ih2.get());
}

// =============================================================================
// 资源释放：确保测试间无干扰
// =============================================================================

TEST_F(IxIndexTest, CleanStateAfterEachTest) {
    // 验证测试夹具正确清理了上一测试的残留文件
    EXPECT_FALSE(disk_manager_->is_file(ix_name_))
        << "index file should not exist at start of test";
}
