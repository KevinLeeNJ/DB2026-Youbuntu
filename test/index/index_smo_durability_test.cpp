/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// A structure-modification operation (split, new root, separator update) dirties
// several pages at once, and index pages take part in no WAL at all. If the
// buffer pool is allowed to publish those pages independently, a crash between
// two evictions leaves a tree on disk that does not describe itself, and
// recovery has no option but to rebuild the whole index.
//
// Every test here ends in a *crash*, not a close: dropping the buffer pool
// without flushing is exactly what SIGKILL does to the pages it still holds.
// close_index() would hide the bug by flushing everything on the way out.

#undef NDEBUG

#define private public
#include "index/ix.h"
#undef private

#include "index/ix_smo_image.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

#include <unistd.h>

#include "gtest/gtest.h"
#include "recovery/index_smo_log.h"
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_meta.h"

namespace {

static_assert(!std::is_assignable_v<decltype(std::declval<IndexSmoWalLayout&>().page_count()), uint32_t>);

// Small enough that a few thousand keys force continuous eviction, which is what
// makes the on-disk image of an in-flight SMO partial in the first place.
constexpr size_t kSmallPoolFrames = 48;
// 64-byte keys keep btree_order at 55, so a few thousand keys build a
// three-level tree spanning far more pages than the pool holds.
constexpr int kKeyLen = 64;
constexpr uint32_t kIndexSmoFixedPayloadBytes =
    sizeof(uint32_t) + sizeof(uint16_t) * 2 + sizeof(uint32_t) * 4 + sizeof(uint64_t);
constexpr uint32_t kIndexSmoImageEnvelopeBytes = sizeof(uint8_t) * 2 + sizeof(uint16_t) + sizeof(uint32_t) * 2;
constexpr uint32_t kIndexSmoStructuredLayoutBytes = sizeof(int32_t) * 3;

template <typename T> void AppendTestScalar(std::vector<char>* bytes, T value) {
    const size_t offset = bytes->size();
    bytes->resize(offset + sizeof(T));
    std::memcpy(bytes->data() + offset, &value, sizeof(T));
}

void RefreshIndexSmoChecksum(std::vector<char>* bytes) {
    const uint32_t checksum = IndexSmoCrc32(bytes->data(), bytes->size() - sizeof(uint32_t));
    std::memcpy(bytes->data() + bytes->size() - sizeof(uint32_t), &checksum, sizeof(checksum));
}

WalRecordView IndexSmoViewOf(const std::vector<char>& bytes) {
    WalRecordView view;
    view.log_type = LogType::INDEX_SMO;
    view.lsn = read_unaligned<lsn_t>(bytes.data() + OFFSET_LSN);
    view.prev_lsn = read_unaligned<lsn_t>(bytes.data() + OFFSET_PREV_LSN);
    view.txn_id = read_unaligned<txn_id_t>(bytes.data() + OFFSET_LOG_TID);
    view.total_len = static_cast<uint32_t>(bytes.size());
    view.bytes = bytes.data();
    return view;
}

std::vector<char> SerializeIndexSmo(const IndexSmoWalData& data) {
    IndexSmoLogRecord record(data);
    record.lsn_ = 17;
    std::vector<char> bytes(record.log_tot_len_);
    record.serialize(bytes.data());
    return bytes;
}

std::vector<char> SerializeLegacyIndexSmo(const IndexSmoWalData& data) {
    const uint32_t total_length =
        LOG_HEADER_SIZE + kIndexSmoFixedPayloadBytes + static_cast<uint32_t>(data.index_file_name.size()) +
        static_cast<uint32_t>(data.pages.size()) * (sizeof(page_id_t) + PAGE_SIZE) + PAGE_SIZE + sizeof(uint32_t);
    std::vector<char> bytes;
    bytes.reserve(total_length);

    LogRecord header;
    header.log_type_ = LogType::INDEX_SMO;
    header.lsn_ = 17;
    header.log_tot_len_ = total_length;
    header.log_tid_ = INVALID_TXN_ID;
    header.prev_lsn_ = INVALID_LSN;
    bytes.resize(LOG_HEADER_SIZE);
    header.serialize(bytes.data());

    AppendTestScalar(&bytes, INDEX_SMO_MAGIC);
    AppendTestScalar(&bytes, INDEX_SMO_FORMAT_LEGACY_RAW);
    AppendTestScalar(&bytes, INDEX_SMO_FLAG_HEADER_IMAGE);
    AppendTestScalar(&bytes, static_cast<uint32_t>(data.index_file_name.size()));
    AppendTestScalar(&bytes, static_cast<uint32_t>(data.pages.size()));
    AppendTestScalar(&bytes, static_cast<uint32_t>(PAGE_SIZE));
    AppendTestScalar(&bytes, static_cast<uint32_t>(PAGE_SIZE));
    AppendTestScalar(&bytes, data.index_generation);
    bytes.insert(bytes.end(), data.index_file_name.begin(), data.index_file_name.end());
    for (const auto& page : data.pages) {
        AppendTestScalar(&bytes, page.page_no);
        bytes.insert(bytes.end(), page.bytes.begin(), page.bytes.end());
    }
    bytes.insert(bytes.end(), data.header.begin(), data.header.end());
    AppendTestScalar(&bytes, IndexSmoCrc32(bytes.data(), bytes.size()));
    EXPECT_EQ(bytes.size(), total_length);
    return bytes;
}

IxFileHdr WalImageLayout(int col_len = 13, int btree_order = 11) {
    IxFileHdr file_hdr;
    file_hdr.col_tot_len_ = col_len;
    file_hdr.btree_order_ = btree_order;
    file_hdr.keys_size_ = IxKeysSize(btree_order + 1, col_len);
    return file_hdr;
}

std::array<char, PAGE_SIZE> WalImageHeader(const IxFileHdr& layout) {
    IxFileHdr header(IX_NO_PAGE, 32, IX_INIT_ROOT_PAGE, 1, layout.col_tot_len_, layout.btree_order_, layout.keys_size_,
                     IX_INIT_ROOT_PAGE, IX_INIT_ROOT_PAGE);
    header.col_types_.push_back(TYPE_STRING);
    header.col_lens_.push_back(layout.col_tot_len_);
    header.update_tot_len();
    std::array<char, PAGE_SIZE> image{};
    header.serialize(image.data());
    return image;
}

std::array<char, PAGE_SIZE> WalImagePage(const IxFileHdr& file_hdr, int num_key, char fill = 'x') {
    std::array<char, PAGE_SIZE> image;
    image.fill(fill);
    const IxPageHdr page_hdr{
        .next_free_page_no = IX_NO_PAGE,
        .parent = 7,
        .num_key = num_key,
        .is_leaf = true,
        .prev_leaf = 3,
        .next_leaf = 9,
    };
    std::memcpy(image.data(), &page_hdr, sizeof(page_hdr));
    for (int i = 0; i < num_key; ++i) {
        std::fill_n(image.data() + sizeof(IxPageHdr) + i * file_hdr.col_tot_len_, file_hdr.col_tot_len_,
                    static_cast<char>('A' + i % 25));
        const Rid rid{i + 20, i + 30};
        std::memcpy(image.data() + sizeof(IxPageHdr) + file_hdr.keys_size_ + i * sizeof(Rid), &rid, sizeof(rid));
    }
    return image;
}

uint8_t FirstStructuredPageCodec(const std::vector<char>& bytes, size_t name_bytes) {
    const uint32_t image_offset = OFFSET_LOG_DATA + kIndexSmoFixedPayloadBytes + kIndexSmoStructuredLayoutBytes +
                                  static_cast<uint32_t>(name_bytes) + sizeof(page_id_t);
    return static_cast<uint8_t>(bytes[image_offset]);
}

void ExpectStructuredCodecRoundTripAndFailClosed(const IndexSmoWalData& data, uint8_t expected_codec) {
    std::vector<char> bytes = SerializeIndexSmo(data);
    ASSERT_EQ(read_unaligned<uint16_t>(bytes.data() + OFFSET_LOG_DATA + sizeof(uint32_t)),
              INDEX_SMO_FORMAT_STRUCTURED);
    ASSERT_EQ(FirstStructuredPageCodec(bytes, data.index_file_name.size()), expected_codec);

    IndexSmoWalLayout layout;
    ASSERT_TRUE(InspectIndexSmoWal(IndexSmoViewOf(bytes), &layout));
    std::vector<IndexSmoWalView::Page> catalogue(layout.page_count());
    ASSERT_TRUE(CopyIndexSmoPageCatalog(IndexSmoViewOf(bytes), catalogue.data(), catalogue.size()));
    ASSERT_EQ(catalogue[0].page_no, data.pages[0].page_no);
    ASSERT_EQ(catalogue[0].image, nullptr);

    std::vector<IndexSmoWalView::Page> pages(layout.page_count());
    std::vector<std::array<char, PAGE_SIZE>> decoded(layout.decoded_count());
    IndexSmoWalDecodedView decoded_view;
    ASSERT_TRUE(DecodeIndexSmoWal(IndexSmoViewOf(bytes), {pages.data(), pages.size(), decoded.data(), decoded.size()},
                                  &decoded_view));
    ASSERT_EQ(decoded_view.index_file_name, data.index_file_name);
    ASSERT_EQ(std::memcmp(decoded_view.pages[0].image, data.pages[0].bytes.data(), PAGE_SIZE), 0);
    ASSERT_EQ(std::memcmp(decoded_view.header_image, data.header.data(), PAGE_SIZE), 0);

    IndexSmoWalView wrapped;
    ASSERT_TRUE(ParseIndexSmoWal(IndexSmoViewOf(bytes), &wrapped));
    ASSERT_EQ(wrapped.index_file_name, decoded_view.index_file_name);
    ASSERT_EQ(std::memcmp(wrapped.page_image(0), decoded_view.pages[0].image, PAGE_SIZE), 0);

    std::array<IndexSmoWalView::Page, 1> untouched{{{999, reinterpret_cast<const char*>(0x7)}}};
    EXPECT_FALSE(CopyIndexSmoPageCatalog(IndexSmoViewOf(bytes), untouched.data(), 0));
    EXPECT_EQ(untouched[0].page_no, 999);
    EXPECT_EQ(untouched[0].image, reinterpret_cast<const char*>(0x7));
    IndexSmoWalDecodedView unchanged;
    unchanged.index_file_name = "unchanged";
    EXPECT_FALSE(DecodeIndexSmoWal(IndexSmoViewOf(bytes), {untouched.data(), 0, decoded.data(), decoded.size()}, &unchanged));
    EXPECT_EQ(unchanged.index_file_name, "unchanged");

    // Preserve the allocation and record length, then give the modified bytes
    // a correct trailing CRC. Safe entry points do not accept the prior
    // inspection token, so they must freshly inspect and decode these bytes.
    const uint32_t page_id_offset = OFFSET_LOG_DATA + kIndexSmoFixedPayloadBytes + kIndexSmoStructuredLayoutBytes +
                                    static_cast<uint32_t>(data.index_file_name.size());
    const page_id_t mutated_page = data.pages[0].page_no + 17;
    std::memcpy(bytes.data() + page_id_offset, &mutated_page, sizeof(mutated_page));
    RefreshIndexSmoChecksum(&bytes);
    EXPECT_TRUE(CopyIndexSmoPageCatalog(IndexSmoViewOf(bytes), untouched.data(), untouched.size()));
    EXPECT_TRUE(DecodeIndexSmoWal(IndexSmoViewOf(bytes), {pages.data(), pages.size(), decoded.data(), decoded.size()},
                                  &unchanged));
}

void ExpectAnalysisCatalog(const IndexSmoWalData& data, const std::vector<char>& bytes) {
    IndexSmoWalLayout inspected;
    ASSERT_TRUE(InspectIndexSmoWal(IndexSmoViewOf(bytes), &inspected));
    std::vector<char> name(data.index_file_name.size(), 'n');
    std::vector<page_id_t> pages(data.pages.size(), INVALID_PAGE_ID);
    IndexSmoWalAnalysis analysis;

    ResetIndexSmoCrc32CallCountForTest();
    ResetIndexSmoImageDecodeCallCountForTest();
    ASSERT_TRUE(AnalyzeIndexSmoWal(IndexSmoViewOf(bytes),
                                   {name.data(), name.size(), pages.data(), pages.size()}, &analysis));
    EXPECT_EQ(IndexSmoCrc32CallCountForTest(), 1U);
    EXPECT_EQ(IndexSmoImageDecodeCallCountForTest(), inspected.decoded_count());
    EXPECT_EQ(analysis.index_name_bytes, data.index_file_name.size());
    EXPECT_EQ(std::string_view(name.data(), analysis.index_name_bytes), data.index_file_name);
    EXPECT_EQ(analysis.page_count, data.pages.size());
    EXPECT_EQ(analysis.index_generation, data.index_generation);
    for (size_t index = 0; index < data.pages.size(); ++index) {
        EXPECT_EQ(pages[index], data.pages[index].page_no);
    }
}

class IndexSmoDurabilityTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager;
    std::unique_ptr<IxManager> ix_manager;
    std::unique_ptr<LogManager> log_manager;
    std::string table_name;
    std::vector<ColMeta> cols;
    std::filesystem::path old_path;
    std::filesystem::path test_path;

    void SetUp() override {
        table_name = "index_smo_durability_test_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        old_path = std::filesystem::current_path();
        test_path = std::filesystem::temp_directory_path() / table_name;
        std::filesystem::remove_all(test_path);
        std::filesystem::create_directories(test_path);
        std::filesystem::current_path(test_path);
        open_storage();
        cols = {ColMeta{
            .tab_name = table_name, .name = "k", .type = TYPE_STRING, .len = kKeyLen, .offset = 0, .index = true}};
        cleanup();
        ix_manager->create_index(table_name, cols);
    }

    void TearDown() override {
        BufferPoolManager::set_flush_page_test_hook({});
        BufferPoolManager::set_flush_page_after_write_test_hook({});
        if (buffer_pool_manager != nullptr) {
            buffer_pool_manager->set_log_manager(nullptr);
        }
        log_manager.reset();
        if (ix_manager != nullptr && disk_manager != nullptr) {
            cleanup();
        }
        ix_manager.reset();
        buffer_pool_manager.reset();
        disk_manager.reset();
        if (!old_path.empty()) {
            std::filesystem::current_path(old_path);
        }
        if (!test_path.empty()) {
            std::filesystem::remove_all(test_path);
        }
    }

    void open_storage() {
        disk_manager = std::make_unique<DiskManager>();
        buffer_pool_manager = std::make_unique<BufferPoolManager>(kSmallPoolFrames, disk_manager.get());
        ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    }

    void enable_wal(DurabilityMode durability_mode = DurabilityMode::STRICT) {
        if (!disk_manager->is_file(LOG_FILE_NAME)) {
            disk_manager->create_file(LOG_FILE_NAME);
        }
        log_manager = std::make_unique<LogManager>(disk_manager.get(), durability_mode);
        buffer_pool_manager->set_log_manager(log_manager.get());
    }

    // Drops the buffer pool and the in-memory index header without flushing
    // either: the on-disk file is left holding exactly what was written before
    // this point, which is what a restart after SIGKILL sees.
    void crash_and_reopen_storage(std::unique_ptr<IxIndexHandle>& ih) {
        ih.reset();
        ix_manager.reset();
        buffer_pool_manager.reset();
        disk_manager.reset();
        open_storage();
    }

    void cleanup() {
        auto index_name = ix_manager->get_index_name(table_name, cols);
        // Every test leaves its handle open on purpose - a crash does not close
        // files - so drop the descriptor here rather than through close_index(),
        // which would flush the very pages the test is checking never reached
        // disk. get_file_fd returns the existing descriptor or temporarily
        // opens the file, keeping cleanup within the public registry lifecycle.
        if (disk_manager->is_file(index_name)) {
            const int fd = disk_manager->get_file_fd(index_name);
            disk_manager->close_file(fd);
            disk_manager->destroy_file(index_name);
        }
    }

    std::unique_ptr<IxIndexHandle> open_index() {
        return ix_manager->open_index(table_name, cols);
    }

    static std::vector<char> key(int value) {
        std::vector<char> buf(kKeyLen, 0);
        // Zero-padded so memcmp order matches numeric order.
        const std::string encoded = std::to_string(1000000 + value);
        std::memcpy(buf.data(), encoded.data(), encoded.size());
        return buf;
    }
};

// The minimal torn SMO, built deliberately instead of waiting for the buffer
// pool to produce one. The first leaf split also replaces the root, so it
// dirties three pages plus the index header; publishing only the new root - one
// single frame eviction - is enough to leave a parent naming a child whose
// persisted image still claims to be a parentless root.
TEST_F(IndexSmoDurabilityTest, SplitPublishesEveryPageItDirtied) {
    auto ih = open_index();
    const int order = ih->file_hdr_->btree_order_;

    // One key more than a single leaf can hold.
    for (int value = 0; value <= order; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }
    ASSERT_NE(ih->file_hdr_->root_page_, IX_INIT_ROOT_PAGE) << "expected the root leaf to split";
    std::array<char, PAGE_SIZE> persisted_header{};
    disk_manager->read_page(ih->fd_, IX_FILE_HDR_PAGE, persisted_header.data(), PAGE_SIZE);
    IxFileHdr disk_header;
    disk_header.deserialize(persisted_header.data());
    EXPECT_EQ(disk_header.root_page_, ih->file_hdr_->root_page_);

    // The single frame the buffer pool happened to evict, simulated exactly.
    buffer_pool_manager->flush_page(PageId{ih->fd_, ih->file_hdr_->root_page_});
    ix_manager->flush_index_header(ih.get());

    crash_and_reopen_storage(ih);
    auto reopened = open_index();
    EXPECT_TRUE(reopened->validate_structure());
}

TEST_F(IndexSmoDurabilityTest, LoggedStructuralUnwindFailsStop) {
    auto ih = open_index();
    EXPECT_EXIT(
        {
            IxIndexHandle::SmoScope scope(ih.get(), IndexWriteWalContext::LoggedRuntime(0));
            ih->note_structure_change();
        },
        ::testing::ExitedWithCode(134), "unwound before durable INDEX_SMO");
}

TEST_F(IndexSmoDurabilityTest, LoggedPreMutationUnwindReleasesTheBarrier) {
    auto ih = open_index();
    { IxIndexHandle::SmoScope scope(ih.get(), IndexWriteWalContext::LoggedRuntime(0)); }
    buffer_pool_manager->begin_index_smo(ih->GetFd());
    buffer_pool_manager->end_index_smo(ih->GetFd());
}

TEST_F(IndexSmoDurabilityTest, LoggedSplitDefersPublicationUntilWalOrderedFlush) {
    enable_wal(DurabilityMode::PROCESS_CRASH);
    auto ih = open_index();
    const std::string index_name = disk_manager->get_file_name(ih->fd_);
    EXPECT_GT(log_manager->ensure_index_binding(index_name), 0U);
    ASSERT_GT(log_manager->get_global_lsn(), 0);

    const int order = ih->file_hdr_->btree_order_;
    const page_id_t old_root_page = ih->file_hdr_->root_page_;
    for (int value = 0; value <= order; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::LoggedRuntime(0));
    }
    ASSERT_NE(ih->file_hdr_->root_page_, old_root_page) << "expected the root leaf to split";
    ASSERT_EQ(log_manager->get_global_lsn(), 2);
    const lsn_t smo_lsn = log_manager->get_global_lsn() - 1;

    EXPECT_LT(log_manager->get_durable_lsn(), smo_lsn);
    EXPECT_EQ(ih->header_dependency_.kind(), PageWriteDependency::Kind::WalLsn);
    EXPECT_EQ(ih->header_dependency_.wal_lsn(), smo_lsn);

    const PageId root_id{ih->fd_, ih->file_hdr_->root_page_};
    Page* root = buffer_pool_manager->fetch_page(root_id);
    ASSERT_NE(root, nullptr);
    {
        std::scoped_lock dirty_guard{root->dirty_latch_};
        EXPECT_EQ(root->write_dependency_.kind(), PageWriteDependency::Kind::WalLsn);
        EXPECT_EQ(root->write_dependency_.wal_lsn(), smo_lsn);
    }
    ASSERT_TRUE(buffer_pool_manager->unpin_page(root_id, false));

    bool before_write_saw_undurable_wal = false;
    bool after_write_saw_durable_wal = false;
    BufferPoolManager::set_flush_page_test_hook([&](PageId page_id, Page*) {
        if (page_id == root_id) {
            before_write_saw_undurable_wal = log_manager->get_durable_lsn() < smo_lsn;
        }
    });
    BufferPoolManager::set_flush_page_after_write_test_hook([&](PageId page_id, Page*) {
        if (page_id == root_id) {
            after_write_saw_durable_wal = log_manager->get_durable_lsn() >= smo_lsn;
        }
    });
    const bool flushed = buffer_pool_manager->flush_page(root_id);
    BufferPoolManager::set_flush_page_test_hook({});
    BufferPoolManager::set_flush_page_after_write_test_hook({});

    EXPECT_TRUE(flushed);
    EXPECT_TRUE(before_write_saw_undurable_wal);
    EXPECT_TRUE(after_write_saw_durable_wal);
    EXPECT_GE(log_manager->get_durable_lsn(), smo_lsn);
}

// The realistic version: enough random inserts through a pool far smaller than
// the tree that the buffer pool is evicting continuously, so at any instant some
// page of some recent split is on disk and some is not.
TEST_F(IndexSmoDurabilityTest, RandomInsertsSurviveACrashWithAValidStructure) {
    constexpr int kKeyCount = 6000;
    std::vector<int> insertion_order(kKeyCount);
    std::iota(insertion_order.begin(), insertion_order.end(), 0);
    std::mt19937 rng(20260727);
    std::shuffle(insertion_order.begin(), insertion_order.end(), rng);

    auto ih = open_index();
    for (const int value : insertion_order) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }

    crash_and_reopen_storage(ih);
    auto reopened = open_index();
    EXPECT_TRUE(reopened->validate_structure());
}

// Deletes route through the tree-exclusive path whenever they hit position zero,
// where they rewrite the separator key in every ancestor. The leaf and those
// ancestors have to reach disk together for the same reason a split's pages do.
TEST_F(IndexSmoDurabilityTest, SeparatorUpdatesSurviveACrashWithAValidStructure) {
    constexpr int kKeyCount = 4000;
    auto ih = open_index();
    for (int value = 0; value < kKeyCount; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }
    // TPC-C's Delivery takes MIN(no_o_id) and deletes it, so new_orders indexes
    // see nothing but position-zero deletes.
    for (int value = 0; value < kKeyCount / 2; ++value) {
        auto k = key(value);
        ASSERT_TRUE(ih->delete_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal()));
    }

    crash_and_reopen_storage(ih);
    auto reopened = open_index();
    EXPECT_TRUE(reopened->validate_structure());
}

// Emptying a leaf completely is a legal steady state, not damage: the delete
// path never merges nodes, so the leaf stays in the tree and in the leaf chain.
// The structure gate has to accept it, otherwise every index over new_orders
// gets rebuilt after every crash.
TEST_F(IndexSmoDurabilityTest, AnEmptyLeafIsNotStructuralDamage) {
    constexpr int kKeyCount = 4000;
    auto ih = open_index();
    for (int value = 0; value < kKeyCount; ++value) {
        auto k = key(value);
        ih->insert_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal());
    }
    // Every key, so the leftmost leaves end up with no keys at all and the root
    // itself is eventually an empty leaf.
    for (int value = 0; value < kKeyCount; ++value) {
        auto k = key(value);
        ASSERT_TRUE(ih->delete_entry(k.data(), Rid{1, value}, IndexWriteWalContext::TestNoWal()));
    }
    // Without this the test could pass vacuously. The old gate rejected any node
    // with `size <= 0`, so the existence of a reachable zero-key leaf is exactly
    // what used to force a rebuild.
    int empty_leaves = 0;
    {
        auto structure_guard = ih->lock_shared();
        for (page_id_t page_no = IX_INIT_ROOT_PAGE; page_no < ih->file_hdr_->num_pages_; ++page_no) {
            IxNodeHandle node;
            ih->fetch_node_into(page_no, node);
            if (node.is_leaf_page() && node.get_size() == 0) {
                ++empty_leaves;
            }
            ih->unpin_if_not_cached(node.get_page_id());
        }
    }
    ASSERT_GT(empty_leaves, 0) << "no leaf was emptied, so this test checks nothing";
    EXPECT_TRUE(ih->validate_structure()) << "an emptied leaf was reported as damage";

    crash_and_reopen_storage(ih);
    auto reopened = open_index();
    EXPECT_TRUE(reopened->validate_structure());
}

TEST(IndexSmoWalImageTest, CanonicalizesOnlyUnusedKeyAndRidSlots) {
    const IxFileHdr file_hdr = WalImageLayout();
    constexpr int num_key = 5;
    auto image = WalImagePage(file_hdr, num_key);
    const auto original = image;
    const size_t rid_begin = sizeof(IxPageHdr) + file_hdr.keys_size_;
    const size_t capacity = static_cast<size_t>(file_hdr.btree_order_ + 1);
    const size_t live_key_end = sizeof(IxPageHdr) + num_key * file_hdr.col_tot_len_;
    const size_t live_rid_end = rid_begin + num_key * sizeof(Rid);
    const size_t capacity_end = rid_begin + capacity * sizeof(Rid);

    ASSERT_TRUE(TryCanonicalizeIxPageImageForWal(file_hdr, IX_INIT_ROOT_PAGE, &image));
    EXPECT_EQ(std::memcmp(image.data(), original.data(), sizeof(IxPageHdr)), 0);
    EXPECT_EQ(std::memcmp(image.data() + sizeof(IxPageHdr), original.data() + sizeof(IxPageHdr),
                          num_key * file_hdr.col_tot_len_),
              0);
    EXPECT_EQ(std::memcmp(image.data() + rid_begin, original.data() + rid_begin, num_key * sizeof(Rid)), 0);
    EXPECT_TRUE(
        std::all_of(image.begin() + live_key_end, image.begin() + rid_begin, [](char byte) { return byte == 0; }));
    EXPECT_TRUE(
        std::all_of(image.begin() + live_rid_end, image.begin() + capacity_end, [](char byte) { return byte == 0; }));
    EXPECT_TRUE(std::equal(image.begin() + capacity_end, image.end(), original.begin() + capacity_end))
        << "bytes outside the known node layout are not canonicalization padding";
}

TEST(IndexSmoWalImageTest, HandlesFullAndLeafListHeaderPages) {
    const IxFileHdr file_hdr = WalImageLayout();
    const int capacity = file_hdr.btree_order_ + 1;
    auto full = WalImagePage(file_hdr, capacity);
    const auto full_original = full;
    ASSERT_TRUE(TryCanonicalizeIxPageImageForWal(file_hdr, IX_INIT_ROOT_PAGE, &full));
    const size_t raw_key_end = sizeof(IxPageHdr) + capacity * file_hdr.col_tot_len_;
    const size_t rid_begin = sizeof(IxPageHdr) + file_hdr.keys_size_;
    EXPECT_TRUE(std::equal(full.begin(), full.begin() + raw_key_end, full_original.begin()));
    EXPECT_TRUE(std::all_of(full.begin() + raw_key_end, full.begin() + rid_begin, [](char byte) { return byte == 0; }))
        << "the key/RID alignment gap is not a live key";
    EXPECT_TRUE(std::equal(full.begin() + rid_begin, full.end(), full_original.begin() + rid_begin));

    auto leaf_header = WalImagePage(file_hdr, 0);
    const auto leaf_header_original = leaf_header;
    ASSERT_TRUE(TryCanonicalizeIxPageImageForWal(file_hdr, IX_LEAF_HEADER_PAGE, &leaf_header));
    EXPECT_EQ(std::memcmp(leaf_header.data(), leaf_header_original.data(), sizeof(IxPageHdr)), 0);
    const size_t capacity_end = rid_begin + capacity * sizeof(Rid);
    EXPECT_TRUE(std::all_of(leaf_header.begin() + sizeof(IxPageHdr), leaf_header.begin() + capacity_end,
                            [](char byte) { return byte == 0; }));
    EXPECT_TRUE(
        std::equal(leaf_header.begin() + capacity_end, leaf_header.end(), leaf_header_original.begin() + capacity_end));
}

TEST(IndexSmoWalImageTest, InvalidLayoutsAndCountsKeepTheRawImage) {
    IxFileHdr file_hdr = WalImageLayout();
    auto expect_unchanged = [&](page_id_t page_no, std::array<char, PAGE_SIZE> image) {
        const auto original = image;
        EXPECT_FALSE(TryCanonicalizeIxPageImageForWal(file_hdr, page_no, &image));
        EXPECT_EQ(image, original);
    };

    auto negative = WalImagePage(file_hdr, 0);
    const int negative_count = -1;
    std::memcpy(negative.data() + offsetof(IxPageHdr, num_key), &negative_count, sizeof(negative_count));
    expect_unchanged(IX_INIT_ROOT_PAGE, negative);

    auto too_many = WalImagePage(file_hdr, 0);
    const int excess_count = file_hdr.btree_order_ + 2;
    std::memcpy(too_many.data() + offsetof(IxPageHdr, num_key), &excess_count, sizeof(excess_count));
    expect_unchanged(IX_INIT_ROOT_PAGE, too_many);

    expect_unchanged(IX_LEAF_HEADER_PAGE, WalImagePage(file_hdr, 1));

    file_hdr.keys_size_++;
    expect_unchanged(IX_INIT_ROOT_PAGE, WalImagePage(WalImageLayout(), 3));

    file_hdr = WalImageLayout(512, 100);
    expect_unchanged(IX_INIT_ROOT_PAGE, WalImagePage(WalImageLayout(), 0));
}

TEST(IndexSmoWalImageTest, CanonicalSlotsMateriallyReduceExistingWalEncoding) {
    const IxFileHdr file_hdr = WalImageLayout(4, 300);
    IndexSmoWalData raw;
    raw.index_file_name = "canonical_slots.idx";
    raw.index_generation = 53;
    raw.pages.resize(1);
    raw.pages[0].page_no = IX_INIT_ROOT_PAGE;
    raw.pages[0].bytes = WalImagePage(file_hdr, 100);
    raw.header.fill('h');

    IndexSmoWalData canonical = raw;
    ASSERT_TRUE(TryCanonicalizeIxPageImageForWal(file_hdr, canonical.pages[0].page_no, &canonical.pages[0].bytes));
    const auto raw_bytes = SerializeIndexSmo(raw);
    const auto canonical_bytes = SerializeIndexSmo(canonical);
    EXPECT_LT(canonical_bytes.size() * 100, raw_bytes.size() * 75);
}

TEST(IndexSmoWalCompressedTest, RoundTripsMixedRawAndRleImagesWithMaterialSizeReduction) {
    IndexSmoWalData data;
    data.index_file_name = "mixed_codec.idx";
    data.index_generation = 23;
    data.pages.resize(2);
    data.pages[0].page_no = 2;
    data.pages[1].page_no = 8;
    std::fill_n(data.pages[0].bytes.begin() + 128, 384, 's');
    uint32_t random = 0x12345678U;
    for (char& byte : data.pages[1].bytes) {
        random ^= random << 13U;
        random ^= random >> 17U;
        random ^= random << 5U;
        byte = static_cast<char>((random & 0xffU) | 1U);
    }
    data.header.fill(0);

    const std::vector<char> bytes = SerializeIndexSmo(data);
    EXPECT_EQ(read_unaligned<uint16_t>(bytes.data() + OFFSET_LOG_DATA + sizeof(uint32_t)),
              INDEX_SMO_FORMAT_COMPRESSED);

    uint32_t offset = OFFSET_LOG_DATA + kIndexSmoFixedPayloadBytes + data.index_file_name.size();
    const uint32_t first_codec_offset = offset + sizeof(page_id_t);
    EXPECT_EQ(static_cast<uint8_t>(bytes[first_codec_offset]), 1U);
    const uint32_t first_encoded_length =
        read_unaligned<uint32_t>(bytes.data() + first_codec_offset + kIndexSmoImageEnvelopeBytes - sizeof(uint32_t));
    offset = first_codec_offset + kIndexSmoImageEnvelopeBytes + first_encoded_length;
    const uint32_t second_codec_offset = offset + sizeof(page_id_t);
    EXPECT_EQ(static_cast<uint8_t>(bytes[second_codec_offset]), 0U);
    const uint32_t second_encoded_length =
        read_unaligned<uint32_t>(bytes.data() + second_codec_offset + kIndexSmoImageEnvelopeBytes - sizeof(uint32_t));
    offset = second_codec_offset + kIndexSmoImageEnvelopeBytes + second_encoded_length;
    EXPECT_EQ(static_cast<uint8_t>(bytes[offset]), 1U);

    IndexSmoWalView parsed;
    ASSERT_TRUE(ParseIndexSmoWal(IndexSmoViewOf(bytes), &parsed));
    ASSERT_EQ(parsed.page_count, 2U);
    EXPECT_EQ(parsed.index_file_name, data.index_file_name);
    EXPECT_EQ(parsed.index_generation, data.index_generation);
    EXPECT_EQ(parsed.page_no(0), 2);
    EXPECT_EQ(parsed.page_no(1), 8);
    EXPECT_EQ(std::memcmp(parsed.page_image(0), data.pages[0].bytes.data(), PAGE_SIZE), 0);
    EXPECT_EQ(std::memcmp(parsed.page_image(1), data.pages[1].bytes.data(), PAGE_SIZE), 0);
    EXPECT_EQ(std::memcmp(parsed.header_image, data.header.data(), PAGE_SIZE), 0);

    const size_t legacy_bytes = LOG_HEADER_SIZE + kIndexSmoFixedPayloadBytes + data.index_file_name.size() +
                                data.pages.size() * (sizeof(page_id_t) + PAGE_SIZE) + PAGE_SIZE + sizeof(uint32_t);
    RecordProperty("compressed_encoded_bytes", static_cast<int>(bytes.size()));
    RecordProperty("legacy_raw_bytes", static_cast<int>(legacy_bytes));
    EXPECT_LT(bytes.size() * 100, legacy_bytes * 45)
        << "the sparse page/header sample should encode to less than 45% of the legacy raw record";
}

TEST(IndexSmoWalStructuredTest, RoundTripsCanonicalPageAndRejectsLayoutMismatch) {
    const IxFileHdr layout = WalImageLayout(16, 32);
    IndexSmoWalData data;
    data.index_file_name = "structured.idx";
    data.index_generation = 73;
    data.pages.resize(1);
    data.pages[0].page_no = IX_INIT_ROOT_PAGE;
    data.pages[0].bytes = WalImagePage(layout, 20);
    data.header = WalImageHeader(layout);

    const std::vector<char> bytes = SerializeIndexSmo(data);
    ASSERT_EQ(read_unaligned<uint16_t>(bytes.data() + OFFSET_LOG_DATA + sizeof(uint32_t)),
              INDEX_SMO_FORMAT_STRUCTURED);
    IndexSmoWalView parsed;
    ASSERT_TRUE(ParseIndexSmoWal(IndexSmoViewOf(bytes), &parsed));
    for (uint32_t index = 0; index < parsed.page_count; ++index) {
        EXPECT_EQ(std::memcmp(parsed.page_image(index), data.pages[index].bytes.data(), PAGE_SIZE), 0);
    }

    std::vector<char> corrupt = bytes;
    const uint32_t layout_offset = OFFSET_LOG_DATA + kIndexSmoFixedPayloadBytes;
    const int bad_order = layout.btree_order_ + 1;
    std::memcpy(corrupt.data() + layout_offset + sizeof(int) * 1, &bad_order, sizeof(bad_order));
    RefreshIndexSmoChecksum(&corrupt);
    EXPECT_FALSE(ParseIndexSmoWal(IndexSmoViewOf(corrupt), &parsed));
}

TEST(IndexSmoWalStructuredTest, WordBitmapWinsForOfficialLikeCanonicalPageAndFailsClosed) {
    const IxFileHdr layout = WalImageLayout(4, 300);
    IndexSmoWalData data;
    data.index_file_name = "structured_bitmap.idx";
    data.index_generation = 79;
    data.pages.resize(4);
    data.pages[0].page_no = IX_INIT_ROOT_PAGE;
    data.pages[0].bytes.fill(0);
    const IxPageHdr page_hdr{.next_free_page_no = IX_NO_PAGE,
                             .parent = 7,
                             .num_key = 300,
                             .is_leaf = true,
                             .prev_leaf = 3,
                             .next_leaf = 9};
    std::memcpy(data.pages[0].bytes.data(), &page_hdr, sizeof(page_hdr));
    for (int key = 0; key < 300; ++key) {
        char* bytes = data.pages[0].bytes.data() + sizeof(IxPageHdr) + key * layout.col_tot_len_;
        const char value = (key / 2) % 2 == 0 ? 'K' : 'J';
        std::memset(bytes, value, layout.col_tot_len_);
        char* rid_bytes = data.pages[0].bytes.data() + sizeof(IxPageHdr) + layout.keys_size_ + key * sizeof(Rid);
        std::memset(rid_bytes, value, sizeof(Rid));
    }
    for (size_t index = 1; index < data.pages.size(); ++index) {
        data.pages[index].page_no = IX_INIT_ROOT_PAGE + static_cast<page_id_t>(index);
        data.pages[index].bytes = data.pages[0].bytes;
        for (int key = 0; key < 300; ++key) {
            char* bytes = data.pages[index].bytes.data() + sizeof(IxPageHdr) + key * layout.col_tot_len_;
            std::memset(bytes, 'K', layout.col_tot_len_);
            const Rid rid{123, 456};
            std::memcpy(data.pages[index].bytes.data() + sizeof(IxPageHdr) + layout.keys_size_ + key * sizeof(Rid), &rid,
                        sizeof(rid));
        }
    }
    data.header = WalImageHeader(layout);
    const std::vector<char> bytes = SerializeIndexSmo(data);
    const uint32_t image_offset = OFFSET_LOG_DATA + kIndexSmoFixedPayloadBytes + sizeof(int) * 3 +
                                  data.index_file_name.size() + sizeof(page_id_t);
    ASSERT_EQ(static_cast<uint8_t>(bytes[image_offset]), 4U);
    const size_t compressed_baseline =
        LOG_HEADER_SIZE + kIndexSmoFixedPayloadBytes + data.index_file_name.size() +
        data.pages.size() * (sizeof(page_id_t) + kIndexSmoImageEnvelopeBytes + PAGE_SIZE) +
        kIndexSmoImageEnvelopeBytes + PAGE_SIZE + sizeof(uint32_t);
    EXPECT_LE(bytes.size() * 100U, compressed_baseline * 32U);
    IndexSmoWalView parsed;
    ASSERT_TRUE(ParseIndexSmoWal(IndexSmoViewOf(bytes), &parsed));
    EXPECT_EQ(std::memcmp(parsed.page_image(0), data.pages[0].bytes.data(), PAGE_SIZE), 0);

    auto reject = [&](std::vector<char> corrupt) {
        RefreshIndexSmoChecksum(&corrupt);
        EXPECT_FALSE(ParseIndexSmoWal(IndexSmoViewOf(corrupt), &parsed));
    };
    {
        auto corrupt = bytes;
        corrupt[image_offset + sizeof(uint8_t)] = 1;
        reject(std::move(corrupt));
    }
    {
        auto corrupt = bytes;
        corrupt[image_offset + sizeof(uint8_t) * 2] = 1;
        reject(std::move(corrupt));
    }
    {
        auto corrupt = bytes;
        const uint32_t short_length = 127;
        std::memcpy(corrupt.data() + image_offset + kIndexSmoImageEnvelopeBytes - sizeof(uint32_t), &short_length,
                    sizeof(short_length));
        reject(std::move(corrupt));
    }
    {
        auto corrupt = bytes;
        const uint32_t misaligned_length = 129;
        std::memcpy(corrupt.data() + image_offset + kIndexSmoImageEnvelopeBytes - sizeof(uint32_t), &misaligned_length,
                    sizeof(misaligned_length));
        reject(std::move(corrupt));
    }
    {
        auto corrupt = bytes;
        corrupt[image_offset + kIndexSmoImageEnvelopeBytes] ^= 1;
        reject(std::move(corrupt));
    }
    {
        IndexSmoWalData invalid = data;
        const int bad_num_key = layout.btree_order_ + 2;
        std::memcpy(invalid.pages[1].bytes.data() + offsetof(IxPageHdr, num_key), &bad_num_key, sizeof(bad_num_key));
        EXPECT_FALSE(ParseIndexSmoWal(IndexSmoViewOf(SerializeIndexSmo(invalid)), &parsed));
    }
    {
        IndexSmoWalData invalid = data;
        invalid.pages[1].bytes[offsetof(IxPageHdr, is_leaf)] = 2;
        EXPECT_FALSE(ParseIndexSmoWal(IndexSmoViewOf(SerializeIndexSmo(invalid)), &parsed));
    }
}

TEST(IndexSmoWalStructuredTest, SelectsRawRleAndBitmapWithSafeCursorParity) {
    const IxFileHdr layout = WalImageLayout(16, 32);
    const std::array<char, PAGE_SIZE> header = WalImageHeader(layout);
    const size_t used = sizeof(IxPageHdr) + static_cast<size_t>(layout.keys_size_) +
                        static_cast<size_t>(layout.btree_order_ + 1) * sizeof(Rid);
    ASSERT_LT(used + 1024, PAGE_SIZE);

    auto make_data = [&](std::string_view name, const std::array<char, PAGE_SIZE>& page) {
        IndexSmoWalData data;
        data.index_file_name = name;
        data.index_generation = 100 + static_cast<uint64_t>(name.size());
        data.pages.resize(1);
        data.pages[0].page_no = IX_INIT_ROOT_PAGE;
        data.pages[0].bytes = page;
        data.header = header;
        return data;
    };

    std::array<char, PAGE_SIZE> raw = WalImagePage(layout, 1, 0);
    uint32_t random = 0x12345678U;
    for (size_t index = sizeof(IxPageHdr); index < raw.size(); ++index) {
        random ^= random << 13U;
        random ^= random >> 17U;
        random ^= random << 5U;
        raw[index] = static_cast<char>((random & 0xffU) | 1U);
    }
    ExpectStructuredCodecRoundTripAndFailClosed(make_data("structured_raw.idx", raw), 0);

    // num_key=0 makes the structured transform identical to the raw page, so
    // ordinary RLE wins its strict tie-break without weakening page validity.
    ExpectStructuredCodecRoundTripAndFailClosed(make_data("structured_rle.idx", WalImagePage(layout, 0, 0)), 1);

    std::array<char, PAGE_SIZE> bitmap = WalImagePage(layout, 1, 0);
    for (size_t index = used + (4 - used % 4) % 4; index < bitmap.size(); index += 4) {
        bitmap[index] = static_cast<char>('b');
    }
    ExpectStructuredCodecRoundTripAndFailClosed(make_data("structured_bitmap.idx", bitmap), 3);
}

TEST(IndexSmoWalStructuredTest, EnvOptOutUsesCompressedFormatInFreshProcess) {
    const char* opt_out = std::getenv("RMDB_INDEX_SMO_V3");
    if (opt_out == nullptr || std::strcmp(opt_out, "0") != 0) {
        const std::string command = "RMDB_INDEX_SMO_V3=0 /proc/" + std::to_string(getpid()) +
                                    "/exe --gtest_filter=IndexSmoWalStructuredTest.EnvOptOutUsesCompressedFormatInFreshProcess";
        EXPECT_EQ(std::system(command.c_str()), 0);
        return;
    }

    const IxFileHdr layout = WalImageLayout(16, 32);
    IndexSmoWalData data;
    data.index_file_name = "structured_opt_out.idx";
    data.index_generation = 83;
    data.pages.resize(1);
    data.pages[0].page_no = IX_INIT_ROOT_PAGE;
    data.pages[0].bytes = WalImagePage(layout, 20);
    data.header = WalImageHeader(layout);
    const std::vector<char> bytes = SerializeIndexSmo(data);
    ASSERT_EQ(read_unaligned<uint16_t>(bytes.data() + OFFSET_LOG_DATA + sizeof(uint32_t)),
              INDEX_SMO_FORMAT_COMPRESSED);
    IndexSmoWalView parsed;
    ASSERT_TRUE(ParseIndexSmoWal(IndexSmoViewOf(bytes), &parsed));
    EXPECT_EQ(std::memcmp(parsed.page_image(0), data.pages[0].bytes.data(), PAGE_SIZE), 0);
}

TEST(IndexSmoWalCompressedTest, ParsesLegacyRawAlongsideCompressed) {
    IndexSmoWalData data;
    data.index_file_name = "legacy.idx";
    data.index_generation = 31;
    data.pages.resize(1);
    data.pages[0].page_no = 4;
    std::iota(data.pages[0].bytes.begin(), data.pages[0].bytes.end(), static_cast<char>(0));
    data.header.fill('h');

    const std::vector<char> legacy = SerializeLegacyIndexSmo(data);
    IndexSmoWalView parsed;
    ASSERT_TRUE(ParseIndexSmoWal(IndexSmoViewOf(legacy), &parsed));
    ASSERT_EQ(parsed.page_count, 1U);
    EXPECT_EQ(parsed.page_no(0), 4);
    EXPECT_EQ(std::memcmp(parsed.page_image(0), data.pages[0].bytes.data(), PAGE_SIZE), 0);
    EXPECT_EQ(std::memcmp(parsed.header_image, data.header.data(), PAGE_SIZE), 0);

    const std::vector<char> current = SerializeIndexSmo(data);
    ASSERT_TRUE(ParseIndexSmoWal(IndexSmoViewOf(current), &parsed));
    EXPECT_EQ(std::memcmp(parsed.page_image(0), data.pages[0].bytes.data(), PAGE_SIZE), 0);
}

TEST(IndexSmoWalCursorTest, InspectDecodeCatalogAndExactArenasPreserveWrapperBytes) {
    const IxFileHdr file_hdr = WalImageLayout(16, 32);
    IndexSmoWalData data;
    data.index_file_name = "cursor.idx";
    data.index_generation = 91;
    data.pages.resize(2);
    data.pages[0].page_no = IX_INIT_ROOT_PAGE;
    data.pages[0].bytes = WalImagePage(file_hdr, 12);
    data.pages[1].page_no = IX_INIT_ROOT_PAGE + 1;
    data.pages[1].bytes = WalImagePage(file_hdr, 8, 'q');
    data.header = WalImageHeader(file_hdr);
    const std::vector<char> bytes = SerializeIndexSmo(data);

    IndexSmoWalLayout layout;
    ASSERT_TRUE(InspectIndexSmoWal(IndexSmoViewOf(bytes), &layout));
    EXPECT_EQ(layout.page_count(), data.pages.size());
    EXPECT_EQ(layout.generation(), data.index_generation);
    EXPECT_GT(layout.decoded_count(), 0U);

    std::vector<IndexSmoWalView::Page> catalogue(layout.page_count());
    ASSERT_TRUE(CopyIndexSmoPageCatalog(IndexSmoViewOf(bytes), catalogue.data(), catalogue.size()));
    EXPECT_EQ(catalogue[0].page_no, data.pages[0].page_no);
    EXPECT_EQ(catalogue[1].page_no, data.pages[1].page_no);
    EXPECT_EQ(catalogue[0].image, nullptr);

    std::vector<IndexSmoWalView::Page> pages(layout.page_count());
    std::vector<std::array<char, PAGE_SIZE>> decoded(layout.decoded_count());
    IndexSmoWalDecodedView view;
    ASSERT_TRUE(DecodeIndexSmoWal(IndexSmoViewOf(bytes), {pages.data(), pages.size(), decoded.data(), decoded.size()},
                                  &view));
    EXPECT_EQ(view.index_file_name, data.index_file_name);
    EXPECT_EQ(std::memcmp(view.pages[0].image, data.pages[0].bytes.data(), PAGE_SIZE), 0);
    EXPECT_EQ(std::memcmp(view.header_image, data.header.data(), PAGE_SIZE), 0);

    IndexSmoWalDecodedView unchanged;
    unchanged.index_file_name = "unchanged";
    ASSERT_FALSE(DecodeIndexSmoWal(IndexSmoViewOf(bytes),
                                   {pages.data(), pages.size() - 1, decoded.data(), decoded.size()}, &unchanged));
    EXPECT_EQ(unchanged.index_file_name, "unchanged");

    IndexSmoWalView wrapped;
    ASSERT_TRUE(ParseIndexSmoWal(IndexSmoViewOf(bytes), &wrapped));
    EXPECT_EQ(wrapped.index_file_name, view.index_file_name);
    EXPECT_EQ(wrapped.index_generation, view.index_generation);
    EXPECT_EQ(std::memcmp(wrapped.page_image(1), view.pages[1].image, PAGE_SIZE), 0);
}

TEST(IndexSmoWalCursorTest, SafeDecodeFreshlyInspectsMutatedBytesAndKeepsArenasUntouchedOnFailure) {
    const IxFileHdr file_hdr = WalImageLayout(16, 32);
    IndexSmoWalData data;
    data.index_file_name = "cursor_rebind.idx";
    data.index_generation = 94;
    data.pages.resize(1);
    data.pages[0].page_no = IX_INIT_ROOT_PAGE;
    data.pages[0].bytes = WalImagePage(file_hdr, 12);
    data.header = WalImageHeader(file_hdr);
    std::vector<char> bytes = SerializeIndexSmo(data);
    IndexSmoWalLayout layout;
    ASSERT_TRUE(InspectIndexSmoWal(IndexSmoViewOf(bytes), &layout));

    std::array<IndexSmoWalView::Page, 1> catalogue{{{777, reinterpret_cast<const char*>(0x1)}}};
    EXPECT_FALSE(CopyIndexSmoPageCatalog(IndexSmoViewOf(bytes), catalogue.data(), 0));
    EXPECT_EQ(catalogue[0].page_no, 777);
    EXPECT_EQ(catalogue[0].image, reinterpret_cast<const char*>(0x1));

    std::array<IndexSmoWalView::Page, 1> pages{{{778, reinterpret_cast<const char*>(0x2)}}};
    std::array<std::array<char, PAGE_SIZE>, 2> decoded{};
    decoded[0].fill('a');
    IndexSmoWalDecodedView view;
    view.index_file_name = "unchanged";
    EXPECT_FALSE(DecodeIndexSmoWal(IndexSmoViewOf(bytes), {pages.data(), pages.size(), decoded.data(), 0},
                                            &view));
    EXPECT_EQ(pages[0].page_no, 778);
    EXPECT_EQ(pages[0].image, reinterpret_cast<const char*>(0x2));
    EXPECT_EQ(decoded[0][0], 'a');
    EXPECT_EQ(view.index_file_name, "unchanged");

    // Same allocation and length, but a different page identity and a valid
    // new CRC: safe Copy performs a fresh inspection rather than consulting a
    // caller-controlled previous layout.
    const uint32_t page_offset = OFFSET_LOG_DATA + kIndexSmoFixedPayloadBytes + kIndexSmoStructuredLayoutBytes +
                                 static_cast<uint32_t>(data.index_file_name.size());
    const page_id_t changed_page = IX_INIT_ROOT_PAGE + 9;
    std::memcpy(bytes.data() + page_offset, &changed_page, sizeof(changed_page));
    RefreshIndexSmoChecksum(&bytes);
    EXPECT_TRUE(CopyIndexSmoPageCatalog(IndexSmoViewOf(bytes), catalogue.data(), catalogue.size()));
    EXPECT_EQ(catalogue[0].page_no, changed_page);

    // A same-address legacy-header mutation is also decoded from the fresh
    // record, never authorized by the earlier opaque inspection result.
    bytes = SerializeLegacyIndexSmo(data);
    ASSERT_TRUE(InspectIndexSmoWal(IndexSmoViewOf(bytes), &layout));
    const uint32_t header_offset = OFFSET_LOG_DATA + kIndexSmoFixedPayloadBytes +
                                   static_cast<uint32_t>(data.index_file_name.size()) + sizeof(page_id_t) + PAGE_SIZE;
    bytes[header_offset + 64] ^= 1;
    RefreshIndexSmoChecksum(&bytes);
    EXPECT_TRUE(DecodeIndexSmoWal(IndexSmoViewOf(bytes), {pages.data(), pages.size(), decoded.data(), decoded.size()},
                                  &view));
}

TEST(IndexSmoWalCursorTest, ParseInspectsOnceBeforeTrustedDecode) {
    IndexSmoWalData data;
    data.index_file_name = "crc_once.idx";
    data.index_generation = 95;
    data.pages.resize(1);
    data.pages[0].page_no = 3;
    data.pages[0].bytes.fill(0);
    data.header.fill(0);
    const std::vector<char> bytes = SerializeIndexSmo(data);
    ResetIndexSmoCrc32CallCountForTest();
    ResetIndexSmoImageDecodeCallCountForTest();
    IndexSmoWalView parsed;
    ASSERT_TRUE(ParseIndexSmoWal(IndexSmoViewOf(bytes), &parsed));
    EXPECT_EQ(IndexSmoCrc32CallCountForTest(), 1U);
    EXPECT_EQ(IndexSmoImageDecodeCallCountForTest(), 4U);
}

TEST(IndexSmoWalCursorTest, AnalysisHelperCopiesOwnedMetadataAcrossPersistentFormatsWithOneInspection) {
    IndexSmoWalData legacy;
    legacy.index_file_name = "analysis_legacy_raw.idx";
    legacy.index_generation = 101;
    legacy.pages.resize(2);
    legacy.pages[0].page_no = 4;
    legacy.pages[1].page_no = 9;
    std::iota(legacy.pages[0].bytes.begin(), legacy.pages[0].bytes.end(), static_cast<char>(0));
    legacy.pages[1].bytes.fill('p');
    legacy.header.fill('h');
    ExpectAnalysisCatalog(legacy, SerializeLegacyIndexSmo(legacy));

    IndexSmoWalData current = legacy;
    current.index_file_name = "analysis_compressed.idx";
    current.index_generation = 102;
    current.pages[0].bytes.fill(0);
    current.pages[1].bytes.fill(0);
    current.header.fill(0);
    ExpectAnalysisCatalog(current, SerializeIndexSmo(current));

    const IxFileHdr layout = WalImageLayout(16, 32);
    current.index_file_name = "analysis_structured.idx";
    current.index_generation = 103;
    current.pages[0].page_no = IX_INIT_ROOT_PAGE;
    current.pages[1].page_no = IX_INIT_ROOT_PAGE + 1;
    current.pages[0].bytes = WalImagePage(layout, 12);
    current.pages[1].bytes = WalImagePage(layout, 8, 'q');
    current.header = WalImageHeader(layout);
    ExpectAnalysisCatalog(current, SerializeIndexSmo(current));
}

TEST(IndexSmoWalCursorTest, AnalysisHelperFailsBeforePublishingForCapacityAliasAndMalformedBytes) {
    IndexSmoWalData data;
    data.index_file_name = "analysis_atomic.idx";
    data.index_generation = 104;
    data.pages.resize(2);
    data.pages[0].page_no = 4;
    data.pages[1].page_no = 8;
    data.pages[0].bytes.fill(0);
    data.pages[1].bytes.fill(0);
    data.header.fill(0);
    std::vector<char> bytes = SerializeIndexSmo(data);

    std::vector<char> name(data.index_file_name.size(), 'n');
    std::array<page_id_t, 2> pages{{701, 702}};
    IndexSmoWalAnalysis analysis{78, 79, 80};
    const auto expect_unchanged = [&] {
        EXPECT_EQ(name, std::vector<char>(data.index_file_name.size(), 'n'));
        EXPECT_EQ(pages, (std::array<page_id_t, 2>{{701, 702}}));
        EXPECT_EQ(analysis.index_name_bytes, 78U);
        EXPECT_EQ(analysis.page_count, 79U);
        EXPECT_EQ(analysis.index_generation, 80U);
    };

    EXPECT_FALSE(AnalyzeIndexSmoWal(IndexSmoViewOf(bytes),
                                    {name.data(), name.size() - 1, pages.data(), pages.size()}, &analysis));
    expect_unchanged();
    EXPECT_FALSE(AnalyzeIndexSmoWal(IndexSmoViewOf(bytes),
                                    {name.data(), name.size(), pages.data(), pages.size() - 1}, &analysis));
    expect_unchanged();

    // An arena that aliases the supposedly immutable record is rejected
    // before either destination or the scalar result can be touched.
    EXPECT_FALSE(AnalyzeIndexSmoWal(IndexSmoViewOf(bytes),
                                    {bytes.data() + OFFSET_LOG_DATA, name.size(), pages.data(), pages.size()},
                                    &analysis));
    expect_unchanged();

    const uint32_t codec_offset =
        OFFSET_LOG_DATA + kIndexSmoFixedPayloadBytes + data.index_file_name.size() + sizeof(page_id_t);
    bytes[codec_offset] = static_cast<char>(0xff);
    RefreshIndexSmoChecksum(&bytes);
    EXPECT_FALSE(AnalyzeIndexSmoWal(IndexSmoViewOf(bytes),
                                    {name.data(), name.size(), pages.data(), pages.size()}, &analysis));
    expect_unchanged();
}

TEST(IndexSmoWalCursorTest, AnalysisHelperFreshlyBindsSameAddressPageIdentity) {
    IndexSmoWalData data;
    data.index_file_name = "analysis_rebind.idx";
    data.index_generation = 105;
    data.pages.resize(2);
    data.pages[0].page_no = 4;
    data.pages[1].page_no = 8;
    data.pages[0].bytes.fill(0);
    data.pages[1].bytes.fill(0);
    data.header.fill(0);
    std::vector<char> bytes = SerializeIndexSmo(data);
    std::vector<char> name(data.index_file_name.size());
    std::array<page_id_t, 2> pages{};
    IndexSmoWalAnalysis analysis;
    ASSERT_TRUE(AnalyzeIndexSmoWal(IndexSmoViewOf(bytes),
                                   {name.data(), name.size(), pages.data(), pages.size()}, &analysis));
    EXPECT_EQ(pages[0], 4);

    const uint32_t first_page_offset =
        OFFSET_LOG_DATA + kIndexSmoFixedPayloadBytes + static_cast<uint32_t>(data.index_file_name.size());
    const page_id_t changed_page = 5;
    std::memcpy(bytes.data() + first_page_offset, &changed_page, sizeof(changed_page));
    RefreshIndexSmoChecksum(&bytes);
    ASSERT_TRUE(AnalyzeIndexSmoWal(IndexSmoViewOf(bytes),
                                   {name.data(), name.size(), pages.data(), pages.size()}, &analysis));
    EXPECT_EQ(pages[0], changed_page);
    EXPECT_EQ(pages[1], 8);
}

TEST(IndexSmoWalCursorTest, InspectFailureLeavesLayoutUnchanged) {
    IndexSmoWalData data;
    data.index_file_name = "legacy_cursor.idx";
    data.index_generation = 92;
    data.pages.resize(1);
    data.pages[0].page_no = 4;
    data.header.fill('h');
    std::vector<char> bytes = SerializeLegacyIndexSmo(data);
    bytes.pop_back();
    IndexSmoWalLayout layout;
    EXPECT_FALSE(InspectIndexSmoWal(IndexSmoViewOf(bytes), &layout));
    EXPECT_EQ(layout.generation(), 0U);
}

TEST(IndexSmoWalCompressedTest, RejectsInvalidEnvelopeAndMalformedRleAfterChecksumValidation) {
    IndexSmoWalData data;
    data.index_file_name = "corrupt_compressed.idx";
    data.index_generation = 47;
    data.pages.resize(1);
    data.pages[0].page_no = 3;
    data.pages[0].bytes.fill(0);
    data.header.fill(0);
    const std::vector<char> valid = SerializeIndexSmo(data);
    const uint32_t codec_offset =
        OFFSET_LOG_DATA + kIndexSmoFixedPayloadBytes + data.index_file_name.size() + sizeof(page_id_t);

    auto expect_rejected = [](std::vector<char> bytes) {
        RefreshIndexSmoChecksum(&bytes);
        IndexSmoWalLayout layout;
        EXPECT_FALSE(InspectIndexSmoWal(IndexSmoViewOf(bytes), &layout));
        IndexSmoWalView parsed;
        EXPECT_FALSE(ParseIndexSmoWal(IndexSmoViewOf(bytes), &parsed));
    };

    {
        std::vector<char> bytes = valid;
        const uint16_t bad_format_tag = INDEX_SMO_FORMAT_STRUCTURED + 1;
        std::memcpy(bytes.data() + OFFSET_LOG_DATA + sizeof(uint32_t), &bad_format_tag, sizeof(bad_format_tag));
        expect_rejected(std::move(bytes));
    }
    {
        std::vector<char> bytes = valid;
        const uint16_t bad_flags = INDEX_SMO_FLAG_HEADER_IMAGE | 2U;
        std::memcpy(bytes.data() + OFFSET_LOG_DATA + sizeof(uint32_t) + sizeof(uint16_t), &bad_flags,
                    sizeof(bad_flags));
        expect_rejected(std::move(bytes));
    }
    {
        std::vector<char> bytes = valid;
        bytes[codec_offset] = 9;
        expect_rejected(std::move(bytes));
    }
    {
        std::vector<char> bytes = valid;
        bytes[codec_offset + sizeof(uint8_t)] = 1;
        expect_rejected(std::move(bytes));
    }
    {
        std::vector<char> bytes = valid;
        const uint32_t bad_raw_length = PAGE_SIZE - 1;
        std::memcpy(bytes.data() + codec_offset + sizeof(uint8_t) * 2 + sizeof(uint16_t), &bad_raw_length,
                    sizeof(bad_raw_length));
        expect_rejected(std::move(bytes));
    }
    {
        std::vector<char> bytes = valid;
        const uint32_t zero_encoded_length = 0;
        std::memcpy(bytes.data() + codec_offset + kIndexSmoImageEnvelopeBytes - sizeof(uint32_t), &zero_encoded_length,
                    sizeof(zero_encoded_length));
        expect_rejected(std::move(bytes));
    }
    {
        std::vector<char> bytes = valid;
        bytes[codec_offset + kIndexSmoImageEnvelopeBytes] = 0;
        expect_rejected(std::move(bytes));
    }
    {
        // A legal envelope whose RLE body overruns its declared input used to
        // pass Inspect and fail only after Decode had begun writing an arena.
        std::vector<char> bytes = valid;
        bytes[codec_offset + kIndexSmoImageEnvelopeBytes] = 127;
        expect_rejected(std::move(bytes));
    }
}

} // namespace
