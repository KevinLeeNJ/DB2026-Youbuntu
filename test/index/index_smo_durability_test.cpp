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

#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "recovery/index_smo_log.h"
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_meta.h"

namespace {

// Small enough that a few thousand keys force continuous eviction, which is what
// makes the on-disk image of an in-flight SMO partial in the first place.
constexpr size_t kSmallPoolFrames = 48;
// 64-byte keys keep btree_order at 55, so a few thousand keys build a
// three-level tree spanning far more pages than the pool holds.
constexpr int kKeyLen = 64;
constexpr uint32_t kIndexSmoFixedPayloadBytes =
    sizeof(uint32_t) + sizeof(uint16_t) * 2 + sizeof(uint32_t) * 4 + sizeof(uint64_t);
constexpr uint32_t kIndexSmoImageEnvelopeBytes = sizeof(uint8_t) * 2 + sizeof(uint16_t) + sizeof(uint32_t) * 2;

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
    AppendTestScalar(&bytes, INDEX_SMO_VERSION_V1);
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
        // disk. Reaching into path2fd_ keeps this working when a test aborts
        // early, so a teardown error can never mask the real failure.
        if (auto it = disk_manager->path2fd_.find(index_name); it != disk_manager->path2fd_.end()) {
            disk_manager->close_file(it->second);
        }
        if (disk_manager->is_file(index_name)) {
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

TEST(IndexSmoWalV2Test, RoundTripsMixedRawAndRleImagesWithMaterialSizeReduction) {
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
    EXPECT_EQ(read_unaligned<uint16_t>(bytes.data() + OFFSET_LOG_DATA + sizeof(uint32_t)), INDEX_SMO_VERSION_V2);

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
    RecordProperty("v2_encoded_bytes", static_cast<int>(bytes.size()));
    RecordProperty("v1_raw_bytes", static_cast<int>(legacy_bytes));
    EXPECT_LT(bytes.size() * 100, legacy_bytes * 45)
        << "the sparse page/header sample should encode to less than 45% of the v1 full-image record";
}

TEST(IndexSmoWalV2Test, ParsesLegacyV1AlongsideV2) {
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

TEST(IndexSmoWalV2Test, RejectsInvalidEnvelopeAndMalformedRleAfterChecksumValidation) {
    IndexSmoWalData data;
    data.index_file_name = "corrupt_v2.idx";
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
        IndexSmoWalView parsed;
        EXPECT_FALSE(ParseIndexSmoWal(IndexSmoViewOf(bytes), &parsed));
    };

    {
        std::vector<char> bytes = valid;
        const uint16_t bad_version = INDEX_SMO_VERSION_V2 + 1;
        std::memcpy(bytes.data() + OFFSET_LOG_DATA + sizeof(uint32_t), &bad_version, sizeof(bad_version));
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
}

} // namespace
