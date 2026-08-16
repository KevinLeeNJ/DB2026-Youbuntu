#include "deltakernel/delta_database.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <set>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>

#include "common/csv.h"
#include "system/sm_meta.h"

namespace deltakernel {
namespace {
constexpr const char* kCatalog = "DELTA_CATALOG";
constexpr const char* kCatalogMagic = "DELTAKERNEL";
constexpr size_t kMaxRowBytes = 1U << 20;
constexpr size_t kMaxColumns = 256;
constexpr size_t kMaxTables = 65536;
constexpr uintmax_t kMaxCatalogBytes = 16U << 20;
constexpr size_t kMaxJoinMaterializedBytes = 64U << 20;
constexpr size_t kMaxJoinMaterializedRows = 1U << 20;
constexpr uint64_t kSidecarMagic = 0x58444941544c4544ULL; // DELTAIDX
constexpr uint64_t kMaxSidecarBytes = 1ULL << 34;
constexpr size_t kCommitBatchSize = 32;
constexpr size_t kCommitQueueLimit = 128;

struct SidecarHeader {
    uint64_t magic;
    uint32_t header_bytes;
    uint32_t entry_bytes;
    uint32_t table_id;
    uint32_t constraint_id;
    uint64_t generation;
    uint64_t count;
    uint64_t total_bytes;
    uint64_t key_bytes;
    uint32_t entries_crc;
    uint32_t keys_crc;
    uint32_t header_crc;
};
static_assert(sizeof(SidecarHeader) == 72);

constexpr std::array<uint32_t, 256> MakeCrc32Table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t value = 0; value < table.size(); ++value) {
        uint32_t crc = value;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1U) ^ (0xedb88320U & (0U - (crc & 1U)));
        table[value] = crc;
    }
    return table;
}
constexpr auto kCrc32Table = MakeCrc32Table();

uint32_t UpdateCrc32(uint32_t crc, const uint8_t* data, size_t size) {
    for (size_t n = 0; n < size; ++n)
        crc = (crc >> 8U) ^ kCrc32Table[(crc ^ data[n]) & 0xffU];
    return crc;
}

uint32_t Crc32(const void* data, size_t size) {
    return ~UpdateCrc32(0xffffffffU, static_cast<const uint8_t*>(data), size);
}

uint32_t Crc32(const std::string& bytes) {
    return Crc32(bytes.data(), bytes.size());
}

template <typename T> void PutLe(std::vector<uint8_t>& out, T value) {
    using U = typename std::make_unsigned<T>::type;
    U bits = static_cast<U>(value);
    for (size_t i = 0; i < sizeof(T); ++i)
        out.push_back(static_cast<uint8_t>(bits >> (i * 8)));
}

template <typename T> void PutBe(std::vector<uint8_t>& out, T value) {
    using U = typename std::make_unsigned<T>::type;
    const U bits = static_cast<U>(value);
    for (size_t i = sizeof(T); i-- > 0;)
        out.push_back(static_cast<uint8_t>(bits >> (i * 8)));
}

std::vector<uint8_t> PrefixSuccessor(std::vector<uint8_t> key) {
    while (!key.empty() && key.back() == 0xff)
        key.pop_back();
    if (key.empty())
        return {};
    ++key.back();
    return key;
}

template <typename T> T GetLe(const std::vector<uint8_t>& bytes, size_t& offset) {
    using U = typename std::make_unsigned<T>::type;
    if (offset > bytes.size() || bytes.size() - offset < sizeof(T))
        throw std::runtime_error("truncated Delta row");
    U value = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
        value |= static_cast<U>(bytes[offset++]) << (i * 8);
    return static_cast<T>(value);
}

template <typename... T> bool ParseLine(const std::string& line, T&... values) {
    std::istringstream input(line);
    std::string extra;
    if (!(input >> ... >> values))
        return false;
    return !(input >> extra);
}

bool CompareResult(int comparison, ast::SvCompOp op) {
    switch (op) {
    case ast::SV_OP_EQ:
        return comparison == 0;
    case ast::SV_OP_NE:
        return comparison != 0;
    case ast::SV_OP_LT:
        return comparison < 0;
    case ast::SV_OP_GT:
        return comparison > 0;
    case ast::SV_OP_LE:
        return comparison <= 0;
    case ast::SV_OP_GE:
        return comparison >= 0;
    default:
        return false;
    }
}

void AppendOverlay(DeltaOverlay& overlay, DeltaOverlayKey key, uint64_t local_id) {
    overlay[std::move(key)].push_back(local_id);
}

void AppendOverlay(CommittedOverlay& overlay, DeltaOverlayKey key, uint64_t local_id) {
    overlay.emplace(std::move(key), std::vector<uint64_t>{local_id});
}

} // namespace

DeltaDatabase::DeltaDatabase(epoch_si_poc::CheckpointDb db) : db_(std::move(db)) {}

void DeltaDatabase::RequireUsable() const {
    if (poisoned_)
        throw std::logic_error("Delta database is poisoned; reopen required");
}

bool DeltaDatabase::IsDeltaDirectory(const std::string& directory) {
    std::error_code error;
    if (!std::filesystem::is_directory(directory, error))
        return false;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        const std::string name = entry.path().filename().string();
        if (name == std::string(kCatalog) + ".tmp" || name == "MANIFEST" || name == "MANIFEST.tmp" ||
            name.rfind("base.", 0) == 0 || name.rfind("db.log.", 0) == 0 || name.rfind("wal.", 0) == 0)
            return true;
    }
    return false;
}

std::unique_ptr<DeltaDatabase> DeltaDatabase::Create(const std::string& directory) {
    auto result = std::unique_ptr<DeltaDatabase>(new DeltaDatabase(epoch_si_poc::CheckpointDb::Create(directory, {})));
    result->directory_ = directory;
    result->SaveCatalog({}, 1, 1, 1);
    return result;
}

std::unique_ptr<DeltaDatabase> DeltaDatabase::Open(const std::string& directory) {
    auto result = std::unique_ptr<DeltaDatabase>(new DeltaDatabase(epoch_si_poc::CheckpointDb::Open(directory)));
    result->directory_ = directory;
    result->LoadCatalog();
    for (const auto& [name, schema] : result->tables_) {
        bool rebuild = false;
        for (const auto& index : schema.indexes)
            rebuild = !result->ValidateSidecar(schema, index) || rebuild;
        if (rebuild)
            result->RebuildSidecars(schema);
    }
    result->db_.engine().VisitLatestVersions([&](epoch_si_poc::RowId id, const epoch_si_poc::RowImage& image) {
        if (const TableSchema* schema = result->TableById(id.table_id))
            result->AddOverlay(result->overlay_, *schema, result->DecodeRow(*schema, image), id);
    });
    return result;
}

void DeltaDatabase::SaveCatalog(const Catalog& tables, epoch_si_poc::TableId next_table_id,
                                epoch_si_poc::ConstraintId next_constraint_id, uint64_t catalog_generation) {
    RequireUsable();
    if (fail_catalog_save_for_test_)
        throw std::runtime_error("injected Delta catalog save failure");
    const std::string temp = directory_ + "/" + kCatalog + ".tmp";
    std::ostringstream body;
    body << kCatalogMagic << '\n'
         << catalog_generation << ' ' << next_table_id << ' ' << next_constraint_id << ' ' << tables.size() << '\n';
    for (const auto& [name, table] : tables) {
        body << "TABLE " << table.id << ' ' << table.version << ' ' << table.name << ' ' << table.columns.size() << ' '
             << table.indexes.size() << '\n';
        for (const Column& column : table.columns)
            body << "COLUMN " << static_cast<unsigned>(column.type) << ' ' << column.length << ' ' << column.nullable
                 << ' ' << column.name << '\n';
        for (const Index& index : table.indexes) {
            body << "INDEX " << index.constraint_id << ' ' << index.unique << ' ' << index.columns.size();
            for (uint32_t column : index.columns)
                body << ' ' << column;
            body << '\n';
        }
    }
    const std::string bytes = body.str();
    const std::string footer = "CRC32 " + std::to_string(Crc32(bytes)) + '\n';
    if (bytes.size() + footer.size() > kMaxCatalogBytes)
        throw std::runtime_error("Delta catalog exceeds limit");
    std::ofstream output(temp, std::ios::binary | std::ios::trunc);
    if (!output)
        throw std::runtime_error("open Delta catalog");
    output << bytes << footer;
    output.flush();
    if (!output)
        throw std::runtime_error("write Delta catalog");
    output.close();
    const int catalog_fd = ::open(temp.c_str(), O_RDONLY | O_CLOEXEC);
    if (catalog_fd < 0 || ::fdatasync(catalog_fd) != 0) {
        if (catalog_fd >= 0)
            ::close(catalog_fd);
        throw std::runtime_error("sync Delta catalog");
    }
    ::close(catalog_fd);
    bool renamed = false;
    try {
        std::filesystem::rename(temp, directory_ + "/" + kCatalog);
        renamed = true;
        if (fail_catalog_post_rename_for_test_)
            throw std::runtime_error("injected post-rename Delta catalog failure");
        const int directory_fd = ::open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (directory_fd < 0 || ::fsync(directory_fd) != 0) {
            if (directory_fd >= 0)
                ::close(directory_fd);
            throw std::runtime_error("sync Delta catalog directory");
        }
        ::close(directory_fd);
    } catch (...) {
        if (renamed)
            poisoned_ = true;
        throw;
    }
}

void DeltaDatabase::LoadCatalog() {
    std::error_code size_error;
    const uintmax_t catalog_size = std::filesystem::file_size(directory_ + "/" + kCatalog, size_error);
    if (size_error || catalog_size > kMaxCatalogBytes)
        throw std::runtime_error("invalid Delta catalog size");
    std::ifstream file(directory_ + "/" + kCatalog, std::ios::binary);
    if (!file)
        throw std::runtime_error("open Delta catalog");
    const std::string bytes{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
    const size_t footer = bytes.rfind("\nCRC32 ");
    if (footer == std::string::npos || bytes.empty() || bytes.back() != '\n')
        throw std::runtime_error("invalid Delta catalog checksum footer");
    const std::string body = bytes.substr(0, footer + 1);
    const std::string footer_line = bytes.substr(footer + 1, bytes.size() - footer - 2);
    std::string checksum_tag;
    uint32_t checksum = 0;
    if (!ParseLine(footer_line, checksum_tag, checksum) || checksum_tag != "CRC32" || checksum != Crc32(body))
        throw std::runtime_error("invalid Delta catalog checksum");
    std::istringstream input(body);
    std::string line;
    if (!std::getline(input, line) || line != kCatalogMagic || !std::getline(input, line))
        throw std::runtime_error("invalid Delta catalog");
    uint64_t generation = 0;
    epoch_si_poc::TableId next_table = 0;
    epoch_si_poc::ConstraintId next_constraint = 0;
    size_t table_count = 0;
    if (!ParseLine(line, generation, next_table, next_constraint, table_count) || generation == 0 || next_table == 0 ||
        next_constraint == 0 || table_count > kMaxTables)
        throw std::runtime_error("invalid Delta catalog header");
    Catalog candidate;
    std::set<epoch_si_poc::TableId> table_ids;
    std::set<epoch_si_poc::ConstraintId> constraint_ids;
    epoch_si_poc::TableId max_table = 0;
    epoch_si_poc::ConstraintId max_constraint = 0;
    for (size_t t = 0; t < table_count; ++t) {
        std::string tag;
        TableSchema table{};
        size_t columns = 0;
        size_t indexes = 0;
        if (!std::getline(input, line) ||
            !ParseLine(line, tag, table.id, table.version, table.name, columns, indexes) || tag != "TABLE" ||
            table.id == 0 || table.version == 0 || columns == 0 || columns > kMaxColumns ||
            !table_ids.insert(table.id).second)
            throw std::runtime_error("invalid Delta table catalog entry");
        std::set<std::string> column_names;
        size_t maximum_row = sizeof(uint32_t) + columns;
        for (size_t c = 0; c < columns; ++c) {
            unsigned type = 0;
            unsigned nullable = 0;
            Column column{};
            if (!std::getline(input, line) || !ParseLine(line, tag, type, column.length, nullable, column.name) ||
                tag != "COLUMN" || type > static_cast<unsigned>(ColumnType::Char) || nullable > 1 ||
                !column_names.insert(column.name).second)
                throw std::runtime_error("invalid Delta column catalog entry");
            column.type = static_cast<ColumnType>(type);
            column.nullable = nullable != 0;
            if ((column.type == ColumnType::Int || column.type == ColumnType::Float) && column.length != 4)
                throw std::runtime_error("invalid fixed Delta column length");
            if (column.type == ColumnType::Char && (column.length == 0 || column.length > kMaxRowBytes))
                throw std::runtime_error("invalid Delta CHAR length");
            maximum_row += column.type == ColumnType::Char ? sizeof(uint32_t) + column.length : 4;
            if (maximum_row > kMaxRowBytes)
                throw std::runtime_error("Delta row schema exceeds limit");
            table.columns.push_back(std::move(column));
        }
        for (size_t i = 0; i < indexes; ++i) {
            std::istringstream row;
            if (!std::getline(input, line))
                throw std::runtime_error("truncated Delta index catalog");
            row.str(line);
            unsigned unique = 0;
            size_t count = 0;
            Index index{};
            if (!(row >> tag >> index.constraint_id >> unique >> count) || tag != "INDEX" || index.constraint_id == 0 ||
                unique > 1 || count == 0 || count > table.columns.size() ||
                !constraint_ids.insert(index.constraint_id).second)
                throw std::runtime_error("invalid Delta index catalog entry");
            index.unique = unique != 0;
            std::set<uint32_t> distinct;
            for (size_t c = 0; c < count; ++c) {
                uint32_t column = 0;
                if (!(row >> column) || column >= table.columns.size() || !distinct.insert(column).second)
                    throw std::runtime_error("invalid Delta index column");
                index.columns.push_back(column);
            }
            std::string extra;
            if (row >> extra)
                throw std::runtime_error("invalid Delta index trailing data");
            max_constraint = std::max(max_constraint, index.constraint_id);
            table.indexes.push_back(std::move(index));
        }
        max_table = std::max(max_table, table.id);
        if (!candidate.emplace(table.name, std::move(table)).second)
            throw std::runtime_error("duplicate Delta table name");
    }
    if (std::getline(input, line) || !input.eof() || next_table <= max_table || next_constraint <= max_constraint)
        throw std::runtime_error("invalid Delta catalog tail");
    tables_.swap(candidate);
    table_by_id_.clear();
    for (const auto& [name, table] : tables_)
        table_by_id_.emplace(table.id, &table);
    next_table_id_ = next_table;
    next_constraint_id_ = next_constraint;
    catalog_generation_ = generation;
}

const DeltaDatabase::TableSchema& DeltaDatabase::Table(const std::string& name) const {
    const auto found = tables_.find(name);
    if (found == tables_.end())
        throw std::runtime_error("DeltaKernel table not found: " + name);
    return found->second;
}

const DeltaDatabase::TableSchema* DeltaDatabase::TableById(epoch_si_poc::TableId id) const {
    const auto found = table_by_id_.find(id);
    return found == table_by_id_.end() ? nullptr : found->second;
}

epoch_si_poc::EpochSiEngine::Txn& DeltaDatabase::Txn(DeltaSession& session) {
    if (!session.txn)
        session.txn.emplace(db_.engine().Begin());
    return *session.txn;
}

CommittedOverlay DeltaDatabase::PrepareCommittedOverlay(const DeltaOverlay& overlay) {
    CommittedOverlay prepared;
    for (const auto& [key, ids] : overlay)
        prepared.emplace(key, ids);
    return prepared;
}

void DeltaDatabase::RunCommitInstallHookForTest() noexcept {
    try {
        if (commit_install_hook_for_test_)
            commit_install_hook_for_test_();
    } catch (...) {
        std::terminate();
    }
}

void DeltaDatabase::InstallCommittedOverlay(CommittedOverlay& overlay) noexcept {
    try {
        overlay_.merge(overlay);
    } catch (...) {
        std::terminate();
    }
}

void DeltaDatabase::Commit(DeltaSession& session, std::unique_lock<std::mutex>& interpreter_lock) {
    if (!session.txn)
        return;
    auto ticket = std::make_shared<CommitTicket>();
    ticket->overlay = PrepareCommittedOverlay(session.overlay);
    ticket->txn.emplace(std::move(*session.txn));
    interpreter_lock.unlock();
    bool leader = false;
    try {
        std::unique_lock<std::mutex> queue_lock(commit_mutex_);
        commit_slot_available_.wait(queue_lock, [&] { return commit_queue_.size() < kCommitQueueLimit; });
        commit_queue_.push_back(ticket);
        if (!commit_leader_) {
            commit_leader_ = true;
            leader = true;
        }
    } catch (...) {
        interpreter_lock.lock();
        session.txn.emplace(std::move(*ticket->txn));
        ticket->txn.reset();
        throw;
    }
    session.txn.reset();
    session.overlay.clear();
    session.explicit_txn = false;
    if (leader)
        DrainCommitQueue();
    {
        std::unique_lock<std::mutex> queue_lock(commit_mutex_);
        ticket->ready.wait(queue_lock, [&] { return ticket->done; });
    }
    interpreter_lock.lock();
    session.admission.reset();
    if (ticket->error)
        std::rethrow_exception(ticket->error);
    if (ticket->result.status != epoch_si_poc::CommitStatus::kCommitted) {
        throw DeltaTransactionAbort(ticket->result.status == epoch_si_poc::CommitStatus::kWriteConflict
                                        ? "SI write conflict"
                                        : "unique key conflict");
    }
}

void DeltaDatabase::DrainCommitQueue() {
    for (;;) {
        std::array<std::shared_ptr<CommitTicket>, kCommitBatchSize> batch;
        size_t batch_count = 0;
        std::exception_ptr error;
        bool engine_called = false;
        {
            try {
                if (commit_batch_hook_for_test_)
                    commit_batch_hook_for_test_();
                std::this_thread::sleep_for(std::chrono::microseconds(200));
                std::lock_guard<std::mutex> queue_lock(commit_mutex_);
                const size_t count = std::min(kCommitBatchSize, commit_queue_.size());
                for (size_t n = 0; n < count; ++n) {
                    batch[batch_count++] = std::move(commit_queue_.front());
                    commit_queue_.pop_front();
                }
                commit_slot_available_.notify_all();
            } catch (...) {
                error = std::current_exception();
                std::lock_guard<std::mutex> queue_lock(commit_mutex_);
                while (batch_count < batch.size() && !commit_queue_.empty()) {
                    batch[batch_count++] = std::move(commit_queue_.front());
                    commit_queue_.pop_front();
                }
                commit_slot_available_.notify_all();
            }
        }
        std::vector<epoch_si_poc::CommitResult> results;
        if (!error && batch_count != 0) {
            try {
                std::vector<epoch_si_poc::EpochSiEngine::Txn*> txns;
                txns.reserve(batch_count);
                for (size_t n = 0; n < batch_count; ++n)
                    txns.push_back(&*batch[n]->txn);
                std::lock_guard<std::mutex> interpreter_lock(mutex_);
                engine_called = true;
                results = db_.engine().CommitBatch(txns);
                if (results.size() != batch_count)
                    std::terminate();
                RunCommitInstallHookForTest();
                for (size_t n = 0; n < batch_count; ++n)
                    if (results[n].status == epoch_si_poc::CommitStatus::kCommitted)
                        InstallCommittedOverlay(batch[n]->overlay);
            } catch (...) {
                error = std::current_exception();
                if (engine_called) {
                    std::lock_guard<std::mutex> interpreter_lock(mutex_);
                    poisoned_ = true;
                }
            }
        }
        {
            std::lock_guard<std::mutex> queue_lock(commit_mutex_);
            for (size_t n = 0; n < batch_count; ++n) {
                batch[n]->result = error ? epoch_si_poc::CommitResult{} : results[n];
                batch[n]->error = error;
                batch[n]->done = true;
                batch[n]->ready.notify_one();
            }
            commit_slot_available_.notify_all();
            if (commit_queue_.empty()) {
                commit_leader_ = false;
                return;
            }
        }
    }
}

void DeltaDatabase::Abort(DeltaSession& session) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    AbortLocked(session);
}

void DeltaDatabase::AbortLocked(DeltaSession& session) noexcept {
    if (session.txn) {
        try {
            db_.engine().Abort(*session.txn);
        } catch (...) {
        }
    }
    session.txn.reset();
    session.explicit_txn = false;
    session.overlay.clear();
    session.admission.reset();
}

void DeltaDatabase::Checkpoint() {
    std::unique_lock<std::shared_mutex> admission(execution_gate_);
    std::lock_guard<std::mutex> lock(mutex_);
    RequireUsable();
    CheckpointSidecars();
}

uint64_t DeltaDatabase::CatalogGeneration() const {
    std::lock_guard<std::mutex> lock(mutex_);
    RequireUsable();
    return catalog_generation_;
}

size_t DeltaDatabase::WalFrameCountForTest() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return db_.engine().wal_frame_count();
}

size_t DeltaDatabase::CommitQueueDepthForTest() const {
    std::lock_guard<std::mutex> lock(commit_mutex_);
    return commit_queue_.size();
}

DeltaDatabase::Cell DeltaDatabase::Literal(const Column& column, const ast::Value& value) const {
    Cell cell;
    if (value.type == ast::AstType::NullLit) {
        if (!column.nullable)
            throw std::runtime_error("NULL in non-nullable Delta column");
        return cell;
    }
    cell.is_null = false;
    if (column.type == ColumnType::Int && value.type == ast::AstType::IntLit)
        cell.integer = static_cast<const ast::IntLit&>(value).val;
    else if (column.type == ColumnType::Float && value.type == ast::AstType::FloatLit)
        cell.floating = static_cast<const ast::FloatLit&>(value).val;
    else if (column.type == ColumnType::Float && value.type == ast::AstType::IntLit)
        cell.floating = static_cast<float>(static_cast<const ast::IntLit&>(value).val);
    else if (column.type == ColumnType::Char && value.type == ast::AstType::StringLit) {
        cell.text = static_cast<const ast::StringLit&>(value).val;
        if (cell.text.size() > column.length)
            throw std::runtime_error("Delta CHAR value too long");
    } else {
        throw std::runtime_error("Delta value type mismatch");
    }
    return cell;
}

std::vector<uint8_t> DeltaDatabase::EncodeKey(const TableSchema& schema, const Index& index,
                                              const std::vector<Cell>& cells, size_t columns) const {
    std::vector<uint8_t> key;
    for (size_t i = 0; i < std::min(columns, index.columns.size()); ++i) {
        const uint32_t position = index.columns[i];
        const Column& column = schema.columns[position];
        const Cell& cell = cells[position];
        key.push_back(cell.is_null ? 0 : 1);
        if (cell.is_null)
            continue;
        if (column.type == ColumnType::Int) {
            PutBe<uint32_t>(key, static_cast<uint32_t>(cell.integer) ^ 0x80000000U);
        } else if (column.type == ColumnType::Float) {
            uint32_t bits;
            std::memcpy(&bits, &cell.floating, sizeof(bits));
            if ((bits & 0x7fffffffU) == 0)
                bits = 0;
            PutBe<uint32_t>(key, bits & 0x80000000U ? ~bits : bits ^ 0x80000000U);
        } else {
            for (unsigned char byte : cell.text) {
                key.push_back(byte);
                if (byte == 0)
                    key.push_back(0xff);
            }
            key.push_back(0);
            key.push_back(0);
        }
    }
    return key;
}

void DeltaDatabase::BuildSidecars(const TableSchema& schema, std::vector<std::vector<SidecarBuildEntry>> entries,
                                  uint64_t generation) {
    for (size_t n = 0; n < schema.indexes.size(); ++n) {
        const Index& index = schema.indexes[n];
        std::vector<SidecarBuildEntry>& sorted = entries[n];
        std::sort(sorted.begin(), sorted.end(), [](const auto& left, const auto& right) {
            return left.key != right.key ? left.key < right.key : left.local_id < right.local_id;
        });
        uint64_t key_bytes = 0;
        for (const auto& entry : sorted) {
            if (entry.key.size() > kMaxSidecarBytes - key_bytes) {
                key_bytes = kMaxSidecarBytes;
                break;
            }
            key_bytes += entry.key.size();
        }
        if (key_bytes > kMaxSidecarBytes - sizeof(SidecarHeader) - sorted.size() * sizeof(SidecarEntry))
            continue;
        std::vector<SidecarEntry> disk;
        disk.reserve(sorted.size());
        uint64_t key_offset = 0;
        for (const auto& entry : sorted) {
            disk.push_back({key_offset, entry.local_id});
            key_offset += entry.key.size();
        }
        SidecarHeader header{kSidecarMagic,
                             sizeof(SidecarHeader),
                             sizeof(SidecarEntry),
                             schema.id,
                             index.constraint_id,
                             generation,
                             disk.size(),
                             sizeof(SidecarHeader) + disk.size() * sizeof(SidecarEntry) + key_bytes,
                             key_bytes,
                             0,
                             0,
                             0};
        header.entries_crc = Crc32(disk.data(), disk.size() * sizeof(SidecarEntry));
        uint32_t keys_crc = 0xffffffffU;
        for (const auto& entry : sorted)
            keys_crc = UpdateCrc32(keys_crc, entry.key.data(), entry.key.size());
        header.keys_crc = ~keys_crc;
        header.header_crc = Crc32(&header, offsetof(SidecarHeader, header_crc));
        const std::string path =
            directory_ + "/deltaidx." + std::to_string(schema.id) + "." + std::to_string(index.constraint_id);
        const std::string temp = path + ".tmp";
        const int fd = open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd < 0)
            continue;
        const auto write_all = [&](const void* data, size_t bytes) {
            const auto* p = static_cast<const uint8_t*>(data);
            while (bytes) {
                const ssize_t wrote = write(fd, p, bytes);
                if (wrote <= 0)
                    return false;
                p += wrote;
                bytes -= static_cast<size_t>(wrote);
            }
            return true;
        };
        bool ok = write_all(&header, sizeof(header)) && write_all(disk.data(), disk.size() * sizeof(SidecarEntry));
        for (const auto& entry : sorted)
            ok = ok && write_all(entry.key.data(), entry.key.size());
        if (ok)
            ok = fsync(fd) == 0;
        if (close(fd) != 0)
            ok = false;
        if (ok)
            ok = rename(temp.c_str(), path.c_str()) == 0;
        if (!ok)
            unlink(temp.c_str());
        else
            sidecars_[index.constraint_id] = SidecarDescriptor{header.count, header.key_bytes};
    }
}

bool DeltaDatabase::ValidateSidecar(const TableSchema& schema, const Index& index) {
    sidecars_.erase(index.constraint_id);
    const std::string path =
        directory_ + "/deltaidx." + std::to_string(schema.id) + "." + std::to_string(index.constraint_id);
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return false;
    ++sidecar_validation_count_;
    SidecarHeader header{};
    struct stat st {};
    const auto generation = db_.TableGeneration(schema.id);
    bool valid = generation && fstat(fd, &st) == 0 && pread(fd, &header, sizeof(header), 0) == sizeof(header) &&
                 header.magic == kSidecarMagic && header.header_bytes == sizeof(header) &&
                 header.entry_bytes == sizeof(SidecarEntry) && header.table_id == schema.id &&
                 header.constraint_id == index.constraint_id && header.generation == *generation &&
                 header.total_bytes == static_cast<uint64_t>(st.st_size) &&
                 header.count <= (kMaxSidecarBytes - sizeof(header)) / sizeof(SidecarEntry) &&
                 header.key_bytes <= kMaxSidecarBytes &&
                 header.total_bytes == sizeof(header) + header.count * sizeof(SidecarEntry) + header.key_bytes &&
                 header.total_bytes <= kMaxSidecarBytes &&
                 header.header_crc == Crc32(&header, offsetof(SidecarHeader, header_crc));
    std::vector<SidecarEntry> chunk(std::min<uint64_t>(header.count, 65536));
    uint64_t done = 0;
    uint32_t crc = 0xffffffffU;
    while (valid && done < header.count) {
        const size_t count = static_cast<size_t>(std::min<uint64_t>(chunk.size(), header.count - done));
        valid = pread(fd, chunk.data(), count * sizeof(SidecarEntry), sizeof(header) + done * sizeof(SidecarEntry)) ==
                static_cast<ssize_t>(count * sizeof(SidecarEntry));
        crc = UpdateCrc32(crc, reinterpret_cast<const uint8_t*>(chunk.data()), count * sizeof(SidecarEntry));
        for (size_t i = 0; valid && i < count; ++i)
            valid = chunk[i].key_offset <= header.key_bytes;
        done += count;
    }
    std::vector<uint8_t> key_chunk(std::min<uint64_t>(header.key_bytes, 1U << 20));
    uint64_t key_done = 0;
    uint32_t keys_crc = 0xffffffffU;
    while (valid && key_done < header.key_bytes) {
        const size_t bytes = static_cast<size_t>(std::min<uint64_t>(key_chunk.size(), header.key_bytes - key_done));
        valid = pread(fd, key_chunk.data(), bytes, sizeof(header) + header.count * sizeof(SidecarEntry) + key_done) ==
                static_cast<ssize_t>(bytes);
        keys_crc = UpdateCrc32(keys_crc, key_chunk.data(), bytes);
        key_done += bytes;
    }
    close(fd);
    if (!valid || ~crc != header.entries_crc || ~keys_crc != header.keys_crc)
        return false;
    sidecars_.emplace(index.constraint_id, SidecarDescriptor{header.count, header.key_bytes});
    return true;
}

void DeltaDatabase::RebuildSidecars(const TableSchema& schema) {
    for (const auto& index : schema.indexes)
        sidecars_.erase(index.constraint_id);
    std::vector<std::vector<SidecarBuildEntry>> entries(schema.indexes.size());
    auto txn = db_.engine().Begin();
    try {
        db_.engine().VisitScan(txn, schema.id, [&](epoch_si_poc::RowId id, const epoch_si_poc::RowImage& image) {
            const auto cells = DecodeRow(schema, image);
            for (size_t n = 0; n < schema.indexes.size(); ++n) {
                entries[n].push_back({EncodeKey(schema, schema.indexes[n], cells), id.local_id});
            }
        });
        db_.engine().Abort(txn);
        if (const auto generation = db_.TableGeneration(schema.id))
            BuildSidecars(schema, std::move(entries), *generation);
    } catch (...) {
        for (const auto& index : schema.indexes)
            sidecars_.erase(index.constraint_id);
    }
}

void DeltaDatabase::CheckpointSidecars() {
    const auto dirty = db_.engine().DirtyTableIds();
    db_.OfflineCheckpoint();
    for (epoch_si_poc::TableId id : dirty) {
        const TableSchema* schema = TableById(id);
        if (!schema)
            continue;
        RebuildSidecars(*schema);
        for (auto it = overlay_.begin(); it != overlay_.end();) {
            if (std::get<0>(it->first) == id)
                it = overlay_.erase(it);
            else
                ++it;
        }
    }
}

std::vector<epoch_si_poc::RowId>
DeltaDatabase::IndexedCandidates(const DeltaSession& session, const TableSchema& schema,
                                 const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions, bool* usable) const {
    *usable = false;
    const Index* selected = nullptr;
    std::vector<Cell> cells;
    std::vector<Cell> selected_cells;
    size_t prefix_columns = 0;
    const ast::BinaryExpr* lower = nullptr;
    const ast::BinaryExpr* upper = nullptr;
    for (const Index& index : schema.indexes) {
        cells.assign(schema.columns.size(), Cell{});
        size_t equal = 0;
        for (; equal < index.columns.size(); ++equal) {
            const uint32_t column = index.columns[equal];
            const auto found = std::find_if(conditions.begin(), conditions.end(), [&](const auto& condition) {
                if (!condition || condition->op != ast::SV_OP_EQ || !condition->lhs || !condition->rhs ||
                    condition->lhs->type != ast::AstType::Col || condition->rhs->type == ast::AstType::Col)
                    return false;
                const auto& lhs = static_cast<const ast::Col&>(*condition->lhs);
                return (lhs.tab_name.empty() || lhs.tab_name == schema.name) &&
                       lhs.col_name == schema.columns[column].name;
            });
            if (found == conditions.end())
                break;
            Cell value = Literal(schema.columns[column], static_cast<const ast::Value&>(*(*found)->rhs));
            if (value.is_null)
                break;
            cells[column] = std::move(value);
        }
        const bool range =
            equal < index.columns.size() &&
            std::any_of(conditions.begin(), conditions.end(), [&](const auto& condition) {
                if (!condition || !condition->lhs || !condition->rhs || condition->lhs->type != ast::AstType::Col ||
                    condition->rhs->type == ast::AstType::Col)
                    return false;
                const auto& lhs = static_cast<const ast::Col&>(*condition->lhs);
                return (lhs.tab_name.empty() || lhs.tab_name == schema.name) &&
                       lhs.col_name == schema.columns[index.columns[equal]].name &&
                       (condition->op == ast::SV_OP_GT || condition->op == ast::SV_OP_GE ||
                        condition->op == ast::SV_OP_LT || condition->op == ast::SV_OP_LE);
            });
        if ((equal || range) && (!selected || equal > prefix_columns)) {
            selected = &index;
            prefix_columns = equal;
            selected_cells = cells;
        }
    }
    if (!selected)
        return {};
    cells = std::move(selected_cells);
    for (const auto& condition : conditions) {
        if (!condition || !condition->lhs || !condition->rhs || condition->lhs->type != ast::AstType::Col ||
            condition->rhs->type == ast::AstType::Col)
            continue;
        const auto& lhs = static_cast<const ast::Col&>(*condition->lhs);
        if ((!lhs.tab_name.empty() && lhs.tab_name != schema.name) || prefix_columns == selected->columns.size() ||
            schema.columns[selected->columns[prefix_columns]].name != lhs.col_name)
            continue;
        if (condition->op == ast::SV_OP_GT || condition->op == ast::SV_OP_GE)
            lower = condition.get();
        else if (condition->op == ast::SV_OP_LT || condition->op == ast::SV_OP_LE)
            upper = condition.get();
    }
    std::vector<epoch_si_poc::RowId> result;
    EncodedKey first_key = EncodeKey(schema, *selected, cells, prefix_columns);
    if (lower) {
        cells[selected->columns[prefix_columns]] =
            Literal(schema.columns[selected->columns[prefix_columns]], static_cast<const ast::Value&>(*lower->rhs));
        first_key = EncodeKey(schema, *selected, cells, prefix_columns + 1);
        if (lower->op == ast::SV_OP_GT)
            first_key = PrefixSuccessor(std::move(first_key));
    }
    EncodedKey last_key = PrefixSuccessor(EncodeKey(schema, *selected, cells, prefix_columns));
    if (upper) {
        cells[selected->columns[prefix_columns]] =
            Literal(schema.columns[selected->columns[prefix_columns]], static_cast<const ast::Value&>(*upper->rhs));
        last_key = EncodeKey(schema, *selected, cells, prefix_columns + 1);
        if (upper->op == ast::SV_OP_LE)
            last_key = PrefixSuccessor(std::move(last_key));
    }
    VisitIndexInterval(
        session, schema, *selected, first_key, last_key,
        [&](const EncodedKey&, epoch_si_poc::RowId id) { result.push_back(id); }, usable);
    if (!*usable)
        return {};
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

void DeltaDatabase::VisitIndexInterval(const DeltaSession& session, const TableSchema& schema, const Index& index,
                                       const EncodedKey& first_key, const EncodedKey& last_key,
                                       const std::function<void(const EncodedKey&, epoch_si_poc::RowId)>& visitor,
                                       bool* usable) const {
    *usable = false;
    const auto descriptor = sidecars_.find(index.constraint_id);
    if (descriptor == sidecars_.end())
        return;
    std::vector<std::pair<EncodedKey, epoch_si_poc::RowId>> keyed;
    const std::string path =
        directory_ + "/deltaidx." + std::to_string(schema.id) + "." + std::to_string(index.constraint_id);
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        const auto read = [&](uint64_t position, SidecarEntry* entry) {
            return pread(fd, entry, sizeof(*entry), sizeof(SidecarHeader) + position * sizeof(*entry)) ==
                   sizeof(*entry);
        };
        const auto key_at = [&](uint64_t position, std::vector<uint8_t>* key, SidecarEntry* entry) {
            SidecarEntry next{};
            if (!read(position, entry) || entry->key_offset > descriptor->second.key_bytes ||
                (position + 1 < descriptor->second.count && !read(position + 1, &next)))
                return false;
            const uint64_t end =
                position + 1 == descriptor->second.count ? descriptor->second.key_bytes : next.key_offset;
            if (end < entry->key_offset || end - entry->key_offset > kMaxRowBytes)
                return false;
            key->resize(static_cast<size_t>(end - entry->key_offset));
            return key->empty() || pread(fd, key->data(), key->size(),
                                         sizeof(SidecarHeader) + descriptor->second.count * sizeof(SidecarEntry) +
                                             entry->key_offset) == static_cast<ssize_t>(key->size());
        };
        const auto lower_bound = [&](const std::vector<uint8_t>& key) {
            uint64_t first = 0, last = descriptor->second.count;
            while (first < last) {
                const uint64_t middle = first + (last - first) / 2;
                SidecarEntry entry{};
                std::vector<uint8_t> current;
                if (!key_at(middle, &current, &entry))
                    return std::pair<uint64_t, bool>{0, false};
                if (current < key)
                    first = middle + 1;
                else
                    last = middle;
            }
            return std::pair<uint64_t, bool>{first, true};
        };
        bool valid = true;
        auto [first, first_ok] = lower_bound(first_key);
        auto [last, last_ok] =
            last_key.empty() ? std::pair<uint64_t, bool>{descriptor->second.count, true} : lower_bound(last_key);
        valid = first_ok && last_ok;
        for (; valid && first < last; ++first) {
            SidecarEntry entry{};
            EncodedKey key;
            valid = key_at(first, &key, &entry);
            if (valid)
                keyed.push_back({std::move(key), {schema.id, entry.local_id}});
        }
        close(fd);
        if (!valid)
            sidecars_.erase(index.constraint_id);
        *usable = valid;
    }
    if (!*usable)
        return;
    VisitOverlayInterval(session, schema.id, index.constraint_id, first_key, last_key,
                         [&](const EncodedKey& key, epoch_si_poc::RowId id) { keyed.push_back({key, id}); });
    std::sort(keyed.begin(), keyed.end());
    keyed.erase(std::unique(keyed.begin(), keyed.end()), keyed.end());
    for (const auto& [key, id] : keyed)
        visitor(key, id);
}

void DeltaDatabase::VisitOverlayInterval(
    const DeltaSession& session, epoch_si_poc::TableId table_id, epoch_si_poc::ConstraintId constraint_id,
    const EncodedKey& first, const EncodedKey& last,
    const std::function<void(const EncodedKey&, epoch_si_poc::RowId)>& visitor) const {
    last_overlay_nodes_probed_ = 0;
    last_row_ids_probed_ = 0;
    const auto append = [&](const auto& overlay) {
        auto position = overlay.lower_bound({table_id, constraint_id, first});
        for (; position != overlay.end() && std::get<0>(position->first) == table_id &&
               std::get<1>(position->first) == constraint_id && (last.empty() || std::get<2>(position->first) < last);
             ++position) {
            ++last_overlay_nodes_probed_;
            last_row_ids_probed_ += position->second.size();
            for (uint64_t local_id : position->second)
                visitor(std::get<2>(position->first), {table_id, local_id});
        }
    };
    append(overlay_);
    append(session.overlay);
}

template <typename Overlay>
void DeltaDatabase::AddOverlay(Overlay& overlay, const TableSchema& schema, const std::vector<Cell>& cells,
                               epoch_si_poc::RowId id, const std::vector<Cell>* previous) {
    for (const Index& index : schema.indexes) {
        bool has_null = false;
        for (uint32_t column : index.columns)
            has_null = has_null || cells[column].is_null;
        if (has_null)
            continue;
        EncodedKey key = EncodeKey(schema, index, cells);
        if (!previous || key != EncodeKey(schema, index, *previous))
            AppendOverlay(overlay, {schema.id, index.constraint_id, std::move(key)}, id.local_id);
    }
}

void DeltaDatabase::VisitRows(DeltaSession& session, const TableSchema& schema,
                              const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions,
                              const std::function<void(epoch_si_poc::RowId, const epoch_si_poc::RowImage&)>& visitor,
                              bool* used_index) {
    auto& txn = Txn(session);
    bool usable = false;
    const auto candidates = IndexedCandidates(session, schema, conditions, &usable);
    if (used_index)
        *used_index = usable;
    last_row_reads_probed_ = 0;
    if (!usable) {
        db_.engine().VisitScan(txn, schema.id, visitor);
        return;
    }
    for (epoch_si_poc::RowId id : candidates) {
        ++last_row_reads_probed_;
        if (auto row = db_.engine().Read(txn, id))
            visitor(id, *row);
    }
}

epoch_si_poc::RowImage DeltaDatabase::EncodeRow(const TableSchema& schema, const std::vector<Cell>& cells) const {
    if (cells.size() != schema.columns.size())
        throw std::runtime_error("Delta row column count mismatch");
    epoch_si_poc::RowImage image;
    PutLe<uint32_t>(image.bytes, schema.version);
    for (size_t i = 0; i < cells.size(); ++i) {
        const Column& column = schema.columns[i];
        const Cell& cell = cells[i];
        if (cell.is_null && !column.nullable)
            throw std::runtime_error("NULL in non-nullable Delta column");
        image.bytes.push_back(cell.is_null ? 0 : 1);
        if (cell.is_null)
            continue;
        if (column.type == ColumnType::Int)
            PutLe<int32_t>(image.bytes, cell.integer);
        else if (column.type == ColumnType::Float) {
            uint32_t bits;
            std::memcpy(&bits, &cell.floating, sizeof(bits));
            PutLe<uint32_t>(image.bytes, bits);
        } else {
            if (cell.text.size() > column.length)
                throw std::runtime_error("Delta CHAR value too long");
            PutLe<uint32_t>(image.bytes, static_cast<uint32_t>(cell.text.size()));
            image.bytes.insert(image.bytes.end(), cell.text.begin(), cell.text.end());
        }
        if (image.bytes.size() > kMaxRowBytes)
            throw std::runtime_error("Delta row exceeds limit");
    }
    for (const Index& index : schema.indexes) {
        if (!index.unique)
            continue;
        bool has_null = false;
        for (uint32_t column : index.columns)
            has_null = has_null || cells[column].is_null;
        if (!has_null)
            image.claims.push_back({index.constraint_id, EncodeKey(schema, index, cells)});
    }
    std::sort(image.claims.begin(), image.claims.end());
    image.claims.erase(std::unique(image.claims.begin(), image.claims.end()), image.claims.end());
    return image;
}

std::vector<DeltaDatabase::Cell> DeltaDatabase::DecodeRow(const TableSchema& schema,
                                                          const epoch_si_poc::RowImage& image) const {
    if (image.deleted || image.bytes.size() > kMaxRowBytes)
        throw std::runtime_error("invalid Delta row image");
    size_t offset = 0;
    if (GetLe<uint32_t>(image.bytes, offset) != schema.version)
        throw std::runtime_error("Delta schema version mismatch");
    std::vector<Cell> cells(schema.columns.size());
    for (size_t i = 0; i < cells.size(); ++i) {
        if (offset >= image.bytes.size())
            throw std::runtime_error("truncated Delta row null tag");
        const uint8_t present = image.bytes[offset++];
        if (present > 1 || (!present && !schema.columns[i].nullable))
            throw std::runtime_error("invalid Delta row null tag");
        if (!present)
            continue;
        cells[i].is_null = false;
        if (schema.columns[i].type == ColumnType::Int)
            cells[i].integer = GetLe<int32_t>(image.bytes, offset);
        else if (schema.columns[i].type == ColumnType::Float) {
            const uint32_t bits = GetLe<uint32_t>(image.bytes, offset);
            std::memcpy(&cells[i].floating, &bits, sizeof(bits));
        } else {
            const uint32_t length = GetLe<uint32_t>(image.bytes, offset);
            if (length > schema.columns[i].length || offset > image.bytes.size() ||
                image.bytes.size() - offset < length)
                throw std::runtime_error("invalid Delta CHAR payload");
            cells[i].text.assign(reinterpret_cast<const char*>(image.bytes.data() + offset), length);
            offset += length;
        }
    }
    if (offset != image.bytes.size())
        throw std::runtime_error("Delta row has trailing bytes");
    return cells;
}

bool DeltaDatabase::Matches(const TableSchema& schema, const std::vector<Cell>& cells,
                            const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions) const {
    const auto position = [&](const ast::Col& column) {
        if (!column.tab_name.empty() && column.tab_name != schema.name)
            throw std::runtime_error("unknown Delta qualifier");
        for (size_t i = 0; i < schema.columns.size(); ++i)
            if (schema.columns[i].name == column.col_name)
                return i;
        throw std::runtime_error("Delta column not found: " + column.col_name);
    };
    for (const auto& condition : conditions) {
        if (condition->lhs->type != ast::AstType::Col)
            throw std::runtime_error("Delta predicate lhs must be a column");
        const size_t lhs_pos = position(static_cast<const ast::Col&>(*condition->lhs));
        const Cell& lhs = cells[lhs_pos];
        if (condition->op == ast::SV_OP_IS_NULL || condition->op == ast::SV_OP_IS_NOT_NULL) {
            if ((condition->op == ast::SV_OP_IS_NULL) != lhs.is_null)
                return false;
            continue;
        }
        Cell rhs;
        if (condition->rhs->type == ast::AstType::Col) {
            const size_t rhs_pos = position(static_cast<const ast::Col&>(*condition->rhs));
            if (schema.columns[rhs_pos].type != schema.columns[lhs_pos].type)
                throw std::runtime_error("Delta predicate type mismatch");
            rhs = cells[rhs_pos];
        } else {
            rhs = Literal(schema.columns[lhs_pos], static_cast<const ast::Value&>(*condition->rhs));
        }
        if (lhs.is_null || rhs.is_null)
            return false;
        int comparison = 0;
        if (schema.columns[lhs_pos].type == ColumnType::Int)
            comparison = lhs.integer < rhs.integer ? -1 : lhs.integer > rhs.integer;
        else if (schema.columns[lhs_pos].type == ColumnType::Float)
            comparison = lhs.floating < rhs.floating ? -1 : lhs.floating > rhs.floating;
        else
            comparison = lhs.text.compare(rhs.text);
        if (!CompareResult(comparison, condition->op))
            return false;
    }
    return true;
}

void DeltaDatabase::EmitCells(const std::vector<Column>& columns, const std::vector<std::vector<Cell>>& rows,
                              QueryResultSink* sink) const {
    if (!sink)
        return;
    std::vector<ColMeta> metadata;
    std::vector<std::string> names;
    int size = 0;
    for (const auto& column : columns) {
        metadata.push_back({"", column.name,
                            column.type == ColumnType::Int     ? TYPE_INT
                            : column.type == ColumnType::Float ? TYPE_FLOAT
                                                               : TYPE_STRING,
                            static_cast<int>(column.length), size});
        names.push_back(column.name);
        size += static_cast<int>(column.length);
    }
    bind_null_positions(metadata, size);
    sink->begin_query(metadata, names);
    for (const auto& row : rows) {
        std::vector<char> tuple(static_cast<size_t>(size + null_bitmap_bytes(metadata.size())), 0);
        auto output = metadata;
        for (size_t i = 0; i < row.size(); ++i) {
            if (row[i].is_null) {
                set_null(tuple.data(), output[i]);
                continue;
            }
            if (output[i].type == TYPE_INT)
                write_unaligned<int32_t>(tuple.data() + output[i].offset, row[i].integer);
            else if (output[i].type == TYPE_FLOAT)
                write_float(tuple.data() + output[i].offset, row[i].floating);
            else {
                std::memcpy(tuple.data() + output[i].offset, row[i].text.data(), row[i].text.size());
                output[i].value_length_is_exact = true;
                output[i].len = static_cast<int>(row[i].text.size());
            }
        }
        sink->append_row(output, tuple.data(), tuple.size());
    }
}

void DeltaDatabase::EmitRows(const TableSchema& schema, const ast::SelectStmt& select,
                             const std::vector<std::vector<Cell>>& rows, QueryResultSink* sink, bool aggregate_values,
                             bool query_started) const {
    if (!sink)
        return;
    const bool aggregate = std::any_of(select.select_items.begin(), select.select_items.end(),
                                       [](const auto& item) { return item->expr->type == ast::AstType::AggExpr; });
    if (aggregate) {
        if (select.has_select_star ||
            std::any_of(select.select_items.begin(), select.select_items.end(),
                        [](const auto& item) { return item->expr->type != ast::AstType::AggExpr; }))
            throw std::runtime_error("mixed Delta aggregate projection is unsupported");
        struct State {
            int64_t count = 0;
            bool seen = false;
            double sum = 0;
            Cell min;
            std::set<std::string> distinct;
        };
        std::vector<State> states(select.select_items.size());
        std::vector<size_t> positions(select.select_items.size());
        std::vector<Column> outputs;
        for (size_t i = 0; i < select.select_items.size(); ++i) {
            const auto& agg = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
            Column output{select.select_items[i]->alias.empty() ? "?column?" : select.select_items[i]->alias,
                          ColumnType::Int, 4, false};
            if (!agg.is_star) {
                if (!agg.col)
                    throw std::runtime_error("invalid Delta aggregate");
                positions[i] = 0;
                while (positions[i] < schema.columns.size() && schema.columns[positions[i]].name != agg.col->col_name)
                    ++positions[i];
                if (positions[i] == schema.columns.size())
                    throw std::runtime_error("Delta aggregate column not found");
                if (agg.func != ast::AGG_COUNT) {
                    output.type = schema.columns[positions[i]].type;
                    output.length = output.type == ColumnType::Char ? schema.columns[positions[i]].length : 4;
                }
            } else if (agg.func != ast::AGG_COUNT)
                throw std::runtime_error("unsupported Delta aggregate");
            outputs.push_back(std::move(output));
        }
        for (const auto& row : rows)
            for (size_t i = 0; !aggregate_values && i < outputs.size(); ++i) {
                const auto& agg = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
                State& state = states[i];
                const Cell value = agg.is_star ? Cell{} : row[positions[i]];
                if (agg.func == ast::AGG_COUNT) {
                    if (agg.is_star) {
                        ++state.count;
                        continue;
                    }
                    if (value.is_null)
                        continue;
                    if (agg.is_distinct) {
                        std::string key = std::to_string(static_cast<unsigned>(outputs[i].type)) + ":";
                        if (outputs[i].type == ColumnType::Int)
                            key += std::to_string(value.integer);
                        else if (outputs[i].type == ColumnType::Float) {
                            uint32_t bits;
                            std::memcpy(&bits, &value.floating, sizeof(bits));
                            key += std::to_string(bits);
                        } else
                            key += value.text;
                        if (!state.distinct.insert(std::move(key)).second)
                            continue;
                    }
                    ++state.count;
                } else if (!value.is_null && agg.func == ast::AGG_SUM) {
                    if (outputs[i].type == ColumnType::Int)
                        state.sum += value.integer;
                    else if (outputs[i].type == ColumnType::Float)
                        state.sum += value.floating;
                    else
                        throw std::runtime_error("Delta SUM requires numeric column");
                    state.seen = true;
                } else if (!value.is_null && agg.func == ast::AGG_MIN) {
                    if (!state.seen || (outputs[i].type == ColumnType::Int     ? value.integer < state.min.integer
                                        : outputs[i].type == ColumnType::Float ? value.floating < state.min.floating
                                                                               : value.text < state.min.text))
                        state.min = value;
                    state.seen = true;
                } else if (agg.func != ast::AGG_MIN)
                    throw std::runtime_error("unsupported Delta aggregate");
            }
        std::vector<ColMeta> metadata;
        std::vector<std::string> names;
        int size = 0;
        for (const auto& output : outputs) {
            metadata.push_back({schema.name, output.name,
                                output.type == ColumnType::Int     ? TYPE_INT
                                : output.type == ColumnType::Float ? TYPE_FLOAT
                                                                   : TYPE_STRING,
                                static_cast<int>(output.length), size});
            names.push_back(output.name);
            size += static_cast<int>(output.length);
        }
        bind_null_positions(metadata, size);
        sink->begin_query(metadata, names);
        std::vector<char> tuple(static_cast<size_t>(size + null_bitmap_bytes(metadata.size())), 0);
        for (size_t i = 0; i < outputs.size(); ++i) {
            const auto& agg = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
            Cell value;
            if (aggregate_values)
                value = rows[0][i];
            else if (agg.func == ast::AGG_COUNT) {
                value.is_null = false;
                value.integer = static_cast<int32_t>(states[i].count);
            } else if (states[i].seen) {
                value = states[i].min;
                if (agg.func == ast::AGG_SUM) {
                    value.is_null = false;
                    if (outputs[i].type == ColumnType::Int)
                        value.integer = static_cast<int32_t>(states[i].sum);
                    else
                        value.floating = static_cast<float>(states[i].sum);
                }
            }
            if (value.is_null) {
                set_null(tuple.data(), metadata[i]);
                continue;
            }
            if (metadata[i].type == TYPE_INT)
                write_unaligned<int32_t>(tuple.data() + metadata[i].offset, value.integer);
            else if (metadata[i].type == TYPE_FLOAT)
                write_float(tuple.data() + metadata[i].offset, value.floating);
            else {
                std::memcpy(tuple.data() + metadata[i].offset, value.text.data(), value.text.size());
                metadata[i].value_length_is_exact = true;
                metadata[i].len = static_cast<int>(value.text.size());
            }
        }
        sink->append_row(metadata, tuple.data(), tuple.size());
        return;
    }
    struct Projection {
        Column column;
        std::optional<size_t> source;
        Cell literal;
    };
    std::vector<Projection> projection;
    if (select.has_select_star) {
        for (size_t i = 0; i < schema.columns.size(); ++i)
            projection.push_back({schema.columns[i], i, {}});
    } else {
        for (const auto& item : select.select_items) {
            if (item->expr->type == ast::AstType::Col) {
                const auto& col = static_cast<const ast::Col&>(*item->expr);
                size_t found = schema.columns.size();
                for (size_t i = 0; i < schema.columns.size(); ++i)
                    if (schema.columns[i].name == col.col_name)
                        found = i;
                if (found == schema.columns.size())
                    throw std::runtime_error("Delta projection column not found");
                Column output = schema.columns[found];
                if (!item->alias.empty())
                    output.name = item->alias;
                projection.push_back({std::move(output), found, {}});
            } else if (item->expr->type == ast::AstType::IntLit) {
                Column output{item->alias.empty() ? "?column?" : item->alias, ColumnType::Int, 4, false};
                projection.push_back(
                    {output, std::nullopt, Literal(output, static_cast<const ast::Value&>(*item->expr))});
            } else if (item->expr->type == ast::AstType::FloatLit) {
                Column output{item->alias.empty() ? "?column?" : item->alias, ColumnType::Float, 4, false};
                projection.push_back(
                    {output, std::nullopt, Literal(output, static_cast<const ast::Value&>(*item->expr))});
            } else if (item->expr->type == ast::AstType::StringLit) {
                const auto& literal = static_cast<const ast::StringLit&>(*item->expr);
                Column output{item->alias.empty() ? "?column?" : item->alias, ColumnType::Char,
                              static_cast<uint32_t>(std::max<size_t>(1, literal.val.size())), false};
                projection.push_back({output, std::nullopt, Literal(output, literal)});
            } else
                throw std::runtime_error("unsupported Delta projection");
        }
    }
    std::vector<ColMeta> metadata;
    std::vector<std::string> names;
    int data_size = 0;
    for (const Projection& item : projection) {
        ColMeta column;
        column.tab_name = schema.name;
        column.name = item.column.name;
        column.type = item.column.type == ColumnType::Int     ? TYPE_INT
                      : item.column.type == ColumnType::Float ? TYPE_FLOAT
                                                              : TYPE_STRING;
        column.len = static_cast<int>(item.column.length);
        column.offset = data_size;
        data_size += column.len;
        metadata.push_back(column);
        names.push_back(column.name);
    }
    bind_null_positions(metadata, data_size);
    if (!query_started)
        sink->begin_query(metadata, names);
    for (const auto& source_row : rows) {
        std::vector<char> tuple(static_cast<size_t>(data_size + null_bitmap_bytes(metadata.size())), 0);
        std::vector<ColMeta> row_columns = metadata;
        for (size_t i = 0; i < projection.size(); ++i) {
            const Cell& cell = projection[i].source ? source_row[*projection[i].source] : projection[i].literal;
            if (metadata[i].type == TYPE_STRING) {
                row_columns[i].value_length_is_exact = true;
                row_columns[i].len = cell.is_null ? 0 : static_cast<int>(cell.text.size());
            }
            if (cell.is_null) {
                set_null(tuple.data(), metadata[i]);
                continue;
            }
            char* target = tuple.data() + metadata[i].offset;
            if (metadata[i].type == TYPE_INT)
                write_unaligned<int32_t>(target, cell.integer);
            else if (metadata[i].type == TYPE_FLOAT)
                write_float(target, cell.floating);
            else
                std::memcpy(target, cell.text.data(), cell.text.size());
        }
        sink->append_row(row_columns, tuple.data(), tuple.size());
    }
}

void DeltaDatabase::EmitTables(QueryResultSink* sink) const {
    if (!sink)
        return;
    ColMeta column;
    column.name = "Tables";
    column.type = TYPE_STRING;
    column.len = 256;
    column.offset = 0;
    std::vector<ColMeta> metadata{column};
    bind_null_positions(metadata, column.len);
    sink->begin_query(metadata, {column.name});
    for (const auto& [name, table] : tables_) {
        std::vector<char> tuple(static_cast<size_t>(column.len + 1), 0);
        std::memcpy(tuple.data(), name.data(), std::min(name.size(), static_cast<size_t>(column.len)));
        auto row_column = metadata;
        row_column[0].len = static_cast<int>(std::min(name.size(), static_cast<size_t>(column.len)));
        row_column[0].value_length_is_exact = true;
        sink->append_row(row_column, tuple.data(), tuple.size());
    }
}

void DeltaDatabase::LoadCsv(const ast::LoadStmt& load, DeltaSession& session) {
    if (session.txn || db_.engine().active_transaction_count() != 0)
        throw std::runtime_error("Delta LOAD requires no active transaction");
    const TableSchema schema = Table(load.tab_name_);
    std::filesystem::path csv_path(load.file_name_);
    if (csv_path.is_relative())
        csv_path = std::filesystem::path(directory_) / csv_path;
    std::ifstream input(csv_path);
    if (!input)
        throw std::runtime_error("open Delta CSV");

    if (std::any_of(schema.indexes.begin(), schema.indexes.end(), [](const Index& index) { return index.unique; }))
        throw std::runtime_error("Delta LOAD into unique-indexed table is unsupported");
    auto writer = db_.BeginTableBase(schema.id);
    std::vector<std::vector<SidecarBuildEntry>> sidecars(schema.indexes.size());

    std::vector<size_t> source(schema.columns.size());
    for (size_t i = 0; i < source.size(); ++i)
        source[i] = i;
    std::vector<const char*> fields;
    fields.reserve(schema.columns.size() * 2);
    bool first_line = true;
    std::string line;
    while (std::getline(input, line)) {
        rmdb_csv::StripCr(line);
        if (!first_line && line.empty())
            continue;
        rmdb_csv::SplitLineInPlace(line, fields);
        if (fields.size() == schema.columns.size() + 1 && *fields.back() == '\0')
            fields.pop_back();

        if (first_line) {
            first_line = false;
            std::vector<size_t> header_source(schema.columns.size(), schema.columns.size());
            std::set<size_t> seen;
            bool header = fields.size() == schema.columns.size();
            for (size_t input_column = 0; input_column < fields.size(); ++input_column) {
                const char* begin = fields[input_column];
                const char* end = begin + std::strlen(begin);
                while (begin < end && std::isspace(static_cast<unsigned char>(*begin)))
                    ++begin;
                while (end > begin && std::isspace(static_cast<unsigned char>(end[-1])))
                    --end;
                const std::string header_name(begin, static_cast<size_t>(end - begin));
                auto found = std::find_if(schema.columns.begin(), schema.columns.end(),
                                          [&](const Column& column) { return column.name == header_name; });
                if (found == schema.columns.end() || !seen.insert(found - schema.columns.begin()).second) {
                    header = false;
                    break;
                }
                header_source[found - schema.columns.begin()] = input_column;
            }
            if (header) {
                source = std::move(header_source);
                continue;
            }
        }
        if (fields.size() < schema.columns.size())
            throw std::runtime_error("Delta CSV column count mismatch");

        std::vector<Cell> cells;
        cells.reserve(schema.columns.size());
        for (size_t i = 0; i < schema.columns.size(); ++i) {
            const char* field = fields[source[i]];
            if (*field == '\0' && schema.columns[i].type != ColumnType::Char) {
                if (!schema.columns[i].nullable)
                    throw std::runtime_error("empty numeric CSV field for non-nullable Delta column");
                cells.emplace_back();
                continue;
            }
            if (schema.columns[i].type == ColumnType::Int) {
                errno = 0;
                char* end = nullptr;
                const long value = std::strtol(field, &end, 10);
                if (errno != 0 || end == field || *end != '\0' || value < INT32_MIN || value > INT32_MAX)
                    throw std::runtime_error("invalid Delta CSV INT");
                Cell cell;
                cell.is_null = false;
                cell.integer = static_cast<int32_t>(value);
                cells.push_back(std::move(cell));
            } else if (schema.columns[i].type == ColumnType::Float) {
                errno = 0;
                char* end = nullptr;
                Cell cell;
                cell.is_null = false;
                cell.floating = std::strtof(field, &end);
                if (errno != 0 || end == field || *end != '\0' || !std::isfinite(cell.floating))
                    throw std::runtime_error("invalid Delta CSV FLOAT");
                cells.push_back(std::move(cell));
            } else {
                const size_t length = std::strlen(field);
                if (length > schema.columns[i].length)
                    throw std::runtime_error("Delta CSV CHAR too long");
                Cell cell;
                cell.is_null = false;
                cell.text.assign(field, length);
                cells.push_back(std::move(cell));
            }
        }
        writer.Append(EncodeRow(schema, cells));
        const uint64_t local_id = writer.row_count() - 1;
        for (size_t i = 0; i < schema.indexes.size(); ++i) {
            sidecars[i].push_back({EncodeKey(schema, schema.indexes[i], cells), local_id});
        }
    }
    if (!input.eof() || first_line)
        throw std::runtime_error("read Delta CSV");
    if (load_before_publish_hook_for_test_)
        load_before_publish_hook_for_test_();
    db_.PublishTableBase(std::move(writer));
    try {
        BuildSidecars(schema, std::move(sidecars), *db_.TableGeneration(schema.id));
    } catch (...) {
        // Sidecars are disposable acceleration artifacts; tablebase is already authoritative.
    }
}

PreparedDescription DeltaDatabase::DescribePrepared(const ast::TreeNode& tree,
                                                    const std::vector<DeltaValueType>& declared_parameters) const {
    std::lock_guard<std::mutex> lock(mutex_);
    RequireUsable();
    PreparedDescription result;
    result.catalog_generation = catalog_generation_;
    const auto value_type = [](ColumnType type) { return static_cast<DeltaValueType>(type); };
    const auto validate_value = [&](const Column& column, const ast::Value& value) {
        if (value.type != ast::AstType::Parameter) {
            (void)Literal(column, value);
            return;
        }
        const auto& parameter = static_cast<const ast::Parameter&>(value);
        if (parameter.ordinal == 0 || parameter.ordinal > declared_parameters.size())
            throw std::runtime_error("prepared Delta parameter is out of range");
        const DeltaValueType actual = declared_parameters[parameter.ordinal - 1];
        if (actual != value_type(column.type) && !(column.type == ColumnType::Float && actual == DeltaValueType::Int))
            throw std::runtime_error("prepared Delta parameter type mismatch");
    };
    const auto column = [&](const TableSchema& schema, const ast::Col& name) -> const Column& {
        if (!name.tab_name.empty() && name.tab_name != schema.name)
            throw std::runtime_error("unknown prepared Delta qualifier");
        auto found = std::find_if(schema.columns.begin(), schema.columns.end(),
                                  [&](const Column& candidate) { return candidate.name == name.col_name; });
        if (found == schema.columns.end())
            throw std::runtime_error("prepared Delta column not found: " + name.col_name);
        return *found;
    };
    const auto conditions = [&](const auto& resolve, const std::vector<std::unique_ptr<ast::BinaryExpr>>& predicates) {
        for (const auto& predicate : predicates) {
            if (!predicate || !predicate->lhs || !predicate->rhs || predicate->lhs->type != ast::AstType::Col)
                throw std::runtime_error("invalid prepared Delta predicate");
            const Column& lhs = resolve(static_cast<const ast::Col&>(*predicate->lhs));
            if (predicate->op == ast::SV_OP_IS_NULL || predicate->op == ast::SV_OP_IS_NOT_NULL)
                continue;
            if (predicate->rhs->type == ast::AstType::Col) {
                if (resolve(static_cast<const ast::Col&>(*predicate->rhs)).type != lhs.type)
                    throw std::runtime_error("prepared Delta predicate type mismatch");
            } else if (predicate->rhs->type == ast::AstType::Parameter ||
                       predicate->rhs->type == ast::AstType::IntLit || predicate->rhs->type == ast::AstType::FloatLit ||
                       predicate->rhs->type == ast::AstType::StringLit ||
                       predicate->rhs->type == ast::AstType::NullLit) {
                validate_value(lhs, static_cast<const ast::Value&>(*predicate->rhs));
            } else {
                throw std::runtime_error("unsupported prepared Delta predicate rhs");
            }
        }
    };

    if (tree.type == ast::AstType::SelectStmt) {
        const auto& select = static_cast<const ast::SelectStmt&>(tree);
        if (select.tabs.size() == 2 && select.jointree.empty() && !select.has_sort && !select.has_limit &&
            select.group_by_cols.empty() && select.having_conds.empty()) {
            if (select.has_select_star || select.select_items.empty())
                throw std::runtime_error("unsupported prepared Delta multi-table projection");
            const TableSchema& first = Table(select.tabs[0].table_name);
            const TableSchema& second = Table(select.tabs[1].table_name);
            const auto multi_column = [&](const ast::Col& name) -> const Column& {
                const auto find = [&](const TableSchema& schema, const ast::TableRef& table) -> const Column* {
                    if (!name.tab_name.empty() && name.tab_name != table.table_name && name.tab_name != table.alias)
                        return nullptr;
                    auto found = std::find_if(schema.columns.begin(), schema.columns.end(),
                                              [&](const Column& candidate) { return candidate.name == name.col_name; });
                    return found == schema.columns.end() ? nullptr : &*found;
                };
                const Column* first_found = find(first, select.tabs[0]);
                const Column* second_found = find(second, select.tabs[1]);
                if (first_found && second_found)
                    throw std::runtime_error("ambiguous prepared Delta multi-table column: " + name.col_name);
                if (!first_found && !second_found)
                    throw std::runtime_error("prepared Delta column not found: " + name.col_name);
                return first_found ? *first_found : *second_found;
            };
            result.query = true;
            if (select.select_items.size() == 1 && select.select_items[0] &&
                select.select_items[0]->expr->type == ast::AstType::AggExpr &&
                static_cast<const ast::AggExpr&>(*select.select_items[0]->expr).func == ast::AGG_COUNT) {
                const auto& aggregate = static_cast<const ast::AggExpr&>(*select.select_items[0]->expr);
                if (aggregate.is_star || !aggregate.col)
                    throw std::runtime_error("unsupported prepared Delta multi-table aggregate");
                (void)multi_column(*aggregate.col);
                conditions(multi_column, select.conds);
                result.names.push_back(select.select_items[0]->alias.empty() ? "?column?"
                                                                             : select.select_items[0]->alias);
                result.types.push_back(DeltaValueType::Int);
                return result;
            }
            for (const auto& item : select.select_items) {
                if (!item || item->expr->type != ast::AstType::Col)
                    throw std::runtime_error("unsupported prepared Delta multi-table projection");
                const auto& source = static_cast<const ast::Col&>(*item->expr);
                const Column& found = multi_column(source);
                result.names.push_back(item->alias.empty() ? source.col_name : item->alias);
                result.types.push_back(value_type(found.type));
            }
            conditions(multi_column, select.conds);
            return result;
        }
        if (select.tabs.size() != 1 || !select.jointree.empty() || !select.having_conds.empty())
            throw std::runtime_error("unsupported prepared Delta SELECT shape");
        const TableSchema& schema = Table(select.tabs[0].table_name);
        conditions([&](const ast::Col& name) -> const Column& { return column(schema, name); }, select.conds);
        result.query = true;
        if (select.has_select_star) {
            if (!select.select_items.empty())
                throw std::runtime_error("invalid prepared Delta star projection");
            for (const Column& item : schema.columns) {
                result.names.push_back(item.name);
                result.types.push_back(value_type(item.type));
            }
        } else {
            if (select.select_items.empty())
                throw std::runtime_error("empty prepared Delta projection");
            for (const auto& item : select.select_items) {
                if (!item || !item->expr)
                    throw std::runtime_error("invalid prepared Delta projection");
                DeltaValueType type;
                std::string name = item->alias.empty() ? "?column?" : item->alias;
                if (item->expr->type == ast::AstType::Col) {
                    const auto& source = static_cast<const ast::Col&>(*item->expr);
                    type = value_type(column(schema, source).type);
                    if (item->alias.empty())
                        name = source.col_name;
                } else if (item->expr->type == ast::AstType::AggExpr) {
                    const auto& aggregate = static_cast<const ast::AggExpr&>(*item->expr);
                    if (aggregate.func == ast::AGG_COUNT)
                        type = DeltaValueType::Int;
                    else if (aggregate.col && (aggregate.func == ast::AGG_MIN || aggregate.func == ast::AGG_MAX ||
                                               aggregate.func == ast::AGG_SUM))
                        type = value_type(column(schema, *aggregate.col).type);
                    else
                        throw std::runtime_error("unsupported prepared Delta aggregate");
                } else if (item->expr->type == ast::AstType::Parameter) {
                    throw std::runtime_error("prepared Delta parameter projection is unsupported");
                } else if (item->expr->type == ast::AstType::IntLit) {
                    type = DeltaValueType::Int;
                } else if (item->expr->type == ast::AstType::FloatLit) {
                    type = DeltaValueType::Float;
                } else if (item->expr->type == ast::AstType::StringLit) {
                    type = DeltaValueType::Char;
                } else {
                    throw std::runtime_error("unsupported prepared Delta projection");
                }
                result.names.push_back(std::move(name));
                result.types.push_back(type);
            }
        }
    } else if (tree.type == ast::AstType::InsertStmt) {
        const auto& insert = static_cast<const ast::InsertStmt&>(tree);
        const TableSchema& schema = Table(insert.tab_name);
        if (insert.vals.size() != schema.columns.size())
            throw std::runtime_error("prepared Delta INSERT column count mismatch");
        for (size_t i = 0; i < insert.vals.size(); ++i) {
            if (!insert.vals[i])
                throw std::runtime_error("invalid prepared Delta INSERT value");
            validate_value(schema.columns[i], *insert.vals[i]);
        }
    } else if (tree.type == ast::AstType::DeleteStmt || tree.type == ast::AstType::UpdateStmt) {
        const bool deleting = tree.type == ast::AstType::DeleteStmt;
        const auto& name = deleting ? static_cast<const ast::DeleteStmt&>(tree).tab_name
                                    : static_cast<const ast::UpdateStmt&>(tree).tab_name;
        const TableSchema& schema = Table(name);
        conditions([&](const ast::Col& name) -> const Column& { return column(schema, name); },
                   deleting ? static_cast<const ast::DeleteStmt&>(tree).conds
                            : static_cast<const ast::UpdateStmt&>(tree).conds);
        if (!deleting) {
            const auto& update = static_cast<const ast::UpdateStmt&>(tree);
            if (update.set_clauses.empty())
                throw std::runtime_error("empty prepared Delta UPDATE");
            for (const auto& clause : update.set_clauses) {
                if (!clause)
                    throw std::runtime_error("invalid prepared Delta UPDATE clause");
                auto target = std::find_if(schema.columns.begin(), schema.columns.end(),
                                           [&](const Column& item) { return item.name == clause->col_name; });
                if (target == schema.columns.end())
                    throw std::runtime_error("prepared Delta UPDATE column not found");
                if (!clause->is_self_ref) {
                    if (!clause->val)
                        throw std::runtime_error("missing prepared Delta UPDATE value");
                    validate_value(*target, *clause->val);
                    continue;
                }
                if (!clause->rhs_col || column(schema, *clause->rhs_col).type != target->type)
                    throw std::runtime_error("prepared Delta UPDATE source mismatch");
                if (clause->op != ast::SetOp::ASSIGNMENT) {
                    if (!clause->val || (target->type != ColumnType::Int && target->type != ColumnType::Float))
                        throw std::runtime_error("invalid prepared Delta arithmetic");
                    validate_value(*target, *clause->val);
                }
                for (const auto& term : clause->additional_terms) {
                    if (!term.val || (target->type != ColumnType::Int && target->type != ColumnType::Float))
                        throw std::runtime_error("invalid prepared Delta arithmetic term");
                    validate_value(*target, *term.val);
                }
            }
        }
    } else if (tree.type != ast::AstType::TxnBegin && tree.type != ast::AstType::TxnCommit &&
               tree.type != ast::AstType::TxnAbort && tree.type != ast::AstType::TxnRollback) {
        throw std::runtime_error("unsupported prepared Delta statement");
    }
    return result;
}

bool DeltaDatabase::Execute(std::unique_ptr<ast::TreeNode> tree, DeltaSession& session, QueryResultSink* sink) {
    if (!tree)
        return false;
    const bool exclusive = tree->type == ast::AstType::StaticCheckpoint || tree->type == ast::AstType::CreateTable ||
                           tree->type == ast::AstType::CreateIndex || tree->type == ast::AstType::LoadStmt;
    if (exclusive && session.admission)
        throw std::runtime_error("Delta schema/checkpoint operation inside transaction");
    std::unique_lock<std::shared_mutex> exclusive_admission;
    std::shared_lock<std::shared_mutex> statement_admission;
    if (exclusive)
        exclusive_admission = std::unique_lock<std::shared_mutex>(execution_gate_);
    else if (!session.admission)
        statement_admission = std::shared_lock<std::shared_mutex>(execution_gate_);
    std::unique_lock<std::mutex> lock(mutex_, std::defer_lock);
    if (execute_blocked_hook_for_test_ && !lock.try_lock())
        execute_blocked_hook_for_test_();
    if (!lock.owns_lock())
        lock.lock();
    RequireUsable();
    if (execute_lock_hook_for_test_)
        execute_lock_hook_for_test_();
    if (tree->type == ast::AstType::ShowTables) {
        EmitTables(sink);
        return true;
    }
    if (tree->type == ast::AstType::SetTransaction) {
        if (static_cast<const ast::SetTransaction&>(*tree).isolation_level_ !=
            ast::IsolationLevelType::SNAPSHOT_ISOLATION)
            throw std::runtime_error("DeltaKernel supports SNAPSHOT ISOLATION only");
        return false;
    }
    if (tree->type == ast::AstType::TxnBegin) {
        if (session.txn)
            throw std::runtime_error("transaction already active");
        session.txn.emplace(db_.engine().Begin());
        session.explicit_txn = true;
        session.admission.emplace(std::move(statement_admission));
        return false;
    }
    if (tree->type == ast::AstType::TxnCommit) {
        Commit(session, lock);
        return false;
    }
    if (tree->type == ast::AstType::TxnAbort || tree->type == ast::AstType::TxnRollback) {
        AbortLocked(session);
        return false;
    }
    if (tree->type == ast::AstType::StaticCheckpoint) {
        CheckpointSidecars();
        return false;
    }
    if (tree->type == ast::AstType::CreateTable) {
        const auto& create = static_cast<const ast::CreateTable&>(*tree);
        if (session.txn || create.fields.empty() || create.fields.size() > kMaxColumns ||
            next_table_id_ == std::numeric_limits<epoch_si_poc::TableId>::max() ||
            catalog_generation_ == std::numeric_limits<uint64_t>::max() || tables_.count(create.tab_name))
            throw std::runtime_error("invalid Delta CREATE TABLE");
        TableSchema table{next_table_id_, 1, create.tab_name, {}, {}};
        std::set<std::string> names;
        size_t maximum_row = sizeof(uint32_t) + create.fields.size();
        for (const auto& field : create.fields) {
            const auto* column = dynamic_cast<const ast::ColDef*>(field.get());
            if (!column || !names.insert(column->col_name).second)
                throw std::runtime_error("invalid Delta column");
            ColumnType type;
            uint32_t length;
            if (column->type_len->type == ast::SV_TYPE_INT) {
                type = ColumnType::Int;
                length = 4;
            } else if (column->type_len->type == ast::SV_TYPE_FLOAT) {
                type = ColumnType::Float;
                length = 4;
            } else if (column->type_len->type == ast::SV_TYPE_STRING && column->type_len->len > 0) {
                type = ColumnType::Char;
                length = static_cast<uint32_t>(column->type_len->len);
            } else
                throw std::runtime_error("DeltaKernel supports INT, FLOAT, and CHAR only");
            maximum_row += type == ColumnType::Char ? sizeof(uint32_t) + length : 4;
            if (maximum_row > kMaxRowBytes)
                throw std::runtime_error("Delta row schema exceeds limit");
            table.columns.push_back({column->col_name, type, length, true});
        }
        Catalog candidate = tables_;
        candidate.emplace(table.name, table);
        const auto next_table = static_cast<epoch_si_poc::TableId>(next_table_id_ + 1);
        const uint64_t generation = catalog_generation_ + 1;
        SaveCatalog(candidate, next_table, next_constraint_id_, generation);
        tables_.swap(candidate);
        table_by_id_.clear();
        for (const auto& [name, schema] : tables_)
            table_by_id_.emplace(schema.id, &schema);
        next_table_id_ = next_table;
        catalog_generation_ = generation;
        return false;
    }
    if (tree->type == ast::AstType::CreateIndex) {
        const auto& create = static_cast<const ast::CreateIndex&>(*tree);
        if (session.txn || create.col_names.empty() ||
            next_constraint_id_ == std::numeric_limits<epoch_si_poc::ConstraintId>::max() ||
            catalog_generation_ == std::numeric_limits<uint64_t>::max())
            throw std::runtime_error("invalid Delta CREATE INDEX");
        Catalog candidate = tables_;
        TableSchema& table = candidate.at(create.tab_name);
        Index index{next_constraint_id_, {}, false};
        for (const std::string& name : create.col_names) {
            auto found = std::find_if(table.columns.begin(), table.columns.end(),
                                      [&](const Column& c) { return c.name == name; });
            if (found == table.columns.end())
                throw std::runtime_error("Delta index column not found");
            const uint32_t position = static_cast<uint32_t>(found - table.columns.begin());
            if (std::find(index.columns.begin(), index.columns.end(), position) != index.columns.end())
                throw std::runtime_error("duplicate Delta index column");
            index.columns.push_back(position);
        }
        table.indexes.push_back(index);
        const auto next_constraint = static_cast<epoch_si_poc::ConstraintId>(next_constraint_id_ + 1);
        const uint64_t generation = catalog_generation_ + 1;
        SaveCatalog(candidate, next_table_id_, next_constraint, generation);
        tables_.swap(candidate);
        table_by_id_.clear();
        for (const auto& [name, schema] : tables_)
            table_by_id_.emplace(schema.id, &schema);
        next_constraint_id_ = next_constraint;
        catalog_generation_ = generation;
        try {
            // A declared index over WAL-only rows needs a tablebase generation before it can persist.
            CheckpointSidecars();
        } catch (...) {
            // The catalog is authoritative; a later checkpoint/Open retries acceleration construction.
        }
        RebuildSidecars(Table(create.tab_name));
        return false;
    }
    if (tree->type == ast::AstType::LoadStmt) {
        LoadCsv(static_cast<const ast::LoadStmt&>(*tree), session);
        return false;
    }
    const bool implicit = !session.txn;
    try {
        if (tree->type == ast::AstType::InsertStmt) {
            const auto& insert = static_cast<const ast::InsertStmt&>(*tree);
            const TableSchema& schema = Table(insert.tab_name);
            if (insert.vals.size() != schema.columns.size())
                throw std::runtime_error("Delta INSERT column count mismatch");
            std::vector<Cell> cells;
            for (size_t i = 0; i < insert.vals.size(); ++i)
                cells.push_back(Literal(schema.columns[i], *insert.vals[i]));
            const auto id = db_.engine().InsertImage(Txn(session), schema.id, EncodeRow(schema, cells));
            AddOverlay(session.overlay, schema, cells, id);
        } else if (tree->type == ast::AstType::SelectStmt) {
            const auto& select = static_cast<const ast::SelectStmt&>(*tree);
            if (select.tabs.empty() || !select.jointree.empty() || !select.having_conds.empty())
                throw std::runtime_error("unsupported Delta SELECT shape");
            if (select.tabs.size() > 2)
                throw std::runtime_error("unsupported Delta SELECT shape");
            if (select.tabs.size() == 2) {
                const bool projection_query =
                    !select.select_items.empty() &&
                    std::all_of(select.select_items.begin(), select.select_items.end(),
                                [](const auto& item) { return item->expr->type == ast::AstType::Col; });
                if ((!projection_query &&
                     (!std::all_of(select.select_items.begin(), select.select_items.end(),
                                   [](const auto& item) { return item->expr->type == ast::AstType::AggExpr; }) ||
                      select.select_items.size() != 1)) ||
                    select.has_sort || select.has_limit)
                    throw std::runtime_error("unsupported Delta multi-table SELECT shape");
                const ast::AggExpr* aggregate_ptr =
                    projection_query ? nullptr : static_cast<const ast::AggExpr*>(select.select_items[0]->expr.get());
                if (!projection_query &&
                    (aggregate_ptr->func != ast::AGG_COUNT || aggregate_ptr->is_star || !aggregate_ptr->col))
                    throw std::runtime_error("unsupported Delta multi-table aggregate");
                const TableSchema& left_schema = Table(select.tabs[0].table_name);
                const TableSchema& right_schema = Table(select.tabs[1].table_name);
                last_parameterized_join_probes_ = 0;
                last_join_inner_rows_resolved_ = 0;
                last_join_pairs_rechecked_ = 0;
                last_join_full_scan_rows_ = 0;
                last_join_right_rows_visited_ = 0;
                const auto pos = [](const TableSchema& schema, const std::string& name) {
                    for (size_t i = 0; i < schema.columns.size(); ++i)
                        if (schema.columns[i].name == name)
                            return i;
                    throw std::runtime_error("Delta multi-table column not found: " + name);
                };
                const auto resolve = [&](const ast::Col& column, const std::vector<Cell>& left,
                                         const std::vector<Cell>& right, const Column*& out_column,
                                         const Cell*& out_cell) {
                    const bool use_left = column.tab_name.empty() || column.tab_name == select.tabs[0].table_name ||
                                          column.tab_name == select.tabs[0].alias;
                    const bool use_right = column.tab_name.empty() || column.tab_name == select.tabs[1].table_name ||
                                           column.tab_name == select.tabs[1].alias;
                    const bool left_has =
                        use_left && std::any_of(left_schema.columns.begin(), left_schema.columns.end(),
                                                [&](const Column& c) { return c.name == column.col_name; });
                    const bool right_has =
                        use_right && std::any_of(right_schema.columns.begin(), right_schema.columns.end(),
                                                 [&](const Column& c) { return c.name == column.col_name; });
                    if (left_has == right_has)
                        throw std::runtime_error("ambiguous Delta multi-table column: " + column.col_name);
                    if (left_has) {
                        const size_t i = pos(left_schema, column.col_name);
                        out_column = &left_schema.columns[i];
                        out_cell = &left[i];
                    } else {
                        const size_t i = pos(right_schema, column.col_name);
                        out_column = &right_schema.columns[i];
                        out_cell = &right[i];
                    }
                };
                std::vector<ColMeta> projection_meta;
                std::vector<std::string> projection_names;
                int projection_size = 0;
                if (projection_query && sink) {
                    for (const auto& item : select.select_items) {
                        const auto& col = static_cast<const ast::Col&>(*item->expr);
                        const Column* source;
                        const Cell* ignored;
                        resolve(col, std::vector<Cell>(left_schema.columns.size()),
                                std::vector<Cell>(right_schema.columns.size()), source, ignored);
                        ColMeta meta;
                        meta.tab_name = "";
                        meta.name = item->alias.empty() ? col.col_name : item->alias;
                        meta.type = source->type == ColumnType::Int     ? TYPE_INT
                                    : source->type == ColumnType::Float ? TYPE_FLOAT
                                                                        : TYPE_STRING;
                        meta.len = static_cast<int>(source->type == ColumnType::Char ? source->length : 4);
                        meta.offset = projection_size;
                        projection_size += meta.len;
                        projection_meta.push_back(meta);
                        projection_names.push_back(meta.name);
                    }
                    bind_null_positions(projection_meta, projection_size);
                }
                struct BoundPredicate {
                    const Column* lhs_column;
                    size_t lhs_pos;
                    int lhs_side;
                    const Column* rhs_column = nullptr;
                    size_t rhs_pos = 0;
                    int rhs_side = -1;
                    Cell literal;
                    ast::SvCompOp op;
                };
                const auto locate = [&](const ast::Col& column) {
                    const bool use_left = column.tab_name.empty() || column.tab_name == select.tabs[0].table_name ||
                                          column.tab_name == select.tabs[0].alias;
                    const bool use_right = column.tab_name.empty() || column.tab_name == select.tabs[1].table_name ||
                                           column.tab_name == select.tabs[1].alias;
                    const bool left_has =
                        use_left && std::any_of(left_schema.columns.begin(), left_schema.columns.end(),
                                                [&](const Column& c) { return c.name == column.col_name; });
                    const bool right_has =
                        use_right && std::any_of(right_schema.columns.begin(), right_schema.columns.end(),
                                                 [&](const Column& c) { return c.name == column.col_name; });
                    if (left_has == right_has)
                        throw std::runtime_error("ambiguous Delta multi-table column: " + column.col_name);
                    return std::tuple<int, size_t, const Column*>{
                        left_has ? 0 : 1,
                        left_has ? pos(left_schema, column.col_name) : pos(right_schema, column.col_name),
                        left_has ? &left_schema.columns[pos(left_schema, column.col_name)]
                                 : &right_schema.columns[pos(right_schema, column.col_name)]};
                };
                std::vector<BoundPredicate> left_predicates, right_predicates, cross_predicates;
                for (const auto& condition : select.conds) {
                    const auto* lhs =
                        condition && condition->lhs ? dynamic_cast<const ast::Col*>(condition->lhs.get()) : nullptr;
                    if (!lhs)
                        throw std::runtime_error("Delta predicate lhs must be a column");
                    auto [lhs_side, lhs_pos, lhs_column] = locate(*lhs);
                    BoundPredicate predicate{lhs_column, lhs_pos, lhs_side, nullptr, 0, -1, {}, condition->op};
                    if (condition->op != ast::SV_OP_IS_NULL && condition->op != ast::SV_OP_IS_NOT_NULL) {
                        if (const auto* rhs = dynamic_cast<const ast::Col*>(condition->rhs.get())) {
                            std::tie(predicate.rhs_side, predicate.rhs_pos, predicate.rhs_column) = locate(*rhs);
                            if (lhs_column->type != predicate.rhs_column->type)
                                throw std::runtime_error("Delta multi-table predicate type mismatch");
                        } else if (const auto* rhs = dynamic_cast<const ast::Value*>(condition->rhs.get()))
                            predicate.literal = Literal(*lhs_column, *rhs);
                        else
                            throw std::runtime_error("unsupported Delta multi-table predicate");
                    }
                    auto& destination = predicate.rhs_side < 0 || predicate.rhs_side == lhs_side
                                            ? (lhs_side == 0 ? left_predicates : right_predicates)
                                            : cross_predicates;
                    destination.push_back(std::move(predicate));
                }
                const auto matches = [](const std::vector<BoundPredicate>& predicates, const std::vector<Cell>& left,
                                        const std::vector<Cell>& right) {
                    for (const auto& predicate : predicates) {
                        const Cell& lhs = predicate.lhs_side == 0 ? left[predicate.lhs_pos] : right[predicate.lhs_pos];
                        if (predicate.op == ast::SV_OP_IS_NULL || predicate.op == ast::SV_OP_IS_NOT_NULL) {
                            if ((predicate.op == ast::SV_OP_IS_NULL) != lhs.is_null)
                                return false;
                            continue;
                        }
                        const Cell& rhs = predicate.rhs_side < 0 ? predicate.literal
                                                                 : (predicate.rhs_side == 0 ? left[predicate.rhs_pos]
                                                                                            : right[predicate.rhs_pos]);
                        if (lhs.is_null || rhs.is_null)
                            return false;
                        const int cmp = predicate.lhs_column->type == ColumnType::Int
                                            ? (lhs.integer < rhs.integer ? -1 : lhs.integer > rhs.integer)
                                        : predicate.lhs_column->type == ColumnType::Float
                                            ? (lhs.floating < rhs.floating ? -1 : lhs.floating > rhs.floating)
                                            : lhs.text.compare(rhs.text);
                        if (!CompareResult(cmp, predicate.op))
                            return false;
                    }
                    return true;
                };
                // ponytail: logical RowImage object/payload bound, not exact RSS; add a spill/hash join when too big.
                std::vector<epoch_si_poc::RowImage> left_rows;
                size_t left_bytes = 0;
                bool left_used_index = false;
                VisitRows(
                    session, left_schema, select.conds,
                    [&](epoch_si_poc::RowId, const epoch_si_poc::RowImage& image) {
                        if (!left_used_index)
                            ++last_join_full_scan_rows_;
                        auto left = DecodeRow(left_schema, image);
                        if (!matches(left_predicates, left, {}))
                            return;
                        size_t image_bytes = sizeof(epoch_si_poc::RowImage);
                        const auto add_image_bytes = [&](size_t bytes) {
                            if (bytes > kMaxJoinMaterializedBytes - image_bytes)
                                throw std::runtime_error("unsupported Delta join exceeds memory limit");
                            image_bytes += bytes;
                        };
                        add_image_bytes(image.bytes.size());
                        if (image.claims.size() >
                            (kMaxJoinMaterializedBytes - image_bytes) / sizeof(epoch_si_poc::ConstraintClaim))
                            throw std::runtime_error("unsupported Delta join exceeds memory limit");
                        image_bytes += image.claims.size() * sizeof(epoch_si_poc::ConstraintClaim);
                        for (const auto& claim : image.claims)
                            add_image_bytes(claim.bytes.size());
                        if (left_rows.size() == kMaxJoinMaterializedRows ||
                            image_bytes > kMaxJoinMaterializedBytes - left_bytes)
                            throw std::runtime_error("unsupported Delta join exceeds memory limit");
                        left_bytes += image_bytes;
                        left_rows.push_back(image);
                    },
                    &left_used_index);
                int64_t count = 0;
                std::set<std::string> distinct;
                const auto emit_pair = [&](const std::vector<Cell>& left, const std::vector<Cell>& right) {
                    if (projection_query) {
                        if (!sink)
                            return;
                        std::vector<char> tuple(
                            static_cast<size_t>(projection_size + null_bitmap_bytes(projection_meta.size())), 0);
                        auto columns = projection_meta;
                        for (size_t i = 0; i < select.select_items.size(); ++i) {
                            const Column* source;
                            const Cell* value;
                            resolve(static_cast<const ast::Col&>(*select.select_items[i]->expr), left, right, source,
                                    value);
                            if (value->is_null) {
                                set_null(tuple.data(), columns[i]);
                                continue;
                            }
                            if (columns[i].type == TYPE_INT)
                                write_unaligned<int32_t>(tuple.data() + columns[i].offset, value->integer);
                            else if (columns[i].type == TYPE_FLOAT)
                                write_float(tuple.data() + columns[i].offset, value->floating);
                            else {
                                std::memcpy(tuple.data() + columns[i].offset, value->text.data(), value->text.size());
                                columns[i].value_length_is_exact = true;
                                columns[i].len = static_cast<int>(value->text.size());
                            }
                        }
                        sink->append_row(columns, tuple.data(), tuple.size());
                        return;
                    }
                    const Column* value_column;
                    const Cell* value;
                    resolve(*aggregate_ptr->col, left, right, value_column, value);
                    if (value->is_null)
                        return;
                    if (aggregate_ptr->is_distinct) {
                        std::string key = std::to_string(static_cast<unsigned>(value_column->type)) + ":" +
                                          (value_column->type == ColumnType::Int     ? std::to_string(value->integer)
                                           : value_column->type == ColumnType::Float ? std::to_string(value->floating)
                                                                                     : value->text);
                        if (!distinct.insert(std::move(key)).second)
                            return;
                    }
                    ++count;
                };
                struct ProbeKeySource {
                    bool from_literal = false;
                    size_t left_pos = 0;
                    Cell literal;
                };
                const Index* probe_index = nullptr;
                std::vector<ProbeKeySource> probe_sources;
                for (const Index& index : right_schema.indexes) {
                    std::vector<ProbeKeySource> sources;
                    for (uint32_t right_pos : index.columns) {
                        ProbeKeySource source;
                        bool found = false;
                        for (const BoundPredicate& predicate : right_predicates) {
                            if (predicate.lhs_pos == right_pos && predicate.op == ast::SV_OP_EQ &&
                                predicate.rhs_side < 0 && !predicate.literal.is_null) {
                                source.from_literal = true;
                                source.literal = predicate.literal;
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            for (const BoundPredicate& predicate : cross_predicates) {
                                if (predicate.op != ast::SV_OP_EQ)
                                    continue;
                                if (predicate.lhs_side == 1 && predicate.lhs_pos == right_pos &&
                                    predicate.rhs_side == 0) {
                                    source.left_pos = predicate.rhs_pos;
                                    found = true;
                                    break;
                                }
                                if (predicate.rhs_side == 1 && predicate.rhs_pos == right_pos &&
                                    predicate.lhs_side == 0) {
                                    source.left_pos = predicate.lhs_pos;
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if (!found) {
                            sources.clear();
                            break;
                        }
                        sources.push_back(std::move(source));
                    }
                    if (sources.size() == index.columns.size() && sidecars_.count(index.constraint_id)) {
                        probe_index = &index;
                        probe_sources = std::move(sources);
                        break;
                    }
                }
                const auto make_probe_key = [&](const std::vector<Cell>& left) -> std::optional<EncodedKey> {
                    if (!probe_index)
                        return std::nullopt;
                    std::vector<Cell> right(right_schema.columns.size());
                    for (size_t i = 0; i < probe_sources.size(); ++i) {
                        const Cell& value =
                            probe_sources[i].from_literal ? probe_sources[i].literal : left[probe_sources[i].left_pos];
                        if (value.is_null)
                            return std::nullopt;
                        right[probe_index->columns[i]] = value;
                    }
                    return EncodeKey(right_schema, *probe_index, right);
                };
                bool parameterized = probe_index != nullptr;
                if (parameterized) {
                    for (const auto& left_image : left_rows) {
                        const auto left = DecodeRow(left_schema, left_image);
                        const auto key = make_probe_key(left);
                        if (!key)
                            continue;
                        bool usable = false;
                        VisitIndexInterval(
                            session, right_schema, *probe_index, *key, PrefixSuccessor(*key),
                            [](const EncodedKey&, epoch_si_poc::RowId) {}, &usable);
                        if (!usable) {
                            parameterized = false;
                            break;
                        }
                    }
                }
                if (projection_query && sink)
                    sink->begin_query(projection_meta, projection_names);
                if (parameterized) {
                    auto& txn = Txn(session);
                    for (const auto& left_image : left_rows) {
                        const auto left = DecodeRow(left_schema, left_image);
                        const auto key = make_probe_key(left);
                        if (!key)
                            continue;
                        ++last_parameterized_join_probes_;
                        bool usable = false;
                        VisitIndexInterval(
                            session, right_schema, *probe_index, *key, PrefixSuccessor(*key),
                            [&](const EncodedKey& candidate_key, epoch_si_poc::RowId id) {
                                ++last_join_inner_rows_resolved_;
                                const auto image = db_.engine().Read(txn, id);
                                if (!image)
                                    return;
                                const auto right = DecodeRow(right_schema, *image);
                                if (EncodeKey(right_schema, *probe_index, right) != candidate_key ||
                                    !matches(right_predicates, {}, right))
                                    return;
                                ++last_join_pairs_rechecked_;
                                if (matches(cross_predicates, left, right))
                                    emit_pair(left, right);
                            },
                            &usable);
                        if (!usable)
                            throw std::runtime_error("Delta parameterized index became unavailable");
                    }
                } else {
                    bool right_used_index = false;
                    VisitRows(
                        session, right_schema, select.conds,
                        [&](epoch_si_poc::RowId, const epoch_si_poc::RowImage& image) {
                            ++last_join_right_rows_visited_;
                            if (!right_used_index)
                                ++last_join_full_scan_rows_;
                            const auto right = DecodeRow(right_schema, image);
                            if (!matches(right_predicates, {}, right))
                                return;
                            for (const auto& left_image : left_rows) {
                                const auto left = DecodeRow(left_schema, left_image);
                                ++last_join_pairs_rechecked_;
                                if (matches(cross_predicates, left, right))
                                    emit_pair(left, right);
                            }
                        },
                        &right_used_index);
                }
                if (!projection_query) {
                    Cell value;
                    value.is_null = false;
                    value.integer = static_cast<int32_t>(count);
                    EmitRows(left_schema, select, {{value}}, sink, true);
                }
                if (implicit)
                    Commit(session, lock);
                return true;
            }
            const TableSchema& schema = Table(select.tabs[0].table_name);
            if (!select.group_by_cols.empty()) {
                if (select.has_sort || select.has_limit)
                    throw std::runtime_error("unsupported Delta GROUP BY shape");
                std::vector<size_t> keys;
                for (const auto& column : select.group_by_cols) {
                    size_t pos = 0;
                    while (pos < schema.columns.size() && schema.columns[pos].name != column->col_name)
                        ++pos;
                    if (pos == schema.columns.size())
                        throw std::runtime_error("Delta GROUP BY column not found");
                    keys.push_back(pos);
                }
                struct State {
                    int64_t count = 0;
                    double sum = 0;
                    Cell value;
                    bool seen = false;
                };
                struct Group {
                    std::vector<Cell> values;
                    std::vector<State> states;
                };
                std::map<std::string, Group> groups;
                VisitRows(session, schema, select.conds, [&](epoch_si_poc::RowId, const epoch_si_poc::RowImage& image) {
                    const auto cells = DecodeRow(schema, image);
                    if (!Matches(schema, cells, select.conds))
                        return;
                    std::string key;
                    for (size_t pos : keys) {
                        key += std::to_string(static_cast<unsigned>(schema.columns[pos].type)) + ":";
                        if (cells[pos].is_null)
                            key += "N";
                        else if (schema.columns[pos].type == ColumnType::Int)
                            key += std::to_string(cells[pos].integer);
                        else if (schema.columns[pos].type == ColumnType::Float) {
                            uint32_t bits;
                            std::memcpy(&bits, &cells[pos].floating, sizeof(bits));
                            key += std::to_string(bits);
                        } else
                            key += cells[pos].text;
                        key += ":";
                    }
                    auto [it, inserted] = groups.emplace(key, Group{});
                    auto& group = it->second;
                    if (inserted) {
                        for (size_t pos : keys)
                            group.values.push_back(cells[pos]);
                        group.states.resize(select.select_items.size());
                    }
                    for (size_t item_index = 0; item_index < select.select_items.size(); ++item_index) {
                        const auto& item = select.select_items[item_index];
                        if (item->expr->type == ast::AstType::AggExpr) {
                            const auto& agg = static_cast<const ast::AggExpr&>(*item->expr);
                            State& state = group.states[item_index];
                            if (agg.func == ast::AGG_COUNT && agg.is_star) {
                                ++state.count;
                                continue;
                            }
                            if (agg.col) {
                                size_t pos = 0;
                                while (pos < schema.columns.size() && schema.columns[pos].name != agg.col->col_name)
                                    ++pos;
                                if (pos == schema.columns.size())
                                    throw std::runtime_error("Delta aggregate column not found");
                                const Cell& value = cells[pos];
                                if (value.is_null)
                                    continue;
                                if (agg.func == ast::AGG_COUNT)
                                    ++state.count;
                                else if (agg.func == ast::AGG_SUM) {
                                    if (schema.columns[pos].type == ColumnType::Int)
                                        state.sum += value.integer;
                                    else if (schema.columns[pos].type == ColumnType::Float)
                                        state.sum += value.floating;
                                    else
                                        throw std::runtime_error("Delta SUM requires numeric column");
                                    state.seen = true;
                                } else if (agg.func == ast::AGG_MIN || agg.func == ast::AGG_MAX) {
                                    const bool smaller = schema.columns[pos].type == ColumnType::Int
                                                             ? value.integer < state.value.integer
                                                         : schema.columns[pos].type == ColumnType::Float
                                                             ? value.floating < state.value.floating
                                                             : value.text < state.value.text;
                                    if (!state.seen || (agg.func == ast::AGG_MIN ? smaller : !smaller))
                                        state.value = value;
                                    state.seen = true;
                                } else
                                    throw std::runtime_error("unsupported Delta GROUP BY aggregate");
                            }
                        }
                    }
                });
                std::vector<Column> columns;
                std::vector<std::vector<Cell>> rows;
                for (const auto& item : select.select_items) {
                    if (const auto* col = dynamic_cast<const ast::Col*>(item->expr.get())) {
                        size_t pos = 0;
                        while (pos < schema.columns.size() && schema.columns[pos].name != col->col_name)
                            ++pos;
                        if (std::find(keys.begin(), keys.end(), pos) == keys.end())
                            throw std::runtime_error("Delta SELECT column must be grouped");
                        Column output = schema.columns[pos];
                        if (!item->alias.empty())
                            output.name = item->alias;
                        columns.push_back(std::move(output));
                    } else if (const auto* agg = dynamic_cast<const ast::AggExpr*>(item->expr.get())) {
                        if (agg->func == ast::AGG_COUNT)
                            columns.push_back(
                                {item->alias.empty() ? "?column?" : item->alias, ColumnType::Int, 4, false});
                        else if ((agg->func == ast::AGG_MAX || agg->func == ast::AGG_MIN ||
                                  agg->func == ast::AGG_SUM) &&
                                 agg->col) {
                            size_t pos = 0;
                            while (pos < schema.columns.size() && schema.columns[pos].name != agg->col->col_name)
                                ++pos;
                            Column output = schema.columns[pos];
                            if (!item->alias.empty())
                                output.name = item->alias;
                            columns.push_back(std::move(output));
                        } else
                            throw std::runtime_error("unsupported Delta GROUP BY aggregate");
                    } else
                        throw std::runtime_error("unsupported Delta GROUP BY projection");
                }
                for (const auto& [name, group] : groups) {
                    std::vector<Cell> row;
                    for (size_t i = 0; i < select.select_items.size(); ++i) {
                        const auto& item = select.select_items[i];
                        if (const auto* col = dynamic_cast<const ast::Col*>(item->expr.get())) {
                            size_t pos = 0;
                            while (pos < schema.columns.size() && schema.columns[pos].name != col->col_name)
                                ++pos;
                            row.push_back(group.values[static_cast<size_t>(std::find(keys.begin(), keys.end(), pos) -
                                                                           keys.begin())]);
                        } else {
                            const auto& agg = static_cast<const ast::AggExpr&>(*item->expr);
                            const State& state = group.states[i];
                            if (agg.func == ast::AGG_COUNT) {
                                Cell value;
                                value.is_null = false;
                                value.integer = static_cast<int32_t>(state.count);
                                row.push_back(value);
                            } else if (state.seen) {
                                Cell value = state.value;
                                if (agg.func == ast::AGG_SUM) {
                                    value.is_null = false;
                                    if (columns[i].type == ColumnType::Int)
                                        value.integer = static_cast<int32_t>(state.sum);
                                    else
                                        value.floating = static_cast<float>(state.sum);
                                }
                                row.push_back(std::move(value));
                            } else
                                row.push_back({});
                        }
                    }
                    rows.push_back(std::move(row));
                }
                EmitCells(columns, rows, sink);
                if (implicit)
                    Commit(session, lock);
                return true;
            }
            const bool aggregate =
                std::any_of(select.select_items.begin(), select.select_items.end(),
                            [](const auto& item) { return item->expr->type == ast::AstType::AggExpr; });
            if (aggregate) {
                if (select.has_sort || select.has_limit || select.has_select_star ||
                    std::any_of(select.select_items.begin(), select.select_items.end(),
                                [](const auto& item) { return item->expr->type != ast::AstType::AggExpr; }))
                    throw std::runtime_error("unsupported Delta aggregate SELECT shape");
                struct State {
                    int64_t count = 0;
                    bool seen = false;
                    double sum = 0;
                    Cell min;
                    std::set<std::string> distinct;
                };
                std::vector<State> states(select.select_items.size());
                std::vector<size_t> positions(select.select_items.size());
                for (size_t i = 0; i < positions.size(); ++i) {
                    const auto& expr = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
                    if (expr.is_star) {
                        if (expr.func != ast::AGG_COUNT)
                            throw std::runtime_error("unsupported Delta aggregate");
                        continue;
                    }
                    if (!expr.col)
                        throw std::runtime_error("invalid Delta aggregate");
                    while (positions[i] < schema.columns.size() &&
                           schema.columns[positions[i]].name != expr.col->col_name)
                        ++positions[i];
                    if (positions[i] == schema.columns.size())
                        throw std::runtime_error("Delta aggregate column not found");
                }
                VisitRows(session, schema, select.conds, [&](epoch_si_poc::RowId, const epoch_si_poc::RowImage& image) {
                    const auto cells = DecodeRow(schema, image);
                    if (!Matches(schema, cells, select.conds))
                        return;
                    for (size_t i = 0; i < states.size(); ++i) {
                        const auto& expr = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
                        State& state = states[i];
                        if (expr.func == ast::AGG_COUNT && expr.is_star) {
                            ++state.count;
                            continue;
                        }
                        const Cell& value = cells[positions[i]];
                        if (value.is_null)
                            continue;
                        if (expr.func == ast::AGG_COUNT) {
                            if (expr.is_distinct) {
                                std::string key =
                                    std::to_string(static_cast<unsigned>(schema.columns[positions[i]].type)) + ":";
                                if (schema.columns[positions[i]].type == ColumnType::Int)
                                    key += std::to_string(value.integer);
                                else if (schema.columns[positions[i]].type == ColumnType::Float) {
                                    uint32_t bits;
                                    std::memcpy(&bits, &value.floating, sizeof(bits));
                                    key += std::to_string(bits);
                                } else
                                    key += value.text;
                                if (!state.distinct.insert(std::move(key)).second)
                                    continue;
                            }
                            ++state.count;
                        } else if (expr.func == ast::AGG_SUM) {
                            if (schema.columns[positions[i]].type == ColumnType::Int)
                                state.sum += value.integer;
                            else if (schema.columns[positions[i]].type == ColumnType::Float)
                                state.sum += value.floating;
                            else
                                throw std::runtime_error("Delta SUM requires numeric column");
                            state.seen = true;
                        } else if (expr.func == ast::AGG_MIN || expr.func == ast::AGG_MAX) {
                            const bool smaller = schema.columns[positions[i]].type == ColumnType::Int
                                                     ? value.integer < state.min.integer
                                                 : schema.columns[positions[i]].type == ColumnType::Float
                                                     ? value.floating < state.min.floating
                                                     : value.text < state.min.text;
                            if (!state.seen || (expr.func == ast::AGG_MIN ? smaller : !smaller))
                                state.min = value;
                            state.seen = true;
                        } else
                            throw std::runtime_error("unsupported Delta aggregate");
                    }
                });
                std::vector<Cell> values;
                for (size_t i = 0; i < states.size(); ++i) {
                    const auto& expr = static_cast<const ast::AggExpr&>(*select.select_items[i]->expr);
                    Cell value;
                    if (expr.func == ast::AGG_COUNT) {
                        value.is_null = false;
                        value.integer = static_cast<int32_t>(states[i].count);
                    } else if (states[i].seen) {
                        value = states[i].min;
                        if (expr.func == ast::AGG_SUM) {
                            value.is_null = false;
                            if (schema.columns[positions[i]].type == ColumnType::Int)
                                value.integer = static_cast<int32_t>(states[i].sum);
                            else
                                value.floating = static_cast<float>(states[i].sum);
                        }
                    }
                    values.push_back(std::move(value));
                }
                EmitRows(schema, select, {std::move(values)}, sink, true);
                if (implicit)
                    Commit(session, lock);
                return true;
            }
            std::vector<std::vector<Cell>> rows;
            bool query_started = false;
            VisitRows(session, schema, select.conds, [&](epoch_si_poc::RowId, const epoch_si_poc::RowImage& image) {
                auto cells = DecodeRow(schema, image);
                if (!Matches(schema, cells, select.conds))
                    return;
                if (!select.has_sort) {
                    EmitRows(schema, select, {std::move(cells)}, sink, false, query_started);
                    query_started = true;
                } else {
                    if (rows.size() == 4096)
                        throw std::runtime_error("Delta ORDER BY result exceeds 4096 rows");
                    rows.push_back(std::move(cells));
                }
            });
            if (select.has_sort) {
                const auto position = [&](const ast::Col& column) {
                    if (!column.tab_name.empty() && column.tab_name != schema.name &&
                        column.tab_name != select.tabs[0].alias)
                        throw std::runtime_error("unknown Delta qualifier");
                    for (size_t i = 0; i < schema.columns.size(); ++i)
                        if (schema.columns[i].name == column.col_name)
                            return i;
                    throw std::runtime_error("Delta ORDER BY column not found");
                };
                std::sort(rows.begin(), rows.end(), [&](const auto& left, const auto& right) {
                    for (const auto& item : select.order_by_items) {
                        const auto* column = dynamic_cast<const ast::Col*>(item->expr.get());
                        if (!column)
                            throw std::runtime_error("Delta ORDER BY expression unsupported");
                        const size_t pos = position(*column);
                        const int comparison =
                            schema.columns[pos].type == ColumnType::Int
                                ? (left[pos].integer < right[pos].integer ? -1 : left[pos].integer > right[pos].integer)
                            : schema.columns[pos].type == ColumnType::Float
                                ? (left[pos].floating < right[pos].floating ? -1
                                                                            : left[pos].floating > right[pos].floating)
                                : left[pos].text.compare(right[pos].text);
                        if (comparison != 0)
                            return item->orderby_dir == ast::OrderBy_DESC ? comparison > 0 : comparison < 0;
                    }
                    return false;
                });
            }
            if (select.has_limit && select.has_sort) {
                const size_t begin = std::min(rows.size(), static_cast<size_t>(std::max(0, select.offset)));
                const size_t end = std::min(rows.size(), begin + static_cast<size_t>(std::max(0, select.limit)));
                rows = std::vector<std::vector<Cell>>(rows.begin() + begin, rows.begin() + end);
            }
            if (select.has_sort)
                EmitRows(schema, select, rows, sink);
            else if (!query_started)
                EmitRows(schema, select, rows, sink);
            if (implicit)
                Commit(session, lock);
            return true;
        } else if (tree->type == ast::AstType::DeleteStmt || tree->type == ast::AstType::UpdateStmt) {
            const bool deleting = tree->type == ast::AstType::DeleteStmt;
            const std::string& name = deleting ? static_cast<const ast::DeleteStmt&>(*tree).tab_name
                                               : static_cast<const ast::UpdateStmt&>(*tree).tab_name;
            const auto& conditions = deleting ? static_cast<const ast::DeleteStmt&>(*tree).conds
                                              : static_cast<const ast::UpdateStmt&>(*tree).conds;
            const TableSchema& schema = Table(name);
            auto& txn = Txn(session);
            std::vector<std::pair<epoch_si_poc::RowId, epoch_si_poc::RowImage>> targets;
            VisitRows(session, schema, conditions, [&](epoch_si_poc::RowId id, const epoch_si_poc::RowImage& image) {
                targets.emplace_back(id, image);
            });
            for (const auto& [row_id, image] : targets) {
                auto cells = DecodeRow(schema, image);
                if (!Matches(schema, cells, conditions))
                    continue;
                if (deleting) {
                    db_.engine().Erase(txn, row_id);
                    continue;
                }
                const auto& update = static_cast<const ast::UpdateStmt&>(*tree);
                auto next = cells;
                for (const auto& clause : update.set_clauses) {
                    auto target = std::find_if(schema.columns.begin(), schema.columns.end(),
                                               [&](const Column& c) { return c.name == clause->col_name; });
                    if (target == schema.columns.end())
                        throw std::runtime_error("Delta UPDATE column not found");
                    const size_t target_pos = static_cast<size_t>(target - schema.columns.begin());
                    if (!clause->is_self_ref) {
                        next[target_pos] = Literal(*target, *clause->val);
                        continue;
                    }
                    auto source = std::find_if(schema.columns.begin(), schema.columns.end(),
                                               [&](const Column& c) { return c.name == clause->rhs_col->col_name; });
                    if (source == schema.columns.end() || source->type != target->type)
                        throw std::runtime_error("Delta UPDATE source mismatch");
                    Cell value = cells[static_cast<size_t>(source - schema.columns.begin())];
                    const auto apply = [&](ast::SetOp op, const ast::Value& operand) {
                        if (value.is_null)
                            return;
                        Cell rhs = Literal(*target, operand);
                        if (rhs.is_null || (target->type != ColumnType::Int && target->type != ColumnType::Float))
                            throw std::runtime_error("invalid Delta arithmetic");
                        if (target->type == ColumnType::Int) {
                            int64_t result = value.integer;
                            if (op == ast::SetOp::SELF_ADD)
                                result += rhs.integer;
                            else if (op == ast::SetOp::SELF_SUB)
                                result -= rhs.integer;
                            else if (op == ast::SetOp::SELF_MUL)
                                result *= rhs.integer;
                            else if (op == ast::SetOp::SELF_DIV) {
                                if (rhs.integer == 0)
                                    throw std::runtime_error("division by zero");
                                result /= rhs.integer;
                            }
                            if (result < INT32_MIN || result > INT32_MAX)
                                throw std::runtime_error("Delta INT arithmetic overflow");
                            value.integer = static_cast<int32_t>(result);
                        } else {
                            if (op == ast::SetOp::SELF_ADD)
                                value.floating = static_cast<float>(value.floating + rhs.floating);
                            else if (op == ast::SetOp::SELF_SUB)
                                value.floating = static_cast<float>(value.floating - rhs.floating);
                            else if (op == ast::SetOp::SELF_MUL)
                                value.floating = static_cast<float>(value.floating * rhs.floating);
                            else if (op == ast::SetOp::SELF_DIV) {
                                if (rhs.floating == 0)
                                    throw std::runtime_error("division by zero");
                                value.floating = static_cast<float>(value.floating / rhs.floating);
                            }
                        }
                    };
                    if (clause->op == ast::SetOp::ASSIGNMENT)
                        value = cells[static_cast<size_t>(source - schema.columns.begin())];
                    else
                        apply(clause->op, *clause->val);
                    for (const auto& term : clause->additional_terms)
                        apply(term.op, *term.val);
                    next[target_pos] = std::move(value);
                }
                db_.engine().PutImage(txn, row_id, EncodeRow(schema, next));
                AddOverlay(session.overlay, schema, next, row_id, &cells);
            }
        } else
            throw std::runtime_error("unsupported SQL for DeltaKernel");
        if (implicit)
            Commit(session, lock);
        return false;
    } catch (...) {
        if (implicit && session.txn)
            AbortLocked(session);
        throw;
    }
}

} // namespace deltakernel
