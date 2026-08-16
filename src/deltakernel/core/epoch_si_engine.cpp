#include "epoch_si_engine.h"

#include "file_wal.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <set>

namespace epoch_si_poc {
namespace {

constexpr uint32_t kFrameMagic = 0x31495345; // ESI1
constexpr uint32_t kFrameVersion = 3;
constexpr uint32_t kLegacyFrameVersion = 2;
constexpr uint32_t kFooterMagic = 0x454e4f44; // DONE
constexpr uint32_t kHeaderBytes = 48;
constexpr uint32_t kFooterBytes = 16;
constexpr uint32_t kHeaderCrcOffset = 44;
constexpr uint32_t kMaxFrameBytes = 16U * 1024U * 1024U;
constexpr uint32_t kMinTxnBytes = 12;
constexpr uint32_t kMinOpBytes = 19;

uint32_t Crc32(const uint8_t* data, size_t size) {
    uint32_t crc = 0xffffffffU;
    for (size_t i = 0; i < size; ++i) {
        crc ^= data[i];
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
        }
    }
    return ~crc;
}

template <typename T> void PutLe(std::vector<uint8_t>& out, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<uint8_t>(value >> (8 * i)));
    }
}

template <typename T> T GetLe(const std::vector<uint8_t>& in, size_t& pos, size_t end) {
    if (pos > end || end - pos < sizeof(T)) {
        throw std::runtime_error("truncated WAL field");
    }
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<T>(in[pos++]) << (8 * i);
    }
    return value;
}

template <typename T> void PutAtLe(std::vector<uint8_t>& out, size_t pos, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        out[pos + i] = static_cast<uint8_t>(value >> (8 * i));
    }
}

void ValidateRow(const RowImage& row) {
    if (row.deleted && (!row.bytes.empty() || !row.claims.empty())) {
        throw std::invalid_argument("deleted row has payload or claims");
    }
    if (row.bytes.size() > kMaxFrameBytes) {
        throw std::invalid_argument("row image too large");
    }
    if (row.claims.size() > std::numeric_limits<uint16_t>::max()) {
        throw std::invalid_argument("too many row constraint claims");
    }
    for (size_t i = 0; i < row.claims.size(); ++i) {
        if (row.claims[i].bytes.size() > kMaxFrameBytes || (i != 0 && !(row.claims[i - 1] < row.claims[i]))) {
            throw std::invalid_argument("row claims must be bounded and unique");
        }
    }
}

RowImage DecodeRow(const std::vector<uint8_t>& in, size_t& pos, size_t end, bool deleted) {
    const uint32_t image_bytes = GetLe<uint32_t>(in, pos, end);
    const uint16_t claim_count = GetLe<uint16_t>(in, pos, end);
    if ((deleted && (image_bytes != 0 || claim_count != 0)) || image_bytes > end - pos ||
        claim_count > (end - pos - image_bytes) / 8) {
        throw std::runtime_error("invalid WAL row image");
    }
    RowImage row;
    row.deleted = deleted;
    row.bytes.assign(in.begin() + static_cast<std::ptrdiff_t>(pos),
                     in.begin() + static_cast<std::ptrdiff_t>(pos + image_bytes));
    pos += image_bytes;
    row.claims.reserve(claim_count);
    for (uint16_t i = 0; i < claim_count; ++i) {
        ConstraintClaim claim;
        claim.constraint_id = GetLe<uint32_t>(in, pos, end);
        const uint32_t claim_bytes = GetLe<uint32_t>(in, pos, end);
        if (claim_bytes > end - pos) {
            throw std::runtime_error("invalid WAL row claim");
        }
        claim.bytes.assign(in.begin() + static_cast<std::ptrdiff_t>(pos),
                           in.begin() + static_cast<std::ptrdiff_t>(pos + claim_bytes));
        pos += claim_bytes;
        if (!row.claims.empty() && !(row.claims.back() < claim)) {
            throw std::runtime_error("invalid WAL row claim");
        }
        row.claims.push_back(std::move(claim));
    }
    return row;
}

void EncodeRow(std::vector<uint8_t>& out, const RowImage& row) {
    ValidateRow(row);
    PutLe<uint32_t>(out, static_cast<uint32_t>(row.bytes.size()));
    PutLe<uint16_t>(out, static_cast<uint16_t>(row.claims.size()));
    out.insert(out.end(), row.bytes.begin(), row.bytes.end());
    for (const auto& claim : row.claims) {
        PutLe<uint32_t>(out, claim.constraint_id);
        PutLe<uint32_t>(out, static_cast<uint32_t>(claim.bytes.size()));
        out.insert(out.end(), claim.bytes.begin(), claim.bytes.end());
    }
}

} // namespace

std::vector<uint8_t> EpochSiEngine::EncodeFrame(Epoch epoch, const std::vector<Txn*>& txns,
                                                const std::vector<uint64_t>& commit_seqs) {
    if (txns.empty() || txns.size() != commit_seqs.size()) {
        throw std::invalid_argument("WAL frame requires committed transactions");
    }
    if (txns.size() > std::numeric_limits<uint32_t>::max())
        throw std::invalid_argument("too many WAL transactions");
    std::vector<uint8_t> out;
    out.reserve(kHeaderBytes + kFooterBytes + txns.size() * kMinTxnBytes);
    PutLe<uint32_t>(out, kFrameMagic);
    PutLe<uint32_t>(out, kFrameVersion);
    PutLe<uint32_t>(out, kHeaderBytes);
    PutLe<uint32_t>(out, 0); // frame bytes
    PutLe<uint64_t>(out, epoch);
    PutLe<uint64_t>(out, commit_seqs.front());
    PutLe<uint32_t>(out, static_cast<uint32_t>(txns.size()));
    uint32_t operation_count = 0;
    for (const Txn* txn : txns) {
        if (txn->writes_.size() > std::numeric_limits<uint32_t>::max() - operation_count) {
            throw std::invalid_argument("too many WAL operations");
        }
        operation_count += static_cast<uint32_t>(txn->writes_.size());
    }
    PutLe<uint32_t>(out, operation_count);
    PutLe<uint32_t>(out, 0); // payload bytes
    PutLe<uint32_t>(out, 0); // header CRC

    const size_t payload_start = out.size();
    for (size_t i = 0; i < txns.size(); ++i) {
        PutLe<uint64_t>(out, commit_seqs[i]);
        PutLe<uint32_t>(out, static_cast<uint32_t>(txns[i]->writes_.size()));
        for (const auto& [row_id, row] : txns[i]->writes_) {
            PutLe<uint32_t>(out, row_id.table_id);
            PutLe<uint64_t>(out, row_id.local_id);
            PutLe<uint8_t>(out, row.deleted ? 2 : 1);
            EncodeRow(out, row);
        }
    }
    const size_t payload_bytes = out.size() - payload_start;
    if (payload_bytes > std::numeric_limits<uint32_t>::max() || out.size() + kFooterBytes > kMaxFrameBytes) {
        throw std::invalid_argument("WAL frame too large");
    }
    const uint32_t frame_bytes = static_cast<uint32_t>(out.size() + kFooterBytes);
    PutAtLe<uint32_t>(out, 12, frame_bytes);
    PutAtLe<uint32_t>(out, 40, static_cast<uint32_t>(payload_bytes));
    PutAtLe<uint32_t>(out, kHeaderCrcOffset, Crc32(out.data(), kHeaderCrcOffset));
    const uint32_t payload_crc = Crc32(out.data() + payload_start, payload_bytes);

    const size_t footer_start = out.size();
    PutLe<uint32_t>(out, kFooterMagic);
    PutLe<uint32_t>(out, frame_bytes);
    PutLe<uint32_t>(out, payload_crc);
    PutLe<uint32_t>(out, Crc32(out.data() + footer_start, 12));
    return out;
}

EpochSiEngine::EpochSiEngine(BaseImage base, Epoch base_epoch) : EpochSiEngine(std::move(base), {}, base_epoch) {}

EpochSiEngine::EpochSiEngine(BaseImage base, ImmutableTables tables, Epoch base_epoch)
    : base_(std::move(base)), immutable_tables_(std::move(tables)), identity_(std::make_shared<OwnerState>()),
      published_epoch_(base_epoch) {
    for (const auto& [row_id, row] : base_) {
        if (immutable_tables_.count(row_id.table_id))
            continue; // A per-table base supersedes legacy rows for that table.
        if (row_id.table_id == 0 || row_id.local_id == std::numeric_limits<uint64_t>::max()) {
            throw std::invalid_argument("invalid or exhausted base RowId");
        }
        next_row_id_[row_id.table_id] = std::max(next_row_id_[row_id.table_id], row_id.local_id + 1);
        ValidateRow(row);
        for (const auto& claim : row.claims) {
            if (!claim_owner_.emplace(claim, row_id).second)
                throw std::invalid_argument("base image violates unique claim");
            last_claim_epoch_[claim] = base_epoch;
        }
        last_row_epoch_[row_id] = base_epoch;
    }
    for (const auto& [table_id, table] : immutable_tables_) {
        if (!table || table_id == 0 || table_id != table->table_id())
            throw std::invalid_argument("invalid immutable table source");
        next_row_id_[table_id] = table->next_local_id();
    }
}

EpochSiEngine::~EpochSiEngine() {
    if (pending_prepared_.load(std::memory_order_acquire))
        std::terminate();
    if (identity_) {
        identity_->valid.store(false, std::memory_order_release);
    }
}

EpochSiEngine::Txn::~Txn() {
    if (prepared_)
        std::terminate();
    Finish();
}

void EpochSiEngine::Txn::Finish() noexcept {
    if (owner_ && active_) {
        active_ = false;
        if (owner_->active_count.fetch_sub(1, std::memory_order_acq_rel) == 0)
            std::terminate();
    }
}

EpochSiEngine::Txn::Txn(Txn&& other) noexcept
    : owner_(std::move(other.owner_)), start_epoch_(other.start_epoch_), active_(other.active_), prepared_(other.prepared_),
      writes_(std::move(other.writes_)), inserts_(std::move(other.inserts_)) {
    if (other.prepared_)
        std::terminate();
    other.start_epoch_ = 0;
    other.active_ = false;
    other.prepared_ = false;
}

EpochSiEngine::EpochSiEngine(EpochSiEngine&& other) noexcept
    : base_(std::move(other.base_)), immutable_tables_(std::move(other.immutable_tables_)),
      versions_(std::move(other.versions_)), last_row_epoch_(std::move(other.last_row_epoch_)),
      last_claim_epoch_(std::move(other.last_claim_epoch_)), claim_owner_(std::move(other.claim_owner_)),
      next_row_id_(std::move(other.next_row_id_)), identity_(std::move(other.identity_)),
      published_epoch_(other.published_epoch_), next_commit_seq_(other.next_commit_seq_),
      version_count_(other.version_count_), volatile_wal_(std::move(other.volatile_wal_)),
      recovery_wal_image_(std::move(other.recovery_wal_image_)), file_wal_(std::move(other.file_wal_)),
      wal_frame_count_(other.wal_frame_count_), wal_transaction_count_(other.wal_transaction_count_),
      dirty_tables_(std::move(other.dirty_tables_)),
      last_publication_staged_entries_(other.last_publication_staged_entries_),
      last_publication_staged_versions_(other.last_publication_staged_versions_),
      last_install_version_nodes_(other.last_install_version_nodes_), crash_point_(other.crash_point_),
      crash_position_(other.crash_position_), poisoned_(other.poisoned_.load(std::memory_order_acquire)),
      diagnostics_(std::move(other.diagnostics_)) {
    if (other.pending_prepared_.load(std::memory_order_acquire))
        std::terminate();
    SetDiagnostics(diagnostics_);
    other.poisoned_.store(true, std::memory_order_release);
    other.identity_.reset();
}

EpochSiEngine& EpochSiEngine::operator=(EpochSiEngine&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    if (pending_prepared_.load(std::memory_order_acquire) ||
        other.pending_prepared_.load(std::memory_order_acquire)) {
        std::terminate();
    }
    const auto retained_diagnostics = diagnostics_;
    if (identity_) {
        identity_->valid.store(false, std::memory_order_release);
    }
    base_ = std::move(other.base_);
    immutable_tables_ = std::move(other.immutable_tables_);
    versions_ = std::move(other.versions_);
    last_row_epoch_ = std::move(other.last_row_epoch_);
    last_claim_epoch_ = std::move(other.last_claim_epoch_);
    claim_owner_ = std::move(other.claim_owner_);
    next_row_id_ = std::move(other.next_row_id_);
    identity_ = std::move(other.identity_);
    published_epoch_ = other.published_epoch_;
    next_commit_seq_ = other.next_commit_seq_;
    version_count_ = other.version_count_;
    volatile_wal_ = std::move(other.volatile_wal_);
    recovery_wal_image_ = std::move(other.recovery_wal_image_);
    file_wal_ = std::move(other.file_wal_);
    wal_frame_count_ = other.wal_frame_count_;
    wal_transaction_count_ = other.wal_transaction_count_;
    dirty_tables_ = std::move(other.dirty_tables_);
    last_publication_staged_entries_ = other.last_publication_staged_entries_;
    last_publication_staged_versions_ = other.last_publication_staged_versions_;
    last_install_version_nodes_ = other.last_install_version_nodes_;
    crash_point_ = other.crash_point_;
    crash_position_ = other.crash_position_;
    poisoned_.store(other.poisoned_.load(std::memory_order_acquire), std::memory_order_release);
    diagnostics_ = other.diagnostics_ ? std::move(other.diagnostics_) : retained_diagnostics;
    SetDiagnostics(diagnostics_);
    other.poisoned_.store(true, std::memory_order_release);
    other.identity_.reset();
    return *this;
}

EpochSiEngine EpochSiEngine::Recover(BaseImage base, const std::vector<uint8_t>& wal_image, Epoch base_epoch) {
    return Recover(std::move(base), {}, wal_image, base_epoch);
}

EpochSiEngine EpochSiEngine::Recover(BaseImage base, ImmutableTables tables, const std::vector<uint8_t>& wal_image,
                                     Epoch base_epoch) {
    EpochSiEngine engine(std::move(base), std::move(tables), base_epoch);
    size_t frame_start = 0;
    size_t last_intact_offset = 0;
    while (frame_start < wal_image.size()) {
        if (wal_image.size() - frame_start < kHeaderBytes) {
            break; // A partial final header is a torn tail.
        }
        size_t pos = frame_start;
        const uint32_t magic = GetLe<uint32_t>(wal_image, pos, frame_start + kHeaderBytes);
        const uint32_t version = GetLe<uint32_t>(wal_image, pos, frame_start + kHeaderBytes);
        const uint32_t header_bytes = GetLe<uint32_t>(wal_image, pos, frame_start + kHeaderBytes);
        const uint32_t frame_bytes = GetLe<uint32_t>(wal_image, pos, frame_start + kHeaderBytes);
        const Epoch epoch = GetLe<uint64_t>(wal_image, pos, frame_start + kHeaderBytes);
        const uint64_t first_commit_seq = GetLe<uint64_t>(wal_image, pos, frame_start + kHeaderBytes);
        const uint32_t txn_count = GetLe<uint32_t>(wal_image, pos, frame_start + kHeaderBytes);
        const uint32_t operation_count = GetLe<uint32_t>(wal_image, pos, frame_start + kHeaderBytes);
        const uint32_t payload_bytes = GetLe<uint32_t>(wal_image, pos, frame_start + kHeaderBytes);
        const uint32_t header_crc = GetLe<uint32_t>(wal_image, pos, frame_start + kHeaderBytes);
        if (magic != kFrameMagic || (version != kFrameVersion && version != kLegacyFrameVersion) ||
            header_bytes != kHeaderBytes || header_crc != Crc32(wal_image.data() + frame_start, kHeaderCrcOffset)) {
            throw std::runtime_error("corrupt WAL frame header");
        }
        if (frame_bytes < kHeaderBytes + kFooterBytes || frame_bytes > kMaxFrameBytes ||
            payload_bytes != frame_bytes - kHeaderBytes - kFooterBytes) {
            throw std::runtime_error("invalid WAL frame length");
        }
        if (frame_bytes > wal_image.size() - frame_start) {
            break; // A valid complete header followed by a partial final frame is a torn tail.
        }
        const size_t frame_end = frame_start + frame_bytes;
        const size_t payload_start = frame_start + kHeaderBytes;
        const size_t footer_start = frame_end - kFooterBytes;
        size_t footer_pos = footer_start;
        const uint32_t footer_magic = GetLe<uint32_t>(wal_image, footer_pos, frame_end);
        const uint32_t footer_frame_bytes = GetLe<uint32_t>(wal_image, footer_pos, frame_end);
        const uint32_t payload_crc = GetLe<uint32_t>(wal_image, footer_pos, frame_end);
        const uint32_t footer_crc = GetLe<uint32_t>(wal_image, footer_pos, frame_end);
        if (footer_magic != kFooterMagic || footer_frame_bytes != frame_bytes ||
            footer_crc != Crc32(wal_image.data() + footer_start, 12) ||
            payload_crc != Crc32(wal_image.data() + payload_start, payload_bytes)) {
            throw std::runtime_error("corrupt WAL frame footer or payload");
        }
        const uint64_t minimum_payload =
            static_cast<uint64_t>(txn_count) * kMinTxnBytes + static_cast<uint64_t>(operation_count) * kMinOpBytes;
        if (txn_count == 0 || (version == kLegacyFrameVersion && txn_count > operation_count) ||
            txn_count > payload_bytes / kMinTxnBytes || operation_count > payload_bytes / kMinOpBytes ||
            minimum_payload > payload_bytes) {
            throw std::runtime_error("impossible WAL counts");
        }
        if (engine.published_epoch_ == std::numeric_limits<Epoch>::max() || epoch != engine.published_epoch_ + 1) {
            throw std::runtime_error("non-contiguous WAL epoch");
        }
        if (first_commit_seq != engine.next_commit_seq_) {
            throw std::runtime_error("non-contiguous WAL commit sequence");
        }

        std::vector<Txn> recovered_txns;
        recovered_txns.reserve(txn_count);
        std::vector<Txn*> recovered_ptrs;
        recovered_ptrs.reserve(txn_count);
        std::set<RowId> batch_rows;
        std::set<ConstraintClaim> batch_claims;
        uint32_t seen_ops = 0;
        uint64_t expected_seq = first_commit_seq;
        size_t payload_pos = payload_start;
        for (uint32_t t = 0; t < txn_count; ++t) {
            if (expected_seq == 0 || expected_seq == std::numeric_limits<uint64_t>::max()) {
                throw std::runtime_error("WAL commit sequence overflow");
            }
            const uint64_t seq = GetLe<uint64_t>(wal_image, payload_pos, footer_start);
            const uint32_t op_count = GetLe<uint32_t>(wal_image, payload_pos, footer_start);
            if (seq != expected_seq || (version == kLegacyFrameVersion && op_count == 0) ||
                op_count > (footer_start - payload_pos) / kMinOpBytes) {
                throw std::runtime_error("invalid WAL transaction");
            }
            recovered_txns.emplace_back();
            Txn& txn = recovered_txns.back();
            txn.owner_ = engine.identity_;
            txn.start_epoch_ = engine.published_epoch_;
            for (uint32_t op = 0; op < op_count; ++op) {
                RowId row_id{GetLe<uint32_t>(wal_image, payload_pos, footer_start),
                             GetLe<uint64_t>(wal_image, payload_pos, footer_start)};
                const uint8_t kind = GetLe<uint8_t>(wal_image, payload_pos, footer_start);
                if (row_id.table_id == 0 || (kind != 1 && kind != 2)) {
                    throw std::runtime_error("invalid WAL operation");
                }
                RowImage row = DecodeRow(wal_image, payload_pos, footer_start, kind == 2);
                const auto old = engine.ReadCommitted(row_id, engine.published_epoch_);
                if (row.deleted && !old) {
                    throw std::runtime_error("WAL deletes a missing row");
                }
                if (!row.deleted && !old) {
                    if (engine.base_.count(row_id) || engine.versions_.count(row_id)) {
                        throw std::runtime_error("WAL resurrects a stable RowId");
                    }
                    txn.inserts_.insert(row_id);
                }
                if (!txn.writes_.emplace(row_id, std::move(row)).second) {
                    throw std::runtime_error("duplicate RowId in WAL transaction");
                }
                ++seen_ops;
            }
            if (engine.Certify(txn, batch_rows, batch_claims) != CommitStatus::kCommitted) {
                throw std::runtime_error("WAL violates row or unique-key certification");
            }
            recovered_ptrs.push_back(&txn);
            ++expected_seq;
        }
        if (seen_ops != operation_count || payload_pos != footer_start) {
            throw std::runtime_error("WAL payload count mismatch");
        }
        engine.Install(recovered_ptrs, epoch);
        engine.published_epoch_ = epoch;
        engine.next_commit_seq_ = expected_seq;
        if (engine.wal_transaction_count_ > std::numeric_limits<size_t>::max() - txn_count) {
            throw std::runtime_error("WAL transaction census overflow");
        }
        ++engine.wal_frame_count_;
        engine.wal_transaction_count_ += txn_count;
        frame_start = frame_end;
        last_intact_offset = frame_end;
    }
    engine.volatile_wal_.assign(wal_image.begin(), wal_image.begin() + last_intact_offset);
    engine.recovery_wal_image_ = engine.volatile_wal_;
    return engine;
}

EpochSiEngine EpochSiEngine::OpenFile(BaseImage base, const std::string& wal_path, Epoch base_epoch) {
    return OpenFile(std::move(base), {}, wal_path, base_epoch);
}

EpochSiEngine EpochSiEngine::OpenFile(BaseImage base, ImmutableTables tables, const std::string& wal_path,
                                      Epoch base_epoch) {
    auto file = std::make_unique<FileWal>(wal_path, FileWal::OpenMode::kExisting);
    const std::vector<uint8_t> image = file->ReadAll();
    EpochSiEngine engine = Recover(std::move(base), std::move(tables), image, base_epoch);
    const size_t intact_bytes = engine.recovery_wal_image_.size();
    if (intact_bytes != image.size()) {
        file->TruncateAndSync(intact_bytes);
    }
    engine.volatile_wal_.clear();
    engine.recovery_wal_image_.clear();
    engine.file_wal_ = std::move(file);
    return engine;
}

EpochSiEngine EpochSiEngine::OpenFileAt(BaseImage base, ImmutableTables tables, int directory_fd,
                                        const std::string& wal_name, Epoch base_epoch) {
    auto file = std::make_unique<FileWal>(directory_fd, wal_name, FileWal::OpenMode::kExisting);
    const std::vector<uint8_t> image = file->ReadAll();
    EpochSiEngine engine = Recover(std::move(base), std::move(tables), image, base_epoch);
    const size_t intact_bytes = engine.recovery_wal_image_.size();
    if (intact_bytes != image.size()) {
        file->TruncateAndSync(intact_bytes);
    }
    engine.volatile_wal_.clear();
    engine.recovery_wal_image_.clear();
    engine.file_wal_ = std::move(file);
    return engine;
}

EpochSiEngine EpochSiEngine::CreateFile(BaseImage base, const std::string& wal_path, Epoch base_epoch) {
    auto file = std::make_unique<FileWal>(wal_path, FileWal::OpenMode::kCreateNew);
    file->Sync();
    EpochSiEngine engine(std::move(base), base_epoch);
    engine.file_wal_ = std::move(file);
    return engine;
}

size_t EpochSiEngine::durable_wal_bytes() const {
    return file_wal_ ? file_wal_->size() : recovery_wal_image_.size();
}

size_t EpochSiEngine::wal_write_calls_for_test() const {
    return file_wal_ ? file_wal_->write_calls_for_test() : 0;
}

size_t EpochSiEngine::wal_sync_calls_for_test() const {
    return file_wal_ ? file_wal_->sync_calls_for_test() : 0;
}

EpochSiEngine::Txn EpochSiEngine::Begin() {
    if (poisoned_.load(std::memory_order_acquire) || !identity_ ||
        !identity_->valid.load(std::memory_order_acquire)) {
        throw std::logic_error("crashed engine");
    }
    Txn txn;
    size_t active = identity_->active_count.load(std::memory_order_relaxed);
    do {
        if (active == std::numeric_limits<size_t>::max())
            throw std::overflow_error("active transaction count exhausted");
    } while (!identity_->active_count.compare_exchange_weak(active, active + 1, std::memory_order_acq_rel,
                                                            std::memory_order_relaxed));
    txn.owner_ = identity_;
    txn.start_epoch_ = published_epoch_;
    txn.active_ = true;
    return txn;
}

BaseImage EpochSiEngine::MaterializePublished() const {
    if (poisoned_.load(std::memory_order_acquire) || !identity_ ||
        !identity_->valid.load(std::memory_order_acquire)) {
        throw std::logic_error("crashed engine");
    }
    std::set<RowId> ids;
    for (const auto& [id, row] : base_) {
        if (!immutable_tables_.count(id.table_id))
            ids.insert(id);
    }
    for (const auto& [table_id, table] : immutable_tables_) {
        if (table->visible_from() <= published_epoch_) {
            for (uint64_t local_id = 0; local_id < table->row_count(); ++local_id)
                ids.insert({table_id, local_id});
        }
    }
    for (const auto& [id, versions] : versions_) {
        ids.insert(id);
    }
    BaseImage snapshot;
    for (RowId id : ids) {
        if (auto row = ReadCommitted(id, published_epoch_)) {
            snapshot.emplace(id, std::move(*row));
        }
    }
    return snapshot;
}

void EpochSiEngine::RequireActive(const Txn& txn) const {
    if (poisoned_.load(std::memory_order_acquire) || !identity_ ||
        !identity_->valid.load(std::memory_order_acquire) || txn.owner_ != identity_ || !txn.active_) {
        throw std::logic_error("foreign, inactive, or crashed transaction");
    }
}

std::optional<Row> EpochSiEngine::ReadCommitted(RowId row_id, Epoch snapshot) const {
    const auto versions = versions_.find(row_id);
    if (versions != versions_.end()) {
        for (const Version* version = versions->second.get(); version != nullptr; version = version->older.get()) {
            if (version->epoch <= snapshot) {
                if (diagnostics_)
                    diagnostics_->version_hits.fetch_add(1, std::memory_order_relaxed);
                return version->row.deleted ? std::nullopt : std::optional<Row>(version->row);
            }
        }
    }
    const auto immutable = immutable_tables_.find(row_id.table_id);
    if (immutable != immutable_tables_.end()) {
        return immutable->second->visible_from() <= snapshot ? immutable->second->Read(row_id.local_id) : std::nullopt;
    }
    const auto base = base_.find(row_id);
    if (diagnostics_ && base != base_.end())
        diagnostics_->base_hits.fetch_add(1, std::memory_order_relaxed);
    return base == base_.end() || base->second.deleted ? std::nullopt : std::optional<Row>(base->second);
}

Epoch EpochSiEngine::LastExistingRowEpoch(RowId row_id) const {
    if (const auto found = last_row_epoch_.find(row_id); found != last_row_epoch_.end())
        return found->second;
    // Base rows and published versions always have explicit metadata. Put/Erase (and WAL decode) prove existence, so
    // an existing write without it can only be the untouched immutable base row.
    if (const auto table = immutable_tables_.find(row_id.table_id); table != immutable_tables_.end())
        return table->second->visible_from();
    throw std::logic_error("existing transaction write has no committed source");
}

std::optional<Row> EpochSiEngine::Read(const Txn& txn, RowId row_id) const {
    RequireActive(txn);
    const auto local = txn.writes_.find(row_id);
    if (local != txn.writes_.end()) {
        if (diagnostics_)
            diagnostics_->private_hits.fetch_add(1, std::memory_order_relaxed);
        return local->second.deleted ? std::nullopt : std::optional<Row>(local->second);
    }
    return ReadCommitted(row_id, txn.start_epoch_);
}

std::vector<std::pair<RowId, Row>> EpochSiEngine::Scan(const Txn& txn, TableId table_id) const {
    RequireActive(txn);
    std::vector<std::pair<RowId, Row>> rows;
    VisitScan(txn, table_id, [&](RowId id, const Row& row) { rows.emplace_back(id, row); });
    return rows;
}

void EpochSiEngine::VisitScan(const Txn& txn, TableId table_id,
                              const std::function<void(RowId, const Row&)>& visitor) const {
    RequireActive(txn);
    const auto diagnostics = diagnostics_;
    std::set<RowId> legacy_base_ids;
    const auto immutable = immutable_tables_.find(table_id);
    const bool immutable_visible =
        immutable != immutable_tables_.end() && immutable->second->visible_from() <= txn.start_epoch_;
    if (immutable_visible) {
        auto version = versions_.lower_bound({table_id, 0});
        auto local = txn.writes_.lower_bound({table_id, 0});
        immutable->second->Visit([&](uint64_t local_id, Row&& row) {
            const RowId id{table_id, local_id};
            while (local != txn.writes_.end() && local->first.table_id == table_id && local->first < id)
                ++local;
            if (local != txn.writes_.end() && local->first == id) {
                if (diagnostics)
                    diagnostics->scan_private_entries_examined.fetch_add(1, std::memory_order_relaxed);
                if (!local->second.deleted)
                    visitor(id, local->second);
                ++local;
                return;
            }
            while (version != versions_.end() && version->first.table_id == table_id && version->first < id)
                ++version;
            if (version != versions_.end() && version->first == id) {
                if (diagnostics)
                    diagnostics->scan_version_entries_examined.fetch_add(1, std::memory_order_relaxed);
                for (const Version* candidate = version->second.get(); candidate != nullptr;
                     candidate = candidate->older.get()) {
                    if (candidate->epoch <= txn.start_epoch_) {
                        if (!candidate->row.deleted)
                            visitor(id, candidate->row);
                        ++version;
                        return;
                    }
                }
                ++version;
            }
            visitor(id, row);
        });
    } else {
        for (auto base = base_.lower_bound({table_id, 0}); base != base_.end() && base->first.table_id == table_id;
             ++base) {
            const RowId id = base->first;
            legacy_base_ids.insert(id);
            if (auto visible = Read(txn, id))
                visitor(id, *visible);
        }
    }
    std::set<RowId> overlay_ids;
    const uint64_t first_overlay_id = immutable_visible ? immutable->second->next_local_id() : 0;
    for (auto version = versions_.lower_bound({table_id, first_overlay_id});
         version != versions_.end() && version->first.table_id == table_id; ++version) {
        if (diagnostics)
            diagnostics->scan_version_entries_examined.fetch_add(1, std::memory_order_relaxed);
        if (!legacy_base_ids.count(version->first))
            overlay_ids.insert(version->first);
    }
    for (auto local = txn.writes_.lower_bound({table_id, first_overlay_id});
         local != txn.writes_.end() && local->first.table_id == table_id; ++local) {
        if (diagnostics)
            diagnostics->scan_private_entries_examined.fetch_add(1, std::memory_order_relaxed);
        if (!legacy_base_ids.count(local->first))
            overlay_ids.insert(local->first);
    }
    for (RowId id : overlay_ids) {
        if (const auto local = txn.writes_.find(id); local != txn.writes_.end()) {
            if (!local->second.deleted)
                visitor(id, local->second);
            continue;
        }
        const auto versions = versions_.find(id);
        if (versions == versions_.end())
            continue;
        for (const Version* version = versions->second.get(); version != nullptr; version = version->older.get()) {
            if (version->epoch <= txn.start_epoch_) {
                if (!version->row.deleted)
                    visitor(id, version->row);
                break;
            }
        }
    }
}

void EpochSiEngine::VisitLatestVersions(const std::function<void(RowId, const Row&)>& visitor) const {
    for (const auto& [id, versions] : versions_)
        if (versions && !versions->row.deleted)
            visitor(id, versions->row);
}

bool EpochSiEngine::CanInstallPristineTable(TableId table_id) const {
    if (table_id == 0 || immutable_tables_.count(table_id) || next_row_id_.count(table_id))
        return false;
    for (const auto& [id, row] : base_)
        if (id.table_id == table_id)
            return false;
    for (const auto& [id, rows] : versions_)
        if (id.table_id == table_id)
            return false;
    for (const auto& [id, epoch] : last_row_epoch_)
        if (id.table_id == table_id)
            return false;
    return true;
}

size_t EpochSiEngine::immutable_index_bytes() const {
    size_t bytes = 0;
    for (const auto& [table_id, table] : immutable_tables_)
        bytes += table->index_bytes();
    return bytes;
}

size_t EpochSiEngine::immutable_read_probes_for_test() const {
    return diagnostics_ ? diagnostics_->immutable_reads.load(std::memory_order_relaxed) : 0;
}

void EpochSiEngine::SetDiagnostics(std::shared_ptr<DeltaDiagnostics> diagnostics) {
    diagnostics_ = std::move(diagnostics);
    if (file_wal_)
        file_wal_->SetDiagnostics(diagnostics_);
    for (const auto& [table_id, table] : immutable_tables_)
        table->SetDiagnostics(diagnostics_);
}

RowId EpochSiEngine::InsertImage(Txn& txn, TableId table_id, RowImage row) {
    RequireActive(txn);
    if (txn.prepared_)
        throw std::logic_error("cannot modify a prepared transaction");
    if (table_id == 0) {
        throw std::invalid_argument("table id zero is reserved");
    }
    if (row.deleted)
        throw std::invalid_argument("cannot insert a deleted row image");
    ValidateRow(row);
    RowId id;
    {
        std::lock_guard<std::mutex> lock(row_id_allocator_mutex_);
        uint64_t& next = next_row_id_[table_id];
        if (next == std::numeric_limits<uint64_t>::max())
            throw std::overflow_error("RowId exhausted");
        id = {table_id, next++};
    }
    txn.writes_[id] = std::move(row);
    txn.inserts_.insert(id);
    return id;
}

void EpochSiEngine::PutImage(Txn& txn, RowId row_id, RowImage row) {
    RequireActive(txn);
    if (txn.prepared_)
        throw std::logic_error("cannot modify a prepared transaction");
    if (!Read(txn, row_id)) {
        throw std::invalid_argument("Put target does not exist in transaction view");
    }
    if (row.deleted)
        throw std::invalid_argument("PutImage requires a live row image");
    ValidateRow(row);
    txn.writes_[row_id] = std::move(row);
}

void EpochSiEngine::Erase(Txn& txn, RowId row_id) {
    RequireActive(txn);
    if (txn.prepared_)
        throw std::logic_error("cannot modify a prepared transaction");
    if (!Read(txn, row_id)) {
        throw std::invalid_argument("Erase target does not exist in transaction view");
    }
    if (txn.inserts_.erase(row_id) != 0) {
        txn.writes_.erase(row_id);
        return;
    }
    txn.writes_[row_id] = RowImage{{}, {}, true};
}

void EpochSiEngine::Abort(Txn& txn) {
    RequireActive(txn);
    if (txn.prepared_)
        throw std::logic_error("cannot abort a prepared transaction");
    txn.writes_.clear();
    txn.inserts_.clear();
    txn.Finish();
}

CommitStatus EpochSiEngine::Certify(const Txn& txn, std::set<RowId>& batch_rows,
                                    std::set<ConstraintClaim>& batch_claims) const {
    bool row_conflict = false;
    bool claim_conflict = false;
    const bool claim_free = claim_owner_.empty() && last_claim_epoch_.empty() && batch_claims.empty() &&
                            std::all_of(txn.writes_.begin(), txn.writes_.end(),
                                        [](const auto& write) { return write.second.claims.empty(); });
    std::set<ConstraintClaim> footprint;
    for (const auto& [id, row] : txn.writes_) {
        const bool inserted = txn.inserts_.count(id) != 0;
        if ((!inserted && LastExistingRowEpoch(id) > txn.start_epoch_) || batch_rows.count(id)) {
            row_conflict = true;
        }
        if (!claim_free && !inserted) {
            const auto old = ReadCommitted(id, published_epoch_);
            if (old) {
                footprint.insert(old->claims.begin(), old->claims.end());
            }
        }
        if (!claim_free)
            footprint.insert(row.claims.begin(), row.claims.end());
    }
    if (claim_free) {
        if (row_conflict)
            return CommitStatus::kWriteConflict;
        for (const auto& [id, row] : txn.writes_)
            batch_rows.insert(id);
        return CommitStatus::kCommitted;
    }
    for (const auto& claim : footprint) {
        const auto last_claim = last_claim_epoch_.find(claim);
        if ((last_claim != last_claim_epoch_.end() && last_claim->second > txn.start_epoch_) ||
            batch_claims.count(claim)) {
            claim_conflict = true;
        }
    }
    // Keep only the claims this transaction changes.  Copying claim_owner_ here
    // made certification linear in all resident unique keys for every writer.
    std::map<ConstraintClaim, std::optional<RowId>> projected_owners;
    for (const auto& [id, row] : txn.writes_) {
        if (txn.inserts_.count(id))
            continue;
        const auto old = ReadCommitted(id, published_epoch_);
        if (old) {
            for (const auto& claim : old->claims) {
                const auto owner = projected_owners.find(claim);
                if (owner != projected_owners.end()) {
                    if (owner->second && *owner->second == id) {
                        owner->second.reset();
                    }
                } else {
                    const auto committed_owner = claim_owner_.find(claim);
                    if (committed_owner != claim_owner_.end() && committed_owner->second == id) {
                        projected_owners.emplace(claim, std::nullopt);
                    }
                }
            }
        }
    }
    for (const auto& [id, row] : txn.writes_) {
        for (const auto& claim : row.claims) {
            const auto owner = projected_owners.find(claim);
            if (owner != projected_owners.end()) {
                if (owner->second && *owner->second != id) {
                    claim_conflict = true;
                } else {
                    owner->second = id;
                }
                continue;
            }
            const auto committed_owner = claim_owner_.find(claim);
            if (committed_owner != claim_owner_.end() && committed_owner->second != id) {
                claim_conflict = true;
            }
            projected_owners.emplace(claim, id);
        }
    }
    if (row_conflict) {
        return CommitStatus::kWriteConflict;
    }
    if (claim_conflict) {
        return CommitStatus::kUniqueConflict;
    }
    for (const auto& [id, row] : txn.writes_) {
        batch_rows.insert(id);
        if (!txn.inserts_.count(id)) {
            const auto old = ReadCommitted(id, published_epoch_);
            if (old) {
                batch_claims.insert(old->claims.begin(), old->claims.end());
            }
        }
        batch_claims.insert(row.claims.begin(), row.claims.end());
    }
    return CommitStatus::kCommitted;
}

void EpochSiEngine::Install(const std::vector<Txn*>& accepted, Epoch epoch) {
    size_t installed_ops = 0;
    if (crash_point_ == CrashPoint::kDuringInstall && crash_position_ == 0) {
        PoisonAndCrash();
    }
    for (Txn* txn : accepted) {
        for (const auto& [id, row] : txn->writes_) {
            if (id.local_id == std::numeric_limits<uint64_t>::max()) {
                throw std::overflow_error("replayed RowId exhausts table");
            }
            const auto old = ReadCommitted(id, published_epoch_);
            if (old) {
                for (const auto& claim : old->claims) {
                    const auto owner = claim_owner_.find(claim);
                    if (owner != claim_owner_.end() && owner->second == id) {
                        claim_owner_.erase(owner);
                    }
                    last_claim_epoch_[claim] = epoch;
                }
            }
            for (const auto& claim : row.claims) {
                claim_owner_[claim] = id;
                last_claim_epoch_[claim] = epoch;
            }
            auto version = std::make_unique<Version>(epoch, row, std::move(versions_[id]));
            versions_[id] = std::move(version);
            last_row_epoch_[id] = epoch;
            next_row_id_[id.table_id] = std::max(next_row_id_[id.table_id], id.local_id + 1);
            dirty_tables_.insert(id.table_id);
            ++version_count_;
            ++installed_ops;
            if (crash_point_ == CrashPoint::kDuringInstall && installed_ops == crash_position_) {
                PoisonAndCrash();
            }
        }
    }
}

EpochSiEngine::PreparedState EpochSiEngine::PreparePublication(const std::vector<Txn*>& accepted, Epoch epoch) const {
    PreparedState prepared{{}, {}, {}, {}, {}, {}, {}, version_count_, 0, 0};
    const bool claim_free = claim_owner_.empty() && last_claim_epoch_.empty() &&
                            std::all_of(accepted.begin(), accepted.end(), [](const Txn* txn) {
                                return std::all_of(txn->writes_.begin(), txn->writes_.end(),
                                                   [](const auto& write) { return write.second.claims.empty(); });
                            });
    for (Txn* txn : accepted) {
        for (const auto& [id, row] : txn->writes_) {
            if (id.local_id == std::numeric_limits<uint64_t>::max() ||
                prepared.version_count == std::numeric_limits<size_t>::max()) {
                throw std::overflow_error("publication state exhausted");
            }
            const bool inserted = prepared.versions.emplace(id, std::make_unique<Version>(epoch, row, nullptr)).second;
            if (!inserted) {
                throw std::logic_error("accepted batch writes one row more than once");
            }
            ++prepared.staged_versions;
            prepared.last_row_epoch.emplace(id, epoch);
            if (!claim_free && !txn->inserts_.count(id)) {
                const auto old = ReadCommitted(id, published_epoch_);
                if (old) {
                    for (const auto& claim : old->claims) {
                        const auto owner = claim_owner_.find(claim);
                        if (owner != claim_owner_.end() && owner->second == id) {
                            prepared.claim_owner_erases.emplace(claim, id);
                        }
                        prepared.last_claim_epoch.emplace(claim, epoch);
                    }
                }
            }
            for (const auto& claim : row.claims) {
                const auto [owner, owner_inserted] = prepared.claim_owner.emplace(claim, id);
                if (!owner_inserted && owner->second != id) {
                    throw std::logic_error("accepted batch has duplicate unique claim");
                }
                prepared.last_claim_epoch.emplace(claim, epoch);
            }
            const auto current_next = next_row_id_.find(id.table_id);
            const auto next = prepared.next_row_id.emplace(
                id.table_id, current_next == next_row_id_.end() ? 0 : current_next->second);
            next.first->second = std::max(next.first->second, id.local_id + 1);
            prepared.dirty_tables.insert(id.table_id);
            ++prepared.version_count;
        }
    }
    prepared.staged_entries = prepared.versions.size() + prepared.last_row_epoch.size() +
                              prepared.last_claim_epoch.size() + prepared.claim_owner.size() +
                              prepared.next_row_id.size() + prepared.dirty_tables.size() +
                              prepared.claim_owner_erases.size();
    return prepared;
}

bool EpochSiEngine::InstallPrepared(PreparedState&& prepared) noexcept {
    last_install_version_nodes_ = 0;
    for (const auto& [claim, id] : prepared.claim_owner_erases) {
        const auto owner = claim_owner_.find(claim);
        if (owner != claim_owner_.end() && owner->second == id) {
            claim_owner_.erase(owner);
        }
    }
    while (!prepared.claim_owner.empty()) {
        claim_owner_.insert(prepared.claim_owner.extract(prepared.claim_owner.begin()));
    }
    while (!prepared.versions.empty()) {
        auto node = prepared.versions.extract(prepared.versions.begin());
        const auto current = versions_.find(node.key());
        if (current == versions_.end()) {
            versions_.insert(std::move(node));
        } else {
            node.mapped()->older = std::move(current->second);
            current->second = std::move(node.mapped());
        }
        ++last_install_version_nodes_;
        if (crash_point_ == CrashPoint::kDuringInstall && crash_position_ == last_install_version_nodes_)
            return false;
    }
    while (!prepared.last_row_epoch.empty()) {
        auto node = prepared.last_row_epoch.extract(prepared.last_row_epoch.begin());
        const auto current = last_row_epoch_.find(node.key());
        if (current == last_row_epoch_.end()) {
            last_row_epoch_.insert(std::move(node));
        } else {
            current->second = node.mapped();
        }
    }
    while (!prepared.last_claim_epoch.empty()) {
        auto node = prepared.last_claim_epoch.extract(prepared.last_claim_epoch.begin());
        const auto current = last_claim_epoch_.find(node.key());
        if (current == last_claim_epoch_.end()) {
            last_claim_epoch_.insert(std::move(node));
        } else {
            current->second = node.mapped();
        }
    }
    while (!prepared.next_row_id.empty()) {
        auto node = prepared.next_row_id.extract(prepared.next_row_id.begin());
        const auto current = next_row_id_.find(node.key());
        if (current == next_row_id_.end()) {
            next_row_id_.insert(std::move(node));
        } else {
            current->second = std::max(current->second, node.mapped());
        }
    }
    while (!prepared.dirty_tables.empty()) {
        dirty_tables_.insert(prepared.dirty_tables.extract(prepared.dirty_tables.begin()));
    }
    version_count_ = prepared.version_count;
    last_publication_staged_entries_ = prepared.staged_entries;
    last_publication_staged_versions_ = prepared.staged_versions;
    return true;
}

EpochSiEngine::PreparedTableInstall
EpochSiEngine::PrepareTableInstall(std::shared_ptr<const ImmutableTable> table) const {
    if (!table || !CanInstallPristineTable(table->table_id()))
        throw std::logic_error("immutable table install requires a pristine table");
    PreparedTableInstall prepared{immutable_tables_, last_row_epoch_, last_claim_epoch_, claim_owner_, next_row_id_};
    prepared.tables.emplace(table->table_id(), table);
    prepared.next_row_id[table->table_id()] = table->next_local_id();
    return prepared;
}

void EpochSiEngine::InstallTablePrepared(PreparedTableInstall&& prepared) noexcept {
    immutable_tables_.swap(prepared.tables);
    if (diagnostics_) {
        for (const auto& [table_id, table] : immutable_tables_)
            table->SetDiagnostics(diagnostics_);
    }
    last_row_epoch_.swap(prepared.last_row_epoch);
    last_claim_epoch_.swap(prepared.last_claim_epoch);
    claim_owner_.swap(prepared.claim_owner);
    next_row_id_.swap(prepared.next_row_id);
}

void EpochSiEngine::VisitPublished(TableId table_id, const std::function<void(RowId, const Row&)>& visitor) {
    Txn txn = Begin();
    try {
        VisitScan(txn, table_id, visitor);
        Abort(txn);
    } catch (...) {
        if (txn.active_)
            Abort(txn);
        throw;
    }
}

void EpochSiEngine::PoisonAndCrash() {
    Poison();
    throw SimulatedCrash();
}

void EpochSiEngine::Poison() noexcept {
    poisoned_.store(true, std::memory_order_release);
    if (identity_)
        identity_->valid.store(false, std::memory_order_release);
}

EpochSiEngine::PreparedCommit::PreparedCommit() = default;
EpochSiEngine::PreparedCommit::~PreparedCommit() {
    if (pending_ && engine_) {
        if (pending_->synced && !pending_->published && !engine_->poisoned_.load(std::memory_order_acquire)) {
            engine_->Poison();
            std::terminate();
        }
        if (!pending_->published) {
            for (Txn* txn : pending_->accepted)
                txn->prepared_ = false;
        }
        engine_->pending_prepared_.store(false, std::memory_order_release);
    }
}

EpochSiEngine::PreparedCommit::PreparedCommit(PreparedCommit&& other) noexcept
    : engine_(other.engine_), pending_(std::move(other.pending_)) {
    other.engine_ = nullptr;
}

bool EpochSiEngine::PreparedCommit::needs_sync() const noexcept {
    return pending_ && !pending_->accepted.empty() && !pending_->synced;
}

bool EpochSiEngine::PreparedCommit::needs_publish() const noexcept {
    return pending_ && !pending_->accepted.empty() && !pending_->published;
}

const std::vector<CommitResult>& EpochSiEngine::PreparedCommit::results() const noexcept {
    static const std::vector<CommitResult> empty;
    return pending_ ? pending_->results : empty;
}

EpochSiEngine::PreparedCommit EpochSiEngine::PrepareCommitBatch(const std::vector<Txn*>& txns) {
    if (poisoned_.load(std::memory_order_acquire) || !identity_) {
        throw std::logic_error("crashed engine");
    }
    std::set<Txn*> distinct;
    for (Txn* txn : txns) {
        if (txn == nullptr || !distinct.insert(txn).second || txn->owner_ != identity_ || !txn->active_ ||
            txn->prepared_) {
            throw std::invalid_argument("commit batch contains null, duplicate, foreign, or inactive transaction");
        }
    }

    const auto diagnostics = diagnostics_;
    if (diagnostics) {
        diagnostics->commit_tickets.fetch_add(txns.size(), std::memory_order_relaxed);
        diagnostics->commit_batch_hist[std::min(txns.size(), diagnostics->commit_batch_hist.size() - 1)].fetch_add(
            1, std::memory_order_relaxed);
    }
    PreparedCommit token;
    token.pending_ = std::make_unique<PendingCommit>();
    if (pending_prepared_.exchange(true, std::memory_order_acq_rel))
        throw std::logic_error("prepared commit already outstanding");
    token.engine_ = this;
    PendingCommit& pending = *token.pending_;
    pending.results.resize(txns.size());
    std::set<RowId> batch_rows;
    std::set<ConstraintClaim> batch_claims;
    for (size_t index = 0; index < txns.size(); ++index) {
        Txn* txn = txns[index];
        if (txn->writes_.empty()) {
            pending.results[index].status = CommitStatus::kCommitted;
            pending.accepted.push_back(txn);
            continue;
        }
        const CommitStatus status = Certify(*txn, batch_rows, batch_claims);
        pending.results[index].status = status;
        if (status == CommitStatus::kCommitted) {
            pending.accepted.push_back(txn);
        } else {
            txn->Finish();
        }
    }
    if (pending.accepted.empty()) {
        pending_prepared_.store(false, std::memory_order_release);
        token.engine_ = nullptr;
        return token;
    }
    for (Txn* txn : pending.accepted)
        txn->prepared_ = true;
    if (published_epoch_ == std::numeric_limits<Epoch>::max()) {
        throw std::overflow_error("commit epoch exhausted");
    }
    if (pending.accepted.size() > std::numeric_limits<uint64_t>::max() - next_commit_seq_) {
        throw std::overflow_error("commit sequence exhausted");
    }
    if (wal_frame_count_ == std::numeric_limits<size_t>::max() ||
        pending.accepted.size() > std::numeric_limits<size_t>::max() - wal_transaction_count_) {
        throw std::overflow_error("WAL census exhausted");
    }
    const Epoch epoch = published_epoch_ + 1;
    std::vector<uint64_t> accepted_seqs;
    accepted_seqs.reserve(pending.accepted.size());
    size_t accepted_index = 0;
    for (size_t index = 0; index < txns.size(); ++index) {
        if (pending.results[index].status == CommitStatus::kCommitted) {
            const uint64_t seq = next_commit_seq_ + accepted_index++;
            pending.results[index] = {CommitStatus::kCommitted, epoch, seq};
            accepted_seqs.push_back(seq);
        }
    }

    const auto encode_started =
        diagnostics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    pending.frame = EncodeFrame(epoch, pending.accepted, accepted_seqs);
    if (diagnostics)
        diagnostics->commit_encode_ns.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - encode_started)
                    .count()),
            std::memory_order_relaxed);
    const auto prepare_started =
        diagnostics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    pending.publication = PreparePublication(pending.accepted, epoch);
    if (diagnostics)
        diagnostics->commit_prepare_ns.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - prepare_started)
                    .count()),
            std::memory_order_relaxed);
    pending.epoch = epoch;
    pending.accepted_count = pending.accepted.size();
    return token;
}

void EpochSiEngine::SyncPreparedCommit(PreparedCommit& token) {
    if (!token.engine_)
        throw std::logic_error("prepared commit is resolved");
    if (token.engine_ != this)
        throw std::invalid_argument("prepared commit belongs to another engine");
    if (poisoned_.load(std::memory_order_acquire))
        throw std::logic_error("crashed engine");
    if (!token.pending_ || token.pending_->published || token.pending_->synced || token.pending_->frame.empty())
        return;
    PendingCommit& pending = *token.pending_;
    try {
        if (crash_point_ == CrashPoint::kBeforeAppend)
            PoisonAndCrash();
        if (file_wal_) {
            if (crash_point_ == CrashPoint::kAfterPartialAppend) {
                file_wal_->Append(pending.frame, crash_position_);
                PoisonAndCrash();
            }
            file_wal_->Append(pending.frame);
            if (crash_point_ == CrashPoint::kAfterAppendBeforeSync)
                PoisonAndCrash();
            file_wal_->Sync();
        } else {
            volatile_wal_.insert(volatile_wal_.end(), pending.frame.begin(), pending.frame.end());
            if (crash_point_ == CrashPoint::kAfterPartialAppend) {
                volatile_wal_.resize(recovery_wal_image_.size() + std::min(crash_position_, pending.frame.size()));
                recovery_wal_image_ = volatile_wal_;
                PoisonAndCrash();
            }
            if (crash_point_ == CrashPoint::kAfterAppendBeforeSync) {
                recovery_wal_image_ = volatile_wal_;
                PoisonAndCrash();
            }
            recovery_wal_image_ = volatile_wal_; // Logical stabilization only; no physical I/O in memory mode.
        }
        if (crash_point_ == CrashPoint::kAfterSync)
            PoisonAndCrash();
        pending.synced = true;
    } catch (...) {
        Poison();
        throw;
    }
}

void EpochSiEngine::PublishPreparedCommit(PreparedCommit& token) {
    if (!token.engine_)
        throw std::logic_error("prepared commit is resolved");
    if (token.engine_ != this)
        throw std::invalid_argument("prepared commit belongs to another engine");
    if (poisoned_.load(std::memory_order_acquire))
        throw std::logic_error("crashed engine");
    if (!token.pending_ || token.pending_->published || token.pending_->accepted.empty())
        return;
    PendingCommit& pending = *token.pending_;
    if (!pending.synced) {
        Poison();
        std::terminate();
    }
    const auto diagnostics = diagnostics_;
    try {
        // kDuringInstall position 0 is before publication; N is after N row heads
        // have actually been linked into the poisoned in-memory state.
        if (crash_point_ == CrashPoint::kDuringInstall && crash_position_ == 0)
            PoisonAndCrash();
        const auto install_started =
            diagnostics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
        if (!InstallPrepared(std::move(pending.publication)))
            PoisonAndCrash();
        if (diagnostics)
            diagnostics->commit_install_ns.fetch_add(
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::steady_clock::now() - install_started)
                                          .count()),
                std::memory_order_relaxed);
    } catch (const SimulatedCrash&) {
        throw;
    } catch (...) {
        Poison();
        std::terminate();
    }
    if (crash_point_ == CrashPoint::kAfterInstallBeforePublish) {
        PoisonAndCrash();
    }
    const auto publish_started =
        diagnostics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    published_epoch_ = pending.epoch;
    next_commit_seq_ += pending.accepted_count;
    ++wal_frame_count_;
    wal_transaction_count_ += pending.accepted_count;
    if (diagnostics) {
        diagnostics->commit_frames.fetch_add(1, std::memory_order_relaxed);
        diagnostics->commit_publish_ns.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - publish_started)
                    .count()),
            std::memory_order_relaxed);
    }
    for (Txn* txn : pending.accepted) {
        txn->prepared_ = false;
        txn->Finish();
    }
    pending.published = true;
    pending_prepared_.store(false, std::memory_order_release);
    token.engine_ = nullptr;
    if (crash_point_ == CrashPoint::kAfterPublishBeforeReturn) {
        PoisonAndCrash();
    }
}

std::vector<CommitResult> EpochSiEngine::CommitBatch(const std::vector<Txn*>& txns) {
    PreparedCommit token = PrepareCommitBatch(txns);
    if (token.needs_sync())
        SyncPreparedCommit(token);
    if (token.needs_publish())
        PublishPreparedCommit(token);
    return std::move(token.pending_->results);
}

void EpochSiEngine::SetCrashPointForTest(CrashPoint point, size_t position) {
    if (poisoned_.load(std::memory_order_acquire) || !identity_ ||
        !identity_->valid.load(std::memory_order_acquire)) {
        throw std::logic_error("crashed engine");
    }
    crash_point_ = point;
    crash_position_ = position;
}

void EpochSiEngine::SetFileMaxWriteChunkForTest(size_t bytes) {
    if (poisoned_.load(std::memory_order_acquire) || !identity_ ||
        !identity_->valid.load(std::memory_order_acquire) || !file_wal_) {
        throw std::logic_error("engine has no usable file WAL");
    }
    file_wal_->SetMaxWriteChunkForTest(bytes);
}

void EpochSiEngine::CloseFileForTest() {
    if (poisoned_.load(std::memory_order_acquire) || !identity_ ||
        !identity_->valid.load(std::memory_order_acquire) || !file_wal_) {
        throw std::logic_error("engine has no usable file WAL");
    }
    file_wal_->CloseForTest();
}

} // namespace epoch_si_poc
