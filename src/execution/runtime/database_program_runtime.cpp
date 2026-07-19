/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
*/

#include "database_program_runtime.h"

#include <atomic>
#include <cstring>

namespace {

using compiled::ExecStatus;
using compiled::RuntimeValue;
using compiled::ValueType;

RmRecord RuntimeTupleToRecord(const RuntimeValue& tuple) {
    if (tuple.type != ValueType::TUPLE || !tuple.initialized) {
        throw InternalError("database runtime requires an initialized tuple");
    }
    RmRecord record(static_cast<int>(tuple.tuple.size()));
    memcpy(record.data, tuple.tuple.data(), tuple.tuple.size());
    return record;
}

std::atomic<uint64_t> next_capability{1};

} // namespace

ExecStatus DatabaseProgramRuntime::Sticky() const noexcept {
    return last_status();
}

ExecStatus DatabaseProgramRuntime::Fail(ExecStatus status, const char* message, std::exception_ptr error) noexcept {
    if (Sticky() != ExecStatus::OK) {
        return Sticky();
    }
    if (error != nullptr) {
        pending_exception_ = std::move(error);
    }
    return SetError(status, message == nullptr ? std::string() : std::string(message));
}

ExecStatus DatabaseProgramRuntime::Fallback(const char* message) noexcept {
    if (Sticky() != ExecStatus::OK) {
        return Sticky();
    }
    if (fallback_allowed_) {
        return Fail(ExecStatus::FALLBACK, message);
    }
    try {
        throw InternalError("database runtime requested fallback after stateful execution began");
    } catch (...) {
        return Fail(ExecStatus::ERROR, message, std::current_exception());
    }
}

ExecStatus DatabaseProgramRuntime::Capture(ExecStatus status, const char* helper) noexcept {
    return Fail(status, helper, std::current_exception());
}

bool DatabaseProgramRuntime::CatalogMatches() const noexcept {
    return sm_manager_ != nullptr && bindings_.catalog_generation == sm_manager_->get_catalog_generation();
}

bool DatabaseProgramRuntime::BeginStateful() noexcept {
    if (Sticky() != ExecStatus::OK) {
        return false;
    }
    fallback_allowed_ = false;
    return true;
}

uint64_t DatabaseProgramRuntime::NewCapability() {
    const uint64_t capability = next_capability.fetch_add(1, std::memory_order_relaxed);
    if (capability == 0) {
        throw InternalError("database runtime capability space exhausted");
    }
    return capability;
}

uint64_t DatabaseProgramRuntime::RegisterRowCapability(const Rid& rid, const std::string& table_name, int table_fd,
                                                       uint64_t catalog_generation) {
    const uint64_t capability = NewCapability();
    row_capabilities_.emplace(capability, RowCapability{rid, table_name, table_fd, catalog_generation});
    return capability;
}

const DatabaseProgramRuntime::RowCapability& DatabaseProgramRuntime::ResolveRowCapability(const RuntimeValue& row,
                                                                                          const std::string& table_name,
                                                                                          int table_fd) const {
    if (row.type != ValueType::ROW_HANDLE || !row.initialized || row.opaque == 0) {
        throw InternalError("invalid row handle");
    }
    auto it = row_capabilities_.find(row.opaque);
    if (it == row_capabilities_.end()) {
        throw InternalError("row handle does not belong to this database runtime");
    }
    const RowCapability& capability = it->second;
    if (capability.table_name != table_name || capability.table_fd != table_fd ||
        capability.catalog_generation != bindings_.catalog_generation || !CatalogMatches()) {
        throw InternalError("row handle execution context mismatch");
    }
    return capability;
}

bool DatabaseProgramRuntime::HasOutstandingUpdate(int table_fd, const Rid& rid) const {
    for (const auto& [capability, prepared] : prepared_updates_) {
        (void)capability;
        if (prepared.table_fd == table_fd && prepared.rid == rid) {
            return true;
        }
    }
    return false;
}

ExecStatus DatabaseProgramRuntime::MakePointKey(uint32_t index_id, const RuntimeValue& value,
                                                RuntimeValue* key) noexcept {
    if (Sticky() != ExecStatus::OK) {
        return Sticky();
    }
    if (!CatalogMatches()) {
        return Fallback("catalog generation changed before point-key construction");
    }
    if (key == nullptr || value.type != ValueType::TUPLE || !value.initialized ||
        index_id >= bindings_.point_indexes.size()) {
        return Fallback("point-key binding is unavailable");
    }
    try {
        const auto& binding = bindings_.point_indexes[index_id];
        auto& table = sm_manager_->db_.get_table(binding.table_name);
        auto index_it = table.get_index_meta(binding.index_col_names);
        if (index_it == table.indexes.end() || binding.tuple_offsets.size() != index_it->cols.size()) {
            return Fallback("point-key index metadata changed");
        }
        std::string encoded(static_cast<size_t>(index_it->col_tot_len), '\0');
        size_t key_offset = 0;
        for (size_t i = 0; i < index_it->cols.size(); ++i) {
            const auto& column = index_it->cols[i];
            const size_t tuple_offset = binding.tuple_offsets[i];
            if (tuple_offset + static_cast<size_t>(column.len) > value.tuple.size()) {
                return Fallback("point-key tuple is too small");
            }
            memcpy(encoded.data() + key_offset, value.tuple.data() + tuple_offset, column.len);
            key_offset += static_cast<size_t>(column.len);
        }
        key->type = ValueType::POINT_KEY;
        key->bytes = std::move(encoded);
        key->opaque = index_id;
        key->initialized = true;
        return ExecStatus::OK;
    } catch (const TransactionAbortException&) {
        return Capture(ExecStatus::TXN_ABORT, "MakePointKey aborted");
    } catch (...) {
        return Capture(ExecStatus::ERROR, "MakePointKey failed");
    }
}

ExecStatus DatabaseProgramRuntime::PointLookup(const RuntimeValue& key, RuntimeValue* row,
                                               RuntimeValue* tuple) noexcept {
    if (Sticky() != ExecStatus::OK) {
        return Sticky();
    }
    if (!CatalogMatches()) {
        return Fallback("catalog generation changed before point lookup");
    }
    if (row == nullptr || tuple == nullptr || key.type != ValueType::POINT_KEY || !key.initialized ||
        key.opaque >= bindings_.point_indexes.size()) {
        return Fallback("point lookup binding is unavailable");
    }
    try {
        const auto& binding = bindings_.point_indexes[key.opaque];
        auto result = PointLookupRuntime::LookupEncoded(binding.table_name, binding.index_col_names, key.bytes.data(),
                                                        key.bytes.size(), sm_manager_, context_,
                                                        binding.index_name.empty() ? nullptr : &binding.index_name);
        if (result.status == PointLookupStatus::FALLBACK) {
            return Fallback("point lookup is ambiguous");
        }
        if (result.status == PointLookupStatus::NOT_FOUND || !result.rid.has_value()) {
            return Fail(ExecStatus::NO_MATCH_RESULT, nullptr);
        }
        auto* fh = sm_manager_->fhs_.at(binding.table_name).get();
        auto visible = GetVisibleRecord(fh, *result.rid, context_);
        if (visible == nullptr) {
            return Fail(ExecStatus::NO_MATCH_RESULT, nullptr);
        }
        row->type = ValueType::ROW_HANDLE;
        row->opaque = RegisterRowCapability(*result.rid, binding.table_name, fh->GetFd(), bindings_.catalog_generation);
        row->initialized = true;
        tuple->type = ValueType::TUPLE;
        tuple->tuple.assign(reinterpret_cast<const uint8_t*>(visible->data),
                            reinterpret_cast<const uint8_t*>(visible->data + visible->size));
        tuple->initialized = true;
        return ExecStatus::OK;
    } catch (const TransactionAbortException&) {
        return Capture(ExecStatus::TXN_ABORT, "PointLookup aborted");
    } catch (...) {
        return Capture(ExecStatus::ERROR, "PointLookup failed");
    }
}

ExecStatus DatabaseProgramRuntime::PrepareUpdate(const RuntimeValue& row, RuntimeValue* current_tuple,
                                                 RuntimeValue* prepared) noexcept {
    if (Sticky() != ExecStatus::OK) {
        return Sticky();
    }
    if (!CatalogMatches()) {
        return Fallback("catalog generation changed before UPDATE preparation");
    }
    if (bindings_.update_info == nullptr || bindings_.update_info->tab_name == nullptr ||
        bindings_.update_info->fh == nullptr || current_tuple == nullptr || prepared == nullptr) {
        return Fallback("UPDATE runtime binding is unavailable");
    }
    if (!BeginStateful()) {
        return Sticky();
    }
    try {
        const auto& row_capability =
            ResolveRowCapability(row, *bindings_.update_info->tab_name, bindings_.update_info->fh->GetFd());
        if (HasOutstandingUpdate(row_capability.table_fd, row_capability.rid)) {
            throw InternalError("row already has an outstanding prepared UPDATE");
        }
        auto token = RowMutationEngine::PrepareUpdate(row_capability.rid, *bindings_.update_info, context_);
        if (!token.has_value()) {
            return Fail(ExecStatus::NO_MATCH_RESULT, nullptr);
        }
        const RmRecord& locked_tuple = token->old_record();
        current_tuple->type = ValueType::TUPLE;
        current_tuple->tuple.assign(reinterpret_cast<const uint8_t*>(locked_tuple.data),
                                    reinterpret_cast<const uint8_t*>(locked_tuple.data + locked_tuple.size));
        current_tuple->initialized = true;
        const uint64_t capability = NewCapability();
        prepared_updates_.emplace(capability,
                                  PreparedUpdateCapability{std::move(*token), row_capability.rid,
                                                           row_capability.table_fd, bindings_.catalog_generation});
        prepared->type = ValueType::PREPARED_UPDATE;
        prepared->opaque = capability;
        prepared->initialized = true;
        return ExecStatus::OK;
    } catch (const TransactionAbortException&) {
        return Capture(ExecStatus::TXN_ABORT, "PrepareUpdate aborted");
    } catch (...) {
        return Capture(ExecStatus::ERROR, "PrepareUpdate failed");
    }
}

ExecStatus DatabaseProgramRuntime::CommitUpdate(const RuntimeValue& prepared,
                                                const RuntimeValue& proposed_tuple) noexcept {
    if (!BeginStateful()) {
        return Sticky();
    }
    try {
        if (bindings_.update_info == nullptr || prepared.type != ValueType::PREPARED_UPDATE || !prepared.initialized ||
            prepared.opaque == 0) {
            throw InternalError("invalid prepared UPDATE handle");
        }
        auto slot = prepared_updates_.find(prepared.opaque);
        if (slot == prepared_updates_.end()) {
            throw InternalError("prepared UPDATE handle does not belong to this database runtime or was consumed");
        }
        if (bindings_.update_info->fh == nullptr || slot->second.table_fd != bindings_.update_info->fh->GetFd() ||
            slot->second.catalog_generation != bindings_.catalog_generation || !CatalogMatches()) {
            throw InternalError("prepared UPDATE execution context mismatch");
        }
        if (proposed_tuple.type != ValueType::TUPLE || !proposed_tuple.initialized ||
            proposed_tuple.tuple.size() != static_cast<size_t>(bindings_.update_info->fh->get_file_hdr().record_size)) {
            throw InternalError("UPDATE tuple size does not match table record size");
        }
        RmRecord proposed = RuntimeTupleToRecord(proposed_tuple);
        PreparedUpdate token = std::move(slot->second.token);
        prepared_updates_.erase(slot);
        RowMutationEngine::CommitUpdate(std::move(token), proposed, *bindings_.update_info, context_);
        return ExecStatus::OK;
    } catch (const TransactionAbortException&) {
        return Capture(ExecStatus::TXN_ABORT, "CommitUpdate aborted");
    } catch (...) {
        return Capture(ExecStatus::ERROR, "CommitUpdate failed");
    }
}

ExecStatus DatabaseProgramRuntime::DeleteRow(const RuntimeValue& row) noexcept {
    if (Sticky() != ExecStatus::OK) {
        return Sticky();
    }
    if (!CatalogMatches()) {
        return Fallback("catalog generation changed before DELETE");
    }
    if (bindings_.delete_info == nullptr || bindings_.delete_info->tab_name == nullptr ||
        bindings_.delete_info->fh == nullptr) {
        return Fallback("DELETE runtime binding is unavailable");
    }
    if (!BeginStateful()) {
        return Sticky();
    }
    try {
        const auto& row_capability =
            ResolveRowCapability(row, *bindings_.delete_info->tab_name, bindings_.delete_info->fh->GetFd());
        if (!DeleteRuntime::DeleteOne(row_capability.rid, *bindings_.delete_info, context_)) {
            return Fail(ExecStatus::NO_MATCH_RESULT, nullptr);
        }
        return ExecStatus::OK;
    } catch (const TransactionAbortException&) {
        return Capture(ExecStatus::TXN_ABORT, "DeleteRow aborted");
    } catch (...) {
        return Capture(ExecStatus::ERROR, "DeleteRow failed");
    }
}

ExecStatus DatabaseProgramRuntime::InsertRow(const RuntimeValue& tuple, RuntimeValue* row) noexcept {
    if (Sticky() != ExecStatus::OK) {
        return Sticky();
    }
    if (!CatalogMatches()) {
        return Fallback("catalog generation changed before INSERT");
    }
    if (bindings_.insert_info == nullptr || bindings_.insert_info->tab_name == nullptr ||
        bindings_.insert_info->fh == nullptr || row == nullptr) {
        return Fallback("INSERT runtime binding is unavailable");
    }
    if (!BeginStateful()) {
        return Sticky();
    }
    try {
        if (tuple.type != ValueType::TUPLE || !tuple.initialized ||
            tuple.tuple.size() != static_cast<size_t>(bindings_.insert_info->fh->get_file_hdr().record_size)) {
            throw InternalError("INSERT tuple size does not match table record size");
        }
        RmRecord record = RuntimeTupleToRecord(tuple);
        const uint64_t capability = NewCapability();
        row_capabilities_.emplace(capability,
                                  RowCapability{Rid{-1, -1}, *bindings_.insert_info->tab_name,
                                                bindings_.insert_info->fh->GetFd(), bindings_.catalog_generation});
        const Rid rid = InsertRuntime::InsertOne(record, *bindings_.insert_info, context_);
        row_capabilities_.at(capability).rid = rid;
        row->type = ValueType::ROW_HANDLE;
        row->opaque = capability;
        row->initialized = true;
        return ExecStatus::OK;
    } catch (const TransactionAbortException&) {
        return Capture(ExecStatus::TXN_ABORT, "InsertRow aborted");
    } catch (...) {
        return Capture(ExecStatus::ERROR, "InsertRow failed");
    }
}

ExecStatus DatabaseProgramRuntime::EmitRow(const RuntimeValue& tuple) noexcept {
    if (Sticky() != ExecStatus::OK) {
        return Sticky();
    }
    if (bindings_.result_sink == nullptr || tuple.type != ValueType::TUPLE || !tuple.initialized) {
        return Fallback("result sink binding is unavailable");
    }
    if (!BeginStateful()) {
        return Sticky();
    }
    try {
        bindings_.result_sink->Emit(
            TupleView{reinterpret_cast<const char*>(tuple.tuple.data()), static_cast<uint32_t>(tuple.tuple.size())});
        return ExecStatus::OK;
    } catch (...) {
        return Capture(ExecStatus::ERROR, "EmitRow failed");
    }
}

ExecStatus DatabaseProgramRuntime::FinishResult() noexcept {
    const bool no_match = Sticky() == ExecStatus::NO_MATCH_RESULT;
    if (Sticky() != ExecStatus::OK && !no_match) {
        return Sticky();
    }
    if (no_match) {
        ClearError();
    }
    if (bindings_.result_sink == nullptr) {
        return Fallback("result sink binding is unavailable");
    }
    if (!BeginStateful()) {
        return Sticky();
    }
    try {
        bindings_.result_sink->Finish();
        return ExecStatus::OK;
    } catch (...) {
        return Capture(ExecStatus::ERROR, "FinishResult failed");
    }
}

void DatabaseProgramRuntime::RethrowPending() const {
    if (pending_exception_ != nullptr) {
        std::rethrow_exception(pending_exception_);
    }
    if (last_status() == ExecStatus::TXN_ABORT || last_status() == ExecStatus::ERROR) {
        throw InternalError(error_message());
    }
}
