#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "checkpoint_db.h"
#include "common/context.h"
#include "parser/ast.h"

namespace deltakernel {

enum class DeltaValueType : uint8_t { Int, Float, Char };
struct PreparedDescription {
    bool query = false;
    std::vector<std::string> names;
    std::vector<DeltaValueType> types;
    uint64_t catalog_generation = 0;
};

class DeltaTransactionAbort : public std::runtime_error {
public:
    explicit DeltaTransactionAbort(const std::string& message) : std::runtime_error(message) {}
};

struct DeltaSession {
    std::optional<epoch_si_poc::EpochSiEngine::Txn> txn;
    bool explicit_txn = false;
};

class DeltaDatabase {
public:
    static std::unique_ptr<DeltaDatabase> Create(const std::string& directory);
    static std::unique_ptr<DeltaDatabase> Open(const std::string& directory);
    static bool IsDeltaDirectory(const std::string& directory);

    bool Execute(std::unique_ptr<ast::TreeNode> tree, DeltaSession& session, QueryResultSink* sink);
    uint64_t CatalogGeneration() const;
    PreparedDescription DescribePrepared(const ast::TreeNode& tree,
                                         const std::vector<DeltaValueType>& declared_parameters) const;
    void Abort(DeltaSession& session) noexcept;
    void Checkpoint();
    void SetCatalogSaveFailureForTest(bool fail) {
        fail_catalog_save_for_test_ = fail;
    }
    void SetCatalogPostRenameFailureForTest(bool fail) {
        fail_catalog_post_rename_for_test_ = fail;
    }
    void SetLoadBeforePublishHookForTest(std::function<void()> hook) {
        load_before_publish_hook_for_test_ = std::move(hook);
    }
    void SetExecuteLockHookForTest(std::function<void()> hook) {
        execute_lock_hook_for_test_ = std::move(hook);
    }

private:
    enum class ColumnType : uint8_t { Int, Float, Char };
    struct Column {
        std::string name;
        ColumnType type;
        uint32_t length;
        bool nullable;
    };
    struct Index {
        epoch_si_poc::ConstraintId constraint_id;
        std::vector<uint32_t> columns;
        bool unique;
    };
    struct TableSchema {
        epoch_si_poc::TableId id;
        uint32_t version;
        std::string name;
        std::vector<Column> columns;
        std::vector<Index> indexes;
    };
    struct Cell {
        bool is_null = true;
        int32_t integer = 0;
        float floating = 0;
        std::string text;
    };
    using Catalog = std::map<std::string, TableSchema>;

    explicit DeltaDatabase(epoch_si_poc::CheckpointDb db);
    void RequireUsable() const;
    void AbortLocked(DeltaSession& session) noexcept;
    void SaveCatalog(const Catalog& tables, epoch_si_poc::TableId next_table_id,
                     epoch_si_poc::ConstraintId next_constraint_id, uint64_t catalog_generation);
    void LoadCatalog();
    const TableSchema& Table(const std::string& name) const;
    epoch_si_poc::EpochSiEngine::Txn& Txn(DeltaSession& session);
    void Commit(DeltaSession& session);
    epoch_si_poc::RowImage EncodeRow(const TableSchema& schema, const std::vector<Cell>& cells) const;
    std::vector<Cell> DecodeRow(const TableSchema& schema, const epoch_si_poc::RowImage& image) const;
    std::vector<uint8_t> EncodeKey(const TableSchema& schema, const Index& index, const std::vector<Cell>& cells) const;
    Cell Literal(const Column& column, const ast::Value& value) const;
    bool Matches(const TableSchema& schema, const std::vector<Cell>& cells,
                 const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions) const;
    void EmitRows(const TableSchema& schema, const ast::SelectStmt& select, const std::vector<std::vector<Cell>>& rows,
                  QueryResultSink* sink, bool aggregate_values = false, bool query_started = false) const;
    void EmitCells(const std::vector<Column>& columns, const std::vector<std::vector<Cell>>& rows,
                   QueryResultSink* sink) const;
    void EmitTables(QueryResultSink* sink) const;
    void LoadCsv(const ast::LoadStmt& load, DeltaSession& session);

    std::string directory_;
    epoch_si_poc::CheckpointDb db_;
    Catalog tables_;
    epoch_si_poc::TableId next_table_id_ = 1;
    epoch_si_poc::ConstraintId next_constraint_id_ = 1;
    uint64_t catalog_generation_ = 1;
    bool fail_catalog_save_for_test_ = false;
    bool fail_catalog_post_rename_for_test_ = false;
    bool poisoned_ = false;
    std::function<void()> load_before_publish_hook_for_test_;
    std::function<void()> execute_lock_hook_for_test_;
    mutable std::mutex mutex_; // ponytail: one interpreter lock; split only when measured contention requires it.
};

} // namespace deltakernel
