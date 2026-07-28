# Repository Guide

## Scope and Source of Truth

This is a C++17 RMDB database system. Server code is under `src/`, with entry point `src/rmdb.cpp`; the standalone
client is under `rmdb_client/`. Build artifacts belong in `build/` and `rmdb_client/build/`.

## Project Layout

- `src/parser`, `src/analyze`, `src/optimizer`: SQL parsing, semantic analysis, and planning.
- `src/execution`: scans, joins, aggregation, sorting, DML, and execution management.
- `src/protocol`: Wire Protocol v3 framing, typed encoding, validation, and socket I/O.
- `src/system`: database/table/index metadata, DDL, and file-handle management.
- `src/storage`, `src/record`, `src/index`, `src/replacer`: pages, buffer pool, records/MVCC metadata, B+ trees, and
  LRU.
- `src/transaction`: transaction lifecycle, MVCC, SI/SSI, watermark, and locking.
- `src/recovery`: WAL, checkpoint, crash analysis/redo/undo, and log truncation.
- `src/common`: shared configuration, context, exceptions, and definitions.
- `test/<subsystem>`: GoogleTest suites; `test/e2e/slt/`: SQLLogicTest cases; `test/e2e/`: live recovery helpers.

The main parser is handwritten; edit `src/parser/parser.cpp`, `src/parser/lexer.cpp`, and related headers instead of
regenerating Flex/Bison sources.

## Common Commands

- `make build`: configure a Debug build if needed and compile all targets.
- `make test`: build, then run CTest with failure output.
- `make run`: build and start `build/bin/rmdb testdb`.
- `make debug` / `make release`: clean and rebuild in the selected mode.
- `make client` / `make client-debug` / `make run-client`: build or run the standalone client.
- `make format`: apply `clang-format-18` to C++ under `src/` and `test/`.
- `make benchmark`: local 32-client SI smoke test, defaulting to 10-second warmup and two 60-second rounds. It is not
  the official 30-second warmup plus three 150-second ranking run. Diagnose it with repository-root `rmdb.log`.

## Final Competition Constraints

### Generality and Integrity

- The evaluator runs `rmdb` as a non-privileged user and owns all generated SQL, data, answers, and hidden checks. Never
  inspect, modify, or depend on evaluator assets or internals.
- Names, file names, build/load/check order, data, parameters, connection schedules, and crash points may vary. Never
  identify requests or substitute execution using names, SQL hashes, fixed `statement_id`, row counts, phases, timing,
  client IDs, or precomputed answers.
- Caches and fast paths must use current parameters and visible database state, maintain correct invalidation, and fall
  back to a general implementation. The official 50-warehouse/32-client setup is not a correctness assumption.
- Readiness requires the expected process tree to own the port and complete exact SQL `show tables;` through the Wire
  Protocol; accepting TCP alone is insufficient.

### Wire Protocol and Output

- Appendix A is byte-exact and incompatible with the historical NUL protocol: use the 8-byte v3 handshake, big-endian
  fields, 8-byte frame headers, 1 MiB payload limit, bounded allocation, and exact loops for short reads/writes.
- `EXEC_STREAM` returns exactly `META -> ROW* -> RESULT_END` for queries or `COMMAND_OK` for non-queries.
  `PREPARE_SET` atomically installs a connection-local typed statement/schema dictionary. `EXEC_BATCH` runs operations
  in order and returns one `BATCH_RESULT`.
- Every ranking batch uses `AUTO_ABORT`. On failure, roll back and end the active transaction before responding, return
  no partial results, and follow Appendix A's status/count rules. `TRANSACTION_ABORT` means rollback is already
  complete.
- Wire values are typed: `INT32`, raw IEEE-754 `FLOAT32`, and unpadded, non-NUL-terminated `CHAR`.
- The official evaluator neither reads `output.txt` nor sends `SET OUTPUT_FILE OFF/ON`. `output.txt` is only a legacy
  local-client/SLT surface; preserve its whitespace-sensitive format unless a task explicitly changes local behavior.

### SQL, Transactions, and FLOAT

- SNAPSHOT ISOLATION and SERIALIZABLE settings persist per connection for later transactions. Ranking sets SI before
  `PREPARE_SET`. SERIALIZABLE must satisfy the specified SSI histories; a statement creating a specified dangerous
  rw-dependency must immediately return `TRANSACTION_ABORT`, roll back the current transaction, and not defer victim
  choice to `COMMIT`.
- Support native server-side `COUNT(DISTINCT ...)` over joins/filters, including both
  `COUNT(DISTINCT col)` and `COUNT(DISTINCT (col))`. StockLevel has no client deduplication fallback.
- Preserve all five TPC-C transactions, including invalid-item NewOrder rollback, concurrent relative stock updates,
  earliest-order Delivery, newest-order OrderStatus, and median customer-by-name selection.
- Records, indexes, counters, visibility, and related writes must commit or roll back atomically on every execution
  path. Performance work must not weaken conflict detection, rollback, WAL, index maintenance, or ACID.
- FLOAT storage/literals/binds are binary32 with round-to-nearest, ties-to-even. Relative updates round once per
  binary32 operation; stale SI updates abort. `SUM(FLOAT)` accumulates exact binary32 inputs in binary64 and rounds once
  to binary32. Use `finalv2.md` for the precise ULP rules.

### Durability and Official Gates

- Before a successful `COMMIT` response, new WAL bytes and the commit record must be covered by an accepted
  stabilization operation on an auditable WAL object under the current database directory. Follow `finalv2.md` for
  allowed WAL names, sync mechanisms, `syncfs` boundaries, and parent-directory fsync requirements.
- Enforce WAL-before-data ordering. After SIGKILL, redo committed work, remove incomplete work, and restore consistent
  tables, indexes, metadata, and FLOAT results. Ranking restart budget is 90 seconds; earlier crash cases use 60
  seconds.
- The official database has dynamic `order_line` cardinality; total rows are `10,050,550 + order_line`. Loading and SQL
  validation share 900 seconds. Do not hard-code generated counts.
- Correctness, COMMIT durability, crash recovery, loading, online consistency, and post-restart consistency are
  mandatory gates. Ranking is the median NewOrder successful commits/min over three 150-second windows; diagnostics do
  not score.

## Development Rules

- State assumptions and a short validation plan before non-trivial implementation. If requirements are genuinely
  ambiguous and a choice would materially change behavior, ask before coding.
- Make the smallest change that satisfies the request. Avoid speculative features, one-off abstractions, adjacent
  refactors, and unrelated cleanup.
- Preserve user changes in a dirty worktree. Remove only unused code introduced by your own change.
- Every changed line should trace to the requested outcome. Prefer explicit ownership with references or
  `std::unique_ptr`, following existing local patterns.
- Define success with reproducible checks: add a regression test for a bug, test invalid input for validation work, and
  compare behavior before/after a refactor.

Follow `.clang-format`: LLVM style, 4 spaces, no tabs, attached braces, 120-column limit, left-aligned pointers, and no
include sorting. Use existing naming conventions: mostly `snake_case`, `PascalCase` classes/fixtures, and `_test.cpp`.

## Testing

Direct `test/` subdirectories containing C++ produce `build/bin/test/<subsystem>_test`; CTest also runs the live Python
Wire Protocol test.

- Parser/AST: `test/parser/parser_test.cpp`.
- Analyzer/planner/executor: `test/analyze/`, `test/optimizer/`, `test/execution/`.
- Protocol: `test/protocol/wire_protocol_test.cpp` and `test/protocol/live_wire_protocol_test.py`.
- SI/SSI and transaction concurrency: `test/snapshot/` and `test/transaction/`.
- Storage, record, index, recovery, system, and replacer: matching subsystem directories.
- Complete SQL behavior: `.slt` cases under `test/e2e/slt/`; preserve exact expected table text.
- Live crash flows: `test/e2e/live_crash_recovery_matrix.py`, `crash_repro_strong.py`, and
  `live_crash_checkpoint.sh`.

Run `make test` before submission. During diagnosis, build and run the smallest relevant binary with
`--gtest_filter=Suite.Test`; for final-facing changes, add the relevant live protocol, concurrent transaction,
forced-crash, or local TPC-C smoke check. `make benchmark` supplements rather than replaces correctness tests.

Tests must be deterministic. Use unique temporary database directories and clean them in teardown; do not depend on or
pollute persistent local databases.

## Commits and Pull Requests

Use the repository's Conventional Commit style (`fix:`, `feat:`, `test(...)`, `perf:`) with a short imperative summary.
PRs should state behavior changes, affected modules, tests run, and known limitations.
