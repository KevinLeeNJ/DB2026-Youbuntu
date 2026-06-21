# Repository Guide

## Project Structure and Module Organization

This is a C++17 RMDB database system. The core server code is in `src/`, and the server entry point is
`src/rmdb.cpp`; the standalone client is in `rmdb_client/`. Third-party dependencies, including GoogleTest, are
vendored under `deps/`. Build artifacts should be placed in `build/` and `rmdb_client/build/`.

The main module responsibilities are as follows:

- `src/parser`: handwritten lexer and recursive descent parser, AST definitions, and conversion from SQL text to
  syntax trees; the current `parser` target only compiles `parser.cpp` and `lexer.cpp`.
- `src/analyze`: semantic analysis, responsible for validation of table names, column names, types, expressions,
  aggregation, grouping, ordering, LIMIT, UNION, and generation of the `Query` structure.
- `src/optimizer`: query plan generation and basic optimization, constructing `Plan` nodes for scan, filter, join,
  projection, aggregation, sort, LIMIT, UNION, DML, DDL, transaction control, and so on.
- `src/execution`: executor framework and concrete executors, covering sequential scan, index scan, filter,
  projection, nested loop join, aggregation, sort, LIMIT, UNION, insert, delete, update, and execution management.
- `src/system`: system management and metadata management, responsible for creating, dropping, opening, and closing
  databases/tables/indexes, metadata persistence, DDL execution, and management of table files and index handles.
- `src/storage`: disk management, page abstraction, and buffer pool management, responsible for page allocation,
  reading, writeback, flushing, and WAL write-before constraints.
- `src/record`: record file management, in-page slot/bitmap/TupleMeta layout, record insert/delete/update/scan, and
  MVCC metadata access.
- `src/index`: B+ tree index management, composite key comparison, index node operations, insert/delete/search, and
  range scan.
- `src/transaction`: transaction lifecycle, commit/rollback, concurrency control modes, MVCC undo/version chains,
  watermark, and SSI dependency tracking.
- `src/transaction/concurrency`: lock manager, responsible for table-level and record-level shared locks, exclusive
  locks, intention locks, SIX locks, and unlocking.
- `src/recovery`: WAL logging, log management, checkpoint, crash recovery analyze/redo/undo, and post-recovery log
  truncation.
- `src/replacer`: buffer pool page replacement strategy, currently implementing the LRU replacer.
- `src/common`: cross-module common configuration, context, exceptions, and basic definitions.
- `src/test` and `src/test/performance_test`: test/performance-test helper targets inside the source tree, not part of
  the main server execution path.

Tests are organized by subsystem under `test/`, including directories such as `analyze`, `execution`, `index`, `nest`,
`optimizer`, `parser`, `portal`, `record`, `recovery`, `replacer`, `snapshot`, `storage`, and `system`. End-to-end
SQLLogicTest-style cases are in `test/e2e/slt/`, and `test/e2e/` also contains Python helper scripts for crash recovery
and related scenarios.

## Build, Test, and Development Commands

- `make build`: configure a Debug CMake build if needed and compile all targets.
- `make test`: build first, then run `ctest --output-on-failure` inside `build/`.
- `make run`: build and start `build/bin/rmdb testdb`.
- `make debug` / `make release`: clean first, then rebuild with Debug or Release parameters respectively.
- `make client`: build `rmdb_client` in Release mode.
- `make client-debug`: rebuild `rmdb_client` in Debug mode.
- `make run-client`: build and run the interactive client.
- `make parser`: historical Flex/Bison generation entry point; the current main parser is handwritten, so when changing
  parsing logic, prefer editing `src/parser/parser.cpp`, `src/parser/lexer.cpp`, and related headers.
- `make format`: apply `clang-format-18` to C++ source code under `src/` and `test/`.

## Coding Style and Naming Conventions

Follow `.clang-format`: LLVM-based style, 4-space indentation, no tabs, left braces do not move to a separate line,
120-column limit, left-aligned pointers, and no include sorting. Prefer the naming patterns already used in the current
module. Files and most functions use `snake_case`, classes and GoogleTest fixtures use `PascalCase`, and test files use
the `_test.cpp` suffix. When feasible, use `std::unique_ptr` or references to make ownership explicit.

## Behavioral Guidelines

The following guidelines are intended to reduce common LLM coding errors. By default, they favor caution over speed; for
very simple tasks, use judgment based on the actual situation.

### 1. Think Before Coding

Do not make assumptions, do not conceal confusion, and actively surface tradeoffs.

- Before starting an implementation, explicitly state your assumptions; if uncertain, ask the user.
- If there are multiple possible interpretations, list them instead of silently choosing one.
- If there is a simpler approach, state it; push back when necessary.
- If the requirement is unclear, stop, point out the specific confusion, and ask a question.

### 2. Prefer Simplicity

Use the smallest amount of code that solves the problem, and do not write speculative content.

- Do not implement features the user did not request.
- Do not add abstractions for one-off code.
- Do not add unrequested "flexibility" or "configurability".
- Do not add error handling for scenarios that cannot occur.
- If you wrote 200 lines but 50 lines would solve it, rewrite and simplify.

Self-check question: would a senior engineer consider this overcomplicated? If yes, simplify it.

### 3. Surgical Changes

Change only what must be changed, and clean up only problems caused by your own changes.

- Do not casually "improve" adjacent code, comments, or formatting.
- Do not refactor code that is not broken.
- Match the existing style, even if you would write it differently.
- If you find unrelated dead code, you may mention it, but do not delete it.
- Remove unused imports, variables, or functions caused by this change.
- Do not delete dead code that already existed before the change unless the user explicitly asks for it.

Acceptance criterion: every changed line should be directly traceable to the user's request.

### 4. Goal-Driven Execution

Define success criteria and iterate until validation passes.

- Turn "add validation" into "write a test for invalid input, then make the test pass".
- Turn "fix a bug" into "write a test that reproduces the bug, then make the test pass".
- Turn "refactor X" into "ensure tests pass both before and after the refactor".
- For multi-step tasks, first provide a short plan and write the validation method for each step.

Example:

```text
1. [Step] -> Validation: [check item]
2. [Step] -> Validation: [check item]
3. [Step] -> Validation: [check item]
```

Strong success criteria can support independent iterative progress; weak criteria, such as "make it work", usually
require continued clarification.

When these guidelines take effect, the result should be fewer unnecessary changes in the diff, less rework caused by
overcomplexity, and clarification questions happening before implementation rather than after things go wrong.

## `output.txt` Compatibility Rules

Cloud evaluation depends on the exact contents and format of `output.txt`.

- Never modify the output format of `output.txt`.
- Unless the user explicitly asks for it, do not add, delete, reorder, or rename fields, columns, headers, separators,
  prompts, status text, blank lines, or ending text in `output.txt`.
- Any change involving the output format of `output.txt` must receive user confirmation first; when requesting
  confirmation, state what you plan to modify and show an example of what the modified `output.txt` format would look
  like.
- Do not modify whitespace-sensitive formatting in `output.txt`, including spaces, tabs, newlines, alignment, and final
  newline.
- Do not localize or translate text written to `output.txt`.
- When modifying query execution, printing logic, logs, client output, or test framework behavior, confirm that
  `output.txt` remains byte-for-byte compatible unless the task explicitly requires changing the output.

## Testing Guide

Tests use GoogleTest and are automatically discovered by CMake from the direct subdirectories of `test/`: each directory
that contains `.cpp` files generates a `<directory_name>_test` target and outputs it to `build/bin/test/`. The current
main targets include `parser_test`, `analyze_test`, `optimizer_test`, `execution_test`, `storage_test`, `record_test`,
`index_test`, `recovery_test`, `system_test`, `snapshot_test`, `portal_test`, `nest_test`, `replacer_test`, and
`e2e_test`.

Choose the test location based on the scope of the change:

- Parser grammar, token, and AST structure changes go in `test/parser/parser_test.cpp`, directly asserting the node types
  and fields from `ast::parse_sql`; there is no need to start the database.
- Semantic analysis, plan generation, and executor internal logic go in `test/analyze/`, `test/optimizer/`, and
  `test/execution/` respectively.
- Low-level behavior for storage, records, indexes, buffer pool replacement, system management, recovery, and similar
  areas goes in the corresponding subdirectory, for example `test/recovery/log_recovery_test.cpp`.
- Changes involving complete SQL behavior, output tables, error paths, aggregation, UNION, JOIN, transactions, index
  selection, or checkpoint should add or update `.slt` cases under `test/e2e/slt/` and ensure `e2e_test` passes.
- For crash recovery or long real server-level flow validation, refer to `test/e2e/live_crash_recovery_matrix.py`,
  `test/e2e/crash_repro_strong.py`, and `test/e2e/live_crash_checkpoint.sh`; these scripts depend on `build/bin/rmdb`
  and local port 8765.

`.slt` files use the three directives currently supported by the runner: `statement ok` means the SQL should succeed,
`statement error` means an `RMDBError` should be thrown, and `query` is followed by SQL, `----`, and the expected output
block. Expected output must match the actual `data_send`/`output.txt` table text; except for trailing extra newlines that
the runner already uniformly ignores, do not casually change column widths, separator lines, spaces, headers,
`Total record(s): N`, or similar content.

When running tests, execute `make test` by default before submission. While diagnosing issues, you may build first and
then run a single target, for example `./build/bin/test/parser_test`, `./build/bin/test/recovery_test`, or
`./build/bin/test/e2e_test`. To filter cases, use GoogleTest's `--gtest_filter=SuiteName.TestName`.

Tests should remain deterministic. Tests that need database directories must use unique directory names and clean them up
in teardown; the existing e2e fixture creates an isolated database by test name and cleans up `output.txt` after each
case. New tests should follow this isolation pattern to avoid depending on or polluting persistent local database
directories.

## Commit and Pull Request Guidelines

Recent history uses Conventional Commit prefixes such as `fix:`, `feat:`, `test(...)`, and `perf:`; keep this style and
use a short imperative summary, in either English or Chinese. Pull Requests should describe behavior changes, affected
modules, test commands that were run, and known limitations. Link related issues if any; attach logs or screenshots only
when user-visible client or CLI behavior is involved.
