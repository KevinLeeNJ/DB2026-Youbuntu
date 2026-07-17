.PHONY: all build test clean run debug release format help client client-debug clean-client benchmark benchmark-sqlite benchmark-random-kill tpcc-go benchmark-clean

BUILD_DIR := build
BINARY := $(BUILD_DIR)/bin/rmdb
CMAKE := cmake
CTEST := ctest
JOBS := 8
CLIENT_JOBS := 4
TPCC_DB ?= tpcc_benchmark_db
TPCC_DATA_DIR ?= benchmark/tpcc/data
TPCC_RESULT ?= benchmark/tpcc/result.json
TPCC_SQLITE_PATH ?= benchmark/tpcc/tpcc.sqlite
TPCC_SQLITE_RESULT ?= benchmark/tpcc/result-sqlite.json
TPCC_SQLITE_BEGIN ?= immediate
TPCC_WAREHOUSES ?= 8
TPCC_WORKERS ?= 16
TPCC_WARMUP ?= 10
TPCC_MEASURE ?= 60
TPCC_ROUNDS ?= 1
TPCC_PROGRESS_INTERVAL ?= 5
TPCC_REGENERATE_DATA ?= 0
TPCC_PORT ?= 8765
TPCC_RESTART_TIMEOUT ?= 120
TPCC_THINK_MS ?= 0
TPCC_RECONNECT_EACH_TXN ?= 0
TPCC_ISOLATION ?= read-committed
TPCC_GO_BINARY := $(BUILD_DIR)/bin/tpcc-go
TPCC_RANDOM_DB ?= tpcc_random_kill_db
TPCC_RANDOM_WORKERS ?= 16
TPCC_RANDOM_WARMUP ?= 2
TPCC_RANDOM_MEASURE ?= 45
TPCC_RANDOM_CYCLES ?= 5
TPCC_RANDOM_MIN_KILL_DELAY ?= 3
TPCC_RANDOM_MAX_KILL_DELAY ?= 8
TPCC_RANDOM_SEED ?= 1

all: build

help:
	@echo "Available targets:"
	@echo "  make build           - Configure and build the project with cmake"
	@echo "  make test            - Run unit tests only"
	@echo "  make run             - Build and run the rmdb binary"
	@echo "  make clean           - Remove build directory"
	@echo "  make debug           - Build with debug flags (default)"
	@echo "  make release         - Build with release/optimized flags"
	@echo "  make format          - Format code with clang-format"
	@echo "  make client          - Build rmdb_client"
	@echo "  make client-debug    - Build rmdb_client with debug flags"
	@echo "  make benchmark       - Run rmdb TPC-C benchmark"
	@echo "  make benchmark-sqlite - Run SQLite TPC-C benchmark"
	@echo "  make benchmark-random-kill - Run TPC-C random kill-9 recovery consistency test"
	@echo "  make benchmark-clean - Remove benchmark runtime data, keep CSV files"

build:
	@if [ ! -d "$(BUILD_DIR)" ]; then \
		mkdir -p $(BUILD_DIR); \
		$(CMAKE) -B $(BUILD_DIR) -S . -DCMAKE_BUILD_TYPE=Debug; \
	fi
	@$(CMAKE) --build $(BUILD_DIR) --parallel $(JOBS)

test: build
	@cd $(BUILD_DIR) && $(CTEST) --output-on-failure

run: build
	@$(BINARY) testdb

clean:
	@rm -rf $(BUILD_DIR)
	@echo "Build directory removed."

debug: clean
	@mkdir -p $(BUILD_DIR)
	@$(CMAKE) -B $(BUILD_DIR) -S . -DCMAKE_BUILD_TYPE=Debug
	@$(CMAKE) --build $(BUILD_DIR) --parallel $(JOBS)

release: clean
	@mkdir -p $(BUILD_DIR)
	@$(CMAKE) -B $(BUILD_DIR) -S . -DCMAKE_BUILD_TYPE=Release
	@$(CMAKE) --build $(BUILD_DIR) --parallel $(JOBS)

format:
	@bash scripts/add_license.sh workspace
	@find src -name '*.cpp' -o -name '*.h' | xargs clang-format-18 -i
	@find test -name '*.cpp' -o -name '*.h' | xargs clang-format-18 -i
	@echo "Code formatted."

client:
	@mkdir -p rmdb_client/build
	@cd rmdb_client/build && $(CMAKE) .. -DCMAKE_BUILD_TYPE=Release
	@$(CMAKE) --build rmdb_client/build --target rmdb_client --parallel $(CLIENT_JOBS)

client-debug: clean-client
	@mkdir -p rmdb_client/build
	@cd rmdb_client/build && $(CMAKE) .. -DCMAKE_BUILD_TYPE=Debug
	@$(CMAKE) --build rmdb_client/build --target rmdb_client --parallel $(CLIENT_JOBS)

clean-client:
	@rm -rf rmdb_client/build
	@echo "Client build directory removed."

run-client: client
	@./rmdb_client/build/rmdb_client

tpcc-go:
	@mkdir -p $(BUILD_DIR)/bin
	@cd benchmark/tpcc/go && go build -o ../../../$(TPCC_GO_BINARY) ./cmd/tpcc-go

benchmark: build tpcc-go
	@scripts/benchmark_tpcc.sh \
		--binary $(BINARY) \
		--db-dir $(TPCC_DB) \
		--port $(TPCC_PORT) \
		--warehouses $(TPCC_WAREHOUSES) \
		--workers $(TPCC_WORKERS) \
		--warmup $(TPCC_WARMUP) \
		--measure $(TPCC_MEASURE) \
		--rounds $(TPCC_ROUNDS) \
		--progress-interval $(TPCC_PROGRESS_INTERVAL) \
		--data-dir $(TPCC_DATA_DIR) \
		--json-out $(TPCC_RESULT) \
		--rmdb-db-dir $(TPCC_DB) \
		--restart-timeout $(TPCC_RESTART_TIMEOUT) \
		--think-ms $(TPCC_THINK_MS) \
		--reconnect-each-txn $(TPCC_RECONNECT_EACH_TXN) \
		--isolation $(TPCC_ISOLATION) \
		--go-binary $(TPCC_GO_BINARY) \
		$$( [ "$(TPCC_REGENERATE_DATA)" = "1" ] && echo "--regenerate-data" )

benchmark-sqlite: tpcc-go
	@rm -f $(TPCC_SQLITE_PATH) $(TPCC_SQLITE_PATH)-wal $(TPCC_SQLITE_PATH)-shm $(TPCC_SQLITE_RESULT)
	@$(TPCC_GO_BINARY) --command load \
		--backend sqlite \
		--sqlite-path $(TPCC_SQLITE_PATH) \
		--data-dir $(TPCC_DATA_DIR) \
		--schema-dir benchmark/tpcc/schema
	@$(TPCC_GO_BINARY) --command run \
		--backend sqlite \
		--sqlite-path $(TPCC_SQLITE_PATH) \
		--sqlite-begin $(TPCC_SQLITE_BEGIN) \
		--workers $(TPCC_WORKERS) \
		--warmup $(TPCC_WARMUP) \
		--measure $(TPCC_MEASURE) \
		--rounds $(TPCC_ROUNDS) \
		--progress-interval $(TPCC_PROGRESS_INTERVAL) \
		--warehouse-policy terminal-home \
		--json-out $(TPCC_SQLITE_RESULT)

benchmark-random-kill: build tpcc-go
	@scripts/benchmark_tpcc_random_kill.sh \
		--binary $(BINARY) \
		--go-binary $(TPCC_GO_BINARY) \
		--db-dir $(TPCC_RANDOM_DB) \
		--workers $(TPCC_RANDOM_WORKERS) \
		--warmup $(TPCC_RANDOM_WARMUP) \
		--measure $(TPCC_RANDOM_MEASURE) \
		--cycles $(TPCC_RANDOM_CYCLES) \
		--min-kill-delay $(TPCC_RANDOM_MIN_KILL_DELAY) \
		--max-kill-delay $(TPCC_RANDOM_MAX_KILL_DELAY) \
		--seed $(TPCC_RANDOM_SEED) \
		--isolation $(TPCC_ISOLATION)

benchmark-clean:
	@rm -rf $(TPCC_DB) $(TPCC_RANDOM_DB) $(TPCC_RESULT) $(TPCC_SQLITE_PATH) $(TPCC_SQLITE_PATH)-wal $(TPCC_SQLITE_PATH)-shm $(TPCC_SQLITE_RESULT) benchmark/tpcc/rmdb-server.log rmdb.log
	@echo "Benchmark runtime data removed; CSV files in $(TPCC_DATA_DIR) were kept."
