.PHONY: all build test clean run debug release format help client client-debug clean-client benchmark benchmark-clean

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
TPCC_WAREHOUSES ?= 8
TPCC_WORKERS ?= 16
TPCC_WARMUP ?= 10
TPCC_MEASURE ?= 60
TPCC_ROUNDS ?= 1
TPCC_PROGRESS_INTERVAL ?= 5
TPCC_REGENERATE_DATA ?= 0
TPCC_PORT ?= 8765
TPCC_RESTART_TIMEOUT ?= 120

TPCC_DATA_ARGS := --reuse-data-dir
ifeq ($(TPCC_REGENERATE_DATA),1)
TPCC_DATA_ARGS := --overwrite-data-dir
endif

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

benchmark: build
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
		$(TPCC_DATA_ARGS) \
		$$( [ "$(TPCC_REGENERATE_DATA)" = "1" ] && echo "--regenerate-data" )

benchmark-clean:
	@rm -rf $(TPCC_DB) $(TPCC_RESULT) benchmark/tpcc/rmdb-server.log
	@rm -rf $(TPCC_SQLITE_PATH) $(TPCC_SQLITE_PATH)-shm $(TPCC_SQLITE_PATH)-wal
	@find benchmark/tpcc -type d -name __pycache__ -prune -exec rm -rf {} +
	@echo "Benchmark runtime data removed; CSV files in $(TPCC_DATA_DIR) were kept."
