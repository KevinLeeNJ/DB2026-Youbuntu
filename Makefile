.PHONY: all build test clean run debug release format help client client-debug clean-client benchmark

BUILD_DIR := build
BINARY := $(BUILD_DIR)/bin/rmdb
CMAKE := cmake
CTEST := ctest
JOBS := 8
CLIENT_JOBS := 4
TPCC_DB ?= tpcc_benchmark_db
TPCC_DATA_DIR ?= benchmark/tpcc/data
TPCC_RESULT ?= benchmark/tpcc/result.json
TPCC_WAREHOUSES ?= 8
TPCC_WORKERS ?= 16
TPCC_WARMUP ?= 30
TPCC_MEASURE ?= 360
TPCC_ROUNDS ?= 2
TPCC_PROGRESS_INTERVAL ?= 5

all: build

help:
	@echo "Available targets:"
	@echo "  make build         - Configure and build the project with cmake (8 threads)"
	@echo "  make test          - Run unit tests only"
	@echo "  make run           - Build and run the rmdb binary"
	@echo "  make clean         - Remove build directory"
	@echo "  make debug         - Build with debug flags (default)"
	@echo "  make release       - Build with release/optimized flags"
	@echo "  make format        - Format code with clang-format"
	@echo "  make client        - Build rmdb_client (Release, 4 threads)"
	@echo "  make client-debug  - Build rmdb_client (Debug, 4 threads)"
	@echo "  make benchmark     - Run rmdb TPC-C benchmark with live progress"

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
	@rm -rf $(TPCC_DB) $(TPCC_DATA_DIR) $(TPCC_RESULT)
	@$(BINARY) $(TPCC_DB) > benchmark/tpcc/rmdb-server.log 2>&1 & \
	server_pid=$$!; \
	trap 'kill $$server_pid 2>/dev/null || true' EXIT INT TERM; \
	sleep 1; \
	python3 -m benchmark.tpcc.tpcc_run all \
		--backend rmdb \
		--warehouses $(TPCC_WAREHOUSES) \
		--workers $(TPCC_WORKERS) \
		--warmup $(TPCC_WARMUP) \
		--measure $(TPCC_MEASURE) \
		--rounds $(TPCC_ROUNDS) \
		--progress-interval $(TPCC_PROGRESS_INTERVAL) \
		--data-dir $(TPCC_DATA_DIR) \
		--json-out $(TPCC_RESULT) \
		--rmdb-db-dir $(TPCC_DB); \
	status=$$?; \
	kill $$server_pid 2>/dev/null || true; \
	exit $$status
