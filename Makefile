.PHONY: all build test clean run debug release format help client client-debug clean-client benchmark benchmark-ci benchmark-clean

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
	@echo "  make benchmark       - Run full TPC-C benchmark"
	@echo "  make benchmark-ci    - Run reduced CI TPC-C consistency benchmark"

build:
	@if [ ! -d "$(BUILD_DIR)" ]; then \
		mkdir -p $(BUILD_DIR); \
		$(CMAKE) -B $(BUILD_DIR) -S . -DCMAKE_BUILD_TYPE=Debug; \
	fi
	@$(CMAKE) --build $(BUILD_DIR) --parallel $(JOBS)

test: build
	@cd $(BUILD_DIR) && $(CTEST) --output-on-failure

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
		$(TPCC_DATA_ARGS)

benchmark-ci:
	@$(MAKE) benchmark TPCC_DB=tpcc_ci_db TPCC_RESULT=benchmark/tpcc/ci_result.json TPCC_WAREHOUSES=1 TPCC_WORKERS=2 TPCC_WARMUP=2 TPCC_MEASURE=8 TPCC_ROUNDS=1 TPCC_PROGRESS_INTERVAL=2 TPCC_RESTART_TIMEOUT=30

clean:
	@rm -rf $(BUILD_DIR)

release: clean
	@mkdir -p $(BUILD_DIR)
	@$(CMAKE) -B $(BUILD_DIR) -S . -DCMAKE_BUILD_TYPE=Release
	@$(CMAKE) --build $(BUILD_DIR) --parallel $(JOBS)

format:
	@bash scripts/add_license.sh workspace
	@find src -name '*.cpp' -o -name '*.h' | xargs clang-format-18 -i
