.PHONY: all build test clean run debug release format help client client-debug clean-client run-client parser

BUILD_DIR := build
BINARY := $(BUILD_DIR)/bin/rmdb
CMAKE := cmake
CTEST := ctest
JOBS := 8
CLIENT_JOBS := 4
PARSER_DIR := src/parser

all: build

help:
	@echo "Available targets:"
	@echo "  make build         - Configure and build the project with cmake (8 threads)"
	@echo "  make test          - Run tests with ctest"
	@echo "  make run           - Build and run the rmdb binary"
	@echo "  make clean         - Remove build directory"
	@echo "  make debug         - Build with debug flags (default)"
	@echo "  make release       - Build with release/optimized flags"
	@echo "  make format        - Format code with clang-format"
	@echo "  make client        - Build rmdb_client (Release, 4 threads)"
	@echo "  make client-debug  - Build rmdb_client (Debug, 4 threads)"
	@echo "  make run-client    - Build and run rmdb_client"
	@echo "  make parser        - Regenerate parser from flex/bison sources"

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
	@find src -name '*.cpp' -o -name '*.h' | xargs clang-format-18 -i
	@echo "Code formatted."

client:
	@mkdir -p rmdb_client/build
	@cd rmdb_client/build && $(CMAKE) .. -DCMAKE_BUILD_TYPE=Release
	@$(CMAKE) --build rmdb_client/build --target rmdb_client --parallel $(CLIENT_JOBS)

client-debug:
	@mkdir -p rmdb_client/build
	@cd rmdb_client/build && $(CMAKE) .. -DCMAKE_BUILD_TYPE=Debug
	@$(CMAKE) --build rmdb_client/build --target rmdb_client --parallel $(CLIENT_JOBS)

clean-client:
	@rm -rf rmdb_client/build
	@echo "Client build directory removed."

run-client: client
	@rmdb_client/build/rmdb_client

parser:
	@cd $(PARSER_DIR) && flex --header-file=lex.yy.hpp -o lex.yy.cpp lex.l
	@cd $(PARSER_DIR) && bison --defines=yacc.tab.hpp -o yacc.tab.cpp yacc.y
	@echo "Parser regenerated."
