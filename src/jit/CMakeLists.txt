set(SOURCES jit_runtime.cpp jit_ir.cpp jit_manager.cpp jit_predicate.cpp)

add_library(jit STATIC ${SOURCES})
target_include_directories(jit PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(jit PRIVATE asmjit::asmjit)

add_executable(jit_predicate_bench jit_predicate_benchmark.cpp)
target_link_libraries(jit_predicate_bench jit execution)
