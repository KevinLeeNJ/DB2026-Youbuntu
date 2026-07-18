set(SOURCES jit_runtime.cpp jit_ir.cpp jit_manager.cpp)

add_library(jit STATIC ${SOURCES})
target_include_directories(jit PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(jit PRIVATE asmjit::asmjit)
