set(SOURCES
    native_abi.cpp
    native_program.cpp
    runtime.cpp)

add_library(jit STATIC ${SOURCES})
target_include_directories(jit PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(jit PUBLIC compiled PRIVATE asmjit::asmjit)
