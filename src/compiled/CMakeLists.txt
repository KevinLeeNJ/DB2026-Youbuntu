set(SOURCES
    bytecode_interpreter.cpp
    parameter_frame.cpp
    program_cache.cpp
    program_template.cpp
    program_runtime.cpp
    program_verifier.cpp)

add_library(compiled STATIC ${SOURCES})
target_include_directories(compiled PUBLIC ${CMAKE_SOURCE_DIR}/src)
