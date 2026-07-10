cmake_minimum_required(VERSION 3.16)
project(RMDB)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
endif()

option(RMDB_ENABLE_NATIVE_OPTIMIZATIONS "Build release binaries for the current host CPU" ON)

add_compile_options(-Wall -Wextra)

set(CMAKE_CXX_FLAGS_DEBUG "-O2 -g -ggdb3 -fno-omit-frame-pointer")
set(CMAKE_C_FLAGS_DEBUG "-O2 -g -ggdb3 -fno-omit-frame-pointer")

set(RMDB_RELEASE_FLAGS "-O3 -DNDEBUG -funroll-loops")
if(RMDB_ENABLE_NATIVE_OPTIMIZATIONS)
    string(APPEND RMDB_RELEASE_FLAGS " -march=native")
endif()
set(CMAKE_CXX_FLAGS_RELEASE "${RMDB_RELEASE_FLAGS}")
set(CMAKE_C_FLAGS_RELEASE "${RMDB_RELEASE_FLAGS}")

enable_testing()
add_subdirectory(src)
add_subdirectory(deps)
add_subdirectory(test)
