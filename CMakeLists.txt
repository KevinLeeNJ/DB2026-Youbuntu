cmake_minimum_required(VERSION 3.16)
project(RMDB)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra)

set(CMAKE_CXX_FLAGS_DEBUG "-O2 -g -ggdb3 -fno-omit-frame-pointer")
set(CMAKE_C_FLAGS_DEBUG "-O2 -g -ggdb3 -fno-omit-frame-pointer")

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    find_program(RMDB_LLD_EXECUTABLE NAMES ld.lld)
    if(RMDB_LLD_EXECUTABLE)
        message(STATUS "Using lld: ${RMDB_LLD_EXECUTABLE}")
        add_link_options(-fuse-ld=lld)
    else()
        find_program(RMDB_BFD_EXECUTABLE NAMES ld.bfd)
        if(RMDB_BFD_EXECUTABLE)
            message(STATUS "Using ld.bfd: ${RMDB_BFD_EXECUTABLE}")
            add_link_options(-fuse-ld=bfd)
        endif()
    endif()
endif()

enable_testing()
add_subdirectory(deps)
add_subdirectory(src)
add_subdirectory(test)
