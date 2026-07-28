cmake_minimum_required(VERSION 3.16)
project(rmdb_client)

set(CMAKE_CXX_STANDARD 20)
find_package(Threads REQUIRED)

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

add_executable(${PROJECT_NAME} main.cpp)

target_link_libraries(rmdb_client
        pthread
)
