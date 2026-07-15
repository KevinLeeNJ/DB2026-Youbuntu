cmake_minimum_required(VERSION 3.16)
project(RMDB)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra)

set(CMAKE_CXX_FLAGS_DEBUG "-O2 -g -ggdb3 -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG -march=native -funroll-loops")
set(CMAKE_C_FLAGS_DEBUG "-O2 -g -ggdb3 -fno-omit-frame-pointer")
set(CMAKE_C_FLAGS_RELEASE "-O2 -DNDEBUG -march=native -funroll-loops")

enable_testing()
add_subdirectory(src)
add_subdirectory(deps)
add_subdirectory(test)
