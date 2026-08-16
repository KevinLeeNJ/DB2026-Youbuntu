include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

include(GoogleTest)

file(GLOB SUBDIRS CONFIGURE_DEPENDS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *)

foreach(DIR ${SUBDIRS})
    if(IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${DIR})
        if(DIR STREQUAL "deltakernel")
            continue()
        endif()
        file(GLOB_RECURSE DIR_SOURCES CONFIGURE_DEPENDS ${DIR}/*.cpp)
        
        if(DIR_SOURCES)
            set(TEST_TARGET "${DIR}_test")

            if(NOT TARGET ${TEST_TARGET})
                add_executable(${TEST_TARGET} ${DIR_SOURCES})

                set_target_properties(${TEST_TARGET} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/test")

                target_link_libraries(${TEST_TARGET}
                        "-Wl,--start-group"
                        analyze parser system storage lru_replacer record index transaction planner execution recovery wire_protocol deltakernel
                        "-Wl,--end-group"
                        gtest_main pthread)
                target_compile_definitions(${TEST_TARGET} PRIVATE RMDB_SRC_DIR="${CMAKE_SOURCE_DIR}")
                gtest_discover_tests(${TEST_TARGET})
            endif()
        endif()
    endif()
endforeach()

add_executable(deltakernel_test
    deltakernel/delta_database_test.cpp
    ${CMAKE_SOURCE_DIR}/src/server/database_instance.cpp)
set_target_properties(deltakernel_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/test")
target_link_libraries(deltakernel_test
        "-Wl,--start-group"
        analyze parser system storage lru_replacer record index transaction planner execution recovery wire_protocol deltakernel
        "-Wl,--end-group"
        gtest_main pthread)
target_compile_definitions(deltakernel_test PRIVATE RMDB_SRC_DIR="${CMAKE_SOURCE_DIR}")
gtest_discover_tests(deltakernel_test)

add_executable(deltakernel_core_test
    deltakernel/core/epoch_si_engine_test.cpp
    deltakernel/core/file_wal_test.cpp
    deltakernel/core/checkpoint_db_test.cpp)
set_target_properties(deltakernel_core_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/test")
target_link_libraries(deltakernel_core_test PRIVATE deltakernel gtest_main pthread)
gtest_discover_tests(deltakernel_core_test)

add_executable(deltakernel_sorter_test deltakernel/sidecar_sorter_test.cpp)
set_target_properties(deltakernel_sorter_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/test")
target_link_libraries(deltakernel_sorter_test PRIVATE deltakernel gtest_main pthread)
gtest_discover_tests(deltakernel_sorter_test)

find_package(Python3 COMPONENTS Interpreter REQUIRED)
add_test(NAME protocol_live_wire_test
         COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/protocol/live_wire_protocol_test.py
                 $<TARGET_FILE:rmdb>)
set_tests_properties(protocol_live_wire_test PROPERTIES TIMEOUT 90)

add_test(NAME deltakernel_live_wire_test
         COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/deltakernel/live_delta_wire_test.py
                 $<TARGET_FILE:rmdb>)
set_tests_properties(deltakernel_live_wire_test PROPERTIES TIMEOUT 45)
