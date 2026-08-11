include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(analyze)
add_subdirectory(record)
add_subdirectory(index)
add_subdirectory(system)
add_subdirectory(execution)
add_subdirectory(parser)
add_subdirectory(optimizer)
add_subdirectory(storage)
add_subdirectory(common)
add_subdirectory(replacer)
add_subdirectory(transaction)
add_subdirectory(recovery)
add_subdirectory(protocol)
add_subdirectory(test)


target_link_libraries(parser execution pthread)

add_executable(rmdb rmdb.cpp server/database_instance.cpp server/wire_session.cpp server/server_runtime.cpp)
target_link_libraries(rmdb parser execution pthread planner analyze wire_protocol)

# Offline post-recovery structural verifier used by crash-consistency tests.
add_executable(rmdb_verify rmdb_verify.cpp)
target_link_libraries(rmdb_verify parser execution pthread planner analyze storage recovery)

# unit_test
add_executable(unit_test unit_test.cpp)
target_link_libraries(unit_test storage lru_replacer record gtest_main)  # add gtest

include(GoogleTest)
