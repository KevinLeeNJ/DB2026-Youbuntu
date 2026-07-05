add_library(statement STATIC statement_runner.cpp)
target_include_directories(statement PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(statement PUBLIC diagnostics transaction recovery system access access_cursor execution planner parser record)
