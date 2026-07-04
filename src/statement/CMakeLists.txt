add_library(statement INTERFACE)
target_include_directories(statement INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(statement INTERFACE diagnostics transaction recovery)
