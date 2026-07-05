add_library(execution INTERFACE)
target_include_directories(execution INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(execution INTERFACE system record transaction planner access access_cursor)
