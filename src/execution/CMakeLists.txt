set(SOURCES execution_manager.cpp row_mutation.cpp)
add_library(execution STATIC ${SOURCES})

target_link_libraries(execution system record transaction planner)
