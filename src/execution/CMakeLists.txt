set(SOURCES execution_manager.cpp prepared_plan_descriptor.cpp row_mutation.cpp)
add_library(execution STATIC ${SOURCES})

target_link_libraries(execution system record transaction planner)
