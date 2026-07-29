set(SOURCES execution_manager.cpp prepared_jit.cpp prepared_plan_descriptor.cpp row_mutation.cpp)
add_library(execution STATIC ${SOURCES})

target_link_libraries(execution system record transaction planner)
target_link_libraries(execution jit)
target_compile_definitions(execution PRIVATE RMDB_ENABLE_JIT=1)
