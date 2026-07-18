set(SOURCES execution_manager.cpp row_mutation.cpp)
add_library(execution STATIC ${SOURCES})

target_link_libraries(execution system record transaction planner)
if(RMDB_ENABLE_JIT)
    target_link_libraries(execution jit)
    target_compile_definitions(execution PUBLIC RMDB_ENABLE_JIT=1)
endif()
