set(SOURCES execution_manager.cpp result_sink.cpp row_mutation.cpp prepared_parameter_binding.cpp
            prepared_select_descriptor.cpp runtime/database_program_runtime.cpp
            runtime/delete_runtime.cpp runtime/insert_runtime.cpp runtime/point_lookup_runtime.cpp
            runtime/program_dispatcher.cpp)
add_library(execution STATIC ${SOURCES})

target_link_libraries(execution system record transaction planner compiled)
if(RMDB_ENABLE_JIT)
    target_link_libraries(execution jit)
    target_compile_definitions(execution PUBLIC RMDB_ENABLE_JIT=1)
endif()
