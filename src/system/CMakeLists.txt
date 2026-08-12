set(SOURCES sm_manager.cpp sm_manager_table.cpp sm_manager_recovery.cpp bulk_load.cpp version_history.cpp)
add_library(system STATIC ${SOURCES})
target_link_libraries(system index record)
