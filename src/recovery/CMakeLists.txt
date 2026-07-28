set(SOURCES log_manager.cpp log_recovery.cpp index_structure_gate.cpp checkpoint_manager.cpp wal_reader.cpp)
add_library(recovery STATIC ${SOURCES})
add_library(recoverys SHARED ${SOURCES})
target_link_libraries(recovery system pthread)
