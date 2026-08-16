add_library(deltakernel STATIC
    delta_database.cpp
    core/epoch_si_engine.cpp
    core/file_wal.cpp
    core/checkpoint_db.cpp
    sidecar_sorter.cpp)
target_include_directories(deltakernel PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/.. ${CMAKE_CURRENT_SOURCE_DIR}/core)
target_link_libraries(deltakernel PUBLIC pthread)
