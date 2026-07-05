set(SOURCES
    table_write_service.cpp
    recovery_access.cpp
    tuple_meta_writer.cpp
    load_data_service.cpp
)
add_library(access STATIC ${SOURCES})
target_include_directories(access PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(access
    system record index transaction
)
