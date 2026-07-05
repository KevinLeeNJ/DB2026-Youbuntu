set(SOURCES table_cursor.cpp index_cursor.cpp table_access.cpp)
add_library(access_cursor STATIC ${SOURCES})
target_link_libraries(access_cursor record index storage system transaction)
