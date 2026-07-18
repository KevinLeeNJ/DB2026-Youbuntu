set(SOURCES statement_template_cache.cpp)
add_library(cache STATIC ${SOURCES})
target_include_directories(cache PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(cache PUBLIC parser pthread)
