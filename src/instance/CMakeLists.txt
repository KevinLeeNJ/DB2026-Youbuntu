set(SOURCES db_instance.cpp)
add_library(instance STATIC ${SOURCES})
target_link_libraries(instance
    storage record index system transaction recovery planner analyze execution parser
)
