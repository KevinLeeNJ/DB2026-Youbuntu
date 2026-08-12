set(SOURCES
    concurrency/lock_manager.cpp
    concurrency/lock_manager_wait.cpp
    concurrency/lock_manager_unique.cpp
    transaction_manager.cpp
    transaction_ssi.cpp
    transaction_gc.cpp
    watermark.cpp)
add_library(transaction STATIC ${SOURCES})
target_link_libraries(transaction system recovery pthread)
