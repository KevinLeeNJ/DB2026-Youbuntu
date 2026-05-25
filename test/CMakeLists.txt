include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

include(GoogleTest)

file(GLOB SUBDIRS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *)

foreach(DIR ${SUBDIRS})
    if(IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${DIR})
        file(GLOB_RECURSE DIR_SOURCES CONFIGURE_DEPENDS ${DIR}/*.cpp)
        
        if(DIR_SOURCES)
            set(TEST_TARGET "${DIR}_test")
            
            add_executable(${TEST_TARGET} ${DIR_SOURCES})
            
            set_target_properties(${TEST_TARGET} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/test")
            
            target_link_libraries(${TEST_TARGET} storage lru_replacer record gtest_main pthread)
            gtest_discover_tests(${TEST_TARGET})
        endif()
    endif()
endforeach()
