if(RMDB_ENABLE_JIT)
    set(ASMJIT_STATIC ON CACHE BOOL "Build AsmJit as a static library" FORCE)
    set(ASMJIT_TEST OFF CACHE BOOL "Do not build AsmJit development targets" FORCE)
    add_subdirectory(asmjit)
endif()

add_subdirectory(googletest)
