set(SOURCES
    analyze.cpp
    analyze_expr.cpp
    analyze_select.cpp
)
add_library(analyze STATIC ${SOURCES})

