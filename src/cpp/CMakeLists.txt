
add_library(parquet-writer SHARED)

target_sources(parquet-writer
    PRIVATE
    parquet_writer.cpp
    parquet_helpers.cpp
)

target_include_directories(parquet-writer PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(parquet-writer PUBLIC ${ARROW_INCLUDE_DIR} ${PARQUET_INCLUDE_DIR})
target_link_libraries(parquet-writer PUBLIC ${ARROW_SHARED_LIB} ${PARQUET_SHARED_LIB})
#target_link_libraries(parquet-writer PRIVATE nlohmann_json::nlohmann_json)

# if BUILD_TOOLS
add_subdirectory(tools/)
