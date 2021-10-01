
##
## initial testing
##
add_executable(test-writer test_writer.cpp)
target_link_libraries(test-writer PRIVATE parquet-writer)
