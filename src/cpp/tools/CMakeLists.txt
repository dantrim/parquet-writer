
##
## initial testing
##
add_executable(test-writer test_writer.cpp)
target_link_libraries(test-writer PRIVATE parquet-writer)
add_executable(test-layout test_layout.cpp)
target_link_libraries(test-layout PRIVATE parquet-writer)
add_executable(test-value test_value.cpp)
target_link_libraries(test-value PRIVATE parquet-writer)
