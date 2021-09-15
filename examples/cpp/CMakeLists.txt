##
## example showing how to fill all basic types and lists of them
##
add_executable(basic-example basic_example.cpp)
target_link_libraries(basic-example PRIVATE parquet-writer)

##
## example showing a few structs
##
add_executable(struct-example struct_example.cpp)
target_link_libraries(struct-example PRIVATE parquet-writer)
