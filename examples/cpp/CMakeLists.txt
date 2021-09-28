##
## example showing how to fill all basic types and lists of them
##
add_executable(basic-example basic_example.cpp)
target_link_libraries(basic-example PRIVATE parquet-writer)

##
## example showing a few structs
##
add_executable(struct-buffer-example struct_buffer_example.cpp)
target_link_libraries(struct-buffer-example PRIVATE parquet-writer)

add_executable(struct-map-example struct_map_example.cpp)
target_link_libraries(struct-map-example PRIVATE parquet-writer)
