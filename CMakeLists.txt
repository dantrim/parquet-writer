cmake_minimum_required(VERSION 3.15 FATAL_ERROR)

project(parquet-writer)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake" "${CMAKE_SOURCE_DIR}/exts/nlohmann_json")

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
#find_package(nlohmann_json REQUIRED)
add_subdirectory(exts/)
include_directories(exts/json/single_include)

add_subdirectory(src/cpp/)
