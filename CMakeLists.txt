cmake_minimum_required(VERSION 3.15 FATAL_ERROR)

project(parquet-writer)


set(CMAKE_CXX_STANDARD 17)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

if (NOT ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    find_package(PkgConfig REQUIRED)
else()
    set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
endif()

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_subdirectory(src/cpp/)
add_subdirectory(examples/cpp/)
