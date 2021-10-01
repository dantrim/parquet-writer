cmake_minimum_required(VERSION 3.15 FATAL_ERROR)

project(parquet-writer)

option(BUILD_TESTS "Whether or not to build the tests" OFF)
option(BUILD_DOCS "Whether or not to build the documentation" OFF)

set(CMAKE_CXX_STANDARD 17)
add_compile_options(-Wshadow)
add_compile_options(-Werror)
add_compile_options(-Wunused)
add_compile_options(-Wall)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
endif()

# documentation
if(BUILD_DOCS)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/FindSphinx.cmake RESULT_VARIABLE FIND_SPHINX_FOUND)
    add_subdirectory(docs/)
endif()

find_package(PkgConfig REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_subdirectory(exts/)
add_subdirectory(src/cpp/)
add_subdirectory(examples/cpp/)

