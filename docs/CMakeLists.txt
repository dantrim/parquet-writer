find_package(Doxygen REQUIRED)
find_package(Sphinx REQUIRED)

# get the header files
set(PARQUET_WRITER_HEADER_DIR ${PROJECT_SOURCE_DIR}/src/cpp)
file(GLOB_RECURSE PARQUET_WRITER_HEADER_FILES ${PARQUET_WRITER_HEADER_DIR}/parquet_writer*.h)

set(DOXYGEN_INPUT_DIR ${PROJECT_SOURCE_DIR}/src/cpp)
set(DOXYGEN_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/docs/doxygen)
set(DOXYGEN_INDEX_FILE ${DOXYGEN_OUTPUT_DIR}/html/index.html)
set(DOXYFILE_IN ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in)
set(DOXYFILE_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)

# replace variables
configure_file(${DOXYFILE_IN} ${DOXYFILE_OUT} @ONLY)
file(MAKE_DIRECTORY ${DOXYGEN_OUTPUT_DIR})

add_custom_command(OUTPUT ${DOXYGEN_INDEX_FILE}
                   DEPENDS ${PARQUET_WRITER_HEADER_FILES}
                   COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYFILE_OUT}
                   MAIN_DEPENDENCY ${DOXYFILE_OUT} ${DOXYFILE_IN}
                   COMMENT "Generating docs")
add_custom_target(Doxygen ALL DEPENDS ${DOXYGEN_INDEX_FILE})


# SPHINX configuration
set(SPHINX_SOURCE ${CMAKE_CURRENT_SOURCE_DIR})
set(SPHINX_BUILD ${CMAKE_CURRENT_BINARY_DIR}/docs/sphinx)
set(SPHINX_INDEX_FILE ${SPHINX_BUILDER}/index.html)
#add_custom_command(Sphinx ALL
#                  COMMAND ${SPHINX_EXECUTABLE} -b html
#				  # tell breathe where to find the doxygen output
#				  -Dbreathe_projects.parquet-writer=${DOXYGEN_OUTPUT_DIR}
#                  ${SPHINX_SOURCE} ${SPHINX_BUILD}
#                  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
#                  COMMENT "Generating documentation with Sphinx")

add_custom_command(OUTPUT ${SPHINX_INDEX_FILE}
	COMMAND 
		${SPHINX_EXECUTABLE} -b html
		# Tell Breathe where to find the Doxygen output
		-Dbreathe_projects.parquet-writer=${DOXYGEN_OUTPUT_DIR}/xml
		${SPHINX_SOURCE} ${SPHINX_BUILD}
	WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
	DEPENDS 
		# Other docs files you want to track should go here (or in some variable)
		${CMAKE_CURRENT_SOURCE_DIR}/index.rst
		${DOXYGEN_INDEX_FILE}
	MAIN_DEPENDENCY ${SPHINX_SOURCE}/conf.py
	COMMENT "Generating documentation with Sphinx")

# Nice named target so we can run the job easily
add_custom_target(Sphinx ALL DEPENDS ${SPHINX_INDEX_FILE})

include(GNUInstallDirs)
install(DIRECTORY ${SPHINX_BUILD}
	DESTINATION ${CMAKE_INSTALL_DOCDIR})
