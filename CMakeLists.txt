cmake_minimum_required(VERSION 3.15...3.28)

# Set extension name here
set(TARGET_NAME stps)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/stps_unified_extension.cpp
    src/utils.cpp
    src/case_transform.cpp
    src/text_normalize.cpp
    src/null_handling.cpp
    src/uuid_functions.cpp
    src/io_operations.cpp
    src/iban_validation.cpp
    src/xml_parser.cpp
    src/gobd_reader.cpp
    # Filesystem functions using DuckDB's FileSystem API
    src/path_function.cpp
    src/scan_function.cpp
    src/filesystem_functions.cpp
    src/shared/filesystem_utils.cpp
    src/shared/pattern_matcher.cpp
    src/shared/content_searcher.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${TARGET_NAME}_loadable_extension
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)
