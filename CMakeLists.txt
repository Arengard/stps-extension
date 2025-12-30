cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME polarsgodmode)

# No external dependencies needed
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/polarsgodmode_extension.cpp
    src/utils.cpp
    src/case_transform.cpp
    src/text_normalize.cpp
    src/null_handling.cpp
    src/uuid_functions.cpp
    src/io_operations.cpp
    src/iban_validation.cpp
    src/xml_parser.cpp
    src/gobd_reader.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
