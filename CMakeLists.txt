cmake_minimum_required(VERSION 3.15...3.28)

# Set extension name here
set(TARGET_NAME stps)

project(${TARGET_NAME})

# Check if building as part of DuckDB
if(NOT COMMAND build_static_extension)
  message(STATUS "Building STPS extension standalone")

  # Set up DuckDB paths
  set(DUCKDB_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/duckdb/src")
  set(DUCKDB_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/duckdb/src/include")

  # Check if DuckDB submodule is initialized
  if(NOT EXISTS "${DUCKDB_INCLUDE_DIR}/duckdb.hpp")
    message(FATAL_ERROR
      "DuckDB submodule not initialized. Please run:\n"
      "  git submodule update --init --recursive\n")
  endif()

  # Add DuckDB include directories
  include_directories(${DUCKDB_INCLUDE_DIR})
  include_directories(${DUCKDB_SRC_DIR})

  # Build DuckDB as a library first
  add_subdirectory(duckdb EXCLUDE_FROM_ALL)

  # Get DuckDB version (set by duckdb/CMakeLists.txt)
  if(NOT DEFINED DUCKDB_VERSION)
    set(DUCKDB_VERSION "v1.4.3")
  endif()

  message(STATUS "Building extension for DuckDB version: ${DUCKDB_VERSION}")

  # Define simplified extension build functions
  function(build_static_extension NAME)
    set(FILES "${ARGV}")
    list(REMOVE_AT FILES 0)
    add_library(${NAME}_extension STATIC ${FILES})
    target_link_libraries(${NAME}_extension duckdb_static)
    target_include_directories(${NAME}_extension PRIVATE src/include)
    
    # Add to export set to resolve CMake dependency error
    install(TARGETS ${NAME}_extension
            EXPORT DuckDBExports
            ARCHIVE DESTINATION lib
            LIBRARY DESTINATION lib)
  endfunction()

  function(build_loadable_extension NAME PARAMETERS)
    set(FILES "${ARGV}")
    list(REMOVE_AT FILES 0 1)

    set(TARGET_NAME ${NAME}_loadable_extension)

    # Create shared library
    add_library(${TARGET_NAME} SHARED ${FILES})

    # Link against DuckDB (static build for portability)
    target_link_libraries(${TARGET_NAME} duckdb_static)

    # Include directories
    target_include_directories(${TARGET_NAME} PRIVATE src/include)

    # Set properties
    set_target_properties(${TARGET_NAME} PROPERTIES
      PREFIX ""
      SUFFIX ".duckdb_extension"
      OUTPUT_NAME "${NAME}"
      DEFINE_SYMBOL ""
      CXX_VISIBILITY_PRESET hidden)

    # Define that we're building a loadable extension
    target_compile_definitions(${TARGET_NAME} PUBLIC -DDUCKDB_BUILD_LOADABLE_EXTENSION)

    # Platform-specific linking
    if(WIN32)
      # Windows: Link statically
      target_link_libraries(${TARGET_NAME} duckdb_static)
    elseif(APPLE)
      # macOS: Use visibility and exported symbols
      set(WHITELIST "-Wl,-exported_symbol,_${NAME}_duckdb_cpp_init")
      target_link_libraries(${TARGET_NAME} duckdb_static -Wl,-dead_strip ${WHITELIST})
    else()
      # Linux: Use visibility and exclude libs
      target_link_libraries(${TARGET_NAME} duckdb_static -Wl,--gc-sections -Wl,--exclude-libs,ALL)
    endif()

    # Add metadata footer (required for DuckDB to recognize the extension)
    add_custom_command(
      TARGET ${TARGET_NAME}
      POST_BUILD
      COMMAND ${CMAKE_COMMAND}
        -DABI_TYPE=CPP
        -DEXTENSION=$<TARGET_FILE:${TARGET_NAME}>
        -DPLATFORM_FILE=${CMAKE_BINARY_DIR}/duckdb/duckdb_platform_out
        -DVERSION_FIELD=${DUCKDB_VERSION}
        -DEXTENSION_VERSION=v1.0.0
        -DNULL_FILE=${CMAKE_CURRENT_SOURCE_DIR}/duckdb/scripts/null.txt
        -P ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/scripts/append_metadata.cmake
      COMMENT "Adding metadata footer to ${NAME} extension"
    )

    # Depend on duckdb_platform target
    add_dependencies(${TARGET_NAME} duckdb_platform)
  endfunction()
endif()

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

# Installation rules
install(
  TARGETS ${TARGET_NAME}_loadable_extension
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)
