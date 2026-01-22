cmake_minimum_required(VERSION 3.15...3.28)

# Set extension name here
set(TARGET_NAME stps)

project(${TARGET_NAME})
include_directories(src/include)

# Find libcurl (optional - AI functions won't work without it)
find_package(CURL QUIET)
if(CURL_FOUND)
    message(STATUS "libcurl found - AI functions will be enabled")
    add_compile_definitions(HAVE_CURL)
else()
    message(WARNING "libcurl not found - AI functions will be disabled. To enable: install vcpkg and run 'vcpkg install curl:x64-windows'")
endif()

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
    src/drop_null_columns_function.cpp
    src/drop_duplicates_function.cpp
    src/street_split.cpp
    src/plz_validation.cpp
    src/address_lookup.cpp
    src/search_columns_function.cpp
    src/ai_functions.cpp
    src/curl_utils.cpp
    # src/fill_functions.cpp  # Temporarily disabled - DuckDB API changed
    src/smart_cast_utils.cpp
    src/smart_cast_scalar.cpp
    src/smart_cast_function.cpp
    src/stps_lambda_function.cpp
    src/shared/filesystem_utils.cpp
    src/shared/pattern_matcher.cpp
    src/shared/content_searcher.cpp
    src/shared/archive_utils.cpp
    # German bank account check digit validation
    src/kontocheck/check_methods.cpp
    src/account_validation.cpp
    src/blz_lut_loader.cpp
    # ZIP archive reading functions (using miniz)
    src/zip_functions.cpp
    src/miniz/miniz.c
    # Nextcloud/WebDAV fetcher
    src/nextcloud_functions.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link zlib for BLZ LUT decompression
find_package(ZLIB REQUIRED)
target_include_directories(${TARGET_NAME}_loadable_extension PRIVATE ${ZLIB_INCLUDE_DIRS})
target_include_directories(${TARGET_NAME}_extension PRIVATE ${ZLIB_INCLUDE_DIRS})
target_link_libraries(${TARGET_NAME}_loadable_extension ${ZLIB_LIBRARIES})
target_link_libraries(${TARGET_NAME}_extension ${ZLIB_LIBRARIES})

# Link libcurl if available
if(CURL_FOUND)
    target_link_libraries(${TARGET_NAME}_loadable_extension CURL::libcurl)
    target_link_libraries(${TARGET_NAME}_extension CURL::libcurl)
    target_include_directories(${TARGET_NAME}_loadable_extension PRIVATE ${CURL_INCLUDE_DIRS})
    target_include_directories(${TARGET_NAME}_extension PRIVATE ${CURL_INCLUDE_DIRS})
endif()

# Add the static extension to DuckDB's export set to resolve linking
if(TARGET ${TARGET_NAME}_extension)
    install(TARGETS ${TARGET_NAME}_extension
            EXPORT DuckDBExports
            ARCHIVE DESTINATION lib
            LIBRARY DESTINATION lib)
endif()

install(
  TARGETS ${TARGET_NAME}_loadable_extension
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)
