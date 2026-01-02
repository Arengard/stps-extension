#include "gobd_reader.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace stps {

// GoBD Reader Functions
//
// The GoBD reader functionality is provided via SQL macros for maximum
// compatibility and performance. To use:
//
// 1. Load the extension:
//    LOAD 'stps.duckdb_extension';
//
// 2. Load the GoBD macros:
//    .read gobd_functions.sql
//
// 3. Use the macros:
//    SELECT * FROM gobd_list_tables('path/to/index.xml');
//    SELECT * FROM gobd_extract_schema(stps_read_xml_json('path/to/index.xml')::JSON);
//
// For reading data, use read_csv with extracted schema:
//    SELECT * FROM read_csv('path/to/file.txt', delim=';', names=[...], header=false);

void RegisterGobdReaderFunctions(ExtensionLoader &loader) {
    // GoBD functions are provided as SQL macros in gobd_functions.sql
    // This keeps the implementation simple and avoids complex C++ table function issues
    // Users should load gobd_functions.sql after loading the extension
}

} // namespace stps
} // namespace duckdb
