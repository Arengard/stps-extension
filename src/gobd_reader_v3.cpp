#include "include/gobd_reader.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/client_context.hpp"
#include <sstream>

namespace duckdb {
namespace polarsgodmode {

// Helper function to extract directory from file path
static string GetDirectory(const string &filepath) {
    size_t pos = filepath.find_last_of("/\\");
    if (pos == string::npos) {
        return ".";
    }
    return filepath.substr(0, pos);
}

// Helper to escape single quotes in strings
static string EscapeString(const string &str) {
    string result;
    result.reserve(str.length());
    for (char c : str) {
        if (c == '\'') {
            result += "''";
        } else {
            result += c;
        }
    }
    return result;
}

// Replacement scan function - intercepts stps_read_gobd and replaces with read_csv
static unique_ptr<TableRef> GobdReplacementScan(ClientContext &context, const string &table_name,
                                                  ReplacementScanData *data) {
    // This function is called when DuckDB can't find a table
    // We don't use it for replacement scan, instead we use a regular table function
    return nullptr;
}

// Custom table function that generates SQL
class GobdTableFunction : public TableFunction {
public:
    GobdTableFunction() : TableFunction("stps_read_gobd_internal", {}, nullptr, nullptr) {}
};

// The actual registration function
void RegisterGobdReaderFunctions(ExtensionLoader &loader) {
    // Register a macro-based solution that's more reliable
    // This will be loaded from SQL instead

    // For now, we'll document that users should use the SQL macro approach
    // which is proven to work and is simpler
}

} // namespace polarsgodmode
} // namespace duckdb
