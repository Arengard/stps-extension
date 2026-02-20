#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include <vector>
#include <string>
#include <map>

namespace duckdb {
namespace stps {

// Shared GoBD data structures
struct GobdColumn {
    string name;
    string data_type;  // Numeric, AlphaNumeric, Date
    int accuracy = -1;
    int order = 0;
};

struct GobdTable {
    string name;
    string url;
    string description;
    vector<GobdColumn> columns;
};

// Import pipeline data: populated by each source, consumed by shared pipeline
struct GobdImportData {
    vector<GobdTable> tables;                   // parsed from index.xml
    std::map<string, string> csv_contents;      // table URL -> CSV content string
};

// Result of importing a single table
struct GobdImportResult {
    string table_name;      // snake_case DuckDB table name
    int64_t rows_imported;
    int32_t columns_created;
    string error;           // empty if success
};

// Shared import pipeline: creates tables, normalizes columns, drops empty cols, smart-casts
vector<GobdImportResult> ExecuteGobdImportPipeline(ClientContext &context,
                                                    const GobdImportData &data,
                                                    char delimiter,
                                                    bool overwrite);

// Encoding helpers
bool IsValidUtf8(const std::string &str);
std::string ConvertWindows1252ToUtf8(const std::string &input);
std::string EnsureUtf8(const std::string &input);
std::string ConvertToUtf8(const std::string &input, const std::string &encoding);

// Parse GoBD index.xml from an in-memory XML string
vector<GobdTable> ParseGobdIndexFromString(const string &xml_content);

// Convert GoBD data type to DuckDB LogicalType
LogicalType GobdTypeToDuckDbType(const string &gobd_type, int accuracy = -1);

// Parse a single CSV line respecting quotes
vector<string> ParseCsvLine(const string &line, char delimiter);

// Simple XML text extraction between tags
string ExtractTagValue(const string &xml, const string &tag_name, size_t start_pos = 0);

// Extract all occurrences of a tag
vector<string> ExtractAllTags(const string &xml, const string &tag_name);

// Register GoBD reader functions
void RegisterGobdReaderFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
