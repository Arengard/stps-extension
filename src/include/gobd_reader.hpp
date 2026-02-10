#pragma once

#include "duckdb.hpp"
#include <vector>
#include <string>

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
