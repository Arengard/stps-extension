#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include <string>
#include <vector>

namespace duckdb {
namespace stps {

// Get file extension (lowercase)
std::string GetFileExtension(const std::string &filename);

// Check if file is a known binary format (parquet, xlsx, etc.)
bool IsBinaryFormat(const std::string &filename);

// Check if content looks like binary data
bool LooksLikeBinary(const std::string &content);

// Get platform-specific temp directory
std::string GetTempDirectory();

// Extract content to a temp file, returns the temp file path
std::string ExtractToTemp(const std::string &content, const std::string &original_filename, const std::string &prefix);

// Detect CSV delimiter from content
char DetectDelimiter(const std::string &content);

// Split a CSV line by delimiter (handles quoted fields)
std::vector<std::string> SplitLine(const std::string &line, char delimiter);

// Parse CSV content into column names, types, and rows
void ParseCSVContent(const std::string &content,
                     std::vector<std::string> &column_names,
                     std::vector<LogicalType> &column_types,
                     std::vector<std::vector<Value>> &rows);

} // namespace stps
} // namespace duckdb
