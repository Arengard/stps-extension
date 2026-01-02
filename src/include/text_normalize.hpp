#pragma once

#include "duckdb.hpp"
#include <string>
#include <unordered_map>

namespace duckdb {
namespace stps {

// Text normalization functions
std::string remove_accents(const std::string& input, bool keep_umlauts = false);
std::string restore_umlauts(const std::string& input);
std::string normalize_text(const std::string& input, bool trim_ws = true, bool lower_case = false);
std::string clean_string(const std::string& input);

// German umlaut helpers
std::string convert_umlauts_to_ascii(const std::string& input);
std::string convert_ascii_to_umlauts(const std::string& input);

// Register text normalization scalar functions
void RegisterTextNormalizeFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
