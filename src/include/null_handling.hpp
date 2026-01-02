#pragma once

#include "duckdb.hpp"
#include <string>

namespace duckdb {
namespace stps {

// Null/empty string handling
std::string map_null_to_empty(const string_t& input, bool is_null);
bool should_map_empty_to_null(const std::string& input);

// Register null handling scalar functions
void RegisterNullHandlingFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
