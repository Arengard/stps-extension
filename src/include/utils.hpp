#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>
#include <cctype>
#include <algorithm>

namespace duckdb {
namespace polarsgodmode {

// String utilities
std::string trim(const std::string& str);
std::string to_lower(const std::string& str);
std::string to_upper(const std::string& str);
bool is_empty_or_whitespace(const std::string& str);

// Character classification helpers
bool is_word_boundary(char c);
bool is_digit_char(char c);
bool is_alpha_char(char c);
bool is_upper_char(char c);
bool is_lower_char(char c);

// String splitting and joining
std::vector<std::string> split_words(const std::string& str);
std::string join_strings(const std::vector<std::string>& strings, const std::string& separator);

} // namespace polarsgodmode
} // namespace duckdb
