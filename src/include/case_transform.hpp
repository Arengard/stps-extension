#pragma once

#include "duckdb.hpp"
#include <string>

namespace duckdb {
namespace polarsgodmode {

// Case transformation functions
std::string to_snake_case(const std::string& input);
std::string to_camel_case(const std::string& input);
std::string to_pascal_case(const std::string& input);
std::string to_kebab_case(const std::string& input);
std::string to_const_case(const std::string& input);
std::string to_sentence_case(const std::string& input);
std::string to_title_case(const std::string& input);

// Register case transformation scalar functions
void RegisterCaseTransformFunctions(ExtensionLoader &loader);

} // namespace polarsgodmode
} // namespace duckdb
