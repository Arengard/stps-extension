#pragma once

#include "duckdb.hpp"
#include <string>

namespace duckdb {
namespace stps {

// UUID generation functions
std::string generate_uuid_v4();
std::string generate_uuid_v5(const std::string& name);

// Register UUID functions
void RegisterUuidFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
