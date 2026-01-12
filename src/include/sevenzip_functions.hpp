#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register 7-Zip archive functions
void Register7zipFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
