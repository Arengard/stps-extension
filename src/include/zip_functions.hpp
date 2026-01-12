#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register ZIP archive functions
void RegisterZipFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
