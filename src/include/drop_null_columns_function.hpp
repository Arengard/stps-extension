#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register drop null columns table function
void RegisterDropNullColumnsFunction(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
