#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register I/O operation functions
void RegisterIoOperationFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
