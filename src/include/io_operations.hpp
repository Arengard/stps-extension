#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace polarsgodmode {

// Register I/O operation functions
void RegisterIoOperationFunctions(ExtensionLoader &loader);

} // namespace polarsgodmode
} // namespace duckdb
