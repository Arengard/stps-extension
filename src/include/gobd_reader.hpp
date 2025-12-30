#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace polarsgodmode {

// Register GoBD reader functions
void RegisterGobdReaderFunctions(ExtensionLoader &loader);

} // namespace polarsgodmode
} // namespace duckdb
