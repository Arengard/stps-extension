#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register GoBD reader functions
void RegisterGobdReaderFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
