#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register cloud GoBD reader functions (requires curl)
void RegisterGobdCloudReaderFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
