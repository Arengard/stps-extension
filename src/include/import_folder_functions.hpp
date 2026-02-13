#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register folder import functions (local + cloud)
void RegisterImportFolderFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
