#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register stps_arrange table function
void RegisterArrangeFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
