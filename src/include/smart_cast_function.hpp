#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register stps_smart_cast and stps_smart_cast_analyze table functions
void RegisterSmartCastTableFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
