#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register stps_smart_cast scalar function
void RegisterSmartCastScalarFunction(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
