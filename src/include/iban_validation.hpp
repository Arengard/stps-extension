#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace polarsgodmode {

// Register IBAN validation functions
void RegisterIbanValidationFunctions(ExtensionLoader &loader);

} // namespace polarsgodmode
} // namespace duckdb
