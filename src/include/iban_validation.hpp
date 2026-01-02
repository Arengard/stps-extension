#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register IBAN validation functions
void RegisterIbanValidationFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
