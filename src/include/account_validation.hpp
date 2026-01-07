#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

void RegisterAccountValidationFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
