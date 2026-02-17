#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

void RegisterMaskFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
