#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {
namespace stps {

void RegisterTimeTravelFunctions(ExtensionLoader &loader);
void RegisterTimeTravelOptimizer(DatabaseInstance &db);

} // namespace stps
} // namespace duckdb
