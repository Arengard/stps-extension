#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

void RegisterCleanDatabaseFunction(ExtensionLoader& loader);

} // namespace stps
} // namespace duckdb
