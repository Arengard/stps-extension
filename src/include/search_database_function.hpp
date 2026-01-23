#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

void RegisterSearchDatabaseFunction(ExtensionLoader& loader);

} // namespace stps
} // namespace duckdb
