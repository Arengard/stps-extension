#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

void RegisterSearchColumnsFunction(ExtensionLoader& loader);

} // namespace stps
} // namespace duckdb
