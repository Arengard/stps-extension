#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {
namespace stps {

void RegisterSearchColumnsFunction(ExtensionLoader& loader);

} // namespace stps
} // namespace duckdb
