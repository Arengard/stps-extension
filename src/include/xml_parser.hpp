#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace polarsgodmode {

// Register XML parsing functions
void RegisterXmlParserFunctions(ExtensionLoader &loader);

} // namespace polarsgodmode
} // namespace duckdb
