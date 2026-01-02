#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register XML parsing functions
void RegisterXmlParserFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
