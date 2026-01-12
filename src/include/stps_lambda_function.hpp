#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension.hpp"

namespace duckdb {
namespace stps {

// Register lambda table function
void RegisterLambdaFunction(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
